//! Honest full-frontier fixtures shared by lifecycle unit tests.

#![allow(clippy::expect_used)]

use alloc::vec;

use crate::wire::{BindingEpoch, ConnectionIncarnation};

use super::{
    BindingState, BindingTerminalOwner, ClaimFrontiers, ClaimFrontiersRestore, FrontierBinding,
    FrontierParticipant, ImmutableOrderCandidateMajorRestore, ImmutableSequenceCandidate,
    LiveMember, MovableOrderClaim, MovableSequenceClaim, OrderClaimFrontierRestore, OrderClaims,
    OrderDirectOwner, OrderHigh, OrderLedger, PendingFinalization, PreparedLeaveAuthority,
    RecoverySequenceReserve, SequenceClaimFrontierRestore, SequenceClaims, SequenceDirectOwner,
    SequenceLedger, SequenceProductRangesRestore, TerminalProductRangeRestore,
};

struct SettledBindingFixture {
    binding: FrontierBinding,
    terminal: Option<BindingTerminalOwner>,
    first_order: u64,
}

struct SettledClaimsFixture {
    sequence: alloc::vec::Vec<MovableSequenceClaim>,
    order: alloc::vec::Vec<MovableOrderClaim>,
    products: SequenceProductRangesRestore,
    active_count: u64,
}

fn settled_binding_fixture<F>(
    member: &LiveMember<F>,
    binding_state: BindingState,
    left_transaction_order: u64,
) -> Option<SettledBindingFixture> {
    let participant_id = member.participant_id();
    match binding_state {
        BindingState::Bound(binding)
            if binding.conversation_id == member.conversation_id()
                && binding.participant_id == participant_id =>
        {
            Some(SettledBindingFixture {
                binding: FrontierBinding::Bound(binding.binding_epoch),
                terminal: Some(BindingTerminalOwner {
                    participant_index: participant_id,
                    binding_epoch: binding.binding_epoch,
                }),
                first_order: left_transaction_order.checked_sub(1)?,
            })
        }
        BindingState::Detached => {
            let binding_epoch = member.latest_terminal().map_or_else(
                || BindingEpoch::new(ConnectionIncarnation::new(1, 1), member.generation()),
                super::CommittedBindingTerminal::binding_epoch,
            );
            Some(SettledBindingFixture {
                binding: FrontierBinding::Detached(binding_epoch),
                terminal: None,
                first_order: left_transaction_order,
            })
        }
        BindingState::Bound(_) | BindingState::PendingFinalization(_) => None,
    }
}

fn settled_claims_fixture(
    participant_id: u64,
    exit_seq: u64,
    left_transaction_order: u64,
    fixture: &SettledBindingFixture,
) -> SettledClaimsFixture {
    let mut sequence = vec![MovableSequenceClaim {
        delivery_seq: exit_seq,
        owner: SequenceDirectOwner::MembershipExit {
            participant_index: participant_id,
        },
    }];
    let mut order = vec![MovableOrderClaim {
        transaction_order: left_transaction_order,
        owner: OrderDirectOwner::MembershipExit {
            participant_index: participant_id,
        },
    }];
    let mut products = SequenceProductRangesRestore::default();
    let active_count = if let Some(owner) = fixture.terminal {
        let terminal_seq = exit_seq
            .checked_add(1)
            .expect("bound fixture has one T position");
        sequence.push(MovableSequenceClaim {
            delivery_seq: terminal_seq,
            owner: SequenceDirectOwner::BindingTerminal(owner),
        });
        products.live_times_terminal = vec![TerminalProductRangeRestore {
            start: terminal_seq
                .checked_add(1)
                .expect("bound fixture has one LxT position"),
            length: 1,
            terminal: owner,
        }];
        order.push(MovableOrderClaim {
            transaction_order: fixture.first_order,
            owner: OrderDirectOwner::ActiveBindingTerminal(owner),
        });
        1
    } else {
        0
    };
    order.sort_by_key(|claim| claim.transaction_order);
    SettledClaimsFixture {
        sequence,
        order,
        products,
        active_count,
    }
}

pub(super) fn settled_leave_authority<F>(
    member: &LiveMember<F>,
    binding_state: BindingState,
    left_transaction_order: u64,
    left_delivery_seq: u64,
) -> PreparedLeaveAuthority {
    let participant_id = member.participant_id();
    let fixture = settled_binding_fixture(member, binding_state, left_transaction_order)
        .expect("settled fixture has exact binding state and a usable A/X order");
    let high_watermark = left_delivery_seq
        .checked_sub(1)
        .expect("settled Leave appends after an allocated high watermark");
    let exit_seq = high_watermark
        .checked_add(1)
        .expect("settled fixture has one E position");
    let claims = settled_claims_fixture(participant_id, exit_seq, left_transaction_order, &fixture);
    let sequence = SequenceLedger::try_new(
        high_watermark,
        SequenceClaims::new(1, claims.active_count, 0, RecoverySequenceReserve::None),
    )
    .expect("settled fixture sequence ledger validates");
    let order = OrderLedger::try_new(
        high_before(fixture.first_order),
        OrderClaims::new(claims.active_count, 1, false, false)
            .expect("settled fixture order claims validate"),
    )
    .expect("settled fixture order ledger validates");
    let frontiers = ClaimFrontiers::restore(
        ClaimFrontiersRestore {
            conversation_id: member.conversation_id(),
            active_identities: vec![FrontierParticipant::new(
                participant_id,
                member.cursor(),
                fixture.binding,
            )],
            identity_slot_limit: participant_id
                .checked_add(1)
                .expect("participant has a half-open identity limit"),
            retained_floor: u128::from(high_watermark) + 1,
            retained_record_limit: 0,
            retained_records: vec![],
            active_marker_anchors: vec![],
            historical_marker_deliveries: vec![],
            historical_causal_facts: vec![],
            sequence: SequenceClaimFrontierRestore {
                movable_claims: claims.sequence,
                immutable_candidates: vec![],
                products: claims.products,
                recovery: None,
            },
            order: OrderClaimFrontierRestore {
                movable_claims: claims.order,
                immutable_candidates: vec![],
                recovery: None,
            },
            recovery_marker_delivery_seq: None,
        },
        sequence,
        order,
    )
    .expect("settled Leave fixture restores complete exact frontiers");
    frontiers
        .prepare_settled_leave_authority(member, binding_state)
        .expect("settled Leave fixture consumes exact X/A order authority")
}

pub(super) fn pending_leave_authority<F>(
    member: &LiveMember<F>,
    pending: PendingFinalization,
    terminal_delivery_seq: u64,
    left_transaction_order: u64,
) -> PreparedLeaveAuthority {
    let participant_id = member.participant_id();
    let terminal_order = pending.admission_order();
    let terminal_owner = BindingTerminalOwner {
        participant_index: participant_id,
        binding_epoch: pending.binding_epoch(),
    };
    let exit_seq = terminal_delivery_seq
        .checked_add(1)
        .expect("pending fixture has one following E position");
    let product_seq = exit_seq
        .checked_add(1)
        .expect("pending fixture has one LxT position");
    let high_watermark = terminal_delivery_seq
        .checked_sub(1)
        .expect("pending terminal fixture starts after sequence zero");
    let sequence = SequenceLedger::try_new(
        high_watermark,
        SequenceClaims::new(1, 1, 0, RecoverySequenceReserve::None),
    )
    .expect("pending fixture sequence ledger validates");
    let order = OrderLedger::try_new(
        high_before(terminal_order.transaction_order()),
        OrderClaims::new(0, 1, false, false).expect("pending fixture order claims validate"),
    )
    .expect("pending fixture order ledger validates");
    let frontiers = ClaimFrontiers::restore(
        ClaimFrontiersRestore {
            conversation_id: member.conversation_id(),
            active_identities: vec![FrontierParticipant::new(
                participant_id,
                member.cursor(),
                FrontierBinding::Detached(pending.binding_epoch()),
            )],
            identity_slot_limit: participant_id
                .checked_add(1)
                .expect("participant has a half-open identity limit"),
            retained_floor: u128::from(high_watermark) + 1,
            retained_record_limit: 0,
            retained_records: vec![],
            active_marker_anchors: vec![],
            historical_marker_deliveries: vec![],
            historical_causal_facts: vec![],
            sequence: SequenceClaimFrontierRestore {
                movable_claims: vec![MovableSequenceClaim {
                    delivery_seq: exit_seq,
                    owner: SequenceDirectOwner::MembershipExit {
                        participant_index: participant_id,
                    },
                }],
                immutable_candidates: vec![ImmutableSequenceCandidate::BindingTerminal {
                    delivery_seq: terminal_delivery_seq,
                    admission_order: terminal_order,
                    owner: terminal_owner,
                }],
                products: SequenceProductRangesRestore {
                    live_times_terminal: vec![TerminalProductRangeRestore {
                        start: product_seq,
                        length: 1,
                        terminal: terminal_owner,
                    }],
                    live_times_replacement_terminal: None,
                    other_live_times_exit: vec![],
                },
                recovery: None,
            },
            order: OrderClaimFrontierRestore {
                movable_claims: vec![MovableOrderClaim {
                    transaction_order: left_transaction_order,
                    owner: OrderDirectOwner::MembershipExit {
                        participant_index: participant_id,
                    },
                }],
                immutable_candidates: vec![ImmutableOrderCandidateMajorRestore {
                    transaction_order: terminal_order.transaction_order(),
                    candidate_keys: vec![terminal_order],
                }],
                recovery: None,
            },
            recovery_marker_delivery_seq: None,
        },
        sequence,
        order,
    )
    .expect("pending Leave fixture restores complete exact frontiers");
    frontiers
        .prepare_pending_leave_authority(member, pending)
        .expect("pending Leave fixture consumes exact terminal/X order authority")
}

const fn high_before(first_claim: u64) -> OrderHigh {
    match first_claim.checked_sub(1) {
        Some(high) => OrderHigh::Allocated(high),
        None => OrderHigh::Empty,
    }
}
