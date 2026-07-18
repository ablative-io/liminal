use alloc::{vec, vec::Vec};

use crate::algebra::{ResourceVector, WideResourceVector};
use crate::outcome::CandidatePhase;
use crate::wire::{
    AttachSecret, BindingEpoch, ClosureCheckedEnvelope, ClosureRefusalReason,
    ConnectionIncarnation, Generation, OrderAllocatingEnvelope, RecordAdmission,
    RecordAdmissionAttemptToken, ServerValue,
};

use super::super::{
    ActiveBinding, AdmissionOrder, BindingState, BindingTerminalOwner, CapacityCounter,
    ClaimFrontiers, ClaimFrontiersRestore, ClosureAccounting, ClosureState,
    ConnectionConversationTracking, EnrollmentFingerprint, FrontierBinding, FrontierParticipant,
    LiveMember, LiveMemberRestore, MovableOrderClaim, MovableSequenceClaim,
    OrderClaimFrontierRestore, OrderClaims, OrderDirectOwner, OrderHigh, OrderLedger,
    PresentedIdentity, RecoverySequenceReserve, RetainedCausalRecord, RetainedCausalRecordKind,
    SequenceClaimFrontierRestore, SequenceClaims, SequenceDirectOwner, SequenceLedger,
    SequenceProductRangesRestore, TerminalProductRangeRestore,
};
use super::{
    OrdinaryProjectionLimits, RecordAdmissionDecision, RecordAdmissionPrestate,
    RetainedRecordCharge, apply_record_admission,
};

type TestResult<T = ()> = Result<T, &'static str>;
type TestFingerprint = [u8; 32];
type TestMember = LiveMember<TestFingerprint>;

const TOKEN: RecordAdmissionAttemptToken = RecordAdmissionAttemptToken::new([0xD1; 16]);

fn generation() -> TestResult<Generation> {
    Generation::new(7).ok_or("test generation must be nonzero")
}

fn member(conversation_id: u64, generation: Generation) -> TestResult<TestMember> {
    LiveMember::restore(LiveMemberRestore {
        participant_id: 0,
        conversation_id,
        generation,
        attach_secret: AttachSecret::new([0xA5; 32]),
        cursor: 0,
        enrollment_fingerprint: EnrollmentFingerprint::new([0xE1; 32]),
        latest_terminal: None,
    })
    .map_err(|_| "test member must restore")
}

fn admission(conversation_id: u64, generation: Generation) -> RecordAdmission {
    RecordAdmission {
        conversation_id,
        participant_id: 0,
        capability_generation: generation,
        record_admission_attempt_token: TOKEN,
        payload: vec![1],
    }
}

fn retained_record() -> RetainedCausalRecord {
    RetainedCausalRecord {
        delivery_seq: 1,
        admission_order: AdmissionOrder::new(1, CandidatePhase::OrdinaryRecord, 0),
        kind: RetainedCausalRecordKind::OrdinaryRecord {
            participant_index: 0,
        },
    }
}

fn frontiers(
    conversation_id: u64,
    binding_epoch: BindingEpoch,
    order_high: u64,
) -> TestResult<ClaimFrontiers> {
    let terminal = BindingTerminalOwner {
        participant_index: 0,
        binding_epoch,
    };
    let sequence = SequenceLedger::try_new(
        1,
        SequenceClaims::new(1, 1, 0, RecoverySequenceReserve::None),
    )
    .map_err(|_| "sequence ledger must be valid")?;
    let order = OrderLedger::try_new(
        OrderHigh::Allocated(order_high),
        OrderClaims::new(1, 1, false, false).map_err(|_| "order claims must fit")?,
    )
    .map_err(|_| "order ledger must be valid")?;
    let first_order = order_high
        .checked_add(1)
        .ok_or("first reserved order must fit")?;
    let second_order = order_high
        .checked_add(2)
        .ok_or("second reserved order must fit")?;
    ClaimFrontiers::restore(
        ClaimFrontiersRestore {
            conversation_id,
            active_identities: vec![FrontierParticipant::new(
                0,
                0,
                FrontierBinding::Bound(binding_epoch),
            )],
            identity_slot_limit: 1,
            retained_floor: 1,
            retained_record_limit: 8,
            retained_records: vec![retained_record()],
            active_marker_anchors: Vec::new(),
            historical_marker_deliveries: Vec::new(),
            historical_causal_facts: Vec::new(),
            sequence: SequenceClaimFrontierRestore {
                movable_claims: vec![
                    MovableSequenceClaim {
                        delivery_seq: 2,
                        owner: SequenceDirectOwner::MembershipExit {
                            participant_index: 0,
                        },
                    },
                    MovableSequenceClaim {
                        delivery_seq: 3,
                        owner: SequenceDirectOwner::BindingTerminal(terminal),
                    },
                ],
                immutable_candidates: Vec::new(),
                products: SequenceProductRangesRestore {
                    live_times_terminal: vec![TerminalProductRangeRestore {
                        start: 4,
                        length: 1,
                        terminal,
                    }],
                    live_times_replacement_terminal: None,
                    other_live_times_exit: Vec::new(),
                },
                recovery: None,
            },
            order: OrderClaimFrontierRestore {
                movable_claims: vec![
                    MovableOrderClaim {
                        transaction_order: first_order,
                        owner: OrderDirectOwner::ActiveBindingTerminal(terminal),
                    },
                    MovableOrderClaim {
                        transaction_order: second_order,
                        owner: OrderDirectOwner::MembershipExit {
                            participant_index: 0,
                        },
                    },
                ],
                immutable_candidates: Vec::new(),
                recovery: None,
            },
            recovery_marker_delivery_seq: None,
        },
        sequence,
        order,
    )
    .map_err(|_| "claim frontiers must restore")
}

fn accounting(cap: ResourceVector) -> TestResult<ClosureAccounting> {
    ClosureAccounting::try_new(
        ClosureState::Clear,
        0,
        0,
        0,
        0,
        ResourceVector::default(),
        WideResourceVector::new(2, 101),
        cap,
        0,
        2,
    )
    .map_err(|_| "closure accounting must be valid")
}

fn prestate<'a>(
    request: RecordAdmission,
    member: &'a TestMember,
    binding: &'a BindingState,
    binding_epoch: BindingEpoch,
    frontiers: ClaimFrontiers,
    accounting: ClosureAccounting,
) -> TestResult<RecordAdmissionPrestate<'a, TestFingerprint, TestFingerprint, TestFingerprint>> {
    let capacity =
        CapacityCounter::try_new(4, 1).map_err(|_| "connection capacity must be valid")?;
    Ok(RecordAdmissionPrestate::new(
        request,
        PresentedIdentity::Live(member),
        binding,
        binding_epoch,
        ConnectionConversationTracking::AlreadyTracked,
        capacity,
        accounting,
        ResourceVector::new(1, 100),
        frontiers,
        vec![RetainedRecordCharge::new(
            1,
            AdmissionOrder::new(1, CandidatePhase::OrdinaryRecord, 0),
            ResourceVector::new(1, 1),
        )],
        1,
        OrdinaryProjectionLimits::new(
            ResourceVector::new(1, 100),
            ResourceVector::new(2, 200),
            ResourceVector::new(2, 200),
        ),
    ))
}

#[test]
fn record_selector_names_order_exhaustion_and_preserves_owner() -> TestResult {
    let conversation_id = 44_001;
    let generation = generation()?;
    let binding_epoch = BindingEpoch::new(ConnectionIncarnation::new(44, 1), generation);
    let member = member(conversation_id, generation)?;
    let binding = BindingState::Bound(ActiveBinding {
        participant_id: 0,
        conversation_id,
        binding_epoch,
    });
    let order_high = u64::MAX - 2;
    let expected_frontiers = frontiers(conversation_id, binding_epoch, order_high)?;
    let decision = apply_record_admission(
        prestate(
            admission(conversation_id, generation),
            &member,
            &binding,
            binding_epoch,
            frontiers(conversation_id, binding_epoch, order_high)?,
            accounting(ResourceVector::new(10, 1_000))?,
        )?,
        ResourceVector::new(1, 1),
    );
    let RecordAdmissionDecision::Respond(refusal) = decision else {
        return Err("order shortfall must select a typed response");
    };
    let ServerValue::ConversationOrderExhausted(exhausted) = refusal.response().server_value()
    else {
        return Err("selector must name ConversationOrderExhausted");
    };
    let OrderAllocatingEnvelope::RecordAdmission(envelope) = exhausted.request() else {
        return Err("order refusal must retain the RecordAdmission envelope");
    };
    assert_eq!(envelope.record_admission_attempt_token, TOKEN);
    assert_eq!(exhausted.high(), order_high);
    assert_eq!(
        refusal.unchanged().prestate().frontiers(),
        &expected_frontiers
    );
    Ok(())
}

#[test]
fn record_selector_names_marker_closure_refusal_and_preserves_owner() -> TestResult {
    let conversation_id = 44_002;
    let generation = generation()?;
    let binding_epoch = BindingEpoch::new(ConnectionIncarnation::new(44, 2), generation);
    let member = member(conversation_id, generation)?;
    let binding = BindingState::Bound(ActiveBinding {
        participant_id: 0,
        conversation_id,
        binding_epoch,
    });
    let expected_frontiers = frontiers(conversation_id, binding_epoch, 1)?;
    let decision = apply_record_admission(
        prestate(
            admission(conversation_id, generation),
            &member,
            &binding,
            binding_epoch,
            frontiers(conversation_id, binding_epoch, 1)?,
            accounting(ResourceVector::new(2, 101))?,
        )?,
        ResourceVector::new(1, 1),
    );
    let RecordAdmissionDecision::Respond(refusal) = decision else {
        return Err("closure capacity must select a typed response");
    };
    let ServerValue::MarkerClosureCapacityExceeded(exceeded) = refusal.response().server_value()
    else {
        return Err("selector must name MarkerClosureCapacityExceeded");
    };
    let ClosureCheckedEnvelope::RecordAdmission(envelope) = &exceeded.request else {
        return Err("closure refusal must retain the RecordAdmission envelope");
    };
    assert_eq!(envelope.record_admission_attempt_token, TOKEN);
    assert!(matches!(exceeded.reason, ClosureRefusalReason::Capacity(_)));
    assert_eq!(
        refusal.unchanged().prestate().frontiers(),
        &expected_frontiers
    );
    Ok(())
}
