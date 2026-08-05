//! Shared claim geometry for the two-identity binding-fate fixtures.
//!
//! Both the F8 marker-poison units and the PRECEDENCE-CLAMP finalizer units
//! build the same frontier shape: two Detached identities, no binding-terminal
//! claim owed, a one-row retained suffix, and the frozen canonical claim order
//! (`E, T, M, RS, RT, L*T, L*RT, L_other*E`) for L=2, T=0, M=0 — two exits and
//! two exit products, and nothing else.
//!
//! That geometry lives here rather than in either unit file so the two cannot
//! drift apart, and so neither builder has to carry it inline. It is layout
//! only: no builder here chooses a retained row, a floor, a marker, or a
//! charge, because those are exactly the inputs the units under it vary.

use alloc::{
    format,
    string::{String, ToString},
    vec,
};

use crate::wire::DeliverySeq;

use crate::lifecycle::{
    ExitProductRangeRestore, MovableOrderClaim, MovableSequenceClaim, OrderClaimFrontierRestore,
    OrderClaims, OrderDirectOwner, OrderHigh, OrderLedger, RecoverySequenceReserve,
    SequenceClaimFrontierRestore, SequenceClaims, SequenceDirectOwner, SequenceLedger,
    SequenceProductRangesRestore,
};

/// The four movable sequence positions two departing identities own, in the
/// frozen canonical order, starting one past the high watermark.
#[derive(Clone, Copy)]
pub(super) struct TwoExitClaims {
    own_exit: DeliverySeq,
    peer_exit: DeliverySeq,
    own_exit_product: DeliverySeq,
    peer_exit_product: DeliverySeq,
}

impl TwoExitClaims {
    /// Lays the four positions out above `high_watermark`.
    ///
    /// # Errors
    ///
    /// Returns a fixture diagnostic if the delivery-sequence domain is
    /// exhausted before all four positions are placed.
    pub(super) fn above(high_watermark: DeliverySeq) -> Result<Self, String> {
        let own_exit = high_watermark
            .checked_add(1)
            .ok_or_else(|| "fixture own exit claim overflow".to_string())?;
        let peer_exit = own_exit
            .checked_add(1)
            .ok_or_else(|| "fixture peer exit claim overflow".to_string())?;
        let own_exit_product = peer_exit
            .checked_add(1)
            .ok_or_else(|| "fixture own exit product overflow".to_string())?;
        let peer_exit_product = own_exit_product
            .checked_add(1)
            .ok_or_else(|| "fixture peer exit product overflow".to_string())?;
        Ok(Self {
            own_exit,
            peer_exit,
            own_exit_product,
            peer_exit_product,
        })
    }
}

/// The two ledgers a two-Detached-identity frontier owes: two live-member
/// sequence claims and two exit order positions, no terminals and no markers.
///
/// # Errors
///
/// Returns a fixture diagnostic if either ledger refuses the claim counts.
pub(super) fn two_identity_ledgers(
    high_watermark: DeliverySeq,
) -> Result<(SequenceLedger, OrderLedger), String> {
    let sequence = SequenceLedger::try_new(
        high_watermark,
        SequenceClaims::new(2, 0, 0, RecoverySequenceReserve::None),
    )
    .map_err(|error| format!("fixture sequence ledger refused: {error:?}"))?;
    let order = OrderLedger::try_new(
        OrderHigh::Empty,
        OrderClaims::new(0, 2, false, false)
            .map_err(|error| format!("fixture order claims refused: {error:?}"))?,
    )
    .map_err(|error| format!("fixture order ledger refused: {error:?}"))?;
    Ok((sequence, order))
}

/// The sequence-lane restore for the two identities' exits and exit products.
pub(super) fn two_identity_sequence_restore(
    claims: TwoExitClaims,
    participant_id: u64,
    peer_id: u64,
) -> SequenceClaimFrontierRestore {
    SequenceClaimFrontierRestore {
        movable_claims: vec![
            MovableSequenceClaim {
                delivery_seq: claims.own_exit,
                owner: SequenceDirectOwner::MembershipExit {
                    participant_index: participant_id,
                },
            },
            MovableSequenceClaim {
                delivery_seq: claims.peer_exit,
                owner: SequenceDirectOwner::MembershipExit {
                    participant_index: peer_id,
                },
            },
        ],
        immutable_candidates: vec![],
        products: SequenceProductRangesRestore {
            live_times_terminal: vec![],
            live_times_replacement_terminal: None,
            other_live_times_exit: vec![
                ExitProductRangeRestore {
                    start: claims.own_exit_product,
                    length: 1,
                    exit_participant: participant_id,
                },
                ExitProductRangeRestore {
                    start: claims.peer_exit_product,
                    length: 1,
                    exit_participant: peer_id,
                },
            ],
        },
        recovery: None,
    }
}

/// The order-lane restore for the two identities' exit majors.
pub(super) fn two_identity_order_restore(
    participant_id: u64,
    peer_id: u64,
) -> OrderClaimFrontierRestore {
    OrderClaimFrontierRestore {
        movable_claims: vec![
            MovableOrderClaim {
                transaction_order: 0,
                owner: OrderDirectOwner::MembershipExit {
                    participant_index: participant_id,
                },
            },
            MovableOrderClaim {
                transaction_order: 1,
                owner: OrderDirectOwner::MembershipExit {
                    participant_index: peer_id,
                },
            },
        ],
        immutable_candidates: vec![],
        recovery: None,
    }
}
