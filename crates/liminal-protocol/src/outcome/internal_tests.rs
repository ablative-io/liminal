#![allow(clippy::panic, clippy::unwrap_used)]

use crate::lifecycle::AdmissionOrder;
use crate::wire::{BindingEpoch, CloseCause, ConnectionIncarnation, Generation, RepaymentEdge};

use super::internal::{
    BindingRecoveryCommitted, BindingRecoveryFinalization, CandidatePhase, ClaimCounter,
    ParticipantStateCorrupt, ParticipantStateCorruptReason, UncleanServerRestartCause,
};

#[test]
fn candidate_phase_values_match_canonical_order() {
    assert_eq!(CandidatePhase::BindingTerminal as u8, 0);
    assert_eq!(CandidatePhase::MembershipExit as u8, 1);
    assert_eq!(CandidatePhase::AttachLifecycle as u8, 2);
    assert_eq!(CandidatePhase::OrdinaryRecord as u8, 3);
    assert_eq!(CandidatePhase::CompactionMarker as u8, 4);
}

#[test]
fn binding_recovery_carries_only_unclean_restart_cause() -> Result<(), &'static str> {
    let generation = Generation::new(3).ok_or("nonzero fixture generation must be valid")?;
    let binding_epoch = BindingEpoch::new(ConnectionIncarnation::new(7, 11), generation);
    let cause = UncleanServerRestartCause {
        prior_server_incarnation: 7,
    };
    let outcome = BindingRecoveryCommitted {
        participant_id: 2,
        conversation_id: 5,
        recovered_binding_epoch: binding_epoch,
        cause,
        assigned_transaction_order: 13,
        finalization: BindingRecoveryFinalization::Pending {
            admission_order: AdmissionOrder::new(13, CandidatePhase::BindingTerminal, 2),
        },
        repayment_edge: RepaymentEdge::None,
    };

    assert_eq!(outcome.recovered_binding_epoch, binding_epoch);
    assert_eq!(
        cause.as_close_cause(),
        CloseCause::UncleanServerRestart {
            prior_server_incarnation: 7,
        }
    );
    Ok(())
}

#[test]
fn fix_two_corruption_taxonomy_keeps_non_occurrence_reasons() {
    let duplicate = ParticipantStateCorrupt {
        conversation_id: 5,
        reason: ParticipantStateCorruptReason::DuplicateCandidateKey {
            transaction_order: 16,
            candidate_phase: CandidatePhase::BindingTerminal,
            participant_index: 0,
        },
    };
    let frontier = ParticipantStateCorrupt {
        conversation_id: 5,
        reason: ParticipantStateCorruptReason::ClaimFrontierInvalid {
            counter: ClaimCounter::DeliverySeq,
            first_bad_position: 1,
        },
    };

    assert!(matches!(
        duplicate.reason,
        ParticipantStateCorruptReason::DuplicateCandidateKey { .. }
    ));
    assert!(matches!(
        frontier.reason,
        ParticipantStateCorruptReason::ClaimFrontierInvalid { .. }
    ));
}
