//! Membership-history and Leave derivation regressions.

#![allow(clippy::expect_used, clippy::panic)]

use alloc::{string::String, vec, vec::Vec};

use crate::outcome::CandidatePhase;
use crate::wire::{
    AttachSecret, BindingEpoch, ConnectionIncarnation, DetachAttemptToken, DetachRequest,
    Generation, LeaveAttemptToken, LeaveRequest,
};

use super::{
    ActiveBinding, AttachSecretProof, BindingState, BindingTerminalDisposition,
    CommittedBindingTerminal, CommittedBindingTerminalPosition, DetachCell, DiedBindingTransition,
    EnrollmentFingerprint, IdentityState, LeaveCommit, LeaveCommitError, LeaveCommitParameters,
    LeaveFingerprint, LiveMember, LiveMemberRestore, OrderClaims, PendingBindingTerminalPosition,
    PendingFinalization, PendingLeaveCommitParameters, RetainedCausalRecordKind, RetiredIdentity,
    SequenceClaims, commit_detach, commit_leave, commit_pending_leave,
    test_support::{pending_leave_authority, settled_leave_authority},
};

#[derive(Debug, PartialEq, Eq)]
struct NonCopyVerifier(Vec<u8>);

fn generation(value: u64) -> Generation {
    Generation::new(value).expect("test generations are nonzero")
}

fn epoch(connection_ordinal: u64, generation_value: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(70, connection_ordinal),
        generation(generation_value),
    )
}

fn active(connection_ordinal: u64) -> ActiveBinding {
    ActiveBinding {
        participant_id: 7,
        conversation_id: 11,
        binding_epoch: epoch(connection_ordinal, 3),
    }
}

fn member(latest_terminal: Option<CommittedBindingTerminal>) -> LiveMember<Vec<u8>> {
    LiveMember::restore(LiveMemberRestore {
        participant_id: 7,
        conversation_id: 11,
        generation: generation(3),
        attach_secret: AttachSecret::new([0xA3; 32]),
        cursor: 5,
        enrollment_fingerprint: EnrollmentFingerprint::new(vec![0xE1, 0xE2, 0xE3]),
        latest_terminal,
    })
    .expect("fixture history belongs to the member")
}

fn leave_request() -> LeaveRequest {
    LeaveRequest {
        conversation_id: 11,
        participant_id: 7,
        capability_generation: generation(3),
        attach_secret: AttachSecret::new([0xA3; 32]),
        leave_attempt_token: LeaveAttemptToken::new([0x51; 16]),
    }
}

fn verify_leave(
    member: &LiveMember<Vec<u8>>,
) -> super::VerifiedLeaveRequest<NonCopyVerifier, String> {
    member
        .verify_leave_request(
            &leave_request(),
            AttachSecretProof::Verified,
            NonCopyVerifier(vec![0xC1, 0xC2]),
            LeaveFingerprint::new(String::from("canonical-leave")),
        )
        .expect("fixture request has current authority")
}

fn assert_retired<EF, V, LF>(commit: LeaveCommit<EF, V, LF>) -> RetiredIdentity<EF, V, LF> {
    let (state, frontiers) = commit.into_parts();
    let IdentityState::Retired(retired) = &state else {
        panic!("Leave must return only a tombstone")
    };
    assert!(
        frontiers
            .active_identities()
            .participants()
            .iter()
            .all(|participant| participant.participant_index() != 7),
        "the atomic post-Leave frontier cannot retain the retired identity"
    );
    assert_eq!(
        frontiers.sequence().ledger().claims().live_members(),
        frontiers.active_identities().len()
    );
    assert_eq!(
        frontiers.order().ledger().claims().membership_exits(),
        frontiers.active_identities().len()
    );
    assert!(frontiers.cross_counter_valid_for_test());
    assert_eq!(
        frontiers.sequence().ledger().claims(),
        SequenceClaims::default()
    );
    assert_eq!(frontiers.order().ledger().claims(), OrderClaims::default());
    assert_eq!(
        frontiers.sequence().ledger().high_watermark(),
        retired.committed_result().left_delivery_seq()
    );
    assert!(matches!(
        frontiers
            .retained_records()
            .last()
            .map(|record| record.kind),
        Some(RetainedCausalRecordKind::MembershipExit {
            participant_index: 7
        })
    ));
    match state {
        IdentityState::Retired(retired) => retired,
        IdentityState::Live(_) => panic!("Leave must return only a tombstone"),
    }
}

#[test]
fn bound_leave_derives_active_epoch_and_moves_generic_fingerprints() {
    let member = member(None);
    let binding = active(1);
    let binding_state = BindingState::Bound(binding);
    let authority = settled_leave_authority(&member, binding_state, 10, 6);
    let verified = verify_leave(&member);
    let retired = assert_retired(
        commit_leave(
            member,
            binding_state,
            DetachCell::<[u8; 4]>::default(),
            verified,
            authority,
            LeaveCommitParameters {
                left_delivery_seq: 6,
            },
        )
        .expect("bound Leave derives its ended epoch"),
    );

    assert_eq!(
        retired.committed_result().ended_binding_epoch(),
        Some(binding.binding_epoch)
    );
    assert_eq!(
        retired.committed_result().prior_terminal_delivery_seq(),
        None
    );
    assert_eq!(
        retired.enrollment_fingerprint().value(),
        &vec![0xE1, 0xE2, 0xE3]
    );
    assert_eq!(retired.leave_fingerprint().value(), "canonical-leave");
    assert_eq!(retired.left_admission_order().transaction_order(), 10);
    assert_eq!(
        retired.left_admission_order().candidate_phase(),
        CandidatePhase::MembershipExit
    );
    assert_eq!(
        retired.leave_request_verifier(),
        &NonCopyVerifier(vec![0xC1, 0xC2])
    );
}

#[test]
fn committed_detach_cell_must_match_retained_terminal_and_derives_sequence() {
    let binding = active(2);
    let request = DetachRequest {
        conversation_id: 11,
        participant_id: 7,
        capability_generation: generation(3),
        detach_attempt_token: DetachAttemptToken::new([0xD1; 16]),
    };
    let verified_detach = binding
        .verify_detach_request(request, [0xD2; 8])
        .expect("detach authority matches");
    let (member, _, binding_state, committed_cell, _) = commit_detach(
        member(None),
        verified_detach,
        DetachCell::default(),
        CommittedBindingTerminalPosition::new(4, 8),
    )
    .expect("detach commits")
    .into_parts();
    let authority = settled_leave_authority(&member, binding_state, 9, 9);
    let verified_leave = verify_leave(&member);
    let retired = assert_retired(
        commit_leave(
            member,
            binding_state,
            DetachCell::Committed(committed_cell),
            verified_leave,
            authority,
            LeaveCommitParameters {
                left_delivery_seq: 9,
            },
        )
        .expect("cell and terminal agree"),
    );

    assert_eq!(retired.committed_result().ended_binding_epoch(), None);
    assert_eq!(
        retired.committed_result().prior_terminal_delivery_seq(),
        Some(8)
    );
}

#[test]
fn separately_committed_death_is_history_even_with_empty_detach_cell() {
    let transition = active(3).connection_lost(BindingTerminalDisposition::Committed(
        CommittedBindingTerminalPosition::new(5, 12),
    ));
    let DiedBindingTransition::Committed(terminal) = transition else {
        panic!("committed disposition must append the Died terminal");
    };
    let member = member(Some(terminal.into()));
    let authority = settled_leave_authority(&member, BindingState::Detached, 13, 13);
    let verified = verify_leave(&member);
    let retired = assert_retired(
        commit_leave(
            member,
            BindingState::Detached,
            DetachCell::<[u8; 1]>::default(),
            verified,
            authority,
            LeaveCommitParameters {
                left_delivery_seq: 13,
            },
        )
        .expect("death history is independent of the detach cell"),
    );

    assert_eq!(
        retired.committed_result().prior_terminal_delivery_seq(),
        Some(12)
    );
}

#[test]
fn terminalized_cell_uses_retained_history_because_fix_one_drops_sequence() {
    let binding = active(4);
    let request = DetachRequest {
        conversation_id: 11,
        participant_id: 7,
        capability_generation: generation(3),
        detach_attempt_token: DetachAttemptToken::new([0xD3; 16]),
    };
    let verified_detach = binding
        .verify_detach_request(request, [0xD4; 8])
        .expect("detach authority matches");
    let (member, _, binding_state, committed, _) = commit_detach(
        member(None),
        verified_detach,
        DetachCell::default(),
        CommittedBindingTerminalPosition::new(6, 14),
    )
    .expect("detach commits")
    .into_parts();
    let terminalized = committed.terminalize_after_attach();
    let authority = settled_leave_authority(&member, binding_state, 15, 15);
    let verified = verify_leave(&member);
    let retired = assert_retired(
        commit_leave(
            member,
            binding_state,
            DetachCell::Terminalized(terminalized),
            verified,
            authority,
            LeaveCommitParameters {
                left_delivery_seq: 15,
            },
        )
        .expect("Terminalized cell obtains prior sequence only from history"),
    );

    assert_eq!(
        retired.committed_result().prior_terminal_delivery_seq(),
        Some(14)
    );
}

#[test]
fn pending_terminal_requires_typed_no_intervening_composition() {
    let transition = active(5).connection_lost(BindingTerminalDisposition::Pending(
        PendingBindingTerminalPosition::new(7),
    ));
    let DiedBindingTransition::Pending(pending) = transition else {
        panic!("pending disposition must retain the Died terminal");
    };
    let pending = PendingFinalization::from(pending);
    let member = member(None);
    let rejected_authority = pending_leave_authority(&member, pending, 20, 8);
    let settled_verified = verify_leave(&member);
    assert!(matches!(
        commit_leave(
            member.clone(),
            BindingState::PendingFinalization(pending),
            DetachCell::<[u8; 1]>::default(),
            settled_verified,
            rejected_authority,
            LeaveCommitParameters {
                left_delivery_seq: 21,
            },
        ),
        Err(LeaveCommitError::PendingTerminalRequiresComposition)
    ));

    let authority = pending_leave_authority(&member, pending, 20, 8);
    let verified = verify_leave(&member);
    let retired = assert_retired(
        commit_pending_leave(
            member,
            pending,
            DetachCell::<[u8; 1]>::default(),
            verified,
            authority,
            PendingLeaveCommitParameters {
                terminal_delivery_seq: 20,
                left_delivery_seq: 21,
            },
        )
        .expect("typed positional composition commits terminal then Left"),
    );

    assert_eq!(retired.committed_result().ended_binding_epoch(), None);
    assert_eq!(
        retired.committed_result().prior_terminal_delivery_seq(),
        Some(20)
    );
    assert_eq!(retired.committed_result().left_delivery_seq(), 21);
}

#[test]
fn empty_cell_and_no_history_derives_none_without_caller_option() {
    let member = member(None);
    let authority = settled_leave_authority(&member, BindingState::Detached, 3, 6);
    let verified = verify_leave(&member);
    let retired = assert_retired(
        commit_leave(
            member,
            BindingState::Detached,
            DetachCell::<[u8; 1]>::default(),
            verified,
            authority,
            LeaveCommitParameters {
                left_delivery_seq: 6,
            },
        )
        .expect("empty detached history has no prior terminal"),
    );
    assert_eq!(
        retired.committed_result().prior_terminal_delivery_seq(),
        None
    );
}
