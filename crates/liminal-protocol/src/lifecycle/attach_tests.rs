#![allow(clippy::expect_used, clippy::panic)]

use alloc::{vec, vec::Vec};

use crate::algebra::WideResourceVector;
use crate::wire::{
    AttachAttemptToken, AttachSecret, BindingEpoch, ConnectionIncarnation, CredentialAttachRequest,
    DetachAttemptToken, DetachRequest, Generation,
};

use super::edge::marker_delivery_for_test;
use super::{
    ActiveBinding, AttachCommitError, AttachCommitParameters, AttachSecretProof, AttachTransition,
    AttachVerificationError, AttachedRecordPosition, BindingState, ClosureDebt, ClosureState,
    CommittedBindingTerminalPosition, CursorFateSuccessor, DebtCompletion, DetachCell,
    EnrollmentFingerprint, Event, LiveMember, LiveMemberRestore, PendingBindingTerminalPosition,
    StoredEdge, commit_attach, commit_detach, start_blocked_detach,
};

fn generation(value: u64) -> Generation {
    Generation::new(value).expect("test generation is nonzero")
}

fn epoch(generation_value: u64, ordinal: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(8, ordinal),
        generation(generation_value),
    )
}

fn member() -> LiveMember<Vec<u8>> {
    LiveMember::restore(LiveMemberRestore {
        participant_id: 3,
        conversation_id: 29,
        generation: generation(4),
        attach_secret: AttachSecret::new([0x44; 32]),
        cursor: 5,
        enrollment_fingerprint: EnrollmentFingerprint::new(vec![3, 29]),
        latest_terminal: None,
    })
    .expect("fixture membership is internally consistent")
}

fn request(marker: Option<u64>) -> CredentialAttachRequest {
    CredentialAttachRequest {
        conversation_id: 29,
        participant_id: 3,
        capability_generation: generation(4),
        attach_secret: AttachSecret::new([0x44; 32]),
        attach_attempt_token: AttachAttemptToken::new([0xA4; 16]),
        accept_marker_delivery_seq: marker,
    }
}

fn parameters() -> AttachCommitParameters {
    AttachCommitParameters {
        binding: ActiveBinding {
            participant_id: 3,
            conversation_id: 29,
            binding_epoch: epoch(5, 12),
        },
        attach_secret: AttachSecret::new([0x55; 32]),
        attached_position: AttachedRecordPosition::new(9, 15),
        receipt_expires_at: 100,
        provenance_expires_at: 200,
    }
}

fn debt() -> ClosureDebt {
    ClosureDebt::new(WideResourceVector::new(1, 10)).expect("test debt is nonzero")
}

#[test]
fn ordinary_attach_cannot_claim_marker_acceptance() {
    assert!(matches!(
        member().verify_detached_attach(
            BindingState::Detached,
            ClosureState::Clear
                .ordinary_detached_attach_admission()
                .expect("clear state admits ordinary attach"),
            request(Some(14)),
            AttachSecretProof::Verified,
            parameters(),
        ),
        Err(AttachVerificationError::MarkerProof)
    ));
}

#[test]
fn clear_closure_admission_reaches_ordinary_binding_authority() {
    let verified = member()
        .verify_detached_attach(
            BindingState::Detached,
            ClosureState::Clear
                .ordinary_detached_attach_admission()
                .expect("clear state admits ordinary attach"),
            request(None),
            AttachSecretProof::Verified,
            parameters(),
        )
        .expect("clear ordinary detached attach verifies");
    let committed = commit_attach(verified, DetachCell::<[u8; 32]>::default())
        .expect("verified ordinary detached attach commits");

    let authority = committed
        .ordinary_binding_authority()
        .expect("ordinary attach carries its admitted binding authority");
    assert_eq!(authority.binding(), parameters().binding);
    assert_eq!(authority.through_seq(), member().cursor());
    assert!(committed.binding_origin().is_unfenced());
    assert_eq!(committed.binding_origin().attached(), committed.attached);
}

#[test]
fn active_same_participant_attach_records_supersession() {
    let old_binding = ActiveBinding {
        participant_id: 3,
        conversation_id: 29,
        binding_epoch: epoch(4, 11),
    };
    let verified = member()
        .verify_superseding_attach(
            old_binding,
            request(None),
            AttachSecretProof::Verified,
            CommittedBindingTerminalPosition::new(9, 14),
            parameters(),
        )
        .expect("same-participant current binding may be superseded");
    let committed = commit_attach(verified, DetachCell::<[u8; 32]>::default())
        .expect("verified supersession commits");

    assert_eq!(committed.member.cursor(), 5);
    assert_eq!(committed.outcome.persisted_cursor(), 5);
    assert_eq!(committed.outcome.accepted_marker_delivery_seq(), None);
    let ordinary_authority = committed
        .ordinary_binding_authority()
        .expect("ordinary supersession mints no-marker fate authority");
    assert_eq!(ordinary_authority.binding(), parameters().binding);
    assert_eq!(ordinary_authority.through_seq(), 5);
    assert!(committed.binding_origin().is_unfenced());
    let AttachTransition::Superseded { terminal } = committed.transition else {
        panic!("supersession must carry its real old terminal");
    };
    assert_eq!(terminal.binding_epoch(), old_binding.binding_epoch);
    assert_eq!(terminal.delivery_seq(), 14);
    assert_eq!(committed.attached.delivery_seq(), 15);
    assert_eq!(committed.member.latest_terminal(), Some(terminal.into()));
}

#[test]
fn fenced_recovery_composes_pending_detach_and_sets_both_marker_fields() {
    let old_binding = ActiveBinding {
        participant_id: 3,
        conversation_id: 29,
        binding_epoch: epoch(4, 11),
    };
    let detach_request = DetachRequest {
        conversation_id: 29,
        participant_id: 3,
        capability_generation: generation(4),
        detach_attempt_token: DetachAttemptToken::new([0xD4; 16]),
    };
    let verified_detach = old_binding
        .verify_detach_request(detach_request, [0xDD; 32])
        .expect("detach request matches old binding");
    let (pending_member, pending_state, pending_cell, _) = start_blocked_detach(
        member(),
        verified_detach,
        DetachCell::default(),
        PendingBindingTerminalPosition::new(7),
        6,
    )
    .expect("blocked detach creates one paired pending cell")
    .into_parts();

    let closure_debt = debt();
    let delivery = marker_delivery_for_test(3, old_binding.binding_epoch, 14)
        .expect("validated marker fixture restores");
    let delivered = delivery
        .delivered(
            closure_debt,
            Event::marker_delivered(3, old_binding.binding_epoch, 14),
        )
        .expect("exact delivery creates marker-backed cursor progress");
    let ClosureState::Owed {
        edge: StoredEdge::ParticipantCursorProgress(progress),
        ..
    } = delivered
    else {
        panic!("marker delivery must select participant cursor progress");
    };
    let fate = progress
        .binding_fate(
            closure_debt,
            Event::binding_fate_observed(3, old_binding.binding_epoch, 5),
        )
        .expect("exact old-epoch fate derives recovery");
    let CursorFateSuccessor::DetachedCredentialRecovery(recovery) = fate else {
        panic!("marker-backed fate must derive credential recovery");
    };
    let proof = recovery
        .fenced_attach(
            closure_debt,
            Event::fenced_recovery_committed(
                3,
                14,
                old_binding.binding_epoch,
                parameters().binding.binding_epoch,
                15,
            ),
            DebtCompletion::clear(),
        )
        .expect("exact fenced event consumes recovery edge");

    let verified = pending_member
        .verify_fenced_attach(
            pending_state,
            request(Some(14)),
            AttachSecretProof::Verified,
            &proof,
            Some(13),
            parameters(),
        )
        .expect("proof and pending finalization name one old epoch");
    let committed = commit_attach(verified, DetachCell::Pending(pending_cell))
        .expect("fenced attach composes the pending terminal");

    assert_eq!(committed.member.cursor(), 14);
    assert_eq!(committed.outcome.persisted_cursor(), 14);
    assert_eq!(committed.outcome.accepted_marker_delivery_seq(), Some(14));
    assert_eq!(committed.ordinary_binding_authority(), None);
    assert!(!committed.binding_origin().is_unfenced());
    assert_eq!(
        committed.binding_origin().recovered_marker(),
        Some((14, old_binding.binding_epoch))
    );
    assert!(matches!(committed.detach_cell, DetachCell::Terminalized(_)));
    assert_eq!(
        committed.transition,
        AttachTransition::FencedRecovery {
            prior_binding_epoch: old_binding.binding_epoch,
            composed_terminal: committed.member.latest_terminal(),
            next_closure_state: ClosureState::Clear,
        }
    );
}

#[test]
fn supersession_cannot_coexist_with_a_committed_detach_cell() {
    let old_binding = ActiveBinding {
        participant_id: 3,
        conversation_id: 29,
        binding_epoch: epoch(4, 11),
    };
    let detach_request = DetachRequest {
        conversation_id: 29,
        participant_id: 3,
        capability_generation: generation(4),
        detach_attempt_token: DetachAttemptToken::new([0xD5; 16]),
    };
    let verified_detach = old_binding
        .verify_detach_request(detach_request, [0xEE; 32])
        .expect("detach request matches old binding");
    let (detached_member, _, _, committed_cell, _) = commit_detach(
        member(),
        verified_detach,
        DetachCell::default(),
        CommittedBindingTerminalPosition::new(8, 9),
    )
    .expect("test detach commits")
    .into_parts();
    let verified = detached_member
        .verify_superseding_attach(
            old_binding,
            request(None),
            AttachSecretProof::Verified,
            CommittedBindingTerminalPosition::new(9, 14),
            parameters(),
        )
        .expect("supersession proof is independently valid");

    assert_eq!(
        commit_attach(verified, DetachCell::Committed(committed_cell)),
        Err(AttachCommitError::DetachCellAuthority)
    );
}
