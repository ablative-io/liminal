#![allow(clippy::expect_used, clippy::panic)]

use alloc::{vec, vec::Vec};

use crate::algebra::{ResourceVector, WideResourceVector};
use crate::wire::{
    AttachAttemptToken, AttachSecret, BindingEpoch, ConnectionIncarnation, CredentialAttachRequest,
    DetachAttemptToken, DetachRequest, Generation,
};

use super::edge::marker_delivery_for_test;
use super::{
    ActiveBinding, AttachCommitError, AttachCommitParameters, AttachSecretProof, AttachTransition,
    AttachVerificationError, AttachedRecordPosition, BindingState, BindingTerminalDisposition,
    ClosureDebt, ClosureState, CommittedBindingTerminal, CommittedBindingTerminalPosition,
    CursorFateSuccessor, DebtCompletion, DetachCell, DetachedAttachRefusal, EnrollmentFingerprint,
    Event, LeaveOnlyEdge, LiveMember, LiveMemberRestore, ObserverProjection,
    PendingBindingTerminalPosition, StoredEdge, commit_attach, commit_detach, start_blocked_detach,
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

fn acceptance_member(
    conversation_id: u64,
    generation_value: u64,
    cursor: u64,
    latest_terminal: Option<CommittedBindingTerminal>,
) -> LiveMember<Vec<u8>> {
    let secret_byte =
        u8::try_from(generation_value).expect("acceptance generation fits one secret byte");
    LiveMember::restore(LiveMemberRestore {
        participant_id: 0,
        conversation_id,
        generation: generation(generation_value),
        attach_secret: AttachSecret::new([secret_byte; 32]),
        cursor,
        enrollment_fingerprint: EnrollmentFingerprint::new(vec![0xEF]),
        latest_terminal,
    })
    .expect("acceptance member is internally consistent")
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
fn acceptance_47_ordinary_attach_cursor_fate_and_leave_stays_crate_internal() {
    const CONVERSATION: u64 = 4_701;
    const MAX: u64 = u64::MAX;
    let floor = MAX - 7;
    let binding_epoch = BindingEpoch::new(ConnectionIncarnation::new(47, 0), generation(3));
    let binding = ActiveBinding {
        participant_id: 0,
        conversation_id: CONVERSATION,
        binding_epoch,
    };
    let verified = acceptance_member(CONVERSATION, 2, 0, None)
        .verify_detached_attach(
            BindingState::Detached,
            ClosureState::Clear
                .ordinary_detached_attach_admission()
                .expect("clear state admits ordinary attach"),
            CredentialAttachRequest {
                conversation_id: CONVERSATION,
                participant_id: 0,
                capability_generation: generation(2),
                attach_secret: AttachSecret::new([2; 32]),
                attach_attempt_token: AttachAttemptToken::new([0xA6; 16]),
                accept_marker_delivery_seq: None,
            },
            AttachSecretProof::Verified,
            AttachCommitParameters {
                binding,
                attach_secret: AttachSecret::new([3; 32]),
                attached_position: AttachedRecordPosition::new(1, 1),
                receipt_expires_at: 1_000,
                provenance_expires_at: 2_000,
            },
        )
        .expect("case 47 ordinary attach verifies");
    let committed = commit_attach(verified, DetachCell::<[u8; 32]>::default())
        .expect("case 47 ordinary attach commits");
    assert_eq!(committed.outcome.persisted_cursor(), 0);
    assert_eq!(committed.outcome.accepted_marker_delivery_seq(), None);

    let authority = committed
        .ordinary_binding_authority()
        .expect("ordinary commit retains crate-internal fate authority")
        .cursor_progressed(
            Event::cursor_progressed(0, binding_epoch, 0, MAX - 8, floor)
                .expect("case 47 cursor advances"),
        )
        .expect("exact ack preserves ordinary provenance");
    assert_eq!(authority.through_seq(), MAX - 8);
    let terminal = match binding.connection_lost(BindingTerminalDisposition::Committed(
        CommittedBindingTerminalPosition::new(MAX - 5, MAX - 2),
    )) {
        super::DiedBindingTransition::Committed(terminal) => terminal,
        super::DiedBindingTransition::Pending(_) => panic!("case 47 terminal is durable"),
    };
    let fate = authority
        .binding_fate(terminal, floor)
        .expect("exact terminal consumes ordinary provenance");
    assert_eq!(fate.through_seq(), MAX - 8);
    let closure_debt =
        ClosureDebt::new(WideResourceVector::new(1, 16)).expect("case 47 debt is nonzero");
    let ClosureState::Owed {
        edge: StoredEdge::DetachedCursorRelease(release),
        ..
    } = fate.into_direct_state(closure_debt)
    else {
        panic!("case 47 ordinary fate selects DCursor")
    };
    assert_eq!(release.last_dead_binding_epoch(), binding_epoch);
    let actual_charge = ResourceVector::new(1, 16);
    let claim = release
        .validate_leave_claim(0, actual_charge, ResourceVector::new(2, 32), 1)
        .expect("case 47 Left consumes one K record");
    assert_eq!(claim.actual_charge(), actual_charge);
    assert_eq!(
        release
            .leave(
                closure_debt,
                Event::detached_leave_committed(0, MAX - 1),
                claim,
                DebtCompletion::clear(),
            )
            .expect("DCursor's sole successor is exact detached Leave"),
        ClosureState::Clear
    );
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "the frozen case-51 transition and every postcondition remain one regression history"
)]
fn acceptance_51_ordinary_fate_preserves_op_then_installs_cursor_release() {
    const CONVERSATION: u64 = 51;
    const MAX: u64 = u64::MAX;
    let h = MAX - 7;
    let old_epoch = BindingEpoch::new(ConnectionIncarnation::new(51, 5), generation(5));
    let new_epoch = BindingEpoch::new(ConnectionIncarnation::new(51, 6), generation(6));
    let old_binding = ActiveBinding {
        participant_id: 0,
        conversation_id: CONVERSATION,
        binding_epoch: old_epoch,
    };
    let old_terminal = match old_binding.connection_lost(BindingTerminalDisposition::Committed(
        CommittedBindingTerminalPosition::new(h - 5, h),
    )) {
        super::DiedBindingTransition::Committed(terminal) => terminal,
        super::DiedBindingTransition::Pending(_) => panic!("case 51 old terminal is durable"),
    };
    let new_binding = ActiveBinding {
        participant_id: 0,
        conversation_id: CONVERSATION,
        binding_epoch: new_epoch,
    };
    let verified = acceptance_member(CONVERSATION, 5, h - 2, Some(old_terminal.into()))
        .verify_detached_attach(
            BindingState::Detached,
            ClosureState::Clear
                .ordinary_detached_attach_admission()
                .expect("clear state admits ordinary attach"),
            CredentialAttachRequest {
                conversation_id: CONVERSATION,
                participant_id: 0,
                capability_generation: generation(5),
                attach_secret: AttachSecret::new([5; 32]),
                attach_attempt_token: AttachAttemptToken::new([0x51; 16]),
                accept_marker_delivery_seq: None,
            },
            AttachSecretProof::Verified,
            AttachCommitParameters {
                binding: new_binding,
                attach_secret: AttachSecret::new([6; 32]),
                attached_position: AttachedRecordPosition::new(h - 4, h + 1),
                receipt_expires_at: 1_000,
                provenance_expires_at: 2_000,
            },
        )
        .expect("case 51 ordinary attach verifies");
    let committed = commit_attach(verified, DetachCell::<[u8; 32]>::default())
        .expect("case 51 ordinary attach commits");
    assert_eq!(committed.outcome.persisted_cursor(), h - 2);
    assert_eq!(committed.outcome.accepted_marker_delivery_seq(), None);

    let terminal = match new_binding.connection_lost(BindingTerminalDisposition::Committed(
        CommittedBindingTerminalPosition::new(h - 3, h + 2),
    )) {
        super::DiedBindingTransition::Committed(terminal) => terminal,
        super::DiedBindingTransition::Pending(_) => panic!("case 51 terminal is durable"),
    };
    let fate = committed
        .ordinary_binding_authority()
        .expect("ordinary commit retains crate-internal fate authority")
        .binding_fate(terminal, h - 1)
        .expect("exact case 51 terminal consumes ordinary provenance");
    assert_eq!(fate.through_seq(), h - 2);
    assert_eq!(fate.resulting_floor(), h - 1);

    let projection = ObserverProjection::new(h + 1);
    let closure_debt =
        ClosureDebt::new(WideResourceVector::new(2, 32)).expect("case 51 debt is nonzero");
    let pending = projection.apply_ordinary_binding_fate(closure_debt, fate);
    assert_eq!(
        pending.current_state(),
        ClosureState::Owed {
            debt: closure_debt,
            edge: StoredEdge::ObserverProjection(projection),
        }
    );
    let state = projection
        .complete_after_binding_fate(
            Event::projection_completed(h + 1),
            Some(closure_debt),
            pending,
        )
        .expect("exact OP completion installs the ordinary cursor suffix");
    let ClosureState::Owed {
        edge: StoredEdge::DetachedCursorRelease(release),
        ..
    } = state
    else {
        panic!("case 51 ordinary fate selects DCursor after OP")
    };
    assert_eq!(release.last_dead_binding_epoch(), new_epoch);
    assert_eq!(
        release.ordinary_attach_refusal(),
        DetachedAttachRefusal::RecoveryFence
    );
    assert_eq!(
        release.marker_attach_refusal(),
        DetachedAttachRefusal::MarkerMismatch
    );
    assert_eq!(
        release.binding_required_refusal(),
        DetachedAttachRefusal::NoBinding
    );
    let actual_charge = ResourceVector::new(1, 16);
    let claim = release
        .validate_leave_claim(0, actual_charge, ResourceVector::new(2, 32), 1)
        .expect("case 51 durable terminal leaves one K-backed Left charge");
    assert_eq!(claim.actual_charge(), actual_charge);
    assert_eq!(
        release
            .leave(
                closure_debt,
                Event::detached_leave_committed(0, h + 2),
                claim,
                DebtCompletion::clear(),
            )
            .expect("DCursor's sole participant successor is detached Leave"),
        ClosureState::Clear
    );
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
