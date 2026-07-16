#![allow(clippy::expect_used)]

use crate::wire::{
    AttachAttemptToken, AttachBound, AttachSecret, BindingEpoch, BindingStateView,
    ConnectionIncarnation, CredentialAttachRequest, DetachAttemptToken, DetachRequest, EnrollBound,
    EnrollmentRequest, EnrollmentToken, Generation, LeaveAttemptToken, LeaveRequest, MarkerAck,
    ParticipantAck, ReceiptExpiryReason, ReceiptReplay, RecordAdmission,
};

use super::lookup::{
    AttachSecretProof, BindingRequiredLookupResult, CredentialAttachLiveReceipt,
    CredentialAttachLookupResult, CredentialAttachProvenance, CredentialAttachTokenPhase,
    DetachLookupContext, DetachLookupResult, DetachTokenResolution, EnrollmentLiveReceipt,
    EnrollmentLookupResult, EnrollmentProvenance, EnrollmentTokenPhase, LeaveLookupResult,
    LeaveSecretProof, ParticipantBindingRequest, PresentedIdentity, ResolvedIdentity,
    lookup_binding_required, lookup_credential_attach, lookup_detach, lookup_enrollment,
    lookup_leave,
};
use super::{
    ActiveBinding, AttachCommitParameters, AttachedRecordPosition, BindingState, ClosureState,
    CommittedBindingTerminalPosition, DetachCell, EnrollmentFingerprint, IdentityState,
    LeaveCommitParameters, LeaveFingerprint, LiveMember, LiveMemberRestore,
    PendingBindingTerminalPosition, commit_attach, commit_detach, commit_leave,
    start_blocked_detach, test_support::settled_leave_authority,
};

type TestFingerprint = [u8; 32];
type TestVerifier = [u8; 32];
type TestMember = LiveMember<TestFingerprint>;
type TestIdentity = IdentityState<TestFingerprint, TestVerifier, TestFingerprint>;

fn generation(value: u64) -> Generation {
    Generation::new(value).expect("test generation is nonzero")
}

fn epoch(generation_value: u64, ordinal: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(9, ordinal),
        generation(generation_value),
    )
}

fn member(generation_value: u64) -> TestMember {
    member_for(7, generation_value)
}

fn member_for(participant_id: u64, generation_value: u64) -> TestMember {
    let secret_byte = u8::try_from(generation_value).expect("test generation fits one byte");
    LiveMember::restore(LiveMemberRestore {
        participant_id,
        conversation_id: 11,
        generation: generation(generation_value),
        attach_secret: AttachSecret::new([secret_byte; 32]),
        cursor: 0,
        enrollment_fingerprint: EnrollmentFingerprint::new([1; 32]),
        latest_terminal: None,
    })
    .expect("test member has no inconsistent terminal history")
}

fn detach_request(generation_value: u64, token_byte: u8) -> DetachRequest {
    DetachRequest {
        conversation_id: 11,
        participant_id: 7,
        capability_generation: generation(generation_value),
        detach_attempt_token: DetachAttemptToken::new([token_byte; 16]),
    }
}

fn committed_detach() -> (
    TestMember,
    crate::lifecycle::CommittedDetach<[u8; 32]>,
    DetachRequest,
    [u8; 32],
) {
    let request = detach_request(3, 0xD3);
    let verifier = [0xA5; 32];
    let binding = ActiveBinding {
        participant_id: 7,
        conversation_id: 11,
        binding_epoch: epoch(3, 1),
    };
    let verified_request = binding
        .verify_detach_request(request.clone(), verifier)
        .expect("request matches binding");
    let transition = commit_detach(
        member(3),
        verified_request,
        DetachCell::default(),
        CommittedBindingTerminalPosition::new(1, 21),
    )
    .expect("empty cell accepts detach");
    let (member, _, _, cell, _) = transition.into_parts();
    (member, cell, request, verifier)
}

fn terminalized_detach() -> (
    TestIdentity,
    DetachCell<TestVerifier>,
    BindingState,
    DetachRequest,
    TestVerifier,
) {
    let (old_member, committed, request, verifier) = committed_detach();
    let attach_request = CredentialAttachRequest {
        conversation_id: 11,
        participant_id: 7,
        capability_generation: generation(3),
        attach_secret: old_member.attach_secret(),
        attach_attempt_token: AttachAttemptToken::new([0xA4; 16]),
        accept_marker_delivery_seq: None,
    };
    let active_binding = ActiveBinding {
        participant_id: 7,
        conversation_id: 11,
        binding_epoch: epoch(4, 2),
    };
    let verified_attach = old_member
        .verify_detached_attach(
            BindingState::Detached,
            ClosureState::Clear
                .ordinary_detached_attach_admission()
                .expect("clear state admits ordinary attach"),
            attach_request,
            AttachSecretProof::Verified,
            AttachCommitParameters {
                binding: active_binding,
                attach_secret: AttachSecret::new([4; 32]),
                attached_position: AttachedRecordPosition::new(2, 22),
                receipt_expires_at: 100,
                provenance_expires_at: 200,
            },
        )
        .expect("detached attach authority is current");
    let attach = commit_attach(verified_attach, DetachCell::Committed(committed))
        .expect("verified attach terminalizes old detach cell");
    let terminalized = attach
        .detach_cell
        .into_terminalized()
        .expect("attach must produce the terminalized variant");
    (
        IdentityState::Live(attach.member),
        DetachCell::Terminalized(terminalized),
        attach.binding_state,
        request,
        verifier,
    )
}

fn tombstone() -> TestIdentity {
    retire(member(3), LeaveAttemptToken::new([0x51; 16]))
}

fn retire(live: TestMember, leave_token: LeaveAttemptToken) -> TestIdentity {
    let authority = settled_leave_authority(&live, BindingState::Detached, 22);
    let request = LeaveRequest {
        conversation_id: 11,
        participant_id: live.participant_id(),
        capability_generation: live.generation(),
        attach_secret: live.attach_secret(),
        leave_attempt_token: leave_token,
    };
    let verified = live
        .verify_leave_request(
            &request,
            AttachSecretProof::Verified,
            [2; 32],
            LeaveFingerprint::new([3; 32]),
        )
        .expect("request describes member");
    commit_leave(
        live,
        BindingState::Detached,
        DetachCell::<TestVerifier>::default(),
        verified,
        authority,
        LeaveCommitParameters {
            left_delivery_seq: 22,
        },
    )
    .expect("typed Leave commit produces tombstone")
}

#[test]
fn detach_tombstone_precedes_exact_committed_cell() {
    let (_, committed, request, verifier) = committed_detach();
    let identity = tombstone();
    let cell = DetachCell::Committed(committed);
    let result = lookup_detach(&DetachLookupContext {
        token_resolution: DetachTokenResolution::Exact(ResolvedIdentity::from(&identity)),
        presented_identity: PresentedIdentity::from(Some(&identity)),
        cell: &cell,
        binding: &BindingState::Detached,
        receiving_binding_epoch: None,
        request: &request,
        request_verifier: verifier,
        observer_progress: 0,
    });
    assert!(matches!(result, DetachLookupResult::Retired(_)));
}

#[test]
fn absent_identity_precedes_impossible_cell_bytes() {
    let (_, committed, request, verifier) = committed_detach();
    let cell = DetachCell::Committed(committed);
    let result =
        lookup_detach(
            &DetachLookupContext {
                token_resolution: DetachTokenResolution::<
                    TestFingerprint,
                    TestVerifier,
                    TestFingerprint,
                >::NoExactMatch,
                presented_identity: PresentedIdentity::Absent,
                cell: &cell,
                binding: &BindingState::Detached,
                receiving_binding_epoch: None,
                request: &request,
                request_verifier: verifier,
                observer_progress: 0,
            },
        );
    assert!(matches!(result, DetachLookupResult::ParticipantUnknown(_)));
}

#[test]
fn terminalized_exact_token_precedes_current_generation_and_binding() {
    let (live, cell, binding_state, request, verifier) = terminalized_detach();
    let result = lookup_detach(&DetachLookupContext {
        token_resolution: DetachTokenResolution::Exact(ResolvedIdentity::from(&live)),
        presented_identity: PresentedIdentity::from(Some(&live)),
        cell: &cell,
        binding: &binding_state,
        receiving_binding_epoch: None,
        request: &request,
        request_verifier: verifier,
        observer_progress: 0,
    });
    assert!(matches!(
        result,
        DetachLookupResult::StaleAuthority(
            crate::wire::DetachStaleAuthority::TerminalizedDetachCell(_)
        )
    ));
}

#[test]
fn terminalized_exact_token_rejects_request_conversation_mismatch() {
    let (live, cell, binding_state, mut request, verifier) = terminalized_detach();
    request.conversation_id = 12;
    let result = lookup_detach(&DetachLookupContext {
        token_resolution: DetachTokenResolution::Exact(ResolvedIdentity::from(&live)),
        presented_identity: PresentedIdentity::Absent,
        cell: &cell,
        binding: &binding_state,
        receiving_binding_epoch: None,
        request: &request,
        request_verifier: verifier,
        observer_progress: 0,
    });
    assert!(matches!(result, DetachLookupResult::ParticipantUnknown(_)));
}

#[test]
fn terminalized_replay_does_not_reflect_another_identity_binding() {
    let (live, cell, _, request, verifier) = terminalized_detach();
    let mismatched_binding = BindingState::Bound(ActiveBinding {
        participant_id: 99,
        conversation_id: 11,
        binding_epoch: epoch(4, 2),
    });
    let result = lookup_detach(&DetachLookupContext {
        token_resolution: DetachTokenResolution::Exact(ResolvedIdentity::from(&live)),
        presented_identity: PresentedIdentity::from(Some(&live)),
        cell: &cell,
        binding: &mismatched_binding,
        receiving_binding_epoch: None,
        request: &request,
        request_verifier: verifier,
        observer_progress: 0,
    });
    assert!(matches!(
        result,
        DetachLookupResult::StaleAuthority(
            crate::wire::DetachStaleAuthority::TerminalizedDetachCell(value)
        ) if value.binding_state() == BindingStateView::Detached
    ));
}

#[test]
fn detach_ordinary_order_is_stale_then_no_binding_then_authorized() {
    let live: TestIdentity = IdentityState::Live(member(4));
    let stale = detach_request(3, 1);
    let current = detach_request(4, 2);
    let verifier = [9; 32];

    assert!(matches!(
        lookup_detach(&DetachLookupContext {
            token_resolution: DetachTokenResolution::NoExactMatch,
            presented_identity: PresentedIdentity::from(Some(&live)),
            cell: &DetachCell::default(),
            binding: &BindingState::Detached,
            receiving_binding_epoch: None,
            request: &stale,
            request_verifier: verifier,
            observer_progress: 0,
        }),
        DetachLookupResult::StaleAuthority(_)
    ));
    assert!(matches!(
        lookup_detach(&DetachLookupContext {
            token_resolution: DetachTokenResolution::NoExactMatch,
            presented_identity: PresentedIdentity::from(Some(&live)),
            cell: &DetachCell::default(),
            binding: &BindingState::Detached,
            receiving_binding_epoch: None,
            request: &current,
            request_verifier: verifier,
            observer_progress: 0,
        }),
        DetachLookupResult::NoBinding(_)
    ));

    let binding = BindingState::Bound(ActiveBinding {
        participant_id: 7,
        conversation_id: 11,
        binding_epoch: epoch(4, 3),
    });
    assert!(matches!(
        lookup_detach(&DetachLookupContext {
            token_resolution: DetachTokenResolution::NoExactMatch,
            presented_identity: PresentedIdentity::from(Some(&live)),
            cell: &DetachCell::default(),
            binding: &binding,
            receiving_binding_epoch: Some(epoch(4, 99)),
            request: &current,
            request_verifier: verifier,
            observer_progress: 0,
        }),
        DetachLookupResult::NoBinding(_)
    ));
    assert!(matches!(
        lookup_detach(&DetachLookupContext {
            token_resolution: DetachTokenResolution::NoExactMatch,
            presented_identity: PresentedIdentity::from(Some(&live)),
            cell: &DetachCell::default(),
            binding: &binding,
            receiving_binding_epoch: Some(epoch(4, 3)),
            request: &current,
            request_verifier: verifier,
            observer_progress: 0,
        }),
        DetachLookupResult::Authorized { .. }
    ));
}

#[test]
fn committed_leave_token_exception_has_fixed_internal_order() {
    let identity = tombstone();
    let request = LeaveRequest {
        conversation_id: 11,
        participant_id: 7,
        capability_generation: generation(3),
        attach_secret: AttachSecret::new([3; 32]),
        leave_attempt_token: LeaveAttemptToken::new([0x51; 16]),
    };

    let wrong_secret_and_generation = LeaveRequest {
        capability_generation: generation(4),
        ..request
    };
    assert!(matches!(
        lookup_leave(
            PresentedIdentity::from(Some(&identity)),
            &BindingState::Detached,
            None,
            &wrong_secret_and_generation,
            LeaveSecretProof::Mismatch,
        ),
        LeaveLookupResult::StaleAuthority(
            crate::wire::LeaveStaleAuthority::CommittedLeaveTombstone { .. }
        )
    ));

    assert!(matches!(
        lookup_leave(
            PresentedIdentity::from(Some(&identity)),
            &BindingState::Detached,
            None,
            &wrong_secret_and_generation,
            LeaveSecretProof::Verified,
        ),
        LeaveLookupResult::AttemptTokenBodyConflict(_)
    ));

    assert!(matches!(
        lookup_leave(
            PresentedIdentity::from(Some(&identity)),
            &BindingState::Detached,
            None,
            &request,
            LeaveSecretProof::Verified,
        ),
        LeaveLookupResult::LeaveCommitted(_)
    ));
}

#[test]
fn different_leave_token_gets_retired_and_detached_live_leave_is_authorized() {
    let retired = tombstone();
    let different_token = LeaveRequest {
        conversation_id: 11,
        participant_id: 7,
        capability_generation: generation(3),
        attach_secret: AttachSecret::new([3; 32]),
        leave_attempt_token: LeaveAttemptToken::new([0x99; 16]),
    };
    assert!(matches!(
        lookup_leave(
            PresentedIdentity::from(Some(&retired)),
            &BindingState::Detached,
            None,
            &different_token,
            LeaveSecretProof::Verified,
        ),
        LeaveLookupResult::Retired(_)
    ));

    let live: TestIdentity = IdentityState::Live(member(3));
    assert!(matches!(
        lookup_leave(
            PresentedIdentity::from(Some(&live)),
            &BindingState::Detached,
            None,
            &different_token,
            LeaveSecretProof::Verified,
        ),
        LeaveLookupResult::AuthorizedDetached { .. }
    ));
}

fn enrollment_request(token_byte: u8) -> EnrollmentRequest {
    EnrollmentRequest {
        conversation_id: 11,
        enrollment_token: EnrollmentToken::new([token_byte; 16]),
    }
}

fn attach_request(
    generation_value: u64,
    token_byte: u8,
    marker: Option<u64>,
) -> CredentialAttachRequest {
    CredentialAttachRequest {
        conversation_id: 11,
        participant_id: 7,
        capability_generation: generation(generation_value),
        attach_secret: AttachSecret::new([0xE1; 32]),
        attach_attempt_token: AttachAttemptToken::new([token_byte; 16]),
        accept_marker_delivery_seq: marker,
    }
}

fn attach_commit() -> AttachBound {
    AttachBound::fenced(
        11,
        AttachAttemptToken::new([0xA7; 16]),
        7,
        generation(3),
        AttachSecret::new([4; 32]),
        epoch(4, 7),
        9,
        1_000,
        2_000,
    )
    .expect("test receipt has exact successor generation and marker cursor")
}

#[test]
fn enrollment_mapping_resolves_live_and_tombstoned_identities_before_status() {
    let request = enrollment_request(0xE0);
    let live: TestIdentity = IdentityState::Live(member(4));
    let known = lookup_enrollment(
        EnrollmentTokenPhase::LifetimeMapping {
            identity: ResolvedIdentity::from(&live),
        },
        &BindingState::Detached,
        &request,
    );
    assert_eq!(
        known,
        EnrollmentLookupResult::EnrollmentKnown(crate::wire::EnrollmentKnown {
            conversation_id: 11,
            token: request.enrollment_token,
            participant_id: 7,
            current_generation: generation(4),
        })
    );

    let other_tombstone = retire(member_for(8, 3), LeaveAttemptToken::new([0x58; 16]));
    let retired = lookup_enrollment(
        EnrollmentTokenPhase::LifetimeMapping {
            identity: ResolvedIdentity::from(&other_tombstone),
        },
        &BindingState::Detached,
        &request,
    );
    assert!(matches!(
        retired,
        EnrollmentLookupResult::Retired(crate::wire::Retired::Enrollment {
            participant_id: 8,
            retired_generation,
            ..
        }) if retired_generation == generation(3)
    ));
}

#[test]
fn enrollment_receipt_and_provenance_are_phase_specific() {
    let request = enrollment_request(0xE2);
    let live: TestIdentity = IdentityState::Live(member(1));
    let committed = EnrollBound::new(
        11,
        request.enrollment_token,
        7,
        AttachSecret::new([1; 32]),
        epoch(1, 2),
        100,
        200,
    )
    .expect("generation-one enrollment receipt");
    let receipt = EnrollmentLiveReceipt::from_commit(committed);
    let exact_binding = BindingState::Bound(ActiveBinding {
        participant_id: 7,
        conversation_id: 11,
        binding_epoch: epoch(1, 2),
    });
    assert!(matches!(
        lookup_enrollment(
            EnrollmentTokenPhase::LiveReceipt {
                identity: ResolvedIdentity::from(&live),
                receipt: &receipt,
            },
            &exact_binding,
            &request,
        ),
        EnrollmentLookupResult::Bound(ReceiptReplay::Enrollment(_))
    ));
    assert!(matches!(
        lookup_enrollment(
            EnrollmentTokenPhase::LiveReceipt {
                identity: ResolvedIdentity::from(&live),
                receipt: &receipt,
            },
            &BindingState::Detached,
            &request,
        ),
        EnrollmentLookupResult::UnboundReceipt(ReceiptReplay::Enrollment(_))
    ));

    assert!(matches!(
        lookup_enrollment(
            EnrollmentTokenPhase::Provenance {
                identity: ResolvedIdentity::from(&live),
                provenance: EnrollmentProvenance::new(
                    generation(1),
                    ReceiptExpiryReason::Deadline,
                ),
            },
            &BindingState::Detached,
            &request,
        ),
        EnrollmentLookupResult::ReceiptExpired(crate::wire::ReceiptExpired::Enrollment {
            result_generation,
            current_generation,
            reason: ReceiptExpiryReason::Deadline,
            ..
        }) if result_generation == generation(1) && current_generation == generation(1)
    ));
}

#[test]
fn live_attach_receipt_checks_secret_then_generation_then_marker() {
    let committed = attach_commit();
    let receipt = CredentialAttachLiveReceipt::from_commit(committed.clone());
    let live: TestIdentity = IdentityState::Live(member(4));
    let binding = BindingState::Bound(ActiveBinding {
        participant_id: 7,
        conversation_id: 11,
        binding_epoch: committed.origin_binding_epoch(),
    });

    let generation_and_marker_conflict = attach_request(2, 0xA7, Some(10));
    assert!(matches!(
        lookup_credential_attach(
            CredentialAttachTokenPhase::LiveReceipt {
                identity: ResolvedIdentity::from(&live),
                receipt: &receipt,
            },
            PresentedIdentity::Absent,
            &binding,
            &generation_and_marker_conflict,
            AttachSecretProof::Mismatch,
        ),
        CredentialAttachLookupResult::StaleAuthority(_)
    ));
    assert!(matches!(
        lookup_credential_attach(
            CredentialAttachTokenPhase::LiveReceipt {
                identity: ResolvedIdentity::from(&live),
                receipt: &receipt,
            },
            PresentedIdentity::Absent,
            &binding,
            &generation_and_marker_conflict,
            AttachSecretProof::Verified,
        ),
        CredentialAttachLookupResult::AttemptTokenBodyConflict(
            crate::wire::AttemptTokenBodyConflict::CredentialAttach {
                conflict: crate::wire::AttemptConflict::Generation,
                ..
            }
        )
    ));

    let marker_conflict = attach_request(3, 0xA7, None);
    assert!(matches!(
        lookup_credential_attach(
            CredentialAttachTokenPhase::LiveReceipt {
                identity: ResolvedIdentity::from(&live),
                receipt: &receipt,
            },
            PresentedIdentity::Absent,
            &binding,
            &marker_conflict,
            AttachSecretProof::Verified,
        ),
        CredentialAttachLookupResult::AttemptTokenBodyConflict(
            crate::wire::AttemptTokenBodyConflict::CredentialAttach {
                conflict: crate::wire::AttemptConflict::MarkerDeliverySequence,
                ..
            }
        )
    ));
}

#[test]
fn exact_attach_receipt_reports_current_bound_or_unbound_status() {
    let committed = attach_commit();
    let receipt = CredentialAttachLiveReceipt::from_commit(committed.clone());
    let request = attach_request(3, 0xA7, Some(9));
    let live: TestIdentity = IdentityState::Live(member(4));
    let exact = BindingState::Bound(ActiveBinding {
        participant_id: 7,
        conversation_id: 11,
        binding_epoch: committed.origin_binding_epoch(),
    });
    let phase = || CredentialAttachTokenPhase::LiveReceipt {
        identity: ResolvedIdentity::from(&live),
        receipt: &receipt,
    };

    assert!(matches!(
        lookup_credential_attach(
            phase(),
            PresentedIdentity::Absent,
            &exact,
            &request,
            AttachSecretProof::Verified,
        ),
        CredentialAttachLookupResult::Bound(ReceiptReplay::CredentialAttach(_))
    ));
    let later = BindingState::Bound(ActiveBinding {
        participant_id: 7,
        conversation_id: 11,
        binding_epoch: epoch(4, 8),
    });
    assert!(matches!(
        lookup_credential_attach(
            phase(),
            PresentedIdentity::Absent,
            &later,
            &request,
            AttachSecretProof::Verified,
        ),
        CredentialAttachLookupResult::UnboundReceipt(ReceiptReplay::CredentialAttach(_))
    ));
}

#[test]
fn attach_token_tombstone_and_provenance_precede_presented_authority() {
    let request = attach_request(1, 0xA7, None);
    let live: TestIdentity = IdentityState::Live(member(5));
    let other_tombstone = retire(member_for(8, 3), LeaveAttemptToken::new([0x68; 16]));
    let receipt = CredentialAttachLiveReceipt::from_commit(attach_commit());
    assert!(matches!(
        lookup_credential_attach(
            CredentialAttachTokenPhase::LiveReceipt {
                identity: ResolvedIdentity::from(&other_tombstone),
                receipt: &receipt,
            },
            PresentedIdentity::from(Some(&live)),
            &BindingState::Detached,
            &request,
            AttachSecretProof::Verified,
        ),
        CredentialAttachLookupResult::Retired(_)
    ));

    let expired = lookup_credential_attach(
        CredentialAttachTokenPhase::Provenance {
            identity: ResolvedIdentity::from(&live),
            provenance: CredentialAttachProvenance::new(
                generation(4),
                ReceiptExpiryReason::Superseded,
            ),
        },
        PresentedIdentity::Absent,
        &BindingState::Detached,
        &request,
        AttachSecretProof::Mismatch,
    );
    assert!(matches!(
        expired,
        CredentialAttachLookupResult::ReceiptExpired(
            crate::wire::ReceiptExpired::CredentialAttach {
                presented_generation,
                presented_marker_delivery_seq: None,
                result_generation,
                current_generation,
                reason: ReceiptExpiryReason::Superseded,
                ..
            }
        ) if presented_generation == generation(1)
            && result_generation == generation(4)
            && current_generation == generation(5)
    ));

    assert!(matches!(
        lookup_credential_attach(
            CredentialAttachTokenPhase::AfterProvenance,
            PresentedIdentity::from(Some(&live)),
            &BindingState::Detached,
            &request,
            AttachSecretProof::Mismatch,
        ),
        CredentialAttachLookupResult::StaleOrUnknownReceipt(
            crate::wire::StaleOrUnknownReceipt {
                current_generation,
                ..
            }
        ) if current_generation == generation(5)
    ));
}

#[test]
fn fresh_attach_runs_presented_tombstone_unknown_stale_then_authorized() {
    let request = attach_request(3, 0xB1, None);
    let retired = tombstone();
    assert!(matches!(
        lookup_credential_attach(
            CredentialAttachTokenPhase::NoMatch,
            PresentedIdentity::from(Some(&retired)),
            &BindingState::Detached,
            &request,
            AttachSecretProof::Verified,
        ),
        CredentialAttachLookupResult::Retired(_)
    ));
    assert!(matches!(
        lookup_credential_attach::<TestFingerprint, TestVerifier, TestFingerprint>(
            CredentialAttachTokenPhase::NoMatch,
            PresentedIdentity::Absent,
            &BindingState::Detached,
            &request,
            AttachSecretProof::Verified,
        ),
        CredentialAttachLookupResult::ParticipantUnknown(_)
    ));

    let live: TestIdentity = IdentityState::Live(member(3));
    assert!(matches!(
        lookup_credential_attach(
            CredentialAttachTokenPhase::NoMatch,
            PresentedIdentity::from(Some(&live)),
            &BindingState::Detached,
            &request,
            AttachSecretProof::Mismatch,
        ),
        CredentialAttachLookupResult::StaleAuthority(_)
    ));
    assert!(matches!(
        lookup_credential_attach(
            CredentialAttachTokenPhase::NoMatch,
            PresentedIdentity::from(Some(&live)),
            &BindingState::Detached,
            &request,
            AttachSecretProof::Verified,
        ),
        CredentialAttachLookupResult::AuthorizedFresh { .. }
    ));
}

#[test]
fn detach_exact_resolution_and_pending_different_token_precede_binding() {
    let request = detach_request(3, 0xD5);
    let verifier = [0xD5; 32];
    let member = member(3);
    let binding = ActiveBinding {
        participant_id: 7,
        conversation_id: 11,
        binding_epoch: epoch(3, 5),
    };
    let verified_detach = binding
        .verify_detach_request(request.clone(), verifier)
        .expect("request matches binding");
    let transition = start_blocked_detach(
        member,
        verified_detach,
        DetachCell::default(),
        PendingBindingTerminalPosition::new(9),
        4,
    )
    .expect("empty cell accepts pending detach");
    let (member, pending_binding, pending, _) = transition.into_parts();
    let live: TestIdentity = IdentityState::Live(member);

    assert!(matches!(
        lookup_detach(&DetachLookupContext {
            token_resolution: DetachTokenResolution::Exact(ResolvedIdentity::from(&live)),
            presented_identity: PresentedIdentity::Absent,
            cell: &DetachCell::Pending(pending),
            binding: &pending_binding,
            receiving_binding_epoch: None,
            request: &request,
            request_verifier: verifier,
            observer_progress: 4,
        }),
        DetachLookupResult::PendingReplayRequired(_)
    ));
    let competing = detach_request(1, 0xD6);
    let retired = tombstone();
    assert!(matches!(
        lookup_detach(&DetachLookupContext {
            token_resolution: DetachTokenResolution::NoExactMatch,
            presented_identity: PresentedIdentity::from(Some(&retired)),
            cell: &DetachCell::Pending(pending),
            binding: &BindingState::Detached,
            receiving_binding_epoch: None,
            request: &competing,
            request_verifier: [0; 32],
            observer_progress: 4,
        }),
        DetachLookupResult::Retired(_)
    ));
    assert!(matches!(
        lookup_detach(&DetachLookupContext {
            token_resolution: DetachTokenResolution::NoExactMatch,
            presented_identity: PresentedIdentity::from(Some(&live)),
            cell: &DetachCell::Pending(pending),
            binding: &BindingState::Detached,
            receiving_binding_epoch: None,
            request: &competing,
            request_verifier: [0; 32],
            observer_progress: 4,
        }),
        DetachLookupResult::DetachInProgress(_)
    ));
}

fn binding_requests(generation_value: u64) -> [ParticipantBindingRequest; 3] {
    [
        ParticipantBindingRequest::ParticipantAck(ParticipantAck {
            conversation_id: 11,
            participant_id: 7,
            capability_generation: generation(generation_value),
            through_seq: 12,
        }),
        ParticipantBindingRequest::MarkerAck(MarkerAck {
            conversation_id: 11,
            participant_id: 7,
            capability_generation: generation(generation_value),
            marker_delivery_seq: 12,
        }),
        ParticipantBindingRequest::RecordAdmission(RecordAdmission {
            conversation_id: 11,
            participant_id: 7,
            capability_generation: generation(generation_value),
            payload: alloc::vec![1, 2, 3],
        }),
    ]
}

#[test]
fn every_generic_binding_request_has_total_phase_one_through_five_order() {
    let retired = tombstone();
    let live: TestIdentity = IdentityState::Live(member(4));
    let binding = BindingState::Bound(ActiveBinding {
        participant_id: 7,
        conversation_id: 11,
        binding_epoch: epoch(4, 9),
    });

    for request in binding_requests(4) {
        assert!(matches!(
            lookup_binding_required(
                PresentedIdentity::from(Some(&retired)),
                &BindingState::Detached,
                None,
                &request,
            ),
            BindingRequiredLookupResult::Retired(_)
        ));
        assert!(matches!(
            lookup_binding_required::<TestFingerprint, TestVerifier, TestFingerprint>(
                PresentedIdentity::Absent,
                &BindingState::Detached,
                None,
                &request,
            ),
            BindingRequiredLookupResult::ParticipantUnknown(_)
        ));
        assert!(matches!(
            lookup_binding_required(
                PresentedIdentity::from(Some(&live)),
                &binding,
                None,
                &request,
            ),
            BindingRequiredLookupResult::NoBinding(_)
        ));
        assert!(matches!(
            lookup_binding_required(
                PresentedIdentity::from(Some(&live)),
                &binding,
                Some(epoch(4, 9)),
                &request,
            ),
            BindingRequiredLookupResult::Authorized { .. }
        ));
    }

    for request in binding_requests(3) {
        assert!(matches!(
            lookup_binding_required(
                PresentedIdentity::from(Some(&live)),
                &binding,
                Some(epoch(4, 9)),
                &request,
            ),
            BindingRequiredLookupResult::StaleAuthority(_)
        ));
    }
}
