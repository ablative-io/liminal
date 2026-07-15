#![allow(clippy::expect_used)]

use crate::wire::{
    AttachSecret, BindingEpoch, ConnectionIncarnation, DetachAttemptToken, DetachRequest,
    Generation, LeaveAttemptToken, LeaveCommitted, LeaveRequest,
};

use super::lookup::{
    DetachLookupResult, LeaveLookupResult, LeaveSecretProof, PresentedIdentity, lookup_detach,
    lookup_leave,
};
use super::{
    ActiveBinding, BindingState, DetachCell, EnrollmentFingerprint, IdentityState,
    LeaveFingerprint, LiveMember, commit_detach,
};

fn generation(value: u64) -> Generation {
    Generation::new(value).expect("test generation is nonzero")
}

fn epoch(generation_value: u64, ordinal: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(9, ordinal),
        generation(generation_value),
    )
}

fn member(generation_value: u64) -> LiveMember {
    let secret_byte = u8::try_from(generation_value).expect("test generation fits one byte");
    LiveMember {
        participant_id: 7,
        conversation_id: 11,
        generation: generation(generation_value),
        attach_secret: AttachSecret::new([secret_byte; 32]),
        cursor: 0,
    }
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
    let (_, committed, _) = commit_detach(binding, verified_request, 21);
    (committed, request, verifier)
}

fn tombstone() -> IdentityState<[u8; 32]> {
    let live = member(3);
    let leave_token = LeaveAttemptToken::new([0x51; 16]);
    let result = LeaveCommitted {
        conversation_id: 11,
        leave_attempt_token: leave_token,
        participant_id: 7,
        presented_generation: generation(3),
        retired_generation: generation(3),
        ended_binding_epoch: None,
        prior_terminal_delivery_seq: Some(20),
        left_delivery_seq: 22,
    };
    let retired = live
        .retire(
            EnrollmentFingerprint::new([1; 32]),
            leave_token,
            [2; 32],
            LeaveFingerprint::new([3; 32]),
            result,
        )
        .expect("result describes member");
    IdentityState::Retired(retired)
}

#[test]
fn detach_tombstone_precedes_exact_committed_cell() {
    let (committed, request, verifier) = committed_detach();
    let identity = tombstone();
    let result = lookup_detach(
        PresentedIdentity::from(Some(&identity)),
        &DetachCell::Committed(committed),
        &BindingState::Detached,
        &request,
        verifier,
        0,
    );
    assert!(matches!(result, DetachLookupResult::Retired(_)));
}

#[test]
fn absent_identity_precedes_impossible_cell_bytes() {
    let (committed, request, verifier) = committed_detach();
    let result = lookup_detach(
        PresentedIdentity::Absent,
        &DetachCell::Committed(committed),
        &BindingState::Detached,
        &request,
        verifier,
        0,
    );
    assert!(matches!(result, DetachLookupResult::ParticipantUnknown(_)));
}

#[test]
fn terminalized_exact_token_precedes_current_generation_and_binding() {
    let (committed, request, verifier) = committed_detach();
    let terminalized = committed.terminalize();
    let live = IdentityState::Live(member(4));
    let new_binding = BindingState::Bound(ActiveBinding {
        participant_id: 7,
        conversation_id: 11,
        binding_epoch: epoch(4, 2),
    });
    let result = lookup_detach(
        PresentedIdentity::from(Some(&live)),
        &DetachCell::Terminalized(terminalized),
        &new_binding,
        &request,
        verifier,
        0,
    );
    assert!(matches!(
        result,
        DetachLookupResult::StaleAuthority(
            crate::wire::DetachStaleAuthority::TerminalizedDetachCell(_)
        )
    ));
}

#[test]
fn detach_ordinary_order_is_stale_then_no_binding_then_authorized() {
    let live = IdentityState::Live(member(4));
    let stale = detach_request(3, 1);
    let current = detach_request(4, 2);
    let verifier = [9; 32];

    assert!(matches!(
        lookup_detach(
            PresentedIdentity::from(Some(&live)),
            &DetachCell::default(),
            &BindingState::Detached,
            &stale,
            verifier,
            0,
        ),
        DetachLookupResult::StaleAuthority(_)
    ));
    assert!(matches!(
        lookup_detach(
            PresentedIdentity::from(Some(&live)),
            &DetachCell::default(),
            &BindingState::Detached,
            &current,
            verifier,
            0,
        ),
        DetachLookupResult::NoBinding(_)
    ));

    let binding = BindingState::Bound(ActiveBinding {
        participant_id: 7,
        conversation_id: 11,
        binding_epoch: epoch(4, 3),
    });
    assert!(matches!(
        lookup_detach(
            PresentedIdentity::from(Some(&live)),
            &DetachCell::default(),
            &binding,
            &current,
            verifier,
            0,
        ),
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

    let live: IdentityState<[u8; 32]> = IdentityState::Live(member(3));
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
