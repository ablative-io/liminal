#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use crate::wire::{
    AttachAttemptToken, AttachEnvelope, AttachSecret, CredentialAttachRequest,
    CredentialAttachResponse, EnrollmentEnvelope, EnrollmentRequest, EnrollmentResponse,
    EnrollmentToken, Generation, IdentityCapacityExceeded, IdentityCapacityScope,
};

use super::capacity::{
    BindingSlotDecision, BindingSlotOccupancy, CapacityCounter, CapacityCounterInvariantError,
    ConnectionConversationTracking, CredentialAttachCapacityCounters, EnrollmentCapacityCounters,
    EnrollmentCapacityDecision, FreshParticipantCapacityCounter,
    FreshParticipantCapacityCounterInvariantError, ParticipantWindowAdmission,
    SemanticConnectionCapacityDecision, select_credential_attach_binding_slot,
    select_credential_attach_capacity, select_enrollment_binding_slot, select_enrollment_capacity,
    select_participant_window, select_semantic_connection_capacity,
};

fn counter(limit: u64, occupied: u64) -> CapacityCounter {
    CapacityCounter::try_new(limit, occupied).expect("test counter must be valid")
}

fn fresh_counter(limit: u64) -> FreshParticipantCapacityCounter {
    FreshParticipantCapacityCounter::try_new(limit, 0)
        .expect("fresh test participant counter must be valid")
}

fn generation(value: u64) -> Generation {
    Generation::new(value).expect("test generation must be nonzero")
}

fn enrollment() -> EnrollmentRequest {
    EnrollmentRequest {
        conversation_id: 41,
        enrollment_token: EnrollmentToken::new([4; 16]),
    }
}

fn enrollment_envelope() -> EnrollmentEnvelope {
    let request = enrollment();
    EnrollmentEnvelope {
        conversation_id: request.conversation_id,
        enrollment_token: request.enrollment_token,
    }
}

fn attach() -> CredentialAttachRequest {
    CredentialAttachRequest {
        conversation_id: 42,
        participant_id: 73,
        capability_generation: generation(7),
        attach_secret: AttachSecret::new([8; 32]),
        attach_attempt_token: AttachAttemptToken::new([9; 16]),
        accept_marker_delivery_seq: Some(101),
    }
}

fn attach_envelope() -> AttachEnvelope {
    let request = attach();
    AttachEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        attach_attempt_token: request.attach_attempt_token,
        accept_marker_delivery_seq: request.accept_marker_delivery_seq,
    }
}

fn enrollment_counters(values: [CapacityCounter; 2]) -> EnrollmentCapacityCounters {
    EnrollmentCapacityCounters::new(values[0], values[1], fresh_counter(31), fresh_counter(32))
}

fn attach_counters(values: [CapacityCounter; 2]) -> CredentialAttachCapacityCounters {
    CredentialAttachCapacityCounters::new(values[0], values[1])
}

#[test]
fn capacity_counter_requires_a_nonzero_bounded_state() {
    assert_eq!(
        CapacityCounter::try_new(0, 0),
        Err(CapacityCounterInvariantError::ZeroLimit),
    );
    assert_eq!(
        CapacityCounter::try_new(3, 4),
        Err(CapacityCounterInvariantError::OccupiedExceedsLimit {
            occupied: 4,
            limit: 3,
        }),
    );

    let full = counter(3, 3);
    assert_eq!(full.limit(), 3);
    assert_eq!(full.occupied(), 3);
    assert!(full.is_full());
}

#[test]
fn fresh_participant_counters_reject_nonempty_restored_state() {
    assert_eq!(
        FreshParticipantCapacityCounter::try_new(2, 1),
        Err(FreshParticipantCapacityCounterInvariantError::Nonempty { occupied: 1 }),
    );
    assert_eq!(
        FreshParticipantCapacityCounter::try_new(0, 0),
        Err(FreshParticipantCapacityCounterInvariantError::Capacity(
            CapacityCounterInvariantError::ZeroLimit,
        )),
    );
}

#[test]
fn already_tracked_semantic_conversation_consumes_zero_capacity_at_full_limit() {
    let decision = select_semantic_connection_capacity(
        ConnectionConversationTracking::AlreadyTracked,
        counter(2, 2),
    );
    let SemanticConnectionCapacityDecision::Commit(commit) = decision else {
        panic!("an already tracked conversation must preserve full capacity");
    };
    assert_eq!(commit.resulting(), counter(2, 2));
    assert!(!commit.newly_tracked());
}

#[test]
fn first_untracked_semantic_conversation_returns_exact_capacity_refusal() {
    assert_eq!(
        select_semantic_connection_capacity(
            ConnectionConversationTracking::Untracked,
            counter(2, 2),
        ),
        SemanticConnectionCapacityDecision::Respond { limit: 2 },
    );

    let decision = select_semantic_connection_capacity(
        ConnectionConversationTracking::Untracked,
        counter(2, 1),
    );
    let SemanticConnectionCapacityDecision::Commit(commit) = decision else {
        panic!("one free connection slot must commit");
    };
    assert_eq!(commit.resulting(), counter(2, 2));
    assert!(commit.newly_tracked());
}

#[test]
fn binding_slot_selectors_return_exact_origin_specific_outcomes() {
    let enrollment_request = enrollment();
    assert_eq!(
        select_enrollment_binding_slot(
            &enrollment_request,
            BindingSlotOccupancy::Occupied {
                participant_id: 999,
            },
        ),
        BindingSlotDecision::Respond(
            EnrollmentResponse::connection_conversation_binding_occupied(&enrollment_envelope()),
        ),
    );
    assert_eq!(
        select_enrollment_binding_slot(&enrollment_request, BindingSlotOccupancy::Empty),
        BindingSlotDecision::Available,
    );

    let attach_request = attach();
    assert_eq!(
        select_credential_attach_binding_slot(
            &attach_request,
            BindingSlotOccupancy::Occupied {
                participant_id: attach_request.participant_id,
            },
        ),
        BindingSlotDecision::Available,
    );
    assert_eq!(
        select_credential_attach_binding_slot(
            &attach_request,
            BindingSlotOccupancy::Occupied { participant_id: 74 },
        ),
        BindingSlotDecision::Respond(
            CredentialAttachResponse::connection_conversation_binding_occupied(&attach_envelope()),
        ),
    );
}

/// Lane p0-39 REWRITE of `enrollment_runtime_capacity_uses_the_exact_five_scope_precedence`.
///
/// That pin walked five refusable scopes. Three of them (`LiveReceiptServer`,
/// `ProvenanceServer`, `ProvenanceConversation`) no longer refuse anything, so
/// its loop over indices 2..5 would have asserted over a premise that is now
/// false — vacuous, not passing. It is rewritten here to assert the surviving
/// law: identity Server precedes identity Conversation, and those two are the
/// COMPLETE refusable set for enrollment.
#[test]
fn enrollment_runtime_capacity_refuses_only_the_two_identity_scopes_in_order() {
    let request = enrollment();
    for failing_index in 0..2 {
        let mut values = [counter(2, 1); 2];
        for (index, value) in values.iter_mut().enumerate().skip(failing_index) {
            let limit = 10 + u64::try_from(index).expect("two indices fit u64");
            *value = counter(limit, limit);
        }

        let expected = match failing_index {
            0 => IdentityCapacityExceeded {
                request: enrollment_envelope(),
                scope: IdentityCapacityScope::Server,
                limit: 10,
                occupied: 10,
            },
            1 => IdentityCapacityExceeded {
                request: enrollment_envelope(),
                scope: IdentityCapacityScope::Conversation,
                limit: 11,
                occupied: 11,
            },
            _ => panic!("two identity scopes are exhaustive"),
        };

        assert_eq!(
            select_enrollment_capacity(&request, enrollment_counters(values)),
            EnrollmentCapacityDecision::Respond(EnrollmentResponse::identity_capacity_exceeded(
                expected,
            )),
        );
    }
}

/// The receipt scopes have NO enrollment refusal arm left, at any occupancy.
///
/// The positive control is in the pin above: the same selector still refuses,
/// loudly and by name, when an IDENTITY scope is full — so a green here
/// measures the receipt scopes' silence and not a dead selector.
#[test]
fn no_receipt_occupancy_can_make_enrollment_refuse() {
    // Identity has one slot of headroom; every per-participant window is at
    // its minimum size of one. Nothing about receipts may produce a refusal.
    let decision = select_enrollment_capacity(
        &enrollment(),
        EnrollmentCapacityCounters::new(
            counter(10, 9),
            counter(11, 10),
            fresh_counter(1),
            fresh_counter(1),
        ),
    );
    let EnrollmentCapacityDecision::Commit(commit) = decision else {
        panic!("no configured receipt number may refuse an honest enrollment");
    };
    assert_eq!(commit.resulting().identity_server(), counter(10, 10));
    assert_eq!(commit.resulting().identity_conversation(), counter(11, 11));
}

/// Lane p0-39 REWRITE of `enrollment_success_carries_every_incremented_counter_atomically`.
///
/// The three shared-scope assertions went vacuous with the fields they read.
/// What survives is asserted here, plus the board #37 fact the old pin had
/// wrong: enrollment reserves a live-receipt slot but fills NO provenance —
/// nothing has proven possession of the secret it just minted.
#[test]
fn enrollment_success_carries_the_identity_counters_and_both_reserved_windows() {
    let current = EnrollmentCapacityCounters::new(
        counter(11, 1),
        counter(12, 2),
        fresh_counter(16),
        fresh_counter(17),
    );
    let decision = select_enrollment_capacity(&enrollment(), current);
    let EnrollmentCapacityDecision::Commit(commit) = decision else {
        panic!("both identity counters have capacity");
    };
    let resulting = commit.resulting();
    assert_eq!(resulting.identity_server(), counter(11, 2));
    assert_eq!(resulting.identity_conversation(), counter(12, 3));
    // The enrollment receipt body occupies its window.
    assert_eq!(resulting.live_receipt_participant(), counter(16, 1));
    // Board #37: the fingerprint is not retained until an attach proves it.
    assert_eq!(resulting.provenance_participant(), counter(17, 0));

    assert_eq!(current.identity_server(), counter(11, 1));
    assert_eq!(current.live_receipt_participant().occupied(), 0);
    assert_eq!(current.provenance_participant().occupied(), 0);
}

/// Lane p0-39 REWRITE of `credential_attach_capacity_uses_all_five_receipt_scopes_in_order`.
///
/// Every assertion in that pin read a `Respond` arm that no longer exists —
/// the whole test was a statement about refusal precedence in a selector that
/// cannot refuse. It is rewritten as the law that replaced it: the selector is
/// TOTAL, and a full window displaces instead of refusing.
#[test]
fn credential_attach_always_admits_and_a_full_window_displaces() {
    // Both windows exactly full: the hardest input the old pin could build,
    // and the one it asserted a refusal for.
    let commit = select_credential_attach_capacity(attach_counters([counter(3, 3), counter(4, 4)]));
    assert!(commit.live_receipt_participant().displaced());
    assert!(commit.provenance_participant().displaced());
    assert_eq!(
        commit.live_receipt_participant().admission(),
        ParticipantWindowAdmission::Displaced,
    );
    // BOUND HOLDS EXACTLY: displacement leaves occupancy at the window size,
    // never one above it.
    assert_eq!(commit.live_receipt_participant().resulting(), counter(3, 3));
    assert_eq!(commit.provenance_participant().resulting(), counter(4, 4));
}

/// Lane p0-39 REWRITE of `credential_attach_success_carries_every_incremented_counter_atomically`.
#[test]
fn credential_attach_with_headroom_lands_without_displacing() {
    let commit =
        select_credential_attach_capacity(attach_counters([counter(22, 2), counter(25, 5)]));
    assert!(!commit.live_receipt_participant().displaced());
    assert!(!commit.provenance_participant().displaced());
    assert_eq!(
        commit.provenance_participant().admission(),
        ParticipantWindowAdmission::Landed,
    );
    assert_eq!(
        commit.live_receipt_participant().resulting(),
        counter(22, 3)
    );
    assert_eq!(commit.provenance_participant().resulting(), counter(25, 6));
}

/// The window selector's complete law, over the whole domain of one window:
/// every occupancy admits, occupancy never exceeds the size, and the size is
/// reached only by displacement.
#[test]
fn a_participant_window_admits_at_every_occupancy_and_never_exceeds_its_size() {
    let size = 4;
    for occupied in 0..=size {
        let commit = select_participant_window(counter(size, occupied));
        let resulting = commit.resulting();
        assert!(
            resulting.occupied() <= size,
            "window of {size} left occupancy {} from {occupied}",
            resulting.occupied(),
        );
        if occupied == size {
            assert!(
                commit.displaced(),
                "a full window must displace, not refuse"
            );
            assert_eq!(resulting.occupied(), size);
        } else {
            assert!(!commit.displaced());
            assert_eq!(resulting.occupied(), occupied + 1);
        }
    }
}

/// A window of one — the tightest configured number a deployment can write —
/// still lands every arrival, displacing its single member each time.
#[test]
fn a_window_of_one_still_lands_every_arrival() {
    let commit = select_participant_window(counter(1, 1));
    assert!(commit.displaced());
    assert_eq!(commit.resulting(), counter(1, 1));
}
