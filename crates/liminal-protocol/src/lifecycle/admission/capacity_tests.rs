#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use crate::wire::{
    AttachAttemptToken, AttachEnvelope, AttachSecret, ConnectionConversationBindingOccupied,
    ConnectionConversationCapacityExceeded, CredentialAttachRequest, EnrollmentEnvelope,
    EnrollmentReceiptCapacityScope, EnrollmentRequest, EnrollmentToken, Generation,
    IdentityCapacityExceeded, IdentityCapacityScope, ReceiptCapacityExceeded, ReceiptCapacityScope,
    ResponseEnvelope, ServerValue,
};

use super::capacity::{
    BindingSlotDecision, BindingSlotOccupancy, CapacityCounter, CapacityCounterInvariantError,
    ConnectionConversationTracking, CredentialAttachCapacityCounters,
    CredentialAttachCapacityDecision, EnrollmentCapacityCounters, EnrollmentCapacityDecision,
    FreshParticipantCapacityCounter, FreshParticipantCapacityCounterInvariantError,
    SemanticConnectionCapacityDecision, select_credential_attach_binding_slot,
    select_credential_attach_capacity, select_enrollment_binding_slot, select_enrollment_capacity,
    select_semantic_connection_capacity,
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

fn enrollment_counters(values: [CapacityCounter; 5]) -> EnrollmentCapacityCounters {
    EnrollmentCapacityCounters::new(
        values[0],
        values[1],
        values[2],
        fresh_counter(31),
        values[3],
        values[4],
        fresh_counter(32),
    )
}

fn attach_counters(values: [CapacityCounter; 5]) -> CredentialAttachCapacityCounters {
    CredentialAttachCapacityCounters::new(values[0], values[1], values[2], values[3], values[4])
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
        ResponseEnvelope::Enrollment(enrollment_envelope()),
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
    let request = ResponseEnvelope::Enrollment(enrollment_envelope());
    assert_eq!(
        select_semantic_connection_capacity(
            request.clone(),
            ConnectionConversationTracking::Untracked,
            counter(2, 2),
        ),
        SemanticConnectionCapacityDecision::Respond(
            ServerValue::ConnectionConversationCapacityExceeded(
                ConnectionConversationCapacityExceeded::SemanticRequest { request, limit: 2 },
            ),
        ),
    );

    let decision = select_semantic_connection_capacity(
        ResponseEnvelope::Enrollment(enrollment_envelope()),
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
        BindingSlotDecision::Respond(ServerValue::ConnectionConversationBindingOccupied(
            ConnectionConversationBindingOccupied::Enrollment {
                conversation_id: enrollment_request.conversation_id,
                enrollment_token: enrollment_request.enrollment_token,
            },
        )),
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
        BindingSlotDecision::Respond(ServerValue::ConnectionConversationBindingOccupied(
            ConnectionConversationBindingOccupied::CredentialAttach {
                conversation_id: attach_request.conversation_id,
                participant_id: attach_request.participant_id,
                capability_generation: attach_request.capability_generation,
                attach_attempt_token: attach_request.attach_attempt_token,
                accept_marker_delivery_seq: attach_request.accept_marker_delivery_seq,
            },
        )),
    );
}

#[test]
fn enrollment_runtime_capacity_uses_the_exact_five_scope_precedence() {
    let request = enrollment();
    for failing_index in 0..5 {
        let mut values = [counter(2, 1); 5];
        for (index, value) in values.iter_mut().enumerate().skip(failing_index) {
            let limit = 10 + u64::try_from(index).expect("five indices fit u64");
            *value = counter(limit, limit);
        }

        let expected = match failing_index {
            0 => ServerValue::IdentityCapacityExceeded(IdentityCapacityExceeded {
                request: enrollment_envelope(),
                scope: IdentityCapacityScope::Server,
                limit: 10,
                occupied: 10,
            }),
            1 => ServerValue::IdentityCapacityExceeded(IdentityCapacityExceeded {
                request: enrollment_envelope(),
                scope: IdentityCapacityScope::Conversation,
                limit: 11,
                occupied: 11,
            }),
            2 => ServerValue::ReceiptCapacityExceeded(ReceiptCapacityExceeded::Enrollment {
                request: enrollment_envelope(),
                scope: EnrollmentReceiptCapacityScope::LiveReceiptServer,
                limit: 12,
                occupied: 12,
            }),
            3 => ServerValue::ReceiptCapacityExceeded(ReceiptCapacityExceeded::Enrollment {
                request: enrollment_envelope(),
                scope: EnrollmentReceiptCapacityScope::ProvenanceServer,
                limit: 13,
                occupied: 13,
            }),
            4 => ServerValue::ReceiptCapacityExceeded(ReceiptCapacityExceeded::Enrollment {
                request: enrollment_envelope(),
                scope: EnrollmentReceiptCapacityScope::ProvenanceConversation,
                limit: 14,
                occupied: 14,
            }),
            _ => panic!("five enrollment scopes are exhaustive"),
        };

        assert_eq!(
            select_enrollment_capacity(&request, enrollment_counters(values)),
            EnrollmentCapacityDecision::Respond(expected),
        );
    }
}

#[test]
fn enrollment_success_carries_every_incremented_counter_atomically() {
    let current = EnrollmentCapacityCounters::new(
        counter(11, 1),
        counter(12, 2),
        counter(13, 3),
        fresh_counter(16),
        counter(14, 4),
        counter(15, 5),
        fresh_counter(17),
    );
    let decision = select_enrollment_capacity(&enrollment(), current);
    let EnrollmentCapacityDecision::Commit(commit) = decision else {
        panic!("all enrollment counters have capacity");
    };
    let resulting = commit.resulting();
    assert_eq!(resulting.identity_server(), counter(11, 2));
    assert_eq!(resulting.identity_conversation(), counter(12, 3));
    assert_eq!(resulting.live_receipt_server(), counter(13, 4));
    assert_eq!(resulting.live_receipt_participant(), counter(16, 1));
    assert_eq!(resulting.provenance_server(), counter(14, 5));
    assert_eq!(resulting.provenance_conversation(), counter(15, 6));
    assert_eq!(resulting.provenance_participant(), counter(17, 1));

    assert_eq!(current.identity_server(), counter(11, 1));
    assert_eq!(current.live_receipt_participant().occupied(), 0);
    assert_eq!(current.provenance_conversation(), counter(15, 5));
    assert_eq!(current.provenance_participant().occupied(), 0);
}

#[test]
fn credential_attach_capacity_uses_all_five_receipt_scopes_in_order() {
    let request = attach();
    let scopes = [
        ReceiptCapacityScope::LiveReceiptServer,
        ReceiptCapacityScope::LiveReceiptParticipant,
        ReceiptCapacityScope::ProvenanceServer,
        ReceiptCapacityScope::ProvenanceConversation,
        ReceiptCapacityScope::ProvenanceParticipant,
    ];

    for (failing_index, scope) in scopes.into_iter().enumerate() {
        let mut values = [counter(2, 1); 5];
        for (index, value) in values.iter_mut().enumerate().skip(failing_index) {
            let limit = 20 + u64::try_from(index).expect("five indices fit u64");
            *value = counter(limit, limit);
        }
        let limit = 20 + u64::try_from(failing_index).expect("five indices fit u64");
        assert_eq!(
            select_credential_attach_capacity(&request, attach_counters(values)),
            CredentialAttachCapacityDecision::Respond(ServerValue::ReceiptCapacityExceeded(
                ReceiptCapacityExceeded::CredentialAttach {
                    request: attach_envelope(),
                    scope,
                    limit,
                    occupied: limit,
                },
            )),
        );
    }
}

#[test]
fn credential_attach_success_carries_every_incremented_counter_atomically() {
    let current = CredentialAttachCapacityCounters::new(
        counter(21, 1),
        counter(22, 2),
        counter(23, 3),
        counter(24, 4),
        counter(25, 5),
    );
    let decision = select_credential_attach_capacity(&attach(), current);
    let CredentialAttachCapacityDecision::Commit(commit) = decision else {
        panic!("all credential-attach counters have capacity");
    };
    let resulting = commit.resulting();
    assert_eq!(resulting.live_receipt_server(), counter(21, 2));
    assert_eq!(resulting.live_receipt_participant(), counter(22, 3));
    assert_eq!(resulting.provenance_server(), counter(23, 4));
    assert_eq!(resulting.provenance_conversation(), counter(24, 5));
    assert_eq!(resulting.provenance_participant(), counter(25, 6));

    assert_eq!(current.live_receipt_server(), counter(21, 1));
    assert_eq!(current.provenance_participant(), counter(25, 5));
}
