//! Legal request-to-response matrix tests for the bound response authorities.
//!
//! The matrix is transcribed from the R-D1 register of
//! `docs/design/PARTICIPANT-CONTRACT.md` @
//! `55856ae3c53206f9c662e6815650dfc67a89ce85`: the outcome table at lines
//! 5624-5689, the exhaustive-pair routing rule at lines 5773-5784, the
//! ack-exclusion rule at lines 5699-5701, and the sequence-reachability rule
//! at lines 5691-5696. Each per-request test constructs one value for every
//! legal outcome class its register rows admit, paired with the exact
//! [`ServerDiscriminant`] the register mandates for that outcome. Each
//! constructed response is checked against the wire's structural
//! `originating_request` echo AND its wire discriminant, proving every
//! constructor stays inside its request's legal set and selects its mandated
//! variant (two same-payload constructors such as `bound`/`unbound_receipt`
//! cannot be silently swapped).
//!
//! This file holds the enrollment and credential-attach matrices; the
//! lifecycle-request matrices (detach, acks, leave) live in
//! `lifecycle_matrix` and the record-admission/observer-recovery matrices in
//! `records_matrix`, keeping every file inside the size ceiling.

#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

mod lifecycle_matrix;
mod records_matrix;
mod support;

use alloc::{boxed::Box, vec};

use self::support::{
    assert_bound, attach_bound, attach_envelope, attach_marker_proof, closure_capacity_exceeded,
    closure_snapshot, enroll_bound, enrollment_envelope, generation, order_exhausted,
    sequence_budget, sequence_exhausted,
};

use super::{
    AttemptConflict, ClientDiscriminant, ClosureCheckedEnvelope, ClosureRefusalReason,
    CredentialAttachResponse, EnrollmentKnown, EnrollmentReceiptCapacityScope, EnrollmentResponse,
    EnrollmentToken, IdentityCapacityExceeded, IdentityCapacityScope, MarkerMismatchBody,
    MarkerNotDeliveredReason, ObserverBackpressureState, OrderAllocatingEnvelope, ReceiptExpired,
    Retired, SequenceAllocatingEnvelope, ServerDiscriminant, StaleOrUnknownReceipt,
};

/// Enrollment legal set: register rows 5641, 5643, 5644, 5649, 5650-5657,
/// and 5663.
#[test]
fn enrollment_constructors_stay_inside_the_register_rows() {
    let responses = vec![
        // Row 5641: first-decoded-semantic-operation connection capacity.
        (
            EnrollmentResponse::connection_conversation_capacity_exceeded(enrollment_envelope(), 4),
            ServerDiscriminant::ConnectionConversationCapacityExceeded,
        ),
        // Row 5643: enrollment binding attempt on an occupied slot.
        (
            EnrollmentResponse::connection_conversation_binding_occupied(&enrollment_envelope()),
            ServerDiscriminant::ConnectionConversationBindingOccupied,
        ),
        // Row 5644: conversation order exhausted, shared-allocator payload
        // with the enrollment envelope arm.
        (
            EnrollmentResponse::from_conversation_order_exhausted(Box::new(order_exhausted(
                OrderAllocatingEnvelope::Enrollment(enrollment_envelope()),
            ))),
            ServerDiscriminant::ConversationOrderExhausted,
        ),
        // Row 5649: marker-closure capacity, shared-selector payload with the
        // enrollment envelope arm.
        (
            EnrollmentResponse::from_marker_closure_capacity_exceeded(Box::new(
                closure_capacity_exceeded(
                    ClosureCheckedEnvelope::Enrollment(enrollment_envelope()),
                ),
            )),
            ServerDiscriminant::MarkerClosureCapacityExceeded,
        ),
        // Row 5650: EnrollBound success.
        (
            EnrollmentResponse::enroll_bound(enroll_bound()),
            ServerDiscriminant::EnrollBound,
        ),
        // Row 5651: EnrollmentKnown post-provenance replay.
        (
            EnrollmentResponse::enrollment_known(EnrollmentKnown {
                conversation_id: 7,
                token: EnrollmentToken::new([1; 16]),
                participant_id: 3,
                current_generation: generation(2),
            }),
            ServerDiscriminant::EnrollmentKnown,
        ),
        // Row 5652: exact enrollment provenance-window expiry.
        (
            EnrollmentResponse::from_receipt_expired(ReceiptExpired::Enrollment {
                conversation_id: 7,
                token: EnrollmentToken::new([1; 16]),
                participant_id: 3,
                result_generation: generation(1),
                current_generation: generation(2),
                reason: super::ReceiptExpiryReason::Deadline,
            }),
            ServerDiscriminant::ReceiptExpired,
        ),
        // Row 5653: enrollment token mapping resolved to a tombstone.
        (
            EnrollmentResponse::from_retired(Retired::Enrollment {
                request: enrollment_envelope(),
                participant_id: 3,
                retired_generation: generation(2),
            }),
            ServerDiscriminant::Retired,
        ),
        // Row 5654: reachable receipt/provenance scope full.
        (
            EnrollmentResponse::receipt_capacity_exceeded(
                enrollment_envelope(),
                EnrollmentReceiptCapacityScope::LiveReceiptServer,
                8,
                8,
            ),
            ServerDiscriminant::ReceiptCapacityExceeded,
        ),
        // Row 5655: identity capacity full, server scope before conversation.
        (
            EnrollmentResponse::identity_capacity_exceeded(IdentityCapacityExceeded {
                request: enrollment_envelope(),
                scope: IdentityCapacityScope::Server,
                limit: 8,
                occupied: 8,
            }),
            ServerDiscriminant::IdentityCapacityExceeded,
        ),
        // Row 5656: observer backpressure with the common envelope.
        (
            EnrollmentResponse::observer_backpressure(
                enrollment_envelope(),
                ObserverBackpressureState::initial(5),
            ),
            ServerDiscriminant::ObserverBackpressure,
        ),
        // Row 5657: canonical sequence-reserve check failed, shared-allocator
        // payload with the enrollment envelope arm.
        (
            EnrollmentResponse::from_conversation_sequence_exhausted(Box::new(sequence_exhausted(
                SequenceAllocatingEnvelope::Enrollment(enrollment_envelope()),
            ))),
            ServerDiscriminant::ConversationSequenceExhausted,
        ),
        // Row 5663: Bound / UnboundReceipt byte-identical replay.
        (
            EnrollmentResponse::bound(enroll_bound()),
            ServerDiscriminant::Bound,
        ),
        (
            EnrollmentResponse::unbound_receipt(enroll_bound()),
            ServerDiscriminant::UnboundReceipt,
        ),
    ];
    for (response, discriminant) in responses {
        assert_bound(
            response.server_value(),
            ClientDiscriminant::EnrollmentRequest,
            discriminant,
        );
    }
}

/// Credential-attach legal set: register rows 5639, 5641, 5643, 5644, 5645,
/// 5647-5649, and 5658-5663.
#[test]
fn credential_attach_constructors_stay_inside_the_register_rows() {
    let responses = vec![
        // Row 5639: verified receipt with changed non-secret body.
        (
            CredentialAttachResponse::attempt_token_body_conflict(
                &attach_envelope(),
                AttemptConflict::Generation,
            ),
            ServerDiscriminant::AttemptTokenBodyConflict,
        ),
        // Row 5641: connection-conversation capacity.
        (
            CredentialAttachResponse::connection_conversation_capacity_exceeded(
                attach_envelope(),
                4,
            ),
            ServerDiscriminant::ConnectionConversationCapacityExceeded,
        ),
        // Row 5643: occupied binding slot.
        (
            CredentialAttachResponse::connection_conversation_binding_occupied(&attach_envelope()),
            ServerDiscriminant::ConnectionConversationBindingOccupied,
        ),
        // Row 5644: conversation order exhausted.
        (
            CredentialAttachResponse::conversation_order_exhausted(
                attach_envelope(),
                9,
                1,
                4,
                0,
                4,
            ),
            ServerDiscriminant::ConversationOrderExhausted,
        ),
        // Row 5645: unknown participant.
        (
            CredentialAttachResponse::participant_unknown(attach_envelope()),
            ServerDiscriminant::ParticipantUnknown,
        ),
        // Rows 5647/5660/5665: live stale authority.
        (
            CredentialAttachResponse::stale_authority(attach_envelope(), generation(3)),
            ServerDiscriminant::StaleAuthority,
        ),
        // Rows 5648/5659/5667: tombstone.
        (
            CredentialAttachResponse::retired(attach_envelope(), generation(3)),
            ServerDiscriminant::Retired,
        ),
        // Rows 5649/5662: marker-closure capacity.
        (
            CredentialAttachResponse::marker_closure_capacity_exceeded(
                attach_envelope(),
                closure_snapshot(),
                ClosureRefusalReason::DeliveredMarkerAwaitingAck,
            ),
            ServerDiscriminant::MarkerClosureCapacityExceeded,
        ),
        // Row 5658: AttachBound success.
        (
            CredentialAttachResponse::attach_bound(attach_bound()),
            ServerDiscriminant::AttachBound,
        ),
        // Rows 5659/5664: provenance-window receipt expiry.
        (
            CredentialAttachResponse::receipt_expired(
                &attach_envelope(),
                generation(2),
                generation(3),
                super::ReceiptExpiryReason::Deadline,
            ),
            ServerDiscriminant::ReceiptExpired,
        ),
        // Rows 5659/5666: post-provenance ambiguity.
        (
            CredentialAttachResponse::stale_or_unknown_receipt(StaleOrUnknownReceipt {
                conversation_id: 7,
                token: super::AttachAttemptToken::new([2; 16]),
                participant_id: 3,
                presented_generation: generation(2),
                presented_marker_delivery_seq: None,
                current_generation: generation(3),
            }),
            ServerDiscriminant::StaleOrUnknownReceipt,
        ),
        // Row 5661: fenced-attach marker proofs.
        (
            CredentialAttachResponse::marker_not_delivered(
                attach_marker_proof(),
                MarkerNotDeliveredReason::NotDeliveredToProofEpoch,
                12,
            ),
            ServerDiscriminant::MarkerNotDelivered,
        ),
        (
            CredentialAttachResponse::marker_mismatch(
                attach_marker_proof(),
                MarkerMismatchBody::NoMarkerExpected,
            ),
            ServerDiscriminant::MarkerMismatch,
        ),
        // Row 5662: receipt capacity, observer backpressure, and sequence
        // exhaustion with the common envelope.
        (
            CredentialAttachResponse::receipt_capacity_exceeded(
                attach_envelope(),
                super::ReceiptCapacityScope::LiveReceiptParticipant,
                8,
                8,
            ),
            ServerDiscriminant::ReceiptCapacityExceeded,
        ),
        (
            CredentialAttachResponse::observer_backpressure(
                attach_envelope(),
                ObserverBackpressureState::initial(5),
            ),
            ServerDiscriminant::ObserverBackpressure,
        ),
        (
            CredentialAttachResponse::conversation_sequence_exhausted(
                attach_envelope(),
                sequence_budget(),
            ),
            ServerDiscriminant::ConversationSequenceExhausted,
        ),
        // Row 5663: Bound / UnboundReceipt byte-identical replay.
        (
            CredentialAttachResponse::bound(attach_bound()),
            ServerDiscriminant::Bound,
        ),
        (
            CredentialAttachResponse::unbound_receipt(attach_bound()),
            ServerDiscriminant::UnboundReceipt,
        ),
    ];
    for (response, discriminant) in responses {
        assert_bound(
            response.server_value(),
            ClientDiscriminant::CredentialAttachRequest,
            discriminant,
        );
    }
}
