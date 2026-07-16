//! Legal request-to-response matrix tests for the bound response authorities.
//!
//! The matrix is transcribed from the R-D1 register of
//! `docs/design/PARTICIPANT-CONTRACT.md` @
//! `55856ae3c53206f9c662e6815650dfc67a89ce85`: the outcome table at lines
//! 5624-5689, the exhaustive-pair routing rule at lines 5773-5784, the
//! ack-exclusion rule at lines 5699-5701, and the sequence-reachability rule
//! at lines 5691-5696. Each per-request test constructs one value for every
//! legal outcome class its register rows admit, and each constructed
//! response is checked against the wire's structural `originating_request`
//! echo, proving every constructor stays inside its request's legal set.

#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

mod support;

use alloc::{boxed::Box, vec, vec::Vec};

use self::support::{
    assert_bound, attach_bound, attach_envelope, attach_marker_proof, closure_capacity_exceeded,
    closure_snapshot, detach_envelope, enroll_bound, enrollment_envelope, epoch, generation,
    leave_envelope, marker_ack_envelope, marker_ack_proof, order_exhausted,
    participant_ack_envelope, record_envelope, sequence_budget, sequence_exhausted,
    terminalized_detach_cell,
};

use crate::algebra::{ResourceDimension, ResourceVector};

use super::{
    AckCommitted, AckGap, AckNoOp, AckRegression, AttemptConflict, BindingRequiredEnvelope,
    ClientDiscriminant, ClosureCheckedEnvelope, ClosureRefusalReason, CommonStaleAuthorityEnvelope,
    CredentialAttachResponse, DetachCommitted, DetachInProgress, DetachResponse,
    DetachStaleAuthority, EnrollmentKnown, EnrollmentReceiptCapacityScope, EnrollmentResponse,
    EnrollmentToken, IdentityCapacityExceeded, IdentityCapacityScope, InvalidObserverEpoch,
    InvalidObserverEpochList, LeaveCommitted, LeaveResponse, LeaveStaleAuthority,
    MarkerAckCommitted, MarkerAckResponse, MarkerMismatch, MarkerMismatchBody, MarkerNotDelivered,
    MarkerNotDeliveredReason, MarkerProofRequest, NoBinding, ObserverBackpressure,
    ObserverBackpressureState, ObserverRecoveryAccepted, ObserverRecoveryResponse,
    OrderAllocatingEnvelope, ParticipantAckResponse, ParticipantReferenceEnvelope,
    ParticipantUnknown, ReceiptExpired, RecordAdmissionResponse, RecordCommitted, RecordTooLarge,
    Retired, SequenceAllocatingEnvelope, ServerDiscriminant, ServerValue, StaleAuthority,
    StaleOrUnknownReceipt,
};

/// Enrollment legal set: register rows 5641, 5643, 5644, 5649, 5650-5657,
/// and 5663.
#[test]
fn enrollment_constructors_stay_inside_the_register_rows() {
    let responses = vec![
        // Row 5641: first-decoded-semantic-operation connection capacity.
        EnrollmentResponse::connection_conversation_capacity_exceeded(enrollment_envelope(), 4),
        // Row 5643: enrollment binding attempt on an occupied slot.
        EnrollmentResponse::connection_conversation_binding_occupied(&enrollment_envelope()),
        // Row 5644: conversation order exhausted, shared-allocator payload
        // with the enrollment envelope arm.
        EnrollmentResponse::from_conversation_order_exhausted(Box::new(order_exhausted(
            OrderAllocatingEnvelope::Enrollment(enrollment_envelope()),
        ))),
        // Row 5649: marker-closure capacity, shared-selector payload with the
        // enrollment envelope arm.
        EnrollmentResponse::from_marker_closure_capacity_exceeded(Box::new(
            closure_capacity_exceeded(ClosureCheckedEnvelope::Enrollment(enrollment_envelope())),
        )),
        // Row 5650: EnrollBound success.
        EnrollmentResponse::enroll_bound(enroll_bound()),
        // Row 5651: EnrollmentKnown post-provenance replay.
        EnrollmentResponse::enrollment_known(EnrollmentKnown {
            conversation_id: 7,
            token: EnrollmentToken::new([1; 16]),
            participant_id: 3,
            current_generation: generation(2),
        }),
        // Row 5652: exact enrollment provenance-window expiry.
        EnrollmentResponse::from_receipt_expired(ReceiptExpired::Enrollment {
            conversation_id: 7,
            token: EnrollmentToken::new([1; 16]),
            participant_id: 3,
            result_generation: generation(1),
            current_generation: generation(2),
            reason: super::ReceiptExpiryReason::Deadline,
        }),
        // Row 5653: enrollment token mapping resolved to a tombstone.
        EnrollmentResponse::from_retired(Retired::Enrollment {
            request: enrollment_envelope(),
            participant_id: 3,
            retired_generation: generation(2),
        }),
        // Row 5654: reachable receipt/provenance scope full.
        EnrollmentResponse::receipt_capacity_exceeded(
            enrollment_envelope(),
            EnrollmentReceiptCapacityScope::LiveReceiptServer,
            8,
            8,
        ),
        // Row 5655: identity capacity full, server scope before conversation.
        EnrollmentResponse::identity_capacity_exceeded(IdentityCapacityExceeded {
            request: enrollment_envelope(),
            scope: IdentityCapacityScope::Server,
            limit: 8,
            occupied: 8,
        }),
        // Row 5656: observer backpressure with the common envelope.
        EnrollmentResponse::observer_backpressure(
            enrollment_envelope(),
            ObserverBackpressureState::initial(5),
        ),
        // Row 5657: canonical sequence-reserve check failed, shared-allocator
        // payload with the enrollment envelope arm.
        EnrollmentResponse::from_conversation_sequence_exhausted(Box::new(sequence_exhausted(
            SequenceAllocatingEnvelope::Enrollment(enrollment_envelope()),
        ))),
        // Row 5663: Bound / UnboundReceipt byte-identical replay.
        EnrollmentResponse::bound(enroll_bound()),
        EnrollmentResponse::unbound_receipt(enroll_bound()),
    ];
    for response in responses {
        assert_bound(
            response.server_value(),
            ClientDiscriminant::EnrollmentRequest,
        );
    }
}

/// Credential-attach legal set: register rows 5639, 5641, 5643, 5644, 5645,
/// 5647-5649, and 5658-5663.
#[test]
fn credential_attach_constructors_stay_inside_the_register_rows() {
    let responses = vec![
        // Row 5639: verified receipt with changed non-secret body.
        CredentialAttachResponse::attempt_token_body_conflict(
            &attach_envelope(),
            AttemptConflict::Generation,
        ),
        // Row 5641: connection-conversation capacity.
        CredentialAttachResponse::connection_conversation_capacity_exceeded(attach_envelope(), 4),
        // Row 5643: occupied binding slot.
        CredentialAttachResponse::connection_conversation_binding_occupied(&attach_envelope()),
        // Row 5644: conversation order exhausted.
        CredentialAttachResponse::conversation_order_exhausted(attach_envelope(), 9, 1, 4, 0, 4),
        // Row 5645: unknown participant.
        CredentialAttachResponse::participant_unknown(attach_envelope()),
        // Rows 5647/5660/5665: live stale authority.
        CredentialAttachResponse::stale_authority(attach_envelope(), generation(3)),
        // Rows 5648/5659/5667: tombstone.
        CredentialAttachResponse::retired(attach_envelope(), generation(3)),
        // Rows 5649/5662: marker-closure capacity.
        CredentialAttachResponse::marker_closure_capacity_exceeded(
            attach_envelope(),
            closure_snapshot(),
            ClosureRefusalReason::DeliveredMarkerAwaitingAck,
        ),
        // Row 5658: AttachBound success.
        CredentialAttachResponse::attach_bound(attach_bound()),
        // Rows 5659/5664: provenance-window receipt expiry.
        CredentialAttachResponse::receipt_expired(
            &attach_envelope(),
            generation(2),
            generation(3),
            super::ReceiptExpiryReason::Deadline,
        ),
        // Rows 5659/5666: post-provenance ambiguity.
        CredentialAttachResponse::stale_or_unknown_receipt(StaleOrUnknownReceipt {
            conversation_id: 7,
            token: super::AttachAttemptToken::new([2; 16]),
            participant_id: 3,
            presented_generation: generation(2),
            presented_marker_delivery_seq: None,
            current_generation: generation(3),
        }),
        // Row 5661: fenced-attach marker proofs.
        CredentialAttachResponse::marker_not_delivered(
            attach_marker_proof(),
            MarkerNotDeliveredReason::NotDeliveredToProofEpoch,
            12,
        ),
        CredentialAttachResponse::marker_mismatch(
            attach_marker_proof(),
            MarkerMismatchBody::NoMarkerExpected,
        ),
        // Row 5662: receipt capacity, observer backpressure, and sequence
        // exhaustion with the common envelope.
        CredentialAttachResponse::receipt_capacity_exceeded(
            attach_envelope(),
            super::ReceiptCapacityScope::LiveReceiptParticipant,
            8,
            8,
        ),
        CredentialAttachResponse::observer_backpressure(
            attach_envelope(),
            ObserverBackpressureState::initial(5),
        ),
        CredentialAttachResponse::conversation_sequence_exhausted(
            attach_envelope(),
            sequence_budget(),
        ),
        // Row 5663: Bound / UnboundReceipt byte-identical replay.
        CredentialAttachResponse::bound(attach_bound()),
        CredentialAttachResponse::unbound_receipt(attach_bound()),
    ];
    for response in responses {
        assert_bound(
            response.server_value(),
            ClientDiscriminant::CredentialAttachRequest,
        );
    }
}

/// Detach legal set: register rows 5641, 5645, 5646, 5647, 5648, and
/// 5668-5673.
#[test]
fn detach_constructors_stay_inside_the_register_rows() {
    let responses = vec![
        // Row 5641: connection-conversation capacity.
        DetachResponse::connection_conversation_capacity_exceeded(detach_envelope(), 4),
        // Row 5645: unknown participant.
        DetachResponse::participant_unknown(detach_envelope()),
        // Row 5646: no current binding and no Pending cell.
        DetachResponse::no_binding(detach_envelope()),
        // Rows 5647/5671: live stale authority.
        DetachResponse::stale_authority(DetachStaleAuthority::Live {
            conversation_id: 7,
            participant_id: 3,
            capability_generation: generation(2),
            detach_attempt_token: super::DetachAttemptToken::new([3; 16]),
            current_generation: generation(3),
        }),
        // Row 5671: verified exact old token resolved to the terminalized
        // detach cell retained by a later attach.
        DetachResponse::stale_authority(DetachStaleAuthority::TerminalizedDetachCell(
            terminalized_detach_cell(),
        )),
        // Rows 5648/5672: tombstone after Leave.
        DetachResponse::retired(detach_envelope(), generation(3)),
        // Row 5668: committed detach.
        DetachResponse::detach_committed(DetachCommitted::new(
            7,
            3,
            super::DetachAttemptToken::new([3; 16]),
            epoch(2),
            21,
        )),
        // Row 5670: competing token against a Pending cell.
        DetachResponse::detach_in_progress(DetachInProgress {
            conversation_id: 7,
            participant_id: 3,
            presented_token: super::DetachAttemptToken::new([9; 16]),
            presented_generation: generation(2),
            committed_binding_epoch: epoch(2),
        }),
        // Rows 5669/5673: blocked append or exact-token Pending replay.
        DetachResponse::observer_backpressure(
            detach_envelope(),
            epoch(2),
            ObserverBackpressureState::initial(5),
        ),
    ];
    for response in responses {
        assert_bound(response.server_value(), ClientDiscriminant::DetachRequest);
    }
}

/// Normal-ack legal set: register rows 5641, 5645, 5646, 5647, and
/// 5674-5677. Rows 5699-5701 exclude backpressure, closure, and order
/// outcomes; the reachability rule at lines 5691-5696 separately excludes
/// sequence exhaustion: no such constructor exists.
#[test]
fn participant_ack_constructors_stay_inside_the_register_rows() {
    let responses = vec![
        // Row 5641: connection-conversation capacity.
        ParticipantAckResponse::connection_conversation_capacity_exceeded(
            participant_ack_envelope(),
            4,
        ),
        // Row 5645: unknown participant, binding-lookup payload with this
        // request's envelope arm.
        ParticipantAckResponse::from_participant_unknown(ParticipantUnknown {
            request: ParticipantReferenceEnvelope::ParticipantAck(participant_ack_envelope()),
        }),
        // Row 5646: exact-binding lookup missed.
        ParticipantAckResponse::from_no_binding(NoBinding {
            request: BindingRequiredEnvelope::ParticipantAck(participant_ack_envelope()),
        }),
        // Row 5647: live stale authority with the common envelope.
        ParticipantAckResponse::from_stale_authority(StaleAuthority::Live {
            request: CommonStaleAuthorityEnvelope::ParticipantAck(participant_ack_envelope()),
            current_generation: generation(3),
        }),
        // Row 5674: committed cumulative ack.
        ParticipantAckResponse::ack_committed(AckCommitted::new(participant_ack_envelope())),
        // Row 5675: idempotent no-op.
        ParticipantAckResponse::ack_no_op(participant_ack_envelope()),
        // Row 5676: gap and regression.
        ParticipantAckResponse::ack_gap(
            AckGap::new(participant_ack_envelope(), 4).expect("through_seq 9 above cursor 4"),
        ),
        ParticipantAckResponse::ack_regression(
            AckRegression::new(participant_ack_envelope(), 12)
                .expect("through_seq 9 below cursor 12"),
        ),
        // Row 5677: presented id has a tombstone.
        ParticipantAckResponse::from_retired(Retired::Participant {
            request: ParticipantReferenceEnvelope::ParticipantAck(participant_ack_envelope()),
            retired_generation: generation(3),
        }),
    ];
    for response in responses {
        assert_bound(response.server_value(), ClientDiscriminant::ParticipantAck);
    }
}

/// Leave legal set: register rows 5639, 5641, 5645, 5646, 5647, 5649, and
/// 5678-5681.
#[test]
fn leave_constructors_stay_inside_the_register_rows() {
    let responses = vec![
        // Row 5639: Leave token body conflict (generation only).
        LeaveResponse::attempt_token_body_conflict(
            super::LeaveAttemptToken::new([4; 16]),
            7,
            3,
            generation(2),
        ),
        // Row 5641: connection-conversation capacity.
        LeaveResponse::connection_conversation_capacity_exceeded(leave_envelope(), 4),
        // Row 5645: unknown participant.
        LeaveResponse::participant_unknown(leave_envelope()),
        // Row 5646: different live binding epoch exists.
        LeaveResponse::no_binding(leave_envelope()),
        // Rows 5647/5680: Leave-specific stale authority.
        LeaveResponse::stale_authority(LeaveStaleAuthority::Live {
            conversation_id: 7,
            participant_id: 3,
            presented_generation: generation(2),
            leave_attempt_token: super::LeaveAttemptToken::new([4; 16]),
            current_generation: generation(3),
        }),
        // Rows 5648/5680: tombstone under a different token.
        LeaveResponse::retired(leave_envelope(), generation(3)),
        // Row 5649: marker-closure capacity.
        LeaveResponse::marker_closure_capacity_exceeded(
            leave_envelope(),
            closure_snapshot(),
            ClosureRefusalReason::DeliveredMarkerAwaitingAck,
        ),
        // Rows 5678/5679: terminal Leave success.
        LeaveResponse::leave_committed(
            LeaveCommitted::new(
                7,
                super::LeaveAttemptToken::new([4; 16]),
                3,
                generation(2),
                Some(epoch(2)),
                None,
                33,
            )
            .expect("matching generation and ordered terminals"),
        ),
        // Row 5681: observer backpressure with the prior-terminal flag.
        LeaveResponse::observer_backpressure(
            leave_envelope(),
            ObserverBackpressureState::initial(5),
            true,
        ),
    ];
    for response in responses {
        assert_bound(response.server_value(), ClientDiscriminant::LeaveRequest);
    }
}

/// Marker-ack legal set: register rows 5641, 5645, 5646, 5647, and
/// 5682-5684. Rows 5699-5701 exclude backpressure, closure, and order
/// outcomes; the reachability rule at lines 5691-5696 separately excludes
/// sequence exhaustion: no such constructor exists.
#[test]
fn marker_ack_constructors_stay_inside_the_register_rows() {
    let responses = vec![
        // Row 5641: connection-conversation capacity.
        MarkerAckResponse::connection_conversation_capacity_exceeded(marker_ack_envelope(), 4),
        // Row 5645: unknown participant, binding-lookup payload with this
        // request's envelope arm.
        MarkerAckResponse::from_participant_unknown(ParticipantUnknown {
            request: ParticipantReferenceEnvelope::MarkerAck(marker_ack_envelope()),
        }),
        // Row 5646: exact-binding lookup missed.
        MarkerAckResponse::from_no_binding(NoBinding {
            request: BindingRequiredEnvelope::MarkerAck(marker_ack_envelope()),
        }),
        // Row 5647: live stale authority with the common envelope.
        MarkerAckResponse::from_stale_authority(StaleAuthority::Live {
            request: CommonStaleAuthorityEnvelope::MarkerAck(marker_ack_envelope()),
            current_generation: generation(3),
        }),
        // Row 5682: committed marker ack.
        MarkerAckResponse::marker_ack_committed(MarkerAckCommitted::new(marker_ack_envelope())),
        // Row 5682: idempotent no-op at the unchanged marker cursor.
        MarkerAckResponse::from_ack_no_op(AckNoOp::marker_ack(marker_ack_envelope())),
        // Row 5683: marker-proof refusals minted with this request's own
        // proof fields.
        MarkerAckResponse::from_marker_not_delivered(MarkerNotDelivered {
            request: MarkerProofRequest::MarkerAck(marker_ack_proof()),
            reason: MarkerNotDeliveredReason::NotDeliveredToProofEpoch,
            expected_marker_delivery_seq: 12,
        }),
        MarkerAckResponse::from_marker_mismatch(MarkerMismatch {
            request: MarkerProofRequest::MarkerAck(marker_ack_proof()),
            mismatch: MarkerMismatchBody::NoMarkerExpected,
        }),
        // Row 5684: presented id has a tombstone.
        MarkerAckResponse::from_retired(Retired::Participant {
            request: ParticipantReferenceEnvelope::MarkerAck(marker_ack_envelope()),
            retired_generation: generation(3),
        }),
    ];
    for response in responses {
        assert_bound(response.server_value(), ClientDiscriminant::MarkerAck);
    }
}

/// Ordinary-admission legal set: register rows 5641, 5644, 5645, 5646, 5647,
/// 5648, 5649, and 5685-5687.
#[test]
fn record_admission_constructors_stay_inside_the_register_rows() {
    let responses = vec![
        // Row 5641: connection-conversation capacity.
        RecordAdmissionResponse::connection_conversation_capacity_exceeded(record_envelope(), 4),
        // Row 5644: conversation order exhausted, shared-allocator payload
        // with the record-admission envelope arm.
        RecordAdmissionResponse::from_conversation_order_exhausted(Box::new(order_exhausted(
            OrderAllocatingEnvelope::RecordAdmission(record_envelope()),
        ))),
        // Row 5645: unknown participant, binding-lookup payload with this
        // request's envelope arm.
        RecordAdmissionResponse::from_participant_unknown(ParticipantUnknown {
            request: ParticipantReferenceEnvelope::RecordAdmission(record_envelope()),
        }),
        // Row 5646: exact-binding lookup missed.
        RecordAdmissionResponse::from_no_binding(NoBinding {
            request: BindingRequiredEnvelope::RecordAdmission(record_envelope()),
        }),
        // Row 5647: live stale authority with the common envelope.
        RecordAdmissionResponse::from_stale_authority(StaleAuthority::Live {
            request: CommonStaleAuthorityEnvelope::RecordAdmission(record_envelope()),
            current_generation: generation(3),
        }),
        // Row 5648: presented id has a tombstone.
        RecordAdmissionResponse::from_retired(Retired::Participant {
            request: ParticipantReferenceEnvelope::RecordAdmission(record_envelope()),
            retired_generation: generation(3),
        }),
        // Rows 5649/5686: marker-closure capacity, shared-selector payload
        // with the record-admission envelope arm.
        RecordAdmissionResponse::from_marker_closure_capacity_exceeded(Box::new(
            closure_capacity_exceeded(ClosureCheckedEnvelope::RecordAdmission(record_envelope())),
        )),
        // Row 5685: committed ordinary record.
        RecordAdmissionResponse::record_committed(RecordCommitted::new(record_envelope(), 44)),
        // Row 5686: static size refusal, Entries before Bytes.
        RecordAdmissionResponse::record_too_large(RecordTooLarge {
            request: record_envelope(),
            dimension: ResourceDimension::Entries,
            encoded_record_charge: ResourceVector::new(2, 10),
            max_ordinary_record_charge: ResourceVector::new(1, 100),
        }),
        // Row 5686: canonical sequence-reserve check failed, shared-allocator
        // payload with the record-admission envelope arm.
        RecordAdmissionResponse::from_conversation_sequence_exhausted(Box::new(
            sequence_exhausted(SequenceAllocatingEnvelope::RecordAdmission(
                record_envelope(),
            )),
        )),
        // Row 5687: hard-observer retention refused the ordinary append.
        RecordAdmissionResponse::from_observer_backpressure(
            ObserverBackpressure::RecordAdmission {
                request: record_envelope(),
                state: ObserverBackpressureState::initial(5),
            },
        ),
    ];
    for response in responses {
        assert_bound(response.server_value(), ClientDiscriminant::RecordAdmission);
    }
}

/// Observer-recovery legal set: register rows 5642, 5688, and 5689. The
/// contract's routing rule (lines 5780-5782) marks these outcomes as
/// request-specific without an `originating_request` echo, so the structural
/// selector is `None` while the wire discriminants stay inside the four
/// recovery values.
#[test]
fn observer_recovery_constructors_stay_inside_the_register_rows() {
    let responses = [
        // Row 5642: batch preflight connection capacity (`0x0124`).
        ObserverRecoveryResponse::connection_capacity_exceeded(7, 4),
        // Row 5688: whole-batch success.
        ObserverRecoveryResponse::accepted(ObserverRecoveryAccepted {
            statuses: Vec::new(),
        }),
        // Row 5689: whole-batch epoch and list refusals.
        ObserverRecoveryResponse::invalid_observer_epoch(
            InvalidObserverEpoch::ConversationUnknown {
                conversation_id: 7,
                presented_epoch: 5,
            },
        ),
        ObserverRecoveryResponse::invalid_observer_epoch_list(
            InvalidObserverEpochList::TooManyEntries {
                presented_entries: 9,
                max_entries: 8,
            },
        ),
    ];
    let expected = [
        ServerDiscriminant::ObserverRecoveryConnectionCapacityExceeded,
        ServerDiscriminant::ObserverRecoveryAccepted,
        ServerDiscriminant::InvalidObserverEpoch,
        ServerDiscriminant::InvalidObserverEpochList,
    ];
    for (response, discriminant) in responses.iter().zip(expected) {
        assert_eq!(response.server_value().originating_request(), None);
        assert_eq!(response.discriminant(), discriminant);
    }
}

/// The `into_server_value` transfer moves the bound value without cloning and
/// preserves the exact wire payload observed through the borrow.
#[test]
fn bound_values_move_out_intact() {
    let response =
        RecordAdmissionResponse::record_committed(RecordCommitted::new(record_envelope(), 44));
    let observed = response.server_value().clone();
    assert_eq!(response.discriminant(), ServerDiscriminant::RecordCommitted);
    assert_eq!(response.into_server_value(), observed);
}

/// Detach responses may carry only detach-shaped payloads: the no-binding
/// constructor embeds the detach envelope arm by construction.
#[test]
fn narrow_constructors_embed_the_exact_origin_arm() {
    let response = DetachResponse::no_binding(detach_envelope());
    let ServerValue::NoBinding(no_binding) = response.server_value() else {
        panic!("no-binding constructor selects the no-binding wire value");
    };
    assert_eq!(
        no_binding.request,
        BindingRequiredEnvelope::Detach(detach_envelope()),
    );
}
