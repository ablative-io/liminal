//! Detach, normal-ack, Leave, and marker-ack legal-outcome matrices.
//!
//! Register citations follow the parent module's transcription of the frozen
//! R-D1 register; every constructed response is asserted against both its
//! structural `originating_request` echo and its exact wire discriminant.

use alloc::vec;

use super::support::{
    assert_bound, closure_snapshot, detach_envelope, epoch, generation, leave_envelope,
    marker_ack_envelope, marker_ack_proof, participant_ack_envelope, terminalized_detach_cell,
};

use super::super::{
    AckCommitted, AckGap, AckNoOp, AckRegression, BindingRequiredEnvelope, ClientDiscriminant,
    ClosureRefusalReason, CommonStaleAuthorityEnvelope, DetachCommitted, DetachInProgress,
    DetachResponse, DetachStaleAuthority, LeaveCommitted, LeaveResponse, LeaveStaleAuthority,
    MarkerAckCommitted, MarkerAckResponse, MarkerMismatch, MarkerMismatchBody, MarkerNotDelivered,
    MarkerNotDeliveredReason, MarkerProofRequest, NoBinding, ObserverBackpressureState,
    ParticipantAckResponse, ParticipantReferenceEnvelope, ParticipantUnknown, Retired,
    ServerDiscriminant, ServerValue, StaleAuthority,
};

/// Detach legal set: register rows 5641, 5645, 5646, 5647, 5648, and
/// 5668-5673.
#[test]
fn detach_constructors_stay_inside_the_register_rows() {
    let responses = vec![
        // Row 5641: connection-conversation capacity.
        (
            DetachResponse::connection_conversation_capacity_exceeded(detach_envelope(), 4),
            ServerDiscriminant::ConnectionConversationCapacityExceeded,
        ),
        // Row 5645: unknown participant.
        (
            DetachResponse::participant_unknown(detach_envelope()),
            ServerDiscriminant::ParticipantUnknown,
        ),
        // Row 5646: no current binding and no Pending cell.
        (
            DetachResponse::no_binding(detach_envelope()),
            ServerDiscriminant::NoBinding,
        ),
        // Rows 5647/5671: live stale authority.
        (
            DetachResponse::stale_authority(DetachStaleAuthority::Live {
                conversation_id: 7,
                participant_id: 3,
                capability_generation: generation(2),
                detach_attempt_token: super::super::DetachAttemptToken::new([3; 16]),
                current_generation: generation(3),
            }),
            ServerDiscriminant::StaleAuthority,
        ),
        // Row 5671: verified exact old token resolved to the terminalized
        // detach cell retained by a later attach.
        (
            DetachResponse::stale_authority(DetachStaleAuthority::TerminalizedDetachCell(
                terminalized_detach_cell(),
            )),
            ServerDiscriminant::StaleAuthority,
        ),
        // Rows 5648/5672: tombstone after Leave.
        (
            DetachResponse::retired(detach_envelope(), generation(3)),
            ServerDiscriminant::Retired,
        ),
        // Row 5668: committed detach.
        (
            DetachResponse::detach_committed(DetachCommitted::new(
                7,
                3,
                super::super::DetachAttemptToken::new([3; 16]),
                epoch(2),
                21,
            )),
            ServerDiscriminant::DetachCommitted,
        ),
        // Row 5670: competing token against a Pending cell.
        (
            DetachResponse::detach_in_progress(DetachInProgress {
                conversation_id: 7,
                participant_id: 3,
                presented_token: super::super::DetachAttemptToken::new([9; 16]),
                presented_generation: generation(2),
                committed_binding_epoch: epoch(2),
            }),
            ServerDiscriminant::DetachInProgress,
        ),
        // Rows 5669/5673: blocked append or exact-token Pending replay.
        (
            DetachResponse::observer_backpressure(
                detach_envelope(),
                epoch(2),
                ObserverBackpressureState::initial(5),
            ),
            ServerDiscriminant::ObserverBackpressure,
        ),
    ];
    for (response, discriminant) in responses {
        assert_bound(
            response.server_value(),
            ClientDiscriminant::DetachRequest,
            discriminant,
        );
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
        (
            ParticipantAckResponse::connection_conversation_capacity_exceeded(
                participant_ack_envelope(),
                4,
            ),
            ServerDiscriminant::ConnectionConversationCapacityExceeded,
        ),
        // Row 5645: unknown participant, binding-lookup payload with this
        // request's envelope arm.
        (
            ParticipantAckResponse::from_participant_unknown(ParticipantUnknown {
                request: ParticipantReferenceEnvelope::ParticipantAck(participant_ack_envelope()),
            }),
            ServerDiscriminant::ParticipantUnknown,
        ),
        // Row 5646: exact-binding lookup missed.
        (
            ParticipantAckResponse::from_no_binding(NoBinding {
                request: BindingRequiredEnvelope::ParticipantAck(participant_ack_envelope()),
            }),
            ServerDiscriminant::NoBinding,
        ),
        // Row 5647: live stale authority with the common envelope.
        (
            ParticipantAckResponse::from_stale_authority(StaleAuthority::Live {
                request: CommonStaleAuthorityEnvelope::ParticipantAck(participant_ack_envelope()),
                current_generation: generation(3),
            }),
            ServerDiscriminant::StaleAuthority,
        ),
        // Row 5674: committed cumulative ack.
        (
            ParticipantAckResponse::ack_committed(AckCommitted::new(participant_ack_envelope())),
            ServerDiscriminant::AckCommitted,
        ),
        // Row 5675: idempotent no-op.
        (
            ParticipantAckResponse::ack_no_op(participant_ack_envelope()),
            ServerDiscriminant::AckNoOp,
        ),
        // Row 5676: gap and regression.
        (
            ParticipantAckResponse::ack_gap(
                AckGap::new(participant_ack_envelope(), 4).expect("through_seq 9 above cursor 4"),
            ),
            ServerDiscriminant::AckGap,
        ),
        (
            ParticipantAckResponse::ack_regression(
                AckRegression::new(participant_ack_envelope(), 12)
                    .expect("through_seq 9 below cursor 12"),
            ),
            ServerDiscriminant::AckRegression,
        ),
        // Row 5677: presented id has a tombstone.
        (
            ParticipantAckResponse::from_retired(Retired::Participant {
                request: ParticipantReferenceEnvelope::ParticipantAck(participant_ack_envelope()),
                retired_generation: generation(3),
            }),
            ServerDiscriminant::Retired,
        ),
    ];
    for (response, discriminant) in responses {
        assert_bound(
            response.server_value(),
            ClientDiscriminant::ParticipantAck,
            discriminant,
        );
    }
}

/// Leave legal set: register rows 5639, 5641, 5645, 5646, 5647, 5649, and
/// 5678-5681.
#[test]
fn leave_constructors_stay_inside_the_register_rows() {
    let responses = vec![
        // Row 5639: Leave token body conflict (generation only).
        (
            LeaveResponse::attempt_token_body_conflict(
                super::super::LeaveAttemptToken::new([4; 16]),
                7,
                3,
                generation(2),
            ),
            ServerDiscriminant::AttemptTokenBodyConflict,
        ),
        // Row 5641: connection-conversation capacity.
        (
            LeaveResponse::connection_conversation_capacity_exceeded(leave_envelope(), 4),
            ServerDiscriminant::ConnectionConversationCapacityExceeded,
        ),
        // Row 5645: unknown participant.
        (
            LeaveResponse::participant_unknown(leave_envelope()),
            ServerDiscriminant::ParticipantUnknown,
        ),
        // Row 5646: different live binding epoch exists.
        (
            LeaveResponse::no_binding(leave_envelope()),
            ServerDiscriminant::NoBinding,
        ),
        // Rows 5647/5680: Leave-specific stale authority.
        (
            LeaveResponse::stale_authority(LeaveStaleAuthority::Live {
                conversation_id: 7,
                participant_id: 3,
                presented_generation: generation(2),
                leave_attempt_token: super::super::LeaveAttemptToken::new([4; 16]),
                current_generation: generation(3),
            }),
            ServerDiscriminant::StaleAuthority,
        ),
        // Rows 5648/5680: tombstone under a different token.
        (
            LeaveResponse::retired(leave_envelope(), generation(3)),
            ServerDiscriminant::Retired,
        ),
        // Row 5649: marker-closure capacity.
        (
            LeaveResponse::marker_closure_capacity_exceeded(
                leave_envelope(),
                closure_snapshot(),
                ClosureRefusalReason::DeliveredMarkerAwaitingAck,
            ),
            ServerDiscriminant::MarkerClosureCapacityExceeded,
        ),
        // Rows 5678/5679: terminal Leave success.
        (
            LeaveResponse::leave_committed(
                LeaveCommitted::new(
                    7,
                    super::super::LeaveAttemptToken::new([4; 16]),
                    3,
                    generation(2),
                    Some(epoch(2)),
                    None,
                    33,
                )
                .expect("matching generation and ordered terminals"),
            ),
            ServerDiscriminant::LeaveCommitted,
        ),
        // Row 5681: observer backpressure with the prior-terminal flag.
        (
            LeaveResponse::observer_backpressure(
                leave_envelope(),
                ObserverBackpressureState::initial(5),
                true,
            ),
            ServerDiscriminant::ObserverBackpressure,
        ),
    ];
    for (response, discriminant) in responses {
        assert_bound(
            response.server_value(),
            ClientDiscriminant::LeaveRequest,
            discriminant,
        );
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
        (
            MarkerAckResponse::connection_conversation_capacity_exceeded(marker_ack_envelope(), 4),
            ServerDiscriminant::ConnectionConversationCapacityExceeded,
        ),
        // Row 5645: unknown participant, binding-lookup payload with this
        // request's envelope arm.
        (
            MarkerAckResponse::from_participant_unknown(ParticipantUnknown {
                request: ParticipantReferenceEnvelope::MarkerAck(marker_ack_envelope()),
            }),
            ServerDiscriminant::ParticipantUnknown,
        ),
        // Row 5646: exact-binding lookup missed.
        (
            MarkerAckResponse::from_no_binding(NoBinding {
                request: BindingRequiredEnvelope::MarkerAck(marker_ack_envelope()),
            }),
            ServerDiscriminant::NoBinding,
        ),
        // Row 5647: live stale authority with the common envelope.
        (
            MarkerAckResponse::from_stale_authority(StaleAuthority::Live {
                request: CommonStaleAuthorityEnvelope::MarkerAck(marker_ack_envelope()),
                current_generation: generation(3),
            }),
            ServerDiscriminant::StaleAuthority,
        ),
        // Row 5682: committed marker ack.
        (
            MarkerAckResponse::marker_ack_committed(MarkerAckCommitted::new(marker_ack_envelope())),
            ServerDiscriminant::MarkerAckCommitted,
        ),
        // Row 5682: idempotent no-op at the unchanged marker cursor.
        (
            MarkerAckResponse::from_ack_no_op(AckNoOp::marker_ack(marker_ack_envelope())),
            ServerDiscriminant::AckNoOp,
        ),
        // Row 5683: marker-proof refusals minted with this request's own
        // proof fields.
        (
            MarkerAckResponse::from_marker_not_delivered(MarkerNotDelivered {
                request: MarkerProofRequest::MarkerAck(marker_ack_proof()),
                reason: MarkerNotDeliveredReason::NotDeliveredToProofEpoch,
                expected_marker_delivery_seq: 12,
            }),
            ServerDiscriminant::MarkerNotDelivered,
        ),
        (
            MarkerAckResponse::from_marker_mismatch(MarkerMismatch {
                request: MarkerProofRequest::MarkerAck(marker_ack_proof()),
                mismatch: MarkerMismatchBody::NoMarkerExpected,
            }),
            ServerDiscriminant::MarkerMismatch,
        ),
        // Row 5684: presented id has a tombstone.
        (
            MarkerAckResponse::from_retired(Retired::Participant {
                request: ParticipantReferenceEnvelope::MarkerAck(marker_ack_envelope()),
                retired_generation: generation(3),
            }),
            ServerDiscriminant::Retired,
        ),
    ];
    for (response, discriminant) in responses {
        assert_bound(
            response.server_value(),
            ClientDiscriminant::MarkerAck,
            discriminant,
        );
    }
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
