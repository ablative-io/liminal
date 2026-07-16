//! Record-admission and observer-recovery legal-outcome matrices.
//!
//! Register citations follow the parent module's transcription of the frozen
//! R-D1 register; every constructed response is asserted against both its
//! structural `originating_request` echo (where the register mandates one)
//! and its exact wire discriminant.

use alloc::{boxed::Box, vec, vec::Vec};

use super::support::{
    assert_bound, closure_capacity_exceeded, generation, order_exhausted, record_envelope,
    sequence_exhausted,
};

use crate::algebra::{ResourceDimension, ResourceVector};

use super::super::{
    BindingRequiredEnvelope, ClientDiscriminant, ClosureCheckedEnvelope,
    CommonStaleAuthorityEnvelope, InvalidObserverEpoch, InvalidObserverEpochList, NoBinding,
    ObserverBackpressure, ObserverBackpressureState, ObserverRecoveryAccepted,
    ObserverRecoveryResponse, OrderAllocatingEnvelope, ParticipantReferenceEnvelope,
    ParticipantUnknown, RecordAdmissionResponse, RecordCommitted, RecordTooLarge, Retired,
    SequenceAllocatingEnvelope, ServerDiscriminant, StaleAuthority,
};

/// Ordinary-admission legal set: register rows 5641, 5644, 5645, 5646, 5647,
/// 5648, 5649, and 5685-5687.
#[test]
fn record_admission_constructors_stay_inside_the_register_rows() {
    let responses = vec![
        // Row 5641: connection-conversation capacity.
        (
            RecordAdmissionResponse::connection_conversation_capacity_exceeded(
                record_envelope(),
                4,
            ),
            ServerDiscriminant::ConnectionConversationCapacityExceeded,
        ),
        // Row 5644: conversation order exhausted, shared-allocator payload
        // with the record-admission envelope arm.
        (
            RecordAdmissionResponse::from_conversation_order_exhausted(Box::new(order_exhausted(
                OrderAllocatingEnvelope::RecordAdmission(record_envelope()),
            ))),
            ServerDiscriminant::ConversationOrderExhausted,
        ),
        // Row 5645: unknown participant, binding-lookup payload with this
        // request's envelope arm.
        (
            RecordAdmissionResponse::from_participant_unknown(ParticipantUnknown {
                request: ParticipantReferenceEnvelope::RecordAdmission(record_envelope()),
            }),
            ServerDiscriminant::ParticipantUnknown,
        ),
        // Row 5646: exact-binding lookup missed.
        (
            RecordAdmissionResponse::from_no_binding(NoBinding {
                request: BindingRequiredEnvelope::RecordAdmission(record_envelope()),
            }),
            ServerDiscriminant::NoBinding,
        ),
        // Row 5647: live stale authority with the common envelope.
        (
            RecordAdmissionResponse::from_stale_authority(StaleAuthority::Live {
                request: CommonStaleAuthorityEnvelope::RecordAdmission(record_envelope()),
                current_generation: generation(3),
            }),
            ServerDiscriminant::StaleAuthority,
        ),
        // Row 5648: presented id has a tombstone.
        (
            RecordAdmissionResponse::from_retired(Retired::Participant {
                request: ParticipantReferenceEnvelope::RecordAdmission(record_envelope()),
                retired_generation: generation(3),
            }),
            ServerDiscriminant::Retired,
        ),
        // Rows 5649/5686: marker-closure capacity, shared-selector payload
        // with the record-admission envelope arm.
        (
            RecordAdmissionResponse::from_marker_closure_capacity_exceeded(Box::new(
                closure_capacity_exceeded(ClosureCheckedEnvelope::RecordAdmission(
                    record_envelope(),
                )),
            )),
            ServerDiscriminant::MarkerClosureCapacityExceeded,
        ),
        // Row 5685: committed ordinary record.
        (
            RecordAdmissionResponse::record_committed(RecordCommitted::new(record_envelope(), 44)),
            ServerDiscriminant::RecordCommitted,
        ),
        // Row 5686: static size refusal, Entries before Bytes.
        (
            RecordAdmissionResponse::record_too_large(RecordTooLarge {
                request: record_envelope(),
                dimension: ResourceDimension::Entries,
                encoded_record_charge: ResourceVector::new(2, 10),
                max_ordinary_record_charge: ResourceVector::new(1, 100),
            }),
            ServerDiscriminant::RecordTooLarge,
        ),
        // Row 5686: canonical sequence-reserve check failed, shared-allocator
        // payload with the record-admission envelope arm.
        (
            RecordAdmissionResponse::from_conversation_sequence_exhausted(Box::new(
                sequence_exhausted(SequenceAllocatingEnvelope::RecordAdmission(
                    record_envelope(),
                )),
            )),
            ServerDiscriminant::ConversationSequenceExhausted,
        ),
        // Row 5687: hard-observer retention refused the ordinary append.
        (
            RecordAdmissionResponse::from_observer_backpressure(
                ObserverBackpressure::RecordAdmission {
                    request: record_envelope(),
                    state: ObserverBackpressureState::initial(5),
                },
            ),
            ServerDiscriminant::ObserverBackpressure,
        ),
    ];
    for (response, discriminant) in responses {
        assert_bound(
            response.server_value(),
            ClientDiscriminant::RecordAdmission,
            discriminant,
        );
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
