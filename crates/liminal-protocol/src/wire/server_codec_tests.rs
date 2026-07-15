use alloc::{boxed::Box, vec, vec::Vec};

use crate::algebra::{ResourceDimension, ResourceVector, WideResourceVector};

use super::{
    AttachAttemptToken, AttachSecret, BindingEpoch, CodecError, ConnectionIncarnation,
    DetachAttemptToken, EnrollmentToken, Generation, LeaveAttemptToken, ParticipantFrame,
    ProtocolVersion, ReceiverDirection, closure as c, envelope as e, response as r, server_codec,
    tags as t,
};

fn generation(value: u64) -> Result<Generation, CodecError> {
    Generation::new(value).ok_or(CodecError::InvalidValue)
}

fn epoch(seed: u64, generation: Generation) -> BindingEpoch {
    BindingEpoch::new(ConnectionIncarnation::new(seed, seed + 1), generation)
}

fn enrollment() -> e::EnrollmentEnvelope {
    e::EnrollmentEnvelope {
        conversation_id: 10,
        enrollment_token: EnrollmentToken::new([1; 16]),
    }
}

fn attach(generation: Generation) -> e::AttachEnvelope {
    e::AttachEnvelope {
        conversation_id: 10,
        participant_id: 20,
        capability_generation: generation,
        attach_attempt_token: AttachAttemptToken::new([2; 16]),
        accept_marker_delivery_seq: Some(30),
    }
}

fn detach(generation: Generation) -> e::DetachEnvelope {
    e::DetachEnvelope {
        conversation_id: 10,
        participant_id: 20,
        capability_generation: generation,
        detach_attempt_token: DetachAttemptToken::new([3; 16]),
    }
}

fn participant_ack(generation: Generation) -> e::ParticipantAckEnvelope {
    e::ParticipantAckEnvelope {
        conversation_id: 10,
        participant_id: 20,
        capability_generation: generation,
        through_seq: 31,
    }
}

fn marker_ack(generation: Generation) -> e::MarkerAckEnvelope {
    e::MarkerAckEnvelope {
        conversation_id: 10,
        participant_id: 20,
        capability_generation: generation,
        marker_delivery_seq: 32,
    }
}

fn record(generation: Generation) -> e::RecordAdmissionEnvelope {
    e::RecordAdmissionEnvelope {
        conversation_id: 10,
        participant_id: 20,
        capability_generation: generation,
    }
}

fn sequence_budget() -> super::SequenceBudget {
    super::SequenceBudget {
        high_watermark: 1,
        remaining: 2,
        e: 3,
        t: 4,
        m: 5,
        rs: 6,
        rt: 7,
        l_times_t: 8,
        l_times_rt: 9,
        l_other_times_e: 10,
    }
}

#[allow(clippy::too_many_lines)]
fn sample_server_values() -> Result<Vec<r::ServerValue>, CodecError> {
    let one = generation(1)?;
    let seven = generation(7)?;
    let eight = generation(8)?;
    let binding = epoch(40, seven);
    let enrollment_binding = epoch(50, one);
    let attach_secret = AttachSecret::new([5; 32]);
    let closure_snapshot = c::ClosureSnapshot {
        marker_capacity_credits: 1,
        marker_anchors: 2,
        entry_debt: 3,
        byte_debt: 4,
        repayment_edge: c::RepaymentEdge::ParticipantCursorProgress(
            c::ParticipantCursorProgressEdge {
                participant_id: 20,
                binding_epoch: binding,
                through_seq: 33,
                marker_delivery_seq: Some(34),
            },
        ),
        edge_sequence_claims: 5,
        edge_order_position_claims: 6,
        edge_k_remaining: ResourceVector::new(7, 8),
        k_headroom: WideResourceVector::new(9, 10),
        episode_churn_used: 11,
        delta_cycles: 12,
        episode_churn_limit: 13,
    };
    let enroll_bound = r::EnrollBound::new(
        10,
        EnrollmentToken::new([1; 16]),
        20,
        attach_secret,
        enrollment_binding,
        100,
        200,
    )
    .ok_or(CodecError::InvalidValue)?;

    Ok(vec![
        r::ServerValue::ParticipantTransportRejected(r::ParticipantTransportRejected {
            reason: r::TransportRejectionReason::ParticipantCapabilityRequired,
        }),
        r::ServerValue::AttemptTokenBodyConflict(r::AttemptTokenBodyConflict::CredentialAttach {
            token: AttachAttemptToken::new([2; 16]),
            conversation_id: 10,
            presented_participant_id: 20,
            presented_generation: seven,
            presented_marker_delivery_seq: Some(30),
            conflict: t::AttemptConflict::MarkerDeliverySequence,
        }),
        r::ServerValue::ConnectionConversationCapacityExceeded(
            r::ConnectionConversationCapacityExceeded::SemanticRequest {
                request: e::ResponseEnvelope::Enrollment(enrollment()),
                limit: 2,
            },
        ),
        r::ServerValue::ConnectionConversationBindingOccupied(
            r::ConnectionConversationBindingOccupied::Enrollment {
                conversation_id: 10,
                enrollment_token: EnrollmentToken::new([1; 16]),
            },
        ),
        r::ServerValue::ConversationOrderExhausted(Box::new(r::ConversationOrderExhausted::new(
            r::OrderAllocatingEnvelope::Enrollment(enrollment()),
            80,
            82,
            83,
            84,
            85,
        ))),
        r::ServerValue::ParticipantUnknown(r::ParticipantUnknown {
            request: r::ParticipantReferenceEnvelope::CredentialAttach(attach(seven)),
        }),
        r::ServerValue::NoBinding(r::NoBinding {
            request: r::BindingRequiredEnvelope::Detach(detach(seven)),
        }),
        r::ServerValue::StaleAuthority(r::StaleAuthority::Live {
            request: r::CommonStaleAuthorityEnvelope::CredentialAttach(attach(seven)),
            current_generation: eight,
        }),
        r::ServerValue::Retired(r::Retired::Enrollment {
            request: enrollment(),
            participant_id: 20,
            retired_generation: eight,
        }),
        r::ServerValue::MarkerClosureCapacityExceeded(Box::new(c::MarkerClosureCapacityExceeded {
            request: c::ClosureCheckedEnvelope::Enrollment(enrollment()),
            snapshot: closure_snapshot,
            reason: c::ClosureRefusalReason::RecoveryFence,
        })),
        r::ServerValue::EnrollBound(enroll_bound.clone()),
        r::ServerValue::EnrollmentKnown(r::EnrollmentKnown {
            conversation_id: 10,
            token: EnrollmentToken::new([1; 16]),
            participant_id: 20,
            current_generation: seven,
        }),
        r::ServerValue::ReceiptExpired(r::ReceiptExpired::Enrollment {
            conversation_id: 10,
            token: EnrollmentToken::new([1; 16]),
            participant_id: 20,
            result_generation: seven,
            current_generation: eight,
            reason: t::ReceiptExpiryReason::Superseded,
        }),
        r::ServerValue::ReceiptCapacityExceeded(r::ReceiptCapacityExceeded::Enrollment {
            request: enrollment(),
            scope: r::EnrollmentReceiptCapacityScope::ProvenanceConversation,
            limit: 3,
            occupied: 3,
        }),
        r::ServerValue::IdentityCapacityExceeded(r::IdentityCapacityExceeded {
            request: enrollment(),
            scope: t::IdentityCapacityScope::Conversation,
            limit: 4,
            occupied: 4,
        }),
        r::ServerValue::ObserverBackpressure(r::ObserverBackpressure::Enrollment {
            request: enrollment(),
            state: r::ObserverBackpressureState::initial(90),
        }),
        r::ServerValue::ConversationSequenceExhausted(Box::new(r::ConversationSequenceExhausted {
            request: r::SequenceAllocatingEnvelope::Enrollment(enrollment()),
            sequence_budget: sequence_budget(),
        })),
        r::ServerValue::AttachBound(
            r::AttachBound::fenced(
                10,
                AttachAttemptToken::new([2; 16]),
                20,
                seven,
                attach_secret,
                epoch(60, eight),
                93,
                100,
                200,
            )
            .ok_or(CodecError::InvalidValue)?,
        ),
        r::ServerValue::StaleOrUnknownReceipt(r::StaleOrUnknownReceipt {
            conversation_id: 10,
            token: AttachAttemptToken::new([2; 16]),
            participant_id: 20,
            presented_generation: seven,
            presented_marker_delivery_seq: Some(30),
            current_generation: eight,
        }),
        r::ServerValue::MarkerNotDelivered(r::MarkerNotDelivered {
            request: r::MarkerProofRequest::CredentialAttach(r::AttachMarkerProof {
                conversation_id: 10,
                token: AttachAttemptToken::new([2; 16]),
                participant_id: 20,
                capability_generation: seven,
                requested_marker_delivery_seq: 94,
            }),
            reason: t::MarkerNotDeliveredReason::NotDeliveredToProofEpoch,
            expected_marker_delivery_seq: 95,
        }),
        r::ServerValue::MarkerMismatch(r::MarkerMismatch {
            request: r::MarkerProofRequest::CredentialAttach(r::AttachMarkerProof {
                conversation_id: 10,
                token: AttachAttemptToken::new([2; 16]),
                participant_id: 20,
                capability_generation: seven,
                requested_marker_delivery_seq: 94,
            }),
            mismatch: r::MarkerMismatchBody::NoMarkerExpected,
        }),
        r::ServerValue::Bound(r::ReceiptReplay::Enrollment(enroll_bound)),
        r::ServerValue::UnboundReceipt(r::ReceiptReplay::CredentialAttach(
            r::AttachBound::fenced(
                10,
                AttachAttemptToken::new([2; 16]),
                20,
                seven,
                attach_secret,
                epoch(60, eight),
                97,
                100,
                200,
            )
            .ok_or(CodecError::InvalidValue)?,
        )),
        r::ServerValue::DetachCommitted(r::DetachCommitted::new(
            10,
            20,
            DetachAttemptToken::new([3; 16]),
            binding,
            98,
        )),
        r::ServerValue::DetachInProgress(r::DetachInProgress {
            conversation_id: 10,
            participant_id: 20,
            presented_token: DetachAttemptToken::new([3; 16]),
            presented_generation: seven,
            committed_binding_epoch: binding,
        }),
        r::ServerValue::AckCommitted(r::AckCommitted::new(participant_ack(seven))),
        r::ServerValue::AckNoOp(r::AckNoOp::marker_ack(marker_ack(seven))),
        r::ServerValue::AckGap(
            r::AckGap::new(participant_ack(seven), 30).ok_or(CodecError::InvalidValue)?,
        ),
        r::ServerValue::AckRegression(
            r::AckRegression::new(participant_ack(seven), 32).ok_or(CodecError::InvalidValue)?,
        ),
        r::ServerValue::LeaveCommitted(
            r::LeaveCommitted::new(
                10,
                LeaveAttemptToken::new([4; 16]),
                20,
                seven,
                Some(binding),
                Some(103),
                104,
            )
            .ok_or(CodecError::InvalidValue)?,
        ),
        r::ServerValue::MarkerAckCommitted(r::MarkerAckCommitted::new(marker_ack(seven))),
        r::ServerValue::RecordCommitted(r::RecordCommitted::new(record(seven), 106)),
        r::ServerValue::RecordTooLarge(r::RecordTooLarge {
            request: record(seven),
            dimension: ResourceDimension::Bytes,
            encoded_record_charge: ResourceVector::new(1, 107),
            max_ordinary_record_charge: ResourceVector::new(1, 106),
        }),
        r::ServerValue::ObserverRecoveryAccepted(r::ObserverRecoveryAccepted {
            statuses: vec![r::ObserverProgressStatus {
                conversation_id: 10,
                refused_epoch: 108,
                current_observer_progress: 109,
                armed: true,
                progressed: false,
            }],
        }),
        r::ServerValue::InvalidObserverEpoch(r::InvalidObserverEpoch::EpochAhead {
            conversation_id: 10,
            presented_epoch: 110,
            current_observer_progress: 109,
        }),
        r::ServerValue::InvalidObserverEpochList(
            r::InvalidObserverEpochList::DuplicateConversation {
                conversation_id: 10,
                first_index: 0,
                duplicate_index: 1,
            },
        ),
        r::ServerValue::ConnectionConversationCapacityExceeded(
            r::ConnectionConversationCapacityExceeded::ObserverRecovery {
                conversation_id: 10,
                limit: 5,
            },
        ),
    ])
}

#[test]
fn all_37_server_values_round_trip_with_contiguous_tags() -> Result<(), CodecError> {
    let values = sample_server_values()?;
    assert_eq!(values.len(), 37);

    for (offset, value) in values.into_iter().enumerate() {
        let (discriminant, body) =
            server_codec::encode_server_value_body(&value, ProtocolVersion::V1)?;
        let expected: u16 = 0x0100_u16
            .checked_add(offset.try_into().map_err(|_| CodecError::LengthOverflow)?)
            .ok_or(CodecError::LengthOverflow)?;
        assert_eq!(discriminant.wire_value(), expected);
        let (decoded, version) =
            server_codec::decode_server_value_body(discriminant, ProtocolVersion::V1, &body)?;
        assert_eq!(version, ProtocolVersion::V1);
        assert_eq!(decoded, value);
    }
    Ok(())
}

#[test]
fn all_37_server_values_round_trip_as_complete_frames() -> Result<(), CodecError> {
    for value in sample_server_values()? {
        let frame = ParticipantFrame::ServerValue(value);
        let mut encoded = vec![0; super::encoded_len(&frame)?];
        let written = super::encode(&frame, &mut encoded)?;
        assert_eq!(written, encoded.len());
        assert_eq!(super::decode(&encoded, ReceiverDirection::Client)?, frame);
    }
    Ok(())
}

#[test]
fn terminalized_detach_wire_arm_retains_old_epoch() -> Result<(), CodecError> {
    let old_generation = generation(7)?;
    let current = generation(8)?;
    let old_epoch = epoch(40, old_generation);
    let mut body = Vec::new();
    put_u16(&mut body, t::ClientDiscriminant::DetachRequest.wire_value());
    put_u16(
        &mut body,
        t::DetachAuthorityStateTag::TerminalizedDetachCell.wire_value(),
    );
    put_u64(&mut body, 10);
    put_u64(&mut body, 20);
    put_u64(&mut body, old_generation.get());
    body.extend_from_slice(&[3; 16]);
    put_u64(&mut body, current.get());
    put_epoch(&mut body, old_epoch);
    put_u16(&mut body, t::BindingStateTag::Detached.wire_value());

    let (decoded, _) = server_codec::decode_server_value_body(
        t::ServerDiscriminant::StaleAuthority,
        ProtocolVersion::V1,
        &body,
    )?;
    let r::ServerValue::StaleAuthority(r::StaleAuthority::Detach(
        r::DetachStaleAuthority::TerminalizedDetachCell(cell),
    )) = decoded
    else {
        return Err(CodecError::InvalidValue);
    };
    assert_eq!(cell.committed_binding_epoch(), old_epoch);
    assert_eq!(cell.current_generation(), current);

    let value = r::ServerValue::StaleAuthority(r::StaleAuthority::Detach(
        r::DetachStaleAuthority::TerminalizedDetachCell(cell),
    ));
    let (_, reencoded) = server_codec::encode_server_value_body(&value, ProtocolVersion::V1)?;
    assert_eq!(reencoded, body);
    Ok(())
}

#[test]
fn closure_churn_count_and_limit_use_u64_wire_widths() -> Result<(), CodecError> {
    let value =
        r::ServerValue::MarkerClosureCapacityExceeded(Box::new(c::MarkerClosureCapacityExceeded {
            request: c::ClosureCheckedEnvelope::Enrollment(enrollment()),
            snapshot: c::ClosureSnapshot {
                marker_capacity_credits: 1,
                marker_anchors: 2,
                entry_debt: 3,
                byte_debt: 4,
                repayment_edge: c::RepaymentEdge::None,
                edge_sequence_claims: 5,
                edge_order_position_claims: 6,
                edge_k_remaining: ResourceVector::new(7, 8),
                k_headroom: WideResourceVector::new(9, 10),
                episode_churn_used: 11,
                delta_cycles: 12,
                episode_churn_limit: 13,
            },
            reason: c::ClosureRefusalReason::RecoveryFence,
        }));
    let (_, body) = server_codec::encode_server_value_body(&value, ProtocolVersion::V1)?;
    assert_eq!(body.len(), 150);
    let tail = body
        .get(body.len() - 24..)
        .ok_or(CodecError::InvalidValue)?;
    let mut expected = Vec::new();
    put_u64(&mut expected, 11);
    put_u64(&mut expected, 12);
    put_u64(&mut expected, 13);
    assert_eq!(tail, expected);
    Ok(())
}

#[test]
fn duplicate_success_fields_are_derived_by_typed_constructors() -> Result<(), CodecError> {
    let seven = generation(7)?;
    let eight = generation(8)?;
    let ordinary = r::AttachBound::ordinary(
        10,
        AttachAttemptToken::new([2; 16]),
        20,
        seven,
        AttachSecret::new([5; 32]),
        epoch(60, eight),
        31,
        100,
        200,
    )
    .ok_or(CodecError::InvalidValue)?;
    assert_eq!(ordinary.capability_generation(), eight);
    assert_eq!(ordinary.persisted_cursor(), 31);
    assert_eq!(ordinary.accepted_marker_delivery_seq(), None);

    let fenced = r::AttachBound::fenced(
        10,
        AttachAttemptToken::new([2; 16]),
        20,
        seven,
        AttachSecret::new([5; 32]),
        epoch(60, eight),
        32,
        100,
        200,
    )
    .ok_or(CodecError::InvalidValue)?;
    assert_eq!(fenced.persisted_cursor(), 32);
    assert_eq!(fenced.accepted_marker_delivery_seq(), Some(32));

    let ack = r::AckCommitted::new(participant_ack(seven));
    assert_eq!(ack.current_cursor(), ack.request().through_seq);
    let marker = r::MarkerAckCommitted::new(marker_ack(seven));
    assert_eq!(
        marker.current_cursor(),
        marker.request().marker_delivery_seq
    );
    let record = r::RecordCommitted::new(record(seven), 40);
    assert_eq!(
        record.sender_participant_id(),
        record.request().participant_id
    );

    let max = r::ConversationOrderExhausted::new(
        r::OrderAllocatingEnvelope::Enrollment(enrollment()),
        u64::MAX,
        0,
        0,
        0,
        0,
    );
    assert_eq!(max.next_value(), None);
    Ok(())
}

#[test]
fn decoder_rejects_impossible_duplicate_success_fields() -> Result<(), CodecError> {
    let seven = generation(7)?;
    let eight = generation(8)?;

    let attach = r::ServerValue::AttachBound(
        r::AttachBound::fenced(
            10,
            AttachAttemptToken::new([2; 16]),
            20,
            seven,
            AttachSecret::new([5; 32]),
            epoch(60, eight),
            93,
            100,
            200,
        )
        .ok_or(CodecError::InvalidValue)?,
    );
    let (_, attach_body) = server_codec::encode_server_value_body(&attach, ProtocolVersion::V1)?;
    for (start, invalid) in [(43, 9), (99, 9), (107, 92)] {
        let mut body = attach_body.clone();
        overwrite_u64(&mut body, start, invalid)?;
        assert_decode_class(
            t::ServerDiscriminant::AttachBound,
            &body,
            t::DecodeClass::InvalidField,
        );
    }

    let ack = r::ServerValue::AckCommitted(r::AckCommitted::new(participant_ack(seven)));
    let (_, mut ack_body) = server_codec::encode_server_value_body(&ack, ProtocolVersion::V1)?;
    overwrite_u64(&mut ack_body, 34, 30)?;
    assert_decode_class(
        t::ServerDiscriminant::AckCommitted,
        &ack_body,
        t::DecodeClass::InvalidField,
    );

    let marker = r::ServerValue::MarkerAckCommitted(r::MarkerAckCommitted::new(marker_ack(seven)));
    let (_, mut marker_body) =
        server_codec::encode_server_value_body(&marker, ProtocolVersion::V1)?;
    overwrite_u64(&mut marker_body, 34, 31)?;
    assert_decode_class(
        t::ServerDiscriminant::MarkerAckCommitted,
        &marker_body,
        t::DecodeClass::InvalidField,
    );

    let record = r::ServerValue::RecordCommitted(r::RecordCommitted::new(record(seven), 40));
    let (_, mut record_body) =
        server_codec::encode_server_value_body(&record, ProtocolVersion::V1)?;
    overwrite_u64(&mut record_body, 26, 21)?;
    assert_decode_class(
        t::ServerDiscriminant::RecordCommitted,
        &record_body,
        t::DecodeClass::InvalidField,
    );
    Ok(())
}

#[test]
fn decoder_rejects_invalid_order_backpressure_and_receipt_relations() -> Result<(), CodecError> {
    let one = generation(1)?;
    let order =
        r::ServerValue::ConversationOrderExhausted(Box::new(r::ConversationOrderExhausted::new(
            r::OrderAllocatingEnvelope::Enrollment(enrollment()),
            80,
            82,
            83,
            84,
            85,
        )));
    let (_, mut order_body) = server_codec::encode_server_value_body(&order, ProtocolVersion::V1)?;
    overwrite_u64(&mut order_body, 37, 82)?;
    assert_decode_class(
        t::ServerDiscriminant::ConversationOrderExhausted,
        &order_body,
        t::DecodeClass::InvalidField,
    );

    let pressure = r::ServerValue::ObserverBackpressure(r::ObserverBackpressure::Enrollment {
        request: enrollment(),
        state: r::ObserverBackpressureState::initial(90),
    });
    let (_, mut pressure_body) =
        server_codec::encode_server_value_body(&pressure, ProtocolVersion::V1)?;
    overwrite_u64(&mut pressure_body, 34, 91)?;
    assert_decode_class(
        t::ServerDiscriminant::ObserverBackpressure,
        &pressure_body,
        t::DecodeClass::InvalidField,
    );

    let enrollment_receipt = r::EnrollBound::new(
        10,
        EnrollmentToken::new([1; 16]),
        20,
        AttachSecret::new([5; 32]),
        epoch(50, one),
        100,
        200,
    )
    .ok_or(CodecError::InvalidValue)?;
    let replay = r::ServerValue::Bound(r::ReceiptReplay::Enrollment(enrollment_receipt));
    let (_, replay_body) = server_codec::encode_server_value_body(&replay, ProtocolVersion::V1)?;

    let mut bad_generation = replay_body.clone();
    overwrite_u64(&mut bad_generation, 35, 2)?;
    assert_decode_class(
        t::ServerDiscriminant::Bound,
        &bad_generation,
        t::DecodeClass::InvalidField,
    );

    let mut bad_cursor = replay_body.clone();
    overwrite_u64(&mut bad_cursor, 99, 1)?;
    assert_decode_class(
        t::ServerDiscriminant::Bound,
        &bad_cursor,
        t::DecodeClass::InvalidField,
    );

    let mut bad_marker = replay_body
        .get(..108)
        .ok_or(CodecError::InvalidValue)?
        .to_vec();
    let marker_tag = bad_marker.get_mut(107).ok_or(CodecError::InvalidValue)?;
    *marker_tag = 1;
    bad_marker.extend_from_slice(&0_u64.to_be_bytes());
    bad_marker.extend_from_slice(replay_body.get(108..).ok_or(CodecError::InvalidValue)?);
    assert_decode_class(
        t::ServerDiscriminant::Bound,
        &bad_marker,
        t::DecodeClass::InvalidField,
    );
    Ok(())
}

#[test]
fn decoder_rejects_invalid_terminal_generation_and_order_relations() -> Result<(), CodecError> {
    let seven = generation(7)?;
    let binding = epoch(40, seven);
    let detach = r::ServerValue::DetachCommitted(r::DetachCommitted::new(
        10,
        20,
        DetachAttemptToken::new([3; 16]),
        binding,
        98,
    ));
    let (_, mut detach_body) =
        server_codec::encode_server_value_body(&detach, ProtocolVersion::V1)?;
    overwrite_u64(&mut detach_body, 18, 8)?;
    assert_decode_class(
        t::ServerDiscriminant::DetachCommitted,
        &detach_body,
        t::DecodeClass::InvalidField,
    );

    let leave = r::ServerValue::LeaveCommitted(
        r::LeaveCommitted::new(
            10,
            LeaveAttemptToken::new([4; 16]),
            20,
            seven,
            Some(binding),
            Some(103),
            104,
        )
        .ok_or(CodecError::InvalidValue)?,
    );
    let (_, leave_body) = server_codec::encode_server_value_body(&leave, ProtocolVersion::V1)?;
    let mut bad_generation = leave_body.clone();
    overwrite_u64(&mut bad_generation, 34, 8)?;
    assert_decode_class(
        t::ServerDiscriminant::LeaveCommitted,
        &bad_generation,
        t::DecodeClass::InvalidField,
    );
    let mut bad_order = leave_body;
    overwrite_u64(&mut bad_order, 76, 104)?;
    assert_decode_class(
        t::ServerDiscriminant::LeaveCommitted,
        &bad_order,
        t::DecodeClass::InvalidField,
    );
    Ok(())
}

#[test]
fn every_originating_request_pair_has_exact_routing() -> Result<(), CodecError> {
    for discriminant_value in 0x0101..=0x0120 {
        let discriminant = t::ServerDiscriminant::try_from(discriminant_value)
            .map_err(|_| CodecError::InvalidValue)?;
        for origin_value in 0x0001..=0x0008 {
            let origin = t::ClientDiscriminant::try_from(origin_value)
                .map_err(|_| CodecError::InvalidValue)?;
            let mut body = origin_value.to_be_bytes().to_vec();
            body.push(0xAA);
            let expected = if expected_origin(discriminant, origin) {
                t::DecodeClass::MissingRequiredField
            } else {
                t::DecodeClass::InvalidField
            };
            assert_decode_class(discriminant, &body, expected);
        }
    }
    Ok(())
}

#[test]
fn recovery_count_routes_before_unread_suffix() {
    let impossible_origin = [0x00, 0x02, 0xAA];
    assert_eq!(
        server_codec::decode_server_value_body(
            t::ServerDiscriminant::EnrollBound,
            ProtocolVersion::V1,
            &impossible_origin,
        ),
        Err(CodecError::Decode {
            class: t::DecodeClass::InvalidField,
        })
    );

    for discriminant in [
        t::ServerDiscriminant::InvalidObserverEpoch,
        t::ServerDiscriminant::InvalidObserverEpochList,
        t::ServerDiscriminant::ObserverRecoveryConnectionCapacityExceeded,
    ] {
        assert_decode_class(
            discriminant,
            &1_u64.to_be_bytes(),
            t::DecodeClass::InvalidField,
        );
    }

    let mut short_success = 1_u64.to_be_bytes().to_vec();
    short_success.extend_from_slice(&[0; 25]);
    assert_decode_class(
        t::ServerDiscriminant::ObserverRecoveryAccepted,
        &short_success,
        t::DecodeClass::MissingRequiredField,
    );
}

#[test]
fn invalid_tags_missing_trailing_and_zero_generation_are_classified() -> Result<(), CodecError> {
    let invalid_origin = 0xFFFF_u16.to_be_bytes();
    assert_decode_class(
        t::ServerDiscriminant::AckCommitted,
        &invalid_origin,
        t::DecodeClass::InvalidField,
    );

    let missing = t::ClientDiscriminant::ParticipantAck
        .wire_value()
        .to_be_bytes();
    assert_decode_class(
        t::ServerDiscriminant::AckCommitted,
        &missing,
        t::DecodeClass::MissingRequiredField,
    );

    let sample =
        r::ServerValue::AckCommitted(r::AckCommitted::new(participant_ack(generation(7)?)));
    let (_, mut body) = server_codec::encode_server_value_body(&sample, ProtocolVersion::V1)?;
    body.push(0);
    assert_decode_class(
        t::ServerDiscriminant::AckCommitted,
        &body,
        t::DecodeClass::CanonicalEncoding,
    );

    body.pop();
    let generation_range = 18..26;
    let generation_bytes = body
        .get_mut(generation_range)
        .ok_or(CodecError::InvalidValue)?;
    generation_bytes.fill(0);
    assert_decode_class(
        t::ServerDiscriminant::AckCommitted,
        &body,
        t::DecodeClass::InvalidField,
    );

    let gap = r::ServerValue::AckGap(
        r::AckGap::new(participant_ack(generation(7)?), 30).ok_or(CodecError::InvalidValue)?,
    );
    let (_, mut invalid_reason) =
        server_codec::encode_server_value_body(&gap, ProtocolVersion::V1)?;
    let reason_start = invalid_reason
        .len()
        .checked_sub(2)
        .ok_or(CodecError::InvalidValue)?;
    invalid_reason
        .get_mut(reason_start..)
        .ok_or(CodecError::InvalidValue)?
        .fill(0xFF);
    invalid_reason.push(0xAA);
    assert_decode_class(
        t::ServerDiscriminant::AckGap,
        &invalid_reason,
        t::DecodeClass::InvalidField,
    );
    Ok(())
}

#[test]
fn unsupported_body_version_is_preserved() -> Result<(), CodecError> {
    let value = sample_server_values()?
        .into_iter()
        .next()
        .ok_or(CodecError::InvalidValue)?;
    let version = ProtocolVersion::new(2, 0);
    assert_eq!(
        server_codec::encode_server_value_body(&value, version),
        Err(CodecError::UnsupportedVersion {
            presented: version,
            supported: ProtocolVersion::V1,
        })
    );
    Ok(())
}

fn assert_decode_class(discriminant: t::ServerDiscriminant, body: &[u8], expected: t::DecodeClass) {
    assert_eq!(
        server_codec::decode_server_value_body(discriminant, ProtocolVersion::V1, body),
        Err(CodecError::Decode { class: expected })
    );
}

#[allow(clippy::too_many_lines)]
const fn expected_origin(
    discriminant: t::ServerDiscriminant,
    origin: t::ClientDiscriminant,
) -> bool {
    use t::ClientDiscriminant as O;
    use t::ServerDiscriminant as D;

    match discriminant {
        D::AttemptTokenBodyConflict => {
            matches!(origin, O::CredentialAttachRequest | O::LeaveRequest)
        }
        D::ConnectionConversationCapacityExceeded => {
            !matches!(origin, O::ObserverRecoveryHandshake)
        }
        D::ConnectionConversationBindingOccupied => {
            matches!(origin, O::EnrollmentRequest | O::CredentialAttachRequest)
        }
        D::ConversationOrderExhausted => matches!(
            origin,
            O::EnrollmentRequest | O::CredentialAttachRequest | O::RecordAdmission
        ),
        D::ParticipantUnknown => {
            !matches!(origin, O::EnrollmentRequest | O::ObserverRecoveryHandshake)
        }
        D::NoBinding => !matches!(
            origin,
            O::EnrollmentRequest | O::CredentialAttachRequest | O::ObserverRecoveryHandshake
        ),
        D::StaleAuthority => !matches!(origin, O::EnrollmentRequest | O::ObserverRecoveryHandshake),
        D::Retired => !matches!(origin, O::ObserverRecoveryHandshake),
        D::MarkerClosureCapacityExceeded => matches!(
            origin,
            O::EnrollmentRequest
                | O::CredentialAttachRequest
                | O::LeaveRequest
                | O::RecordAdmission
        ),
        D::EnrollBound | D::EnrollmentKnown | D::IdentityCapacityExceeded => {
            matches!(origin, O::EnrollmentRequest)
        }
        D::ReceiptExpired | D::ReceiptCapacityExceeded | D::Bound | D::UnboundReceipt => {
            matches!(origin, O::EnrollmentRequest | O::CredentialAttachRequest)
        }
        D::ObserverBackpressure => matches!(
            origin,
            O::EnrollmentRequest
                | O::CredentialAttachRequest
                | O::DetachRequest
                | O::LeaveRequest
                | O::RecordAdmission
        ),
        D::ConversationSequenceExhausted => matches!(
            origin,
            O::EnrollmentRequest | O::CredentialAttachRequest | O::RecordAdmission
        ),
        D::AttachBound | D::StaleOrUnknownReceipt => {
            matches!(origin, O::CredentialAttachRequest)
        }
        D::MarkerNotDelivered | D::MarkerMismatch => {
            matches!(origin, O::CredentialAttachRequest | O::MarkerAck)
        }
        D::DetachCommitted | D::DetachInProgress => matches!(origin, O::DetachRequest),
        D::AckCommitted | D::AckGap | D::AckRegression => matches!(origin, O::ParticipantAck),
        D::AckNoOp => matches!(origin, O::ParticipantAck | O::MarkerAck),
        D::LeaveCommitted => matches!(origin, O::LeaveRequest),
        D::MarkerAckCommitted => matches!(origin, O::MarkerAck),
        D::RecordCommitted | D::RecordTooLarge => matches!(origin, O::RecordAdmission),
        D::ParticipantTransportRejected
        | D::ObserverRecoveryAccepted
        | D::InvalidObserverEpoch
        | D::InvalidObserverEpochList
        | D::ObserverRecoveryConnectionCapacityExceeded => false,
    }
}

fn put_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn put_epoch(output: &mut Vec<u8>, value: BindingEpoch) {
    put_u64(output, value.connection_incarnation.server_incarnation);
    put_u64(output, value.connection_incarnation.connection_ordinal);
    put_u64(output, value.capability_generation.get());
}

fn overwrite_u64(output: &mut [u8], start: usize, value: u64) -> Result<(), CodecError> {
    let end = start.checked_add(8).ok_or(CodecError::LengthOverflow)?;
    let target = output.get_mut(start..end).ok_or(CodecError::InvalidValue)?;
    target.copy_from_slice(&value.to_be_bytes());
    Ok(())
}
