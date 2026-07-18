use alloc::vec;
use alloc::vec::Vec;

use super::codec::{
    AuthenticationState, CodecError, FRAME_MAX, InboundGateContext, InboundGateError,
    NegotiatedParticipantCapability, PARTICIPANT_FRAME_OVERHEAD, PRECAP_PARTICIPANT_FRAME_MAX,
    ParticipantCapabilityState, ParticipantFrame, ReceiverDirection, ValidatedFrameLimit, decode,
    encode, encoded_len, gate_inbound,
};
use super::{
    AttachAttemptToken, AttachSecret, BindingEpoch, ClientRequest, ConnectionIncarnation,
    CredentialAttachRequest, DecodeClass, DetachAttemptToken, DetachRequest, DetachedCause,
    DiedCause, EnrollmentRequest, EnrollmentToken, Generation, LeaveAttemptToken, LeaveRequest,
    MarkerAck, ObserverRecoveryHandshake, ObserverRefusal, ParticipantAck, ParticipantDelivery,
    ParticipantRecord, ParticipantTransportRejected, ProtocolVersion, RecordAdmission, ServerPush,
    ServerValue, TransportRejectionReason,
};

fn generation(value: u64) -> Result<Generation, CodecError> {
    Generation::new(value).ok_or(CodecError::InvalidValue)
}

fn epoch(value: u64) -> Result<BindingEpoch, CodecError> {
    Ok(BindingEpoch::new(
        ConnectionIncarnation::new(value, value + 1),
        generation(value + 1)?,
    ))
}

fn request_frames() -> Result<Vec<ParticipantFrame>, CodecError> {
    let generation = generation(7)?;
    Ok(vec![
        ParticipantFrame::ClientRequest(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: 1,
            enrollment_token: EnrollmentToken::new([1; 16]),
        })),
        ParticipantFrame::ClientRequest(ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: 2,
            participant_id: 3,
            capability_generation: generation,
            attach_secret: AttachSecret::new([2; 32]),
            attach_attempt_token: AttachAttemptToken::new([3; 16]),
            accept_marker_delivery_seq: Some(4),
        })),
        ParticipantFrame::ClientRequest(ClientRequest::Detach(DetachRequest {
            conversation_id: 5,
            participant_id: 6,
            capability_generation: generation,
            detach_attempt_token: DetachAttemptToken::new([4; 16]),
        })),
        ParticipantFrame::ClientRequest(ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: 7,
            participant_id: 8,
            capability_generation: generation,
            through_seq: 9,
        })),
        ParticipantFrame::ClientRequest(ClientRequest::Leave(LeaveRequest {
            conversation_id: 10,
            participant_id: 11,
            capability_generation: generation,
            attach_secret: AttachSecret::new([5; 32]),
            leave_attempt_token: LeaveAttemptToken::new([6; 16]),
        })),
        ParticipantFrame::ClientRequest(ClientRequest::MarkerAck(MarkerAck {
            conversation_id: 12,
            participant_id: 13,
            capability_generation: generation,
            marker_delivery_seq: 14,
        })),
        ParticipantFrame::ClientRequest(ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: 15,
            participant_id: 16,
            capability_generation: generation,
            record_admission_attempt_token: crate::wire::RecordAdmissionAttemptToken::new(
                [0xA7; 16],
            ),
            payload: vec![0xAA, 0xBB, 0xCC],
        })),
        ParticipantFrame::ClientRequest(ClientRequest::ObserverRecovery(
            ObserverRecoveryHandshake {
                observer_refusals: vec![
                    ObserverRefusal {
                        conversation_id: 17,
                        refused_epoch: 18,
                    },
                    ObserverRefusal {
                        conversation_id: 19,
                        refused_epoch: 20,
                    },
                ],
            },
        )),
    ])
}

fn delivery(record: ParticipantRecord, sequence: u64) -> ParticipantFrame {
    ParticipantFrame::ServerPush(ServerPush::ParticipantDelivery(ParticipantDelivery {
        conversation_id: 41,
        delivery_seq: sequence,
        record,
    }))
}

fn push_frames() -> Result<Vec<ParticipantFrame>, CodecError> {
    let binding = epoch(21)?;
    Ok(vec![
        ParticipantFrame::ServerPush(ServerPush::ObserverProgressed {
            conversation_id: 22,
            refused_epoch: 23,
            observer_progress: 24,
        }),
        delivery(
            ParticipantRecord::OrdinaryRecord {
                sender_participant_id: 25,
                payload: vec![1, 2, 3, 4],
            },
            26,
        ),
        delivery(
            ParticipantRecord::Attached {
                affected_participant_id: 27,
                binding_epoch: binding,
            },
            28,
        ),
        delivery(
            ParticipantRecord::Detached {
                affected_participant_id: 29,
                binding_epoch: binding,
                cause: DetachedCause::Superseded,
            },
            30,
        ),
        delivery(
            ParticipantRecord::Died {
                affected_participant_id: 31,
                binding_epoch: binding,
                cause: DiedCause::UncleanServerRestart {
                    prior_server_incarnation: 32,
                },
            },
            33,
        ),
        delivery(
            ParticipantRecord::Left {
                affected_participant_id: 34,
                ended_binding_epoch: Some(binding),
            },
            35,
        ),
        delivery(
            ParticipantRecord::HistoryCompacted {
                affected_participant_id: 36,
                abandoned_after: 37,
                abandoned_through: 38,
                physical_floor_at_decision: 39,
            },
            40,
        ),
    ])
}

fn encoded(frame: &ParticipantFrame) -> Result<Vec<u8>, CodecError> {
    let mut bytes = vec![0; encoded_len(frame)?];
    let written = encode(frame, &mut bytes)?;
    assert_eq!(written, bytes.len());
    Ok(bytes)
}

fn gate_context(
    receiver: ReceiverDirection,
    authentication: AuthenticationState,
    participant_capability: ParticipantCapabilityState,
) -> InboundGateContext {
    InboundGateContext {
        receiver,
        authentication,
        participant_capability,
    }
}

fn negotiated_v1(max_frame_bytes: u64) -> Result<ParticipantCapabilityState, CodecError> {
    Ok(ParticipantCapabilityState::Negotiated(
        NegotiatedParticipantCapability::v1(ValidatedFrameLimit::new(max_frame_bytes)?),
    ))
}

fn participant_transport_rejection(
    reason: TransportRejectionReason,
) -> ParticipantTransportRejected {
    ParticipantTransportRejected { reason }
}

fn transport_rejection(reason: TransportRejectionReason) -> InboundGateError {
    InboundGateError::ParticipantRejected(participant_transport_rejection(reason))
}

fn raw_authentication_rejection_frame() -> Vec<u8> {
    let mut bytes = vec![0; 18];
    bytes[0] = 0x1A;
    bytes[6..10].copy_from_slice(&8_u32.to_be_bytes());
    bytes[10..12].copy_from_slice(&1_u16.to_be_bytes());
    bytes[12..14].copy_from_slice(&0_u16.to_be_bytes());
    bytes[14..16].copy_from_slice(&0x0100_u16.to_be_bytes());
    bytes[16..18].copy_from_slice(&4_u16.to_be_bytes());
    bytes
}

#[test]
fn all_client_requests_round_trip() -> Result<(), CodecError> {
    for frame in request_frames()? {
        let bytes = encoded(&frame)?;
        assert_eq!(decode(&bytes, ReceiverDirection::Server)?, frame);
    }
    Ok(())
}

#[test]
fn all_push_record_kinds_round_trip() -> Result<(), CodecError> {
    for frame in push_frames()? {
        let bytes = encoded(&frame)?;
        assert_eq!(decode(&bytes, ReceiverDirection::Client)?, frame);
    }
    Ok(())
}

#[test]
fn fixed_request_and_delivery_sizes_match_the_contract() -> Result<(), CodecError> {
    let requests = request_frames()?;
    assert_eq!(encoded_len(&requests[0])?, 40);
    assert_eq!(encoded_len(&requests[1])?, 97);
    assert_eq!(encoded_len(&requests[6])?, 60 + 3);
    assert_eq!(encoded_len(&requests[7])?, 16 + 8 + (2 * 16));

    let ordinary = delivery(
        ParticipantRecord::OrdinaryRecord {
            sender_participant_id: 1,
            payload: vec![0; 5],
        },
        1,
    );
    assert_eq!(encoded_len(&ordinary)?, 46 + 5);
    Ok(())
}

#[test]
fn header_and_prefix_are_network_order_and_exact() -> Result<(), CodecError> {
    let frame = request_frames()?.remove(0);
    let bytes = encoded(&frame)?;
    assert_eq!(bytes[0], 0x1A);
    assert_eq!(bytes[1], 0);
    assert_eq!(&bytes[2..6], &0_u32.to_be_bytes());
    assert_eq!(&bytes[6..10], &30_u32.to_be_bytes());
    assert_eq!(&bytes[10..12], &1_u16.to_be_bytes());
    assert_eq!(&bytes[12..14], &0_u16.to_be_bytes());
    assert_eq!(&bytes[14..16], &1_u16.to_be_bytes());
    assert_eq!(PARTICIPANT_FRAME_OVERHEAD, 16);
    Ok(())
}

#[test]
fn wrong_direction_and_unassigned_values_are_unknown() -> Result<(), CodecError> {
    let frame = request_frames()?.remove(0);
    let mut bytes = encoded(&frame)?;
    assert_eq!(
        decode(&bytes, ReceiverDirection::Client),
        Err(CodecError::Decode {
            class: super::DecodeClass::UnknownDiscriminant,
        })
    );

    bytes[14..16].copy_from_slice(&0xFFFF_u16.to_be_bytes());
    assert_eq!(
        decode(&bytes, ReceiverDirection::Server),
        Err(CodecError::Decode {
            class: super::DecodeClass::UnknownDiscriminant,
        })
    );
    Ok(())
}

#[test]
fn non_participant_outer_type_bypasses_every_participant_gate_stage() -> Result<(), CodecError> {
    assert_eq!(
        decode(&[0x19], ReceiverDirection::Server),
        Err(CodecError::NotParticipantFrame { frame_type: 0x19 })
    );

    let mut unrelated = vec![0; 10];
    unrelated[0] = 0x19;
    unrelated[1] = 0xFF;
    unrelated[6..10].copy_from_slice(&u32::MAX.to_be_bytes());

    assert_eq!(
        decode(&unrelated, ReceiverDirection::Server),
        Err(CodecError::NotParticipantFrame { frame_type: 0x19 })
    );
    let context = gate_context(
        ReceiverDirection::Server,
        AuthenticationState::Unauthenticated,
        negotiated_v1(16)?,
    );
    assert_eq!(
        gate_inbound(&unrelated, context),
        Err(InboundGateError::NotParticipantFrame { frame_type: 0x19 })
    );
    Ok(())
}

#[test]
fn trailing_truncated_and_zero_generation_classes_are_stable() -> Result<(), CodecError> {
    let attach = request_frames()?.remove(1);
    let mut zero_generation = encoded(&attach)?;
    zero_generation[32..40].copy_from_slice(&0_u64.to_be_bytes());
    assert_eq!(
        decode(&zero_generation, ReceiverDirection::Server),
        Err(CodecError::Decode {
            class: super::DecodeClass::InvalidField,
        })
    );

    let mut trailing = zero_generation.clone();
    trailing.push(0);
    let payload_length =
        u32::try_from(trailing.len() - 10).map_err(|_| CodecError::LengthOverflow)?;
    trailing[6..10].copy_from_slice(&payload_length.to_be_bytes());
    assert_eq!(
        decode(&trailing, ReceiverDirection::Server),
        Err(CodecError::Decode {
            class: super::DecodeClass::CanonicalEncoding,
        })
    );

    let mut truncated = encoded(&attach)?;
    let _ = truncated.pop();
    assert_eq!(
        decode(&truncated, ReceiverDirection::Server),
        Err(CodecError::Decode {
            class: super::DecodeClass::MissingRequiredField,
        })
    );
    Ok(())
}

#[test]
fn recovery_count_is_bounded_before_allocation() -> Result<(), CodecError> {
    let frame = ParticipantFrame::ClientRequest(ClientRequest::ObserverRecovery(
        ObserverRecoveryHandshake {
            observer_refusals: Vec::new(),
        },
    ));
    let mut bytes = encoded(&frame)?;
    bytes[16..24].copy_from_slice(&u64::MAX.to_be_bytes());
    assert_eq!(
        decode(&bytes, ReceiverDirection::Server),
        Err(CodecError::Decode {
            class: super::DecodeClass::MissingRequiredField,
        })
    );
    Ok(())
}

#[test]
fn short_output_reports_exact_requirement() -> Result<(), CodecError> {
    let frame = request_frames()?.remove(0);
    let required = encoded_len(&frame)?;
    let mut short = vec![0; required - 1];
    assert_eq!(
        encode(&frame, &mut short),
        Err(CodecError::OutputTooSmall {
            required,
            available: required - 1,
        })
    );
    Ok(())
}

#[test]
fn validated_frame_limit_enforces_the_explicit_complete_frame_range() {
    assert_eq!(
        ValidatedFrameLimit::new(15),
        Err(CodecError::InvalidFrameLimit {
            max_frame_bytes: 15,
        })
    );
    assert_eq!(
        ValidatedFrameLimit::new(16).map(ValidatedFrameLimit::get),
        Ok(16)
    );
    assert_eq!(
        ValidatedFrameLimit::new(FRAME_MAX).map(ValidatedFrameLimit::get),
        Ok(FRAME_MAX)
    );
    assert_eq!(
        ValidatedFrameLimit::new(FRAME_MAX + 1),
        Err(CodecError::InvalidFrameLimit {
            max_frame_bytes: FRAME_MAX + 1,
        })
    );
}

#[test]
fn inbound_gate_size_precedes_framing() -> Result<(), CodecError> {
    let frame = request_frames()?.remove(0);
    let mut bytes = encoded(&frame)?;
    bytes[1] = 1;
    let context = gate_context(
        ReceiverDirection::Server,
        AuthenticationState::Authenticated,
        negotiated_v1(16)?,
    );

    assert_eq!(
        gate_inbound(&bytes, context),
        Err(transport_rejection(
            TransportRejectionReason::FrameTooLarge {
                complete_frame_bytes: 40,
                max_frame_bytes: 16,
            }
        ))
    );
    Ok(())
}

#[test]
fn inbound_gate_framing_precedes_version() -> Result<(), CodecError> {
    let frame = request_frames()?.remove(0);
    let mut bytes = encoded(&frame)?;
    bytes[1] = 1;
    bytes[10..12].copy_from_slice(&2_u16.to_be_bytes());
    let context = gate_context(
        ReceiverDirection::Server,
        AuthenticationState::Authenticated,
        negotiated_v1(FRAME_MAX)?,
    );

    assert_eq!(
        gate_inbound(&bytes, context),
        Err(transport_rejection(
            TransportRejectionReason::DecodeFailed {
                decode_class: DecodeClass::Framing,
            }
        ))
    );
    Ok(())
}

#[test]
fn inbound_gate_version_precedes_discriminant() -> Result<(), CodecError> {
    let frame = request_frames()?.remove(0);
    let mut bytes = encoded(&frame)?;
    bytes[10..12].copy_from_slice(&2_u16.to_be_bytes());
    bytes[14..16].copy_from_slice(&0xFFFF_u16.to_be_bytes());
    let context = gate_context(
        ReceiverDirection::Server,
        AuthenticationState::Authenticated,
        negotiated_v1(FRAME_MAX)?,
    );

    assert_eq!(
        gate_inbound(&bytes, context),
        Err(transport_rejection(
            TransportRejectionReason::UnsupportedVersion {
                presented_version: ProtocolVersion::new(2, 0),
                supported_version: ProtocolVersion::V1,
            }
        ))
    );
    Ok(())
}

#[test]
fn inbound_gate_maps_unknown_and_selected_body_failures() -> Result<(), CodecError> {
    let context = gate_context(
        ReceiverDirection::Server,
        AuthenticationState::Authenticated,
        negotiated_v1(FRAME_MAX)?,
    );

    let frame = request_frames()?.remove(0);
    let mut unknown = encoded(&frame)?;
    unknown[14..16].copy_from_slice(&0xFFFF_u16.to_be_bytes());
    assert_eq!(
        gate_inbound(&unknown, context),
        Err(transport_rejection(
            TransportRejectionReason::DecodeFailed {
                decode_class: DecodeClass::UnknownDiscriminant,
            }
        ))
    );

    let mut trailing = encoded(&frame)?;
    trailing.push(0);
    trailing[6..10].copy_from_slice(&31_u32.to_be_bytes());
    assert_eq!(
        gate_inbound(&trailing, context),
        Err(transport_rejection(
            TransportRejectionReason::DecodeFailed {
                decode_class: DecodeClass::CanonicalEncoding,
            }
        ))
    );

    let mut missing = encoded(&frame)?;
    let _ = missing.pop();
    missing[6..10].copy_from_slice(&29_u32.to_be_bytes());
    assert_eq!(
        gate_inbound(&missing, context),
        Err(transport_rejection(
            TransportRejectionReason::DecodeFailed {
                decode_class: DecodeClass::MissingRequiredField,
            }
        ))
    );

    let attach = request_frames()?.remove(1);
    let mut invalid = encoded(&attach)?;
    invalid[32..40].copy_from_slice(&0_u64.to_be_bytes());
    assert_eq!(
        gate_inbound(&invalid, context),
        Err(transport_rejection(
            TransportRejectionReason::DecodeFailed {
                decode_class: DecodeClass::InvalidField,
            }
        ))
    );
    Ok(())
}

#[test]
fn inbound_gate_authentication_precedes_capability() -> Result<(), CodecError> {
    let frame = request_frames()?.remove(0);
    let bytes = encoded(&frame)?;
    let unauthenticated = gate_context(
        ReceiverDirection::Server,
        AuthenticationState::Unauthenticated,
        ParticipantCapabilityState::Missing,
    );
    let authenticated = gate_context(
        ReceiverDirection::Server,
        AuthenticationState::Authenticated,
        ParticipantCapabilityState::Missing,
    );

    assert_eq!(
        gate_inbound(&bytes, unauthenticated),
        Err(transport_rejection(
            TransportRejectionReason::AuthenticationFailed
        ))
    );
    assert_eq!(
        gate_inbound(&bytes, authenticated),
        Err(transport_rejection(
            TransportRejectionReason::ParticipantCapabilityRequired
        ))
    );
    Ok(())
}

#[test]
fn inbound_gate_uses_the_pre_capability_constant_before_allocation() {
    let mut header = vec![0; 10];
    header[0] = 0x1A;
    header[6..10].copy_from_slice(&1_048_567_u32.to_be_bytes());
    let context = gate_context(
        ReceiverDirection::Server,
        AuthenticationState::Unauthenticated,
        ParticipantCapabilityState::Missing,
    );

    assert_eq!(
        gate_inbound(&header, context),
        Err(transport_rejection(
            TransportRejectionReason::FrameTooLarge {
                complete_frame_bytes: 1_048_577,
                max_frame_bytes: PRECAP_PARTICIPANT_FRAME_MAX,
            }
        ))
    );
}

#[test]
fn sdk_transport_rejection_bypasses_local_authentication_and_capability() {
    let bytes = raw_authentication_rejection_frame();
    let context = gate_context(
        ReceiverDirection::Client,
        AuthenticationState::Unauthenticated,
        ParticipantCapabilityState::Missing,
    );

    assert_eq!(
        gate_inbound(&bytes, context),
        Ok(ParticipantFrame::ServerValue(
            ServerValue::ParticipantTransportRejected(participant_transport_rejection(
                TransportRejectionReason::AuthenticationFailed,
            ))
        ))
    );
}

#[test]
fn malformed_sdk_transport_rejection_does_not_receive_the_exception() {
    let mut bytes = raw_authentication_rejection_frame();
    bytes[16..18].copy_from_slice(&0xFFFF_u16.to_be_bytes());
    let context = gate_context(
        ReceiverDirection::Client,
        AuthenticationState::Unauthenticated,
        ParticipantCapabilityState::Missing,
    );

    assert_eq!(
        gate_inbound(&bytes, context),
        Err(transport_rejection(
            TransportRejectionReason::DecodeFailed {
                decode_class: DecodeClass::InvalidField,
            }
        ))
    );
}
