use liminal::protocol::{Frame, decode as decode_generic, encode as encode_generic, encoded_len};
use liminal_protocol::wire::{
    ClientRequest, EnrollmentRequest, EnrollmentToken, FRAME_MAX, ParticipantFrame,
    ReceiverDirection, ServerValue, TransportRejectionReason, decode, encode,
    encoded_len as participant_encoded_len,
};

use super::transport::{
    ParticipantIngress, ParticipantSession, encode_server_value, gate_generic_frame,
    normalize_configured_frame_limit, preflight_generic_bytes,
};

fn encoded_enrollment() -> Result<Vec<u8>, String> {
    let frame = ParticipantFrame::ClientRequest(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: 41,
        enrollment_token: EnrollmentToken::new([7; 16]),
    }));
    let needed = participant_encoded_len(&frame).map_err(|error| format!("{error:?}"))?;
    let mut bytes = vec![0; needed];
    let written = encode(&frame, &mut bytes).map_err(|error| format!("{error:?}"))?;
    bytes.truncate(written);
    Ok(bytes)
}

fn generic_round_trip(bytes: &[u8]) -> Result<Frame, String> {
    let (frame, consumed) = decode_generic(bytes).map_err(|error| error.to_string())?;
    assert_eq!(consumed, bytes.len());
    Ok(frame)
}

fn negotiated_session(configured_wf: u64) -> Result<ParticipantSession, String> {
    let limit =
        normalize_configured_frame_limit(configured_wf).map_err(|error| format!("{error:?}"))?;
    let mut session = ParticipantSession::default();
    session.negotiate_v1(limit);
    Ok(session)
}

#[test]
fn configured_wire_frame_limit_normalizes_to_generic_ceiling() -> Result<(), String> {
    assert_eq!(
        normalize_configured_frame_limit(u64::MAX)
            .map_err(|error| format!("{error:?}"))?
            .get(),
        FRAME_MAX
    );
    assert!(normalize_configured_frame_limit(15).is_err());
    Ok(())
}

#[test]
fn negotiated_unknown_outer_frame_uses_shared_request_decoder() -> Result<(), String> {
    let generic = generic_round_trip(&encoded_enrollment()?)?;
    let session = negotiated_session(u64::MAX)?;

    let ingress = gate_generic_frame(&generic, true, session);

    assert_eq!(
        ingress,
        ParticipantIngress::Request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: 41,
            enrollment_token: EnrollmentToken::new([7; 16]),
        }))
    );
    Ok(())
}

#[test]
fn unauthenticated_participant_request_returns_shared_rejection() -> Result<(), String> {
    let generic = generic_round_trip(&encoded_enrollment()?)?;
    let session = negotiated_session(u64::MAX)?;

    let ParticipantIngress::Rejected(rejection) = gate_generic_frame(&generic, false, session)
    else {
        return Err("expected authentication rejection".to_owned());
    };
    assert_eq!(
        rejection.reason,
        TransportRejectionReason::AuthenticationFailed
    );
    Ok(())
}

#[test]
fn authenticated_request_without_advertised_capability_is_rejected() -> Result<(), String> {
    let generic = generic_round_trip(&encoded_enrollment()?)?;
    let ParticipantIngress::Rejected(rejection) =
        gate_generic_frame(&generic, true, ParticipantSession::default())
    else {
        return Err("expected participant capability rejection".to_owned());
    };
    assert_eq!(
        rejection.reason,
        TransportRejectionReason::ParticipantCapabilityRequired
    );
    Ok(())
}

#[test]
fn crate_server_value_survives_generic_transport_round_trip() -> Result<(), String> {
    let generic = generic_round_trip(&encoded_enrollment()?)?;
    let ParticipantIngress::Rejected(rejection) =
        gate_generic_frame(&generic, false, ParticipantSession::default())
    else {
        return Err("expected shared gate rejection".to_owned());
    };
    let outbound = encode_server_value(ServerValue::ParticipantTransportRejected(rejection))
        .map_err(|error| format!("{error:?}"))?;
    let generic_len = encoded_len(&outbound).map_err(|error| error.to_string())?;
    let mut generic_bytes = vec![0; generic_len];
    let written =
        encode_generic(&outbound, &mut generic_bytes).map_err(|error| error.to_string())?;
    generic_bytes.truncate(written);

    let decoded =
        decode(&generic_bytes, ReceiverDirection::Client).map_err(|error| format!("{error:?}"))?;
    assert!(matches!(
        decoded,
        ParticipantFrame::ServerValue(ServerValue::ParticipantTransportRejected(_))
    ));
    Ok(())
}

#[test]
fn unrelated_unknown_frame_remains_outside_participant_protocol() {
    let frame = Frame::Unknown {
        type_id: 0xFE,
        flags: 0,
        stream_id: 9,
        payload: vec![1, 2, 3],
    };
    assert_eq!(
        gate_generic_frame(&frame, true, ParticipantSession::default()),
        ParticipantIngress::NotParticipant
    );
}

#[test]
fn negotiated_limit_rejects_from_header_before_body_arrives() -> Result<(), String> {
    let session = negotiated_session(128)?;
    let declared_payload = 119_u32;
    let mut header = vec![0x1A, 0, 0, 0, 0, 0];
    header.extend_from_slice(&declared_payload.to_be_bytes());

    let Some(rejection) = preflight_generic_bytes(&header, true, session) else {
        return Err("oversized participant header was not rejected".to_owned());
    };
    assert_eq!(
        rejection.reason,
        TransportRejectionReason::FrameTooLarge {
            complete_frame_bytes: 129,
            max_frame_bytes: 128,
        }
    );
    Ok(())
}

#[test]
fn incomplete_frame_within_limit_remains_in_incremental_decoder() -> Result<(), String> {
    let session = negotiated_session(256)?;
    let mut header = vec![0x1A, 0, 0, 0, 0, 0];
    header.extend_from_slice(&128_u32.to_be_bytes());

    assert_eq!(preflight_generic_bytes(&header, true, session), None);
    Ok(())
}
