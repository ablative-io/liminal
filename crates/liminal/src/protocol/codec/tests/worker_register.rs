use super::{Frame, ProtocolError, WorkerRegistration, decode, encode, encoded_len, round_trip};
use crate::protocol::WorkerRegisterOutcome;

use super::super::tests_support::worker_register_frames;

#[test]
fn worker_register_frames_round_trip() -> Result<(), ProtocolError> {
    for frame in worker_register_frames() {
        assert_eq!(round_trip(&frame)?, frame);
    }
    Ok(())
}

#[test]
fn worker_register_node_presence_distinguishes_none_from_empty() -> Result<(), ProtocolError> {
    // node = None must NOT round-trip into Some(""): the presence byte keeps the
    // optional-locality distinction the routing model relies on.
    let absent = Frame::WorkerRegister {
        flags: 0,
        registration: WorkerRegistration {
            namespaces: vec!["default".to_owned()],
            task_queue: "q".to_owned(),
            node: None,
            activity_types: vec!["a".to_owned()],
            identity: "id".to_owned(),
            activities: Vec::new(),
        },
    };
    let present_empty = Frame::WorkerRegister {
        flags: 0,
        registration: WorkerRegistration {
            namespaces: vec!["default".to_owned()],
            task_queue: "q".to_owned(),
            node: Some(String::new()),
            activity_types: vec!["a".to_owned()],
            identity: "id".to_owned(),
            activities: Vec::new(),
        },
    };

    let decoded_absent = round_trip(&absent)?;
    let decoded_present = round_trip(&present_empty)?;
    assert!(matches!(
        decoded_absent,
        Frame::WorkerRegister { registration, .. } if registration.node.is_none()
    ));
    assert!(matches!(
        decoded_present,
        Frame::WorkerRegister { registration, .. } if registration.node.as_deref() == Some("")
    ));
    // The two frames must NOT be byte-identical (None vs Some("") are distinct).
    let mut absent_bytes = vec![0_u8; encoded_len(&absent)?];
    let mut present_bytes = vec![0_u8; encoded_len(&present_empty)?];
    encode(&absent, &mut absent_bytes)?;
    encode(&present_empty, &mut present_bytes)?;
    assert_ne!(absent_bytes, present_bytes);
    Ok(())
}

#[test]
fn worker_register_ack_outcome_round_trips() -> Result<(), ProtocolError> {
    let accepted = Frame::WorkerRegisterAck {
        flags: 0,
        outcome: WorkerRegisterOutcome::Accepted,
    };
    let rejected = Frame::WorkerRegisterAck {
        flags: 0,
        outcome: WorkerRegisterOutcome::Rejected {
            reason: "no such task queue".to_owned(),
        },
    };
    assert_eq!(round_trip(&accepted)?, accepted);
    assert_eq!(round_trip(&rejected)?, rejected);
    assert!(matches!(
        round_trip(&rejected)?,
        Frame::WorkerRegisterAck {
            outcome: WorkerRegisterOutcome::Rejected { reason },
            ..
        } if reason == "no such task queue"
    ));
    Ok(())
}

#[test]
fn worker_register_ack_invalid_status_byte_is_rejected() {
    // type 0x18 = WorkerRegisterAck, control frame on stream 0, payload = [0x7F]
    // (an undefined status byte). Decode must error, not panic or silently accept.
    let input = [0x18, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0x7F];
    assert!(matches!(
        decode(&input),
        Err(ProtocolError::CodecError { .. })
    ));
}

#[test]
fn worker_register_discriminants_are_additive_and_unknown_preserved() -> Result<(), ProtocolError> {
    // The assigned discriminants now run through 0x19 (Deliver); the next free byte
    // (0x1A) must still decode to Frame::Unknown, proving the additions did not
    // consume a forward-compatibility slot.
    let input = [0x1A, 0x00, 0, 0, 0, 0, 0, 0, 0, 2, 0xAB, 0xCD];
    let (frame, consumed) = decode(&input)?;
    assert_eq!(consumed, input.len());
    assert_eq!(
        frame,
        Frame::Unknown {
            type_id: 0x1A,
            flags: 0x00,
            stream_id: 0,
            payload: vec![0xAB, 0xCD],
        }
    );
    Ok(())
}
