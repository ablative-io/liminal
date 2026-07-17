//! Canonical participant framing over the WebSocket transport's generic frame.
//!
//! Byte-identical mirror of the TCP transport's participant framing (which is
//! private to the tcp module): a participant payload rides one generic frame
//! with the stable participant type byte, and one such frame is exactly one
//! binary WebSocket message. The cross-transport participant byte-identity
//! test pins the two framings together.

use alloc::format;
use alloc::string::ToString;
use alloc::vec;

use liminal::protocol::Frame;
use liminal_protocol::wire::{
    ClientRequest, PARTICIPANT_FRAME_TYPE, ParticipantFrame, ReceiverDirection, decode, encode,
    encoded_len,
};

use crate::SdkError;

/// Wraps one client participant request as the generic participant frame.
pub(super) fn request_frame(request: &ClientRequest) -> Result<Frame, SdkError> {
    let frame = ParticipantFrame::ClientRequest(request.clone());
    let needed = encoded_len(&frame).map_err(codec_error)?;
    let mut complete = vec![0_u8; needed];
    let written = encode(&frame, &mut complete).map_err(codec_error)?;
    let payload = complete
        .get(10..written)
        .ok_or_else(|| SdkError::Protocol {
            description: "participant encoder returned an invalid generic frame length".to_string(),
        })?
        .to_vec();
    Ok(Frame::Unknown {
        type_id: PARTICIPANT_FRAME_TYPE,
        flags: 0,
        stream_id: 0,
        payload,
    })
}

/// Direction-decodes one generic frame as a canonical participant response.
pub(super) fn response_frame(frame: Frame) -> Result<ParticipantFrame, SdkError> {
    let Frame::Unknown {
        type_id,
        flags,
        stream_id,
        payload,
    } = frame
    else {
        return Err(SdkError::Protocol {
            description: "expected a participant response frame".to_string(),
        });
    };
    let payload_length = u32::try_from(payload.len()).map_err(|_| SdkError::Protocol {
        description: "participant payload exceeds the generic u32 length".to_string(),
    })?;
    let capacity = 10_usize
        .checked_add(payload.len())
        .ok_or_else(|| SdkError::Protocol {
            description: "participant complete-frame length overflow".to_string(),
        })?;
    let mut complete = alloc::vec::Vec::with_capacity(capacity);
    complete.push(type_id);
    complete.push(flags);
    complete.extend_from_slice(&stream_id.to_be_bytes());
    complete.extend_from_slice(&payload_length.to_be_bytes());
    complete.extend_from_slice(&payload);
    decode(&complete, ReceiverDirection::Client).map_err(codec_error)
}

fn codec_error(error: liminal_protocol::wire::CodecError) -> SdkError {
    SdkError::Protocol {
        description: format!("participant frame codec failed: {error:?}"),
    }
}
