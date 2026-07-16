//! Canonical byte codec for durable participant-conversation events.
//!
//! This module owns the stable v1 `LPCE` envelope: the tag and length
//! constants, the per-kind body encoders, and the per-kind decoders that
//! rebuild operation payloads through their crate-private constructors,
//! re-validating every canonical field invariant. The aggregate shell
//! (`conversation.rs`) owns event selection, the durability barrier, and
//! replay; it delegates every byte-level concern here.

use alloc::vec::Vec;

use crate::wire::{
    ConversationId, DetachAttemptToken, Generation, LeaveAttemptToken, LeaveCommitted,
};

use super::conversation::{ConversationEventBody, ConversationEventDecodeError};
use super::operation_event::{
    AttachedOperation, BindingFateOperation, ConversationOperation, DetachedOperation,
    EnrolledOperation, LeftOperation, NonzeroDebtAckOperation,
};

const EVENT_MAGIC: [u8; 4] = *b"LPCE";
const EVENT_CODEC_MAJOR: u16 = 1;
const EVENT_CODEC_MINOR: u16 = 0;
const GENESIS_VALIDATED_TAG: u16 = 1;
const PARTICIPANT_ENROLLED_TAG: u16 = 2;
const PARTICIPANT_ATTACHED_TAG: u16 = 3;
const PARTICIPANT_DETACHED_TAG: u16 = 4;
const PARTICIPANT_LEFT_TAG: u16 = 5;
const BINDING_FATE_RECORDED_TAG: u16 = 6;
const NONZERO_DEBT_ACK_RECORDED_TAG: u16 = 7;
pub(super) const EVENT_HEADER_LEN: usize = 30;
const BINDING_EPOCH_LEN: u32 = 24;
const ATTACH_SHAPED_BODY_LEN: u32 = 48;
const DETACHED_BODY_LEN: u32 = 64;
const LEFT_BODY_BASE_LEN: u32 = 49;
const BINDING_FATE_BODY_LEN: u32 = 40;
const NONZERO_DEBT_ACK_BODY_LEN: u32 = 24;
const LEFT_ENDED_EPOCH_FLAG: u8 = 0b01;
const LEFT_PRIOR_TERMINAL_FLAG: u8 = 0b10;
const LEFT_FLAG_MASK: u8 = LEFT_ENDED_EPOCH_FLAG | LEFT_PRIOR_TERMINAL_FLAG;

/// Returns the stable body tag assigned to one event body kind.
const fn body_tag(body: &ConversationEventBody) -> u16 {
    match body {
        ConversationEventBody::GenesisValidated => GENESIS_VALIDATED_TAG,
        ConversationEventBody::Operation(operation) => match operation {
            ConversationOperation::Enrolled(_) => PARTICIPANT_ENROLLED_TAG,
            ConversationOperation::Attached(_) => PARTICIPANT_ATTACHED_TAG,
            ConversationOperation::Detached(_) => PARTICIPANT_DETACHED_TAG,
            ConversationOperation::Left(_) => PARTICIPANT_LEFT_TAG,
            ConversationOperation::BindingFate(_) => BINDING_FATE_RECORDED_TAG,
            ConversationOperation::NonzeroDebtAck(_) => NONZERO_DEBT_ACK_RECORDED_TAG,
        },
    }
}

/// Returns the exact canonical body length of one event body.
pub(super) const fn encoded_body_len(body: &ConversationEventBody) -> u32 {
    match body {
        ConversationEventBody::GenesisValidated => 0,
        ConversationEventBody::Operation(operation) => match operation {
            ConversationOperation::Enrolled(_) | ConversationOperation::Attached(_) => {
                ATTACH_SHAPED_BODY_LEN
            }
            ConversationOperation::Detached(_) => DETACHED_BODY_LEN,
            ConversationOperation::Left(operation) => {
                let committed = operation.committed();
                let mut length = LEFT_BODY_BASE_LEN;
                if committed.ended_binding_epoch().is_some() {
                    length += BINDING_EPOCH_LEN;
                }
                if committed.prior_terminal_delivery_seq().is_some() {
                    length += 8;
                }
                length
            }
            ConversationOperation::BindingFate(_) => BINDING_FATE_BODY_LEN,
            ConversationOperation::NonzeroDebtAck(_) => NONZERO_DEBT_ACK_BODY_LEN,
        },
    }
}

/// Encodes the stable v1 event envelope in network byte order.
pub(super) fn encode_event(
    conversation_id: ConversationId,
    ordinal: u64,
    body: &ConversationEventBody,
) -> Vec<u8> {
    let body_len = encoded_body_len(body);
    let mut encoded = Vec::with_capacity(EVENT_HEADER_LEN + body_len as usize);
    encoded.extend_from_slice(&EVENT_MAGIC);
    encoded.extend_from_slice(&EVENT_CODEC_MAJOR.to_be_bytes());
    encoded.extend_from_slice(&EVENT_CODEC_MINOR.to_be_bytes());
    encoded.extend_from_slice(&conversation_id.to_be_bytes());
    encoded.extend_from_slice(&ordinal.to_be_bytes());
    encoded.extend_from_slice(&body_tag(body).to_be_bytes());
    encoded.extend_from_slice(&body_len.to_be_bytes());
    encode_body_into(body, &mut encoded);
    encoded
}

fn encode_body_into(body: &ConversationEventBody, encoded: &mut Vec<u8>) {
    match body {
        ConversationEventBody::GenesisValidated => {}
        ConversationEventBody::Operation(ConversationOperation::Enrolled(operation)) => {
            encoded.extend_from_slice(&operation.participant_id().to_be_bytes());
            encode_epoch_into(encoded, operation.binding_epoch());
            encoded.extend_from_slice(&operation.attached_transaction_order().to_be_bytes());
            encoded.extend_from_slice(&operation.attached_delivery_seq().to_be_bytes());
        }
        ConversationEventBody::Operation(ConversationOperation::Attached(operation)) => {
            encoded.extend_from_slice(&operation.participant_id().to_be_bytes());
            encode_epoch_into(encoded, operation.binding_epoch());
            encoded.extend_from_slice(&operation.attached_transaction_order().to_be_bytes());
            encoded.extend_from_slice(&operation.attached_delivery_seq().to_be_bytes());
        }
        ConversationEventBody::Operation(ConversationOperation::Detached(operation)) => {
            encoded.extend_from_slice(operation.detach_attempt_token().as_bytes());
            encoded.extend_from_slice(&operation.participant_id().to_be_bytes());
            encode_epoch_into(encoded, operation.committed_binding_epoch());
            encoded.extend_from_slice(&operation.detached_transaction_order().to_be_bytes());
            encoded.extend_from_slice(&operation.detached_delivery_seq().to_be_bytes());
        }
        ConversationEventBody::Operation(ConversationOperation::Left(operation)) => {
            let committed = operation.committed();
            encoded.extend_from_slice(committed.leave_attempt_token().as_bytes());
            encoded.extend_from_slice(&committed.participant_id().to_be_bytes());
            encoded.extend_from_slice(&committed.retired_generation().get().to_be_bytes());
            let mut flags = 0_u8;
            if committed.ended_binding_epoch().is_some() {
                flags |= LEFT_ENDED_EPOCH_FLAG;
            }
            if committed.prior_terminal_delivery_seq().is_some() {
                flags |= LEFT_PRIOR_TERMINAL_FLAG;
            }
            encoded.push(flags);
            if let Some(epoch) = committed.ended_binding_epoch() {
                encode_epoch_into(encoded, epoch);
            }
            if let Some(prior) = committed.prior_terminal_delivery_seq() {
                encoded.extend_from_slice(&prior.to_be_bytes());
            }
            encoded.extend_from_slice(&operation.left_transaction_order().to_be_bytes());
            encoded.extend_from_slice(&committed.left_delivery_seq().to_be_bytes());
        }
        ConversationEventBody::Operation(ConversationOperation::BindingFate(operation)) => {
            encoded.extend_from_slice(&operation.participant_id().to_be_bytes());
            encode_epoch_into(encoded, operation.last_dead_binding_epoch());
            encoded.extend_from_slice(&operation.resulting_floor().to_be_bytes());
        }
        ConversationEventBody::Operation(ConversationOperation::NonzeroDebtAck(operation)) => {
            encoded.extend_from_slice(&operation.participant_id().to_be_bytes());
            encoded.extend_from_slice(&operation.capability_generation().get().to_be_bytes());
            encoded.extend_from_slice(&operation.through_seq().to_be_bytes());
        }
    }
}

/// Decodes one exact canonical v1 event envelope into its header fields and
/// re-validated body.
///
/// # Errors
///
/// Returns [`ConversationEventDecodeError`] for truncation, an invalid magic
/// prefix, an unsupported codec version or body tag, or any declared body
/// length that differs from the complete supplied frame.
pub(super) fn decode_event(
    input: &[u8],
) -> Result<(ConversationId, u64, ConversationEventBody), ConversationEventDecodeError> {
    if input.len() < EVENT_HEADER_LEN {
        return Err(ConversationEventDecodeError::Truncated {
            required: EVENT_HEADER_LEN,
            available: input.len(),
        });
    }
    if input.get(0..4) != Some(EVENT_MAGIC.as_slice()) {
        return Err(ConversationEventDecodeError::InvalidMagic);
    }
    let codec_major = take_u16(input, 4)?;
    let codec_minor = take_u16(input, 6)?;
    if (codec_major, codec_minor) != (EVENT_CODEC_MAJOR, EVENT_CODEC_MINOR) {
        return Err(ConversationEventDecodeError::UnsupportedCodec {
            major: codec_major,
            minor: codec_minor,
        });
    }
    let conversation_id = take_u64(input, 8)?;
    let ordinal = take_u64(input, 16)?;
    let event_tag = take_u16(input, 24)?;
    let declared_body_len = take_u32(input, 26)?;
    let declared_body_len_usize = usize::try_from(declared_body_len)
        .map_err(|_| ConversationEventDecodeError::LengthOverflow)?;
    let actual_body_len = input.len() - EVENT_HEADER_LEN;
    if declared_body_len_usize != actual_body_len {
        return Err(ConversationEventDecodeError::NonCanonicalLength {
            declared_body_len,
            actual_body_len,
        });
    }
    let body = match event_tag {
        GENESIS_VALIDATED_TAG if declared_body_len == 0 => ConversationEventBody::GenesisValidated,
        GENESIS_VALIDATED_TAG => {
            return Err(ConversationEventDecodeError::NonCanonicalLength {
                declared_body_len,
                actual_body_len,
            });
        }
        PARTICIPANT_ENROLLED_TAG => {
            require_body_len(ATTACH_SHAPED_BODY_LEN, declared_body_len, actual_body_len)?;
            decode_enrolled(conversation_id, input)?
        }
        PARTICIPANT_ATTACHED_TAG => {
            require_body_len(ATTACH_SHAPED_BODY_LEN, declared_body_len, actual_body_len)?;
            decode_attached(conversation_id, input)?
        }
        PARTICIPANT_DETACHED_TAG => {
            require_body_len(DETACHED_BODY_LEN, declared_body_len, actual_body_len)?;
            decode_detached(conversation_id, input)?
        }
        PARTICIPANT_LEFT_TAG => {
            decode_left(conversation_id, input, declared_body_len, actual_body_len)?
        }
        BINDING_FATE_RECORDED_TAG => {
            require_body_len(BINDING_FATE_BODY_LEN, declared_body_len, actual_body_len)?;
            decode_binding_fate(conversation_id, input)?
        }
        NONZERO_DEBT_ACK_RECORDED_TAG => {
            require_body_len(
                NONZERO_DEBT_ACK_BODY_LEN,
                declared_body_len,
                actual_body_len,
            )?;
            decode_nonzero_debt_ack(conversation_id, input)?
        }
        unknown => {
            return Err(ConversationEventDecodeError::UnknownEventKind { tag: unknown });
        }
    };
    Ok((conversation_id, ordinal, body))
}

fn take_u16(input: &[u8], start: usize) -> Result<u16, ConversationEventDecodeError> {
    let end = start
        .checked_add(2)
        .ok_or(ConversationEventDecodeError::LengthOverflow)?;
    let bytes: [u8; 2] = input
        .get(start..end)
        .ok_or(ConversationEventDecodeError::Truncated {
            required: end,
            available: input.len(),
        })?
        .try_into()
        .map_err(|_| ConversationEventDecodeError::LengthOverflow)?;
    Ok(u16::from_be_bytes(bytes))
}

fn take_u32(input: &[u8], start: usize) -> Result<u32, ConversationEventDecodeError> {
    let end = start
        .checked_add(4)
        .ok_or(ConversationEventDecodeError::LengthOverflow)?;
    let bytes: [u8; 4] = input
        .get(start..end)
        .ok_or(ConversationEventDecodeError::Truncated {
            required: end,
            available: input.len(),
        })?
        .try_into()
        .map_err(|_| ConversationEventDecodeError::LengthOverflow)?;
    Ok(u32::from_be_bytes(bytes))
}

fn take_u64(input: &[u8], start: usize) -> Result<u64, ConversationEventDecodeError> {
    let end = start
        .checked_add(8)
        .ok_or(ConversationEventDecodeError::LengthOverflow)?;
    let bytes: [u8; 8] = input
        .get(start..end)
        .ok_or(ConversationEventDecodeError::Truncated {
            required: end,
            available: input.len(),
        })?
        .try_into()
        .map_err(|_| ConversationEventDecodeError::LengthOverflow)?;
    Ok(u64::from_be_bytes(bytes))
}

fn take_u8(input: &[u8], start: usize) -> Result<u8, ConversationEventDecodeError> {
    input
        .get(start)
        .copied()
        .ok_or_else(|| ConversationEventDecodeError::Truncated {
            required: start.saturating_add(1),
            available: input.len(),
        })
}

fn take_bytes16(input: &[u8], start: usize) -> Result<[u8; 16], ConversationEventDecodeError> {
    let end = start
        .checked_add(16)
        .ok_or(ConversationEventDecodeError::LengthOverflow)?;
    input
        .get(start..end)
        .ok_or(ConversationEventDecodeError::Truncated {
            required: end,
            available: input.len(),
        })?
        .try_into()
        .map_err(|_| ConversationEventDecodeError::LengthOverflow)
}

fn take_epoch(
    input: &[u8],
    start: usize,
    tag: u16,
) -> Result<crate::wire::BindingEpoch, ConversationEventDecodeError> {
    let server_incarnation = take_u64(input, start)?;
    let connection_ordinal = take_u64(input, start.saturating_add(8))?;
    let raw_generation = take_u64(input, start.saturating_add(16))?;
    let capability_generation = Generation::new(raw_generation)
        .ok_or(ConversationEventDecodeError::NonCanonicalBody { tag })?;
    Ok(crate::wire::BindingEpoch::new(
        crate::wire::ConnectionIncarnation::new(server_incarnation, connection_ordinal),
        capability_generation,
    ))
}

fn take_generation(
    input: &[u8],
    start: usize,
    tag: u16,
) -> Result<Generation, ConversationEventDecodeError> {
    Generation::new(take_u64(input, start)?)
        .ok_or(ConversationEventDecodeError::NonCanonicalBody { tag })
}

fn encode_epoch_into(encoded: &mut Vec<u8>, epoch: crate::wire::BindingEpoch) {
    encoded.extend_from_slice(
        &epoch
            .connection_incarnation
            .server_incarnation
            .to_be_bytes(),
    );
    encoded.extend_from_slice(
        &epoch
            .connection_incarnation
            .connection_ordinal
            .to_be_bytes(),
    );
    encoded.extend_from_slice(&epoch.capability_generation.get().to_be_bytes());
}

const fn require_body_len(
    expected: u32,
    declared_body_len: u32,
    actual_body_len: usize,
) -> Result<(), ConversationEventDecodeError> {
    if declared_body_len == expected {
        Ok(())
    } else {
        Err(ConversationEventDecodeError::NonCanonicalLength {
            declared_body_len,
            actual_body_len,
        })
    }
}

fn decode_enrolled(
    conversation_id: ConversationId,
    input: &[u8],
) -> Result<ConversationEventBody, ConversationEventDecodeError> {
    let participant_id = take_u64(input, EVENT_HEADER_LEN)?;
    let binding_epoch = take_epoch(input, EVENT_HEADER_LEN + 8, PARTICIPANT_ENROLLED_TAG)?;
    let attached_transaction_order = take_u64(input, EVENT_HEADER_LEN + 32)?;
    let attached_delivery_seq = take_u64(input, EVENT_HEADER_LEN + 40)?;
    let operation = EnrolledOperation::from_decoded(
        conversation_id,
        participant_id,
        binding_epoch,
        attached_transaction_order,
        attached_delivery_seq,
    )
    .ok_or(ConversationEventDecodeError::NonCanonicalBody {
        tag: PARTICIPANT_ENROLLED_TAG,
    })?;
    Ok(ConversationEventBody::Operation(
        ConversationOperation::Enrolled(operation),
    ))
}

fn decode_attached(
    conversation_id: ConversationId,
    input: &[u8],
) -> Result<ConversationEventBody, ConversationEventDecodeError> {
    let participant_id = take_u64(input, EVENT_HEADER_LEN)?;
    let binding_epoch = take_epoch(input, EVENT_HEADER_LEN + 8, PARTICIPANT_ATTACHED_TAG)?;
    let attached_transaction_order = take_u64(input, EVENT_HEADER_LEN + 32)?;
    let attached_delivery_seq = take_u64(input, EVENT_HEADER_LEN + 40)?;
    let operation = AttachedOperation::from_decoded(
        conversation_id,
        participant_id,
        binding_epoch,
        attached_transaction_order,
        attached_delivery_seq,
    )
    .ok_or(ConversationEventDecodeError::NonCanonicalBody {
        tag: PARTICIPANT_ATTACHED_TAG,
    })?;
    Ok(ConversationEventBody::Operation(
        ConversationOperation::Attached(operation),
    ))
}

fn decode_detached(
    conversation_id: ConversationId,
    input: &[u8],
) -> Result<ConversationEventBody, ConversationEventDecodeError> {
    let detach_attempt_token = DetachAttemptToken::new(take_bytes16(input, EVENT_HEADER_LEN)?);
    let participant_id = take_u64(input, EVENT_HEADER_LEN + 16)?;
    let committed_binding_epoch =
        take_epoch(input, EVENT_HEADER_LEN + 24, PARTICIPANT_DETACHED_TAG)?;
    let detached_transaction_order = take_u64(input, EVENT_HEADER_LEN + 48)?;
    let detached_delivery_seq = take_u64(input, EVENT_HEADER_LEN + 56)?;
    Ok(ConversationEventBody::Operation(
        ConversationOperation::Detached(DetachedOperation::from_decoded(
            detach_attempt_token,
            conversation_id,
            participant_id,
            committed_binding_epoch,
            detached_transaction_order,
            detached_delivery_seq,
        )),
    ))
}

fn decode_left(
    conversation_id: ConversationId,
    input: &[u8],
    declared_body_len: u32,
    actual_body_len: usize,
) -> Result<ConversationEventBody, ConversationEventDecodeError> {
    if declared_body_len < LEFT_BODY_BASE_LEN {
        return Err(ConversationEventDecodeError::NonCanonicalLength {
            declared_body_len,
            actual_body_len,
        });
    }
    let leave_attempt_token = LeaveAttemptToken::new(take_bytes16(input, EVENT_HEADER_LEN)?);
    let participant_id = take_u64(input, EVENT_HEADER_LEN + 16)?;
    let retired_generation = take_generation(input, EVENT_HEADER_LEN + 24, PARTICIPANT_LEFT_TAG)?;
    let flags = take_u8(input, EVENT_HEADER_LEN + 32)?;
    if flags & !LEFT_FLAG_MASK != 0 {
        return Err(ConversationEventDecodeError::NonCanonicalBody {
            tag: PARTICIPANT_LEFT_TAG,
        });
    }
    let mut expected_body_len = LEFT_BODY_BASE_LEN;
    if flags & LEFT_ENDED_EPOCH_FLAG != 0 {
        expected_body_len += BINDING_EPOCH_LEN;
    }
    if flags & LEFT_PRIOR_TERMINAL_FLAG != 0 {
        expected_body_len += 8;
    }
    if declared_body_len != expected_body_len {
        return Err(ConversationEventDecodeError::NonCanonicalLength {
            declared_body_len,
            actual_body_len,
        });
    }
    let mut cursor = EVENT_HEADER_LEN + 33;
    let ended_binding_epoch = if flags & LEFT_ENDED_EPOCH_FLAG == 0 {
        None
    } else {
        let epoch = take_epoch(input, cursor, PARTICIPANT_LEFT_TAG)?;
        cursor += BINDING_EPOCH_LEN as usize;
        Some(epoch)
    };
    let prior_terminal_delivery_seq = if flags & LEFT_PRIOR_TERMINAL_FLAG == 0 {
        None
    } else {
        let prior = take_u64(input, cursor)?;
        cursor += 8;
        Some(prior)
    };
    let left_transaction_order = take_u64(input, cursor)?;
    let left_delivery_seq = take_u64(input, cursor + 8)?;
    let committed = LeaveCommitted::new(
        conversation_id,
        leave_attempt_token,
        participant_id,
        retired_generation,
        ended_binding_epoch,
        prior_terminal_delivery_seq,
        left_delivery_seq,
    )
    .ok_or(ConversationEventDecodeError::NonCanonicalBody {
        tag: PARTICIPANT_LEFT_TAG,
    })?;
    Ok(ConversationEventBody::Operation(
        ConversationOperation::Left(LeftOperation::from_decoded(
            committed,
            left_transaction_order,
        )),
    ))
}

fn decode_binding_fate(
    conversation_id: ConversationId,
    input: &[u8],
) -> Result<ConversationEventBody, ConversationEventDecodeError> {
    let participant_id = take_u64(input, EVENT_HEADER_LEN)?;
    let last_dead_binding_epoch =
        take_epoch(input, EVENT_HEADER_LEN + 8, BINDING_FATE_RECORDED_TAG)?;
    let resulting_floor = take_u64(input, EVENT_HEADER_LEN + 32)?;
    Ok(ConversationEventBody::Operation(
        ConversationOperation::BindingFate(BindingFateOperation::from_decoded(
            conversation_id,
            participant_id,
            last_dead_binding_epoch,
            resulting_floor,
        )),
    ))
}

fn decode_nonzero_debt_ack(
    conversation_id: ConversationId,
    input: &[u8],
) -> Result<ConversationEventBody, ConversationEventDecodeError> {
    let participant_id = take_u64(input, EVENT_HEADER_LEN)?;
    let capability_generation =
        take_generation(input, EVENT_HEADER_LEN + 8, NONZERO_DEBT_ACK_RECORDED_TAG)?;
    let through_seq = take_u64(input, EVENT_HEADER_LEN + 16)?;
    Ok(ConversationEventBody::Operation(
        ConversationOperation::NonzeroDebtAck(NonzeroDebtAckOperation::from_decoded(
            conversation_id,
            participant_id,
            capability_generation,
            through_seq,
        )),
    ))
}
