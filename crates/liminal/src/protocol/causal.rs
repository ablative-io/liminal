use std::str;

use super::error::ProtocolError;

const ABSENT_TAG: u8 = 0;
const PRESENT_TAG: u8 = 1;
const U8_LEN: usize = 1;
const U32_LEN: usize = 4;
const U64_LEN: usize = 8;
const ENVELOPE_SCHEMA_ID_LEN: usize = 32;

/// Protocol-level opaque identifier for a message in a causal chain.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MessageId(
    /// Stable UTF-8 identifier bytes used on the wire.
    pub String,
);

impl MessageId {
    /// Wrap a stable message identifier string.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Return the identifier as a UTF-8 string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume this identifier and return the wrapped string.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl From<String> for MessageId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for MessageId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

/// Causal metadata carried inside a protocol message envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CausalContext {
    /// Optional reference to the causally preceding message.
    pub parent_id: Option<MessageId>,
    /// Optional logical timestamp for this message's causal-chain position.
    pub vector_clock_entry: Option<u64>,
}

impl CausalContext {
    /// Create a causally independent context with no parent or vector-clock entry.
    #[must_use]
    pub const fn independent() -> Self {
        Self {
            parent_id: None,
            vector_clock_entry: None,
        }
    }

    /// Create a context that follows `parent_id` in a causal chain.
    #[must_use]
    pub const fn with_parent(parent_id: MessageId) -> Self {
        Self {
            parent_id: Some(parent_id),
            vector_clock_entry: None,
        }
    }

    /// Return the number of bytes needed for deterministic causal-context encoding.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::CodecError`] when the message id length cannot fit
    /// in the protocol's `u32` length field or the total length overflows `usize`.
    pub fn encoded_len(&self) -> Result<usize, ProtocolError> {
        let parent_len = match &self.parent_id {
            Some(parent_id) => sum_lengths(&[
                U8_LEN,
                U32_LEN,
                checked_u32_len(parent_id.as_str().len(), "message id")?,
            ])?,
            None => U8_LEN,
        };
        let vector_len = if self.vector_clock_entry.is_some() {
            U8_LEN + U64_LEN
        } else {
            U8_LEN
        };
        sum_lengths(&[parent_len, vector_len])
    }

    /// Serialize this causal context with deterministic big-endian field encoding.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::CodecError`] when a length field cannot represent
    /// the encoded context.
    pub fn serialize(&self) -> Result<Vec<u8>, ProtocolError> {
        let len = self.encoded_len()?;
        let mut bytes = Vec::with_capacity(len);

        match &self.parent_id {
            Some(parent_id) => {
                bytes.push(PRESENT_TAG);
                write_u32(&mut bytes, parent_id.as_str().len(), "message id")?;
                bytes.extend_from_slice(parent_id.as_str().as_bytes());
            }
            None => bytes.push(ABSENT_TAG),
        }

        match self.vector_clock_entry {
            Some(entry) => {
                bytes.push(PRESENT_TAG);
                bytes.extend_from_slice(&entry.to_be_bytes());
            }
            None => bytes.push(ABSENT_TAG),
        }

        if bytes.len() == len {
            Ok(bytes)
        } else {
            Err(ProtocolError::codec(
                "causal context encoder produced an unexpected length",
            ))
        }
    }

    /// Serialize this causal context for wire transport.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::CodecError`] when a length field cannot represent
    /// the encoded context.
    pub fn to_wire_bytes(&self) -> Result<Vec<u8>, ProtocolError> {
        self.serialize()
    }

    /// Deserialize a causal context from deterministic wire bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::CodecError`] when the bytes are truncated, contain
    /// invalid presence tags, contain invalid UTF-8 message ids, or contain
    /// trailing bytes.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, ProtocolError> {
        Self::from_wire_bytes(bytes)
    }

    /// Deserialize a causal context from deterministic wire bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::CodecError`] when the bytes are truncated, contain
    /// invalid presence tags, contain invalid UTF-8 message ids, or contain
    /// trailing bytes.
    pub fn from_wire_bytes(bytes: &[u8]) -> Result<Self, ProtocolError> {
        let mut offset = 0;
        let parent_id = match read_u8(bytes, &mut offset, "parent id presence tag")? {
            ABSENT_TAG => None,
            PRESENT_TAG => {
                let len = read_u32_as_usize(bytes, &mut offset, "message id length")?;
                let id_bytes = read_slice(bytes, &mut offset, len, "message id bytes")?;
                let id = str::from_utf8(id_bytes)
                    .map_err(|_| ProtocolError::codec("message id was not valid utf-8"))?;
                Some(MessageId::new(id))
            }
            _ => return Err(ProtocolError::codec("parent id presence tag was invalid")),
        };

        let vector_clock_entry = match read_u8(bytes, &mut offset, "vector clock presence tag")? {
            ABSENT_TAG => None,
            PRESENT_TAG => Some(read_u64(bytes, &mut offset, "vector clock entry")?),
            _ => {
                return Err(ProtocolError::codec(
                    "vector clock presence tag was invalid",
                ));
            }
        };

        if offset == bytes.len() {
            Ok(Self {
                parent_id,
                vector_clock_entry,
            })
        } else {
            Err(ProtocolError::codec(
                "causal context contained trailing bytes",
            ))
        }
    }
}

impl Default for CausalContext {
    fn default() -> Self {
        Self::independent()
    }
}

/// Extract causal context bytes from a serialized message envelope without parsing payload bytes.
///
/// The envelope layout is `schema_id(32) + causal_context_length(u32) +
/// causal_context_bytes + payload_length(u32) + payload_bytes`; this function
/// reads only the schema prefix, causal-context length, and causal-context bytes.
///
/// # Errors
///
/// Returns [`ProtocolError::CodecError`] when the envelope is truncated before or
/// within the causal-context section, or when the causal-context bytes are
/// malformed.
pub fn extract_causal_context(envelope_bytes: &[u8]) -> Result<CausalContext, ProtocolError> {
    let mut offset = 0;
    let _schema_id = read_slice(
        envelope_bytes,
        &mut offset,
        ENVELOPE_SCHEMA_ID_LEN,
        "schema id",
    )?;
    let causal_len = read_u32_as_usize(envelope_bytes, &mut offset, "causal context length")?;
    let causal_bytes = read_slice(
        envelope_bytes,
        &mut offset,
        causal_len,
        "causal context bytes",
    )?;
    CausalContext::deserialize(causal_bytes)
}

fn checked_u32_len(len: usize, field: &str) -> Result<usize, ProtocolError> {
    u32::try_from(len)
        .map(|_| len)
        .map_err(|_| ProtocolError::codec(format!("{field} length exceeded u32::MAX")))
}

fn sum_lengths(parts: &[usize]) -> Result<usize, ProtocolError> {
    let mut total = 0_usize;
    for part in parts {
        total = total
            .checked_add(*part)
            .ok_or_else(|| ProtocolError::codec("causal context length overflowed usize"))?;
    }
    Ok(total)
}

fn write_u32(buffer: &mut Vec<u8>, value: usize, field: &str) -> Result<(), ProtocolError> {
    let value = u32::try_from(value)
        .map_err(|_| ProtocolError::codec(format!("{field} length exceeded u32::MAX")))?;
    buffer.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn read_u8(bytes: &[u8], offset: &mut usize, field: &str) -> Result<u8, ProtocolError> {
    let bytes = read_slice(bytes, offset, U8_LEN, field)?;
    let [value] = bytes else {
        return Err(ProtocolError::codec(format!("{field} was truncated")));
    };
    Ok(*value)
}

fn read_u32_as_usize(
    bytes: &[u8],
    offset: &mut usize,
    field: &str,
) -> Result<usize, ProtocolError> {
    let bytes = read_slice(bytes, offset, U32_LEN, field)?;
    let [b0, b1, b2, b3] = bytes else {
        return Err(ProtocolError::codec(format!("{field} was truncated")));
    };
    usize::try_from(u32::from_be_bytes([*b0, *b1, *b2, *b3]))
        .map_err(|_| ProtocolError::codec(format!("{field} cannot fit usize")))
}

fn read_u64(bytes: &[u8], offset: &mut usize, field: &str) -> Result<u64, ProtocolError> {
    let bytes = read_slice(bytes, offset, U64_LEN, field)?;
    let [b0, b1, b2, b3, b4, b5, b6, b7] = bytes else {
        return Err(ProtocolError::codec(format!("{field} was truncated")));
    };
    Ok(u64::from_be_bytes([*b0, *b1, *b2, *b3, *b4, *b5, *b6, *b7]))
}

fn read_slice<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    len: usize,
    field: &str,
) -> Result<&'a [u8], ProtocolError> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| ProtocolError::codec(format!("{field} offset overflowed usize")))?;
    let Some(slice) = bytes.get(*offset..end) else {
        return Err(ProtocolError::codec(format!(
            "{field} exceeded available bytes"
        )));
    };
    *offset = end;
    Ok(slice)
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use super::{CausalContext, MessageId, extract_causal_context};
    use crate::protocol::{MessageEnvelope, ProtocolError, SchemaId};

    #[test]
    fn causal_context_trait_bounds_are_available() {
        fn assert_traits<T: Debug + Clone + PartialEq + Eq>() {}

        assert_traits::<CausalContext>();
    }

    #[test]
    fn constructors_create_expected_context_shapes() {
        let independent = CausalContext::independent();
        assert_eq!(independent.parent_id, None);
        assert_eq!(independent.vector_clock_entry, None);

        let parent = MessageId::from("parent-1");
        let child = CausalContext::with_parent(parent.clone());
        assert_eq!(child.parent_id, Some(parent));
        assert_eq!(child.vector_clock_entry, None);
    }

    #[test]
    fn causal_context_serialization_round_trips() -> Result<(), ProtocolError> {
        let context = CausalContext {
            parent_id: Some(MessageId::from("parent-1")),
            vector_clock_entry: Some(7),
        };
        let encoded = context.serialize()?;
        let decoded = CausalContext::deserialize(&encoded)?;

        assert_eq!(decoded, context);
        assert_eq!(encoded, context.serialize()?);
        Ok(())
    }

    #[test]
    fn independent_context_serializes_as_absent_fields() -> Result<(), ProtocolError> {
        let encoded = CausalContext::independent().serialize()?;

        assert_eq!(encoded, vec![0, 0]);
        assert_eq!(
            CausalContext::deserialize(&encoded)?,
            CausalContext::independent()
        );
        Ok(())
    }

    #[test]
    fn extract_reads_causal_context_without_payload_parsing() -> Result<(), ProtocolError> {
        let context = CausalContext {
            parent_id: Some(MessageId::from("parent-2")),
            vector_clock_entry: Some(11),
        };
        let envelope = MessageEnvelope::new(
            SchemaId::new([0xAB; 32]),
            context.clone(),
            vec![0xFF, 0xFE, 0xFD],
        );
        let encoded = envelope.serialize()?;

        assert_eq!(extract_causal_context(&encoded)?, context);
        Ok(())
    }
}
