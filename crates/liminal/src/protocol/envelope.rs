use super::causal::CausalContext;
use super::error::ProtocolError;

const U32_LEN: usize = 4;

/// Content schema hash that identifies the opaque payload's type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SchemaId([u8; Self::WIRE_LEN]);

impl SchemaId {
    /// Number of bytes used by the schema id hash on the wire.
    pub const WIRE_LEN: usize = 32;

    /// Wrap a 32-byte content schema hash.
    #[must_use]
    pub const fn new(bytes: [u8; Self::WIRE_LEN]) -> Self {
        Self(bytes)
    }

    /// Return the wrapped hash bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; Self::WIRE_LEN] {
        &self.0
    }

    /// Consume this schema id and return its hash bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; Self::WIRE_LEN] {
        self.0
    }
}

/// Protocol message envelope carried by publish and conversation-message frames.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageEnvelope {
    /// Content type hash identifying the payload schema.
    pub schema_id: SchemaId,
    /// Structured causal metadata used by the bus for ordering decisions.
    pub causal_context: CausalContext,
    /// Opaque application payload bytes.
    pub payload: Vec<u8>,
}

impl MessageEnvelope {
    /// Create a protocol message envelope from all of its fields.
    #[must_use]
    pub const fn new(schema_id: SchemaId, causal_context: CausalContext, payload: Vec<u8>) -> Self {
        Self {
            schema_id,
            causal_context,
            payload,
        }
    }

    /// Return the deterministic encoded length of this envelope.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::CodecError`] when the causal-context or payload
    /// length cannot fit in the protocol's `u32` length fields, or when the total
    /// length overflows `usize`.
    pub fn encoded_len(&self) -> Result<usize, ProtocolError> {
        let causal_len = self.causal_context.encoded_len()?;
        checked_u32_len(causal_len, "causal context")?;
        checked_u32_len(self.payload.len(), "payload")?;
        sum_lengths(&[
            SchemaId::WIRE_LEN,
            U32_LEN,
            causal_len,
            U32_LEN,
            self.payload.len(),
        ])
    }

    /// Serialize this envelope with deterministic big-endian field encoding.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::CodecError`] when a length field cannot represent
    /// the encoded envelope.
    pub fn serialize(&self) -> Result<Vec<u8>, ProtocolError> {
        let causal_bytes = self.causal_context.serialize()?;
        checked_u32_len(causal_bytes.len(), "causal context")?;
        checked_u32_len(self.payload.len(), "payload")?;
        let len = sum_lengths(&[
            SchemaId::WIRE_LEN,
            U32_LEN,
            causal_bytes.len(),
            U32_LEN,
            self.payload.len(),
        ])?;
        let mut bytes = Vec::with_capacity(len);

        bytes.extend_from_slice(self.schema_id.as_bytes());
        write_u32(&mut bytes, causal_bytes.len(), "causal context")?;
        bytes.extend_from_slice(&causal_bytes);
        write_u32(&mut bytes, self.payload.len(), "payload")?;
        bytes.extend_from_slice(&self.payload);

        if bytes.len() == len {
            Ok(bytes)
        } else {
            Err(ProtocolError::codec(
                "message envelope encoder produced an unexpected length",
            ))
        }
    }

    /// Serialize this envelope for wire transport.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::CodecError`] when a length field cannot represent
    /// the encoded envelope.
    pub fn to_wire_bytes(&self) -> Result<Vec<u8>, ProtocolError> {
        self.serialize()
    }

    /// Deserialize an envelope from deterministic wire bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::CodecError`] when the bytes are truncated,
    /// contain malformed causal context bytes, or contain trailing bytes.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, ProtocolError> {
        Self::from_wire_bytes(bytes)
    }

    /// Deserialize an envelope from deterministic wire bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::CodecError`] when the bytes are truncated,
    /// contain malformed causal context bytes, or contain trailing bytes.
    pub fn from_wire_bytes(bytes: &[u8]) -> Result<Self, ProtocolError> {
        let mut offset = 0;
        let schema_id = SchemaId::new(read_schema_id(bytes, &mut offset)?);
        let causal_len = read_u32_as_usize(bytes, &mut offset, "causal context length")?;
        let causal_bytes = read_slice(bytes, &mut offset, causal_len, "causal context bytes")?;
        let causal_context = CausalContext::deserialize(causal_bytes)?;
        let payload_len = read_u32_as_usize(bytes, &mut offset, "payload length")?;
        let payload = read_slice(bytes, &mut offset, payload_len, "payload bytes")?.to_vec();

        if offset == bytes.len() {
            Ok(Self {
                schema_id,
                causal_context,
                payload,
            })
        } else {
            Err(ProtocolError::codec(
                "message envelope contained trailing bytes",
            ))
        }
    }
}

fn checked_u32_len(len: usize, field: &str) -> Result<(), ProtocolError> {
    u32::try_from(len)
        .map(|_| ())
        .map_err(|_| ProtocolError::codec(format!("{field} length exceeded u32::MAX")))
}

fn sum_lengths(parts: &[usize]) -> Result<usize, ProtocolError> {
    let mut total = 0_usize;
    for part in parts {
        total = total
            .checked_add(*part)
            .ok_or_else(|| ProtocolError::codec("message envelope length overflowed usize"))?;
    }
    Ok(total)
}

fn write_u32(buffer: &mut Vec<u8>, value: usize, field: &str) -> Result<(), ProtocolError> {
    let value = u32::try_from(value)
        .map_err(|_| ProtocolError::codec(format!("{field} length exceeded u32::MAX")))?;
    buffer.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn read_schema_id(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<[u8; SchemaId::WIRE_LEN], ProtocolError> {
    let schema_bytes = read_slice(bytes, offset, SchemaId::WIRE_LEN, "schema id")?;
    let mut schema_id = [0_u8; SchemaId::WIRE_LEN];
    schema_id.copy_from_slice(schema_bytes);
    Ok(schema_id)
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

    use super::{MessageEnvelope, SchemaId};
    use crate::protocol::{CausalContext, MessageId, ProtocolError, extract_causal_context};

    #[test]
    fn envelope_trait_bounds_are_available() {
        fn assert_schema_traits<T: Debug + Clone + Copy + PartialEq + Eq>() {}
        fn assert_envelope_traits<T: Debug + Clone + PartialEq + Eq>() {}

        assert_schema_traits::<SchemaId>();
        assert_envelope_traits::<MessageEnvelope>();
    }

    #[test]
    fn schema_id_wraps_exactly_thirty_two_bytes() {
        let bytes = [0xAB; SchemaId::WIRE_LEN];
        let schema_id = SchemaId::new(bytes);

        assert_eq!(SchemaId::WIRE_LEN, 32);
        assert_eq!(schema_id.as_bytes(), &bytes);
        assert_eq!(schema_id.into_bytes(), bytes);
    }

    #[test]
    fn constructor_sets_all_fields() {
        let schema_id = SchemaId::new([1; 32]);
        let causal_context = CausalContext::with_parent(MessageId::from("parent"));
        let payload = vec![1, 2, 3];

        let envelope = MessageEnvelope::new(schema_id, causal_context.clone(), payload.clone());

        assert_eq!(envelope.schema_id, schema_id);
        assert_eq!(envelope.causal_context, causal_context);
        assert_eq!(envelope.payload, payload);
    }

    #[test]
    fn identical_fields_produce_identical_bytes() -> Result<(), ProtocolError> {
        let first = sample_envelope(vec![5, 6, 7]);
        let second = sample_envelope(vec![5, 6, 7]);

        assert_eq!(first.serialize()?, second.serialize()?);
        Ok(())
    }

    #[test]
    fn serialization_round_trips_losslessly() -> Result<(), ProtocolError> {
        let envelope = sample_envelope(vec![9, 8, 7]);
        let encoded = envelope.serialize()?;
        let decoded = MessageEnvelope::deserialize(&encoded)?;

        assert_eq!(decoded, envelope);
        Ok(())
    }

    #[test]
    fn encoded_layout_starts_with_schema_id_and_big_endian_lengths() -> Result<(), ProtocolError> {
        let schema_id = SchemaId::new([0x42; 32]);
        let causal_context = CausalContext {
            parent_id: Some(MessageId::from("parent")),
            vector_clock_entry: Some(0x0102_0304_0506_0708),
        };
        let causal_len = causal_context.encoded_len()?;
        let envelope = MessageEnvelope::new(schema_id, causal_context, vec![0xAA, 0xBB]);
        let encoded = envelope.serialize()?;

        assert_eq!(&encoded[..32], schema_id.as_bytes());
        assert_eq!(
            &encoded[32..36],
            &u32::try_from(causal_len)
                .map_err(|_| ProtocolError::codec("test causal length exceeded u32"))?
                .to_be_bytes()
        );
        let payload_len_offset = 36 + causal_len;
        assert_eq!(
            &encoded[payload_len_offset..payload_len_offset + 4],
            &2_u32.to_be_bytes()
        );
        Ok(())
    }

    #[test]
    fn empty_payload_round_trips() -> Result<(), ProtocolError> {
        let envelope = sample_envelope(Vec::new());
        let encoded = envelope.serialize()?;
        let decoded = MessageEnvelope::deserialize(&encoded)?;

        assert_eq!(decoded, envelope);
        assert_eq!(decoded.payload, Vec::<u8>::new());
        Ok(())
    }

    #[test]
    fn causal_context_is_extractable_from_envelope_bytes() -> Result<(), ProtocolError> {
        let envelope = sample_envelope(vec![1, 2, 3, 4]);
        let encoded = envelope.serialize()?;

        assert_eq!(extract_causal_context(&encoded)?, envelope.causal_context);
        Ok(())
    }

    fn sample_envelope(payload: Vec<u8>) -> MessageEnvelope {
        MessageEnvelope::new(
            SchemaId::new([0x11; 32]),
            CausalContext {
                parent_id: Some(MessageId::from("parent-1")),
                vector_clock_entry: Some(99),
            },
            payload,
        )
    }
}
