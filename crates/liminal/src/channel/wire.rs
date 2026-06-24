//! SRV-005: a deterministic byte codec for [`Envelope`] used to carry a
//! published message across a beamr distribution link to a remote subscriber.
//!
//! Cross-node fan-out (the cluster `sync` module in `liminal-server`) sends a
//! published envelope to each remote pg member as a single beamr binary term.
//! The binary payload is encoded here and decoded back inside the remote
//! subscriber process before it lands in that subscriber's inbox. The format is
//! self-describing and length-prefixed so a partial or corrupt frame is
//! rejected rather than mis-parsed — there is no reliance on `serde` or on the
//! beamr ETF term shape, keeping the wire contract owned entirely by liminal.
//!
//! Layout (all integers big-endian):
//! ```text
//! magic:    4 bytes  = b"LMW1"
//! message_id:       16 bytes (UUID)
//! schema_id:        16 bytes (UUID)
//! timestamp_millis:  8 bytes (i64)
//! publisher_id:      4-byte length + UTF-8 bytes
//! payload:           4-byte length + bytes
//! parent_chain_len:  4 bytes (count of MessageId entries)
//! parent_chain:      count * 16 bytes (UUID each)
//! ```

use chrono::{DateTime, TimeZone, Utc};
use uuid::Uuid;

use crate::causal::{CausalContext, MessageId};
use crate::channel::SchemaId;
use crate::envelope::{Envelope, PublisherId};

/// Magic prefix identifying a liminal cross-node envelope frame, version 1.
const MAGIC: [u8; 4] = *b"LMW1";
const UUID_LEN: usize = 16;
const LEN_PREFIX: usize = 4;

/// A reason an [`Envelope`] frame could not be decoded from received bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WireError {
    /// The frame was shorter than the layout requires at some field.
    Truncated,
    /// The leading magic bytes did not match a liminal envelope frame.
    BadMagic,
    /// A UTF-8 field (the publisher id) was not valid UTF-8.
    BadUtf8,
}

impl std::fmt::Display for WireError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => formatter.write_str("cross-node envelope frame was truncated"),
            Self::BadMagic => formatter.write_str("cross-node envelope frame had an unknown magic"),
            Self::BadUtf8 => {
                formatter.write_str("cross-node envelope publisher id was not valid UTF-8")
            }
        }
    }
}

impl std::error::Error for WireError {}

/// Encodes `envelope` into a self-describing, length-prefixed byte frame.
#[must_use]
pub fn encode_envelope(envelope: &Envelope) -> Vec<u8> {
    let publisher = envelope.publisher_id.as_str().as_bytes();
    let chain = envelope
        .causal_context
        .as_ref()
        .map(CausalContext::parent_chain)
        .unwrap_or_default();
    let mut bytes = Vec::with_capacity(
        MAGIC.len()
            + UUID_LEN * 2
            + 8
            + LEN_PREFIX
            + publisher.len()
            + LEN_PREFIX
            + envelope.payload.len()
            + LEN_PREFIX
            + chain.len() * UUID_LEN,
    );
    bytes.extend_from_slice(&MAGIC);
    bytes.extend_from_slice(envelope.message_id.as_uuid().as_bytes());
    bytes.extend_from_slice(envelope.schema_id.as_uuid().as_bytes());
    bytes.extend_from_slice(&envelope.timestamp.timestamp_millis().to_be_bytes());
    write_length_prefixed(&mut bytes, publisher);
    write_length_prefixed(&mut bytes, &envelope.payload);
    write_u32(&mut bytes, u32_len(chain.len()));
    for parent in chain {
        bytes.extend_from_slice(parent.as_uuid().as_bytes());
    }
    bytes
}

/// Decodes an [`Envelope`] from a byte frame produced by [`encode_envelope`].
///
/// # Errors
/// Returns [`WireError`] when the frame is truncated, has an unknown magic, or
/// carries a non-UTF-8 publisher id.
pub fn decode_envelope(bytes: &[u8]) -> Result<Envelope, WireError> {
    let mut cursor = Cursor::new(bytes);
    if cursor.take(MAGIC.len())? != MAGIC {
        return Err(WireError::BadMagic);
    }
    let message_id = MessageId::from_uuid(read_uuid(&mut cursor)?);
    let schema_id = SchemaId::from_uuid(read_uuid(&mut cursor)?);
    let timestamp_millis = i64::from_be_bytes(read_array(&mut cursor)?);
    let timestamp = millis_to_datetime(timestamp_millis);
    let publisher = std::str::from_utf8(cursor.take_length_prefixed()?)
        .map_err(|_| WireError::BadUtf8)?
        .to_owned();
    let payload = cursor.take_length_prefixed()?.to_vec();
    let chain_len = u32::from_be_bytes(read_array(&mut cursor)?) as usize;
    let mut parent_chain = Vec::with_capacity(chain_len);
    for _ in 0..chain_len {
        parent_chain.push(MessageId::from_uuid(read_uuid(&mut cursor)?));
    }
    let causal_context = if parent_chain.is_empty() {
        None
    } else {
        Some(CausalContext::from_parent_chain(parent_chain))
    };
    Ok(Envelope::with_message_id_and_timestamp(
        message_id,
        payload,
        causal_context,
        schema_id,
        PublisherId::from(publisher),
        timestamp,
    ))
}

fn write_length_prefixed(bytes: &mut Vec<u8>, value: &[u8]) {
    write_u32(bytes, u32_len(value.len()));
    bytes.extend_from_slice(value);
}

fn write_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

/// Saturating conversion so an implausibly large field can never wrap; a frame
/// that genuinely exceeds `u32::MAX` bytes would be rejected as truncated on
/// decode rather than silently mis-encoded.
fn u32_len(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn millis_to_datetime(millis: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(millis)
        .single()
        .unwrap_or_else(Utc::now)
}

fn read_uuid(cursor: &mut Cursor<'_>) -> Result<Uuid, WireError> {
    Ok(Uuid::from_bytes(read_array::<UUID_LEN>(cursor)?))
}

fn read_array<const N: usize>(cursor: &mut Cursor<'_>) -> Result<[u8; N], WireError> {
    let slice = cursor.take(N)?;
    let mut array = [0_u8; N];
    array.copy_from_slice(slice);
    Ok(array)
}

/// A forward-only reader over a borrowed byte frame.
struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], WireError> {
        let end = self.offset.checked_add(count).ok_or(WireError::Truncated)?;
        let slice = self
            .bytes
            .get(self.offset..end)
            .ok_or(WireError::Truncated)?;
        self.offset = end;
        Ok(slice)
    }

    fn take_length_prefixed(&mut self) -> Result<&'a [u8], WireError> {
        let mut length = [0_u8; LEN_PREFIX];
        length.copy_from_slice(self.take(LEN_PREFIX)?);
        self.take(u32::from_be_bytes(length) as usize)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::{MAGIC, WireError, decode_envelope, encode_envelope};
    use crate::causal::{CausalContext, MessageId};
    use crate::channel::SchemaId;
    use crate::envelope::{Envelope, PublisherId};
    use chrono::{TimeZone, Utc};

    fn sample_envelope() -> Envelope {
        let parent = MessageId::new();
        let grandparent = MessageId::new();
        Envelope::with_message_id_and_timestamp(
            MessageId::new(),
            b"{\"value\":42}".to_vec(),
            Some(CausalContext::from_parent_chain(vec![parent, grandparent])),
            SchemaId::new(),
            PublisherId::from("publisher-7"),
            Utc.timestamp_millis_opt(1_700_000_000_123)
                .single()
                .expect("valid millis"),
        )
    }

    #[test]
    fn round_trips_a_full_envelope() {
        let original = sample_envelope();
        let bytes = encode_envelope(&original);
        let decoded = decode_envelope(&bytes).expect("decode should succeed");
        assert_eq!(decoded, original);
    }

    #[test]
    fn round_trips_an_envelope_without_causal_context() {
        let original = Envelope::with_message_id_and_timestamp(
            MessageId::new(),
            Vec::new(),
            None,
            SchemaId::new(),
            PublisherId::default(),
            Utc.timestamp_millis_opt(0).single().expect("epoch millis"),
        );
        let bytes = encode_envelope(&original);
        let decoded = decode_envelope(&bytes).expect("decode should succeed");
        assert_eq!(decoded, original);
        assert!(decoded.causal_context.is_none());
    }

    #[test]
    fn rejects_an_unknown_magic() {
        let mut bytes = encode_envelope(&sample_envelope());
        bytes[0] = b'X';
        assert_eq!(decode_envelope(&bytes), Err(WireError::BadMagic));
    }

    #[test]
    fn rejects_a_truncated_frame() {
        let bytes = encode_envelope(&sample_envelope());
        let truncated = &bytes[..bytes.len() - 4];
        assert_eq!(decode_envelope(truncated), Err(WireError::Truncated));
    }

    #[test]
    fn rejects_an_empty_frame() {
        assert_eq!(decode_envelope(&[]), Err(WireError::Truncated));
    }

    #[test]
    fn magic_is_stable() {
        assert_eq!(&MAGIC, b"LMW1");
    }
}
