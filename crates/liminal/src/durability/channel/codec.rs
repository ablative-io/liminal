use super::{CausalContext, MessageEnvelope};
use crate::durability::DurabilityError;

const ENVELOPE_MAGIC: [u8; 4] = *b"LME1";
const ABSENT: u8 = 0;
const PRESENT: u8 = 1;

impl MessageEnvelope {
    /// Serializes the envelope into a canonical binary representation.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError::EnvelopeError`] if a field length cannot be
    /// represented in the storage format.
    pub fn serialize(&self) -> Result<Vec<u8>, DurabilityError> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&ENVELOPE_MAGIC);
        write_bytes(&mut bytes, &self.payload)?;
        write_optional_causal_context(&mut bytes, self.causal_context.as_ref())?;
        write_u64(&mut bytes, self.timestamp);
        write_string(&mut bytes, &self.publisher_id)?;
        write_optional_string(&mut bytes, self.idempotency_key.as_deref())?;
        Ok(bytes)
    }

    /// Deserializes an envelope previously produced by [`Self::serialize`].
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError::EnvelopeError`] when bytes are malformed,
    /// truncated, contain invalid UTF-8, or carry trailing data.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, DurabilityError> {
        let mut reader = EnvelopeReader::new(bytes);
        reader.read_magic()?;
        let payload = reader.read_bytes()?.to_vec();
        let causal_context = reader.read_optional_causal_context()?;
        let timestamp = reader.read_u64()?;
        let publisher_id = reader.read_string()?;
        let idempotency_key = reader.read_optional_string()?;
        reader.finish()?;

        Ok(Self {
            payload,
            causal_context,
            timestamp,
            publisher_id,
            idempotency_key,
        })
    }
}

fn write_optional_causal_context(
    bytes: &mut Vec<u8>,
    context: Option<&CausalContext>,
) -> Result<(), DurabilityError> {
    match context {
        Some(context) => {
            bytes.push(PRESENT);
            write_optional_string(bytes, context.parent_id.as_deref())?;
            write_optional_u64(bytes, context.vector_clock_entry);
        }
        None => bytes.push(ABSENT),
    }
    Ok(())
}

fn write_optional_string(bytes: &mut Vec<u8>, value: Option<&str>) -> Result<(), DurabilityError> {
    match value {
        Some(value) => {
            bytes.push(PRESENT);
            write_string(bytes, value)?;
        }
        None => bytes.push(ABSENT),
    }
    Ok(())
}

fn write_optional_u64(bytes: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            bytes.push(PRESENT);
            write_u64(bytes, value);
        }
        None => bytes.push(ABSENT),
    }
}

fn write_string(bytes: &mut Vec<u8>, value: &str) -> Result<(), DurabilityError> {
    write_bytes(bytes, value.as_bytes())
}

fn write_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<(), DurabilityError> {
    let len = u64::try_from(value.len()).map_err(|error| {
        DurabilityError::EnvelopeError(format!("field length cannot be encoded: {error}"))
    })?;
    write_u64(bytes, len);
    bytes.extend_from_slice(value);
    Ok(())
}

fn write_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

struct EnvelopeReader<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> EnvelopeReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn read_magic(&mut self) -> Result<(), DurabilityError> {
        let magic = self.read_exact(ENVELOPE_MAGIC.len())?;
        if magic == ENVELOPE_MAGIC {
            Ok(())
        } else {
            Err(DurabilityError::EnvelopeError(
                "invalid envelope magic".to_owned(),
            ))
        }
    }

    fn read_optional_causal_context(&mut self) -> Result<Option<CausalContext>, DurabilityError> {
        if self.read_presence()? {
            Ok(Some(CausalContext {
                parent_id: self.read_optional_string()?,
                vector_clock_entry: self.read_optional_u64()?,
            }))
        } else {
            Ok(None)
        }
    }

    fn read_optional_string(&mut self) -> Result<Option<String>, DurabilityError> {
        if self.read_presence()? {
            self.read_string().map(Some)
        } else {
            Ok(None)
        }
    }

    fn read_optional_u64(&mut self) -> Result<Option<u64>, DurabilityError> {
        if self.read_presence()? {
            self.read_u64().map(Some)
        } else {
            Ok(None)
        }
    }

    fn read_string(&mut self) -> Result<String, DurabilityError> {
        let bytes = self.read_bytes()?;
        String::from_utf8(bytes.to_vec()).map_err(|error| {
            DurabilityError::EnvelopeError(format!("invalid UTF-8 string field: {error}"))
        })
    }

    fn read_bytes(&mut self) -> Result<&'a [u8], DurabilityError> {
        let len_u64 = self.read_u64()?;
        let len = usize::try_from(len_u64).map_err(|error| {
            DurabilityError::EnvelopeError(format!("encoded length cannot fit memory: {error}"))
        })?;
        self.read_exact(len)
    }

    fn read_presence(&mut self) -> Result<bool, DurabilityError> {
        match self.read_u8()? {
            ABSENT => Ok(false),
            PRESENT => Ok(true),
            value => Err(DurabilityError::EnvelopeError(format!(
                "invalid option marker {value}"
            ))),
        }
    }

    fn read_u8(&mut self) -> Result<u8, DurabilityError> {
        let byte = self.read_exact(1)?;
        Ok(byte[0])
    }

    fn read_u64(&mut self) -> Result<u64, DurabilityError> {
        let bytes = self.read_exact(8)?;
        let mut array = [0_u8; 8];
        array.copy_from_slice(bytes);
        Ok(u64::from_be_bytes(array))
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], DurabilityError> {
        let end = self
            .cursor
            .checked_add(len)
            .ok_or_else(|| DurabilityError::EnvelopeError("envelope cursor overflow".to_owned()))?;
        if end > self.bytes.len() {
            return Err(DurabilityError::EnvelopeError(
                "truncated envelope bytes".to_owned(),
            ));
        }
        let slice = &self.bytes[self.cursor..end];
        self.cursor = end;
        Ok(slice)
    }

    fn finish(&self) -> Result<(), DurabilityError> {
        if self.cursor == self.bytes.len() {
            Ok(())
        } else {
            Err(DurabilityError::EnvelopeError(
                "trailing envelope bytes".to_owned(),
            ))
        }
    }
}
