use super::ConversationEvent;
use crate::durability::DurabilityError;

const EVENT_MAGIC: [u8; 4] = *b"LCE1";
const TAG_MESSAGE_RECEIVED: u8 = 1;
const TAG_PROCESSING_STARTED: u8 = 2;
const TAG_STEP_COMPLETED: u8 = 3;
const TAG_PROCESSING_FINISHED: u8 = 4;
const TAG_ERROR_OCCURRED: u8 = 5;

impl ConversationEvent {
    /// Serializes this event into deterministic storage bytes.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError::EnvelopeError`] when a field length cannot be encoded.
    pub fn serialize(&self) -> Result<Vec<u8>, DurabilityError> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&EVENT_MAGIC);
        match self {
            Self::MessageReceived {
                message_id,
                received_at,
            } => {
                bytes.push(TAG_MESSAGE_RECEIVED);
                write_string(&mut bytes, message_id)?;
                write_u64(&mut bytes, *received_at);
            }
            Self::ProcessingStarted { message_id } => {
                bytes.push(TAG_PROCESSING_STARTED);
                write_string(&mut bytes, message_id)?;
            }
            Self::StepCompleted {
                message_id,
                step_index,
                output,
            } => {
                bytes.push(TAG_STEP_COMPLETED);
                write_string(&mut bytes, message_id)?;
                write_u32(&mut bytes, *step_index);
                write_bytes(&mut bytes, output)?;
            }
            Self::ProcessingFinished { message_id } => {
                bytes.push(TAG_PROCESSING_FINISHED);
                write_string(&mut bytes, message_id)?;
            }
            Self::ErrorOccurred { message_id, error } => {
                bytes.push(TAG_ERROR_OCCURRED);
                write_string(&mut bytes, message_id)?;
                write_string(&mut bytes, error)?;
            }
        }
        Ok(bytes)
    }

    /// Deserializes an event previously produced by [`Self::serialize`].
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError::EnvelopeError`] when bytes are malformed,
    /// truncated, contain invalid UTF-8, use an unknown tag, or carry trailing data.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, DurabilityError> {
        let mut reader = EventReader::new(bytes);
        reader.read_magic()?;
        let tag = reader.read_u8()?;
        let event = match tag {
            TAG_MESSAGE_RECEIVED => Self::MessageReceived {
                message_id: reader.read_string()?,
                received_at: reader.read_u64()?,
            },
            TAG_PROCESSING_STARTED => Self::ProcessingStarted {
                message_id: reader.read_string()?,
            },
            TAG_STEP_COMPLETED => Self::StepCompleted {
                message_id: reader.read_string()?,
                step_index: reader.read_u32()?,
                output: reader.read_bytes()?.to_vec(),
            },
            TAG_PROCESSING_FINISHED => Self::ProcessingFinished {
                message_id: reader.read_string()?,
            },
            TAG_ERROR_OCCURRED => Self::ErrorOccurred {
                message_id: reader.read_string()?,
                error: reader.read_string()?,
            },
            value => {
                return Err(DurabilityError::EnvelopeError(format!(
                    "invalid conversation event tag {value}"
                )));
            }
        };
        reader.finish()?;
        Ok(event)
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

fn write_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn write_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

struct EventReader<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> EventReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn read_magic(&mut self) -> Result<(), DurabilityError> {
        let magic = self.read_exact(EVENT_MAGIC.len())?;
        if magic == EVENT_MAGIC {
            Ok(())
        } else {
            Err(DurabilityError::EnvelopeError(
                "invalid conversation event magic".to_owned(),
            ))
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

    fn read_u8(&mut self) -> Result<u8, DurabilityError> {
        let byte = self.read_exact(1)?;
        Ok(byte[0])
    }

    fn read_u32(&mut self) -> Result<u32, DurabilityError> {
        let bytes = self.read_exact(4)?;
        let mut array = [0_u8; 4];
        array.copy_from_slice(bytes);
        Ok(u32::from_be_bytes(array))
    }

    fn read_u64(&mut self) -> Result<u64, DurabilityError> {
        let bytes = self.read_exact(8)?;
        let mut array = [0_u8; 8];
        array.copy_from_slice(bytes);
        Ok(u64::from_be_bytes(array))
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], DurabilityError> {
        let end = self.cursor.checked_add(len).ok_or_else(|| {
            DurabilityError::EnvelopeError("conversation event cursor overflow".to_owned())
        })?;
        if end > self.bytes.len() {
            return Err(DurabilityError::EnvelopeError(
                "truncated conversation event bytes".to_owned(),
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
                "trailing conversation event bytes".to_owned(),
            ))
        }
    }
}
