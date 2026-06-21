use crate::durability::DurabilityError;

use super::DedupEntry;

const ENTRY_MAGIC: [u8; 4] = *b"LDE1";
const TOMBSTONE_MAGIC: [u8; 4] = *b"LDT1";
const ABSENT: u8 = 0;
const PRESENT: u8 = 1;

#[derive(Clone)]
pub(super) enum DedupRecord {
    Active(DedupEntry),
    Tombstone {
        idempotency_key: String,
        timestamp_millis: u64,
    },
}

impl DedupRecord {
    pub(super) const fn tombstone(idempotency_key: String, timestamp_millis: u64) -> Self {
        Self::Tombstone {
            idempotency_key,
            timestamp_millis,
        }
    }

    pub(super) fn idempotency_key(&self) -> &str {
        match self {
            Self::Active(entry) => entry.idempotency_key(),
            Self::Tombstone {
                idempotency_key, ..
            } => idempotency_key,
        }
    }

    pub(super) fn into_active(self) -> Option<DedupEntry> {
        match self {
            Self::Active(entry) => Some(entry),
            Self::Tombstone { .. } => None,
        }
    }

    pub(super) fn serialize(&self) -> Result<Vec<u8>, DurabilityError> {
        let mut bytes = Vec::new();
        match self {
            Self::Active(entry) => {
                bytes.extend_from_slice(&ENTRY_MAGIC);
                write_string(&mut bytes, entry.idempotency_key())?;
                write_optional_bytes(&mut bytes, entry.receipt())?;
                write_u64(&mut bytes, entry.timestamp_millis());
            }
            Self::Tombstone {
                idempotency_key,
                timestamp_millis,
            } => {
                bytes.extend_from_slice(&TOMBSTONE_MAGIC);
                write_string(&mut bytes, idempotency_key)?;
                write_u64(&mut bytes, *timestamp_millis);
            }
        }
        Ok(bytes)
    }

    pub(super) fn deserialize(bytes: &[u8]) -> Result<Self, DurabilityError> {
        let mut reader = DedupReader::new(bytes);
        let magic = reader.read_magic()?;
        let record = if magic == ENTRY_MAGIC {
            let idempotency_key = reader.read_string()?;
            let receipt = reader.read_optional_bytes()?.map(<[u8]>::to_vec);
            let timestamp_millis = reader.read_u64()?;
            Self::Active(DedupEntry::new(idempotency_key, receipt, timestamp_millis))
        } else if magic == TOMBSTONE_MAGIC {
            Self::tombstone(reader.read_string()?, reader.read_u64()?)
        } else {
            return Err(DurabilityError::EnvelopeError(
                "invalid dedup entry magic".to_owned(),
            ));
        };
        reader.finish()?;
        Ok(record)
    }
}

fn write_optional_bytes(bytes: &mut Vec<u8>, value: Option<&[u8]>) -> Result<(), DurabilityError> {
    match value {
        Some(value) => {
            bytes.push(PRESENT);
            write_bytes(bytes, value)?;
        }
        None => bytes.push(ABSENT),
    }
    Ok(())
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

struct DedupReader<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> DedupReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn read_magic(&mut self) -> Result<[u8; 4], DurabilityError> {
        let bytes = self.read_exact(4)?;
        let mut magic = [0_u8; 4];
        magic.copy_from_slice(bytes);
        Ok(magic)
    }

    fn read_optional_bytes(&mut self) -> Result<Option<&'a [u8]>, DurabilityError> {
        match self.read_u8()? {
            ABSENT => Ok(None),
            PRESENT => self.read_bytes().map(Some),
            value => Err(DurabilityError::EnvelopeError(format!(
                "invalid option marker {value}"
            ))),
        }
    }

    fn read_string(&mut self) -> Result<String, DurabilityError> {
        let bytes = self.read_bytes()?;
        String::from_utf8(bytes.to_vec()).map_err(|error| {
            DurabilityError::EnvelopeError(format!("invalid UTF-8 string field: {error}"))
        })
    }

    fn read_bytes(&mut self) -> Result<&'a [u8], DurabilityError> {
        let len = usize::try_from(self.read_u64()?).map_err(|error| {
            DurabilityError::EnvelopeError(format!("encoded length cannot fit memory: {error}"))
        })?;
        self.read_exact(len)
    }

    fn read_u8(&mut self) -> Result<u8, DurabilityError> {
        Ok(self.read_exact(1)?[0])
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
            .ok_or_else(|| DurabilityError::EnvelopeError("dedup cursor overflow".to_owned()))?;
        if end > self.bytes.len() {
            return Err(DurabilityError::EnvelopeError(
                "truncated dedup entry bytes".to_owned(),
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
                "trailing dedup entry bytes".to_owned(),
            ))
        }
    }
}
