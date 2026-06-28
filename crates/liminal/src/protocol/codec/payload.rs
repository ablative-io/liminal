use std::str;

use crate::protocol::{ProtocolError, SchemaId};

pub(super) const U16_LEN: usize = 2;
pub(super) const U32_LEN: usize = 4;
pub(super) const U64_LEN: usize = 8;

const U8_LEN: usize = 1;

pub(super) fn sum_lengths(parts: &[usize]) -> Result<usize, ProtocolError> {
    let mut total = 0_usize;
    for part in parts {
        total = total
            .checked_add(*part)
            .ok_or_else(|| ProtocolError::codec("payload length overflowed usize"))?;
    }
    Ok(total)
}

pub(super) fn checked_u32_len(len: usize) -> Result<(), ProtocolError> {
    u32::try_from(len)
        .map(|_| ())
        .map_err(|_| ProtocolError::codec("payload field length exceeded u32::MAX"))
}

pub(super) fn bytes_field_len(bytes: &[u8]) -> Result<usize, ProtocolError> {
    checked_u32_len(bytes.len())?;
    sum_lengths(&[U32_LEN, bytes.len()])
}

pub(super) fn string_field_len(value: &str) -> Result<usize, ProtocolError> {
    bytes_field_len(value.as_bytes())
}

pub(super) fn option_string_len(value: Option<&str>) -> Result<usize, ProtocolError> {
    match value {
        Some(inner) => sum_lengths(&[U8_LEN, string_field_len(inner)?]),
        None => Ok(U8_LEN),
    }
}

pub(super) fn schema_ids_field_len(schema_ids: &[SchemaId]) -> Result<usize, ProtocolError> {
    checked_u32_len(schema_ids.len())?;
    let ids_len = schema_ids
        .len()
        .checked_mul(SchemaId::WIRE_LEN)
        .ok_or_else(|| ProtocolError::codec("schema id vector length overflowed usize"))?;
    sum_lengths(&[U32_LEN, ids_len])
}

pub(super) const fn option_u16_len(value: Option<u16>) -> usize {
    match value {
        Some(_) => U8_LEN + U16_LEN,
        None => U8_LEN,
    }
}

/// Encoded length of a count-prefixed vector of length-prefixed strings.
pub(super) fn string_vec_field_len(values: &[String]) -> Result<usize, ProtocolError> {
    checked_u32_len(values.len())?;
    let mut total = U32_LEN;
    for value in values {
        total = total
            .checked_add(string_field_len(value)?)
            .ok_or_else(|| ProtocolError::codec("string vector length overflowed usize"))?;
    }
    Ok(total)
}

pub(super) struct PayloadWriter<'a> {
    buffer: &'a mut [u8],
    offset: usize,
}

impl<'a> PayloadWriter<'a> {
    pub(super) const fn new(buffer: &'a mut [u8]) -> Self {
        Self { buffer, offset: 0 }
    }

    pub(super) fn write_u8(&mut self, value: u8) -> Result<(), ProtocolError> {
        self.write_slice(&[value])
    }

    pub(super) fn write_u16(&mut self, value: u16) -> Result<(), ProtocolError> {
        self.write_slice(&value.to_be_bytes())
    }

    pub(super) fn write_u32(&mut self, value: u32) -> Result<(), ProtocolError> {
        self.write_slice(&value.to_be_bytes())
    }

    pub(super) fn write_u64(&mut self, value: u64) -> Result<(), ProtocolError> {
        self.write_slice(&value.to_be_bytes())
    }

    pub(super) fn write_slice(&mut self, bytes: &[u8]) -> Result<(), ProtocolError> {
        let end = self
            .offset
            .checked_add(bytes.len())
            .ok_or_else(|| ProtocolError::codec("write offset overflowed usize"))?;
        let Some(target) = self.buffer.get_mut(self.offset..end) else {
            return Err(ProtocolError::codec("payload writer exceeded buffer"));
        };
        target.copy_from_slice(bytes);
        self.offset = end;
        Ok(())
    }

    pub(super) fn write_schema_id(&mut self, schema_id: SchemaId) -> Result<(), ProtocolError> {
        self.write_slice(schema_id.as_bytes())
    }

    pub(super) fn write_schema_ids_field(
        &mut self,
        schema_ids: &[SchemaId],
    ) -> Result<(), ProtocolError> {
        let len = u32::try_from(schema_ids.len())
            .map_err(|_| ProtocolError::codec("schema id count exceeded u32::MAX"))?;
        self.write_u32(len)?;
        for schema_id in schema_ids {
            self.write_schema_id(*schema_id)?;
        }
        Ok(())
    }

    pub(super) fn write_bytes_field(&mut self, bytes: &[u8]) -> Result<(), ProtocolError> {
        let len = u32::try_from(bytes.len())
            .map_err(|_| ProtocolError::codec("payload field length exceeded u32::MAX"))?;
        self.write_u32(len)?;
        self.write_slice(bytes)
    }

    pub(super) fn write_string_field(&mut self, value: &str) -> Result<(), ProtocolError> {
        self.write_bytes_field(value.as_bytes())
    }

    pub(super) fn write_string_vec_field(
        &mut self,
        values: &[String],
    ) -> Result<(), ProtocolError> {
        let len = u32::try_from(values.len())
            .map_err(|_| ProtocolError::codec("string vector count exceeded u32::MAX"))?;
        self.write_u32(len)?;
        for value in values {
            self.write_string_field(value)?;
        }
        Ok(())
    }

    pub(super) fn write_optional_string(
        &mut self,
        value: Option<&str>,
    ) -> Result<(), ProtocolError> {
        match value {
            Some(inner) => {
                self.write_u8(1)?;
                self.write_string_field(inner)
            }
            None => self.write_u8(0),
        }
    }

    pub(super) fn write_optional_u16(&mut self, value: Option<u16>) -> Result<(), ProtocolError> {
        match value {
            Some(inner) => {
                self.write_u8(1)?;
                self.write_u16(inner)
            }
            None => self.write_u8(0),
        }
    }

    pub(super) fn finish(self) -> Result<(), ProtocolError> {
        if self.offset == self.buffer.len() {
            Ok(())
        } else {
            Err(ProtocolError::codec("payload writer did not fill buffer"))
        }
    }
}

pub(super) struct PayloadReader<'a> {
    buffer: &'a [u8],
    offset: usize,
}

impl<'a> PayloadReader<'a> {
    pub(super) const fn new(buffer: &'a [u8]) -> Self {
        Self { buffer, offset: 0 }
    }

    pub(super) fn read_u8(&mut self) -> Result<u8, ProtocolError> {
        let bytes = self.read_slice(U8_LEN)?;
        let Some(value) = bytes.first() else {
            return Err(ProtocolError::codec("payload u8 field was truncated"));
        };
        Ok(*value)
    }

    pub(super) fn read_u16(&mut self) -> Result<u16, ProtocolError> {
        let bytes = self.read_slice(U16_LEN)?;
        let [high, low] = bytes else {
            return Err(ProtocolError::codec("payload u16 field was truncated"));
        };
        Ok(u16::from_be_bytes([*high, *low]))
    }

    pub(super) fn read_u32(&mut self) -> Result<u32, ProtocolError> {
        let bytes = self.read_slice(U32_LEN)?;
        let [b0, b1, b2, b3] = bytes else {
            return Err(ProtocolError::codec("payload u32 field was truncated"));
        };
        Ok(u32::from_be_bytes([*b0, *b1, *b2, *b3]))
    }

    pub(super) fn read_u64(&mut self) -> Result<u64, ProtocolError> {
        let bytes = self.read_slice(U64_LEN)?;
        let [b0, b1, b2, b3, b4, b5, b6, b7] = bytes else {
            return Err(ProtocolError::codec("payload u64 field was truncated"));
        };
        Ok(u64::from_be_bytes([*b0, *b1, *b2, *b3, *b4, *b5, *b6, *b7]))
    }

    pub(super) fn read_schema_id(&mut self) -> Result<SchemaId, ProtocolError> {
        let bytes = self.read_slice(SchemaId::WIRE_LEN)?;
        let mut schema_id = [0_u8; SchemaId::WIRE_LEN];
        schema_id.copy_from_slice(bytes);
        Ok(SchemaId::new(schema_id))
    }

    pub(super) fn read_schema_ids_field(&mut self) -> Result<Vec<SchemaId>, ProtocolError> {
        let count = usize::try_from(self.read_u32()?)
            .map_err(|_| ProtocolError::codec("schema id count cannot fit usize"))?;
        let byte_len = count
            .checked_mul(SchemaId::WIRE_LEN)
            .ok_or_else(|| ProtocolError::codec("schema id vector length overflowed usize"))?;
        let bytes = self.read_slice(byte_len)?;
        let mut schema_ids = Vec::new();
        schema_ids
            .try_reserve_exact(count)
            .map_err(|_| ProtocolError::codec("schema id vector allocation failed"))?;

        for chunk in bytes.chunks_exact(SchemaId::WIRE_LEN) {
            let mut schema_id = [0_u8; SchemaId::WIRE_LEN];
            schema_id.copy_from_slice(chunk);
            schema_ids.push(SchemaId::new(schema_id));
        }

        Ok(schema_ids)
    }

    fn read_slice(&mut self, len: usize) -> Result<&'a [u8], ProtocolError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| ProtocolError::codec("read offset overflowed usize"))?;
        let Some(slice) = self.buffer.get(self.offset..end) else {
            return Err(ProtocolError::codec(
                "payload field exceeded payload length",
            ));
        };
        self.offset = end;
        Ok(slice)
    }

    pub(super) fn read_bytes_field(&mut self) -> Result<Vec<u8>, ProtocolError> {
        let len = usize::try_from(self.read_u32()?)
            .map_err(|_| ProtocolError::codec("payload field length cannot fit usize"))?;
        self.read_slice(len).map(<[u8]>::to_vec)
    }

    pub(super) fn read_string_field(&mut self) -> Result<String, ProtocolError> {
        let bytes = self.read_bytes_field()?;
        str::from_utf8(&bytes)
            .map(str::to_owned)
            .map_err(|_| ProtocolError::codec("payload string field was not valid utf-8"))
    }

    pub(super) fn read_string_vec_field(&mut self) -> Result<Vec<String>, ProtocolError> {
        let count = usize::try_from(self.read_u32()?)
            .map_err(|_| ProtocolError::codec("string vector count cannot fit usize"))?;
        let mut values = Vec::new();
        values
            .try_reserve(count)
            .map_err(|_| ProtocolError::codec("string vector allocation failed"))?;
        for _ in 0..count {
            values.push(self.read_string_field()?);
        }
        Ok(values)
    }

    pub(super) fn read_optional_string(&mut self) -> Result<Option<String>, ProtocolError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_string_field().map(Some),
            _ => Err(ProtocolError::codec("optional string tag was invalid")),
        }
    }

    pub(super) fn read_optional_u16(&mut self) -> Result<Option<u16>, ProtocolError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_u16().map(Some),
            _ => Err(ProtocolError::codec("optional u16 tag was invalid")),
        }
    }

    pub(super) fn finish(self) -> Result<(), ProtocolError> {
        if self.offset == self.buffer.len() {
            Ok(())
        } else {
            Err(ProtocolError::codec("payload contained trailing bytes"))
        }
    }
}
