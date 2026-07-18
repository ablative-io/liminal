//! Checked big-endian primitives for the Unit 2 canonical codec.

use super::super::OutboxLogError;

pub(super) struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    pub(super) const fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    pub(super) fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub(super) fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub(super) fn length(
        &mut self,
        field: &'static str,
        length: usize,
    ) -> Result<(), OutboxLogError> {
        let value =
            u32::try_from(length).map_err(|_| OutboxLogError::LengthOverflow { field, length })?;
        self.u32(value);
        Ok(())
    }

    pub(super) fn bytes(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    pub(super) fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

pub(super) struct Decoder<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    pub(super) const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn take(&mut self, field: &'static str, length: usize) -> Result<&'a [u8], OutboxLogError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(OutboxLogError::UnexpectedEnd { field })?;
        let bytes = self
            .input
            .get(self.offset..end)
            .ok_or(OutboxLogError::UnexpectedEnd { field })?;
        self.offset = end;
        Ok(bytes)
    }

    pub(super) fn u8(&mut self, field: &'static str) -> Result<u8, OutboxLogError> {
        let bytes = self.take(field, 1)?;
        bytes
            .first()
            .copied()
            .ok_or(OutboxLogError::UnexpectedEnd { field })
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, OutboxLogError> {
        let bytes = self.take(field, 4)?;
        let mut value = [0_u8; 4];
        value.copy_from_slice(bytes);
        Ok(u32::from_be_bytes(value))
    }

    pub(super) fn u64(&mut self, field: &'static str) -> Result<u64, OutboxLogError> {
        let bytes = self.take(field, 8)?;
        let mut value = [0_u8; 8];
        value.copy_from_slice(bytes);
        Ok(u64::from_be_bytes(value))
    }

    pub(super) fn length(&mut self, field: &'static str) -> Result<usize, OutboxLogError> {
        usize::try_from(self.u32(field)?).map_err(|_| OutboxLogError::LengthOverflow {
            field,
            length: usize::MAX,
        })
    }

    pub(super) fn bytes(
        &mut self,
        field: &'static str,
        length: usize,
    ) -> Result<&'a [u8], OutboxLogError> {
        self.take(field, length)
    }

    pub(super) fn finish(self) -> Result<(), OutboxLogError> {
        let remaining = self.input.len().saturating_sub(self.offset);
        if remaining == 0 {
            Ok(())
        } else {
            Err(OutboxLogError::TrailingBytes { remaining })
        }
    }
}
