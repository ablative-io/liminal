use std::fmt;

/// Opaque processing outcome supplied by a consumer for dedup completion.
#[derive(Clone, PartialEq, Eq)]
pub struct ProcessingReceipt(Vec<u8>);

impl ProcessingReceipt {
    /// Wraps consumer-defined receipt bytes without interpretation.
    #[must_use]
    pub const fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Returns the opaque receipt bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the receipt and returns its opaque bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl fmt::Debug for ProcessingReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ProcessingReceipt({} bytes)", self.0.len())
    }
}

#[cfg(test)]
mod tests {
    use super::ProcessingReceipt;

    #[test]
    fn receipt_debug_reports_length_not_contents() {
        let receipt = ProcessingReceipt::new(b"secret receipt".to_vec());

        let debug = format!("{receipt:?}");

        assert_eq!(debug, "ProcessingReceipt(14 bytes)");
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn receipts_compare_by_bytes() {
        let left = ProcessingReceipt::new(vec![1, 2, 3]);
        let right = ProcessingReceipt::new(vec![1, 2, 3]);

        assert_eq!(left, right);
    }
}
