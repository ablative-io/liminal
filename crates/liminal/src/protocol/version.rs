use super::error::ProtocolError;

/// Protocol version negotiated during the connection handshake.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProtocolVersion {
    /// Major protocol version component.
    pub major: u16,
    /// Minor protocol version component.
    pub minor: u16,
}

impl ProtocolVersion {
    /// Number of bytes used by a serialized protocol version.
    pub const WIRE_LEN: usize = 4;

    /// Create a protocol version from its major and minor components.
    #[must_use]
    pub const fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }

    /// Serialize this version as big-endian `major` followed by big-endian `minor`.
    #[must_use]
    pub const fn serialize(self) -> [u8; Self::WIRE_LEN] {
        let major = self.major.to_be_bytes();
        let minor = self.minor.to_be_bytes();
        [major[0], major[1], minor[0], minor[1]]
    }

    /// Serialize this version for wire transport.
    #[must_use]
    pub const fn to_wire_bytes(self) -> [u8; Self::WIRE_LEN] {
        self.serialize()
    }

    /// Deserialize a protocol version from wire bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::CodecError`] when `bytes` is not exactly four bytes long.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, ProtocolError> {
        Self::from_wire_bytes(bytes)
    }

    /// Deserialize a protocol version from wire bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::CodecError`] when `bytes` is not exactly four bytes long.
    pub fn from_wire_bytes(bytes: &[u8]) -> Result<Self, ProtocolError> {
        match bytes {
            [major_high, major_low, minor_high, minor_low] => Ok(Self::new(
                u16::from_be_bytes([*major_high, *major_low]),
                u16::from_be_bytes([*minor_high, *minor_low]),
            )),
            _ => Err(ProtocolError::codec(
                "protocol version must be exactly 4 bytes",
            )),
        }
    }
}

/// Select the highest server-supported protocol version within the client's range.
///
/// # Errors
///
/// Returns [`ProtocolError::VersionMismatch`] when `supported_versions` contains no
/// version in the inclusive range from `min_version` through `max_version`.
pub fn negotiate_version(
    min_version: ProtocolVersion,
    max_version: ProtocolVersion,
    supported_versions: &[ProtocolVersion],
) -> Result<ProtocolVersion, ProtocolError> {
    supported_versions
        .iter()
        .copied()
        .filter(|version| min_version <= *version && *version <= max_version)
        .max()
        .ok_or_else(|| ProtocolError::VersionMismatch {
            message: Some("no mutually supported protocol version".to_owned()),
        })
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use super::{ProtocolVersion, negotiate_version};
    use crate::protocol::ProtocolError;

    #[test]
    fn version_trait_bounds_are_available() {
        fn assert_traits<T: Debug + Clone + Copy + PartialEq + Eq + PartialOrd + Ord>() {}

        assert_traits::<ProtocolVersion>();
    }

    #[test]
    fn versions_order_by_major_then_minor() {
        assert!(ProtocolVersion::new(1, 0) < ProtocolVersion::new(1, 1));
        assert!(ProtocolVersion::new(1, 1) < ProtocolVersion::new(2, 0));
    }

    #[test]
    fn version_serializes_to_exactly_four_bytes() {
        let bytes = ProtocolVersion::new(0x0102, 0x0304).serialize();

        assert_eq!(bytes.len(), ProtocolVersion::WIRE_LEN);
        assert_eq!(bytes, [0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn version_round_trips_through_wire_bytes() -> Result<(), ProtocolError> {
        let version = ProtocolVersion::new(2, 7);
        let bytes = version.to_wire_bytes();

        assert_eq!(ProtocolVersion::deserialize(&bytes)?, version);
        assert_eq!(ProtocolVersion::from_wire_bytes(&bytes)?, version);
        Ok(())
    }

    #[test]
    fn deserialize_rejects_wrong_length() {
        assert!(matches!(
            ProtocolVersion::deserialize(&[0, 1, 0]),
            Err(ProtocolError::CodecError { .. })
        ));
    }

    #[test]
    fn negotiation_selects_highest_mutual_version() -> Result<(), ProtocolError> {
        let supported = [
            ProtocolVersion::new(1, 0),
            ProtocolVersion::new(1, 1),
            ProtocolVersion::new(2, 0),
            ProtocolVersion::new(3, 0),
        ];

        let selected = negotiate_version(
            ProtocolVersion::new(1, 0),
            ProtocolVersion::new(2, 0),
            &supported,
        )?;

        assert_eq!(selected, ProtocolVersion::new(2, 0));
        Ok(())
    }

    #[test]
    fn negotiation_reports_version_mismatch() {
        let supported = [ProtocolVersion::new(2, 0), ProtocolVersion::new(3, 0)];

        let result = negotiate_version(
            ProtocolVersion::new(1, 0),
            ProtocolVersion::new(1, 5),
            &supported,
        );

        assert!(matches!(result, Err(ProtocolError::VersionMismatch { .. })));
    }

    #[test]
    fn negotiation_selects_exact_single_version() -> Result<(), ProtocolError> {
        let supported = [ProtocolVersion::new(1, 0)];

        let selected = negotiate_version(
            ProtocolVersion::new(1, 0),
            ProtocolVersion::new(1, 0),
            &supported,
        )?;

        assert_eq!(selected, ProtocolVersion::new(1, 0));
        Ok(())
    }
}
