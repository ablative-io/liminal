use core::fmt;

/// One entry/byte resource vector before arithmetic widening.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ResourceVector {
    /// Entry component.
    pub entries: u64,
    /// Encoded-byte component.
    pub bytes: u64,
}

impl ResourceVector {
    /// Creates an entry/byte resource vector.
    #[must_use]
    pub const fn new(entries: u64, bytes: u64) -> Self {
        Self { entries, bytes }
    }

    /// Widens both components before any arithmetic is performed.
    #[must_use]
    pub const fn widen(self) -> WideResourceVector {
        WideResourceVector::new(widen_u64(self.entries), widen_u64(self.bytes))
    }
}

/// `From<u64>` is not const at the workspace MSRV. This lossless cast keeps
/// every public formula const while centralizing that compiler limitation.
#[allow(clippy::cast_lossless)]
pub(super) const fn widen_u64(value: u64) -> u128 {
    value as u128
}

/// One entry/byte resource vector after widening to the protocol arithmetic domain.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WideResourceVector {
    /// Entry component.
    pub entries: u128,
    /// Encoded-byte component.
    pub bytes: u128,
}

impl WideResourceVector {
    /// Creates a widened entry/byte vector.
    #[must_use]
    pub const fn new(entries: u128, bytes: u128) -> Self {
        Self { entries, bytes }
    }

    /// Returns whether both components are zero.
    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.entries == 0 && self.bytes == 0
    }
}

/// Resource dimension, in the contract's entry-before-byte precedence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceDimension {
    /// Retained entry count.
    Entries,
    /// Retained encoded bytes.
    Bytes,
}

/// Invalid inputs to the retained-baseline formula.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BaselineError {
    /// More marker credits were supplied than configured identity slots.
    MarkerCreditsExceedIdentitySlots {
        /// Configured identity slots.
        identity_slots: u64,
        /// Slots currently owning marker credits.
        marker_credits: u64,
    },
}

impl fmt::Display for BaselineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MarkerCreditsExceedIdentitySlots {
                identity_slots,
                marker_credits,
            } => write!(
                formatter,
                "marker credits ({marker_credits}) exceed identity slots ({identity_slots})"
            ),
        }
    }
}

/// Results of the mandatory-class debt and absolute-fit formulas.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MandatoryCapacity {
    /// Exact componentwise post-transaction debt.
    pub debt: WideResourceVector,
    /// Whether `B' + K_remaining' <= cap` holds componentwise.
    pub absolute_fit: bool,
    /// Whether `d' <= Q` holds componentwise.
    pub debt_within_mandatory_bound: bool,
}

impl MandatoryCapacity {
    /// Returns whether both mandatory-class checks pass.
    #[must_use]
    pub const fn is_legal(self) -> bool {
        self.absolute_fit && self.debt_within_mandatory_bound
    }
}

/// Exact post-transfer values for a recovery transaction of charge `r`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryTransfer {
    /// `B' = B_removed + r`.
    pub baseline: WideResourceVector,
    /// `K_remaining' = K_remaining - r`.
    pub remaining_recovery_claim: ResourceVector,
}

/// Invalid or unrepresentable recovery-charge transfer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryTransferError {
    /// First component in which the transferred charge exceeded the claim or
    /// the widened post-transfer baseline overflowed.
    pub dimension: ResourceDimension,
}

impl fmt::Display for RecoveryTransferError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "recovery charge exceeds remaining {:?} claim",
            self.dimension
        )
    }
}

/// Reproducible outputs of the participant physical-floor rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FloorComputation {
    /// Minimum member cursor after membership changes, or `H'` when empty.
    pub member_cursor: u64,
    /// `min(m, observer_progress) + 1` in the widened boundary domain.
    pub preferred_floor: u128,
    /// `max(F, preferred_floor, cap_floor)`.
    pub resulting_floor: u128,
}
