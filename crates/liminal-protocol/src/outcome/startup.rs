/// Receipt/identity configuration field subject to the nonzero check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapabilityLimitField {
    /// Attach-receipt lifetime in milliseconds.
    AttachReceiptTtlMs,
    /// Receipt-provenance lifetime in milliseconds.
    ReceiptProvenanceTtlMs,
    /// Server-wide live attach-receipt capacity.
    MaxLiveAttachReceiptsServer,
    /// Per-participant live attach-receipt capacity.
    MaxLiveAttachReceiptsPerParticipant,
    /// Server-wide receipt-provenance capacity.
    MaxReceiptProvenanceServer,
    /// Per-conversation receipt-provenance capacity.
    MaxReceiptProvenancePerConversation,
    /// Per-participant receipt-provenance capacity.
    MaxReceiptProvenancePerParticipant,
    /// Server-wide retired identity-slot capacity.
    MaxRetiredIdentitySlotsServer,
    /// Per-conversation retired identity-slot capacity.
    MaxRetiredIdentitySlotsPerConversation,
}

/// Participant receipt/identity configuration is invalid.
///
/// Each variant is the exact flat dimension body; no generic reason or
/// optional operand bag exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantCapabilityConfigurationInvalid {
    /// First signed capability limit in validation order was zero.
    NonzeroLimit {
        /// Offending configuration field.
        field: CapabilityLimitField,
        /// Actual value, which is zero for this variant.
        actual: u64,
        /// Required minimum, which is one for this variant.
        required_minimum: u64,
    },
    /// Receipt provenance would expire before the receipt.
    ReceiptDeadlineOrder {
        /// Signed attach-receipt lifetime.
        attach_receipt_ttl_ms: u64,
        /// Signed receipt-provenance lifetime.
        receipt_provenance_ttl_ms: u64,
        /// Required minimum provenance lifetime.
        required_minimum_provenance_ttl_ms: u64,
    },
}

/// Participant retention configuration failed its fixed startup validation.
///
/// `SuccessorOccurrenceArray` is deliberately absent under
/// `docs/design/LP-EXTRACTION-GOAL.md` Fix 2: cursor-progress accounting is
/// per participant and no serialized fixed occurrence array is part of this
/// crate's state model.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantRetentionCapacityInvalid {
    /// Configured retained-entry capacity is below the exact required value.
    EntryCapacity {
        /// Exact widened required entry capacity.
        required: u128,
        /// Configured retained-entry capacity.
        configured: u64,
    },
    /// Configured retained-byte capacity is below the exact required value.
    ByteCapacity {
        /// Exact widened required byte capacity.
        required: u128,
        /// Configured retained-byte capacity.
        configured: u64,
    },
    /// Episode churn limit is outside the proved bounded domain.
    EpisodeChurnLimit {
        /// Configured raw episode churn limit.
        configured: u64,
        /// Required minimum, which is two.
        required_minimum: u64,
        /// Required maximum, which is `u32::MAX`.
        required_maximum: u64,
    },
}

/// Connection-incarnation mint exhausted one monotonic component.
///
/// The enum shape fixes `current_value` to `u64::MAX` and makes the server arm
/// carry no attempted incarnation while the ordinal arm always carries one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionIncarnationExhausted {
    /// Persisted server-incarnation counter is exhausted.
    ServerIncarnation,
    /// Connection ordinal is exhausted for this server incarnation.
    ConnectionOrdinal {
        /// Current server incarnation whose ordinal space is exhausted.
        attempted_server_incarnation: u64,
    },
}

impl ConnectionIncarnationExhausted {
    /// Returns the terminal current component value.
    #[must_use]
    pub const fn current_value(self) -> u64 {
        let _ = self;
        u64::MAX
    }

    /// Returns the attempted server incarnation only for ordinal exhaustion.
    #[must_use]
    pub const fn attempted_server_incarnation(self) -> Option<u64> {
        match self {
            Self::ServerIncarnation => None,
            Self::ConnectionOrdinal {
                attempted_server_incarnation,
            } => Some(attempted_server_incarnation),
        }
    }
}
