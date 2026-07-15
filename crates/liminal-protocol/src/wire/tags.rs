use core::fmt;

use crate::algebra::ResourceDimension;

/// An unassigned value in a closed participant registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TagError {
    /// Unassigned numeric value.
    pub value: u16,
}

impl fmt::Display for TagError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "unassigned participant tag 0x{:04X}", self.value)
    }
}

macro_rules! u16_registry {
    ($(#[$meta:meta])* $name:ident { $($(#[$variant_meta:meta])* $variant:ident = $value:expr),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        #[repr(u16)]
        pub enum $name {
            $($(#[$variant_meta])* $variant = $value),+
        }

        impl $name {
            /// Returns the stable v1 wire value.
            #[must_use]
            pub const fn wire_value(self) -> u16 {
                self as u16
            }
        }

        impl From<$name> for u16 {
            fn from(value: $name) -> Self {
                value.wire_value()
            }
        }

        impl TryFrom<u16> for $name {
            type Error = TagError;

            fn try_from(value: u16) -> Result<Self, Self::Error> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    value => Err(TagError { value }),
                }
            }
        }
    };
}

u16_registry! {
    /// Client-to-server participant request registry.
    ClientDiscriminant {
        /// Participant enrollment.
        EnrollmentRequest = 0x0001,
        /// Credential-bearing attach or rotation.
        CredentialAttachRequest = 0x0002,
        /// Explicit detach.
        DetachRequest = 0x0003,
        /// Continuous cumulative acknowledgement.
        ParticipantAck = 0x0004,
        /// Terminal participant Leave.
        LeaveRequest = 0x0005,
        /// Explicit marker acknowledgement.
        MarkerAck = 0x0006,
        /// Ordinary record admission.
        RecordAdmission = 0x0007,
        /// Reconnect observer-recovery batch.
        ObserverRecoveryHandshake = 0x0008,
    }
}

u16_registry! {
    /// Server-to-client semantic participant value registry.
    ServerDiscriminant {
        /// Pre-semantic structural/authentication rejection.
        ParticipantTransportRejected = 0x0100,
        /// A token was reused with a changed canonical body.
        AttemptTokenBodyConflict = 0x0101,
        /// Connection conversation capacity was exhausted.
        ConnectionConversationCapacityExceeded = 0x0102,
        /// The connection's conversation binding slot was occupied.
        ConnectionConversationBindingOccupied = 0x0103,
        /// Transaction order was exhausted.
        ConversationOrderExhausted = 0x0104,
        /// Presented participant identity does not exist.
        ParticipantUnknown = 0x0105,
        /// Valid identity has no required binding.
        NoBinding = 0x0106,
        /// Presented authority is stale.
        StaleAuthority = 0x0107,
        /// Presented identity is permanently retired.
        Retired = 0x0108,
        /// Marker-closure capacity or recovery fence refused admission.
        MarkerClosureCapacityExceeded = 0x0109,
        /// Enrollment committed and bound.
        EnrollBound = 0x010A,
        /// Enrollment token maps to an existing live identity.
        EnrollmentKnown = 0x010B,
        /// Stored receipt expired or was superseded.
        ReceiptExpired = 0x010C,
        /// Receipt/provenance capacity was exhausted.
        ReceiptCapacityExceeded = 0x010D,
        /// Participant identity capacity was exhausted.
        IdentityCapacityExceeded = 0x010E,
        /// Observer progress prevents the mutation.
        ObserverBackpressure = 0x010F,
        /// Conversation sequence reserve was exhausted.
        ConversationSequenceExhausted = 0x0110,
        /// Credential attach committed and bound.
        AttachBound = 0x0111,
        /// Receipt is stale or no longer known.
        StaleOrUnknownReceipt = 0x0112,
        /// Requested marker was not delivered to the proof epoch.
        MarkerNotDelivered = 0x0113,
        /// Presented marker does not match current marker state.
        MarkerMismatch = 0x0114,
        /// Receipt replay still names the current binding.
        Bound = 0x0115,
        /// Receipt replay no longer names the current binding.
        UnboundReceipt = 0x0116,
        /// Explicit detach committed.
        DetachCommitted = 0x0117,
        /// Another detach token is pending.
        DetachInProgress = 0x0118,
        /// Continuous acknowledgement advanced the cursor.
        AckCommitted = 0x0119,
        /// Acknowledgement was an idempotent no-op.
        AckNoOp = 0x011A,
        /// Continuous acknowledgement crossed an unavailable gap.
        AckGap = 0x011B,
        /// Continuous acknowledgement regressed below the cursor.
        AckRegression = 0x011C,
        /// Terminal Leave committed.
        LeaveCommitted = 0x011D,
        /// Marker acknowledgement committed.
        MarkerAckCommitted = 0x011E,
        /// Ordinary record committed.
        RecordCommitted = 0x011F,
        /// Ordinary record exceeded its configured maximum.
        RecordTooLarge = 0x0120,
        /// Observer-recovery batch succeeded.
        ObserverRecoveryAccepted = 0x0121,
        /// Observer-recovery entry has an invalid epoch.
        InvalidObserverEpoch = 0x0122,
        /// Observer-recovery request list is invalid.
        InvalidObserverEpochList = 0x0123,
        /// Observer-recovery preflight exceeded connection capacity.
        ObserverRecoveryConnectionCapacityExceeded = 0x0124,
    }
}

u16_registry! {
    /// Server-pushed participant control/value registry.
    PushDiscriminant {
        /// Observer progress wake.
        ObserverProgressed = 0x0200,
        /// Participant record delivery.
        ParticipantDelivery = 0x0201,
    }
}

u16_registry! {
    /// Participant delivery record-kind registry.
    RecordKind {
        /// Ordinary application record.
        OrdinaryRecord = 0x0000,
        /// Participant attached.
        Attached = 0x0001,
        /// Binding detached cleanly or by supersession/shutdown.
        Detached = 0x0002,
        /// Binding died unexpectedly.
        Died = 0x0003,
        /// Participant permanently left.
        Left = 0x0004,
        /// Retained history was explicitly compacted.
        HistoryCompacted = 0x0005,
    }
}

u16_registry! {
    /// Binding close-cause tag registry.
    CloseCauseTag {
        /// Clean deregistration.
        CleanDeregister = 1,
        /// Transport connection was lost.
        ConnectionLost = 2,
        /// Participant process was killed.
        ProcessKilled = 3,
        /// Participant protocol error.
        ProtocolError = 4,
        /// Binding was superseded.
        Superseded = 5,
        /// Server performed an orderly shutdown.
        ServerShutdown = 6,
        /// Binding was recovered after an unclean server restart.
        UncleanServerRestart = 7,
    }
}

/// Exact close cause; only unclean restart carries a suffix field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloseCause {
    /// Clean deregistration.
    CleanDeregister,
    /// Transport connection was lost.
    ConnectionLost,
    /// Participant process was killed.
    ProcessKilled,
    /// Participant protocol error.
    ProtocolError,
    /// Binding was superseded.
    Superseded,
    /// Server performed an orderly shutdown.
    ServerShutdown,
    /// Binding was recovered after an unclean restart.
    UncleanServerRestart {
        /// Server incarnation that previously owned the binding.
        prior_server_incarnation: u64,
    },
}

impl CloseCause {
    /// Returns this cause's exact wire tag.
    #[must_use]
    pub const fn tag(self) -> CloseCauseTag {
        match self {
            Self::CleanDeregister => CloseCauseTag::CleanDeregister,
            Self::ConnectionLost => CloseCauseTag::ConnectionLost,
            Self::ProcessKilled => CloseCauseTag::ProcessKilled,
            Self::ProtocolError => CloseCauseTag::ProtocolError,
            Self::Superseded => CloseCauseTag::Superseded,
            Self::ServerShutdown => CloseCauseTag::ServerShutdown,
            Self::UncleanServerRestart { .. } => CloseCauseTag::UncleanServerRestart,
        }
    }
}

u16_registry! {
    /// Transport rejection union tag.
    TransportReasonTag {
        /// Complete frame exceeds the active bound.
        FrameTooLarge = 1,
        /// Structural participant decode failed.
        DecodeFailed = 2,
        /// Participant version is unsupported.
        UnsupportedVersion = 3,
        /// Connection authentication failed.
        AuthenticationFailed = 4,
        /// Participant capability was not negotiated.
        ParticipantCapabilityRequired = 5,
    }
}

u16_registry! {
    /// Exhaustive participant structural decode classes.
    DecodeClass {
        /// Invalid outer frame flags, stream, or prefix length.
        Framing = 1,
        /// Inner discriminant is unassigned or wrong-direction.
        UnknownDiscriminant = 2,
        /// Complete selected shape contains trailing bytes.
        CanonicalEncoding = 3,
        /// Selected shape is missing required bytes.
        MissingRequiredField = 4,
        /// Selector, tag, or fixed scalar violates its domain.
        InvalidField = 5,
    }
}

u16_registry! {
    /// Operation selector inside an attempt-token body conflict.
    AttemptOperation {
        /// Credential attach.
        CredentialAttachRequest = 1,
        /// Terminal Leave.
        LeaveRequest = 2,
    }
}

u16_registry! {
    /// Ordered attempt-token conflict class.
    AttemptConflict {
        /// Presented generation changed.
        Generation = 1,
        /// Presented marker-delivery sequence changed.
        MarkerDeliverySequence = 2,
    }
}

u16_registry! {
    /// Detach-specific outer authority-state selector.
    DetachAuthorityStateTag {
        /// Live identity authority mismatch.
        Live = 1,
        /// Exact old token resolved to a terminalized detach cell.
        TerminalizedDetachCell = 2,
    }
}

/// Compatibility name for the detach authority-state registry.
///
/// Leave deliberately uses [`LeaveAuthorityStateTag`], so its committed
/// tombstone state cannot be confused with a terminalized detach cell.
pub type AuthorityStateTag = DetachAuthorityStateTag;

u16_registry! {
    /// Leave-specific outer authority-state selector.
    LeaveAuthorityStateTag {
        /// Live identity authority mismatch.
        Live = 1,
        /// Exact token resolved to the committed Leave tombstone.
        CommittedLeaveTombstone = 2,
    }
}

u16_registry! {
    /// Binding view selector inside terminalized-detach stale authority.
    BindingStateTag {
        /// A current binding exists.
        Bound = 1,
        /// No current binding exists.
        Detached = 2,
    }
}

u16_registry! {
    /// Receipt-expiry reason.
    ReceiptExpiryReason {
        /// Receipt deadline elapsed.
        Deadline = 1,
        /// A newer credential superseded the receipt.
        Superseded = 2,
    }
}

u16_registry! {
    /// Receipt/provenance capacity scope.
    ReceiptCapacityScope {
        /// Server-wide live receipt rows.
        LiveReceiptServer = 1,
        /// Per-participant live receipt rows.
        LiveReceiptParticipant = 2,
        /// Server-wide provenance rows.
        ProvenanceServer = 3,
        /// Per-conversation provenance rows.
        ProvenanceConversation = 4,
        /// Per-participant provenance rows.
        ProvenanceParticipant = 5,
    }
}

u16_registry! {
    /// Participant identity-capacity scope.
    IdentityCapacityScope {
        /// Server-wide identities.
        Server = 1,
        /// Per-conversation identities.
        Conversation = 2,
    }
}

u16_registry! {
    /// Marker-closure refusal scope.
    ClosureScope {
        /// Entry or byte capacity.
        Capacity = 1,
        /// Detached recovery fence.
        RecoveryFence = 2,
        /// Delivered marker still awaits acknowledgement.
        DeliveredMarkerAwaitingAck = 3,
        /// Debt-episode churn limit.
        EpisodeChurnLimit = 4,
    }
}

u16_registry! {
    /// Entry-before-bytes resource dimension registry.
    ResourceDimensionTag {
        /// Retained entry count.
        Entries = 1,
        /// Retained encoded bytes.
        Bytes = 2,
    }
}

impl From<ResourceDimension> for ResourceDimensionTag {
    fn from(value: ResourceDimension) -> Self {
        match value {
            ResourceDimension::Entries => Self::Entries,
            ResourceDimension::Bytes => Self::Bytes,
        }
    }
}

impl From<ResourceDimensionTag> for ResourceDimension {
    fn from(value: ResourceDimensionTag) -> Self {
        match value {
            ResourceDimensionTag::Entries => Self::Entries,
            ResourceDimensionTag::Bytes => Self::Bytes,
        }
    }
}

u16_registry! {
    /// Stored repayment-edge tag, including the clear state.
    RepaymentEdgeTag {
        /// No edge and zero debt.
        None = 1,
        /// Observer projection.
        ObserverProjection = 2,
        /// Physical compaction.
        PhysicalCompaction = 3,
        /// Marker delivery.
        MarkerDelivery = 4,
        /// Participant cursor progress.
        ParticipantCursorProgress = 5,
        /// Detached credential recovery.
        DetachedCredentialRecovery = 6,
        /// Detached marker release.
        DetachedMarkerRelease = 7,
        /// Detached cursor release.
        DetachedCursorRelease = 8,
    }
}

u16_registry! {
    /// Singleton marker-not-delivered reason registry.
    MarkerNotDeliveredReason {
        /// Marker was not delivered to the proof epoch.
        NotDeliveredToProofEpoch = 1,
    }
}

u16_registry! {
    /// Marker mismatch reason registry.
    MarkerMismatchReason {
        /// Requested marker is below the current cursor.
        BelowCursor = 1,
        /// No marker is expected by this edge.
        NoMarkerExpected = 2,
        /// A different marker is expected.
        ExpectedDifferentMarker = 3,
    }
}

u16_registry! {
    /// Singleton normal-ack gap reason.
    AckGapReason {
        /// Requested range was not contiguously available.
        NotContiguouslyAvailable = 1,
    }
}

u16_registry! {
    /// Singleton normal-ack regression reason.
    AckRegressionReason {
        /// Requested boundary is below the current cursor.
        BelowCursor = 1,
    }
}

u16_registry! {
    /// Invalid observer-epoch reason.
    InvalidObserverEpochReason {
        /// Conversation is unknown.
        ConversationUnknown = 1,
        /// Presented epoch is ahead of current progress.
        EpochAhead = 2,
    }
}

u16_registry! {
    /// Invalid observer-recovery list reason.
    InvalidObserverEpochListReason {
        /// Request contains too many entries.
        TooManyEntries = 1,
        /// Request repeats a conversation.
        DuplicateConversation = 2,
    }
}

u16_registry! {
    /// Counter selector used by conversation-order exhaustion.
    Counter {
        /// Conversation transaction order.
        TransactionOrder = 1,
    }
}
