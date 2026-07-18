/// Stable conversation identifier.
pub type ConversationId = u64;

/// Permanent participant identifier and base identity index.
pub type ParticipantId = u64;

/// Permanent participant index used by participant-scoped progress accounting.
pub type ParticipantIndex = u64;

/// Nonzero participant capability or retired generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Generation(core::num::NonZeroU64);

impl Generation {
    /// Canonical first participant generation.
    pub const ONE: Self = Self(core::num::NonZeroU64::MIN);

    /// Creates a generation, returning `None` for the forbidden zero value.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match core::num::NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the nonzero wire value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Conversation delivery sequence.
pub type DeliverySeq = u64;

/// Serialized conversation transaction order.
pub type TransactionOrder = u64;

/// Observer refusal/progress epoch.
pub type ObserverEpoch = u64;

/// Participant protocol version carried in every inner frame prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProtocolVersion {
    /// Major version.
    pub major: u16,
    /// Minor version.
    pub minor: u16,
}

impl ProtocolVersion {
    /// Participant protocol v1.0.
    pub const V1: Self = Self { major: 1, minor: 0 };

    /// Creates a protocol version.
    #[must_use]
    pub const fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }
}

/// Server and connection identity for one accepted connection incarnation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConnectionIncarnation {
    /// Durable server incarnation.
    pub server_incarnation: u64,
    /// Connection ordinal within that server incarnation.
    pub connection_ordinal: u64,
}

impl ConnectionIncarnation {
    /// Creates a connection incarnation.
    #[must_use]
    pub const fn new(server_incarnation: u64, connection_ordinal: u64) -> Self {
        Self {
            server_incarnation,
            connection_ordinal,
        }
    }
}

/// Immutable participant binding epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BindingEpoch {
    /// Connection that owns the binding.
    pub connection_incarnation: ConnectionIncarnation,
    /// Capability generation captured by the binding commit.
    pub capability_generation: Generation,
}

impl BindingEpoch {
    /// Creates a binding epoch.
    #[must_use]
    pub const fn new(
        connection_incarnation: ConnectionIncarnation,
        capability_generation: Generation,
    ) -> Self {
        Self {
            connection_incarnation,
            capability_generation,
        }
    }
}

macro_rules! fixed_credential {
    ($(#[$meta:meta])* $name:ident, $length:expr) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub struct $name([u8; $length]);

        impl $name {
            /// Creates the fixed-width value from its canonical bytes.
            #[must_use]
            pub const fn new(bytes: [u8; $length]) -> Self {
                Self(bytes)
            }

            /// Returns the canonical fixed-width bytes.
            #[must_use]
            pub const fn into_bytes(self) -> [u8; $length] {
                self.0
            }

            /// Borrows the canonical fixed-width bytes.
            #[must_use]
            pub const fn as_bytes(&self) -> &[u8; $length] {
                &self.0
            }
        }
    };
}

fixed_credential!(
    /// Single-purpose enrollment attempt token.
    EnrollmentToken,
    16
);
fixed_credential!(
    /// Single-purpose credential-attach attempt token.
    AttachAttemptToken,
    16
);
fixed_credential!(
    /// Single-purpose explicit-detach attempt token.
    DetachAttemptToken,
    16
);
fixed_credential!(
    /// Single-purpose terminal Leave attempt token.
    LeaveAttemptToken,
    16
);
fixed_credential!(
    /// Single-purpose ordinary record-admission attempt token.
    RecordAdmissionAttemptToken,
    16
);
fixed_credential!(
    /// Participant attach secret.
    AttachSecret,
    32
);
