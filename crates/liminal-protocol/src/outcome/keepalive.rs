use alloc::string::String;

/// Signed platform descriptor used in keepalive certification failures.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlatformName(String);

impl PlatformName {
    /// Creates a platform descriptor from its signed name.
    #[must_use]
    pub const fn new(name: String) -> Self {
        Self(name)
    }

    /// Borrows the signed platform name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the owned signed platform name.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

/// Numeric keepalive configuration field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeepaliveField {
    /// Idle seconds before the first probe.
    IdleSeconds,
    /// Seconds between probes.
    IntervalSeconds,
    /// Probe count before connection failure.
    ProbeCount,
}

/// Socket option selected by certification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeepaliveOption {
    /// Boolean `SO_KEEPALIVE` enablement.
    SoKeepalive,
    /// Platform idle-time option.
    Idle,
    /// Platform probe-interval option.
    Interval,
    /// Platform probe-count option.
    Count,
}

/// Phase in which keepalive certification failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeepalivePhase {
    /// Listener startup configuration validation.
    StartupConfiguration,
    /// Configuration of one accepted participant socket.
    AcceptedSocket,
}

/// Typed read-back value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeepaliveValue {
    /// Boolean enablement used only for `SO_KEEPALIVE`.
    Enabled(bool),
    /// Unsigned value used by idle, interval, and count options.
    Unsigned(u64),
}

/// Startup-only keepalive validation or support failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartupKeepaliveReason {
    /// A required numeric value was zero.
    Zero {
        /// First zero field in certification order.
        field: KeepaliveField,
        /// Requested value, which is zero for this variant.
        requested: u64,
        /// Required minimum, which is one for this variant.
        required_minimum: u64,
    },
    /// Requested value is outside the signed platform range.
    OutOfRange {
        /// First out-of-range field in certification order.
        field: KeepaliveField,
        /// Requested value.
        requested: u64,
        /// Inclusive signed platform minimum.
        supported_min: u64,
        /// Inclusive signed platform maximum.
        supported_max: u64,
        /// Signed target-platform descriptor.
        platform: PlatformName,
    },
    /// Requested value cannot be represented at platform granularity.
    GranularityMismatch {
        /// First mismatched field in certification order.
        field: KeepaliveField,
        /// Requested value.
        requested: u64,
        /// Signed platform granularity.
        granularity: u64,
        /// Signed target-platform descriptor.
        platform: PlatformName,
    },
    /// No signed keepalive descriptor exists for the platform.
    UnsupportedPlatform {
        /// Unsupported target-platform descriptor.
        platform: PlatformName,
    },
    /// A required option is absent from the signed descriptor.
    UnsupportedOption {
        /// First unsupported option in certification order.
        option: KeepaliveOption,
        /// Signed target-platform descriptor.
        platform: PlatformName,
    },
}

/// Accepted-socket-only set or read-back certification failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AcceptedSocketKeepaliveReason {
    /// Setting a required option failed.
    SetFailed {
        /// First option whose set failed.
        option: KeepaliveOption,
        /// Signed target-platform descriptor.
        platform: PlatformName,
        /// Raw operating-system error code.
        os_error: i32,
    },
    /// Reading back a required option failed.
    ReadbackFailed {
        /// First option whose read-back failed.
        option: KeepaliveOption,
        /// Signed target-platform descriptor.
        platform: PlatformName,
        /// Raw operating-system error code.
        os_error: i32,
    },
    /// Read-back value differs from the exact requested value.
    ReadbackMismatch {
        /// First option whose read-back differed.
        option: KeepaliveOption,
        /// Exact requested typed value.
        requested: KeepaliveValue,
        /// Exact effective typed value.
        effective: KeepaliveValue,
        /// Signed target-platform descriptor.
        platform: PlatformName,
    },
}

/// Keepalive startup or accepted-socket certification failed.
///
/// The phase is encoded by the outer variant, so startup-only and
/// accepted-socket-only reasons cannot be combined with the wrong phase.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeepaliveCertificationFailed {
    /// Startup configuration failed before opening the participant listener.
    StartupConfiguration(StartupKeepaliveReason),
    /// One accepted socket failed before participant negotiation.
    AcceptedSocket(AcceptedSocketKeepaliveReason),
}

impl KeepaliveCertificationFailed {
    /// Returns the phase fixed by this outcome's reason family.
    #[must_use]
    pub const fn phase(&self) -> KeepalivePhase {
        match self {
            Self::StartupConfiguration(_) => KeepalivePhase::StartupConfiguration,
            Self::AcceptedSocket(_) => KeepalivePhase::AcceptedSocket,
        }
    }
}
