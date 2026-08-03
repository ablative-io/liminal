//! Runtime channel registration vocabulary: the roster's value types and its two
//! error taxonomies.
//!
//! The registration APIs themselves are inherent methods on
//! [`super::services::LiminalConnectionServices`] — the one adapter that owns a
//! channel roster — and they live beside the roster because they need its
//! private state. This module owns everything a CALLER of those methods names:
//! the request, the outcomes, the probe's answers, and the two error enums.
//!
//! # Why two error enums
//!
//! They have disjoint call sites and only one of them reaches the wire.
//! [`ChannelRegistryError`] answers the embedding host holding the services
//! handle: it is a control-plane refusal, never rendered into a frame.
//! [`ChannelAccessError`] answers a frame: it is produced by the roster
//! admission funnel on the publish/subscribe path and carries its own wire
//! reason code.
//!
//! # No `#[non_exhaustive]`
//!
//! Deliberate, and ruled on the record: these enums are exhaustive so a consumer
//! can `match` them and have the compiler tell it when this lane adds a case.
//! The cost — a new variant is a breaking change — is the price of that signal,
//! and it is priced at the cut rather than avoided by leaving every consumer a
//! catch-all arm it can never reason about.

use liminal::channel::ChannelMode;
use liminal::protocol::SchemaId as ProtocolSchemaId;
use liminal_protocol::reason_code::{CHANNEL_NOT_REGISTERED_CODE, CHANNEL_QUIESCED_CODE};

#[cfg(test)]
#[path = "channel_registry_tests.rs"]
mod channel_registry_tests;

/// The undifferentiated server error code, for the one [`ChannelAccessError`]
/// that is not a statement about the roster.
///
/// # Why this value is re-declared here rather than imported
///
/// `SERVER_ERROR_CODE` has no single owning definition in this crate: it is
/// minted privately, at the same value, in each module that needs it
/// (`apply.rs`, `pending_reply.rs`, `delivery.rs`). Each of those is a private
/// module-level `const`, reachable from nowhere else, so there is nothing to
/// import; this module is the fourth site and follows the same shape. The
/// authority the value is checked against is the band map in
/// `liminal_protocol::reason_code`, which records `0xFFFF` and enumerates every
/// site that mints it — this one included. Hoisting the four into one shared
/// definition is a worthwhile cleanup and is NOT this lane's: it would edit
/// `apply.rs`, which this step is fenced out of.
const SERVER_ERROR_CODE: u16 = 0xFFFF;

/// A channel to register at runtime.
///
/// Mirrors exactly what the boot loop consumes from a configured channel, minus
/// the config-file concerns: there is no `schema_ref` here because a path
/// resolved relative to a config file is not a runtime input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelRegistration {
    /// Channel name — the roster key.
    pub name: String,
    /// Raw JSON Schema bytes. `None` means the permissive empty schema `{}`,
    /// identical to a boot channel that declared no `schema_ref`. The protocol
    /// schema id is derived from THESE bytes by the same derivation the boot
    /// path uses, so an SDK deriving ids from schema bytes converges on it
    /// exactly as it does for a boot channel.
    pub schema_bytes: Option<Vec<u8>>,
    /// Durable vs ephemeral — the same bit a configured channel's `durable`
    /// carries, selecting the same [`ChannelMode`] and the same constructor.
    pub durable: bool,
}

/// Whether a registration created the channel or found it already identical.
///
/// Distinguishable on purpose: a projector's truth stream reports which one it
/// saw, and "already identical" is the answer that makes re-projection safe.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Registered {
    /// The channel was not on the roster and now is.
    Created,
    /// The channel was already on the roster with an identical configuration;
    /// nothing was built and nothing changed.
    AlreadyIdentical,
}

/// Where a roster entry came from.
///
/// Behaviourally inert — a boot-configured and a runtime-registered channel are
/// the same kind of object, built by the same function, on the same supervisor,
/// over the same store. The tag exists because it is the only field that
/// predicts what a process restart does: a `BootConfigured` entry is rebuilt
/// from the config file, a `RuntimeRegistered` entry is simply absent. It never
/// flips, so the population it names stays well defined over time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelOrigin {
    /// Built by the boot loop from the operator's `[[channels]]` config.
    BootConfigured,
    /// Registered at runtime through the embedded registration API.
    RuntimeRegistered,
}

/// The state machine's two states, as a value.
///
/// `Quiesced` is terminal within a process lifetime: there is no un-quiesce and
/// no removal, so the only exit is a restart, which drops the runtime roster
/// entirely.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChannelState {
    /// New publishes and new subscribes are admitted.
    Active,
    /// New publishes and new subscribes are refused, carrying `reason`;
    /// subscribers that already hold a stream keep it.
    Quiesced {
        /// The operator-supplied cause, recorded once at the transition.
        reason: String,
    },
}

/// The probe's typed answer.
///
/// A carrier-waits protocol keys its backoff on this and nothing else: each
/// answer is a distinct constructor, so a consumer reports which one it saw
/// without a string in sight.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChannelStatus {
    /// No entry of this name is on the roster.
    NotRegistered,
    /// The channel is on the roster and admitting.
    Active {
        /// Where the entry came from, and therefore what a restart does to it.
        origin: ChannelOrigin,
        /// Durable or ephemeral, read back off the live entry.
        mode: ChannelMode,
        /// The protocol schema id advertised to subscribers.
        schema: ProtocolSchemaId,
    },
    /// The channel is on the roster and refusing new access.
    Quiesced {
        /// The cause recorded at the transition.
        reason: String,
        /// Where the entry came from, and therefore what a restart does to it.
        origin: ChannelOrigin,
        /// Durable or ephemeral, read back off the live entry.
        mode: ChannelMode,
    },
}

/// One roster entry, minimally.
///
/// Deliberately NOT the full status: an enumeration is a census instrument, and
/// a census needs names, origins, and states — not schemas. A by-name probe can
/// confirm that every EXPECTED name is present but can never detect an
/// unexpected extra, which is why the enumerator exists at all.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelDescriptor {
    /// The roster key.
    pub name: String,
    /// Where the entry came from.
    pub origin: ChannelOrigin,
    /// The entry's state at the moment of the read.
    pub state: ChannelState,
}

/// The fields compared for configuration identity.
///
/// Named by type rather than by string so a consumer branches on the field that
/// differed instead of parsing a message for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelConfigField {
    /// Durable vs ephemeral.
    Mode,
    /// The protocol schema id advertised to subscribers. Derived from the RAW
    /// schema bytes, so two byte sequences that parse to the same document but
    /// differ in whitespace differ HERE — and must, because the id is on the
    /// wire.
    SchemaId,
    /// The parsed JSON Schema document. Compared as well as the id because the
    /// id is a 64-bit non-cryptographic digest: without this, a digest
    /// collision would silently accept a different schema as identical.
    SchemaDocument,
}

impl std::fmt::Display for ChannelConfigField {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mode => formatter.write_str("mode"),
            Self::SchemaId => formatter.write_str("schema id"),
            Self::SchemaDocument => formatter.write_str("schema document"),
        }
    }
}

/// The config key whose value bounds runtime registration.
///
/// Carried on both cap refusals so an operator is told what to declare rather
/// than merely that something is missing.
pub const MAX_CHANNELS_KEY: &str = "limits.max_channels";

/// Failures of the registration APIs. Never reaches the wire.
#[derive(Debug, thiserror::Error)]
pub enum ChannelRegistryError {
    /// The name exists with a DIFFERENT configuration. `field` names the first
    /// field that differs, by type — never a string a consumer must parse.
    #[error("channel '{name}' is already registered with a different {field}")]
    AlreadyRegistered {
        /// The contested roster key.
        name: String,
        /// The first field of the compared set that did not match.
        field: ChannelConfigField,
    },

    /// Quiesce or probe named a channel that is not on the roster.
    #[error("channel '{name}' is not registered")]
    NotRegistered {
        /// The absent roster key.
        name: String,
    },

    /// Re-quiesce under a DIFFERENT reason. Quiesce is one-way and its reason is
    /// written once; a second reason would silently lose one of the two.
    #[error("channel '{name}' is already quiesced: {reason}")]
    AlreadyQuiesced {
        /// The already-quiesced roster key.
        name: String,
        /// The reason already on record — not the one that was just refused.
        reason: String,
    },

    /// The schema bytes did not parse as JSON, or did not compile as a JSON
    /// Schema.
    #[error("channel '{name}' schema rejected: {message}")]
    SchemaRejected {
        /// The channel whose schema was refused.
        name: String,
        /// The parser's or the compiler's own diagnostic.
        message: String,
    },

    /// Durable initialization over the shared store failed.
    #[error("durable channel '{name}' could not be initialized: {message}")]
    DurableInitFailed {
        /// The channel whose durable construction failed.
        name: String,
        /// The library's own diagnostic.
        message: String,
    },

    /// Runtime registration was attempted with no cap declared. Refused rather
    /// than admitted: unbounded-by-default is not a bound.
    #[error(
        "runtime channel registration refused: no {cap} is configured; \
         a deployment that registers channels at runtime must declare its bound"
    )]
    CapNotConfigured {
        /// The config key the operator must declare.
        cap: &'static str,
    },

    /// The declared cap is already reached.
    #[error("channel registration refused: the {cap} limit of {limit} is reached")]
    CapReached {
        /// The config key that declared the bound.
        cap: &'static str,
        /// The configured value, so the refusal states the bound it enforced.
        limit: usize,
    },

    /// The roster lock is poisoned.
    #[error("channel roster unavailable: {message}")]
    RosterUnavailable {
        /// The diagnostic for the unavailable roster.
        message: String,
    },
}

/// The hot-path admission refusal.
///
/// Produced by the roster admission funnel and rendered to the wire with a
/// reason code. Distinct from [`ChannelRegistryError`] because this one crosses
/// to a client: every variant here has to answer [`Self::reason_code`].
#[derive(Debug, thiserror::Error)]
pub enum ChannelAccessError {
    /// No entry of this name is on the roster.
    #[error("channel '{name}' is not registered")]
    NotRegistered {
        /// The refused channel name.
        name: String,
    },

    /// The entry is on the roster but quiesced.
    #[error("channel '{name}' is quiesced: {reason}")]
    Quiesced {
        /// The refused channel name.
        name: String,
        /// The cause recorded at the transition, carried to the caller.
        reason: String,
    },

    /// The roster lock is poisoned, so no admission decision can be made.
    #[error("channel roster unavailable: {message}")]
    RosterUnavailable {
        /// The diagnostic for the unavailable roster.
        message: String,
    },
}

impl ChannelAccessError {
    /// The stable wire reason code for this refusal.
    ///
    /// The two roster codes are minted in `liminal-protocol` so an SDK client —
    /// which depends on that crate and not on this one — names them instead of
    /// hardcoding a literal. [`Self::RosterUnavailable`] keeps the
    /// undifferentiated server code: it is an internal fault, not a statement
    /// about the channel, and claiming otherwise would be a typed lie.
    #[must_use]
    pub const fn reason_code(&self) -> u16 {
        match self {
            Self::NotRegistered { .. } => CHANNEL_NOT_REGISTERED_CODE,
            Self::Quiesced { .. } => CHANNEL_QUIESCED_CODE,
            Self::RosterUnavailable { .. } => SERVER_ERROR_CODE,
        }
    }
}

/// A [`super::services::ConfiguredChannel`] could not be built.
///
/// Lane-internal plumbing, not part of the designed error taxonomy: it exists
/// so the ONE construction function can fail in a way BOTH of its callers map
/// without inspecting a message. The boot loop renders it into the exact
/// `ServerError::ConfigValidation` strings it has always produced
/// ([`Self::boot_message`]); registration renders it into the typed
/// [`ChannelRegistryError`] variants above.
#[derive(Debug)]
pub(super) enum ChannelBuildError {
    /// The JSON Schema document did not compile.
    SchemaRejected {
        /// The compiler's own diagnostic.
        message: String,
    },
    /// Durable initialization over the shared store failed.
    DurableInitFailed {
        /// The library's own diagnostic.
        message: String,
    },
}

impl ChannelBuildError {
    /// The boot path's message for this failure, byte-for-byte as the boot loop
    /// has always formatted it.
    pub(super) fn boot_message(&self, name: &str) -> String {
        match self {
            Self::SchemaRejected { message } => {
                format!("failed to initialize channel '{name}': {message}")
            }
            Self::DurableInitFailed { message } => {
                format!("failed to initialize durable channel '{name}': {message}")
            }
        }
    }

    /// The registration path's typed error for this failure.
    pub(super) fn into_registry_error(self, name: &str) -> ChannelRegistryError {
        match self {
            Self::SchemaRejected { message } => ChannelRegistryError::SchemaRejected {
                name: name.to_owned(),
                message,
            },
            Self::DurableInitFailed { message } => ChannelRegistryError::DurableInitFailed {
                name: name.to_owned(),
                message,
            },
        }
    }
}

/// The `AtomicU8` encoding of [`ChannelState`] on a roster entry: admitting.
pub(super) const STATE_ACTIVE: u8 = 0;

/// The `AtomicU8` encoding of [`ChannelState`] on a roster entry: refusing.
///
/// The transition `STATE_ACTIVE` → `STATE_QUIESCED` is performed by one
/// `compare_exchange` with `Release` success ordering, AFTER the reason has been
/// written to the entry's `OnceLock`. Every reader loads with `Acquire`, so a
/// reader that observes this value happens-after the reason's write and can read
/// it.
pub(super) const STATE_QUIESCED: u8 = 1;

/// The reason reported for an entry observed `STATE_QUIESCED` whose recorded
/// reason is not readable.
///
/// Unreachable by construction: the reason is written strictly BEFORE the
/// `Release` flip and read after an `Acquire` load of it. It exists because the
/// workspace denies `unwrap`/`expect`/`panic`, so the impossible branch must
/// still yield a value — and a value that names its own impossibility is the
/// only honest one. Seeing this string in a refusal means the ordering above was
/// broken, not that an operator supplied it.
pub(super) const UNRECORDED_QUIESCE_REASON: &str = "quiesce reason unrecorded (ordering violated)";
