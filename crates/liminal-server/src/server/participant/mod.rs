//! Server-owned participant storage and transport bindings.
//!
//! The conversation event stream, opaque aggregate owner, and connection-
//! incarnation stream are production durability boundaries over protocol-owned
//! transitions. The older crash/cursor/detach repositories remain test-only
//! regression fixtures until their operations move behind the aggregate.

#[allow(
    dead_code,
    reason = "production aggregate boundary is wired before its participant runtime consumer"
)]
mod aggregate;
#[allow(
    dead_code,
    reason = "production aggregate registry is wired before participant handlers consume it"
)]
mod aggregate_registry;
mod conversation_stream;
#[cfg(test)]
mod crash_repository;
#[cfg(test)]
mod cursor_repository;
#[cfg(test)]
mod detach_repository;
mod dispatch;
pub(super) mod incarnation_stream;
mod production;
pub(crate) mod publication;
mod transport;

#[cfg(test)]
mod aggregate_registry_tests;
#[cfg(test)]
mod aggregate_tests;
#[cfg(test)]
mod conversation_stream_tests;
#[cfg(test)]
mod crash_repository_tests;
#[cfg(test)]
mod cursor_repository_tests;
#[cfg(test)]
mod detach_repository_tests;
#[cfg(test)]
mod dispatch_tests;
#[cfg(test)]
mod incarnation_stream_tests;
#[cfg(test)]
mod transport_tests;

pub use dispatch::{
    InstalledParticipantService, ParticipantConnectionContext, ParticipantConnectionConversations,
    ParticipantDispatch, ParticipantDispatchError, ParticipantSemanticError,
    ParticipantSemanticHandler, dispatch_generic_frame,
};
pub(crate) use production::{ProductionParticipantHandler, constant_time_eq};
pub(crate) use publication::{
    ObserverPublication, ParticipantPublicationError, ParticipantPublicationInbox,
    ParticipantPublicationRegistry,
};
pub use publication::{
    ObserverPublicationTarget, ParticipantOfferedProgress, ParticipantPublication,
};
pub use transport::{
    PARTICIPANT_CAPABILITY_BIT, ParticipantIngress, ParticipantSession, encode_server_push,
    encode_server_value, gate_generic_frame, normalize_configured_frame_limit,
    preflight_generic_bytes,
};
