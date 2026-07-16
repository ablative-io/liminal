//! Server-owned participant transport bindings.
//!
//! The repository modules are test-only executable regression fixtures. They
//! must not become production bindings until one durable conversation
//! aggregate can persist the protocol crate's complete typed transition.

#[cfg(test)]
mod crash_repository;
#[cfg(test)]
mod cursor_repository;
#[cfg(test)]
mod detach_repository;
mod transport;

#[cfg(test)]
mod crash_repository_tests;
#[cfg(test)]
mod cursor_repository_tests;
#[cfg(test)]
mod detach_repository_tests;
#[cfg(test)]
mod transport_tests;

pub use transport::{
    PARTICIPANT_CAPABILITY_BIT, ParticipantIngress, ParticipantSession, encode_server_value,
    gate_generic_frame, preflight_generic_bytes,
};
