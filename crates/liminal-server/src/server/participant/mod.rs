//! Server-owned participant transport, durability, and transaction bindings.

mod cursor_repository;
mod transport;

#[cfg(test)]
mod cursor_repository_tests;
#[cfg(test)]
mod transport_tests;

pub use cursor_repository::{
    CursorAckCommand, CursorEpisodeRepository, CursorEpisodeStart, CursorRepositoryError,
};

pub use transport::{
    PARTICIPANT_CAPABILITY_BIT, ParticipantIngress, ParticipantSession, encode_server_value,
    gate_generic_frame, preflight_generic_bytes,
};
