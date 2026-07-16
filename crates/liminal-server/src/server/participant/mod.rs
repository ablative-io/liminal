//! Server-owned participant transport, durability, and transaction bindings.

mod crash_repository;
mod cursor_repository;
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

pub use crash_repository::{
    CrashEnrollmentAllocation, CrashEnrollmentDigest, CrashTerminalDisposition,
    ParticipantCrashCause, ParticipantCrashRepository, ParticipantCrashRepositoryError,
    RecoveredCrashState,
};
pub use cursor_repository::{
    CursorAckCommand, CursorEpisodeRepository, CursorEpisodeStart, CursorRepositoryError,
};
pub use detach_repository::{
    DetachAllocation, EnrollmentAllocation, OrdinaryAttachAllocation, ParticipantDetachRepository,
    ParticipantDetachRepositoryError, ParticipantRequestDigest,
};

pub use transport::{
    PARTICIPANT_CAPABILITY_BIT, ParticipantIngress, ParticipantSession, encode_server_value,
    gate_generic_frame, preflight_generic_bytes,
};
