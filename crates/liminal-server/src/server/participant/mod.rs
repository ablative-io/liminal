//! Server-owned participant transport, durability, and transaction bindings.

mod transport;

#[cfg(test)]
mod transport_tests;

pub use transport::{
    PARTICIPANT_CAPABILITY_BIT, ParticipantIngress, ParticipantSession, encode_server_value,
    gate_generic_frame,
};
