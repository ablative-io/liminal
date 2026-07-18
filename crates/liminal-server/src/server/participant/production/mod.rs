//! Production participant semantics (LP gap closure, Part B activation).
//!
//! ONE production semantic handler exists, here, in the server. It owns no
//! lifecycle rules: every classification flows through the protocol crate's
//! lookups and total selectors, every mutation through the crate's typed
//! commits, every shell event through the A3 aggregate durability barrier,
//! and every reply through the A5 request-bound response authorities.
//!
//! Durability model: per conversation, one append-only transition-input log
//! whose entries carry the exact operation inputs plus the canonical shell
//! event bytes minted for them. Cold restore replays the log through the
//! same transitions and cross-checks the re-minted canonical bytes — the
//! server never serializes protocol state and never grows a second
//! implementation of lifecycle rules.

mod barrier;
mod capacity;
mod facts;
mod frontier;
mod handler;
mod handler_observer;
mod log;
mod observer;
mod occupancy;
mod ops_acks;
mod ops_attach;
mod ops_attach_capacity;
mod ops_attach_lookup;
mod ops_enroll;
mod ops_enroll_capacity;
mod ops_frontier;
mod ops_leave;
mod ops_session;
mod outbox;
mod outbox_log;
mod outbox_projection;
mod outbox_replay;
mod registry;
mod state;

#[cfg(test)]
mod e2e_cold_tests;
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod e2e_tests;
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests;
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests_binding;
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests_capacity;
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests_capacity_attach;
#[cfg(test)]
mod tests_config_d2;
#[cfg(test)]
mod tests_history;
#[cfg(test)]
mod tests_layer2;
#[cfg(test)]
mod tests_leave;
#[cfg(test)]
mod tests_log_v2;
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests_observer;
#[cfg(test)]
mod tests_outbox_log;
#[cfg(test)]
mod tests_outbox_owner;
#[cfg(test)]
mod tests_outbox_replay;
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests_receipts;
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests_receipts_enrollment;
#[cfg(test)]
mod tests_record_admission;
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests_residue;

pub use facts::constant_time_eq;
pub use handler::ProductionParticipantHandler;
