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
mod binding_fate_completion;
mod capacity;
mod connection_fate;
mod connection_fate_allocation;
mod connection_fate_dispatch;
mod connection_fate_replay;
mod connection_fate_rows;
mod dispatch_impact;
#[cfg(test)]
pub mod dispatch_work;
mod facts;
mod fate_occurrence;
mod fenced_attach_codec;
mod fenced_attach_terminal;
mod frontier;
mod handler;
mod handler_observer;
mod handler_observer_reconcile;
mod handler_semantic;
mod log;
mod log_error;
mod log_v2;
mod log_v3;
mod marker_progress;
pub mod marker_source;
mod non_presenting_finalizer;
mod observer;
mod observer_progress;
mod observer_progress_plan;
mod occupancy;
mod ops_acks;
mod ops_attach;
mod ops_attach_capacity;
mod ops_attach_finalizer;
mod ops_attach_lookup;
mod ops_attach_verify;
mod ops_enroll;
mod ops_enroll_capacity;
mod ops_frontier;
mod ops_leave;
mod ops_nonzero_ack;
mod ops_session;
mod ops_session_replay;
mod outbox;
mod outbox_log;
mod outbox_projection;
mod outbox_replay;
mod pending_died_finalizer;
mod registry;
mod state;

#[cfg(test)]
mod e2e_cold_all_shapes;
#[cfg(test)]
mod e2e_cold_all_shapes_fixture;
#[cfg(test)]
mod e2e_cold_tests;
#[cfg(test)]
mod e2e_leave_commit_boundary;
#[cfg(test)]
mod e2e_leave_regression;
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod e2e_tests;
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
pub mod tests;
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
mod tests_delivery_acceptance;
#[cfg(test)]
mod tests_history;
#[cfg(test)]
mod tests_layer2;
#[cfg(test)]
mod tests_leave;
#[cfg(test)]
mod tests_log_v2;
#[cfg(test)]
mod tests_marker_ack;
#[cfg(test)]
mod tests_marker_ack_fixture;
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests_observer;
#[cfg(test)]
mod tests_observer_wake;
#[cfg(test)]
mod tests_observer_wake_fixture;
#[cfg(test)]
mod tests_outbox_barrier;
#[cfg(test)]
mod tests_outbox_barrier_fixture;
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
#[cfg(test)]
mod tests_unit2_layer1;
#[cfg(test)]
mod tests_w1a;
#[cfg(test)]
mod tests_w1a_leave_barrier;
#[cfg(test)]
mod tests_w1b_census;
#[cfg(test)]
mod tests_w1b_connection_fate;
#[cfg(test)]
mod tests_w1b_fate_completion;
#[cfg(test)]
mod tests_w1b_fenced_codec;
#[cfg(test)]
mod tests_w1b_fenced_finalizer;
#[cfg(test)]
mod tests_w1b_fenced_presenting;
#[cfg(test)]
mod tests_w1b_intent_recovery;
#[cfg(test)]
mod tests_w1b_marker_source;
#[cfg(test)]
mod tests_w1b_occurrence;
#[cfg(test)]
mod tests_w1b_oracle26;
#[cfg(test)]
mod tests_w1b_pending_detached_leave;
#[cfg(test)]
mod tests_w1b_pending_died_restart;
#[cfg(test)]
mod tests_w1b_pending_finalizer;
#[cfg(test)]
mod tests_w1b_projection;
#[cfg(test)]
mod tests_w1b_source_flush;
#[cfg(test)]
mod tests_w1b_substrate;
#[cfg(test)]
mod tests_w1b_umbrella;
#[cfg(test)]
mod tests_w1b_validation_memory;
#[cfg(test)]
mod tests_w2_impacts;
#[cfg(test)]
mod tests_w2_leg1_census;
#[cfg(test)]
mod tests_w2_leg2_census;
#[cfg(test)]
mod tests_w2_leg3_crash_early;
#[cfg(test)]
mod tests_w3_restore;
#[cfg(test)]
mod tests_w3_restore_fixture;

pub use facts::constant_time_eq;
pub use handler::ProductionParticipantHandler;
