use std::error::Error;

use super::e2e_cold_all_shapes::cold_reopen_reconciles_and_replays_all_record_shapes;
use super::tests_w1b_fenced_finalizer::pending_died_recovered_reservation_makes_fenced_attach_finalizer_non_presenting;
use super::tests_w1b_fenced_presenting::pending_terminal_composed_by_attach_presents_only_attached_source;
use super::tests_w1b_intent_recovery::run_post_middle_failure_recovery;
use super::tests_w1b_pending_detached_leave::pending_detached_finalized_by_leave_presents_only_live_leave_commit;
use super::tests_w1b_pending_died_restart::pending_died_finalized_by_leave_presents_only_live_leave_commit;
use super::tests_w1b_pending_finalizer::pending_died_recovered_reservation_makes_leave_finalizer_non_presenting;

#[test]
fn fate_live_and_cold_replay_produce_identical_witnesses_and_state() -> Result<(), Box<dyn Error>> {
    // Historical Open recovery proves failure-tail bounds and startup repair.
    run_post_middle_failure_recovery()?;

    // Exercise every pending-finalizer grammar/presentation branch and compare
    // its live durable rows, reservations, and sole observer witnesses.
    pending_died_finalized_by_leave_presents_only_live_leave_commit()?;
    pending_detached_finalized_by_leave_presents_only_live_leave_commit()?;
    pending_terminal_composed_by_attach_presents_only_attached_source()?;
    pending_died_recovered_reservation_makes_leave_finalizer_non_presenting()?;
    pending_died_recovered_reservation_makes_fenced_attach_finalizer_non_presenting()?;

    // The all-shapes socket fixture decodes the four fate classes plus outbox,
    // derives replay obligations from those observed rows, cold-opens the same
    // store, and compares every replayed delivery to that live durable state.
    cold_reopen_reconciles_and_replays_all_record_shapes()
}
