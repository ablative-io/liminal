//! The crate's trybuild gate, in one dedicated integration target.
//!
//! WHY THIS TARGET EXISTS. trybuild spawns a full nested cargo build per case.
//! While these runners lived inside `src/.../production/tests_*.rs` they rode
//! inside the LIB TEST binary, so every scoped run that touched
//! `-p liminal-server` — including a two-crate iteration run that has no
//! interest in compile-fail contracts — silently dragged 13 nested builds and
//! their target trees behind it. That is what made a "scoped" command
//! unboundable: the cost was real but nothing in the command named it. Moving
//! the runners here does not make them cheaper; it makes them ADDRESSABLE, so
//! a tier can include or exclude them by naming this target.
//!
//! PATH RESOLUTION IS WHY THIS MOVE IS SAFE. trybuild resolves case paths
//! against `CARGO_MANIFEST_DIR`, which is the crate root for a unit test and
//! for an integration target alike. Every case string below is therefore
//! byte-identical to the string that was passed before the move, and no path
//! was rewritten to make the move work.
//!
//! THE CASE COUNT IS THE CONTRACT: 13 cases, 1 `pass` and 12 `compile_fail`,
//! which exactly exhausts `tests/trybuild/` (13 `.rs` files; 12 `.stderr`
//! expectation files, the `pass` case correctly needing none). A moved gate
//! that silently enumerates fewer cases is a gate that stopped gating, so the
//! count is stated here to be checked rather than assumed.

/// Lifted from `tests_w1a.rs`'s
/// `leave_projection_has_one_surviving_producer_and_duplicate_injection_refuses`.
///
/// Only the trybuild half moved. That function is a hybrid: two trybuild lines
/// bolted onto a live runtime oracle that drives a real
/// `ProductionParticipantHandler` through five crate-private seams
/// (`with_duplicate_leave_injection`, the `#[cfg(test)]` `dispatch` and
/// `test_participant_config`, and two file-local helpers). The oracle cannot
/// follow the case here without making those seams public, which would be an
/// API change wearing a move's clothes — so it stays exactly where it is, and
/// only the compile-fail contract crosses over.
#[test]
fn leave_projection_removal_stays_a_compile_error() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/trybuild/plain_leave_projection_removed.rs");
}

/// Moved verbatim from `tests_w1b_substrate.rs:449-463`. Eleven cases, name
/// and body unchanged; this function had no dependency on anything in that
/// file, not one of its imports and not one of its helpers.
#[test]
fn fenced_attach_linearity_ui_contract() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/trybuild/fenced_descriptions_remain_copy.rs");
    cases.compile_fail("tests/trybuild/fenced_raw_mint_is_private.rs");
    cases.compile_fail("tests/trybuild/fenced_owner_cannot_mint_twice.rs");
    cases.compile_fail("tests/trybuild/fenced_proof_cannot_clone.rs");
    cases.compile_fail("tests/trybuild/fenced_proof_cannot_copy.rs");
    cases.compile_fail("tests/trybuild/fenced_proof_cannot_reuse_after_verify.rs");
    cases.compile_fail("tests/trybuild/fenced_proof_fate_method_is_private.rs");
    cases.compile_fail("tests/trybuild/fenced_attach_commit_cannot_split_twice.rs");
    cases.compile_fail("tests/trybuild/validated_marker_record_cannot_clone.rs");
    cases.compile_fail("tests/trybuild/validated_marker_record_cannot_copy.rs");
    cases.compile_fail("tests/trybuild/validated_marker_record_cannot_feed_two_recoveries.rs");
}

/// Moved verbatim from `tests_w1b_connection_fate.rs:419-423`. One case, name
/// and body unchanged, no dependencies.
///
/// This is the THIRD runner. The move was dispatched as a two-runner move; the
/// bytes carried three, and leaving this one behind would have left trybuild
/// riding inside the lib test binary — defeating the entire purpose of the
/// target while the diff still looked like it had done the job.
#[test]
fn process_killed_has_no_production_participant_binding_emitter() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/trybuild/production_connection_fate_cannot_select_process_killed.rs");
}
