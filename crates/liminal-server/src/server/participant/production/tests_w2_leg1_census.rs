//! Exact-name structural acceptance for the W2 leg-1 owner and TOLD seams.

use std::error::Error;

const HANDLER_SEMANTIC: &str = include_str!("handler_semantic.rs");
const TCP_PROCESS: &str = include_str!("../../connection/process.rs");
const WEBSOCKET_PROCESS: &str = include_str!("../../connection/websocket/process.rs");
const PUBLICATION: &str = include_str!("../publication.rs");

fn assert_order(source: &str, needles: &[&str]) -> Result<(), Box<dyn Error>> {
    let mut remaining = source;
    for needle in needles {
        let position = remaining
            .find(needle)
            .ok_or_else(|| format!("ordered production seam `{needle}` was absent"))?;
        remaining = &remaining[position + needle.len()..];
    }
    Ok(())
}

#[test]
fn obligation_debt_dispatch_seams_before_delivery_after_under_one_owner()
-> Result<(), Box<dyn Error>> {
    let seam = "let decision = decide_obligation_debt_dispatch(";
    assert_eq!(HANDLER_SEMANTIC.match_indices(seam).count(), [seam].len());
    assert_order(
        HANDLER_SEMANTIC,
        &[
            "let owner = cell",
            ".lock()",
            "let decision = decide_obligation_debt_dispatch(",
            "outbox",
            ".delivery_after(participant_id, dispatch_after)",
            "drop(owner)",
        ],
    )
}

#[test]
fn obligation_debt_dispatch_never_unlocks_between_debt_and_obligation_selection()
-> Result<(), Box<dyn Error>> {
    assert_order(
        HANDLER_SEMANTIC,
        &[
            "fn next_publication(",
            ".lock()",
            "let Some(dispatch_state) = authority.obligation_debt_dispatch()",
            "let decision = decide_obligation_debt_dispatch(",
            ".delivery_after(participant_id, dispatch_after)",
            "drop(owner)",
        ],
    )?;
    let locked_decision = HANDLER_SEMANTIC
        .split_once("fn next_publication(")
        .and_then(|(_, tail)| tail.split_once("drop(owner)"))
        .map(|(body, _)| body)
        .ok_or("next_publication locked decision body was not bounded")?;
    assert!(!locked_decision.contains("publication_registry"));
    assert!(!locked_decision.contains("encode_server_push"));
    Ok(())
}

#[test]
fn obligation_debt_dispatch_preserves_pump_order_on_both_transports() -> Result<(), Box<dyn Error>>
{
    assert_order(
        TCP_PROCESS,
        &[
            "self.service_socket(pid)",
            "self.service_pending_replies()",
            "self.service_participant_pushes(pid)",
            "service_subscriptions(",
            "self.drain_outbound()",
        ],
    )?;
    assert_order(
        WEBSOCKET_PROCESS,
        &[
            "self.service_socket(pid)",
            "self.service_pending_replies()",
            "service_participant_publications(",
            "service_subscriptions(",
            "self.drain_outbound(",
        ],
    )?;
    assert!(TCP_PROCESS.contains("service_participant_publications("));
    assert!(WEBSOCKET_PROCESS.contains("service_participant_publications("));
    Ok(())
}

#[test]
fn dispatch_source_has_no_timer_sweep_or_periodic_probe() -> Result<(), Box<dyn Error>> {
    let selector_call = "let decision = decide_obligation_debt_dispatch(";
    assert_eq!(
        HANDLER_SEMANTIC.match_indices(selector_call).count(),
        [selector_call].len()
    );
    for source in [TCP_PROCESS, WEBSOCKET_PROCESS, PUBLICATION] {
        assert!(!source.contains(selector_call));
    }
    let decision_body = HANDLER_SEMANTIC
        .split_once(selector_call)
        .and_then(|(_, tail)| tail.split_once("drop(owner)"))
        .map(|(body, _)| body)
        .ok_or("dispatch decision body was not bounded")?;
    for forbidden in ["sleep(", "interval(", "timer(", "sweep(", "register("] {
        assert!(
            !decision_body.contains(forbidden),
            "dispatch decision gained forbidden source `{forbidden}`"
        );
    }
    Ok(())
}

#[test]
fn debt_tell_between_drain_and_wait_is_seen_by_final_probe() -> Result<(), Box<dyn Error>> {
    for source in [TCP_PROCESS, WEBSOCKET_PROCESS] {
        assert_order(
            source,
            &[
                "self.arm_readiness(pid, ctx, interest)",
                "self.runtime.run_pre_wait_barrier()",
                "self.final_probe(pid, ctx)",
            ],
        )?;
        let final_probe = source
            .split_once("fn final_probe(")
            .map(|(_, body)| body)
            .ok_or("transport final probe was absent")?;
        assert!(final_probe.contains("participant_publication"));
        assert!(final_probe.contains("has_pending()"));
    }
    Ok(())
}

#[test]
fn temporary_fate_preserves_cursor_facts_and_rebinds_exact_epoch() -> Result<(), Box<dyn Error>> {
    super::e2e_tests::ack_after_reattach_before_replay_accepts_after_reconciliation()
}

#[test]
fn coupled_debt_owner_covers_every_w1b_fate_and_finalizer_route() -> Result<(), Box<dyn Error>> {
    super::tests_w1b_umbrella::fate_live_and_cold_replay_produce_identical_witnesses_and_state()
}

#[test]
fn marker_drain_retry_accumulates_all_prefix_effects() -> Result<(), Box<dyn Error>> {
    let frontier = include_str!("ops_frontier.rs");
    let leave = include_str!("ops_leave.rs");
    let record_call = "self.apply_record_admission_with_impact(";
    let leave_call = "self.apply_leave_with_impact(";
    let record_calls = frontier.match_indices(record_call).count();
    let leave_calls = leave.match_indices(leave_call).count();
    let adapter_and_recursion = ["test adapter", "production recursion"];
    assert_eq!(record_calls, adapter_and_recursion.len());
    assert_eq!(leave_calls, adapter_and_recursion.len());
    assert!(frontier.contains("RecordAdmissionDecision::DrainFirst"));
    assert!(leave.contains("if let Some(candidate) = next_immutable"));
    assert_order(
        frontier,
        &[
            "RecordAdmissionDecision::DrainFirst",
            "persist_drain_first(candidate, owner, appender, impact)",
            record_call,
        ],
    )?;
    let terminal_drain = include_str!("ops_terminal_drain.rs");
    assert_order(
        terminal_drain,
        &[
            "ImmutableSequenceCandidate::Marker(_)",
            "persist_next_marker(candidate, owner, appender, impact)",
            "ImmutableSequenceCandidate::BindingTerminal { .. }",
            "persist_terminal_drain(candidate, owner, appender, impact)",
        ],
    )?;
    assert_order(
        leave,
        &[
            "if let Some(candidate) = next_immutable",
            "persist_next_marker(candidate, owner, appender, impact)",
            leave_call,
        ],
    )?;

    let outbox_log = include_str!("outbox_log.rs");
    let projection = include_str!("outbox_projection.rs");
    let produced_kinds = [
        "Enrolled",
        "Attached",
        "Detached",
        "Died",
        "MarkerDrained",
        "RecordAdmission",
        "Left",
    ];
    for kind in produced_kinds {
        assert!(outbox_log.contains(&format!("    {kind},")));
        assert!(projection.contains(&format!("ProducedSourceKind::{kind}")));
    }

    super::e2e_cold_all_shapes::cold_reopen_reconciles_and_replays_all_record_shapes()
}
