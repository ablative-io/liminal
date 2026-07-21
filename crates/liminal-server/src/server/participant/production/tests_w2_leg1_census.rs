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
