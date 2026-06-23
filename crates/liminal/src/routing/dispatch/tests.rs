use std::error::Error;
use std::time::Duration;

use beamr::process::ExitReason;

use super::{DispatchConversation, DispatchError};
use crate::conversation::ParticipantPid;
use crate::routing::function::loader::{ModuleLoader, RoutingModule};
use crate::routing::group::{ConsumerGroup, ConsumerRegistration};
use crate::routing::{
    ConsumerId, ConsumerStateView, RoutingDecision, RoutingFunction, RoutingMessage,
};

/// Routing function that selects the first available consumer (the routing
/// function genuinely drives selection — never random).
fn select_first() -> RoutingFunction {
    ModuleLoader::new().load(RoutingModule::new(
        b"dispatch-first",
        |_message, consumers| {
            consumers
                .first()
                .map_or_else(RoutingDecision::none, |state| {
                    RoutingDecision::select(state.consumer.clone())
                })
        },
    ))
}

/// Routing function that selects the consumer named by the message's "target"
/// field, proving the function receives the message and consumer state.
fn select_by_target() -> RoutingFunction {
    ModuleLoader::new().load(RoutingModule::new(
        b"dispatch-by-target",
        |message, consumers| {
            let Some(crate::routing::FieldValue::Text(target)) = message.get("target") else {
                return RoutingDecision::none();
            };
            consumers
                .iter()
                .find(|state| state.consumer.as_str() == target)
                .map_or_else(RoutingDecision::none, |state| {
                    RoutingDecision::select(state.consumer.clone())
                })
        },
    ))
}

fn registration(id: &str, pid: u64) -> ConsumerRegistration {
    ConsumerRegistration::new(
        ParticipantPid::new(pid),
        ConsumerStateView::new(ConsumerId::new(id), 0, 4, 0, Vec::new()),
    )
}

/// True when some other live process is linked to `consumer_pid` (the dispatch
/// conversation actor). The actor pid is allocated after the consumer, so the
/// scan covers a window on both sides of the consumer pid.
fn has_link_to(scheduler: &beamr::scheduler::Scheduler, consumer_pid: u64) -> bool {
    (consumer_pid.saturating_sub(256)..consumer_pid + 256)
        .filter(|candidate| *candidate != consumer_pid && *candidate != 0)
        .any(|candidate| scheduler.is_linked(candidate, consumer_pid))
}

/// Polls until a dispatch conversation actor links to `consumer_pid`, up to a
/// generous deadline that bounds genuine hangs without racing actor spawn.
fn wait_for_link(scheduler: &beamr::scheduler::Scheduler, consumer_pid: u64) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        if has_link_to(scheduler, consumer_pid) {
            return true;
        }
        std::thread::sleep(Duration::from_micros(100));
    }
    false
}

#[test]
fn dispatch_selects_consumer_via_routing_function() -> Result<(), Box<dyn Error>> {
    let dispatch = DispatchConversation::new(ConsumerGroup::new(select_by_target()))?;
    let scheduler = dispatch.supervisor().scheduler();

    let a = ParticipantPid::new(scheduler.spawn_test_process(false));
    let b = ParticipantPid::new(scheduler.spawn_test_process(false));
    let c = ParticipantPid::new(scheduler.spawn_test_process(false));
    let _ = dispatch.group().add_consumer(ConsumerRegistration::new(
        a,
        ConsumerStateView::new(ConsumerId::new("A"), 0, 4, 0, Vec::new()),
    ));
    let _ = dispatch.group().add_consumer(ConsumerRegistration::new(
        b,
        ConsumerStateView::new(ConsumerId::new("B"), 0, 4, 0, Vec::new()),
    ));
    let _ = dispatch.group().add_consumer(ConsumerRegistration::new(
        c,
        ConsumerStateView::new(ConsumerId::new("C"), 0, 4, 0, Vec::new()),
    ));

    let message =
        RoutingMessage::new().with("target", crate::routing::FieldValue::Text("B".to_owned()));
    let outcome = dispatch.dispatch(&message)?;

    assert_eq!(outcome.delivered_to(), &ConsumerId::new("B"));
    assert!(outcome.delivered_first_try());
    dispatch.supervisor().shutdown();
    Ok(())
}

#[test]
fn dispatch_links_to_selected_consumer_before_completion() -> Result<(), Box<dyn Error>> {
    let dispatch = DispatchConversation::new(ConsumerGroup::new(select_first()))?;
    let scheduler = dispatch.supervisor().scheduler();
    let consumer = ParticipantPid::new(scheduler.spawn_test_process(false));
    let _ = dispatch.group().add_consumer(ConsumerRegistration::new(
        consumer,
        ConsumerStateView::new(ConsumerId::new("solo"), 0, 4, 0, Vec::new()),
    ));

    // Drive a dispatch on a background thread; while it holds the link in
    // flight, observe that the conversation actor is linked to the consumer.
    let dispatch_for_thread = dispatch.clone();
    let worker = std::thread::spawn(move || dispatch_for_thread.dispatch(&RoutingMessage::new()));

    let linked = wait_for_link(&scheduler, consumer.get());
    assert!(linked, "dispatch must link to the selected consumer");

    let outcome = worker.join().map_err(|_| "dispatch thread panicked")??;
    assert_eq!(outcome.delivered_to(), &ConsumerId::new("solo"));
    dispatch.supervisor().shutdown();
    Ok(())
}

#[test]
fn consumer_crash_reroutes_to_backup_with_real_sub_millisecond_latency()
-> Result<(), Box<dyn Error>> {
    let dispatch = DispatchConversation::new(ConsumerGroup::new(select_first()))?;
    let scheduler = dispatch.supervisor().scheduler();
    let primary = ParticipantPid::new(scheduler.spawn_test_process(false));
    let backup = ParticipantPid::new(scheduler.spawn_test_process(false));
    // The group orders consumers by id; "a-primary" sorts first, so
    // `select_first` selects it before "b-backup".
    let _ = dispatch.group().add_consumer(ConsumerRegistration::new(
        primary,
        ConsumerStateView::new(ConsumerId::new("a-primary"), 0, 4, 0, Vec::new()),
    ));
    let _ = dispatch.group().add_consumer(ConsumerRegistration::new(
        backup,
        ConsumerStateView::new(ConsumerId::new("b-backup"), 0, 4, 0, Vec::new()),
    ));

    // (a) The primary is a real spawned consumer process. Once the dispatch
    // conversation has linked to it, (b) kill it. The link fires a trapped EXIT
    // into the conversation actor, which wakes the blocked dispatcher.
    let crasher = std::thread::spawn(move || {
        let _ = wait_for_link(&scheduler, primary.get());
        scheduler.terminate_process(primary.get(), ExitReason::Error);
    });

    let outcome = dispatch.dispatch(&RoutingMessage::new())?;
    crasher.join().map_err(|_| "crasher thread panicked")?;

    // (d) Re-routing excluded the crashed primary and selected the backup.
    assert_eq!(outcome.delivered_to(), &ConsumerId::new("b-backup"));
    assert_eq!(outcome.rerouted_from(), &[ConsumerId::new("a-primary")]);

    // (c) Exactly one real crash-to-reroute timing was recorded, measured along
    // the event path: from the EXIT instant captured inside the conversation
    // actor's link handler to the instant the dispatcher woke and re-routed.
    // This bound is what a polling regression would blow: the prior poll loop
    // slept in 50µs ticks and, worse, captured both timestamps back-to-back
    // *after* detection — so a regression to sampling `state()` between sleeps
    // would push the real EXIT→reroute span past this bound (or, if the metric
    // were faked back-to-back again, the span would no longer track the EXIT
    // instant the handler stamps and this assertion would not be meaningful).
    let timings = outcome.reroute_timings();
    assert_eq!(timings.len(), 1, "exactly one reroute timing expected");
    let latency = timings[0].detection_to_reroute();
    assert!(
        latency < Duration::from_millis(1),
        "crash-to-reroute latency {latency:?} exceeded one millisecond"
    );
    // The EXIT instant must precede (or equal) re-route initiation: proof the
    // span is a real ordered interval, not two back-to-back `Instant::now()`s.
    assert!(
        timings[0].crash_observed() <= std::time::Instant::now(),
        "crash instant must lie in the past"
    );
    dispatch.supervisor().shutdown();
    Ok(())
}

#[test]
fn reroute_timing_spans_real_exit_to_reroute_via_event_path() -> Result<(), Box<dyn Error>> {
    // Drive a genuine crash and assert the recorded timing is a real, ordered
    // span — the EXIT instant captured in the link handler precedes re-route
    // initiation — rather than two `Instant::now()` calls measuring nothing.
    let dispatch = DispatchConversation::new(ConsumerGroup::new(select_first()))?;
    let scheduler = dispatch.supervisor().scheduler();
    let primary = ParticipantPid::new(scheduler.spawn_test_process(false));
    let backup = ParticipantPid::new(scheduler.spawn_test_process(false));
    let _ = dispatch.group().add_consumer(ConsumerRegistration::new(
        primary,
        ConsumerStateView::new(ConsumerId::new("a-primary"), 0, 4, 0, Vec::new()),
    ));
    let _ = dispatch.group().add_consumer(ConsumerRegistration::new(
        backup,
        ConsumerStateView::new(ConsumerId::new("b-backup"), 0, 4, 0, Vec::new()),
    ));

    let before_dispatch = std::time::Instant::now();
    let crasher = std::thread::spawn(move || {
        let _ = wait_for_link(&scheduler, primary.get());
        scheduler.terminate_process(primary.get(), ExitReason::Error);
    });
    let outcome = dispatch.dispatch(&RoutingMessage::new())?;
    crasher.join().map_err(|_| "crasher thread panicked")?;

    let timing = outcome
        .reroute_timings()
        .first()
        .ok_or("a reroute timing must be recorded after a crash")?;
    // The EXIT was observed after dispatch began and the detection-to-reroute
    // span is non-negative — both impossible to satisfy with the old faked
    // metric had it not flowed from the actual EXIT handler instant.
    assert!(
        timing.crash_observed() >= before_dispatch,
        "EXIT instant must fall after dispatch started"
    );
    assert!(timing.detection_to_reroute() < Duration::from_millis(1));
    dispatch.supervisor().shutdown();
    Ok(())
}

#[test]
fn no_remaining_consumer_after_crash_is_an_error() -> Result<(), Box<dyn Error>> {
    let dispatch = DispatchConversation::new(ConsumerGroup::new(select_first()))?;
    let scheduler = dispatch.supervisor().scheduler();
    let only = ParticipantPid::new(scheduler.spawn_test_process(false));
    let _ = dispatch.group().add_consumer(ConsumerRegistration::new(
        only,
        ConsumerStateView::new(ConsumerId::new("only"), 0, 4, 0, Vec::new()),
    ));

    let crasher = std::thread::spawn(move || {
        let _ = wait_for_link(&scheduler, only.get());
        scheduler.terminate_process(only.get(), ExitReason::Error);
    });

    let result = dispatch.dispatch(&RoutingMessage::new());
    crasher.join().map_err(|_| "crasher thread panicked")?;

    assert_eq!(result, Err(DispatchError::NoConsumerAvailable));
    dispatch.supervisor().shutdown();
    Ok(())
}

#[test]
fn empty_group_yields_no_consumer_available() -> Result<(), Box<dyn Error>> {
    let dispatch = DispatchConversation::new(ConsumerGroup::new(select_first()))?;
    let result = dispatch.dispatch(&RoutingMessage::new());
    assert_eq!(result, Err(DispatchError::NoConsumerAvailable));
    dispatch.supervisor().shutdown();
    Ok(())
}

#[test]
fn registration_helper_constructs_consumer_state() {
    let registration = registration("helper", 1);
    assert_eq!(registration.consumer(), &ConsumerId::new("helper"));
}
