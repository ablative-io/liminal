//! LIM-002 behavioural tests for the process-backed channel actor.
//!
//! Every test runs against a dedicated [`ChannelSupervisor`] (its own beamr
//! scheduler) so they are isolated from the process-global default supervisor
//! and from each other. Delivery is performed synchronously inside the actor's
//! `Publish` handler before its reply is sent, so a message is observable on a
//! subscriber's inbox as soon as `publish` returns; lifecycle effects driven by
//! a process EXIT (subscriber death) are asynchronous and are awaited with a
//! bounded poll.

use std::error::Error;
use std::time::{Duration, Instant};

use beamr::process::ExitReason;
use serde_json::{Value, json};

use crate::channel::registry::ChannelRegistry;
use crate::channel::subscription::SubscriptionHandle;
use crate::channel::supervisor::{ChannelRestartPolicy, ChannelSupervisor};
use crate::channel::{ChannelConfig, ChannelHandle, ChannelMode, Schema, SchemaValidationError};
use crate::envelope::Envelope;
use crate::error::LiminalError;

fn order_schema() -> Result<Schema, SchemaValidationError> {
    Schema::new(json!({
        "type": "object",
        "properties": {
            "order_id": {"type": "string"},
            "quantity": {"type": "integer", "minimum": 1}
        },
        "required": ["order_id", "quantity"],
        "additionalProperties": false
    }))
}

fn order_channel(supervisor: &ChannelSupervisor) -> Result<ChannelHandle, Box<dyn Error>> {
    let config = ChannelConfig::new("orders".to_owned(), order_schema()?, ChannelMode::Ephemeral);
    Ok(ChannelHandle::with_supervisor(config, supervisor.clone()))
}

/// Bounded poll for the next delivered envelope (delivery is in-process and
/// immediate after `publish`, but a small wait absorbs scheduler hand-off).
fn await_envelope(subscription: &SubscriptionHandle) -> Result<Envelope, Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if let Some(envelope) = subscription.try_next()? {
            return Ok(envelope);
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    Err("expected a delivered envelope".into())
}

/// Bounded poll until `predicate` over the live subscriber count holds.
fn await_count<F>(handle: &ChannelHandle, predicate: F) -> Result<usize, Box<dyn Error>>
where
    F: Fn(usize) -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut last = handle.subscriber_count()?;
    while Instant::now() < deadline {
        last = handle.subscriber_count()?;
        if predicate(last) {
            return Ok(last);
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    Err(format!("subscriber count {last} never satisfied predicate").into())
}

/// Crash the channel actor process `handle` is bound to with an abnormal exit
/// (the same `terminate_process(_, ExitReason::Error)` path the conversation
/// crash tests use) and block until that pid has actually left the scheduler's
/// process table, so the subsequent operation deterministically observes a dead
/// actor and drives a restart. Returns the crashed pid.
fn crash_actor(handle: &ChannelHandle) -> Result<u64, Box<dyn Error>> {
    let pid = handle.actor_pid()?;
    let scheduler = handle.scheduler()?;
    scheduler.terminate_process(pid, ExitReason::Error);
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if scheduler.process_table().get(pid).is_none() {
            return Ok(pid);
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    Err(format!("channel actor pid {pid} never left the process table after crash").into())
}

#[test]
fn channel_restarts_after_crash_and_publish_succeeds() -> Result<(), Box<dyn Error>> {
    let supervisor = ChannelSupervisor::new()?;
    let handle = order_channel(&supervisor)?;
    let subscription = handle.subscribe()?;
    let original_pid = handle.actor_pid()?;

    // Crash the live actor, then publish: the supervisor must restart it.
    crash_actor(&handle)?;
    handle.publish(br#"{"order_id":"A1","quantity":3}"#)?;

    // (a) The channel came back on a NEW pid and the publish was delivered.
    let restarted_pid = handle.actor_pid()?;
    assert_ne!(
        original_pid, restarted_pid,
        "the restarted actor must run on a fresh pid"
    );
    let envelope = await_envelope(&subscription)?;
    assert_eq!(
        envelope.payload,
        br#"{"order_id":"A1","quantity":3}"#.to_vec()
    );
    supervisor.shutdown();
    Ok(())
}

#[test]
fn subscriber_death_detection_survives_restart() -> Result<(), Box<dyn Error>> {
    // MAJOR-1 regression guard. A subscriber created BEFORE the crash must still
    // have its death detected AFTER the actor restarts. This only works if the
    // restarted actor re-links to the surviving subscriber on boot; without the
    // re-link the EXIT never reaches the new actor and the count never drops, so
    // this test fails (times out in `await_count`) before the fix and passes
    // after it.
    let supervisor = ChannelSupervisor::new()?;
    let handle = order_channel(&supervisor)?;
    let keep = handle.subscribe()?;
    let transient = handle.subscribe()?;
    assert_eq!(await_count(&handle, |count| count == 2)?, 2);

    // Crash + restart (the publish drives the restart and the boot re-link).
    crash_actor(&handle)?;
    handle.publish(br#"{"order_id":"A1","quantity":3}"#)?;
    assert_eq!(
        await_count(&handle, |count| count == 2)?,
        2,
        "both pre-crash subscribers must still be registered after restart"
    );

    // Drop one subscriber AFTER the restart: its EXIT must prune it from the
    // restarted actor's fan-out — proving the re-link was re-established.
    drop(transient);
    assert_eq!(await_count(&handle, |count| count == 1)?, 1);

    // The survivor still receives published messages on the restarted actor.
    handle.publish(br#"{"order_id":"A2","quantity":4}"#)?;
    let _ = await_envelope(&keep)?;
    supervisor.shutdown();
    Ok(())
}

#[test]
fn max_restarts_budget_bounds_restarts() -> Result<(), Box<dyn Error>> {
    // A budget of exactly one restart: the first crash restarts, the second
    // crash exhausts the budget and the next operation must surface an error
    // rather than silently respawning again.
    let supervisor = ChannelSupervisor::with_policy(ChannelRestartPolicy::one_for_one(1))?;
    let handle = order_channel(&supervisor)?;
    let _subscription = handle.subscribe()?;

    // First crash + restart (consumes the single restart from the budget).
    crash_actor(&handle)?;
    handle.publish(br#"{"order_id":"A1","quantity":3}"#)?;

    // Second crash: the budget is now exhausted, so the next operation must fail
    // and the actor must NOT be respawned.
    crash_actor(&handle)?;
    let result = handle.publish(br#"{"order_id":"A2","quantity":4}"#);
    assert!(
        matches!(result, Err(LiminalError::DeliveryFailed { .. })),
        "exhausting the restart budget must surface a DeliveryFailed error, got {result:?}"
    );
    supervisor.shutdown();
    Ok(())
}

#[test]
fn command_on_dead_actor_returns_promptly_not_after_timeout() -> Result<(), Box<dyn Error>> {
    // MINOR-2: if the actor dies while a command's reply sender sits in the
    // command queue, the host must NOT block the full 5s COMMAND_TIMEOUT. With a
    // `never` restart policy a crashed actor stays dead, so a subsequent
    // operation must surface an error well within the timeout. We assert a tight
    // wall-clock bound: a hang would blow far past it.
    let supervisor = ChannelSupervisor::with_policy(ChannelRestartPolicy::never())?;
    let handle = order_channel(&supervisor)?;
    let _subscription = handle.subscribe()?;
    // Confirm the actor is live, then crash it so it can never reply.
    let _ = handle.actor_pid()?;
    crash_actor(&handle)?;

    let started = Instant::now();
    let result = handle.publish(br#"{"order_id":"A1","quantity":3}"#);
    let elapsed = started.elapsed();
    assert!(
        result.is_err(),
        "publish to a dead non-restarting actor must fail"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "command on a dead actor must return promptly (took {elapsed:?}), not block the 5s timeout"
    );
    supervisor.shutdown();
    Ok(())
}

#[test]
fn restarting_one_channel_does_not_disturb_another() -> Result<(), Box<dyn Error>> {
    let supervisor = ChannelSupervisor::new()?;
    let orders = ChannelHandle::with_supervisor(
        ChannelConfig::new("orders".to_owned(), order_schema()?, ChannelMode::Ephemeral),
        supervisor.clone(),
    );
    let events = ChannelHandle::with_supervisor(
        ChannelConfig::new("events".to_owned(), order_schema()?, ChannelMode::Ephemeral),
        supervisor.clone(),
    );
    let orders_sub = orders.subscribe()?;
    let events_sub = events.subscribe()?;
    let events_pid = events.actor_pid()?;

    // Crash + restart ONLY the orders channel.
    crash_actor(&orders)?;
    orders.publish(br#"{"order_id":"only-orders","quantity":2}"#)?;
    let delivered = await_envelope(&orders_sub)?;
    assert_eq!(
        delivered.payload,
        br#"{"order_id":"only-orders","quantity":2}"#.to_vec()
    );

    // The events channel was untouched: same pid, still delivering.
    assert_eq!(
        events.actor_pid()?,
        events_pid,
        "the unrelated channel actor must keep its original pid"
    );
    events.publish(br#"{"order_id":"events-live","quantity":1}"#)?;
    let event = await_envelope(&events_sub)?;
    assert_eq!(
        event.payload,
        br#"{"order_id":"events-live","quantity":1}"#.to_vec()
    );
    supervisor.shutdown();
    Ok(())
}

#[test]
fn publish_valid_message_delivers_envelope() -> Result<(), Box<dyn Error>> {
    let supervisor = ChannelSupervisor::new()?;
    let handle = order_channel(&supervisor)?;
    let subscription = handle.subscribe()?;

    handle.publish_from("publisher-1", br#"{"order_id":"A1","quantity":3}"#)?;
    let envelope = await_envelope(&subscription)?;

    assert_eq!(
        envelope.payload,
        br#"{"order_id":"A1","quantity":3}"#.to_vec()
    );
    assert_eq!(envelope.publisher_id.as_str(), "publisher-1");
    assert!(envelope.causal_context.is_none());
    supervisor.shutdown();
    Ok(())
}

#[test]
fn publish_invalid_message_returns_schema_mismatch_without_delivery() -> Result<(), Box<dyn Error>>
{
    let supervisor = ChannelSupervisor::new()?;
    let handle = order_channel(&supervisor)?;
    let subscription = handle.subscribe()?;

    let result = handle.publish(br#"{"order_id":"A1","quantity":0}"#);

    assert!(matches!(result, Err(LiminalError::SchemaMismatch { .. })));
    assert!(subscription.try_next()?.is_none());
    supervisor.shutdown();
    Ok(())
}

#[test]
fn evolved_schema_keeps_existing_subscriber_and_applies_default() -> Result<(), Box<dyn Error>> {
    let supervisor = ChannelSupervisor::new()?;
    let handle = order_channel(&supervisor)?;
    let subscription = handle.subscribe()?;
    let schema_id =
        handle.evolve_schema_add_field("priority", json!({"type":"string"}), json!("normal"))?;

    handle.publish(br#"{"order_id":"A1","quantity":3}"#)?;
    let envelope = await_envelope(&subscription)?;
    let payload: Value = serde_json::from_slice(&envelope.payload)?;

    assert_eq!(envelope.schema_id, schema_id);
    assert_eq!(payload.get("priority"), Some(&json!("normal")));
    supervisor.shutdown();
    Ok(())
}

#[test]
fn predicate_subscription_only_receives_matching_messages() -> Result<(), Box<dyn Error>> {
    let supervisor = ChannelSupervisor::new()?;
    let handle = order_channel(&supervisor)?;
    // Only deliver orders with quantity >= 5.
    let filtered = handle.subscribe_filtered(|envelope: &Envelope| {
        serde_json::from_slice::<Value>(&envelope.payload)
            .ok()
            .and_then(|value| value.get("quantity").and_then(Value::as_u64))
            .is_some_and(|quantity| quantity >= 5)
    })?;
    let unfiltered = handle.subscribe()?;

    handle.publish(br#"{"order_id":"small","quantity":1}"#)?;
    handle.publish(br#"{"order_id":"big","quantity":9}"#)?;

    // The unfiltered subscriber receives both, in order.
    let first = await_envelope(&unfiltered)?;
    let second = await_envelope(&unfiltered)?;
    assert_eq!(
        first.payload,
        br#"{"order_id":"small","quantity":1}"#.to_vec()
    );
    assert_eq!(
        second.payload,
        br#"{"order_id":"big","quantity":9}"#.to_vec()
    );

    // The filtered subscriber receives ONLY the matching (big) order.
    let matched = await_envelope(&filtered)?;
    assert_eq!(
        matched.payload,
        br#"{"order_id":"big","quantity":9}"#.to_vec()
    );
    assert!(filtered.try_next()?.is_none());
    supervisor.shutdown();
    Ok(())
}

#[test]
fn dropping_subscription_removes_subscriber_via_exit() -> Result<(), Box<dyn Error>> {
    let supervisor = ChannelSupervisor::new()?;
    let handle = order_channel(&supervisor)?;
    let keep = handle.subscribe()?;
    let transient = handle.subscribe()?;
    assert_eq!(await_count(&handle, |count| count == 2)?, 2);

    // Dropping the handle terminates the subscriber process; the channel actor
    // traps the resulting EXIT and prunes it from the fan-out list — no polling
    // of a weak pointer is involved.
    drop(transient);
    assert_eq!(await_count(&handle, |count| count == 1)?, 1);

    // The surviving subscriber still receives published messages.
    handle.publish(br#"{"order_id":"A1","quantity":3}"#)?;
    let envelope = await_envelope(&keep)?;
    assert_eq!(
        envelope.payload,
        br#"{"order_id":"A1","quantity":3}"#.to_vec()
    );
    supervisor.shutdown();
    Ok(())
}

#[test]
fn explicit_unsubscribe_removes_subscriber() -> Result<(), Box<dyn Error>> {
    let supervisor = ChannelSupervisor::new()?;
    let handle = order_channel(&supervisor)?;
    let subscription = handle.subscribe()?;
    assert_eq!(await_count(&handle, |count| count == 1)?, 1);

    handle.unsubscribe(&subscription)?;
    assert_eq!(await_count(&handle, |count| count == 0)?, 0);
    supervisor.shutdown();
    Ok(())
}

#[test]
fn publish_after_close_is_rejected() -> Result<(), Box<dyn Error>> {
    let supervisor = ChannelSupervisor::new()?;
    let handle = order_channel(&supervisor)?;
    handle.close()?;

    // The actor process stopped on close; a publish therefore cannot be
    // delivered. It must fail rather than silently succeed.
    let result = handle.publish(br#"{"order_id":"A1","quantity":3}"#);
    assert!(result.is_err());
    supervisor.shutdown();
    Ok(())
}

#[test]
fn registry_creates_looks_up_lists_and_rejects_duplicates() -> Result<(), Box<dyn Error>> {
    let registry = ChannelRegistry::new()?;
    let config = ChannelConfig::new("orders".to_owned(), order_schema()?, ChannelMode::Ephemeral);

    let handle = registry.create(config.clone())?;
    let _subscription = handle.subscribe()?;

    // Lookup returns the same channel.
    assert!(registry.lookup("orders")?.is_some());
    assert!(registry.lookup("missing")?.is_none());

    // Duplicate names are rejected.
    assert!(matches!(
        registry.create(config),
        Err(LiminalError::PublishFailed { .. })
    ));

    // List reports the channel with its live subscriber count.
    let summaries = registry.list()?;
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].name, "orders");
    assert_eq!(summaries[0].subscriber_count, 1);

    // Close removes it.
    assert!(registry.close("orders")?);
    assert!(registry.lookup("orders")?.is_none());
    assert!(!registry.close("orders")?);
    registry.shutdown();
    Ok(())
}

#[test]
fn separate_channels_are_independent_processes() -> Result<(), Box<dyn Error>> {
    let supervisor = ChannelSupervisor::new()?;
    let orders = ChannelHandle::with_supervisor(
        ChannelConfig::new("orders".to_owned(), order_schema()?, ChannelMode::Ephemeral),
        supervisor.clone(),
    );
    let events = ChannelHandle::with_supervisor(
        ChannelConfig::new("events".to_owned(), order_schema()?, ChannelMode::Ephemeral),
        supervisor.clone(),
    );
    let orders_sub = orders.subscribe()?;
    let events_sub = events.subscribe()?;

    orders.publish(br#"{"order_id":"only-orders","quantity":2}"#)?;
    let delivered = await_envelope(&orders_sub)?;
    assert_eq!(
        delivered.payload,
        br#"{"order_id":"only-orders","quantity":2}"#.to_vec()
    );
    // The events channel's subscriber receives nothing — separate process,
    // separate subscriber list.
    assert!(events_sub.try_next()?.is_none());
    supervisor.shutdown();
    Ok(())
}

#[test]
fn restart_policy_never_is_configurable() -> Result<(), Box<dyn Error>> {
    // Supervision is configurable (R4): a `never` policy is accepted and exposed.
    let supervisor = ChannelSupervisor::with_policy(ChannelRestartPolicy::never())?;
    assert_eq!(supervisor.policy(), &ChannelRestartPolicy::never());
    let handle = order_channel(&supervisor)?;
    let subscription = handle.subscribe()?;
    handle.publish(br#"{"order_id":"A1","quantity":3}"#)?;
    let _ = await_envelope(&subscription)?;
    supervisor.shutdown();
    Ok(())
}
