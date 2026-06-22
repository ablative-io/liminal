use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use super::projection::{HistoryProjection, HistoryProjectionError, InMemoryHistoryProjection};
use super::*;
use crate::aion::types::Payload;

#[test]
fn start_creates_durable_named_channel_and_reuses_history() -> Result<(), Box<dyn Error>> {
    let context = HistoryContext::default();
    let channel = context.start_workflow_history("prod", "wf-1")?;

    assert_eq!(channel.channel_name.as_str(), "aion.history.prod.wf-1");
    assert_eq!(channel.mode, ChannelMode::Durable);
    context.publish_recorded_event("prod", "wf-1", event(1))?;
    let restarted = context.start_workflow_history("prod", "wf-1")?;
    let subscriber = context.subscribe_history("prod", "wf-1", None)?;

    assert_eq!(restarted.channel_name, channel.channel_name);
    assert_eq!(subscriber.try_next()?.map(|event| event.sequence), Some(1));
    Ok(())
}

#[test]
fn publish_after_record_orders_append_before_projection() -> Result<(), Box<dyn Error>> {
    let projection = Arc::new(TrackingProjection::default());
    let context = HistoryContext::with_projection(projection.clone());
    let published = context.publish_after_record("prod", "wf-2", || {
        projection.record("store")?;
        Ok(event(7))
    })?;
    let subscriber = context.subscribe_history("prod", "wf-2", Some(0))?;

    assert_eq!(published.sequence, 7);
    assert_eq!(projection.steps()?, vec!["store", "projection"]);
    assert_eq!(subscriber.try_next()?.map(|event| event.sequence), Some(7));
    Ok(())
}

#[test]
fn publish_after_record_does_not_publish_when_append_fails() -> Result<(), Box<dyn Error>> {
    let context = HistoryContext::default();
    let result = context.publish_after_record("prod", "wf-append-fails", || {
        Err(AionSurfaceError::StreamingFailed {
            channel_name: "event-store".to_owned(),
            workflow_id: "wf-append-fails".to_owned(),
            message: "store append failed".to_owned(),
        })
    });
    let subscriber = context.subscribe_history("prod", "wf-append-fails", None)?;

    assert!(matches!(
        result,
        Err(AionSurfaceError::StreamingFailed { .. })
    ));
    assert_eq!(subscriber.try_next()?, None);
    Ok(())
}

#[test]
fn publish_failure_is_reported_without_returning_error() -> Result<(), Box<dyn Error>> {
    let projection = Arc::new(FailingProjection);
    let reporter = Arc::new(CollectingReporter::default());
    let context = HistoryContext::with_projection_and_reporter(projection, reporter.clone());
    let result = context.publish_recorded_event("prod", "wf-3", event(3));
    let failures = reporter.failures()?;

    assert!(result.is_ok());
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].channel_name, "aion.history.prod.wf-3");
    assert_eq!(failures[0].workflow_id, "wf-3");
    assert_eq!(failures[0].sequence, 3);
    assert!(failures[0].error.contains("forced projection failure"));
    Ok(())
}

#[test]
fn cursor_replay_and_live_delivery_are_typed_and_independent() -> Result<(), Box<dyn Error>> {
    let context = HistoryContext::default();
    for sequence in 1..=10 {
        context.publish_recorded_event("prod", "wf-4", event(sequence))?;
    }
    let from_five = context.subscribe_history("prod", "wf-4", Some(5))?;
    let from_start = context.subscribe_history("prod", "wf-4", None)?;
    let from_zero = context.subscribe_history("prod", "wf-4", Some(0))?;

    assert_eq!(drain(&from_five)?, (6..=10).collect::<Vec<_>>());
    assert_eq!(drain(&from_start)?, (1..=10).collect::<Vec<_>>());
    assert_eq!(drain(&from_zero)?, (1..=10).collect::<Vec<_>>());

    let live = context.subscribe_history("prod", "wf-4", Some(10))?;
    let expected = event(11);
    context.publish_recorded_event("prod", "wf-4", expected.clone())?;
    assert_eq!(live.try_next()?, Some(expected));
    Ok(())
}

#[test]
fn multiple_subscribers_receive_live_history_independently() -> Result<(), Box<dyn Error>> {
    let context = HistoryContext::default();
    let first = context.subscribe_history("prod", "wf-multi", Some(0))?;
    let second = context.subscribe_history("prod", "wf-multi", Some(0))?;
    let expected = event(21);

    context.publish_recorded_event("prod", "wf-multi", expected.clone())?;

    assert_eq!(first.try_next()?, Some(expected.clone()));
    assert_eq!(second.try_next()?, Some(expected));
    Ok(())
}

#[test]
fn replay_and_live_overlap_delivers_each_event_exactly_once() -> Result<(), Box<dyn Error>> {
    let projection = Arc::new(RaceProjection::default());
    let context = HistoryContext::with_projection(projection.clone());
    for sequence in 1..=5 {
        context.publish_recorded_event("prod", "wf-overlap", event(sequence))?;
    }

    // Force the publish-racing-subscribe overlap: event 6 is published from inside
    // `read_after`, after `subscribe_history` has captured the live handle but before it
    // takes the replay snapshot. Event 6 therefore lands in BOTH the live inbox and the
    // replay snapshot, so the dedup guard must drop the duplicate live copy.
    let racing_context = context.clone();
    projection.set_on_first_read(Box::new(move || {
        let _ = racing_context.publish_recorded_event("prod", "wf-overlap", event(6));
    }));

    let subscription = context.subscribe_history("prod", "wf-overlap", Some(0))?;
    let delivered = drain(&subscription)?;

    assert_eq!(delivered, (1..=6).collect::<Vec<_>>());
    assert_eq!(
        delivered.iter().filter(|&&sequence| sequence == 6).count(),
        1,
        "overlapping event 6 must be delivered exactly once"
    );
    Ok(())
}

#[test]
fn cursor_boundary_overlap_delivers_event_exactly_once() -> Result<(), Box<dyn Error>> {
    let projection = Arc::new(RaceProjection::default());
    let context = HistoryContext::with_projection(projection.clone());
    for sequence in 1..=5 {
        context.publish_recorded_event("prod", "wf-boundary", event(sequence))?;
    }

    // Boundary case: a subscriber resumes at cursor N-1 (5) while event N (6) arrives
    // concurrently, reaching both the live inbox and a replay snapshot that now includes
    // it. Exactly one copy must survive the dedup guard.
    let racing_context = context.clone();
    projection.set_on_first_read(Box::new(move || {
        let _ = racing_context.publish_recorded_event("prod", "wf-boundary", event(6));
    }));

    let subscription = context.subscribe_history("prod", "wf-boundary", Some(5))?;
    let delivered = drain(&subscription)?;

    assert_eq!(delivered, vec![6]);
    assert_eq!(
        delivered.iter().filter(|&&sequence| sequence == 6).count(),
        1,
        "boundary event 6 must be delivered exactly once"
    );
    Ok(())
}

fn drain(subscription: &HistorySubscription) -> Result<Vec<u64>, AionSurfaceError> {
    let mut sequences = Vec::new();
    while let Some(event) = subscription.try_next()? {
        sequences.push(event.sequence);
    }
    Ok(sequences)
}

fn event(sequence: u64) -> HistoryEvent {
    let byte = u8::try_from(sequence).unwrap_or(0);
    HistoryEvent {
        sequence,
        event_type: "WorkflowTaskCompleted".to_owned(),
        timestamp: UNIX_EPOCH + Duration::from_millis(sequence),
        payload: Payload {
            data: vec![byte],
            content_type: "application/json".to_owned(),
        },
    }
}

#[derive(Debug, Default)]
struct CollectingReporter {
    failures: Mutex<Vec<HistoryPublishFailure>>,
}

impl HistoryPublishReporter for CollectingReporter {
    fn publish_failed(&self, failure: &HistoryPublishFailure) {
        if let Ok(mut failures) = self.failures.lock() {
            failures.push(failure.clone());
        }
    }
}

impl CollectingReporter {
    fn failures(&self) -> Result<Vec<HistoryPublishFailure>, Box<dyn Error>> {
        Ok(self
            .failures
            .lock()
            .map_err(|_| "reporter lock poisoned")?
            .clone())
    }
}

#[derive(Debug, Default)]
struct TrackingProjection {
    inner: InMemoryHistoryProjection,
    steps: Mutex<Vec<&'static str>>,
}

impl TrackingProjection {
    fn record(&self, step: &'static str) -> Result<(), AionSurfaceError> {
        let channel_name = history_channel("prod", "wf-2")?;
        self.steps
            .lock()
            .map_err(|error| streaming_failed(&channel_name, "wf-2", error))?
            .push(step);
        Ok(())
    }

    fn steps(&self) -> Result<Vec<&'static str>, Box<dyn Error>> {
        Ok(self
            .steps
            .lock()
            .map_err(|_| "steps lock poisoned")?
            .clone())
    }
}

impl HistoryProjection for TrackingProjection {
    fn ensure_channel(&self, channel_name: &str) -> Result<(), HistoryProjectionError> {
        self.inner.ensure_channel(channel_name)
    }

    fn append(
        &self,
        channel_name: &str,
        event: HistoryEvent,
    ) -> Result<(), HistoryProjectionError> {
        self.record("projection")
            .map_err(|error| HistoryProjectionError::new(error.to_string()))?;
        self.inner.append(channel_name, event)
    }

    fn read_after(
        &self,
        channel_name: &str,
        cursor: u64,
    ) -> Result<Vec<HistoryEvent>, HistoryProjectionError> {
        self.inner.read_after(channel_name, cursor)
    }
}

type ReadHook = Box<dyn FnMut() + Send>;

/// Projection that runs a one-shot hook the first time `read_after` is called, letting a test
/// publish an event between the live subscribe and the replay snapshot inside `subscribe_history`.
#[derive(Default)]
struct RaceProjection {
    inner: InMemoryHistoryProjection,
    on_read: Mutex<Option<ReadHook>>,
}

impl RaceProjection {
    fn set_on_first_read(&self, hook: ReadHook) {
        if let Ok(mut guard) = self.on_read.lock() {
            *guard = Some(hook);
        }
    }
}

impl std::fmt::Debug for RaceProjection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RaceProjection")
            .finish_non_exhaustive()
    }
}

impl HistoryProjection for RaceProjection {
    fn ensure_channel(&self, channel_name: &str) -> Result<(), HistoryProjectionError> {
        self.inner.ensure_channel(channel_name)
    }

    fn append(
        &self,
        channel_name: &str,
        event: HistoryEvent,
    ) -> Result<(), HistoryProjectionError> {
        self.inner.append(channel_name, event)
    }

    fn read_after(
        &self,
        channel_name: &str,
        cursor: u64,
    ) -> Result<Vec<HistoryEvent>, HistoryProjectionError> {
        let hook = self.on_read.lock().ok().and_then(|mut guard| guard.take());
        if let Some(mut hook) = hook {
            hook();
        }
        self.inner.read_after(channel_name, cursor)
    }
}

#[derive(Debug)]
struct FailingProjection;

impl HistoryProjection for FailingProjection {
    fn ensure_channel(&self, channel_name: &str) -> Result<(), HistoryProjectionError> {
        std::hint::black_box(channel_name);
        Ok(())
    }

    fn append(
        &self,
        channel_name: &str,
        event: HistoryEvent,
    ) -> Result<(), HistoryProjectionError> {
        std::hint::black_box(channel_name);
        std::hint::black_box(event);
        Err(HistoryProjectionError::new("forced projection failure"))
    }

    fn read_after(
        &self,
        channel_name: &str,
        cursor: u64,
    ) -> Result<Vec<HistoryEvent>, HistoryProjectionError> {
        std::hint::black_box(channel_name);
        std::hint::black_box(cursor);
        Ok(Vec::new())
    }
}
