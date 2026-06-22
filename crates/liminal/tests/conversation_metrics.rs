use std::time::Duration;

use liminal::metrics::{
    ConversationMetrics, ConversationOutcome, MetricKind, MetricValue, MetricsRegistry,
};

#[test]
fn conversation_metrics_is_clone_debug() -> Result<(), Box<dyn std::error::Error>> {
    fn assert_clone_debug<T: Clone + std::fmt::Debug>() {}

    assert_clone_debug::<ConversationMetrics>();

    let registry = MetricsRegistry::new();
    let metrics = ConversationMetrics::new(&registry, vec![1.0, 10.0])?;
    let cloned = metrics.clone();
    metrics.record_start();

    assert!(!format!("{cloned:?}").is_empty());

    Ok(())
}

#[test]
fn active_count_tracks_open_conversations() -> Result<(), Box<dyn std::error::Error>> {
    let registry = MetricsRegistry::new();
    let metrics = ConversationMetrics::new(&registry, vec![1.0, 10.0])?;

    for _ in 0..5 {
        metrics.record_start();
    }
    for _ in 0..2 {
        metrics.record_end();
    }

    assert_eq!(metrics.active_count.value(), 3);
    assert_global_metric(
        &registry,
        "conversation_active_count",
        MetricKind::Gauge,
        |value| value == &MetricValue::Gauge(3),
    );

    Ok(())
}

#[test]
fn completion_count_tracks_successful_terminal_events() -> Result<(), Box<dyn std::error::Error>> {
    let registry = MetricsRegistry::new();
    let metrics = ConversationMetrics::new(&registry, vec![1.0, 10.0])?;

    for _ in 0..10 {
        metrics.record_completion();
    }

    assert_eq!(metrics.completion_count.value(), 10);
    assert_global_metric(
        &registry,
        "conversation_completion_count",
        MetricKind::Counter,
        |value| value == &MetricValue::Counter(10),
    );

    Ok(())
}

#[test]
fn duration_records_seconds_into_configured_histogram() -> Result<(), Box<dyn std::error::Error>> {
    let registry = MetricsRegistry::new();
    let metrics = ConversationMetrics::new(&registry, vec![10.0, 1.0, 5.0])?;

    metrics.record_duration(Duration::from_secs(5));

    assert_global_metric(
        &registry,
        "conversation_duration",
        MetricKind::Histogram,
        |value| {
            matches!(value, MetricValue::Histogram(histogram)
                if histogram.buckets.len() == 4
                    && histogram.buckets[0].upper_bound == Some(1.0)
                    && histogram.buckets[0].count == 0
                    && histogram.buckets[1].upper_bound == Some(5.0)
                    && histogram.buckets[1].count == 1
                    && histogram.buckets[2].upper_bound == Some(10.0)
                    && histogram.buckets[2].count == 0
                    && histogram.buckets[3].upper_bound.is_none()
                    && histogram.buckets[3].count == 0)
        },
    );

    Ok(())
}

#[test]
fn error_count_tracks_failed_terminal_events() -> Result<(), Box<dyn std::error::Error>> {
    let registry = MetricsRegistry::new();
    let metrics = ConversationMetrics::new(&registry, vec![1.0, 10.0])?;

    for _ in 0..3 {
        metrics.record_error();
    }

    assert_eq!(metrics.error_count.value(), 3);
    assert_global_metric(
        &registry,
        "conversation_error_count",
        MetricKind::Counter,
        |value| value == &MetricValue::Counter(3),
    );

    Ok(())
}

#[test]
fn terminal_completed_records_completion_duration_and_end() -> Result<(), Box<dyn std::error::Error>>
{
    let registry = MetricsRegistry::new();
    let metrics = ConversationMetrics::new(&registry, vec![1.0, 5.0, 10.0])?;

    metrics.record_start();
    metrics.record_terminal(ConversationOutcome::Completed, Duration::from_secs(5));

    assert_eq!(metrics.active_count.value(), 0);
    assert_eq!(metrics.completion_count.value(), 1);
    assert_eq!(metrics.error_count.value(), 0);
    assert_duration_bucket_count(&registry, 5.0, 1);

    Ok(())
}

#[test]
fn terminal_failures_record_error_duration_and_end() -> Result<(), Box<dyn std::error::Error>> {
    let registry = MetricsRegistry::new();
    let metrics = ConversationMetrics::new(&registry, vec![1.0, 5.0, 10.0])?;

    metrics.record_start();
    metrics.record_terminal(ConversationOutcome::Failed, Duration::from_secs(5));
    metrics.record_start();
    metrics.record_terminal(ConversationOutcome::TimedOut, Duration::from_secs(5));

    assert_eq!(metrics.active_count.value(), 0);
    assert_eq!(metrics.completion_count.value(), 0);
    assert_eq!(metrics.error_count.value(), 2);
    assert_duration_bucket_count(&registry, 5.0, 2);

    Ok(())
}

#[test]
fn conversation_outcome_has_expected_variants() {
    assert_eq!(outcome_index(ConversationOutcome::Completed), 0);
    assert_eq!(outcome_index(ConversationOutcome::Failed), 1);
    assert_eq!(outcome_index(ConversationOutcome::TimedOut), 2);
}

const fn outcome_index(outcome: ConversationOutcome) -> u8 {
    match outcome {
        ConversationOutcome::Completed => 0,
        ConversationOutcome::Failed => 1,
        ConversationOutcome::TimedOut => 2,
    }
}

fn assert_duration_bucket_count(registry: &MetricsRegistry, upper_bound: f64, count: u64) {
    assert_global_metric(
        registry,
        "conversation_duration",
        MetricKind::Histogram,
        |value| {
            matches!(value, MetricValue::Histogram(histogram)
                if histogram
                    .buckets
                    .iter()
                    .any(|bucket| bucket.upper_bound == Some(upper_bound) && bucket.count == count))
        },
    );
}

fn assert_global_metric(
    registry: &MetricsRegistry,
    name: &str,
    kind: MetricKind,
    value_matches: impl Fn(&MetricValue) -> bool,
) {
    assert!(registry.snapshot().metrics().iter().any(|metric| {
        metric.name == name
            && metric.labels.is_empty()
            && metric.kind == kind
            && value_matches(&metric.value)
    }));
}
