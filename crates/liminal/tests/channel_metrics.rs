use std::time::Duration;

use liminal::metrics::{ChannelMetrics, MetricKind, MetricSnapshot, MetricValue, MetricsRegistry};

#[test]
fn channel_metrics_is_clone_debug() -> Result<(), Box<dyn std::error::Error>> {
    fn assert_clone_debug<T: Clone + std::fmt::Debug>() {}

    assert_clone_debug::<ChannelMetrics>();

    let registry = MetricsRegistry::new();
    let metrics = ChannelMetrics::new(&registry, vec![10, 100])?;
    let cloned = metrics.clone();
    metrics.record_publish("orders");

    assert!(!format!("{cloned:?}").is_empty());

    Ok(())
}

#[test]
fn record_publish_counts_per_channel() -> Result<(), Box<dyn std::error::Error>> {
    let registry = MetricsRegistry::new();
    let metrics = ChannelMetrics::new(&registry, vec![10, 100])?;

    for _ in 0..100 {
        metrics.record_publish("orders");
    }
    metrics.record_publish("payments");

    assert_metric(
        &registry,
        "channel_message_rate",
        "orders",
        MetricKind::Counter,
        |value| value == &MetricValue::Counter(100),
    );
    assert_metric(
        &registry,
        "channel_message_rate",
        "payments",
        MetricKind::Counter,
        |value| value == &MetricValue::Counter(1),
    );

    Ok(())
}

#[test]
fn subscriber_count_tracks_active_subscribers() -> Result<(), Box<dyn std::error::Error>> {
    let registry = MetricsRegistry::new();
    let metrics = ChannelMetrics::new(&registry, vec![10, 100])?;

    metrics.record_subscribe("orders");
    metrics.record_subscribe("orders");
    metrics.record_subscribe("orders");
    metrics.record_unsubscribe("orders");

    assert_metric(
        &registry,
        "channel_subscriber_count",
        "orders",
        MetricKind::Gauge,
        |value| value == &MetricValue::Gauge(2),
    );

    Ok(())
}

#[test]
fn queue_depth_sets_absolute_value() -> Result<(), Box<dyn std::error::Error>> {
    let registry = MetricsRegistry::new();
    let metrics = ChannelMetrics::new(&registry, vec![10, 100])?;

    metrics.set_queue_depth("orders", 42);
    assert_metric(
        &registry,
        "channel_queue_depth",
        "orders",
        MetricKind::Gauge,
        |value| value == &MetricValue::Gauge(42),
    );

    metrics.set_queue_depth("orders", 0);
    assert_metric(
        &registry,
        "channel_queue_depth",
        "orders",
        MetricKind::Gauge,
        |value| value == &MetricValue::Gauge(0),
    );

    Ok(())
}

#[test]
fn delivery_latency_records_configured_histogram() -> Result<(), Box<dyn std::error::Error>> {
    let registry = MetricsRegistry::new();
    let metrics = ChannelMetrics::new(&registry, vec![100, 10])?;

    metrics.record_delivery_latency("orders", Duration::from_millis(50));

    assert_metric(
        &registry,
        "channel_delivery_latency",
        "orders",
        MetricKind::Histogram,
        |value| {
            matches!(value, MetricValue::Histogram(histogram)
                if histogram.buckets.len() == 3
                    && histogram.buckets[0].upper_bound == Some(10.0)
                    && histogram.buckets[0].count == 0
                    && histogram.buckets[1].upper_bound == Some(100.0)
                    && histogram.buckets[1].count == 1
                    && histogram.buckets[2].upper_bound.is_none()
                    && histogram.buckets[2].count == 0)
        },
    );

    Ok(())
}

fn assert_metric(
    registry: &MetricsRegistry,
    name: &str,
    channel: &str,
    kind: MetricKind,
    value_matches: impl Fn(&MetricValue) -> bool,
) {
    assert!(registry.snapshot().metrics().iter().any(|metric| {
        metric.name == name
            && has_channel_label(metric, channel)
            && metric.kind == kind
            && value_matches(&metric.value)
    }));
}

fn has_channel_label(metric: &MetricSnapshot, channel: &str) -> bool {
    metric
        .labels
        .iter()
        .any(|(name, value)| name == "channel" && value == channel)
}
