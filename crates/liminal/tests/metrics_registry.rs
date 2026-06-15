use liminal::MetricsRegistry;
use liminal::metrics::{MetricKind, MetricValue, MetricsSnapshot};

#[test]
fn metrics_registry_is_clone_send_sync() {
    fn assert_clone_send_sync<T: Clone + Send + Sync>() {}

    assert_clone_send_sync::<MetricsRegistry>();
}

#[test]
fn metrics_snapshot_is_owned_static_debug() {
    fn assert_debug_static<T: std::fmt::Debug + 'static>() {}

    assert_debug_static::<MetricsSnapshot>();
}

#[test]
fn counter_saturates_instead_of_wrapping() -> Result<(), Box<dyn std::error::Error>> {
    let registry = MetricsRegistry::new();
    let counter =
        registry.register_counter("messages_total", std::iter::empty::<(&str, &str)>())?;

    counter.increment_by(u64::MAX);
    counter.increment();

    assert_eq!(counter.value(), u64::MAX);

    Ok(())
}

#[test]
fn snapshot_tolerates_concurrent_writes() -> Result<(), Box<dyn std::error::Error>> {
    let registry = MetricsRegistry::new();
    let counter = registry.register_counter("concurrent_messages", [("channel", "alpha")])?;
    let gauge = registry.register_gauge("concurrent_subscribers", [("channel", "alpha")])?;
    let histogram =
        registry.register_histogram("concurrent_latency", [("channel", "alpha")], vec![10, 100])?;

    let writers = (0..4)
        .map(|_| {
            let counter = counter.clone();
            let gauge = gauge.clone();
            let histogram = histogram.clone();
            std::thread::spawn(move || {
                for value in 0..1_000 {
                    counter.increment();
                    gauge.increment();
                    if value % 2 == 0 {
                        gauge.decrement();
                    }
                    histogram.observe(value);
                }
            })
        })
        .collect::<Vec<_>>();

    for _ in 0..100 {
        assert_eq!(registry.snapshot().metrics().len(), 3);
    }

    for writer in writers {
        assert!(writer.join().is_ok());
    }
    assert_eq!(registry.snapshot().metrics().len(), 3);

    Ok(())
}

#[test]
fn duplicate_name_with_different_kind_returns_error() -> Result<(), Box<dyn std::error::Error>> {
    let registry = MetricsRegistry::new();
    let counter = registry.register_counter("delivery_events", [("channel", "alpha")])?;

    assert_eq!(counter.value(), 0);
    assert!(
        registry
            .register_gauge("delivery_events", [("channel", "alpha")])
            .is_err()
    );

    Ok(())
}

#[test]
fn snapshot_contains_owned_metric_values() -> Result<(), Box<dyn std::error::Error>> {
    let registry = MetricsRegistry::new();
    let counter = registry.register_counter("messages_total", [("channel", "alpha")])?;
    let gauge = registry.register_gauge("subscribers", [("channel", "alpha")])?;
    let histogram =
        registry.register_histogram("latency_ms", [("channel", "alpha")], vec![10, 100])?;

    counter.increment_by(3);
    gauge.set(2);
    histogram.observe(42);

    let snapshot = registry.snapshot();

    assert_eq!(snapshot.metrics().len(), 3);
    assert!(snapshot.metrics().iter().any(|metric| {
        metric.name == "messages_total"
            && metric.kind == MetricKind::Counter
            && metric.value == MetricValue::Counter(3)
    }));
    assert!(snapshot.metrics().iter().any(|metric| {
        metric.name == "subscribers"
            && metric.kind == MetricKind::Gauge
            && metric.value == MetricValue::Gauge(2)
    }));
    assert!(snapshot.metrics().iter().any(|metric| {
        metric.name == "latency_ms"
            && metric.kind == MetricKind::Histogram
            && matches!(&metric.value, MetricValue::Histogram(histogram) if histogram.buckets[1].count == 1)
    }));

    Ok(())
}
