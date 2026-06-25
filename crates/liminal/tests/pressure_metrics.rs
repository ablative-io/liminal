use liminal::metrics::{
    DeliveryOutcome, MetricKind, MetricSnapshot, MetricValue, MetricsRegistry, PressureMetrics,
};

#[test]
fn pressure_metrics_is_clone_debug() -> Result<(), Box<dyn std::error::Error>> {
    fn assert_clone_debug<T: Clone + std::fmt::Debug>() {}

    assert_clone_debug::<PressureMetrics>();

    let registry = MetricsRegistry::new();
    let metrics = PressureMetrics::new(&registry)?;
    let cloned = metrics.clone();
    metrics.record_reject("orders");

    assert!(!format!("{cloned:?}").is_empty());

    Ok(())
}

#[test]
fn delivery_outcome_has_exactly_three_variants() {
    // Exhaustive match: adding or removing a variant breaks compilation here.
    for outcome in [
        DeliveryOutcome::Accepted,
        DeliveryOutcome::Deferred,
        DeliveryOutcome::Rejected,
    ] {
        match outcome {
            DeliveryOutcome::Accepted | DeliveryOutcome::Deferred | DeliveryOutcome::Rejected => {}
        }
    }
}

#[test]
fn record_reject_counts_per_channel() -> Result<(), Box<dyn std::error::Error>> {
    let registry = MetricsRegistry::new();
    let metrics = PressureMetrics::new(&registry)?;

    for _ in 0..5 {
        metrics.record_reject("orders");
    }
    metrics.record_reject("payments");

    assert_metric(
        &registry,
        "pressure_reject_count",
        "channel",
        "orders",
        MetricKind::Counter,
        |value| value == &MetricValue::Counter(5),
    );
    assert_metric(
        &registry,
        "pressure_reject_count",
        "channel",
        "payments",
        MetricKind::Counter,
        |value| value == &MetricValue::Counter(1),
    );

    Ok(())
}

#[test]
fn record_defer_counts_per_channel() -> Result<(), Box<dyn std::error::Error>> {
    let registry = MetricsRegistry::new();
    let metrics = PressureMetrics::new(&registry)?;

    for _ in 0..8 {
        metrics.record_defer("orders");
    }

    assert_metric(
        &registry,
        "pressure_defer_count",
        "channel",
        "orders",
        MetricKind::Counter,
        |value| value == &MetricValue::Counter(8),
    );

    Ok(())
}

#[test]
fn set_capacity_utilization_sets_gauge_per_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let registry = MetricsRegistry::new();
    let metrics = PressureMetrics::new(&registry)?;

    metrics.set_capacity_utilization("worker-a", 75);
    assert_metric(
        &registry,
        "pressure_capacity_utilization",
        "consumer",
        "worker-a",
        MetricKind::Gauge,
        |value| value == &MetricValue::Gauge(75),
    );

    metrics.set_capacity_utilization("worker-a", 0);
    assert_metric(
        &registry,
        "pressure_capacity_utilization",
        "consumer",
        "worker-a",
        MetricKind::Gauge,
        |value| value == &MetricValue::Gauge(0),
    );

    Ok(())
}

#[test]
fn set_capacity_utilization_does_not_clamp() -> Result<(), Box<dyn std::error::Error>> {
    let registry = MetricsRegistry::new();
    let metrics = PressureMetrics::new(&registry)?;

    metrics.set_capacity_utilization("worker-b", 150);
    assert_metric(
        &registry,
        "pressure_capacity_utilization",
        "consumer",
        "worker-b",
        MetricKind::Gauge,
        |value| value == &MetricValue::Gauge(150),
    );

    Ok(())
}

#[test]
fn record_delivery_outcome_accepted_is_noop() -> Result<(), Box<dyn std::error::Error>> {
    let registry = MetricsRegistry::new();
    let metrics = PressureMetrics::new(&registry)?;

    metrics.record_delivery_outcome("orders", DeliveryOutcome::Accepted);

    // Accepted touches no counter, so no per-channel handle is materialised.
    assert_metric_absent(&registry, "pressure_reject_count", "channel", "orders");
    assert_metric_absent(&registry, "pressure_defer_count", "channel", "orders");

    Ok(())
}

#[test]
fn record_delivery_outcome_deferred_increments_defer() -> Result<(), Box<dyn std::error::Error>> {
    let registry = MetricsRegistry::new();
    let metrics = PressureMetrics::new(&registry)?;

    metrics.record_delivery_outcome("orders", DeliveryOutcome::Deferred);

    assert_metric(
        &registry,
        "pressure_defer_count",
        "channel",
        "orders",
        MetricKind::Counter,
        |value| value == &MetricValue::Counter(1),
    );
    assert_metric_absent(&registry, "pressure_reject_count", "channel", "orders");

    Ok(())
}

#[test]
fn record_delivery_outcome_rejected_increments_reject() -> Result<(), Box<dyn std::error::Error>> {
    let registry = MetricsRegistry::new();
    let metrics = PressureMetrics::new(&registry)?;

    metrics.record_delivery_outcome("orders", DeliveryOutcome::Rejected);

    assert_metric(
        &registry,
        "pressure_reject_count",
        "channel",
        "orders",
        MetricKind::Counter,
        |value| value == &MetricValue::Counter(1),
    );
    assert_metric_absent(&registry, "pressure_defer_count", "channel", "orders");

    Ok(())
}

fn assert_metric(
    registry: &MetricsRegistry,
    name: &str,
    label_name: &str,
    label_value: &str,
    kind: MetricKind,
    value_matches: impl Fn(&MetricValue) -> bool,
) {
    assert!(registry.snapshot().metrics().iter().any(|metric| {
        metric.name == name
            && has_label(metric, label_name, label_value)
            && metric.kind == kind
            && value_matches(&metric.value)
    }));
}

fn assert_metric_absent(
    registry: &MetricsRegistry,
    name: &str,
    label_name: &str,
    label_value: &str,
) {
    assert!(
        !registry
            .snapshot()
            .metrics()
            .iter()
            .any(|metric| { metric.name == name && has_label(metric, label_name, label_value) })
    );
}

fn has_label(metric: &MetricSnapshot, label_name: &str, label_value: &str) -> bool {
    metric
        .labels
        .iter()
        .any(|(name, value)| name == label_name && value == label_value)
}
