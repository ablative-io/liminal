#![allow(clippy::module_name_repetitions)]

use std::{collections::BTreeMap, fmt, sync::Arc};

type ScoringFn = dyn Fn(&[ConsumerPressureMetrics]) -> f64 + Send + Sync;

/// Per-consumer pressure counters tracked by the bus.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConsumerPressureMetrics {
    /// Messages currently in flight for the consumer.
    pub current_in_flight: usize,
    /// Maximum messages the consumer declared it can process concurrently.
    pub max_in_flight: usize,
    /// Messages currently buffered for the consumer.
    pub buffer_depth: usize,
    /// Number of accept decisions recorded for the consumer.
    pub accept_count: usize,
    /// Number of defer decisions recorded for the consumer.
    pub defer_count: usize,
    /// Number of reject decisions recorded for the consumer.
    pub reject_count: usize,
}

impl ConsumerPressureMetrics {
    /// Creates a metric snapshot with zeroed accept, defer, and reject counts.
    #[must_use]
    pub const fn new(current_in_flight: usize, max_in_flight: usize, buffer_depth: usize) -> Self {
        Self {
            current_in_flight,
            max_in_flight,
            buffer_depth,
            accept_count: 0,
            defer_count: 0,
            reject_count: 0,
        }
    }

    /// Returns the consumer's in-flight utilization clamped to `[0.0, 1.0]`.
    #[must_use]
    pub fn utilization(&self) -> f64 {
        if self.max_in_flight == 0 {
            0.0
        } else {
            clamp_pressure(usize_to_f64(self.current_in_flight) / usize_to_f64(self.max_in_flight))
        }
    }

    /// Records one accepted delivery decision for this consumer.
    pub const fn record_accept(&mut self) {
        self.accept_count = self.accept_count.saturating_add(1);
    }

    /// Records one deferred delivery decision for this consumer.
    pub const fn record_defer(&mut self) {
        self.defer_count = self.defer_count.saturating_add(1);
    }

    /// Records one rejected delivery decision for this consumer.
    pub const fn record_reject(&mut self) {
        self.reject_count = self.reject_count.saturating_add(1);
    }
}

/// Observable pressure snapshot for one tracked consumer.
#[derive(Clone, Debug, PartialEq)]
pub struct ConsumerPressureSnapshot {
    /// Consumer identifier supplied by the routing or dispatch subsystem.
    pub consumer_id: String,
    /// Latest metrics recorded for the consumer.
    pub metrics: ConsumerPressureMetrics,
    /// Current in-flight utilization for the consumer.
    pub utilization: f64,
}

/// Observable pressure snapshot for a channel and all tracked consumers.
#[derive(Clone, Debug, PartialEq)]
pub struct ChannelPressureSnapshot {
    /// Channel identifier whose pressure was measured.
    pub channel_id: String,
    /// Configurable aggregate pressure score clamped to `[0.0, 1.0]`.
    pub pressure_score: f64,
    /// Per-consumer metric snapshots contributing to the pressure score.
    pub consumers: Vec<ConsumerPressureSnapshot>,
}

impl ChannelPressureSnapshot {
    /// Returns the number of consumers currently tracked for the channel.
    #[must_use]
    pub fn consumer_count(&self) -> usize {
        self.consumers.len()
    }
}

/// Tracks pressure metrics and derives channel pressure scores.
pub struct PressureMonitor {
    channels: BTreeMap<String, BTreeMap<String, ConsumerPressureMetrics>>,
    scoring: Arc<ScoringFn>,
}

impl fmt::Debug for PressureMonitor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PressureMonitor")
            .field("channels", &self.channels)
            .finish_non_exhaustive()
    }
}

impl Default for PressureMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl PressureMonitor {
    /// Creates an empty pressure monitor with the default average-utilization scorer.
    #[must_use]
    pub fn new() -> Self {
        Self::with_scoring(default_channel_score)
    }

    /// Creates an empty pressure monitor using a caller-supplied scoring function.
    #[must_use]
    pub fn with_scoring<F>(scoring: F) -> Self
    where
        F: Fn(&[ConsumerPressureMetrics]) -> f64 + Send + Sync + 'static,
    {
        Self {
            channels: BTreeMap::new(),
            scoring: Arc::new(scoring),
        }
    }

    /// Returns true when no consumers are tracked on any channel.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.channels.values().all(BTreeMap::is_empty)
    }

    /// Returns the number of consumers tracked across all channels.
    #[must_use]
    pub fn total_consumer_count(&self) -> usize {
        self.channels.values().map(BTreeMap::len).sum()
    }

    /// Records the latest metrics for a consumer and returns the updated channel snapshot.
    pub fn record_consumer_metrics(
        &mut self,
        channel_id: impl Into<String>,
        consumer_id: impl Into<String>,
        metrics: ConsumerPressureMetrics,
    ) -> ChannelPressureSnapshot {
        let channel_id = channel_id.into();
        let consumer_id = consumer_id.into();
        self.channels
            .entry(channel_id.clone())
            .or_default()
            .insert(consumer_id, metrics);
        self.channel_snapshot(&channel_id)
    }

    /// Records one accept decision for a consumer and returns the updated channel snapshot.
    pub fn record_accept(
        &mut self,
        channel_id: impl Into<String>,
        consumer_id: impl Into<String>,
    ) -> ChannelPressureSnapshot {
        self.record_signal(
            channel_id,
            consumer_id,
            ConsumerPressureMetrics::record_accept,
        )
    }

    /// Records one defer decision for a consumer and returns the updated channel snapshot.
    pub fn record_defer(
        &mut self,
        channel_id: impl Into<String>,
        consumer_id: impl Into<String>,
    ) -> ChannelPressureSnapshot {
        self.record_signal(
            channel_id,
            consumer_id,
            ConsumerPressureMetrics::record_defer,
        )
    }

    /// Records one reject decision for a consumer and returns the updated channel snapshot.
    pub fn record_reject(
        &mut self,
        channel_id: impl Into<String>,
        consumer_id: impl Into<String>,
    ) -> ChannelPressureSnapshot {
        self.record_signal(
            channel_id,
            consumer_id,
            ConsumerPressureMetrics::record_reject,
        )
    }

    /// Returns the latest metrics recorded for one consumer.
    #[must_use]
    pub fn consumer_metrics(
        &self,
        channel_id: &str,
        consumer_id: &str,
    ) -> Option<&ConsumerPressureMetrics> {
        self.channels.get(channel_id)?.get(consumer_id)
    }

    /// Returns the latest utilization for one consumer.
    #[must_use]
    pub fn consumer_utilization(&self, channel_id: &str, consumer_id: &str) -> Option<f64> {
        self.consumer_metrics(channel_id, consumer_id)
            .map(ConsumerPressureMetrics::utilization)
    }

    /// Returns the current aggregate pressure score for a channel.
    #[must_use]
    pub fn channel_pressure(&self, channel_id: &str) -> f64 {
        self.channel_snapshot(channel_id).pressure_score
    }

    /// Returns the current number of tracked consumers for a channel.
    #[must_use]
    pub fn channel_consumer_count(&self, channel_id: &str) -> usize {
        self.channels.get(channel_id).map_or(0, BTreeMap::len)
    }

    /// Returns an observable snapshot for one channel.
    #[must_use]
    pub fn channel_snapshot(&self, channel_id: &str) -> ChannelPressureSnapshot {
        let consumers = self.consumer_snapshots(channel_id);
        let metrics = consumers
            .iter()
            .map(|consumer| consumer.metrics.clone())
            .collect::<Vec<_>>();
        let pressure_score = clamp_pressure((self.scoring)(&metrics));
        ChannelPressureSnapshot {
            channel_id: channel_id.to_owned(),
            pressure_score,
            consumers,
        }
    }

    fn record_signal(
        &mut self,
        channel_id: impl Into<String>,
        consumer_id: impl Into<String>,
        record: fn(&mut ConsumerPressureMetrics),
    ) -> ChannelPressureSnapshot {
        let channel_id = channel_id.into();
        let consumer_id = consumer_id.into();
        let metrics = self
            .channels
            .entry(channel_id.clone())
            .or_default()
            .entry(consumer_id)
            .or_default();
        record(metrics);
        self.channel_snapshot(&channel_id)
    }

    fn consumer_snapshots(&self, channel_id: &str) -> Vec<ConsumerPressureSnapshot> {
        self.channels
            .get(channel_id)
            .map_or_else(Vec::new, |consumers| {
                consumers
                    .iter()
                    .map(|(consumer_id, metrics)| ConsumerPressureSnapshot {
                        consumer_id: consumer_id.clone(),
                        metrics: metrics.clone(),
                        utilization: metrics.utilization(),
                    })
                    .collect()
            })
    }
}

fn default_channel_score(metrics: &[ConsumerPressureMetrics]) -> f64 {
    if metrics.is_empty() {
        0.0
    } else {
        let total_utilization = metrics
            .iter()
            .map(ConsumerPressureMetrics::utilization)
            .sum::<f64>();
        total_utilization / usize_to_f64(metrics.len())
    }
}

const fn clamp_pressure(score: f64) -> f64 {
    if score.is_nan() {
        0.0
    } else {
        score.clamp(0.0, 1.0)
    }
}

fn usize_to_f64(value: usize) -> f64 {
    u32::try_from(value).map_or_else(|_| f64::from(u32::MAX), f64::from)
}

#[cfg(test)]
mod tests {
    use super::{ConsumerPressureMetrics, PressureMonitor};

    fn close_to(actual: f64, expected: f64) -> bool {
        (actual - expected).abs() < f64::EPSILON
    }

    #[test]
    fn pressure_monitor_starts_without_tracked_consumers() {
        let monitor = PressureMonitor::new();

        assert!(monitor.is_empty());
        assert_eq!(monitor.total_consumer_count(), 0);
        assert_eq!(monitor.channel_consumer_count("orders"), 0);
    }

    #[test]
    fn consumer_utilization_uses_current_and_max_in_flight() {
        let mut monitor = PressureMonitor::new();

        monitor.record_consumer_metrics(
            "orders",
            "consumer-a",
            ConsumerPressureMetrics::new(8, 10, 0),
        );

        let utilization = monitor.consumer_utilization("orders", "consumer-a");
        assert!(matches!(utilization, Some(score) if close_to(score, 0.8)));
    }

    #[test]
    fn channel_pressure_aggregates_across_consumers() {
        let mut monitor = PressureMonitor::new();

        monitor.record_consumer_metrics(
            "orders",
            "consumer-a",
            ConsumerPressureMetrics::new(8, 10, 2),
        );
        let snapshot = monitor.record_consumer_metrics(
            "orders",
            "consumer-b",
            ConsumerPressureMetrics::new(2, 10, 1),
        );

        assert_eq!(snapshot.consumer_count(), 2);
        assert!(close_to(snapshot.pressure_score, 0.5));
        assert!(close_to(monitor.channel_pressure("orders"), 0.5));
    }

    #[test]
    fn monitor_tracks_accept_defer_and_reject_counts_per_consumer() {
        let mut monitor = PressureMonitor::new();

        monitor.record_accept("orders", "consumer-a");
        monitor.record_defer("orders", "consumer-a");
        monitor.record_reject("orders", "consumer-a");

        let metrics = monitor.consumer_metrics("orders", "consumer-a");
        assert!(matches!(
            metrics,
            Some(ConsumerPressureMetrics {
                accept_count: 1,
                defer_count: 1,
                reject_count: 1,
                ..
            })
        ));
    }

    #[test]
    fn pressure_scores_are_clamped_to_unit_range() {
        let mut high = PressureMonitor::with_scoring(|_| 3.0);
        let mut low = PressureMonitor::with_scoring(|_| -2.0);
        let mut not_a_number = PressureMonitor::with_scoring(|_| f64::NAN);

        high.record_consumer_metrics(
            "orders",
            "consumer-a",
            ConsumerPressureMetrics::new(1, 1, 0),
        );
        low.record_consumer_metrics(
            "orders",
            "consumer-a",
            ConsumerPressureMetrics::new(1, 1, 0),
        );
        not_a_number.record_consumer_metrics(
            "orders",
            "consumer-a",
            ConsumerPressureMetrics::new(1, 1, 0),
        );

        assert!(close_to(high.channel_pressure("orders"), 1.0));
        assert!(close_to(low.channel_pressure("orders"), 0.0));
        assert!(close_to(not_a_number.channel_pressure("orders"), 0.0));
    }

    #[test]
    fn scoring_function_is_configurable() {
        let mut monitor = PressureMonitor::with_scoring(|metrics| {
            if metrics.iter().any(|metric| metric.buffer_depth > 0) {
                0.75
            } else {
                0.25
            }
        });

        monitor.record_consumer_metrics(
            "orders",
            "consumer-a",
            ConsumerPressureMetrics::new(1, 10, 3),
        );

        assert!(close_to(monitor.channel_pressure("orders"), 0.75));
    }
}
