use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use super::{HistogramBucketSnapshot, HistogramSnapshot};

#[derive(Debug)]
pub(super) struct CounterMetric {
    value: AtomicU64,
}

impl CounterMetric {
    pub(super) const fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
        }
    }

    pub(super) fn snapshot(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub(super) struct GaugeMetric {
    value: AtomicI64,
}

impl GaugeMetric {
    pub(super) const fn new() -> Self {
        Self {
            value: AtomicI64::new(0),
        }
    }

    pub(super) fn snapshot(&self) -> i64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub(super) struct HistogramMetric {
    boundaries: Vec<u64>,
    counts: Vec<AtomicU64>,
}

impl HistogramMetric {
    pub(super) fn new(boundaries: Vec<u64>) -> Self {
        let counts_len = boundaries.len().saturating_add(1);
        let counts = (0..counts_len).map(|_| AtomicU64::new(0)).collect();

        Self { boundaries, counts }
    }

    pub(super) fn boundaries(&self) -> &[u64] {
        &self.boundaries
    }

    pub(super) fn snapshot(&self) -> HistogramSnapshot {
        let buckets = self
            .counts
            .iter()
            .enumerate()
            .map(|(index, count)| HistogramBucketSnapshot {
                upper_bound: self.boundaries.get(index).copied(),
                count: count.load(Ordering::Relaxed),
            })
            .collect();

        HistogramSnapshot { buckets }
    }

    fn bucket_index(&self, value: u64) -> usize {
        for (index, boundary) in self.boundaries.iter().enumerate() {
            if value <= *boundary {
                return index;
            }
        }

        self.boundaries.len()
    }
}

#[derive(Clone, Debug)]
pub struct CounterHandle {
    pub(super) metric: Arc<CounterMetric>,
}

impl CounterHandle {
    #[must_use]
    pub(super) fn noop() -> Self {
        let metric = Arc::new(CounterMetric::new());
        Self { metric }
    }

    pub fn increment(&self) {
        self.increment_by(1);
    }

    pub fn increment_by(&self, amount: u64) {
        let _ = self
            .metric
            .value
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(amount))
            });
    }

    #[must_use]
    pub fn value(&self) -> u64 {
        self.metric.value.load(Ordering::Relaxed)
    }
}

#[derive(Clone, Debug)]
pub struct GaugeHandle {
    pub(super) metric: Arc<GaugeMetric>,
}

impl GaugeHandle {
    #[must_use]
    pub(super) fn noop() -> Self {
        let metric = Arc::new(GaugeMetric::new());
        Self { metric }
    }

    pub fn set(&self, value: i64) {
        self.metric.value.store(value, Ordering::Relaxed);
    }

    pub fn increment(&self) {
        self.increment_by(1);
    }

    pub fn increment_by(&self, amount: i64) {
        self.metric.value.fetch_add(amount, Ordering::Relaxed);
    }

    pub fn decrement(&self) {
        self.decrement_by(1);
    }

    pub fn decrement_by(&self, amount: i64) {
        self.metric.value.fetch_sub(amount, Ordering::Relaxed);
    }

    #[must_use]
    pub fn value(&self) -> i64 {
        self.metric.value.load(Ordering::Relaxed)
    }
}

#[derive(Clone, Debug)]
pub struct HistogramHandle {
    pub(super) metric: Arc<HistogramMetric>,
}

impl HistogramHandle {
    #[must_use]
    pub(super) fn noop(boundaries: Vec<u64>) -> Self {
        let metric = Arc::new(HistogramMetric::new(boundaries));
        Self { metric }
    }

    pub fn observe(&self, value: u64) {
        let bucket_index = self.metric.bucket_index(value);
        if let Some(count) = self.metric.counts.get(bucket_index) {
            count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[must_use]
    pub fn boundaries(&self) -> &[u64] {
        self.metric.boundaries()
    }
}
