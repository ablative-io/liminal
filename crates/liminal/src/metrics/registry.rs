#![allow(clippy::module_name_repetitions)]

mod collectors;

use std::collections::{BTreeMap, btree_map::Entry};
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub use collectors::{CounterHandle, GaugeHandle, HistogramHandle};

use collectors::{CounterMetric, GaugeMetric, HistogramMetric};

static METRICS_ENABLED: AtomicBool = AtomicBool::new(false);
static GLOBAL_METRICS: OnceLock<MetricsRegistry> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

impl Display for MetricKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Counter => formatter.write_str("counter"),
            Self::Gauge => formatter.write_str("gauge"),
            Self::Histogram => formatter.write_str("histogram"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricRegistrationError {
    IncompatibleMetricKind {
        name: String,
        existing: MetricKind,
        requested: MetricKind,
    },
    IncompatibleHistogramBuckets {
        name: String,
        labels: Vec<(String, String)>,
    },
    GlobalRegistryAlreadyInstalled,
}

impl Display for MetricRegistrationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::IncompatibleMetricKind {
                name,
                existing,
                requested,
            } => write!(
                formatter,
                "metric `{name}` is registered as {existing}, not {requested}"
            ),
            Self::IncompatibleHistogramBuckets { name, labels } => write!(
                formatter,
                "histogram `{name}` with labels {labels:?} is registered with different buckets"
            ),
            Self::GlobalRegistryAlreadyInstalled => {
                formatter.write_str("global metrics registry is already installed")
            }
        }
    }
}

impl Error for MetricRegistrationError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub metrics: Vec<MetricSnapshot>,
}

impl MetricsSnapshot {
    #[must_use]
    pub fn metrics(&self) -> &[MetricSnapshot] {
        &self.metrics
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricSnapshot {
    pub name: String,
    pub labels: Vec<(String, String)>,
    pub kind: MetricKind,
    pub value: MetricValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricValue {
    Counter(u64),
    Gauge(i64),
    Histogram(HistogramSnapshot),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistogramSnapshot {
    pub buckets: Vec<HistogramBucketSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistogramBucketSnapshot {
    pub upper_bound: Option<u64>,
    pub count: u64,
}

#[derive(Clone, Debug)]
pub struct MetricsRegistry {
    inner: Arc<RegistryInner>,
}

impl MetricsRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RegistryInner::new()),
        }
    }

    /// # Errors
    ///
    /// Returns an error if `name` was previously registered with a different kind.
    pub fn register_counter<Labels, Key, Value>(
        &self,
        name: impl Into<String>,
        labels: Labels,
    ) -> Result<CounterHandle, MetricRegistrationError>
    where
        Labels: IntoIterator<Item = (Key, Value)>,
        Key: Into<String>,
        Value: Into<String>,
    {
        let name = name.into();
        let collector = self.register_collector(
            name.clone(),
            normalize_labels(labels),
            MetricKind::Counter,
            || MetricCollector::Counter(Arc::new(CounterMetric::new())),
            |existing| ensure_collector_kind(existing, &name, MetricKind::Counter),
        )?;

        match collector {
            MetricCollector::Counter(metric) => Ok(CounterHandle { metric }),
            other => Err(incompatible_kind(&name, other.kind(), MetricKind::Counter)),
        }
    }

    /// # Errors
    ///
    /// Returns an error if `name` was previously registered with a different kind.
    pub fn register_gauge<Labels, Key, Value>(
        &self,
        name: impl Into<String>,
        labels: Labels,
    ) -> Result<GaugeHandle, MetricRegistrationError>
    where
        Labels: IntoIterator<Item = (Key, Value)>,
        Key: Into<String>,
        Value: Into<String>,
    {
        let name = name.into();
        let collector = self.register_collector(
            name.clone(),
            normalize_labels(labels),
            MetricKind::Gauge,
            || MetricCollector::Gauge(Arc::new(GaugeMetric::new())),
            |existing| ensure_collector_kind(existing, &name, MetricKind::Gauge),
        )?;

        match collector {
            MetricCollector::Gauge(metric) => Ok(GaugeHandle { metric }),
            other => Err(incompatible_kind(&name, other.kind(), MetricKind::Gauge)),
        }
    }

    /// # Errors
    ///
    /// Returns an error if `name` was previously registered with a different
    /// kind, or if the exact histogram was registered with different buckets.
    pub fn register_histogram<Labels, Key, Value>(
        &self,
        name: impl Into<String>,
        labels: Labels,
        buckets: Vec<u64>,
    ) -> Result<HistogramHandle, MetricRegistrationError>
    where
        Labels: IntoIterator<Item = (Key, Value)>,
        Key: Into<String>,
        Value: Into<String>,
    {
        let name = name.into();
        let labels = normalize_labels(labels);
        let bucket_boundaries = normalize_buckets(buckets);
        let labels_for_error = labels.clone();
        let collector = self.register_collector(
            name.clone(),
            labels,
            MetricKind::Histogram,
            || {
                MetricCollector::Histogram(Arc::new(HistogramMetric::new(
                    bucket_boundaries.clone(),
                )))
            },
            |existing| {
                ensure_histogram_buckets(existing, &name, &labels_for_error, &bucket_boundaries)
            },
        )?;

        match collector {
            MetricCollector::Histogram(metric) => Ok(HistogramHandle { metric }),
            other => Err(incompatible_kind(
                &name,
                other.kind(),
                MetricKind::Histogram,
            )),
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        let metrics = {
            let state = read_registry_state(&self.inner.state);
            state.metrics.values().map(MetricEntry::snapshot).collect()
        };
        MetricsSnapshot { metrics }
    }

    fn register_collector(
        &self,
        name: String,
        labels: Vec<(String, String)>,
        kind: MetricKind,
        create: impl FnOnce() -> MetricCollector,
        validate_existing: impl FnOnce(&MetricCollector) -> Result<(), MetricRegistrationError>,
    ) -> Result<MetricCollector, MetricRegistrationError> {
        let mut state = write_registry_state(&self.inner.state);
        ensure_name_kind(&mut state, &name, kind)?;
        let key = MetricKey::new(name.clone(), labels.clone());

        match state.metrics.entry(key) {
            Entry::Occupied(entry) => {
                let collector = entry.get().collector.clone();
                validate_existing(&collector)?;
                Ok(collector)
            }
            Entry::Vacant(entry) => {
                let collector = create();
                entry.insert(MetricEntry::new(name, labels, kind, collector.clone()));
                Ok(collector)
            }
        }
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// # Errors
///
/// Returns an error when a global registry has already been installed.
pub fn install_global_registry(registry: MetricsRegistry) -> Result<(), MetricRegistrationError> {
    match GLOBAL_METRICS.set(registry) {
        Ok(()) => {
            METRICS_ENABLED.store(true, Ordering::Release);
            Ok(())
        }
        Err(_registry) => Err(MetricRegistrationError::GlobalRegistryAlreadyInstalled),
    }
}

#[must_use]
pub fn metrics_enabled() -> bool {
    METRICS_ENABLED.load(Ordering::Acquire)
}

#[must_use]
pub fn global_registry() -> Option<&'static MetricsRegistry> {
    if metrics_enabled() {
        GLOBAL_METRICS.get()
    } else {
        None
    }
}

#[derive(Debug)]
struct RegistryInner {
    state: RwLock<RegistryState>,
}

impl RegistryInner {
    fn new() -> Self {
        Self {
            state: RwLock::new(RegistryState::default()),
        }
    }
}

#[derive(Debug, Default)]
struct RegistryState {
    kind_by_name: BTreeMap<String, MetricKind>,
    metrics: BTreeMap<MetricKey, MetricEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MetricKey {
    name: String,
    labels: Vec<(String, String)>,
}

impl MetricKey {
    const fn new(name: String, labels: Vec<(String, String)>) -> Self {
        Self { name, labels }
    }
}

#[derive(Debug)]
struct MetricEntry {
    name: String,
    labels: Vec<(String, String)>,
    kind: MetricKind,
    collector: MetricCollector,
}

impl MetricEntry {
    const fn new(
        name: String,
        labels: Vec<(String, String)>,
        kind: MetricKind,
        collector: MetricCollector,
    ) -> Self {
        Self {
            name,
            labels,
            kind,
            collector,
        }
    }

    fn snapshot(&self) -> MetricSnapshot {
        MetricSnapshot {
            name: self.name.clone(),
            labels: self.labels.clone(),
            kind: self.kind,
            value: self.collector.snapshot(),
        }
    }
}

#[derive(Debug, Clone)]
enum MetricCollector {
    Counter(Arc<CounterMetric>),
    Gauge(Arc<GaugeMetric>),
    Histogram(Arc<HistogramMetric>),
}

impl MetricCollector {
    const fn kind(&self) -> MetricKind {
        match self {
            Self::Counter(_) => MetricKind::Counter,
            Self::Gauge(_) => MetricKind::Gauge,
            Self::Histogram(_) => MetricKind::Histogram,
        }
    }

    fn snapshot(&self) -> MetricValue {
        match self {
            Self::Counter(metric) => MetricValue::Counter(metric.snapshot()),
            Self::Gauge(metric) => MetricValue::Gauge(metric.snapshot()),
            Self::Histogram(metric) => MetricValue::Histogram(metric.snapshot()),
        }
    }
}

fn normalize_labels<Labels, Key, Value>(labels: Labels) -> Vec<(String, String)>
where
    Labels: IntoIterator<Item = (Key, Value)>,
    Key: Into<String>,
    Value: Into<String>,
{
    let mut labels = labels
        .into_iter()
        .map(|(key, value)| (key.into(), value.into()))
        .collect::<Vec<_>>();
    labels.sort_unstable();
    labels.dedup();
    labels
}

fn normalize_buckets(mut buckets: Vec<u64>) -> Vec<u64> {
    buckets.sort_unstable();
    buckets.dedup();
    buckets
}

fn read_registry_state(lock: &RwLock<RegistryState>) -> RwLockReadGuard<'_, RegistryState> {
    match lock.read() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn write_registry_state(lock: &RwLock<RegistryState>) -> RwLockWriteGuard<'_, RegistryState> {
    match lock.write() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn ensure_name_kind(
    state: &mut RegistryState,
    name: &str,
    kind: MetricKind,
) -> Result<(), MetricRegistrationError> {
    match state.kind_by_name.entry(name.to_owned()) {
        Entry::Occupied(entry) if *entry.get() != kind => {
            Err(incompatible_kind(name, *entry.get(), kind))
        }
        Entry::Occupied(_) => Ok(()),
        Entry::Vacant(entry) => {
            entry.insert(kind);
            Ok(())
        }
    }
}

fn ensure_collector_kind(
    collector: &MetricCollector,
    name: &str,
    requested: MetricKind,
) -> Result<(), MetricRegistrationError> {
    let existing = collector.kind();
    if existing == requested {
        Ok(())
    } else {
        Err(incompatible_kind(name, existing, requested))
    }
}

fn ensure_histogram_buckets(
    collector: &MetricCollector,
    name: &str,
    labels: &[(String, String)],
    requested_buckets: &[u64],
) -> Result<(), MetricRegistrationError> {
    ensure_collector_kind(collector, name, MetricKind::Histogram)?;
    match collector {
        MetricCollector::Histogram(metric) if metric.boundaries() == requested_buckets => Ok(()),
        MetricCollector::Histogram(_) => {
            Err(MetricRegistrationError::IncompatibleHistogramBuckets {
                name: name.to_owned(),
                labels: labels.to_vec(),
            })
        }
        MetricCollector::Counter(_) | MetricCollector::Gauge(_) => Err(incompatible_kind(
            name,
            collector.kind(),
            MetricKind::Histogram,
        )),
    }
}

fn incompatible_kind(
    name: &str,
    existing: MetricKind,
    requested: MetricKind,
) -> MetricRegistrationError {
    MetricRegistrationError::IncompatibleMetricKind {
        name: name.to_owned(),
        existing,
        requested,
    }
}
