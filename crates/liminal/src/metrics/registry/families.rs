use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::{
    CounterHandle, GaugeHandle, HistogramHandle, HistogramValue, MetricKind,
    MetricRegistrationError, MetricsRegistry, RegistryState, ensure_histogram_bucket_count,
    ensure_name_kind, normalize_buckets, write_registry_state,
};

#[derive(Clone, Debug)]
pub struct CounterFamily {
    inner: Arc<CounterFamilyInner>,
}

impl CounterFamily {
    pub(super) fn new(
        registry: MetricsRegistry,
        name: impl Into<String>,
        label_name: impl Into<String>,
    ) -> Result<Self, MetricRegistrationError> {
        let name = name.into();
        registry.reserve_metric_family(&name, MetricKind::Counter)?;
        Ok(Self {
            inner: Arc::new(CounterFamilyInner {
                registry,
                name,
                label_name: label_name.into(),
                handles: RwLock::new(HashMap::new()),
            }),
        })
    }

    pub fn increment(&self, label_value: &str) {
        self.handle(label_value).increment();
    }

    #[must_use]
    pub fn value(&self, label_value: &str) -> u64 {
        self.handle(label_value).value()
    }

    fn handle(&self, label_value: &str) -> CounterHandle {
        if let Some(handle) = self.cached_handle(label_value) {
            return handle;
        }

        self.register_handle(label_value)
    }

    fn cached_handle(&self, label_value: &str) -> Option<CounterHandle> {
        read_cache(&self.inner.handles).get(label_value).cloned()
    }

    fn register_handle(&self, label_value: &str) -> CounterHandle {
        let mut handles = write_cache(&self.inner.handles);
        if let Some(handle) = handles.get(label_value) {
            return handle.clone();
        }

        let handle = match self.inner.registry.register_counter(
            self.inner.name.clone(),
            [(self.inner.label_name.as_str(), label_value)],
        ) {
            Ok(handle) => handle,
            Err(_error) => CounterHandle::noop(),
        };
        handles.insert(label_value.to_owned(), handle.clone());
        handle
    }
}

#[derive(Debug)]
struct CounterFamilyInner {
    registry: MetricsRegistry,
    name: String,
    label_name: String,
    handles: RwLock<HashMap<String, CounterHandle>>,
}

#[derive(Clone, Debug)]
pub struct GaugeFamily {
    inner: Arc<GaugeFamilyInner>,
}

impl GaugeFamily {
    pub(super) fn new(
        registry: MetricsRegistry,
        name: impl Into<String>,
        label_name: impl Into<String>,
    ) -> Result<Self, MetricRegistrationError> {
        let name = name.into();
        registry.reserve_metric_family(&name, MetricKind::Gauge)?;
        Ok(Self {
            inner: Arc::new(GaugeFamilyInner {
                registry,
                name,
                label_name: label_name.into(),
                handles: RwLock::new(HashMap::new()),
            }),
        })
    }

    pub fn set(&self, label_value: &str, value: i64) {
        self.handle(label_value).set(value);
    }

    pub fn increment(&self, label_value: &str) {
        self.handle(label_value).increment();
    }

    pub fn decrement(&self, label_value: &str) {
        self.handle(label_value).decrement();
    }

    #[must_use]
    pub fn value(&self, label_value: &str) -> i64 {
        self.handle(label_value).value()
    }

    fn handle(&self, label_value: &str) -> GaugeHandle {
        if let Some(handle) = self.cached_handle(label_value) {
            return handle;
        }

        self.register_handle(label_value)
    }

    fn cached_handle(&self, label_value: &str) -> Option<GaugeHandle> {
        read_cache(&self.inner.handles).get(label_value).cloned()
    }

    fn register_handle(&self, label_value: &str) -> GaugeHandle {
        let mut handles = write_cache(&self.inner.handles);
        if let Some(handle) = handles.get(label_value) {
            return handle.clone();
        }

        let handle = match self.inner.registry.register_gauge(
            self.inner.name.clone(),
            [(self.inner.label_name.as_str(), label_value)],
        ) {
            Ok(handle) => handle,
            Err(_error) => GaugeHandle::noop(),
        };
        handles.insert(label_value.to_owned(), handle.clone());
        handle
    }
}

#[derive(Debug)]
struct GaugeFamilyInner {
    registry: MetricsRegistry,
    name: String,
    label_name: String,
    handles: RwLock<HashMap<String, GaugeHandle>>,
}

#[derive(Clone, Debug)]
pub struct HistogramFamily {
    inner: Arc<HistogramFamilyInner>,
}

impl HistogramFamily {
    pub(super) fn new<Bucket>(
        registry: MetricsRegistry,
        name: impl Into<String>,
        label_name: impl Into<String>,
        buckets: Vec<Bucket>,
    ) -> Result<Self, MetricRegistrationError>
    where
        Bucket: HistogramValue,
    {
        let name = name.into();
        let buckets = normalize_buckets(buckets);
        registry.reserve_histogram_family(name.clone(), buckets.clone())?;
        Ok(Self {
            inner: Arc::new(HistogramFamilyInner {
                registry,
                name,
                label_name: label_name.into(),
                buckets,
                handles: RwLock::new(HashMap::new()),
            }),
        })
    }

    pub fn observe<Value>(&self, label_value: &str, value: Value)
    where
        Value: HistogramValue,
    {
        self.handle(label_value).observe(value);
    }

    #[must_use]
    pub fn boundaries(&self) -> &[f64] {
        &self.inner.buckets
    }

    fn handle(&self, label_value: &str) -> HistogramHandle {
        if let Some(handle) = self.cached_handle(label_value) {
            return handle;
        }

        self.register_handle(label_value)
    }

    fn cached_handle(&self, label_value: &str) -> Option<HistogramHandle> {
        read_cache(&self.inner.handles).get(label_value).cloned()
    }

    fn register_handle(&self, label_value: &str) -> HistogramHandle {
        let mut handles = write_cache(&self.inner.handles);
        if let Some(handle) = handles.get(label_value) {
            return handle.clone();
        }

        let handle = match self.inner.registry.register_histogram(
            self.inner.name.clone(),
            [(self.inner.label_name.as_str(), label_value)],
            self.inner.buckets.clone(),
        ) {
            Ok(handle) => handle,
            Err(_error) => HistogramHandle::noop(self.inner.buckets.clone()),
        };
        handles.insert(label_value.to_owned(), handle.clone());
        handle
    }
}

#[derive(Debug)]
struct HistogramFamilyInner {
    registry: MetricsRegistry,
    name: String,
    label_name: String,
    buckets: Vec<f64>,
    handles: RwLock<HashMap<String, HistogramHandle>>,
}

impl MetricsRegistry {
    /// # Errors
    ///
    /// Returns an error if `name` was previously registered with a different kind.
    pub fn register_counter_family(
        &self,
        name: impl Into<String>,
        label_name: impl Into<String>,
    ) -> Result<CounterFamily, MetricRegistrationError> {
        CounterFamily::new(self.clone(), name, label_name)
    }

    /// # Errors
    ///
    /// Returns an error if `name` was previously registered with a different kind.
    pub fn register_gauge_family(
        &self,
        name: impl Into<String>,
        label_name: impl Into<String>,
    ) -> Result<GaugeFamily, MetricRegistrationError> {
        GaugeFamily::new(self.clone(), name, label_name)
    }

    /// # Errors
    ///
    /// Returns an error if `name` was previously registered with a different kind
    /// or if the histogram family was registered with different buckets.
    pub fn register_histogram_family<Bucket>(
        &self,
        name: impl Into<String>,
        label_name: impl Into<String>,
        buckets: Vec<Bucket>,
    ) -> Result<HistogramFamily, MetricRegistrationError>
    where
        Bucket: HistogramValue,
    {
        HistogramFamily::new(self.clone(), name, label_name, buckets)
    }

    pub(super) fn reserve_metric_family(
        &self,
        name: &str,
        kind: MetricKind,
    ) -> Result<(), MetricRegistrationError> {
        let mut state = write_registry_state(&self.inner.state);
        ensure_name_kind(&mut state, name, kind)
    }

    pub(super) fn reserve_histogram_family(
        &self,
        name: String,
        buckets: Vec<f64>,
    ) -> Result<(), MetricRegistrationError> {
        ensure_histogram_bucket_count(&name, buckets.len())?;
        let mut state = write_registry_state(&self.inner.state);
        ensure_name_kind(&mut state, &name, MetricKind::Histogram)?;
        ensure_histogram_family_buckets(&mut state, name, buckets)
    }
}

fn ensure_histogram_family_buckets(
    state: &mut RegistryState,
    name: String,
    buckets: Vec<f64>,
) -> Result<(), MetricRegistrationError> {
    match state.histogram_buckets_by_name.get(&name) {
        Some(existing) if existing != &buckets => {
            Err(MetricRegistrationError::IncompatibleHistogramBuckets {
                name,
                labels: Vec::new(),
            })
        }
        Some(_) => Ok(()),
        None => {
            state.histogram_buckets_by_name.insert(name, buckets);
            Ok(())
        }
    }
}

fn read_cache<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    match lock.read() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn write_cache<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    match lock.write() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
