use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};

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
    TooManyHistogramBuckets {
        name: String,
        count: usize,
        max: usize,
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
            Self::TooManyHistogramBuckets { name, count, max } => write!(
                formatter,
                "histogram `{name}` has {count} bucket boundaries, exceeding max {max}"
            ),
            Self::GlobalRegistryAlreadyInstalled => {
                formatter.write_str("global metrics registry is already installed")
            }
        }
    }
}

impl Error for MetricRegistrationError {}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricsSnapshot {
    pub metrics: Vec<MetricSnapshot>,
}

impl MetricsSnapshot {
    #[must_use]
    pub fn metrics(&self) -> &[MetricSnapshot] {
        &self.metrics
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricSnapshot {
    pub name: String,
    pub labels: Vec<(String, String)>,
    pub kind: MetricKind,
    pub value: MetricValue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MetricValue {
    Counter(u64),
    Gauge(i64),
    Histogram(HistogramSnapshot),
}

#[derive(Debug, Clone, PartialEq)]
pub struct HistogramSnapshot {
    pub buckets: Vec<HistogramBucketSnapshot>,
    pub sum: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HistogramBucketSnapshot {
    pub upper_bound: Option<f64>,
    pub count: u64,
}
