use std::time::Duration;

use super::{
    CounterHandle, GaugeHandle, HistogramHandle, HistogramValue, MetricRegistrationError,
    MetricsRegistry,
};

const ACTIVE_COUNT_NAME: &str = "conversation_active_count";
const COMPLETION_COUNT_NAME: &str = "conversation_completion_count";
const DURATION_NAME: &str = "conversation_duration";
const ERROR_COUNT_NAME: &str = "conversation_error_count";

#[derive(Clone, Debug)]
pub struct ConversationMetrics {
    pub active_count: GaugeHandle,
    pub completion_count: CounterHandle,
    pub duration: HistogramHandle,
    pub error_count: CounterHandle,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConversationOutcome {
    Completed,
    Failed,
    TimedOut,
}

impl ConversationMetrics {
    /// `duration_buckets` are the histogram's upper-bound thresholds **in seconds**,
    /// matching the unit used by [`Self::record_duration`] (which observes
    /// `Duration::as_secs_f64`).
    ///
    /// # Errors
    ///
    /// Returns an error when any conversation metric name was previously
    /// registered with an incompatible metric kind, or when the duration
    /// histogram was registered with different bucket boundaries.
    pub fn new<Bucket>(
        registry: &MetricsRegistry,
        duration_buckets: Vec<Bucket>,
    ) -> Result<Self, MetricRegistrationError>
    where
        Bucket: HistogramValue,
    {
        let active_count =
            registry.register_gauge(ACTIVE_COUNT_NAME, std::iter::empty::<(&str, &str)>())?;
        let completion_count =
            registry.register_counter(COMPLETION_COUNT_NAME, std::iter::empty::<(&str, &str)>())?;
        let duration = registry.register_histogram(
            DURATION_NAME,
            std::iter::empty::<(&str, &str)>(),
            duration_buckets,
        )?;
        let error_count =
            registry.register_counter(ERROR_COUNT_NAME, std::iter::empty::<(&str, &str)>())?;

        Ok(Self {
            active_count,
            completion_count,
            duration,
            error_count,
        })
    }

    pub fn record_start(&self) {
        self.active_count.increment();
    }

    pub fn record_end(&self) {
        self.active_count.decrement();
    }

    pub fn record_completion(&self) {
        self.completion_count.increment();
    }

    /// Records a conversation's elapsed duration. `duration` is observed in
    /// seconds (`Duration::as_secs_f64`); bucket boundaries supplied to
    /// [`Self::new`] must also be in seconds.
    pub fn record_duration(&self, duration: Duration) {
        self.duration.observe(duration.as_secs_f64());
    }

    pub fn record_error(&self) {
        self.error_count.increment();
    }

    pub fn record_terminal(&self, outcome: ConversationOutcome, duration: Duration) {
        match outcome {
            ConversationOutcome::Completed => self.record_completion(),
            ConversationOutcome::Failed | ConversationOutcome::TimedOut => self.record_error(),
        }
        self.record_duration(duration);
        self.record_end();
    }
}
