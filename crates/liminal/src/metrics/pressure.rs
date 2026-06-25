use super::{CounterFamily, GaugeFamily, MetricRegistrationError, MetricsRegistry};

const CHANNEL_LABEL: &str = "channel";
const CONSUMER_LABEL: &str = "consumer";
const REJECT_COUNT_NAME: &str = "pressure_reject_count";
const DEFER_COUNT_NAME: &str = "pressure_defer_count";
const CAPACITY_UTILIZATION_NAME: &str = "pressure_capacity_utilization";

#[derive(Clone, Debug)]
pub struct PressureMetrics {
    pub reject_count: CounterFamily,
    pub defer_count: CounterFamily,
    pub capacity_utilization: GaugeFamily,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryOutcome {
    Accepted,
    Deferred,
    Rejected,
}

impl PressureMetrics {
    /// # Errors
    ///
    /// Returns an error when any pressure metric family name was previously
    /// registered with an incompatible metric kind.
    pub fn new(registry: &MetricsRegistry) -> Result<Self, MetricRegistrationError> {
        let reject_count = registry.register_counter_family(REJECT_COUNT_NAME, CHANNEL_LABEL)?;
        let defer_count = registry.register_counter_family(DEFER_COUNT_NAME, CHANNEL_LABEL)?;
        let capacity_utilization =
            registry.register_gauge_family(CAPACITY_UTILIZATION_NAME, CONSUMER_LABEL)?;

        Ok(Self {
            reject_count,
            defer_count,
            capacity_utilization,
        })
    }

    pub fn record_reject(&self, channel_name: &str) {
        self.reject_count.increment(channel_name);
    }

    pub fn record_defer(&self, channel_name: &str) {
        self.defer_count.increment(channel_name);
    }

    pub fn set_capacity_utilization(&self, consumer_id: &str, utilization: i64) {
        self.capacity_utilization.set(consumer_id, utilization);
    }

    pub fn record_delivery_outcome(&self, channel_name: &str, outcome: DeliveryOutcome) {
        match outcome {
            DeliveryOutcome::Accepted => {}
            DeliveryOutcome::Deferred => self.record_defer(channel_name),
            DeliveryOutcome::Rejected => self.record_reject(channel_name),
        }
    }
}
