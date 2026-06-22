use std::time::Duration;

use super::{
    CounterFamily, GaugeFamily, HistogramFamily, MetricRegistrationError, MetricsRegistry,
};

const CHANNEL_LABEL: &str = "channel";
const MESSAGE_RATE_NAME: &str = "channel_message_rate";
const SUBSCRIBER_COUNT_NAME: &str = "channel_subscriber_count";
const QUEUE_DEPTH_NAME: &str = "channel_queue_depth";
const DELIVERY_LATENCY_NAME: &str = "channel_delivery_latency";

#[derive(Clone, Debug)]
pub struct ChannelMetrics {
    pub message_rate: CounterFamily,
    pub subscriber_count: GaugeFamily,
    pub queue_depth: GaugeFamily,
    pub delivery_latency: HistogramFamily,
}

impl ChannelMetrics {
    /// # Errors
    ///
    /// Returns an error when any channel metric family name was previously
    /// registered with an incompatible metric kind, or when the delivery
    /// latency histogram family was registered with different bucket boundaries.
    pub fn new(
        registry: &MetricsRegistry,
        delivery_latency_buckets: Vec<u64>,
    ) -> Result<Self, MetricRegistrationError> {
        let message_rate = registry.register_counter_family(MESSAGE_RATE_NAME, CHANNEL_LABEL)?;
        let subscriber_count =
            registry.register_gauge_family(SUBSCRIBER_COUNT_NAME, CHANNEL_LABEL)?;
        let queue_depth = registry.register_gauge_family(QUEUE_DEPTH_NAME, CHANNEL_LABEL)?;
        let delivery_latency = registry.register_histogram_family(
            DELIVERY_LATENCY_NAME,
            CHANNEL_LABEL,
            delivery_latency_buckets,
        )?;

        Ok(Self {
            message_rate,
            subscriber_count,
            queue_depth,
            delivery_latency,
        })
    }

    pub fn record_publish(&self, channel_name: &str) {
        self.message_rate.increment(channel_name);
    }

    pub fn record_subscribe(&self, channel_name: &str) {
        self.subscriber_count.increment(channel_name);
    }

    pub fn record_unsubscribe(&self, channel_name: &str) {
        self.subscriber_count.decrement(channel_name);
    }

    pub fn set_queue_depth(&self, channel_name: &str, depth: i64) {
        self.queue_depth.set(channel_name, depth);
    }

    pub fn record_delivery_latency(&self, channel_name: &str, duration: Duration) {
        self.delivery_latency
            .observe(channel_name, duration_to_millis(duration));
    }
}

fn duration_to_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
