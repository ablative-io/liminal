//! Server-side metric recording over the process-global liminal registry.
//!
//! [`init`] installs a global [`MetricsRegistry`] — which flips the library's
//! metrics gate on for this process — and registers the first-wave server
//! families, caching their handles. Every recording helper no-ops until `init`
//! has run, so a standalone liminal library user (who never calls `init`) pays
//! nothing and the registry stays disabled.

use std::sync::OnceLock;

use liminal::metrics::{
    CounterHandle, GaugeHandle, MetricsRegistry, global_registry, install_global_registry,
};

use crate::server::mount::MountKind;

const CONNECTIONS_ACTIVE: &str = "liminal_connections_active";
const PUBLISHES_TOTAL: &str = "liminal_publishes_total";
const DELIVERIES_TOTAL: &str = "liminal_deliveries_total";
/// Second-wave: deliveries split by the transport that carried them.
///
/// ADDITIVE. [`DELIVERIES_TOTAL`] keeps its meaning and its exposition line
/// unchanged — an operator's existing `/metrics` scrape sees every line it saw
/// before, plus these. The two families count different things and are not
/// expected to agree: the aggregate counts subscriber deliveries accepted at
/// PUBLISH time (channel-actor fan-out), this one counts `Deliver` frames the
/// per-connection pump actually enqueued toward a socket.
const TRANSPORT_DELIVERIES_TOTAL: &str = "liminal_transport_deliveries_total";
/// Second-wave: subscriptions shed by an inbox overflow (P0 #55).
///
/// Deliberately UNLABELLED. A per-channel or per-subscription shed counter is
/// unbounded cardinality on a surface a scraper keeps forever; the channel and
/// subscription of any individual shed are on the `warn` line the shed emits
/// beside this increment, where they cost one log record rather than a permanent
/// time series.
const SHEDS_TOTAL: &str = "liminal_subscription_sheds_total";
/// Label key carried by [`TRANSPORT_DELIVERIES_TOTAL`]. Cardinality is bounded by
/// [`MountKind`]'s variants, which is why THIS split is affordable and a
/// per-channel one is not.
const TRANSPORT_LABEL: &str = "transport";

static SERVER_METRICS: OnceLock<ServerMetrics> = OnceLock::new();

/// Cached handles for the first-wave server metrics.
#[derive(Clone, Debug)]
struct ServerMetrics {
    connections_active: GaugeHandle,
    publishes_total: CounterHandle,
    deliveries_total: CounterHandle,
    /// One pre-registered handle per [`MountKind`], so the delivery hot path
    /// looks a counter up by enum discriminant and never registers, allocates a
    /// label string, or hashes a name mid-slice.
    transport_deliveries: [CounterHandle; MOUNT_KINDS.len()],
    sheds_total: CounterHandle,
}

/// Every transport a delivery can ride, in the order their handles are stored.
const MOUNT_KINDS: [MountKind; 3] = [MountKind::Tcp, MountKind::WebSocket, MountKind::Loopback];

const fn transport_slot(transport: MountKind) -> usize {
    match transport {
        MountKind::Tcp => 0,
        MountKind::WebSocket => 1,
        MountKind::Loopback => 2,
    }
}

/// Enables metrics for this server process and registers the server families.
///
/// Idempotent: a second call is a no-op. Called once at server startup so the
/// `/metrics` endpoint has data to render; the recording helpers below stay
/// inert until this runs.
pub fn init() {
    if SERVER_METRICS.get().is_some() {
        return;
    }
    let Some(registry) = global_or_install() else {
        return;
    };
    if let Some(metrics) = ServerMetrics::register(registry) {
        let _ = SERVER_METRICS.set(metrics);
    }
}

/// Records the spawn of a supervised connection (`liminal_connections_active`
/// gauge increment). Paired with [`connection_closed`] on every teardown route.
pub fn connection_spawned() {
    if let Some(metrics) = SERVER_METRICS.get() {
        metrics.connections_active.increment();
    }
}

/// Records the teardown of a supervised connection (`liminal_connections_active`
/// gauge decrement). Paired with [`connection_spawned`].
pub fn connection_closed() {
    if let Some(metrics) = SERVER_METRICS.get() {
        metrics.connections_active.decrement();
    }
}

/// Records one accepted publish on the services publish path
/// (`liminal_publishes_total`).
pub fn publish_accepted() {
    if let Some(metrics) = SERVER_METRICS.get() {
        metrics.publishes_total.increment();
    }
}

/// Records `count` genuine subscriber deliveries from a single publish
/// (`liminal_deliveries_total`). A publish that reached no subscriber records
/// nothing.
pub fn deliveries_recorded(count: u64) {
    if count == 0 {
        return;
    }
    if let Some(metrics) = SERVER_METRICS.get() {
        metrics.deliveries_total.increment_by(count);
    }
}

/// Records pump deliveries on `transport` (`liminal_transport_deliveries_total`).
///
/// `count` is the number of `Deliver` frames one connection slice enqueued. A
/// slice that delivered nothing records nothing, so an idle connection touches no
/// atomic at all.
pub fn transport_deliveries_recorded(transport: MountKind, count: u64) {
    if count == 0 {
        return;
    }
    if let Some(metrics) = SERVER_METRICS.get() {
        metrics.transport_deliveries[transport_slot(transport)].increment_by(count);
    }
}

/// Records one subscription shed by an inbox overflow
/// (`liminal_subscription_sheds_total`).
///
/// Paired at its only call site with the `warn` that names the channel,
/// subscription and transport: the counter is what an alert fires on, the log
/// line is what the alert is then read against.
pub fn subscription_shed() {
    if let Some(metrics) = SERVER_METRICS.get() {
        metrics.sheds_total.increment();
    }
}

/// The current value of the accepted-publish counter, for a test that needs an
/// UNRELATED counter to prove its harness measured anything at all.
///
/// `None` means the family is not readable — either [`init`] has not run in this
/// process or the registry holds no such counter — which a caller must treat as
/// "no measurement", never as zero. The name is read from the same constant the
/// registration uses, so the two cannot drift.
#[cfg(test)]
pub(crate) fn publishes_total_value() -> Option<u64> {
    use liminal::metrics::MetricValue;

    let registry = global_registry()?;
    registry
        .snapshot()
        .metrics()
        .iter()
        .find(|metric| metric.name == PUBLISHES_TOTAL)
        .and_then(|metric| match metric.value {
            MetricValue::Counter(value) => Some(value),
            MetricValue::Gauge(_) | MetricValue::Histogram(_) => None,
        })
}

impl ServerMetrics {
    fn register(registry: &MetricsRegistry) -> Option<Self> {
        let connections_active = registry
            .register_gauge(CONNECTIONS_ACTIVE, no_labels())
            .ok()?;
        let publishes_total = registry
            .register_counter(PUBLISHES_TOTAL, no_labels())
            .ok()?;
        let deliveries_total = registry
            .register_counter(DELIVERIES_TOTAL, no_labels())
            .ok()?;
        let mut transport_deliveries = Vec::with_capacity(MOUNT_KINDS.len());
        for transport in MOUNT_KINDS {
            transport_deliveries.push(
                registry
                    .register_counter(
                        TRANSPORT_DELIVERIES_TOTAL,
                        [(TRANSPORT_LABEL, transport.as_str())],
                    )
                    .ok()?,
            );
        }
        let transport_deliveries: [CounterHandle; MOUNT_KINDS.len()] =
            transport_deliveries.try_into().ok()?;
        let sheds_total = registry.register_counter(SHEDS_TOTAL, no_labels()).ok()?;
        Some(Self {
            connections_active,
            publishes_total,
            deliveries_total,
            transport_deliveries,
            sheds_total,
        })
    }
}

const fn no_labels() -> std::iter::Empty<(&'static str, &'static str)> {
    std::iter::empty()
}

/// Returns the process-global registry, installing a fresh one when none exists.
///
/// Enabling the gate here (rather than in the library) keeps standalone liminal
/// users on the disabled fast path; the server is the sole installer.
fn global_or_install() -> Option<&'static MetricsRegistry> {
    if let Some(registry) = global_registry() {
        return Some(registry);
    }
    // Best-effort install; if a concurrent caller won the race we still read the
    // now-installed registry back below.
    let _ = install_global_registry(MetricsRegistry::new());
    global_registry()
}

#[cfg(test)]
mod tests {
    use super::{
        CONNECTIONS_ACTIVE, DELIVERIES_TOTAL, PUBLISHES_TOTAL, connection_spawned,
        deliveries_recorded, init, publish_accepted,
    };
    use liminal::metrics::{global_registry, render};

    #[test]
    fn init_registers_the_three_server_families_on_the_global_registry()
    -> Result<(), Box<dyn std::error::Error>> {
        init();
        connection_spawned();
        publish_accepted();
        deliveries_recorded(2);

        let registry =
            global_registry().ok_or("init must install and enable the global registry")?;
        let exposition = render(&registry.snapshot());

        assert!(exposition.contains(CONNECTIONS_ACTIVE));
        assert!(exposition.contains(PUBLISHES_TOTAL));
        assert!(exposition.contains(DELIVERIES_TOTAL));

        Ok(())
    }
}
