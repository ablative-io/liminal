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

const CONNECTIONS_ACTIVE: &str = "liminal_connections_active";
const PUBLISHES_TOTAL: &str = "liminal_publishes_total";
const DELIVERIES_TOTAL: &str = "liminal_deliveries_total";

static SERVER_METRICS: OnceLock<ServerMetrics> = OnceLock::new();

/// Cached handles for the first-wave server metrics.
#[derive(Clone, Debug)]
struct ServerMetrics {
    connections_active: GaugeHandle,
    publishes_total: CounterHandle,
    deliveries_total: CounterHandle,
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
        Some(Self {
            connections_active,
            publishes_total,
            deliveries_total,
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
