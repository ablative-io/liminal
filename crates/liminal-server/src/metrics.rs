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

use crate::server::connection::refusal::AdmissionRefusal;
use crate::server::connection::websocket::UpgradeRefusal;
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
/// P0 #56: connections turned away at the admission door, by reason class.
///
/// ADDITIVE, and it has to be, because no existing family can answer this.
/// [`CONNECTIONS_ACTIVE`] is incremented in `register_record`
/// (`server/connection/supervisor.rs:2822`), which is reached only AFTER
/// admission has succeeded — a refusal is exactly the case where it never runs.
/// On a server refusing every connection that gauge therefore reads a flat
/// zero, indistinguishable from a healthy idle server, which is what the field
/// estate's dashboards showed while 82,166 consecutive connections were turned
/// away. This counter can only move when a connection was refused.
const ADMISSION_REFUSALS_TOTAL: &str = "liminal_admission_refusals_total";
/// P0 #56: WebSocket upgrades refused before admission was even attempted.
///
/// Separate from [`ADMISSION_REFUSALS_TOTAL`] because it counts a different
/// event at a different door: these connections never reached the shared
/// admission bound, the incarnation authority, or the registry. Folding them
/// together would make an origin-policy rejection look like a capacity problem.
const HANDSHAKE_REFUSALS_TOTAL: &str = "liminal_handshake_refusals_total";
/// Label key carried by both refusal families.
///
/// Cardinality is bounded by the variant count of `AdmissionRefusal` and
/// `UpgradeRefusal` respectively — fixed, enum-derived strings. A peer address,
/// an origin value, or an error message here would be unbounded cardinality on
/// a surface a scraper keeps forever.
const REASON_LABEL: &str = "reason";
/// Lane p0-39: entries displaced out of a per-participant retention window.
///
/// ADDITIVE, and it has to be: the event it counts did not exist before. The
/// stage-8 receipt windows used to REFUSE at their bound, which was loud on the
/// wire (a typed `ReceiptCapacityExceeded` the client could see). They now
/// displace instead, which is silent to the arriving client BY DESIGN — the
/// (N+1)th honest fingerprint lands. A bound that neither refuses nor discloses
/// would hide exactly what the old wall at least made loud, so displacement is
/// silent to experience and loud to record: this counter is the record.
const RECEIPT_DISPLACEMENTS_TOTAL: &str = "liminal_receipt_displacements_total";
/// Lane p0-39: observations that a SHARED receipt pool is carrying a churn
/// storm (`liminal_receipt_pool_runaway_total`).
///
/// The three shared pools stopped being admission gates entirely — no
/// configured number may refuse an honest third party there — so their only
/// bound is the TTL window. This counter is the tripwire that replaces the
/// wall: it is an OCCUPANCY OBSERVATION, never a gate, and nothing is refused
/// because it moved. It increments once per admitted operation that observed a
/// pool at or above its configured reporting threshold, so its rate is the
/// storm's rate.
const RECEIPT_POOL_RUNAWAY_TOTAL: &str = "liminal_receipt_pool_runaway_total";
/// Label key carried by [`RECEIPT_DISPLACEMENTS_TOTAL`]. Cardinality is bounded
/// by [`ReceiptWindowScope`]'s two variants.
const SCOPE_LABEL: &str = "scope";
/// Label key carried by [`RECEIPT_POOL_RUNAWAY_TOTAL`]. Cardinality is bounded
/// by [`SharedReceiptPool`]'s three variants.
const POOL_LABEL: &str = "pool";

/// The per-participant retention windows that can displace an older entry.
///
/// A fixed, enum-derived label vocabulary: these are the only two scopes whose
/// configured number is a window size rather than a gate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReceiptWindowScope {
    /// Stage-8 `LiveReceiptParticipant`.
    LiveReceiptParticipant,
    /// Stage-8 `ProvenanceParticipant`.
    ProvenanceParticipant,
}

impl ReceiptWindowScope {
    /// Every window scope, in handle-storage order.
    pub(crate) const LABELS: [&'static str; 2] =
        ["live_receipt_participant", "provenance_participant"];

    const fn slot(self) -> usize {
        match self {
            Self::LiveReceiptParticipant => 0,
            Self::ProvenanceParticipant => 1,
        }
    }
}

/// The shared receipt pools whose retention is TTL-bounded and tripwired.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SharedReceiptPool {
    /// Stage-8 `LiveReceiptServer`.
    LiveReceiptServer,
    /// Stage-8 `ProvenanceServer`.
    ProvenanceServer,
    /// Stage-8 `ProvenanceConversation`.
    ProvenanceConversation,
}

impl SharedReceiptPool {
    /// Every shared pool, in handle-storage order.
    pub(crate) const LABELS: [&'static str; 3] = [
        "live_receipt_server",
        "provenance_server",
        "provenance_conversation",
    ];

    const fn slot(self) -> usize {
        match self {
            Self::LiveReceiptServer => 0,
            Self::ProvenanceServer => 1,
            Self::ProvenanceConversation => 2,
        }
    }

    /// Human-readable pool name for the paired rising-edge warning.
    pub(crate) const fn label(self) -> &'static str {
        Self::LABELS[self.slot()]
    }
}

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
    /// One pre-registered handle per admission-refusal class, so the refusal
    /// path looks a counter up by discriminant and never allocates a label
    /// string while it is busy turning a connection away.
    admission_refusals: [CounterHandle; AdmissionRefusal::LABELS.len()],
    /// One pre-registered handle per WebSocket upgrade-refusal class.
    handshake_refusals: [CounterHandle; UpgradeRefusal::LABELS.len()],
    /// One pre-registered handle per per-participant retention window.
    receipt_displacements: [CounterHandle; ReceiptWindowScope::LABELS.len()],
    /// One pre-registered handle per shared receipt pool.
    receipt_pool_runaway: [CounterHandle; SharedReceiptPool::LABELS.len()],
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

/// Records one connection turned away at the admission door
/// (`liminal_admission_refusals_total`, labelled by reason class).
///
/// Called at the three admission doors — TCP accept, WebSocket upgrade, and the
/// in-process loopback — and nowhere else, so a refusal is counted exactly once
/// no matter which mount knocked.
pub(crate) fn admission_refused(refusal: AdmissionRefusal) {
    if let Some(metrics) = SERVER_METRICS.get() {
        metrics.admission_refusals[refusal.slot()].increment();
    }
}

/// Records one WebSocket upgrade refused before admission was attempted
/// (`liminal_handshake_refusals_total`, labelled by reason class).
pub(crate) fn handshake_refused(refusal: &UpgradeRefusal) {
    if let Some(metrics) = SERVER_METRICS.get() {
        metrics.handshake_refusals[refusal.slot()].increment();
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

/// Records `count` entries displaced out of one per-participant retention
/// window (`liminal_receipt_displacements_total`, labelled by scope).
///
/// The arriving client sees nothing — that is the ruled behaviour, "silent to
/// experience" — so this counter and the `debug` line beside it are the only
/// record that a bound did work. A zero count returns early, so the ordinary
/// with-headroom path touches no atomic.
pub(crate) fn receipt_entries_displaced(scope: ReceiptWindowScope, count: u64) {
    if count == 0 {
        return;
    }
    if let Some(metrics) = SERVER_METRICS.get() {
        metrics.receipt_displacements[scope.slot()].increment_by(count);
    }
}

/// Records one observation of a shared receipt pool at or above its configured
/// reporting threshold (`liminal_receipt_pool_runaway_total`, labelled by pool).
///
/// An OBSERVATION, not a refusal: the operation that made it was admitted. The
/// rising-edge `warn` beside the first such observation carries the occupancy
/// and threshold; this counter carries the storm's rate.
pub(crate) fn receipt_pool_runaway_observed(pool: SharedReceiptPool) {
    if let Some(metrics) = SERVER_METRICS.get() {
        metrics.receipt_pool_runaway[pool.slot()].increment();
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

/// The current value of one lane p0-39 displacement counter, by scope.
///
/// `None` means the family is not readable — either [`init`] has not run in
/// this process or the registry holds no such counter — which a caller must
/// treat as "no measurement", never as zero. Name and label are read from the
/// same constants the registration uses, so a rename shows up as a failure
/// rather than as a vacuous pass.
#[cfg(test)]
pub(crate) fn receipt_displacements_value(scope: ReceiptWindowScope) -> Option<u64> {
    labelled_counter_value(
        RECEIPT_DISPLACEMENTS_TOTAL,
        SCOPE_LABEL,
        ReceiptWindowScope::LABELS[scope.slot()],
    )
}

/// The current value of one lane p0-39 shared-pool tripwire counter, by pool.
///
/// `None` means "not measured", never zero — see
/// [`receipt_displacements_value`].
#[cfg(test)]
pub(crate) fn receipt_pool_runaway_value(pool: SharedReceiptPool) -> Option<u64> {
    labelled_counter_value(RECEIPT_POOL_RUNAWAY_TOTAL, POOL_LABEL, pool.label())
}

#[cfg(test)]
fn labelled_counter_value(name: &str, key: &str, label: &str) -> Option<u64> {
    use liminal::metrics::MetricValue;

    let registry = global_registry()?;
    registry
        .snapshot()
        .metrics()
        .iter()
        .find(|metric| {
            metric.name == name
                && metric
                    .labels
                    .iter()
                    .any(|(metric_key, value)| metric_key == key && value == label)
        })
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
        let admission_refusals = register_labelled(
            registry,
            ADMISSION_REFUSALS_TOTAL,
            REASON_LABEL,
            &AdmissionRefusal::LABELS,
        )?;
        let handshake_refusals = register_labelled(
            registry,
            HANDSHAKE_REFUSALS_TOTAL,
            REASON_LABEL,
            &UpgradeRefusal::LABELS,
        )?;
        let receipt_displacements = register_labelled(
            registry,
            RECEIPT_DISPLACEMENTS_TOTAL,
            SCOPE_LABEL,
            &ReceiptWindowScope::LABELS,
        )?;
        let receipt_pool_runaway = register_labelled(
            registry,
            RECEIPT_POOL_RUNAWAY_TOTAL,
            POOL_LABEL,
            &SharedReceiptPool::LABELS,
        )?;
        Some(Self {
            connections_active,
            publishes_total,
            deliveries_total,
            transport_deliveries,
            sheds_total,
            admission_refusals,
            handshake_refusals,
            receipt_displacements,
            receipt_pool_runaway,
        })
    }
}

/// Pre-registers one counter per label value, in label order.
///
/// Every class is registered at `init`, not lazily on first refusal, so the
/// exposition carries an explicit zero for classes that have not fired. A
/// missing line and a zero line mean very different things to an operator: the
/// first is "this server does not know about that failure mode", the second is
/// "it has not happened".
fn register_labelled<const N: usize>(
    registry: &MetricsRegistry,
    name: &'static str,
    key: &'static str,
    labels: &[&'static str; N],
) -> Option<[CounterHandle; N]> {
    let mut handles = Vec::with_capacity(N);
    for label in labels {
        handles.push(registry.register_counter(name, [(key, *label)]).ok()?);
    }
    handles.try_into().ok()
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
