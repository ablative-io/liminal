use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

/// Process liveness state returned by the liveness probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthState {
    /// The server process is alive.
    Healthy,
    /// The server process is not alive.
    Unhealthy,
}

/// Result of the server liveness probe.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct HealthStatus {
    /// Liveness status for the process.
    pub status: HealthState,
    /// Optional operator-facing liveness detail.
    pub message: Option<String>,
}

impl HealthStatus {
    /// Returns the healthy liveness status used while the process is running.
    #[must_use]
    pub const fn healthy() -> Self {
        Self {
            status: HealthState::Healthy,
            message: None,
        }
    }

    /// Returns an unhealthy liveness status with explanatory detail.
    #[must_use]
    pub fn unhealthy(message: impl Into<String>) -> Self {
        Self {
            status: HealthState::Unhealthy,
            message: Some(message.into()),
        }
    }
}

/// Returns the server process liveness status.
///
/// This is deliberately a liveness probe, not a readiness probe: if the process
/// can call this function, the process is alive and the result is healthy. That
/// contract is intact and correct, and P0 #56 did NOT change it.
///
/// It does mean this function cannot answer "is the server serving?", and on the
/// field estate it was read as if it could: a boot whose admission authority was
/// latched refused 82,166 consecutive connections while this probe stayed green
/// throughout, which it was right to do — the process was alive. The question
/// that was actually being asked belongs to [`readiness_check`], which since
/// P0 #56 reports [`ReadinessCondition::AdmissionAvailable`] unmet when the
/// server cannot admit a connection. An orchestrator that wants traffic steered
/// away from a server that cannot serve must read READINESS.
#[must_use]
pub const fn health_check() -> HealthStatus {
    HealthStatus::healthy()
}

/// Shared, lock-free view of whether the server can admit a connection.
///
/// Owned by the connection-incarnation authority (the thing that actually knows)
/// and read by the readiness probe. A handle rather than a snapshot because the
/// answer changes at runtime: the authority arms it false when a durable write
/// goes ambiguous, and back to true when a resume replay re-establishes ground
/// truth.
#[derive(Clone, Debug)]
pub struct AdmissionReadiness {
    available: Arc<AtomicBool>,
}

impl AdmissionReadiness {
    /// Creates a handle that starts available.
    #[must_use]
    pub fn available() -> Self {
        Self {
            available: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Whether the server can currently admit a connection.
    #[must_use]
    pub fn is_available(&self) -> bool {
        self.available.load(Ordering::SeqCst)
    }

    /// Records whether the server can currently admit a connection.
    pub fn set_available(&self, available: bool) {
        self.available.store(available, Ordering::SeqCst);
    }
}

impl Default for AdmissionReadiness {
    fn default() -> Self {
        Self::available()
    }
}

/// Cluster readiness requirement for a startup snapshot.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ClusterReadiness {
    /// No cluster configuration is present, so membership is not required.
    #[default]
    NotConfigured,
    /// Cluster configuration is present and membership must be established.
    Configured {
        /// Whether beamr distribution membership has been established.
        membership_established: bool,
    },
}

/// Startup state evaluated by the readiness probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadinessState {
    /// Whether configuration has loaded, environment overrides applied, and
    /// validation completed successfully.
    pub config_loaded: bool,
    /// Whether the main wire protocol listener is bound and accepting traffic.
    ///
    /// A bound listener is not a serving one: a socket can be accepted and then
    /// refused at the admission door, which is the P0 #56 failure. See
    /// [`Self::admission_available`].
    pub listener_bound: bool,
    /// Conditional cluster startup state.
    pub cluster: ClusterReadiness,
    /// Whether the server can admit a connection at all (P0 #56).
    ///
    /// Distinct from [`Self::listener_bound`] because the field defect sat
    /// exactly between the two: ports listening, accepts succeeding, and every
    /// connection refused a moment later by a latched incarnation authority.
    pub admission_available: bool,
}

impl ReadinessState {
    /// Creates a startup readiness snapshot.
    #[must_use]
    pub const fn new(config_loaded: bool, listener_bound: bool, cluster: ClusterReadiness) -> Self {
        Self::with_admission(config_loaded, listener_bound, cluster, true)
    }

    /// Creates a startup readiness snapshot including the admission gate.
    #[must_use]
    pub const fn with_admission(
        config_loaded: bool,
        listener_bound: bool,
        cluster: ClusterReadiness,
        admission_available: bool,
    ) -> Self {
        Self {
            config_loaded,
            listener_bound,
            cluster,
            admission_available,
        }
    }

    /// Creates a fully ready snapshot for a non-clustered server.
    #[must_use]
    pub const fn ready_without_cluster() -> Self {
        Self::new(true, true, ClusterReadiness::NotConfigured)
    }

    /// Creates a fully ready snapshot for a clustered server.
    #[must_use]
    pub const fn ready_with_cluster() -> Self {
        Self::new(
            true,
            true,
            ClusterReadiness::Configured {
                membership_established: true,
            },
        )
    }
}

impl Default for ReadinessState {
    /// The pre-startup snapshot: nothing loaded, nothing bound, and admission
    /// ASSUMED AVAILABLE.
    ///
    /// Not a derive, because `bool::default()` is false and that would be a
    /// different claim: it would report a brand-new server as unable to admit
    /// connections, which is not something anything has observed yet. The
    /// admission gate is the one condition here that reports a RUNTIME fact
    /// rather than a startup step, so its honest default is "no evidence
    /// against", matching what `SharedReadinessState::snapshot` reports before
    /// an admission source is installed.
    fn default() -> Self {
        Self::with_admission(false, false, ClusterReadiness::NotConfigured, true)
    }
}

/// Readiness conditions that can prevent a server from receiving traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadinessCondition {
    /// Configuration has not completed loading and validation.
    ConfigLoaded,
    /// The main wire protocol listener is not bound and accepting traffic.
    ListenerBound,
    /// Cluster configuration is present but membership is not established.
    ClusterMembershipEstablished,
    /// The server cannot admit a connection (P0 #56). Ports may be listening
    /// and accepts may be succeeding; what fails is everything after that.
    AdmissionAvailable,
}

/// Result of the server readiness probe.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ReadinessStatus {
    /// True only when all applicable startup gates are satisfied.
    pub ready: bool,
    /// Startup gates that are not yet satisfied, in stable evaluation order.
    pub unmet_conditions: Vec<ReadinessCondition>,
}

impl ReadinessStatus {
    /// Creates a readiness status from unmet startup gates.
    #[must_use]
    pub fn from_unmet_conditions(unmet_conditions: Vec<ReadinessCondition>) -> Self {
        Self {
            ready: unmet_conditions.is_empty(),
            unmet_conditions,
        }
    }
}

/// Thread-safe readiness state shared with the HTTP endpoint server.
#[derive(Debug, Clone)]
pub struct SharedReadinessState {
    inner: Arc<ReadinessFlags>,
}

impl SharedReadinessState {
    /// Creates a shared readiness state from an initial startup snapshot.
    #[must_use]
    pub fn new(initial: ReadinessState) -> Self {
        Self {
            inner: Arc::new(ReadinessFlags::from_state(initial)),
        }
    }

    /// Returns a consistent snapshot of the current readiness flags.
    #[must_use]
    pub fn snapshot(&self) -> ReadinessState {
        let cluster = if self.inner.cluster_configured.load(Ordering::SeqCst) {
            ClusterReadiness::Configured {
                membership_established: self
                    .inner
                    .cluster_membership_established
                    .load(Ordering::SeqCst),
            }
        } else {
            ClusterReadiness::NotConfigured
        };

        ReadinessState::with_admission(
            self.inner.config_loaded.load(Ordering::SeqCst),
            self.inner.listener_bound.load(Ordering::SeqCst),
            cluster,
            self.inner
                .admission
                .get()
                .is_none_or(AdmissionReadiness::is_available),
        )
    }

    /// Installs the admission-availability source readiness reports on.
    ///
    /// Called once, after the connection supervisor is built, because the
    /// authority that owns the answer does not exist before then. A second call
    /// is a no-op rather than a silent rebind.
    pub fn track_admission(&self, admission: AdmissionReadiness) {
        let _ = self.inner.admission.set(admission);
    }

    /// Updates whether configuration loading and validation completed.
    pub fn set_config_loaded(&self, loaded: bool) {
        self.inner.config_loaded.store(loaded, Ordering::SeqCst);
    }

    /// Updates whether the main wire protocol listener is bound.
    pub fn set_listener_bound(&self, bound: bool) {
        self.inner.listener_bound.store(bound, Ordering::SeqCst);
    }

    /// Updates whether cluster configuration is present.
    pub fn set_cluster_configured(&self, configured: bool) {
        self.inner
            .cluster_configured
            .store(configured, Ordering::SeqCst);
        if !configured {
            self.set_cluster_membership_established(false);
        }
    }

    /// Updates whether clustered membership is established.
    pub fn set_cluster_membership_established(&self, established: bool) {
        self.inner
            .cluster_membership_established
            .store(established, Ordering::SeqCst);
    }
}

impl Default for SharedReadinessState {
    fn default() -> Self {
        Self::new(ReadinessState::default())
    }
}

#[derive(Debug)]
struct ReadinessFlags {
    config_loaded: AtomicBool,
    listener_bound: AtomicBool,
    cluster_configured: AtomicBool,
    cluster_membership_established: AtomicBool,
    /// Installed once, after the connection supervisor exists (P0 #56).
    ///
    /// A `OnceLock` rather than an `AtomicBool` because the authoritative flag
    /// is OWNED by the incarnation authority — readiness reads it, it does not
    /// keep its own copy that something would have to remember to update. An
    /// uninstalled source reads as available, which is correct for a server
    /// whose supervisor is not built yet: the health endpoint binds before the
    /// supervisor exists so that liveness is answerable during startup.
    admission: OnceLock<AdmissionReadiness>,
}

impl ReadinessFlags {
    const fn from_state(state: ReadinessState) -> Self {
        let (cluster_configured, cluster_membership_established) = match state.cluster {
            ClusterReadiness::NotConfigured => (false, false),
            ClusterReadiness::Configured {
                membership_established,
            } => (true, membership_established),
        };

        Self {
            config_loaded: AtomicBool::new(state.config_loaded),
            listener_bound: AtomicBool::new(state.listener_bound),
            cluster_configured: AtomicBool::new(cluster_configured),
            cluster_membership_established: AtomicBool::new(cluster_membership_established),
            admission: OnceLock::new(),
        }
    }
}

/// Evaluates whether the server has completed every applicable startup gate.
#[must_use]
pub fn readiness_check(state: &ReadinessState) -> ReadinessStatus {
    let mut unmet_conditions = Vec::new();

    if !state.config_loaded {
        unmet_conditions.push(ReadinessCondition::ConfigLoaded);
    }

    if !state.listener_bound {
        unmet_conditions.push(ReadinessCondition::ListenerBound);
    }

    if state.cluster
        == (ClusterReadiness::Configured {
            membership_established: false,
        })
    {
        unmet_conditions.push(ReadinessCondition::ClusterMembershipEstablished);
    }

    // P0 #56. Last in evaluation order because it is the only condition that can
    // go unmet AFTER a successful startup: the other three are startup gates
    // that, once met, stay met.
    if !state.admission_available {
        unmet_conditions.push(ReadinessCondition::AdmissionAvailable);
    }

    ReadinessStatus::from_unmet_conditions(unmet_conditions)
}

#[cfg(test)]
mod tests {
    use super::{
        ClusterReadiness, HealthState, ReadinessCondition, ReadinessState, SharedReadinessState,
        health_check, readiness_check,
    };

    #[test]
    fn health_check_is_always_healthy_liveness() {
        let status = health_check();

        assert_eq!(status.status, HealthState::Healthy);
        assert!(status.message.is_none());
    }

    #[test]
    fn readiness_reports_missing_config() {
        let state = ReadinessState::new(false, true, ClusterReadiness::NotConfigured);

        let status = readiness_check(&state);

        assert!(!status.ready);
        assert_eq!(
            status.unmet_conditions,
            vec![ReadinessCondition::ConfigLoaded]
        );
    }

    #[test]
    fn readiness_reports_missing_listener() {
        let state = ReadinessState::new(true, false, ClusterReadiness::NotConfigured);

        let status = readiness_check(&state);

        assert!(!status.ready);
        assert_eq!(
            status.unmet_conditions,
            vec![ReadinessCondition::ListenerBound]
        );
    }

    #[test]
    fn readiness_requires_cluster_membership_when_configured() {
        let state = ReadinessState::new(
            true,
            true,
            ClusterReadiness::Configured {
                membership_established: false,
            },
        );

        let status = readiness_check(&state);

        assert!(!status.ready);
        assert_eq!(
            status.unmet_conditions,
            vec![ReadinessCondition::ClusterMembershipEstablished]
        );
    }

    #[test]
    fn readiness_ignores_cluster_membership_when_not_configured() {
        let state = ReadinessState::ready_without_cluster();

        let status = readiness_check(&state);

        assert!(status.ready);
        assert!(status.unmet_conditions.is_empty());
    }

    #[test]
    fn readiness_is_ready_only_when_all_applicable_conditions_are_met() {
        let state = ReadinessState::ready_with_cluster();

        let status = readiness_check(&state);

        assert!(status.ready);
        assert!(status.unmet_conditions.is_empty());
    }

    #[test]
    fn shared_readiness_state_snapshots_updates() {
        let shared = SharedReadinessState::default();
        shared.set_config_loaded(true);
        shared.set_listener_bound(true);
        shared.set_cluster_configured(true);
        shared.set_cluster_membership_established(true);

        assert_eq!(shared.snapshot(), ReadinessState::ready_with_cluster());
    }
}
