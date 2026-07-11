use std::collections::HashMap;
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError, channel};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use beamr::atom::{Atom, AtomTable};
use beamr::module::ModuleRegistry;
use beamr::native::native_process::NativeHandlerFactory;
use beamr::process::ExitReason;
use beamr::scheduler::{Scheduler, SchedulerConfig};

use liminal::protocol::WorkerRegistration;

use super::notifier::ConnectionNotifier;
use super::process::ConnectionProcess;
use super::services::{
    ConnectionServices, LiminalConnectionServices, ProductionSubsystems, SubsystemFactory,
    build_connection_services_via,
};
use crate::ServerError;
use crate::config::types::{LimitsConfig, ServerConfig};

const CONNECTION_SCHEDULER_THREADS: usize = 4;
const CONNECTION_SHUTDOWN_CONTROL_ATOM: &str = "liminal_server_connection_shutdown_control";
/// R6 (§1.2(4)): the single `READY` wake vocabulary for a connection. One atom;
/// any marker (or N coalesced) triggers one full slice servicing all sources.
const CONNECTION_READY_ATOM: &str = "liminal_server_connection_ready";

#[cfg(test)]
#[path = "supervisor_tests.rs"]
mod tests;

/// Supervisor that owns the beamr scheduler for per-connection processes.
#[derive(Clone, Debug)]
pub struct ConnectionSupervisor {
    inner: Arc<SupervisorInner>,
}

impl ConnectionSupervisor {
    /// Creates a connection supervisor backed by the services the config's
    /// `[services]` profile selects: the full liminal channel/conversation stack
    /// (the default) or the capability-scoped worker front door. Profile
    /// enforcement is [`build_connection_services`](super::services::build_connection_services)'s,
    /// so this constructor can never build full services for a worker-front-door
    /// config.
    ///
    /// # Errors
    /// Returns [`ServerError`] when service construction or scheduler startup fails.
    pub fn from_config(config: &ServerConfig) -> Result<Self, ServerError> {
        Self::from_config_via(config, &ProductionSubsystems)
    }

    /// [`Self::from_config`] with the §9 D2 subsystem factory injected.
    ///
    /// The factory is the only route to every scheduler-owning subsystem the
    /// services construction builds, so a recording factory observes exactly what
    /// was constructed; the connection scheduler itself (built below for BOTH
    /// profiles) is the census baseline, not a census entry.
    fn from_config_via(
        config: &ServerConfig,
        subsystems: &dyn SubsystemFactory,
    ) -> Result<Self, ServerError> {
        let services = build_connection_services_via(config, subsystems)?;
        // The configured token (if any) is carried opaquely as bytes for a
        // constant-time comparison against the handshake's `auth_token`. Absent
        // `[auth]` leaves it `None`, so the connection stays open-access.
        let auth_token = config
            .auth
            .as_ref()
            .map(|auth| auth.token.clone().into_bytes());
        SupervisorInner::new(services, None, auth_token, config.limits).map(|inner| Self {
            inner: Arc::new(inner),
        })
    }

    /// Creates a connection supervisor with no configured channels.
    ///
    /// # Errors
    /// Returns [`ServerError`] when scheduler startup fails.
    pub fn new() -> Result<Self, ServerError> {
        Self::with_services(Arc::new(LiminalConnectionServices::empty()?))
    }

    /// Creates a connection supervisor using an explicit service adapter.
    ///
    /// # Errors
    /// Returns [`ServerError`] when scheduler startup fails.
    pub fn with_services(services: Arc<dyn ConnectionServices>) -> Result<Self, ServerError> {
        SupervisorInner::new(services, None, None, LimitsConfig::default()).map(|inner| Self {
            inner: Arc::new(inner),
        })
    }

    /// Creates a connection supervisor with an explicit service adapter and the
    /// configured connection auth token.
    ///
    /// This is the production constructor for callers that build services
    /// themselves (the runtime needs the shared channel cluster before the
    /// supervisor takes ownership) and therefore cannot use
    /// [`Self::from_config`]: without it the configured `[auth]` token would be
    /// silently dropped and the server would run open-access.
    ///
    /// # Errors
    /// Returns [`ServerError`] when scheduler startup fails.
    pub fn with_services_and_auth(
        services: Arc<dyn ConnectionServices>,
        auth_token: Option<Vec<u8>>,
    ) -> Result<Self, ServerError> {
        SupervisorInner::new(services, None, auth_token, LimitsConfig::default()).map(|inner| {
            Self {
                inner: Arc::new(inner),
            }
        })
    }

    /// Creates a connection supervisor with an explicit service adapter and a
    /// connection-keyed worker-registration notifier.
    ///
    /// The `notifier` is invoked when a worker registers on a connection and when
    /// such a connection closes. Supervisors built via [`Self::with_services`],
    /// [`Self::from_config`], or [`Self::new`] carry no notifier, so liminal still
    /// runs standalone; a `WorkerRegister` frame is then accepted without any
    /// application callback.
    ///
    /// # Errors
    /// Returns [`ServerError`] when scheduler startup fails.
    pub fn with_services_and_notifier(
        services: Arc<dyn ConnectionServices>,
        notifier: Arc<dyn ConnectionNotifier>,
    ) -> Result<Self, ServerError> {
        SupervisorInner::new(services, Some(notifier), None, LimitsConfig::default()).map(|inner| {
            Self {
                inner: Arc::new(inner),
            }
        })
    }

    /// Spawns one supervised beamr process that owns `stream`.
    ///
    /// # Errors
    /// Returns [`ServerError`] when stream configuration or beamr spawn fails.
    pub fn spawn_connection(&self, stream: TcpStream) -> Result<ConnectionHandle, ServerError> {
        self.inner.spawn_connection(stream)
    }

    /// Returns the underlying beamr scheduler.
    #[must_use]
    pub fn scheduler(&self) -> Arc<Scheduler> {
        Arc::clone(&self.inner.scheduler)
    }

    /// Reaps connection processes that have exited outside the normal handler path.
    #[must_use]
    pub fn reap_crashed_connections(&self) -> usize {
        self.inner.runtime.reap_crashed(&self.inner.scheduler)
    }

    /// Returns true when `pid` is still tracked by the supervisor.
    #[must_use]
    pub fn is_tracked(&self, pid: u64) -> bool {
        self.inner.runtime.contains(pid)
    }

    /// Returns the number of tracked live connections.
    #[must_use]
    pub fn active_connection_count(&self) -> usize {
        self.inner.runtime.active_count()
    }

    /// Returns the beamr process ids of the currently tracked live connections.
    ///
    /// Useful for addressing a specific connection — e.g. as the `pid` argument to
    /// [`push_to_connection`](Self::push_to_connection) when the caller knows there
    /// is a single connected client.
    #[must_use]
    pub fn active_connection_pids(&self) -> Vec<u64> {
        self.inner
            .runtime
            .active_connections()
            .into_iter()
            .map(|connection| connection.pid)
            .collect()
    }

    /// Broadcasts a best-effort shutdown notification to active connections.
    ///
    /// Connections with no active subscriptions ignore the notification. Failures
    /// to enqueue the control message are logged and skipped; they are not retried.
    pub fn notify_shutdown_subscribers(&self) {
        self.inner
            .broadcast_control(&ConnectionControl::NotifyShutdown);
    }

    /// Sends a force-close control message to every tracked connection process.
    ///
    /// Each live process attempts one shutdown notification before closing its
    /// stream and exiting normally. Enqueue failures are logged and skipped.
    pub fn force_close_active_connections(&self) {
        for connection in self.inner.runtime.active_connections() {
            tracing::warn!(
                connection_pid = connection.pid,
                peer_addr = ?connection.peer_addr,
                "forcefully closing connection after drain timeout"
            );
            if !self
                .inner
                .enqueue_control(connection.pid, ConnectionControl::ForceClose)
            {
                tracing::warn!(
                    connection_pid = connection.pid,
                    peer_addr = ?connection.peer_addr,
                    "failed to request forceful connection close; process is not live"
                );
            }
        }
    }

    /// Pushes an opaque payload to a specific connected client over that client's
    /// existing connection and returns an awaiter for the client's correlated reply.
    ///
    /// This is the server-initiated leg (server-to-client), the inverse of every
    /// other request frame. It allocates a correlation id, registers a one-shot
    /// reply slot keyed by that id, and enqueues a [`ConnectionControl::Push`] for
    /// the connection process owning `pid`; that process writes a [`Frame::Push`]
    /// out on its socket. When the client answers with a `PushReply` carrying the
    /// same correlation id, the connection process resolves the awaiter's slot. The
    /// returned [`PushReplyAwaiter`] blocks (bounded) for that reply.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the correlation id cannot be allocated, the
    /// reply slot cannot be registered, or the control message cannot be enqueued
    /// for the (possibly already-gone) connection process.
    pub fn push_to_connection(
        &self,
        pid: u64,
        payload: Vec<u8>,
    ) -> Result<PushReplyAwaiter, ServerError> {
        let correlation_id = self.inner.runtime.next_push_correlation_id();
        let receiver = self.inner.runtime.register_push(pid, correlation_id)?;
        let control = ConnectionControl::Push {
            correlation_id,
            payload,
        };
        if self.inner.enqueue_control(pid, control) {
            Ok(PushReplyAwaiter {
                correlation_id,
                receiver,
                runtime: Arc::downgrade(&self.inner.runtime),
            })
        } else {
            // The process is gone; drop the now-unreachable reply slot so it cannot
            // leak in the correlation registry.
            self.inner.runtime.cancel_push(correlation_id);
            Err(ServerError::ListenerAccept {
                message: format!("cannot push to connection process {pid}: process is not live"),
            })
        }
    }

    /// Flushes durable channel state through the configured liminal services.
    ///
    /// # Errors
    /// Returns [`ServerError::ShutdownFlush`] when the underlying service flush fails.
    pub fn flush_durable_state(&self) -> Result<(), ServerError> {
        self.inner.runtime.services().flush_durable_state()
    }

    /// Stops the beamr scheduler used by connection processes.
    pub fn shutdown(&self) {
        self.inner.scheduler.shutdown();
    }

    /// R7 test instrument: slices serviced by connection `pid` since spawn.
    #[cfg(test)]
    pub(super) fn slice_count(&self, pid: u64) -> u64 {
        self.inner.runtime.slice_count(pid)
    }

    /// R6 test seam: a [`ReadyWaker`](super::wake::ReadyWaker) for `pid` — the same
    /// handle a subscription-inbox or reply-availability notifier fires.
    #[cfg(test)]
    pub(super) fn ready_waker(&self, pid: u64) -> Option<super::wake::ReadyWaker> {
        self.inner.runtime.ready_waker(pid)
    }
}

/// Handle for one supervised connection process.
#[derive(Clone, Debug)]
pub struct ConnectionHandle {
    pid: u64,
    peer_addr: Option<SocketAddr>,
    supervisor: Arc<SupervisorInner>,
}

impl ConnectionHandle {
    /// Returns the beamr process id for this connection.
    #[must_use]
    pub const fn pid(&self) -> u64 {
        self.pid
    }

    /// Returns the peer address if it was available from the accepted stream.
    #[must_use]
    pub const fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer_addr
    }

    /// Returns whether the beamr process is still live.
    #[must_use]
    pub fn is_live(&self) -> bool {
        self.supervisor
            .scheduler
            .process_table()
            .get(self.pid)
            .is_some()
    }

    /// Requests an error exit for tests and supervisor control paths.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the process is no longer live.
    pub fn request_crash(&self) -> Result<(), ServerError> {
        if self
            .supervisor
            .scheduler
            .enqueue_atom_message(self.pid, Atom::ERROR)
        {
            Ok(())
        } else {
            Err(ServerError::ListenerAccept {
                message: format!("connection process {} is not live", self.pid),
            })
        }
    }
}

/// Awaits the correlated reply to a single server-initiated push.
///
/// Returned by [`ConnectionSupervisor::push_to_connection`]. The reply slot is
/// resolved when the originating connection process receives a `PushReply` frame
/// carrying the same correlation id, so [`PushReplyAwaiter::receive`] blocks
/// (bounded) for that one correlated answer.
#[derive(Debug)]
pub struct PushReplyAwaiter {
    correlation_id: u64,
    receiver: Receiver<Vec<u8>>,
    /// Weak handle to the owning runtime, used to cancel this awaiter's reserved
    /// reply slot the moment its deadline passes so a timed-out push does not leak
    /// the slot until connection close. `Weak` so the awaiter never keeps the
    /// runtime alive; if it is already gone, the slot is gone with it.
    runtime: Weak<ConnectionRuntime>,
}

impl PushReplyAwaiter {
    /// Returns the correlation id this awaiter is matched on.
    #[must_use]
    pub const fn correlation_id(&self) -> u64 {
        self.correlation_id
    }

    /// Blocks up to `timeout` for the client's correlated reply payload.
    ///
    /// # Errors
    /// Returns [`ServerError::PushReplyTimeout`] when no reply arrives within
    /// `timeout` (the worker is connected but slow), or
    /// [`ServerError::PushReplyDisconnected`] when the connection process dropped
    /// the reply slot (the connection closed — the prompt worker-death signal).
    /// The two are distinct variants so callers classify by type, not message.
    pub fn receive(&self, timeout: Duration) -> Result<Vec<u8>, ServerError> {
        match self.receiver.recv_timeout(timeout) {
            Ok(payload) => Ok(payload),
            Err(RecvTimeoutError::Timeout) => self.resolve_timeout(),
            Err(RecvTimeoutError::Disconnected) => Err(ServerError::PushReplyDisconnected {
                correlation_id: self.correlation_id,
            }),
        }
    }

    /// Settles a deadline expiry against the slot's atomic resolved-vs-cancelled
    /// transition (removal under the registry mutex). Winning the transition
    /// frees the reserved slot immediately — a timed-out push must not hold its
    /// slot until connection close — and any later reply finds no slot and is
    /// discarded. Losing it means the resolver already delivered (its send
    /// happens under the same lock, so the payload is present now — return it
    /// rather than a timeout for an answer that arrived) or the connection
    /// closed (sender dropped, disconnected).
    fn resolve_timeout(&self) -> Result<Vec<u8>, ServerError> {
        let timeout_error = || ServerError::PushReplyTimeout {
            correlation_id: self.correlation_id,
        };
        let Some(runtime) = self.runtime.upgrade() else {
            // The runtime is gone, and the slot map with it.
            return Err(timeout_error());
        };
        if runtime.cancel_push(self.correlation_id) {
            return Err(timeout_error());
        }
        match self.receiver.try_recv() {
            Ok(payload) => Ok(payload),
            Err(TryRecvError::Disconnected) => Err(ServerError::PushReplyDisconnected {
                correlation_id: self.correlation_id,
            }),
            // Unreachable by construction (a removed slot either sent under the
            // lock or dropped its sender); honest fallback rather than a panic.
            Err(TryRecvError::Empty) => Err(timeout_error()),
        }
    }
}

pub(super) struct SupervisorInner {
    scheduler: Arc<Scheduler>,
    runtime: Arc<ConnectionRuntime>,
}

impl std::fmt::Debug for SupervisorInner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SupervisorInner")
            .field("runtime", &self.runtime)
            .finish_non_exhaustive()
    }
}

impl SupervisorInner {
    fn new(
        services: Arc<dyn ConnectionServices>,
        notifier: Option<Arc<dyn ConnectionNotifier>>,
        auth_token: Option<Vec<u8>>,
        limits: LimitsConfig,
    ) -> Result<Self, ServerError> {
        let atoms = AtomTable::with_common_atoms();
        let control_atom = atoms.intern(CONNECTION_SHUTDOWN_CONTROL_ATOM);
        let registry = Arc::new(ModuleRegistry::new());

        let scheduler = Scheduler::new(
            SchedulerConfig {
                thread_count: Some(CONNECTION_SCHEDULER_THREADS),
                ..SchedulerConfig::default()
            },
            registry,
        )
        .map_err(|message| ServerError::ListenerAccept {
            message: format!("failed to start connection scheduler: {message}"),
        })?;
        let ready_atom = atoms.intern(CONNECTION_READY_ATOM);
        let scheduler = Arc::new(scheduler);
        // The runtime captures a WEAK handle to the connection scheduler so
        // notifier wakes (R3/R1(vi)) can be fired from another actor's slice
        // without a strong scheduler↔process↔runtime cycle that would leak the
        // whole connection scheduler.
        Ok(Self {
            runtime: Arc::new(ConnectionRuntime::new(
                services,
                control_atom,
                ready_atom,
                Arc::downgrade(&scheduler),
                notifier,
                auth_token,
                limits,
            )),
            scheduler,
        })
    }

    fn spawn_connection(
        self: &Arc<Self>,
        stream: TcpStream,
    ) -> Result<ConnectionHandle, ServerError> {
        // §5 `max_connections`: ATOMIC admission reservation acquired BEFORE any
        // process construction (review round 1 item 7 — a signed bound must not
        // be exceedable by concurrent callers; check-then-spawn across an
        // unlocked window was). The CAS reservation is released on every failure
        // path below and converts into the connection record at `register`;
        // thereafter the single record-removal path (`remove`) releases it. An
        // over-cap accept therefore costs nothing and the bound holds under any
        // concurrency.
        self.runtime.try_reserve_admission()?;
        let reservation = AdmissionReservation {
            runtime: &self.runtime,
            armed: true,
        };
        stream
            .set_nonblocking(true)
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("failed to configure connection stream: {error}"),
            })?;
        let peer_addr = stream.peer_addr().ok();
        let holder = Arc::new(Mutex::new(Some(stream)));
        let runtime = Arc::clone(&self.runtime);
        let process_holder = Arc::clone(&holder);
        let factory: NativeHandlerFactory = Box::new(move || {
            Box::new(ConnectionProcess::from_holder(
                Arc::clone(&runtime),
                peer_addr,
                &process_holder,
            ))
        });
        let pid =
            self.scheduler
                .spawn_native(factory)
                .map_err(|error| ServerError::ListenerAccept {
                    message: format!("failed to spawn connection process: {error}"),
                })?;
        self.runtime.register(pid, peer_addr)?;
        // The reservation is now owned by the registered record: `remove` (the
        // single record-removal path — finish/mark_crashed/reap all funnel
        // through it) releases the admission when the record goes away.
        reservation.convert();
        Ok(ConnectionHandle {
            pid,
            peer_addr,
            supervisor: Arc::clone(self),
        })
    }

    fn broadcast_control(&self, control: &ConnectionControl) {
        for connection in self.runtime.active_connections() {
            if !self.enqueue_control(connection.pid, control.clone()) {
                tracing::debug!(
                    connection_pid = connection.pid,
                    peer_addr = ?connection.peer_addr,
                    ?control,
                    "connection control message skipped because process is not live"
                );
            }
        }
    }

    fn enqueue_control(&self, pid: u64, control: ConnectionControl) -> bool {
        // Keep a key for the failure-path removal before the control is moved into
        // the queue, so a non-`Copy` (push) control can still be located and pulled
        // back out if the scheduler wakeup fails.
        let removal_key = control.clone();
        if self.runtime.push_control(pid, control).is_err() {
            return false;
        }
        if self
            .scheduler
            .enqueue_atom_message(pid, self.runtime.control_atom())
        {
            true
        } else {
            self.runtime.remove_control(pid, &removal_key);
            false
        }
    }
}

/// RAII guard for one §5 `max_connections` admission reservation.
///
/// Acquired (via [`ConnectionRuntime::try_reserve_admission`]) before any process
/// construction in `spawn_connection`; every early-return failure path releases
/// it through `Drop`, and a successful `register` converts it into the
/// connection record (whose removal releases the admission instead). RAII means
/// no failure path — present or future — can leak a reservation.
struct AdmissionReservation<'a> {
    runtime: &'a ConnectionRuntime,
    armed: bool,
}

impl AdmissionReservation<'_> {
    /// Converts the reservation into record ownership: `Drop` no longer releases
    /// it, because the registered record's removal will.
    fn convert(mut self) {
        self.armed = false;
    }
}

impl Drop for AdmissionReservation<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.runtime.release_admission();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ConnectionControl {
    NotifyShutdown,
    ForceClose,
    /// Server-initiated push of an opaque payload, correlated by `correlation_id`,
    /// to be written out as a [`Frame::Push`] by the receiving connection process.
    Push {
        correlation_id: u64,
        payload: Vec<u8>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveConnection {
    pid: u64,
    peer_addr: Option<SocketAddr>,
}

#[derive(Debug)]
pub(super) struct ConnectionRuntime {
    services: Arc<dyn ConnectionServices>,
    records: Mutex<HashMap<u64, ConnectionRecord>>,
    controls: Mutex<Vec<QueuedConnectionControl>>,
    control_atom: Atom,
    /// R6 single `READY` wake atom for this connection scheduler. Fired by every
    /// wake source's notifier (R3/R1(vi)); coalescing and duplicates are harmless.
    ready_atom: Atom,
    /// Weak handle to the connection scheduler, used to build [`ReadyWaker`]s a
    /// notifier fires from another actor's slice. Weak so it never keeps the
    /// scheduler alive (the scheduler owns the processes that own this runtime).
    scheduler: Weak<Scheduler>,
    /// R7 (§1.2(6)) test-only per-connection slice counter, keyed by pid. Bumped
    /// once at the head of every serviced slice. The park-flip's permanent rule-1
    /// assertion (a parked connection's counter must not advance without an event)
    /// reads this; the instrument lands now with a test proving it counts slices.
    #[cfg(test)]
    slice_counts: Mutex<HashMap<u64, u64>>,
    /// One-shot reply slots for in-flight server pushes, keyed by correlation id.
    /// The supervisor registers a slot in `push_to_connection`; the connection
    /// process resolves it when the matching `PushReply` frame arrives. Each slot
    /// records the owning connection pid so the close path can drop a connection's
    /// outstanding slots and wake their awaiters with a prompt disconnected error.
    push_replies: Mutex<HashMap<u64, PendingPush>>,
    /// Monotonic source of push correlation ids. Server-allocated, so it never
    /// collides with a client-chosen id on this connection.
    next_push_id: AtomicU64,
    /// §5 `max_connections` admission counter. Incremented atomically (CAS
    /// against the limit) BEFORE a connection process is constructed and
    /// decremented on every spawn-failure path and on final record removal, so
    /// the signed bound holds under concurrent spawns — admission is never
    /// derived from the records-map length across an unlocked window.
    admissions: AtomicU64,
    /// Optional application hook invoked on worker registration and on the close
    /// of a connection that had registered. `None` keeps liminal standalone: a
    /// `WorkerRegister` is accepted with no callback.
    notifier: Option<Arc<dyn ConnectionNotifier>>,
    /// Configured connection auth token (the `[auth]` section's token as opaque
    /// bytes). `Some` gates the `Connect` handshake — the frame's `auth_token` must
    /// match under a constant-time comparison; `None` leaves the server open-access,
    /// byte-identical to the pre-auth behaviour.
    auth_token: Option<Vec<u8>>,
    /// Operational caps (§5). Enforced with typed refusals at admission:
    /// per-connection subscription, conversation, push, and pending-reply counts,
    /// plus the shared inbox byte budget. Non-config constructors carry the signed
    /// defaults ([`LimitsConfig::default`]).
    limits: LimitsConfig,
}

impl ConnectionRuntime {
    fn new(
        services: Arc<dyn ConnectionServices>,
        control_atom: Atom,
        ready_atom: Atom,
        scheduler: Weak<Scheduler>,
        notifier: Option<Arc<dyn ConnectionNotifier>>,
        auth_token: Option<Vec<u8>>,
        limits: LimitsConfig,
    ) -> Self {
        Self {
            services,
            records: Mutex::new(HashMap::new()),
            controls: Mutex::new(Vec::new()),
            control_atom,
            ready_atom,
            scheduler,
            #[cfg(test)]
            slice_counts: Mutex::new(HashMap::new()),
            push_replies: Mutex::new(HashMap::new()),
            next_push_id: AtomicU64::new(1),
            admissions: AtomicU64::new(0),
            notifier,
            auth_token,
            limits,
        }
    }

    /// Atomically reserves one §5 `max_connections` admission slot: a CAS loop
    /// against the configured limit, so N concurrent callers racing for the last
    /// slot admit EXACTLY one — the bound cannot be transiently exceeded.
    ///
    /// # Errors
    /// Returns [`ServerError::ConnectionLimitReached`] when every slot is taken.
    fn try_reserve_admission(&self) -> Result<(), ServerError> {
        let limit = self.limits.max_connections as u64;
        let mut current = self.admissions.load(Ordering::Acquire);
        loop {
            if current >= limit {
                return Err(ServerError::ConnectionLimitReached {
                    limit: self.limits.max_connections,
                });
            }
            match self.admissions.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }

    /// Releases one admission slot. Called by the spawn failure paths (via the
    /// [`AdmissionReservation`] guard) and by [`Self::remove`] when a registered
    /// record is removed — exactly one release per reservation. Saturating so a
    /// spurious release can never wrap the counter.
    fn release_admission(&self) {
        let mut current = self.admissions.load(Ordering::Acquire);
        loop {
            let next = current.saturating_sub(1);
            match self.admissions.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    /// The operational caps (§5) this runtime enforces.
    pub(super) const fn limits(&self) -> &LimitsConfig {
        &self.limits
    }

    /// The connection's single R6 `READY` wake atom.
    pub(super) const fn ready_atom(&self) -> Atom {
        self.ready_atom
    }

    /// Builds a [`ReadyWaker`] targeting `pid` on the connection scheduler, if the
    /// scheduler is still live. `None` when the scheduler is gone (teardown) or in
    /// scheduler-free unit tests — a notifier with no waker simply never wakes,
    /// which under the busy loop is redundant anyway (the every-slice pump still
    /// services the source). This is the seam every wake source installs its
    /// notifier through (R3/R1(vi)).
    pub(super) fn ready_waker(&self, pid: u64) -> Option<super::wake::ReadyWaker> {
        let scheduler = self.scheduler.upgrade()?;
        Some(super::wake::ReadyWaker::new(
            &scheduler,
            pid,
            self.ready_atom,
        ))
    }

    /// R7: records one serviced slice for `pid`. Bumped at the head of every
    /// slice; the park-flip's quiescence assertion reads [`Self::slice_count`].
    #[cfg(test)]
    pub(super) fn record_slice(&self, pid: u64) {
        if let Ok(mut counts) = self.slice_counts.lock() {
            *counts.entry(pid).or_insert(0) += 1;
        }
    }

    /// R7: slices serviced by connection `pid` since spawn (test instrument).
    #[cfg(test)]
    pub(super) fn slice_count(&self, pid: u64) -> u64 {
        self.slice_counts
            .lock()
            .map_or(0, |counts| counts.get(&pid).copied().unwrap_or(0))
    }

    /// Builds a runtime wrapping `services` for unit tests that exercise
    /// `apply_frame` without a live scheduler. Uses a fresh interned control atom
    /// and no notifier.
    #[cfg(test)]
    pub(super) fn for_tests(services: Arc<dyn ConnectionServices>) -> Self {
        let atoms = AtomTable::with_common_atoms();
        let control_atom = atoms.intern(CONNECTION_SHUTDOWN_CONTROL_ATOM);
        let ready_atom = atoms.intern(CONNECTION_READY_ATOM);
        Self::new(
            services,
            control_atom,
            ready_atom,
            Weak::new(),
            None,
            None,
            LimitsConfig::default(),
        )
    }

    /// Builds a runtime wrapping `services` with explicit `limits` for unit tests
    /// that exercise the §5 admission caps without a live scheduler.
    #[cfg(test)]
    pub(super) fn for_tests_with_limits(
        services: Arc<dyn ConnectionServices>,
        limits: LimitsConfig,
    ) -> Self {
        let atoms = AtomTable::with_common_atoms();
        let control_atom = atoms.intern(CONNECTION_SHUTDOWN_CONTROL_ATOM);
        let ready_atom = atoms.intern(CONNECTION_READY_ATOM);
        Self::new(
            services,
            control_atom,
            ready_atom,
            Weak::new(),
            None,
            None,
            limits,
        )
    }

    /// Builds a runtime wrapping `services` with a configured auth `token` for unit
    /// tests that exercise the `Connect` handshake enforcement without a live
    /// scheduler. Uses a fresh interned control atom and no notifier.
    #[cfg(test)]
    pub(super) fn for_tests_with_auth_token(
        services: Arc<dyn ConnectionServices>,
        token: Vec<u8>,
    ) -> Self {
        let atoms = AtomTable::with_common_atoms();
        let control_atom = atoms.intern(CONNECTION_SHUTDOWN_CONTROL_ATOM);
        let ready_atom = atoms.intern(CONNECTION_READY_ATOM);
        Self::new(
            services,
            control_atom,
            ready_atom,
            Weak::new(),
            None,
            Some(token),
            LimitsConfig::default(),
        )
    }

    /// Builds a runtime wrapping `services` with a `notifier` for unit tests that
    /// exercise `apply_frame` and the close path without a live scheduler.
    #[cfg(test)]
    pub(super) fn for_tests_with_notifier(
        services: Arc<dyn ConnectionServices>,
        notifier: Arc<dyn ConnectionNotifier>,
    ) -> Self {
        let atoms = AtomTable::with_common_atoms();
        let control_atom = atoms.intern(CONNECTION_SHUTDOWN_CONTROL_ATOM);
        let ready_atom = atoms.intern(CONNECTION_READY_ATOM);
        Self::new(
            services,
            control_atom,
            ready_atom,
            Weak::new(),
            Some(notifier),
            None,
            LimitsConfig::default(),
        )
    }

    pub(super) fn services(&self) -> &dyn ConnectionServices {
        self.services.as_ref()
    }

    /// Returns the configured connection auth token as opaque bytes, or `None` when
    /// no `[auth]` section was configured (open access).
    pub(super) fn auth_token(&self) -> Option<&[u8]> {
        self.auth_token.as_deref()
    }

    /// Returns the configured connection-keyed notifier, if any.
    pub(super) fn notifier(&self) -> Option<&Arc<dyn ConnectionNotifier>> {
        self.notifier.as_ref()
    }

    /// Offers a channel publish to the notifier's observability-drain tap, returning
    /// `true` when the application consumed it (so the connection process skips the
    /// normal fan-out). `false` when no notifier is installed (liminal standalone) or
    /// the notifier did not recognise the channel, so the caller can invoke it
    /// unconditionally and fall through to the normal publish path.
    pub(super) fn notifier_channel_publish(&self, pid: u64, channel: &str, payload: &[u8]) -> bool {
        self.notifier
            .as_ref()
            .is_some_and(|notifier| notifier.on_channel_publish(pid, channel, payload))
    }

    /// Stores `registration` on the connection record for `pid`, so the close
    /// path can later fire `on_worker_unregistered` for exactly the connections
    /// that registered. A missing record (the connection already closed) is a
    /// no-op.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the connection registry mutex is poisoned.
    pub(super) fn set_registration(
        &self,
        pid: u64,
        registration: WorkerRegistration,
    ) -> Result<(), ServerError> {
        if let Some(record) = lock(&self.records, "connection registry")?.get_mut(&pid) {
            record.registration = Some(registration);
        }
        Ok(())
    }

    /// Allocates the next monotonic push correlation id.
    fn next_push_correlation_id(&self) -> u64 {
        self.next_push_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Registers a one-shot reply slot for `correlation_id`, owned by connection
    /// `pid`, and returns its receiver. The connection process resolves the slot
    /// via [`resolve_push`]; the close path drops the connection's outstanding
    /// slots via [`cancel_pushes_for_connection`].
    ///
    /// # Errors
    /// Returns [`ServerError`] when the correlation registry mutex is poisoned.
    fn register_push(
        &self,
        pid: u64,
        correlation_id: u64,
    ) -> Result<Receiver<Vec<u8>>, ServerError> {
        let (sender, receiver) = channel();
        let limit = self.limits.max_pending_pushes_per_connection;
        {
            let mut slots = lock(&self.push_replies, "push correlation registry")?;
            // §5 `max_pending_pushes_per_connection`: refuse a new in-flight push
            // once this connection already holds the cap. Counted per owning pid so
            // one connection cannot exhaust the shared registry; slots free on
            // reply, timeout, or connection close. The count-and-insert stays under
            // the one lock so the cap is enforced atomically.
            let outstanding = slots.values().filter(|pending| pending.pid == pid).count();
            if outstanding >= limit {
                return Err(ServerError::ConnectionCapReached {
                    operation: "server push".to_owned(),
                    cap: "max_pending_pushes_per_connection",
                    limit,
                });
            }
            slots.insert(correlation_id, PendingPush { pid, sender });
        }
        Ok(receiver)
    }

    /// Drops a registered reply slot without resolving it (the push could not be
    /// delivered, or its reply deadline passed). Dropping the slot's `Sender`
    /// wakes a still-waiting awaiter with a disconnected error.
    ///
    /// Returns whether THIS call removed the slot. Removal under the registry
    /// mutex is the atomic resolved-vs-cancelled transition: `false` means
    /// another path won — [`resolve_push`](Self::resolve_push) already sent the
    /// reply (its send happens under the same lock, so the payload is already in
    /// the channel when this returns), or the connection's close path dropped
    /// the slot (sender gone, channel disconnected). The timed-out awaiter
    /// disambiguates the two with a non-blocking receive.
    pub(super) fn cancel_push(&self, correlation_id: u64) -> bool {
        self.push_replies
            .lock()
            .is_ok_and(|mut slots| slots.remove(&correlation_id).is_some())
    }

    /// Drops every reply slot owned by connection `pid`, waking each awaiter with a
    /// disconnected error (the dropped `Sender` disconnects the awaiter's
    /// `Receiver`). Called from the close path so a connection that exits with
    /// in-flight pushes signals worker death immediately instead of leaving each
    /// awaiter to block the full push-reply timeout. A slot that [`resolve_push`]
    /// already removed is gone, so it is untouched here; an unknown pid is a no-op.
    fn cancel_pushes_for_connection(&self, pid: u64) {
        if let Ok(mut slots) = self.push_replies.lock() {
            slots.retain(|_correlation_id, pending| pending.pid != pid);
        }
    }

    /// Number of reserved push reply slots outstanding. A timed-out push must
    /// release its slot, so the D4 gate pins this back to zero after a timeout.
    #[cfg(test)]
    pub(super) fn pending_push_count(&self) -> usize {
        self.push_replies.lock().map_or(0, |slots| slots.len())
    }

    /// Resolves the reply slot for `correlation_id` with the client's reply
    /// payload, waking the [`PushReplyAwaiter`]. Called by the connection process
    /// when a correlated `PushReply` frame arrives. A missing slot (already
    /// resolved, cancelled, or unknown id) is ignored — a reply that lost the
    /// resolved-vs-timed-out transition is discarded, never delivered late.
    pub(super) fn resolve_push(&self, correlation_id: u64, payload: Vec<u8>) {
        let Ok(mut slots) = self.push_replies.lock() else {
            return;
        };
        if let Some(pending) = slots.remove(&correlation_id) {
            // The send stays under the registry lock so removal and delivery are
            // one atomic step: a timed-out awaiter that observes the slot gone
            // (its `cancel_push` returned false) is then GUARANTEED to find the
            // payload already in the channel — without this ordering the awaiter
            // could see the removal, find the channel still empty, and report a
            // timeout for a reply that was about to land. The send itself never
            // blocks (unbounded channel), and a receiver dropped after an
            // abandoned wait makes it a benign discard.
            pending.sender.send(payload).ok();
        }
    }

    pub(super) const fn control_atom(&self) -> Atom {
        self.control_atom
    }

    /// Sole registration path for a connection: the spawn thread inserts the
    /// record synchronously, before `spawn_connection` returns the handle, so
    /// `is_tracked`/`active_connection_count` reflect the connection
    /// immediately. The connection handler never writes the registry (it only
    /// reads via `mark_crashed`/`finish`), so there is a single writer here and
    /// no register/ensure-register race.
    ///
    /// Ordering note: `spawn_native` only enqueues the process, so its first
    /// slice may run on another worker thread before this insert lands. If that
    /// first slice exits immediately (e.g. a missing-stream crash) its
    /// `mark_crashed`/`finish` removes nothing and this insert then leaves a
    /// record for an already-dead pid. That orphan is self-healing:
    /// `reap_crashed`, driven continuously by the listener loop, removes any
    /// record whose pid is absent from the scheduler process table.
    fn register(&self, pid: u64, peer_addr: Option<SocketAddr>) -> Result<(), ServerError> {
        lock(&self.records, "connection registry")?.insert(
            pid,
            ConnectionRecord {
                peer_addr,
                registration: None,
            },
        );
        // Single-writer insert (see doc above) pairs one gauge increment with the
        // decrement in `remove`, keeping `liminal_connections_active` equal to the
        // live record count on every teardown route.
        crate::metrics::connection_spawned();
        Ok(())
    }

    pub(super) fn mark_crashed(&self, pid: u64, reason: ExitReason, peer_addr: Option<SocketAddr>) {
        let removed = self.remove(pid).unwrap_or(ConnectionRecord {
            peer_addr,
            registration: None,
        });
        self.fire_unregistered(pid, &removed);
        tracing::warn!(
            connection_pid = pid,
            peer_addr = ?removed.peer_addr,
            reason = ?reason,
            "connection process crashed"
        );
    }

    pub(super) fn finish(&self, pid: u64) {
        if let Some(removed) = self.remove(pid) {
            self.fire_unregistered(pid, &removed);
        }
    }

    /// Invokes `on_worker_unregistered` for a removed connection record that
    /// carried a worker registration. A record with no registration (a plain
    /// connection, or a worker connection that never registered) is a no-op, so
    /// only connections that actually registered deregister.
    fn fire_unregistered(&self, pid: u64, record: &ConnectionRecord) {
        if record.registration.is_some() {
            if let Some(notifier) = self.notifier.as_ref() {
                notifier.on_worker_unregistered(pid);
            }
        }
    }

    fn reap_crashed(&self, scheduler: &Scheduler) -> usize {
        let pids = match self.records.lock() {
            Ok(records) => records.keys().copied().collect::<Vec<_>>(),
            Err(error) => {
                tracing::warn!(%error, "connection registry unavailable during crash reap");
                return 0;
            }
        };
        let mut reaped = 0;
        for pid in pids {
            if scheduler.process_table().get(pid).is_none() {
                let removed = self.remove(pid);
                if let Some(record) = removed.as_ref() {
                    self.fire_unregistered(pid, record);
                }
                let peer_addr = removed.and_then(|record| record.peer_addr);
                // This process exited without ever reaching `mark_crashed`/`finish`
                // (e.g. the beamr scheduler terminated it externally). beamr records
                // the real `ExitReason` in its private `exit_tombstones` map, but its
                // public `Scheduler` API exposes no non-blocking accessor for it
                // (only `run_until_exit`, which blocks). So we cannot recover the true
                // reason here; log a truthful, specific message rather than the
                // misleading literal "unknown". If beamr later grows a public,
                // non-blocking exit-reason query for a dead pid, read it here instead.
                tracing::warn!(
                    connection_pid = pid,
                    ?peer_addr,
                    reason = "terminated externally (no exit reason recorded by supervisor)",
                    "connection process crashed"
                );
                reaped += 1;
            }
        }
        reaped
    }

    fn contains(&self, pid: u64) -> bool {
        self.records
            .lock()
            .is_ok_and(|records| records.contains_key(&pid))
    }

    fn active_connections(&self) -> Vec<ActiveConnection> {
        self.records.lock().map_or_else(
            |_| Vec::new(),
            |records| {
                records
                    .iter()
                    .map(|(&pid, record)| ActiveConnection {
                        pid,
                        peer_addr: record.peer_addr,
                    })
                    .collect()
            },
        )
    }

    fn push_control(&self, pid: u64, control: ConnectionControl) -> Result<(), ServerError> {
        lock(&self.controls, "connection control queue")?
            .push(QueuedConnectionControl { pid, control });
        Ok(())
    }

    pub(super) fn pop_control(&self, pid: u64) -> Option<ConnectionControl> {
        let mut controls = self.controls.lock().ok()?;
        let index = controls.iter().position(|queued| queued.pid == pid)?;
        Some(controls.remove(index).control)
    }

    fn remove_control(&self, pid: u64, control: &ConnectionControl) {
        let Ok(mut controls) = self.controls.lock() else {
            return;
        };
        let Some(index) = controls
            .iter()
            .position(|queued| queued.pid == pid && &queued.control == control)
        else {
            return;
        };
        controls.remove(index);
    }

    fn active_count(&self) -> usize {
        self.records.lock().map_or(0, |records| records.len())
    }

    /// Removes the connection record for `pid` and, in the same close step, drops
    /// every push reply slot that connection still owns so each waiting
    /// [`PushReplyAwaiter`] wakes immediately with a disconnected error. This runs
    /// on every close route — `finish`, `mark_crashed`, and `reap_crashed` all
    /// remove through here — and fires regardless of whether the connection ever
    /// registered a worker, so a plain push target is covered too.
    fn remove(&self, pid: u64) -> Option<ConnectionRecord> {
        self.cancel_pushes_for_connection(pid);
        let removed = self
            .records
            .lock()
            .ok()
            .and_then(|mut records| records.remove(&pid));
        // Decrement only when a record was actually present so a double-remove
        // (e.g. `finish` after `reap_crashed`) cannot drive the gauge negative.
        // The §5 admission slot is released on the same guard: the reservation
        // acquired in `spawn_connection` converted into this record at
        // `register`, so its removal is exactly one release per reservation.
        if removed.is_some() {
            crate::metrics::connection_closed();
            self.release_admission();
        }
        removed
    }
}

/// One in-flight server-push reply slot, associating the awaiter's reply `sender`
/// with the `pid` of the connection that owns the push. The pid lets the close
/// path drop exactly that connection's slots; the correlation id (the map key)
/// still drives [`ConnectionRuntime::resolve_push`] and
/// [`ConnectionRuntime::cancel_push`].
#[derive(Debug)]
struct PendingPush {
    pid: u64,
    sender: Sender<Vec<u8>>,
}

#[derive(Debug, Clone)]
struct ConnectionRecord {
    peer_addr: Option<SocketAddr>,
    /// Worker registration declared on this connection, set by `set_registration`
    /// when a `WorkerRegister` frame is accepted. `Some` marks a connection whose
    /// close must fire `on_worker_unregistered`.
    registration: Option<WorkerRegistration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueuedConnectionControl {
    pid: u64,
    control: ConnectionControl,
}

fn lock<'a, T>(mutex: &'a Mutex<T>, context: &str) -> Result<MutexGuard<'a, T>, ServerError> {
    mutex.lock().map_err(|error| ServerError::ListenerAccept {
        message: format!("{context} unavailable: {error}"),
    })
}
