use std::collections::HashMap;
use std::net::{SocketAddr, TcpStream};
use std::os::fd::RawFd;
#[cfg(test)]
use std::sync::Barrier;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError, channel};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use beamr::atom::{Atom, AtomTable};
use beamr::module::ModuleRegistry;
use beamr::native::native_process::NativeHandlerFactory;
use beamr::process::ExitReason;
use beamr::scheduler::{ReadinessToken, Scheduler, SchedulerConfig, SchedulerServices};
use beamr::timer::TimerRef;

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
    /// The reply's lifetime belongs to the push, not to any one
    /// [`PushReplyAwaiter::receive`] call: this no-deadline push reserves a slot
    /// that is reclaimed only by (a) the reply being consumed or (b) the
    /// connection closing. An elapsed `receive` poll is a benign re-arm, never a
    /// failure and never a cancellation. The §5
    /// `max_pending_pushes_per_connection` cap bounds abandonment; use
    /// [`push_to_connection_with_deadline`](Self::push_to_connection_with_deadline)
    /// when the reply must have an explicit expiry.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the correlation id cannot be allocated, the
    /// reply slot cannot be registered, or the control message cannot be enqueued
    /// for the (possibly already-gone or concurrently-closing) connection
    /// process. PUBLICATION INVARIANT: an `Err` guarantees no `Push` control was
    /// published — the client never sees a `Push` frame for a failed call.
    /// Conversely `Ok` promises ADMISSION, not delivery: the awaiter's outcome
    /// is the delivery truth (a push admitted just as its connection closes
    /// resolves to the truthful disconnected outcome, never to a lost reply).
    pub fn push_to_connection(
        &self,
        pid: u64,
        payload: Vec<u8>,
    ) -> Result<PushReplyAwaiter, ServerError> {
        self.push_with_deadline(pid, payload, None)
    }

    /// Like [`push_to_connection`](Self::push_to_connection) but attaches an
    /// explicit reply deadline to the reserved slot: `deadline` is a DURATION
    /// FROM NOW bounding the reply's lifetime — a property of THIS push rather
    /// than of any [`PushReplyAwaiter::receive`] wait quantum.
    ///
    /// Deadline expiry is evaluated HOST-SIDE and LAZILY — at the next `receive`
    /// touch, and at connection close at the latest. It never wakes the connection
    /// process, adds no timer thread, and runs no periodic sweeper: a push that is
    /// abandoned and never polled resolves at the next host-side touch (connection
    /// close). At expiry the slot resolves to [`ServerError::PushReplyExpired`],
    /// is removed, and its §5 `max_pending_pushes_per_connection` cap admission is
    /// released. An elapsed `receive` poll BEFORE the deadline is still a benign
    /// re-arm. A `receive` call in flight when the deadline falls due returns
    /// the terminal expiry PROMPTLY — it waits the earlier of its quantum and
    /// the deadline, so a large quantum can never extend the reply's lifetime
    /// and the terminal outcome is quantum-independent.
    ///
    /// The deadline is evaluated at OBSERVATION POINTS, not enforced against the
    /// wall clock: a reply that arrives before expiry is observed is delivered
    /// normally, even if it arrives after the deadline instant. The deadline
    /// bounds waiting and slot occupancy; it is not a delivery-freshness
    /// guarantee. (This is deliberate — a reply is checked for at every
    /// observation point before the deadline is, so an answer in hand always
    /// beats an expiry.)
    ///
    /// # Errors
    /// Returns [`ServerError`] when `deadline` is not representable on the
    /// monotonic clock (an extreme duration is refused, never a panic), the
    /// correlation id cannot be allocated, the reply slot cannot be registered,
    /// or the control message cannot be enqueued for the (possibly already-gone
    /// or concurrently-closing) connection process. PUBLICATION INVARIANT: an
    /// `Err` guarantees no `Push` control was published — the client never sees
    /// a `Push` frame for a failed call. Conversely `Ok` promises ADMISSION,
    /// not delivery: the awaiter's outcome is the delivery truth.
    pub fn push_to_connection_with_deadline(
        &self,
        pid: u64,
        payload: Vec<u8>,
        deadline: Duration,
    ) -> Result<PushReplyAwaiter, ServerError> {
        self.push_with_deadline(pid, payload, Some(deadline))
    }

    /// Shared body for the no-deadline and explicit-deadline push paths. With
    /// `deadline == None` this is byte-for-byte the historical
    /// `push_to_connection` behaviour (no per-slot deadline); with `Some`, the
    /// slot carries an absolute expiry evaluated lazily at `receive`.
    fn push_with_deadline(
        &self,
        pid: u64,
        payload: Vec<u8>,
        deadline: Option<Duration>,
    ) -> Result<PushReplyAwaiter, ServerError> {
        // S5: an extreme `Duration` must surface as this fallible API's typed
        // error, not an `Instant` addition panic. Checked BEFORE any slot is
        // registered so a refused deadline leaves nothing to roll back.
        let deadline_at = match deadline {
            None => None,
            Some(window) => {
                Some(
                    Instant::now()
                        .checked_add(window)
                        .ok_or_else(|| ServerError::ListenerAccept {
                            message: format!(
                                "cannot push to connection process {pid}: reply deadline of {window:?} overflows the monotonic clock"
                            ),
                        })?,
                )
            }
        };
        let correlation_id = self.inner.runtime.next_push_correlation_id();
        let receiver = self
            .inner
            .runtime
            .register_push(pid, correlation_id, deadline_at)?;
        // S3+S7 close-vs-register wall, ordered INSERT -> CONFIRM -> PUBLISH.
        // The confirmation runs BEFORE the control is enqueued, which yields the
        // PUBLICATION INVARIANT: an `Err` from this method guarantees no `Push`
        // control was published — the client never sees a Push for a failed
        // call. (Confirming after the enqueue was S7's non-linearizable race: a
        // close could sweep, the published Push could already be answered and
        // resolved, and the failed confirmation then returned `Err` for a push
        // the client had received.) A close landing AFTER a successful confirm
        // linearizes after push admission: the enqueue either fails (process
        // gone — rollback below, `Err` truthful, nothing delivered) or succeeds
        // with the slot already swept, and the awaiter then reads the truthful
        // DISCONNECTED while a late client reply is the pinned harmless no-op.
        // The exactly-one-side-observes argument lives at
        // `confirm_push_registration`.
        if !self
            .inner
            .runtime
            .confirm_push_registration(pid, correlation_id)
        {
            return Err(ServerError::ListenerAccept {
                message: format!(
                    "cannot push to connection process {pid}: the connection closed during push registration"
                ),
            });
        }
        let control = ConnectionControl::Push {
            correlation_id,
            payload,
        };
        if self.inner.enqueue_control(pid, control) {
            Ok(PushReplyAwaiter {
                correlation_id,
                receiver,
                deadline: deadline_at,
                runtime: Arc::downgrade(&self.inner.runtime),
            })
        } else {
            // The process is gone AND the control provably never reached a
            // consumer: `enqueue_control` returns false only when its failed-wake
            // rollback REMOVED the queued control (S8 — an entry a drain already
            // consumed counts as published and returns true, with the slot
            // lifecycle carrying the delivery truth). Dropping the now-unreachable
            // reply slot here therefore keeps the publication invariant exact on
            // every `Err` path.
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
        // Remove every host record while the readiness owner is still live. The
        // removal path ACKs deregistration and only then releases each fd guard;
        // scheduler shutdown subsequently drops the process-owned handles.
        for connection in self.inner.runtime.active_connections() {
            self.inner.runtime.finish(connection.pid);
        }
        self.inner.scheduler.shutdown();
    }

    /// R7 test instrument: slices serviced by connection `pid` since spawn.
    #[cfg(test)]
    pub(super) fn slice_count(&self, pid: u64) -> u64 {
        self.inner.runtime.slice_count(pid)
    }

    /// Reserved push reply slots outstanding (test observability for the public
    /// push paths — lets e2e tests assert slot reclamation and cap accounting).
    #[cfg(test)]
    pub(super) fn pending_push_count(&self) -> usize {
        self.inner.runtime.pending_push_count()
    }

    /// R6 test seam: a [`ReadyWaker`](super::wake::ReadyWaker) for `pid` — the same
    /// handle a subscription-inbox or reply-availability notifier fires.
    #[cfg(test)]
    pub(super) fn ready_waker(&self, pid: u64) -> Option<super::wake::ReadyWaker> {
        self.inner.runtime.ready_waker(pid)
    }

    /// Registered readiness tokens held in host records (test observability).
    #[cfg(test)]
    pub(super) fn readiness_registration_count(&self) -> usize {
        self.inner.runtime.readiness_registration_count()
    }

    /// Kernel fd registered for `pid` (test observability for fd-reuse races).
    #[cfg(test)]
    pub(super) fn readiness_fd(&self, pid: u64) -> Option<RawFd> {
        self.inner.runtime.readiness_fd(pid)
    }

    /// Installs a one-use observation for the process-owned stream at `fd` being
    /// dropped. External scheduler termination removes the process-table entry
    /// before an executing native handler is destroyed, so table absence is not
    /// sufficient evidence that the descriptor is reusable.
    #[cfg(test)]
    pub(super) fn observe_process_stream_drop(&self, fd: RawFd) -> Receiver<()> {
        self.inner.runtime.observe_process_stream_drop(fd)
    }

    /// Installs a one-use arm-to-probe barrier and returns its test endpoints.
    #[cfg(test)]
    pub(super) fn install_pre_wait_barrier(&self) -> (Arc<Barrier>, Arc<Barrier>) {
        self.inner.runtime.install_pre_wait_barrier()
    }

    /// Barrier-staged final probes that observed newly arrived work.
    #[cfg(test)]
    pub(super) fn pre_wait_probe_hits(&self) -> u64 {
        self.inner.runtime.pre_wait_probe_hits()
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
    /// This push's absolute reply deadline, mirrored from its slot. `None` (the
    /// default push) selects the no-deadline receive path, which NEVER touches
    /// the runtime — byte-compatible with 0.2.3, no shared-lock exposure.
    /// `Some` lets `receive` wait `min(caller quantum, time until deadline)` and
    /// resolve expiry promptly, so the caller's quantum can never select a
    /// deadlined push's terminal outcome.
    deadline: Option<Instant>,
    /// Weak handle to the owning runtime, used ONLY by the explicit-deadline
    /// path to resolve expiry host-side at [`receive`](Self::receive). A
    /// no-deadline push never upgrades it. `Weak` so the awaiter never keeps the
    /// runtime alive; if it is already gone, the slot (and its sender) is gone
    /// with it — the connection side is torn down.
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
    /// `timeout` is a WAIT QUANTUM ONLY — a MAXIMUM wait, not a promise to
    /// block: an elapsed poll is a benign re-arm, never a failure; the reply's
    /// lifetime belongs to the push. A caller may re-invoke `receive`
    /// indefinitely after a [`ServerError::PushReplyTimeout`]: the reserved slot
    /// is untouched and a later reply is still delivered byte-exact. The poll
    /// quantum never changes the protocol outcome — for a deadlined push the
    /// call waits no longer than the EARLIER of the caller's quantum and the
    /// push's deadline, so the terminal expiry is returned promptly once due,
    /// never held until the quantum ends and never deferred past it.
    ///
    /// A push with no explicit deadline never touches shared supervisor state
    /// here: the elapsed quantum returns straight from the channel wait
    /// (behaviour-compatible with 0.2.3 — no registry lock, no contention, no
    /// poison exposure on the unchanged API).
    ///
    /// # Errors
    /// Returns [`ServerError::PushReplyTimeout`] when no reply arrived within this
    /// `timeout` quantum and the push's deadline (if any) is not yet due (a
    /// benign re-arm — call again to keep waiting);
    /// [`ServerError::PushReplyExpired`] when the push carried an explicit reply
    /// deadline (via
    /// [`push_to_connection_with_deadline`](ConnectionSupervisor::push_to_connection_with_deadline))
    /// and that deadline is due (terminal: the slot is removed and its §5 cap
    /// admission released; returned as soon as the deadline passes, even
    /// mid-quantum — but evaluated at observation points, not against the wall
    /// clock: a reply already delivered when this call observes the slot wins
    /// over expiry, even if it arrived after the deadline instant); or
    /// [`ServerError::PushReplyDisconnected`] when the connection process
    /// dropped the reply slot (the connection closed — the prompt worker-death
    /// signal). The variants are distinct so callers classify by type, not
    /// message.
    pub fn receive(&self, timeout: Duration) -> Result<Vec<u8>, ServerError> {
        self.deadline.map_or_else(
            || self.receive_no_deadline(timeout),
            |deadline| self.receive_deadlined(timeout, deadline),
        )
    }

    /// The default-push receive: exactly the 0.2.3 shape. One bounded channel
    /// wait; an elapsed quantum is a benign timeout straight from the channel —
    /// no runtime upgrade, no registry lock, EVER (unrelated registry work can
    /// never stretch this call past its quantum, and registry poison cannot
    /// reach it).
    fn receive_no_deadline(&self, timeout: Duration) -> Result<Vec<u8>, ServerError> {
        match self.receiver.recv_timeout(timeout) {
            Ok(payload) => Ok(payload),
            Err(RecvTimeoutError::Timeout) => Err(ServerError::PushReplyTimeout {
                correlation_id: self.correlation_id,
            }),
            Err(RecvTimeoutError::Disconnected) => Err(ServerError::PushReplyDisconnected {
                correlation_id: self.correlation_id,
            }),
        }
    }

    /// The deadlined receive: waits `min(caller quantum, time until deadline)`
    /// and re-evaluates reply-first-then-expiry on every wake, so the caller's
    /// quantum can never select the terminal outcome (S1). Order per iteration:
    ///
    /// 1. Deliver a reply already in hand — an answer that is here must never be
    ///    reported as a timeout OR an expiry (the observation-point rule).
    /// 2. If the deadline is due, resolve expiry atomically against the registry
    ///    (`expire_slot`) and return the terminal outcome promptly — even when
    ///    the caller's quantum has time left (the quantum is a max wait).
    /// 3. Otherwise wait for the earlier of quantum-remaining and deadline; a
    ///    wake re-runs 1-2, and an exhausted quantum before the deadline is the
    ///    benign `PushReplyTimeout` re-arm with the slot untouched.
    fn receive_deadlined(
        &self,
        timeout: Duration,
        deadline: Instant,
    ) -> Result<Vec<u8>, ServerError> {
        let started = Instant::now();
        loop {
            if let Some(result) = self.try_take_reply() {
                return result;
            }
            let now = Instant::now();
            if now >= deadline {
                return self.expire_slot();
            }
            let quantum_left = timeout.saturating_sub(now.duration_since(started));
            if quantum_left.is_zero() {
                return Err(ServerError::PushReplyTimeout {
                    correlation_id: self.correlation_id,
                });
            }
            match self
                .receiver
                .recv_timeout(quantum_left.min(deadline.duration_since(now)))
            {
                Ok(payload) => return Ok(payload),
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(ServerError::PushReplyDisconnected {
                        correlation_id: self.correlation_id,
                    });
                }
                // Re-loop: deliver a reply that raced the wake, expire a
                // now-due deadline, or report the exhausted quantum benignly.
                Err(RecvTimeoutError::Timeout) => {}
            }
        }
    }

    /// Resolves a due deadline against the registry's atomic removal transition.
    fn expire_slot(&self) -> Result<Vec<u8>, ServerError> {
        let timeout_error = || ServerError::PushReplyTimeout {
            correlation_id: self.correlation_id,
        };
        let Some(runtime) = self.runtime.upgrade() else {
            // The runtime is gone, and the slot map (with every sender) with it:
            // the connection side is torn down. Re-check the channel so the
            // dropped sender reads as the established DISCONNECTED outcome — a
            // dead runtime must not be misreported as a benign healthy-but-slow
            // timeout (S4).
            return self
                .try_take_reply()
                .unwrap_or(Err(ServerError::PushReplyDisconnected {
                    correlation_id: self.correlation_id,
                }));
        };
        match runtime.expire_push_if_due(self.correlation_id) {
            PushSlotDisposition::Expired => Err(ServerError::PushReplyExpired {
                correlation_id: self.correlation_id,
            }),
            // Unreachable by construction (this is only called with the deadline
            // due, and the registry re-reads a monotonic clock); honest benign
            // fallback rather than a panic.
            PushSlotDisposition::Live => Err(timeout_error()),
            // Another path (a concurrent `resolve_push`, or connection close)
            // removed the slot under the registry lock while we waited on it. Its
            // send, if any, happens under that same lock, so re-check the channel:
            // a delivered reply is present now; a dropped sender is disconnected.
            PushSlotDisposition::Absent => self
                .try_take_reply()
                .unwrap_or_else(|| Err(timeout_error())),
        }
    }

    /// Non-blocking check for a reply already sitting in the channel. `Some` with
    /// the payload or a disconnected error; `None` when the channel is still empty
    /// (no reply yet — the caller re-arms).
    fn try_take_reply(&self) -> Option<Result<Vec<u8>, ServerError>> {
        match self.receiver.try_recv() {
            Ok(payload) => Some(Ok(payload)),
            Err(TryRecvError::Disconnected) => Some(Err(ServerError::PushReplyDisconnected {
                correlation_id: self.correlation_id,
            })),
            Err(TryRecvError::Empty) => None,
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

        let scheduler = Scheduler::with_services(
            SchedulerConfig {
                thread_count: Some(CONNECTION_SCHEDULER_THREADS),
                ..SchedulerConfig::default()
            },
            SchedulerServices::from_config().owned_readiness(),
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
        // The host-held duplicate keeps the fd alive until the single record-removal
        // path has synchronously deregistered readiness. External process death can
        // therefore never let fd reuse overtake host-side deregistration.
        let fd_guard = stream
            .try_clone()
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("failed to retain connection fd for teardown: {error}"),
            })?;
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
        if let Err(error) = self.runtime.register_with_fd(pid, peer_addr, fd_guard) {
            // Registration failure leaves no host record to reap. Terminate the
            // just-spawned process explicitly so neither its stream nor admission
            // reservation can escape this failed spawn.
            self.scheduler.terminate_process(pid, ExitReason::Error);
            return Err(error);
        }
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

    /// Queues `control` for `pid` and wakes the process. Returns whether the
    /// control was PUBLISHED (left in the queue with a successful wake, or
    /// already consumed by a drain) — `false` guarantees no consumer ever saw
    /// it.
    ///
    /// S8: a failed wake does NOT prove the queued control was never consumed.
    /// The insert releases the queue lock before the wake attempt, and a
    /// process already executing a control drain (each control atom drains ALL
    /// queued controls for the pid) can pop the just-inserted entry in that
    /// window, then exit before the wake check. Publication is therefore
    /// disambiguated BY OBSERVATION on the failed-wake path: `remove_control`
    /// finding and removing the entry proves no consumer saw it (truly
    /// unpublished — `false`); finding nothing proves a drain consumed it
    /// (`pop_control` is the only other remover of queue entries, and the
    /// removal key embeds the push's runtime-unique correlation id, so it can
    /// never match a different entry) — the control was published and the
    /// caller's slot lifecycle carries the delivery truth (`true`).
    fn enqueue_control(&self, pid: u64, control: ConnectionControl) -> bool {
        // Keep a key for the failure-path removal before the control is moved into
        // the queue, so a non-`Copy` (push) control can still be located and pulled
        // back out if the scheduler wakeup fails.
        let removal_key = control.clone();
        if self.runtime.push_control(pid, control).is_err() {
            return false;
        }
        // Deterministic test seam in the insert->wake window (S8 staging).
        #[cfg(test)]
        self.runtime.run_pre_wake_barrier();
        if self
            .scheduler
            .enqueue_atom_message(pid, self.runtime.control_atom())
        {
            true
        } else {
            // Failed wake: the entry's fate is the publication verdict. Removed
            // here => nobody consumed it => unpublished. Already gone => a
            // drain consumed it before the wake check => published. (A poisoned
            // queue lock reads as not-removed => published — the safe
            // direction: the slot lifecycle then reports the truthful outcome,
            // whereas claiming "unpublished" could be a lie.)
            !self.runtime.remove_control(pid, &removal_key)
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

#[cfg(test)]
#[derive(Debug, Clone)]
struct PreWaitBarrier {
    armed: Arc<Barrier>,
    release: Arc<Barrier>,
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
    /// Deterministic test gate placed after arm and before the final probe.
    #[cfg(test)]
    pre_wait_barrier: Mutex<Option<PreWaitBarrier>>,
    /// Deterministic test gate in `enqueue_control`'s insert->wake window (S8).
    #[cfg(test)]
    pre_wake_barrier: Mutex<Option<PreWaitBarrier>>,
    /// Barrier-staged slices where the final probe found newly arrived work.
    #[cfg(test)]
    pre_wait_probe_hits: AtomicU64,
    /// One-use observers for process-owned streams reaching their actual drop
    /// boundary after external scheduler termination.
    #[cfg(test)]
    process_stream_drop_observers: Mutex<HashMap<RawFd, Sender<()>>>,
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
            #[cfg(test)]
            pre_wait_barrier: Mutex::new(None),
            #[cfg(test)]
            pre_wake_barrier: Mutex::new(None),
            #[cfg(test)]
            pre_wait_probe_hits: AtomicU64::new(0),
            #[cfg(test)]
            process_stream_drop_observers: Mutex::new(HashMap::new()),
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

    #[cfg(test)]
    fn install_pre_wait_barrier(&self) -> (Arc<Barrier>, Arc<Barrier>) {
        let armed = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        if let Ok(mut slot) = self.pre_wait_barrier.lock() {
            *slot = Some(PreWaitBarrier {
                armed: Arc::clone(&armed),
                release: Arc::clone(&release),
            });
        }
        (armed, release)
    }

    /// Runs a one-use deterministic test gate after arm. Returns whether the gate
    /// was installed so only that staged probe contributes to observability.
    #[cfg(test)]
    pub(super) fn run_pre_wait_barrier(&self) -> bool {
        let barrier = self
            .pre_wait_barrier
            .lock()
            .ok()
            .and_then(|mut slot| slot.take());
        let Some(barrier) = barrier else {
            return false;
        };
        barrier.armed.wait();
        barrier.release.wait();
        true
    }

    /// Installs a one-use barrier in `enqueue_control`'s insert->wake window
    /// (S8 staging: lets a test act as the control-drain consumer between the
    /// queue insertion and the wake attempt) and returns its test endpoints.
    #[cfg(test)]
    pub(super) fn install_pre_wake_barrier(&self) -> (Arc<Barrier>, Arc<Barrier>) {
        let armed = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        if let Ok(mut slot) = self.pre_wake_barrier.lock() {
            *slot = Some(PreWaitBarrier {
                armed: Arc::clone(&armed),
                release: Arc::clone(&release),
            });
        }
        (armed, release)
    }

    /// Runs the one-use insert->wake test gate, if installed.
    #[cfg(test)]
    pub(super) fn run_pre_wake_barrier(&self) {
        let barrier = self
            .pre_wake_barrier
            .lock()
            .ok()
            .and_then(|mut slot| slot.take());
        if let Some(barrier) = barrier {
            barrier.armed.wait();
            barrier.release.wait();
        }
    }

    #[cfg(test)]
    pub(super) fn record_pre_wait_probe_hit(&self) {
        self.pre_wait_probe_hits.fetch_add(1, Ordering::AcqRel);
    }

    #[cfg(test)]
    fn pre_wait_probe_hits(&self) -> u64 {
        self.pre_wait_probe_hits.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn observe_process_stream_drop(&self, fd: RawFd) -> Receiver<()> {
        let (sender, receiver) = channel();
        if let Ok(mut observers) = self.process_stream_drop_observers.lock() {
            observers.insert(fd, sender);
        }
        receiver
    }

    /// Publishes the process-owned stream's real drop boundary to a waiting test.
    #[cfg(test)]
    pub(super) fn record_process_stream_drop(&self, fd: RawFd) {
        let observer = self
            .process_stream_drop_observers
            .lock()
            .ok()
            .and_then(|mut observers| observers.remove(&fd));
        if let Some(observer) = observer {
            let _ = observer.send(());
        }
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
    /// `pid`, and returns its receiver. `deadline` is the slot's optional absolute
    /// reply expiry (`None` = the default no-deadline shape). The connection
    /// process resolves the slot via [`resolve_push`]; the close path drops the
    /// connection's outstanding slots via [`cancel_pushes_for_connection`]; an
    /// explicit deadline resolves it via [`expire_push_if_due`].
    ///
    /// # Errors
    /// Returns [`ServerError`] when the correlation registry mutex is poisoned.
    fn register_push(
        &self,
        pid: u64,
        correlation_id: u64,
        deadline: Option<Instant>,
    ) -> Result<Receiver<Vec<u8>>, ServerError> {
        let (sender, receiver) = channel();
        let limit = self.limits.max_pending_pushes_per_connection;
        {
            let mut slots = lock(&self.push_replies, "push correlation registry")?;
            // §5 `max_pending_pushes_per_connection`: refuse a new in-flight push
            // once this connection already holds the cap. Counted per owning pid so
            // one connection cannot exhaust the shared registry; slots free on
            // reply, deadline expiry, or connection close. The count-and-insert
            // stays under the one lock so the cap is enforced atomically.
            let outstanding = slots.values().filter(|pending| pending.pid == pid).count();
            if outstanding >= limit {
                return Err(ServerError::ConnectionCapReached {
                    operation: "server push".to_owned(),
                    cap: "max_pending_pushes_per_connection",
                    limit,
                });
            }
            slots.insert(
                correlation_id,
                PendingPush {
                    pid,
                    sender,
                    deadline,
                },
            );
        }
        Ok(receiver)
    }

    /// Host-side, lazy evaluation of a push's reply deadline, called from an
    /// elapsed [`PushReplyAwaiter::receive`] quantum. This NEVER wakes the
    /// connection process and runs no timer — it inspects supervisor-owned state
    /// under the registry lock only.
    ///
    /// A slot with an explicit deadline that has passed is removed here (dropping
    /// its `Sender` and releasing its §5 `max_pending_pushes_per_connection` cap
    /// admission, since the cap is the per-pid slot count) and reported
    /// [`PushSlotDisposition::Expired`]. A slot with no deadline, or a deadline
    /// still in the future, is left UNTOUCHED and reported
    /// [`PushSlotDisposition::Live`] — the elapsed quantum is a benign re-arm. A
    /// missing slot is [`PushSlotDisposition::Absent`].
    fn expire_push_if_due(&self, correlation_id: u64) -> PushSlotDisposition {
        // S4: a poisoned registry must NOT read as slot absence — the slot (and
        // its cap admission) may still be in the map. Reclamation recovers the
        // guard: removal-only operations are sound on a recovered map (a panic
        // in another critical section cannot leave the HashMap itself in a
        // partial state; only our bookkeeping invariants could be stale, and
        // removal restores them). Admission (`register_push`) stays fail-closed.
        let mut slots = recover_lock(&self.push_replies);
        let Some(pending) = slots.get(&correlation_id) else {
            return PushSlotDisposition::Absent;
        };
        // Copy the deadline out so the immutable borrow of `slots` ends before the
        // conditional `remove` below takes a mutable one.
        let deadline = pending.deadline;
        match deadline {
            Some(at) if Instant::now() >= at => {
                slots.remove(&correlation_id);
                PushSlotDisposition::Expired
            }
            _ => PushSlotDisposition::Live,
        }
    }

    /// Drops a registered reply slot without resolving it, used on the
    /// push-enqueue failure path (the control could not be delivered to a
    /// now-gone process, so the just-reserved slot is unreachable). Dropping the
    /// slot's `Sender` wakes a still-waiting awaiter with a disconnected error.
    ///
    /// Returns whether THIS call removed the slot. Removal under the registry
    /// mutex is the atomic resolved-vs-cancelled transition: `false` means
    /// another path won — [`resolve_push`](Self::resolve_push) already sent the
    /// reply (its send happens under the same lock, so the payload is already in
    /// the channel when this returns), or the connection's close path dropped
    /// the slot (sender gone, channel disconnected).
    pub(super) fn cancel_push(&self, correlation_id: u64) -> bool {
        // S4: reclamation recovers a poisoned guard — a rollback that silently
        // skipped its removal would strand the slot and its cap admission.
        recover_lock(&self.push_replies)
            .remove(&correlation_id)
            .is_some()
    }

    /// Drops every reply slot owned by connection `pid`, waking each awaiter with a
    /// disconnected error (the dropped `Sender` disconnects the awaiter's
    /// `Receiver`). Called from the close path so a connection that exits with
    /// in-flight pushes signals worker death immediately instead of leaving each
    /// awaiter to block the full push-reply timeout. A slot that [`resolve_push`]
    /// already removed is gone, so it is untouched here; an unknown pid is a no-op.
    fn cancel_pushes_for_connection(&self, pid: u64) {
        // S4: the close sweep is the reclamation of last resort ("connection
        // close at the latest") — it must complete on a poisoned map too.
        recover_lock(&self.push_replies).retain(|_correlation_id, pending| pending.pid != pid);
    }

    /// S3 second half (shape (b), check-after-insert): pre-publication
    /// confirmation that the connection record for `pid` still exists, run in
    /// the INSERT -> CONFIRM -> PUBLISH order (S7 — confirming after the
    /// enqueue let a close-swept-then-answered push report `Err` for a Push the
    /// client had received). `true` leaves the slot in place and the caller may
    /// publish; `false` means a concurrent close already removed the record —
    /// this call then removes the caller's own just-inserted slot (rolling back
    /// its cap admission) so nothing is stranded, and the caller returns
    /// WITHOUT publishing: an `Err` from the push methods guarantees no `Push`
    /// control was published.
    ///
    /// Why exactly one side always observes the slot: `remove` (the single
    /// record-removal path) removes the host record BEFORE sweeping the pid's
    /// push slots, and this check reads the record AFTER inserting the slot and
    /// BEFORE the control is published. Both records accesses are serialized by
    /// the `records` mutex, so either (i) this read precedes the record removal
    /// — then the slot insert precedes the sweep (insert < read < removal <
    /// sweep in the happens-before order) and the SWEEP observes and removes
    /// the slot: if the control was published in the meantime the awaiter reads
    /// the truthful disconnected outcome and a late client reply is a harmless
    /// no-op; or (ii) this read follows the record removal — then THIS call
    /// observes the absence, rolls the slot back itself, and nothing was
    /// published. When both observe (a sweep and a rollback can both run in
    /// case (ii) if the insert also preceded the sweep), removal is idempotent
    /// and the cap is derived from map membership, so nothing double-releases.
    ///
    /// Lock discipline: `records` and `push_replies` are NEVER held together —
    /// here (`records` read, released, then `push_replies` on rollback), in
    /// `remove` (`records` removal, released, then the sweep), and everywhere
    /// else in this file the two mutexes are taken strictly sequentially, so no
    /// lock-order inversion is possible. This adds ZERO work to the connection
    /// slice path: the re-check runs on the push caller's thread only.
    pub(super) fn confirm_push_registration(&self, pid: u64, correlation_id: u64) -> bool {
        if self.is_registered(pid) {
            return true;
        }
        self.cancel_push(correlation_id);
        false
    }

    /// Number of reserved push reply slots outstanding. A benign wait-quantum
    /// timeout must NOT change this (the slot survives); an explicit-deadline
    /// expiry, a consumed reply, and connection close each release exactly one.
    #[cfg(test)]
    pub(super) fn pending_push_count(&self) -> usize {
        recover_lock(&self.push_replies).len()
    }

    /// Reserved push reply slots owned by connection `pid` — the exact quantity
    /// the §5 `max_pending_pushes_per_connection` cap counts (test instrument).
    #[cfg(test)]
    pub(super) fn pending_push_count_for(&self, pid: u64) -> usize {
        recover_lock(&self.push_replies)
            .values()
            .filter(|pending| pending.pid == pid)
            .count()
    }

    /// Resolves the reply slot for `correlation_id` with the client's reply
    /// payload, waking the [`PushReplyAwaiter`]. Called by the connection process
    /// when a correlated `PushReply` frame arrives. A missing slot — already
    /// resolved, expired at its explicit deadline, dropped by connection close, or
    /// an unknown id — is a harmless no-op: a late `PushReply` for a slot that is
    /// gone is discarded here, never delivered and never a panic or desync.
    pub(super) fn resolve_push(&self, correlation_id: u64, payload: Vec<u8>) {
        // S4: delivery-plus-removal recovers a poisoned guard — dropping a real
        // reply (and stranding its slot) because an unrelated critical section
        // panicked would kill reclamation and exact cap accounting.
        let mut slots = recover_lock(&self.push_replies);
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
    fn register_with_fd(
        &self,
        pid: u64,
        peer_addr: Option<SocketAddr>,
        fd_guard: TcpStream,
    ) -> Result<(), ServerError> {
        self.register_record(pid, peer_addr, Some(fd_guard))
    }

    #[cfg(test)]
    fn register(&self, pid: u64, peer_addr: Option<SocketAddr>) -> Result<(), ServerError> {
        self.register_record(pid, peer_addr, None)
    }

    fn register_record(
        &self,
        pid: u64,
        peer_addr: Option<SocketAddr>,
        fd_guard: Option<TcpStream>,
    ) -> Result<(), ServerError> {
        lock(&self.records, "connection registry")?.insert(
            pid,
            ConnectionRecord {
                peer_addr,
                registration: None,
                readiness: None,
                fd_guard,
            },
        );
        // Single-writer insert (see doc above) pairs one gauge increment with the
        // decrement in `remove`, keeping `liminal_connections_active` equal to the
        // live record count on every teardown route.
        crate::metrics::connection_spawned();
        Ok(())
    }

    pub(super) fn mark_crashed(&self, pid: u64, reason: ExitReason, peer_addr: Option<SocketAddr>) {
        let removed = self.remove(pid);
        if let Some(record) = removed.as_ref() {
            self.fire_unregistered(pid, record);
        }
        let removed_peer_addr = removed
            .as_ref()
            .and_then(|record| record.peer_addr)
            .or(peer_addr);
        tracing::warn!(
            connection_pid = pid,
            peer_addr = ?removed_peer_addr,
            reason = ?reason,
            "connection process crashed"
        );
    }

    /// Whether the spawn thread has installed the host record. A first native
    /// slice can win the enqueue-vs-record race and must remain runnable until it
    /// has somewhere host-reachable to publish its readiness token.
    pub(super) fn is_registered(&self, pid: u64) -> bool {
        self.contains(pid)
    }

    /// Removes a token minted in-slice when publishing it to the host record fails.
    pub(super) fn deregister_unpublished_readiness(&self, token: ReadinessToken) {
        if let Some(scheduler) = self.scheduler.upgrade() {
            scheduler.readiness_deregister(token);
        }
    }

    /// Cancels deadline timers detached by reply completion or connection close.
    pub(super) fn cancel_deadline_timers(&self, timers: Vec<TimerRef>) {
        let Some(scheduler) = self.scheduler.upgrade() else {
            return;
        };
        if let Ok(mut wheel) = scheduler.timers().lock() {
            for timer in timers {
                wheel.cancel(timer);
            }
        }
    }

    pub(super) fn finish(&self, pid: u64) {
        if let Some(removed) = self.remove(pid) {
            self.fire_unregistered(pid, &removed);
        }
    }

    /// Records the one readiness token minted for this connection. A live
    /// connection never re-registers: later parked slices rearm this identity.
    pub(super) fn set_readiness_token_once(
        &self,
        pid: u64,
        token: ReadinessToken,
        fd: RawFd,
    ) -> Result<(), ServerError> {
        let mut records = lock(&self.records, "connection registry")?;
        let record = records
            .get_mut(&pid)
            .ok_or_else(|| ServerError::ListenerAccept {
                message: format!("connection {pid} has no host record for readiness registration"),
            })?;
        if record.readiness.is_some() {
            return Err(ServerError::ListenerAccept {
                message: format!("connection {pid} attempted to replace its readiness token"),
            });
        }
        record.readiness = Some(ReadinessRegistration { token, fd });
        drop(records);
        Ok(())
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

    #[cfg(test)]
    fn readiness_registration_count(&self) -> usize {
        self.records.lock().map_or(0, |records| {
            records
                .values()
                .filter(|record| record.readiness.is_some())
                .count()
        })
    }

    #[cfg(test)]
    fn readiness_fd(&self, pid: u64) -> Option<RawFd> {
        self.records
            .lock()
            .ok()?
            .get(&pid)
            .and_then(|record| record.readiness.map(|registration| registration.fd))
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

    /// Non-consuming final-probe query for controls enqueued after mailbox drain.
    pub(super) fn has_control(&self, pid: u64) -> bool {
        self.controls
            .lock()
            .is_ok_and(|controls| controls.iter().any(|queued| queued.pid == pid))
    }

    /// Pulls a queued-but-unconsumed control back out of the queue. Returns
    /// whether THIS call removed it — `false` means the entry already left the
    /// queue, and since [`Self::pop_control`] is the only other remover, a
    /// consumer drain took it (S8's publication disambiguator). Matching is
    /// `pid` + full control equality; a `Push` control embeds its
    /// runtime-unique correlation id, so this can never remove a different
    /// push's entry and misreport.
    fn remove_control(&self, pid: u64, control: &ConnectionControl) -> bool {
        let Ok(mut controls) = self.controls.lock() else {
            return false;
        };
        let Some(index) = controls
            .iter()
            .position(|queued| queued.pid == pid && &queued.control == control)
        else {
            return false;
        };
        controls.remove(index);
        true
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
    ///
    /// ORDER MATTERS (S3/S7): the record is removed BEFORE the push sweep. A
    /// push registering concurrently runs INSERT -> CONFIRM -> PUBLISH
    /// (`confirm_push_registration` reads the record after inserting its slot
    /// and before publishing its control), so with this ordering exactly one
    /// side always observes a racing slot: a confirm that ran before this
    /// removal implies the slot was inserted before the sweep below (which then
    /// reaps it — a control published after that confirm is answered into a
    /// swept slot, read as the truthful disconnected outcome); a confirm after
    /// this removal sees the absence, rolls the slot back itself, and never
    /// publishes. Sweeping first (the original order) left a window — sweep,
    /// then insert+confirm, then record removal — where NEITHER side observed
    /// the slot and it leaked past connection close. The two locks are taken
    /// strictly sequentially (never nested), so no lock-order inversion.
    fn remove(&self, pid: u64) -> Option<ConnectionRecord> {
        let mut removed = self
            .records
            .lock()
            .ok()
            .and_then(|mut records| records.remove(&pid));
        self.cancel_pushes_for_connection(pid);
        if let Some(registration) = removed.as_mut().and_then(|record| record.readiness.take()) {
            if let Some(scheduler) = self.scheduler.upgrade() {
                // This call is ACK'd: it returns only after the poll owner has
                // removed the registration. `fd_guard` is still live here.
                scheduler.readiness_deregister(registration.token);
                tracing::debug!(
                    registered_fd = registration.fd,
                    "connection readiness deregistration acknowledged"
                );
            }
        }
        // Decrement only when a record was actually present so a double-remove
        // (e.g. `finish` after `reap_crashed`) cannot drive the gauge negative.
        // The §5 admission slot is released on the same guard: the reservation
        // acquired in `spawn_connection` converted into this record at
        // `register`, so its removal is exactly one release per reservation.
        if removed.is_some() {
            crate::metrics::connection_closed();
            self.release_admission();
        }
        if let Some(record) = removed.as_mut() {
            // Explicit after-deregister drop documents and enforces the fd wall.
            drop(record.fd_guard.take());
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
    /// Absolute reply deadline for this push, when one was requested via
    /// [`ConnectionSupervisor::push_to_connection_with_deadline`]. `None` is the
    /// default 0.2.3 shape: the slot has no per-slot deadline and is reclaimed
    /// only by reply-consumed or connection-close. `Some` is evaluated host-side
    /// and lazily in [`ConnectionRuntime::expire_push_if_due`].
    deadline: Option<Instant>,
}

/// Host-side disposition of a reply slot at an elapsed `receive` quantum.
enum PushSlotDisposition {
    /// The slot carried an explicit deadline that has passed; this call removed
    /// it (releasing its §5 cap admission).
    Expired,
    /// The slot is present with no deadline, or a deadline still in the future:
    /// the elapsed quantum is a benign re-arm and the slot is untouched.
    Live,
    /// No slot for this correlation id — a concurrent resolve or connection close
    /// already removed it.
    Absent,
}

#[derive(Debug)]
struct ConnectionRecord {
    peer_addr: Option<SocketAddr>,
    /// Worker registration declared on this connection, set by `set_registration`
    /// when a `WorkerRegister` frame is accepted. `Some` marks a connection whose
    /// close must fire `on_worker_unregistered`.
    registration: Option<WorkerRegistration>,
    /// Host-reachable identity for ACK'd deregistration after external death.
    readiness: Option<ReadinessRegistration>,
    /// Keeps the fd alive until deregistration has been acknowledged, preventing
    /// stale registration delivery to a subsequently reused descriptor number.
    fd_guard: Option<TcpStream>,
}

#[derive(Debug, Clone, Copy)]
struct ReadinessRegistration {
    token: ReadinessToken,
    fd: RawFd,
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

/// Locks `mutex`, RECOVERING a poisoned guard instead of failing (S4). For
/// lifecycle-cleanup paths only (reply delivery, expiry, cancellation, the
/// close sweep): removal-style operations are sound on a recovered map, and a
/// cleanup that silently skipped its removal would strand slots and their §5
/// cap admissions forever. Admission paths keep the fail-closed [`lock`].
fn recover_lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
