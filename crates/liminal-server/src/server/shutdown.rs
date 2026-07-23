use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use signal_hook::consts::signal::{SIGINT, SIGTERM};
use signal_hook::iterator::{Handle as SignalIteratorHandle, Signals};

use crate::ServerError;
use crate::server::connection::{ConnectionSupervisor, WebSocketListener};
use crate::server::listener::ServerListener;

/// Bounded window the force-close settle waits for the forced connections to
/// deliver their exits, as a single admitted one-shot deadline — not a poll
/// interval. The waiter parks on the shared TOLD drain-completion notification
/// (W4 leg 3, §4.3) and this deadline only bounds how long it will wait.
const FORCE_CLOSE_SETTLE_WINDOW: Duration = Duration::from_millis(500);

/// Idempotent shutdown activation handle shared by the runtime and signal thread.
#[derive(Clone)]
pub struct ShutdownHandle {
    inner: Arc<ShutdownState>,
}

impl ShutdownHandle {
    /// Creates a new inactive shutdown handle.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ShutdownState::new()),
        }
    }

    /// Initiates shutdown exactly once.
    ///
    /// Returns `true` for the first caller that transitions the handle to active,
    /// and `false` for subsequent calls.
    pub fn initiate(&self) -> bool {
        if self.inner.initiated.swap(true, Ordering::SeqCst) {
            tracing::debug!("shutdown request ignored because shutdown is already active");
            return false;
        }

        tracing::info!("shutdown requested");
        self.inner.notify();
        true
    }

    /// Blocks until shutdown is initiated.
    pub fn wait(&self) {
        if self.is_initiated() {
            return;
        }
        let Ok(mut guard) = self.inner.wait_lock.lock() else {
            return;
        };
        while !self.is_initiated() {
            match self.inner.waiter.wait(guard) {
                Ok(next_guard) => guard = next_guard,
                Err(_) => return,
            }
        }
    }

    /// Returns whether shutdown has been initiated.
    #[must_use]
    pub fn is_initiated(&self) -> bool {
        self.inner.initiated.load(Ordering::SeqCst)
    }
}

impl Default for ShutdownHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ShutdownHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ShutdownHandle")
            .field("initiated", &self.is_initiated())
            .finish()
    }
}

#[derive(Debug)]
struct ShutdownState {
    initiated: AtomicBool,
    wait_lock: Mutex<()>,
    waiter: Condvar,
}

impl ShutdownState {
    const fn new() -> Self {
        Self {
            initiated: AtomicBool::new(false),
            wait_lock: Mutex::new(()),
            waiter: Condvar::new(),
        }
    }

    fn notify(&self) {
        if let Ok(_guard) = self.wait_lock.lock() {
            self.waiter.notify_all();
        }
    }
}

/// Process-global OS signal registration for graceful shutdown.
#[derive(Debug)]
pub struct SignalShutdownRegistration {
    signal_handle: SignalIteratorHandle,
    worker: Option<JoinHandle<()>>,
}

impl SignalShutdownRegistration {
    const fn new(signal_handle: SignalIteratorHandle, worker: JoinHandle<()>) -> Self {
        Self {
            signal_handle,
            worker: Some(worker),
        }
    }
}

impl Drop for SignalShutdownRegistration {
    fn drop(&mut self) {
        self.signal_handle.close();
        let Some(worker) = self.worker.take() else {
            return;
        };
        if worker.join().is_err() {
            tracing::debug!("shutdown signal worker terminated unexpectedly");
        }
    }
}

/// Registers SIGTERM and SIGINT handlers that initiate the supplied handle.
///
/// # Errors
/// Returns [`ServerError::ListenerAccept`] when the OS signal registration fails.
pub fn register_signal_handlers(
    handle: ShutdownHandle,
) -> Result<SignalShutdownRegistration, ServerError> {
    let mut signals =
        Signals::new([SIGTERM, SIGINT]).map_err(|error| ServerError::ListenerAccept {
            message: format!("failed to register shutdown signal handlers: {error}"),
        })?;
    let signal_handle = signals.handle();
    let worker = thread::spawn(move || {
        for signal in signals.forever() {
            tracing::info!(signal, "received shutdown signal");
            handle.initiate();
        }
    });
    Ok(SignalShutdownRegistration::new(signal_handle, worker))
}

/// Runs the graceful shutdown sequence after the handle has been activated.
///
/// The optional sibling WebSocket listener (LP-WS-TRANSPORT R1) stops
/// accepting — and interrupts its in-flight upgrade handshakes — in the same
/// pre-notification window as the main listener, so no connection on EITHER
/// transport can slip past the shutdown broadcast. Already-admitted WebSocket
/// connections live in the shared supervisor and are drained/force-closed by
/// the same sequence below.
///
/// # Errors
/// Returns [`ServerError`] when stop-accepting or durable flush fails.
pub fn run_shutdown_sequence(
    listener: &mut ServerListener,
    websocket_listener: Option<&mut WebSocketListener>,
    supervisor: &ConnectionSupervisor,
    drain_timeout: Duration,
) -> Result<(), ServerError> {
    tracing::info!(?drain_timeout, "starting graceful shutdown sequence");
    // Stop accepting new connections first so none can slip into the accept
    // window after shutdown begins and miss the notification broadcast below.
    if let Some(websocket_listener) = websocket_listener {
        websocket_listener.stop_accepting()?;
    }
    listener.stop_accepting()?;

    // FIX A-ii: flush accepted-but-unfanned-out publishes to their subscriber
    // connections BEFORE broadcasting the shutdown Disconnect. Accept is now
    // stopped, so the set of accepted publishes is bounded; this TOLD barrier
    // parks on the delivery-quiescence signal (a connection parks only once every
    // accepted publish has been pumped to its socket) until every active
    // connection has quiesced, bounded by the same `drain_timeout` budget. Without
    // it, `notify_shutdown_subscribers` below could enqueue a subscriber's
    // Disconnect ahead of an in-flight fan-out (measured 8-131 ms) and the
    // subscriber's reader would exit before delivery. A timeout here is logged,
    // not fatal — the drain and force-close legs below still run.
    let flush_deadline = Instant::now() + drain_timeout;
    if !supervisor.wait_for_delivery_quiesced(flush_deadline) {
        tracing::warn!(
            ?drain_timeout,
            "delivery flush barrier did not quiesce before its budget; proceeding to shutdown notification"
        );
    }

    supervisor.notify_shutdown_subscribers();

    let drained = drain_connections(supervisor, drain_timeout);
    if !drained {
        supervisor.force_close_active_connections();
        wait_after_force_close(supervisor);
    }

    flush_durable_state(supervisor)?;
    supervisor.shutdown();
    tracing::info!("graceful shutdown sequence complete");
    Ok(())
}

/// Waits for every active connection to exit, or for `drain_timeout` to elapse.
///
/// This is the TOLD drain (W4 leg 3, §4.3): it parks on the supervisor's
/// drain-completion notification, woken only by a delivered connection exit —
/// every exit route (in-slice `mark_crashed`/`finish`, the reclaim reactor, and
/// the reconciliation scan) funnels through the single `remove()` teardown that
/// bumps the drain generation — or by the one admitted `drain_timeout` deadline.
/// It runs no per-iteration reap or active-count poll: the retired
/// reap/count/sleep loop sampled completion ~100 times a second; this samples it
/// zero times while the connections are held and drops the deadline exactly once.
fn drain_connections(supervisor: &ConnectionSupervisor, drain_timeout: Duration) -> bool {
    let active_at_start = supervisor.active_connection_count();
    if active_at_start == 0 {
        return true;
    }
    tracing::info!(
        active_connections = active_at_start,
        ?drain_timeout,
        "waiting for active connections to drain"
    );
    let deadline = Instant::now() + drain_timeout;
    let drained = supervisor.wait_for_connections_drained(deadline);
    if drained {
        tracing::info!("all connections drained before timeout");
    } else {
        tracing::warn!(
            active_connections = supervisor.active_connection_count(),
            ?drain_timeout,
            "drain timeout expired with active connections"
        );
    }
    drained
}

pub(crate) fn wait_after_force_close(supervisor: &ConnectionSupervisor) {
    // Force-close reuses the SAME TOLD exit notification the graceful drain parks
    // on (§4.3) — the forced connections deliver their exits through the one
    // `remove()` funnel — bounded by its own single settle deadline. There is no
    // second settle poll loop and no reap scan.
    let deadline = Instant::now() + FORCE_CLOSE_SETTLE_WINDOW;
    if supervisor.wait_for_connections_drained(deadline) {
        return;
    }
    let remaining = supervisor.active_connection_count();
    if remaining > 0 {
        tracing::warn!(
            active_connections = remaining,
            "connections remained active after force-close settle window"
        );
    }
}

fn flush_durable_state(supervisor: &ConnectionSupervisor) -> Result<(), ServerError> {
    tracing::info!("flushing durable channel state");
    supervisor.flush_durable_state().map_err(|error| {
        tracing::error!(%error, "durable state flush failed during shutdown");
        match error {
            ServerError::ShutdownFlush { .. } => error,
            other => ServerError::ShutdownFlush {
                message: other.to_string(),
            },
        }
    })?;
    tracing::info!("durable channel state flushed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use super::{ShutdownHandle, drain_connections};
    use crate::server::connection::ConnectionSupervisor;

    #[test]
    fn shutdown_handle_initiates_once() {
        let handle = ShutdownHandle::new();

        assert!(!handle.is_initiated());
        assert!(handle.initiate());
        assert!(handle.is_initiated());
        assert!(!handle.initiate());
    }

    #[test]
    fn shutdown_handle_wait_unblocks_on_initiate() -> Result<(), Box<dyn std::error::Error>> {
        let handle = ShutdownHandle::new();
        let waiter = handle.clone();
        let worker = thread::spawn(move || {
            waiter.wait();
            waiter.is_initiated()
        });

        thread::sleep(Duration::from_millis(10));
        assert!(handle.initiate());
        let observed = worker.join().map_err(|_| "wait worker panicked")?;

        assert!(observed);
        Ok(())
    }

    #[test]
    fn drain_returns_immediately_when_no_connections_are_active()
    -> Result<(), Box<dyn std::error::Error>> {
        let supervisor = ConnectionSupervisor::new()?;

        let drained = drain_connections(&supervisor, Duration::from_secs(5));

        assert!(drained);
        supervisor.shutdown();
        Ok(())
    }

    /// Oracle 13 (W4 leg 3, §4.3) — absence proof over the drain/settle
    /// implementation (this module before its `mod tests`): none of the retired
    /// poll constants nor the per-iteration reap scan survive. The forbid-list
    /// literals below live in the test section, so `split` excludes them from the
    /// implementation slice under inspection.
    #[test]
    fn drain_source_has_no_reap_count_sleep_loop() {
        let source = include_str!("shutdown.rs");
        // `split` always yields a first segment; `unwrap_or` keeps this panic-free
        // under the workspace lint deny while never falling back in practice.
        let implementation = source.split("mod tests").next().unwrap_or(source);
        for forbidden in [
            "DRAIN_PROGRESS_INTERVAL",
            "FORCE_CLOSE_SETTLE_TIMEOUT",
            "FORCE_CLOSE_POLL_INTERVAL",
            "reap_crashed_connections",
        ] {
            assert!(
                !implementation.contains(forbidden),
                "retired poll/reap token `{forbidden}` must not appear in the drain/settle implementation"
            );
        }
    }
}
