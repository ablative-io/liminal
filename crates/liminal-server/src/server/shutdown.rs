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

const DRAIN_PROGRESS_INTERVAL: Duration = Duration::from_millis(100);
const FORCE_CLOSE_SETTLE_TIMEOUT: Duration = Duration::from_millis(500);
const FORCE_CLOSE_POLL_INTERVAL: Duration = Duration::from_millis(10);

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

fn drain_connections(supervisor: &ConnectionSupervisor, drain_timeout: Duration) -> bool {
    let deadline = Instant::now() + drain_timeout;
    let mut last_log = Instant::now()
        .checked_sub(DRAIN_PROGRESS_INTERVAL)
        .unwrap_or_else(Instant::now);

    loop {
        let reaped = supervisor.reap_crashed_connections();
        if reaped > 0 {
            tracing::debug!(
                reaped_connections = reaped,
                "reaped connections during drain"
            );
        }

        let active = supervisor.active_connection_count();
        if active == 0 {
            tracing::info!("all connections drained before timeout");
            return true;
        }

        let now = Instant::now();
        if now >= deadline {
            tracing::warn!(
                active_connections = active,
                ?drain_timeout,
                "drain timeout expired with active connections"
            );
            return false;
        }

        if now.duration_since(last_log) >= DRAIN_PROGRESS_INTERVAL {
            tracing::info!(
                active_connections = active,
                "waiting for active connections to drain"
            );
            last_log = now;
        }

        let remaining = deadline.saturating_duration_since(now);
        thread::sleep(remaining.min(FORCE_CLOSE_POLL_INTERVAL));
    }
}

pub(crate) fn wait_after_force_close(supervisor: &ConnectionSupervisor) {
    let deadline = Instant::now() + FORCE_CLOSE_SETTLE_TIMEOUT;
    while Instant::now() < deadline {
        let reaped = supervisor.reap_crashed_connections();
        let active = supervisor.active_connection_count();
        if active == 0 {
            return;
        }
        if reaped > 0 {
            tracing::debug!(
                reaped_connections = reaped,
                active_connections = active,
                "reaped connections after force close"
            );
        }
        thread::sleep(FORCE_CLOSE_POLL_INTERVAL);
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
}
