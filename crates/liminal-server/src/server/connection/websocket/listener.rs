//! The sibling WebSocket accept worker (R1.1/R1.3).
//!
//! Mirrors the main TCP [`ServerListener`](crate::server::listener::ServerListener)
//! ownership shape exactly (W4 leg 1, §4.1): a BLOCKING bound listener whose one
//! accept worker kernel-parks in `accept` (zero idle wakes), an idempotent
//! stop-accepting/shutdown handle whose interrupt is an explicit self-connect,
//! and Drop-time teardown. Accepted sockets are handed to the acceptor's own
//! handshake supervision; completed upgrades join the SHARED connection
//! supervisor.

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use super::super::ConnectionSupervisor;
use super::AcceptorSettings;
use super::supervisor::HandshakeSupervisor;
use crate::ServerError;
use crate::config::types::WebSocketConfig;
use crate::server::listener::{loopback_interrupt_target, shed_on_fd_exhaustion};

#[cfg(test)]
#[path = "listener_tests.rs"]
mod tests;

/// Running sibling WebSocket listener.
#[derive(Debug)]
pub struct WebSocketListener {
    local_addr: SocketAddr,
    /// Loopback-normalised self-connect target used to interrupt the blocking
    /// `accept` at shutdown.
    interrupt_target: SocketAddr,
    shutdown: Arc<AtomicBool>,
    worker: Option<JoinHandle<Result<(), ServerError>>>,
    handshakes: HandshakeSupervisor,
    /// `accept` calls issued by the worker (test observability, oracle 2).
    #[cfg(test)]
    accept_attempts: Arc<AtomicU64>,
    /// Connections shed under fd exhaustion (test observability, oracle 24).
    #[cfg(test)]
    shed_count: Arc<AtomicU64>,
}

impl WebSocketListener {
    /// Binds the configured WebSocket listen address and starts the accept
    /// worker. The acceptor is explicit opt-in: callers reach this only when a
    /// `[websocket]` section is present, so an absent section starts no
    /// HTTP/WebSocket listener at all.
    ///
    /// # Errors
    /// Returns [`ServerError::ListenerBind`] when the configured address cannot
    /// bind, and [`ServerError::ListenerAccept`] when listener setup fails
    /// after binding or the build target cannot represent the pinned liminal
    /// frame bound.
    pub fn bind(
        config: &WebSocketConfig,
        supervisor: ConnectionSupervisor,
    ) -> Result<Self, ServerError> {
        let settings = AcceptorSettings {
            path: config.path.clone(),
            allowed_origins: config.allowed_origins.clone(),
            ping_interval: config.ping_interval_ms.map(Duration::from_millis),
            message_bound: super::liminal_ws_message_bound()?,
        };
        let listener = TcpListener::bind(config.listen_address).map_err(|source| {
            ServerError::ListenerBind {
                address: config.listen_address,
                source,
            }
        })?;
        // The listener stays BLOCKING (W4 leg 1): the accept worker kernel-parks
        // in `accept` with zero idle wakes; shutdown wakes it via the self-connect
        // interrupt below.
        let local_addr = listener
            .local_addr()
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("failed to inspect websocket listener address: {error}"),
            })?;
        let interrupt_target = loopback_interrupt_target(local_addr);
        tracing::info!(
            websocket_listen_address = %local_addr,
            upgrade_path = %settings.path,
            allowed_origins = settings.allowed_origins.len(),
            keepalive = ?settings.ping_interval,
            "liminal websocket listener bound"
        );

        let handshakes = HandshakeSupervisor::new(supervisor, settings);
        let shutdown = Arc::new(AtomicBool::new(false));
        let accept_attempts = Arc::new(AtomicU64::new(0));
        let shed_count = Arc::new(AtomicU64::new(0));
        let worker_shutdown = Arc::clone(&shutdown);
        let worker_handshakes = handshakes.clone();
        let worker_attempts = Arc::clone(&accept_attempts);
        let worker_shed = Arc::clone(&shed_count);
        let worker = thread::spawn(move || {
            accept_loop(
                &listener,
                &worker_handshakes,
                &worker_shutdown,
                &worker_attempts,
                &worker_shed,
            )
        });

        Ok(Self {
            local_addr,
            interrupt_target,
            shutdown,
            worker: Some(worker),
            handshakes,
            #[cfg(test)]
            accept_attempts,
            #[cfg(test)]
            shed_count,
        })
    }

    /// Returns the bound address for the WebSocket listener.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stops accepting new upgrades, interrupts in-flight handshakes, and
    /// waits for the accept worker to finish. Already-admitted connections
    /// remain supervised by the shared connection supervisor, so the graceful
    /// shutdown coordinator drains or force-closes them exactly like TCP
    /// connections.
    ///
    /// # Errors
    /// Returns [`ServerError`] if the accept worker panicked or returned a
    /// fatal error.
    pub fn stop_accepting(&mut self) -> Result<(), ServerError> {
        self.stop_worker()
    }

    /// Stops the listener entirely (identical to [`Self::stop_accepting`];
    /// consumes the listener for end-of-life callers).
    ///
    /// # Errors
    /// Returns [`ServerError`] if the accept worker panicked or returned a
    /// fatal error.
    pub fn shutdown(mut self) -> Result<(), ServerError> {
        self.stop_worker()
    }

    fn stop_worker(&mut self) -> Result<(), ServerError> {
        self.shutdown.store(true, Ordering::SeqCst);
        self.handshakes.stop();
        let Some(worker) = self.worker.take() else {
            return Ok(());
        };
        // Explicit cross-platform interrupt (mirrors the main listener): a single
        // self-connect wakes the blocked `accept`; the worker sheds the woken
        // socket via its post-accept shutdown recheck. If the worker already
        // exited, the listener is gone and this connect fails fast.
        if let Ok(waker) = TcpStream::connect(self.interrupt_target) {
            drop(waker);
        }
        worker.join().map_err(|_| ServerError::ListenerAccept {
            message: "websocket listener accept worker terminated unexpectedly".to_owned(),
        })?
    }

    /// `accept` calls issued by the worker (test observability, oracle 2).
    #[cfg(test)]
    fn accept_attempts(&self) -> u64 {
        self.accept_attempts.load(Ordering::SeqCst)
    }

    /// Connections shed under fd exhaustion (test observability, oracle 24).
    #[cfg(test)]
    fn shed_count(&self) -> u64 {
        self.shed_count.load(Ordering::SeqCst)
    }
}

impl Drop for WebSocketListener {
    fn drop(&mut self) {
        if let Err(error) = self.stop_worker() {
            tracing::debug!(%error, "websocket listener shutdown during drop failed");
        }
    }
}

fn accept_loop(
    listener: &TcpListener,
    handshakes: &HandshakeSupervisor,
    shutdown: &AtomicBool,
    accept_attempts: &AtomicU64,
    shed_count: &AtomicU64,
) -> Result<(), ServerError> {
    // One reserve descriptor held for the shed-with-spare-fd EMFILE policy.
    let mut reserve = listener.try_clone().ok();
    while !shutdown.load(Ordering::SeqCst) {
        accept_attempts.fetch_add(1, Ordering::SeqCst);
        match listener.accept() {
            Ok((stream, peer_addr)) => {
                if shutdown.load(Ordering::SeqCst) {
                    // Accepted while shutdown fired — the self-connect interrupt,
                    // or a real peer racing the broadcast. Shed it, never begin a
                    // handshake: no connection slips past the shutdown broadcast
                    // (oracle 20).
                    drop(stream);
                    continue;
                }
                handshakes.begin(stream, Some(peer_addr));
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) if is_transient_accept_error(&error) => {
                shed_on_fd_exhaustion(listener, &mut reserve, shed_count, &error);
            }
            Err(error) => {
                return Err(ServerError::ListenerAccept {
                    message: format!("websocket listener accept failed: {error}"),
                });
            }
        }
    }
    Ok(())
}

/// EMFILE/ENFILE resource exhaustion is transient, exactly as the main TCP
/// accept loop treats it.
fn is_transient_accept_error(error: &std::io::Error) -> bool {
    matches!(error.raw_os_error(), Some(code) if code == 24 || code == 23)
}
