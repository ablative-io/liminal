//! The sibling WebSocket accept worker (R1.1/R1.3).
//!
//! Mirrors the main TCP [`ServerListener`](crate::server::listener::ServerListener)
//! ownership shape exactly: a non-blocking bound listener, one accept worker
//! thread, an idempotent stop-accepting/shutdown handle, and Drop-time
//! teardown. Accepted sockets are handed to the acceptor's own handshake
//! supervision; completed upgrades join the SHARED connection supervisor.

use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use super::super::ConnectionSupervisor;
use super::AcceptorSettings;
use super::supervisor::HandshakeSupervisor;
use crate::ServerError;
use crate::config::types::WebSocketConfig;

#[cfg(test)]
#[path = "listener_tests.rs"]
mod tests;

/// Accept-idle backoff, matching the main TCP listener's accept loop.
const ACCEPT_IDLE_BACKOFF: Duration = Duration::from_millis(10);
/// Transient accept-error backoff, matching the main TCP listener.
const TRANSIENT_ERROR_BACKOFF: Duration = Duration::from_millis(50);

/// Running sibling WebSocket listener.
#[derive(Debug)]
pub struct WebSocketListener {
    local_addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    worker: Option<JoinHandle<Result<(), ServerError>>>,
    handshakes: HandshakeSupervisor,
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
        listener
            .set_nonblocking(true)
            .map_err(|error| ServerError::ListenerAccept {
                message: format!(
                    "failed to configure websocket listener for nonblocking accept: {error}"
                ),
            })?;
        let local_addr = listener
            .local_addr()
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("failed to inspect websocket listener address: {error}"),
            })?;
        tracing::info!(
            websocket_listen_address = %local_addr,
            upgrade_path = %settings.path,
            allowed_origins = settings.allowed_origins.len(),
            keepalive = ?settings.ping_interval,
            "liminal websocket listener bound"
        );

        let handshakes = HandshakeSupervisor::new(supervisor, settings);
        let shutdown = Arc::new(AtomicBool::new(false));
        let worker_shutdown = Arc::clone(&shutdown);
        let worker_handshakes = handshakes.clone();
        let worker =
            thread::spawn(move || accept_loop(&listener, &worker_handshakes, &worker_shutdown));

        Ok(Self {
            local_addr,
            shutdown,
            worker: Some(worker),
            handshakes,
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
        worker.join().map_err(|_| ServerError::ListenerAccept {
            message: "websocket listener accept worker terminated unexpectedly".to_owned(),
        })?
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
) -> Result<(), ServerError> {
    while !shutdown.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, peer_addr)) => {
                handshakes.begin(stream, Some(peer_addr));
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(ACCEPT_IDLE_BACKOFF);
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) if is_transient_accept_error(&error) => {
                tracing::warn!(%error, "transient websocket listener accept error");
                thread::sleep(TRANSIENT_ERROR_BACKOFF);
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
