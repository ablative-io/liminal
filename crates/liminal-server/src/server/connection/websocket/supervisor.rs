//! Handshake supervision for the WebSocket sibling acceptor (R1.1/R1.3).
//!
//! The acceptor owns its own registry of in-flight upgrade handshakes: each
//! accepted socket is driven through the bounded HTTP upgrade on a dedicated
//! blocking worker thread (blocking on socket input — never a poll loop), and
//! shutdown interrupts every in-flight handshake by shutting its socket down,
//! so no worker can outlive the listener. A COMPLETED upgrade is then spawned
//! into the SHARED [`ConnectionSupervisor`] — the one §5 admission bound, the
//! one incarnation authority, the one registry — via the sibling-transport
//! spawn seam, becoming an ordinary supervised connection process.

use std::collections::HashMap;
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use beamr::native::native_process::NativeHandlerFactory;

use tungstenite::protocol::{Role, WebSocket};

use super::super::ConnectionSupervisor;
use super::process::WebSocketConnectionProcess;
use super::{AcceptorSettings, HandshakeOutcome, perform_upgrade, pinned_protocol_config};

/// Supervises in-flight upgrade handshakes and spawns completed upgrades into
/// the shared connection supervisor.
#[derive(Clone, Debug)]
pub(super) struct HandshakeSupervisor {
    inner: Arc<HandshakeShared>,
}

#[derive(Debug)]
struct HandshakeShared {
    supervisor: ConnectionSupervisor,
    settings: Arc<AcceptorSettings>,
    /// Set when the acceptor stops: refuses new handshakes and interrupts
    /// in-flight ones.
    stopping: AtomicBool,
    /// Host-held socket duplicates for in-flight handshakes, keyed by
    /// handshake id, so stop can interrupt a blocked upgrade read.
    inflight: Mutex<HashMap<u64, TcpStream>>,
    /// Worker join handles, keyed by handshake id.
    workers: Mutex<HashMap<u64, JoinHandle<()>>>,
    next_id: AtomicU64,
}

impl HandshakeSupervisor {
    pub(super) fn new(supervisor: ConnectionSupervisor, settings: AcceptorSettings) -> Self {
        Self {
            inner: Arc::new(HandshakeShared {
                supervisor,
                settings: Arc::new(settings),
                stopping: AtomicBool::new(false),
                inflight: Mutex::new(HashMap::new()),
                workers: Mutex::new(HashMap::new()),
                next_id: AtomicU64::new(1),
            }),
        }
    }

    /// Begins one handshake on a dedicated worker thread. Refused (socket
    /// closed) when the acceptor is already stopping.
    pub(super) fn begin(&self, stream: TcpStream, peer_addr: Option<SocketAddr>) {
        if self.inner.stopping.load(Ordering::SeqCst) {
            if let Err(error) = stream.shutdown(std::net::Shutdown::Both) {
                tracing::debug!(?peer_addr, %error, "post-stop handshake socket shutdown failed");
            }
            return;
        }
        let handshake_id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        // The host-held duplicate lets `stop` interrupt a blocked handshake
        // read; a failed clone means the handshake cannot be interrupted, so it
        // is refused up front rather than left uninterruptible.
        let guard = match stream.try_clone() {
            Ok(guard) => guard,
            Err(error) => {
                tracing::warn!(?peer_addr, %error, "handshake socket could not be retained");
                if let Err(error) = stream.shutdown(std::net::Shutdown::Both) {
                    tracing::debug!(?peer_addr, %error, "handshake refusal shutdown failed");
                }
                return;
            }
        };
        match self.inner.inflight.lock() {
            Ok(mut inflight) => {
                inflight.insert(handshake_id, guard);
            }
            Err(poisoned) => {
                tracing::error!(
                    ?peer_addr,
                    error = %poisoned,
                    "handshake registry poisoned; refusing the connection"
                );
                if let Err(error) = stream.shutdown(std::net::Shutdown::Both) {
                    tracing::debug!(?peer_addr, %error, "handshake refusal shutdown failed");
                }
                return;
            }
        }
        let shared = Arc::clone(&self.inner);
        let worker = std::thread::spawn(move || {
            shared.run_handshake(handshake_id, stream, peer_addr);
        });
        if let Ok(mut workers) = self.inner.workers.lock() {
            workers.insert(handshake_id, worker);
        }
    }

    /// Joins workers whose handshakes finished; called from the accept loop so
    /// completed threads never accumulate.
    pub(super) fn reap_finished(&self) {
        let finished: Vec<(u64, JoinHandle<()>)> = self.inner.workers.lock().map_or_else(
            |_| Vec::new(),
            |mut workers| {
                let mut done = Vec::new();
                let mut live = HashMap::new();
                for (id, handle) in workers.drain() {
                    if handle.is_finished() {
                        done.push((id, handle));
                    } else {
                        live.insert(id, handle);
                    }
                }
                *workers = live;
                done
            },
        );
        for (handshake_id, handle) in finished {
            if handle.join().is_err() {
                tracing::error!(handshake_id, "websocket handshake worker panicked");
            }
        }
    }

    /// Stops the supervisor: refuses new handshakes, interrupts every in-flight
    /// one by shutting its socket down, and joins every worker.
    pub(super) fn stop(&self) {
        self.inner.stopping.store(true, Ordering::SeqCst);
        if let Ok(mut inflight) = self.inner.inflight.lock() {
            for (handshake_id, guard) in inflight.drain() {
                if let Err(error) = guard.shutdown(std::net::Shutdown::Both) {
                    tracing::debug!(
                        handshake_id,
                        %error,
                        "in-flight handshake socket shutdown failed (already closed)"
                    );
                }
            }
        }
        let workers: Vec<(u64, JoinHandle<()>)> = self
            .inner
            .workers
            .lock()
            .map_or_else(|_| Vec::new(), |mut workers| workers.drain().collect());
        for (handshake_id, handle) in workers {
            if handle.join().is_err() {
                tracing::error!(handshake_id, "websocket handshake worker panicked");
            }
        }
    }
}

impl HandshakeShared {
    /// Drives one accepted socket: blocking bounded upgrade, then spawn into
    /// the shared supervisor. Every exit removes the in-flight registration.
    fn run_handshake(
        &self,
        handshake_id: u64,
        mut stream: TcpStream,
        peer_addr: Option<SocketAddr>,
    ) {
        // The accept loop's listener is non-blocking and BSD-derived platforms
        // let accepted sockets inherit that; the handshake reads block, so pin
        // the mode explicitly rather than inherit platform behaviour.
        if let Err(error) = stream.set_nonblocking(false) {
            tracing::warn!(?peer_addr, %error, "handshake socket mode change failed");
            self.remove_inflight(handshake_id);
            return;
        }
        let outcome = perform_upgrade(&mut stream, &self.settings);
        self.remove_inflight(handshake_id);
        match outcome {
            HandshakeOutcome::Upgraded => {
                if self.stopping.load(Ordering::SeqCst) {
                    // Stop raced the upgrade completion: the acceptor is
                    // draining, so the just-upgraded socket is closed rather
                    // than admitted behind the drain notification.
                    if let Err(error) = stream.shutdown(std::net::Shutdown::Both) {
                        tracing::debug!(?peer_addr, %error, "post-stop upgrade shutdown failed");
                    }
                    return;
                }
                self.spawn_upgraded(stream, peer_addr);
            }
            HandshakeOutcome::Refused(refusal) => {
                tracing::info!(
                    ?peer_addr,
                    status = %refusal.status(),
                    reason = %refusal,
                    "websocket upgrade refused"
                );
            }
            HandshakeOutcome::SocketError(error) => {
                tracing::debug!(?peer_addr, %error, "websocket handshake socket failed");
            }
        }
    }

    /// Wraps the upgraded stream with the pinned F2 protocol configuration and
    /// spawns the supervised WebSocket connection process.
    fn spawn_upgraded(&self, stream: TcpStream, peer_addr: Option<SocketAddr>) {
        if let Err(error) = stream.set_nonblocking(true) {
            tracing::warn!(?peer_addr, %error, "upgraded socket mode change failed");
            return;
        }
        let fd_guard = match stream.try_clone() {
            Ok(guard) => guard,
            Err(error) => {
                tracing::warn!(?peer_addr, %error, "failed to retain connection fd for teardown");
                return;
            }
        };
        let socket = WebSocket::from_raw_socket(
            stream,
            Role::Server,
            Some(pinned_protocol_config(self.settings.message_bound)),
        );
        let holder = Arc::new(Mutex::new(Some(socket)));
        let settings = Arc::clone(&self.settings);
        let build = move |runtime: Arc<super::super::supervisor::ConnectionRuntime>,
                          incarnation: Option<liminal_protocol::wire::ConnectionIncarnation>|
              -> NativeHandlerFactory {
            let holder = Arc::clone(&holder);
            let settings = Arc::clone(&settings);
            Box::new(move || {
                Box::new(WebSocketConnectionProcess::from_holder(
                    Arc::clone(&runtime),
                    peer_addr,
                    &holder,
                    incarnation,
                    &settings,
                ))
            })
        };
        match self
            .supervisor
            .spawn_transport_connection(peer_addr, fd_guard, &build)
        {
            Ok(handle) => {
                tracing::debug!(
                    ?peer_addr,
                    connection_pid = handle.pid(),
                    "websocket connection admitted"
                );
            }
            Err(error) => {
                // Admission refusal (§5 max_connections) or spawn failure: the
                // refusal is loud and the socket is closed; the holder (and the
                // upgraded socket inside it) drops here.
                tracing::warn!(?peer_addr, %error, "websocket connection refused at spawn");
            }
        }
    }

    fn remove_inflight(&self, handshake_id: u64) {
        if let Ok(mut inflight) = self.inflight.lock() {
            inflight.remove(&handshake_id);
        }
    }
}
