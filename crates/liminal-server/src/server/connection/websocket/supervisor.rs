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
use std::collections::hash_map::Entry;
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

use beamr::native::native_process::NativeHandlerFactory;

use tungstenite::protocol::{Role, WebSocket};

use super::super::ConnectionSupervisor;
use super::process::WebSocketConnectionProcess;
use super::{AcceptorSettings, HandshakeOutcome, perform_upgrade, pinned_protocol_config};

#[cfg(test)]
#[path = "supervisor_tests.rs"]
mod tests;

/// Supervises in-flight upgrade handshakes and spawns completed upgrades into
/// the shared connection supervisor.
#[derive(Clone, Debug)]
pub(super) struct HandshakeSupervisor {
    inner: Arc<HandshakeShared>,
}

/// A handshake worker's record in the supervisor registry.
#[derive(Debug)]
enum WorkerRecord {
    /// [`HandshakeSupervisor::begin`] installed the worker's join handle; the
    /// worker is live (or is unwinding toward its completion delivery).
    Live(JoinHandle<()>),
    /// The worker delivered its completion before `begin` installed the join
    /// handle — a fast worker won the install race. The tombstone is netted
    /// away when the install lands, returning the record count to zero without
    /// any liveness scan.
    CompletedBeforeInstall,
}

/// Drop guard owned by each handshake worker. On EVERY exit route — a completed
/// upgrade handed to the shared supervisor, a refused or failed upgrade, a
/// pre-upgrade shutdown interrupt, or a panic unwind — its drop delivers the
/// worker's own thread-end completion, so the worker record and its join handle
/// are reclaimed by the delivery rather than by a per-iteration join-scan.
///
/// The completion fires on unwind because it is a `Drop`; the workspace denies
/// panics in production code, so this unwind path exists to contain a
/// `std::thread` panic (the join handle), never to introduce one.
struct HandshakeCompletion {
    shared: Arc<HandshakeShared>,
    handshake_id: u64,
}

impl Drop for HandshakeCompletion {
    fn drop(&mut self) {
        self.shared.complete_worker(self.handshake_id);
    }
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
    /// Worker records, keyed by handshake id. A record is reclaimed by the
    /// worker's own completion delivery (see [`HandshakeCompletion`]) — never by
    /// a per-iteration join-scan; the two-phase [`WorkerRecord`] tolerates a
    /// worker that finishes and delivers before `begin` has installed its join
    /// handle.
    workers: Mutex<HashMap<u64, WorkerRecord>>,
    /// Count of handshake-worker completions delivered. Guarded (not atomic) so
    /// a waiter can block on `completion_signal` until a target count is
    /// reached. This is the TOLD completion accounting the delivery updates; it
    /// never samples worker liveness.
    completions: Mutex<u64>,
    /// Notified on every delivered completion so a blocked waiter wakes without
    /// polling.
    completion_signal: Condvar,
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
                completions: Mutex::new(0),
                completion_signal: Condvar::new(),
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
            // The completion guard is declared first so it drops LAST: on every
            // exit route (completed upgrade, refused/failed upgrade, pre-upgrade
            // shutdown, or panic unwind) its drop delivers this worker's
            // completion, reclaiming the record without any join-scan.
            let completion = HandshakeCompletion {
                shared,
                handshake_id,
            };
            completion
                .shared
                .run_handshake(handshake_id, stream, peer_addr);
        });
        // Install the join handle AFTER the spawn. A worker that already
        // completed and delivered first (winning this race) is netted away by
        // the two-phase record inside `install_handle`.
        self.inner.install_handle(handshake_id, worker);
    }

    /// Stops the supervisor: refuses new handshakes, interrupts every in-flight
    /// one by shutting its socket down, and joins every live worker.
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
        // First pass joins every live worker. Second pass clears the inert
        // tombstones those joined workers' completion guards deliver after the
        // first drain removed their records (and joins any worker a `begin` that
        // raced the stopping flag installed late), returning the registry to
        // empty. Two deterministic passes — no sampling loop.
        self.inner.drain_and_join_live();
        self.inner.drain_and_join_live();
    }
}

impl HandshakeShared {
    /// Installs a worker's join handle after its thread has been spawned. If the
    /// worker already delivered its completion first (winning the race against
    /// this install), the tombstone is removed and the now-finished handle is
    /// dropped (detached) instead of stored, netting the record back to zero.
    fn install_handle(&self, handshake_id: u64, worker: JoinHandle<()>) {
        if let Ok(mut workers) = self.workers.lock() {
            match workers.entry(handshake_id) {
                Entry::Occupied(entry) => {
                    // The completion delivery beat this install: net the record
                    // away and drop the finished thread's handle.
                    entry.remove();
                    drop(worker);
                }
                Entry::Vacant(entry) => {
                    entry.insert(WorkerRecord::Live(worker));
                }
            }
        }
    }

    /// Delivers one worker's thread-end: reclaims its record without a join-scan
    /// and accounts the completion so a waiter wakes. Called from the worker's
    /// [`HandshakeCompletion`] drop guard on every exit route, including panic
    /// unwind. A thread never joins itself, so a live record's handle is dropped
    /// (detached) here — the thread is already returning.
    fn complete_worker(&self, handshake_id: u64) {
        if let Ok(mut workers) = self.workers.lock() {
            match workers.entry(handshake_id) {
                Entry::Occupied(entry) => {
                    // The install already landed: remove the live record and drop
                    // this worker's own handle.
                    entry.remove();
                }
                Entry::Vacant(entry) => {
                    // Completion won the race against the install; leave a
                    // tombstone for `install_handle` to net away.
                    entry.insert(WorkerRecord::CompletedBeforeInstall);
                }
            }
        }
        // Account the delivery AFTER the record mutation, then wake any waiter,
        // so an observer that sees the incremented count also sees the reclaimed
        // record.
        if let Ok(mut completions) = self.completions.lock() {
            *completions = completions.saturating_add(1);
            self.completion_signal.notify_all();
        }
    }

    /// Drains the worker registry, joining every `Live` handle and discarding
    /// `CompletedBeforeInstall` tombstones. Used by [`HandshakeSupervisor::stop`]
    /// to reclaim live workers with no liveness scan.
    fn drain_and_join_live(&self) {
        let live: Vec<(u64, JoinHandle<()>)> = self.workers.lock().map_or_else(
            |_| Vec::new(),
            |mut workers| {
                let mut handles = Vec::new();
                for (handshake_id, record) in workers.drain() {
                    if let WorkerRecord::Live(handle) = record {
                        handles.push((handshake_id, handle));
                    }
                }
                handles
            },
        );
        for (handshake_id, handle) in live {
            if handle.join().is_err() {
                tracing::error!(handshake_id, "websocket handshake worker panicked");
            }
        }
    }

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

#[cfg(test)]
impl HandshakeSupervisor {
    /// Number of worker records currently held. Test observation of the
    /// registry size for the completion-delivery oracle.
    fn worker_record_count(&self) -> usize {
        self.inner.workers.lock().map_or(0, |workers| workers.len())
    }

    /// Blocks until at least `target` completions have been delivered, waking on
    /// the completion signal (TOLD — never a poll, never a timed sample). Used
    /// by the oracle to await reclamation without a timing-based assertion.
    fn wait_for_completions(&self, target: u64) {
        let Ok(guard) = self.inner.completions.lock() else {
            return;
        };
        let _held = self
            .inner
            .completion_signal
            .wait_while(guard, |count| *count < target);
    }
}
