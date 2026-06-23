use std::collections::HashMap;
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex, MutexGuard};

use beamr::atom::Atom;
use beamr::module::ModuleRegistry;
use beamr::native::native_process::NativeHandlerFactory;
use beamr::process::ExitReason;
use beamr::scheduler::{Scheduler, SchedulerConfig};

use super::process::ConnectionProcess;
use super::services::{ConnectionServices, LiminalConnectionServices};
use crate::ServerError;
use crate::config::types::ServerConfig;

const CONNECTION_SCHEDULER_THREADS: usize = 4;

#[cfg(test)]
#[path = "supervisor_tests.rs"]
mod tests;

/// Supervisor that owns the beamr scheduler for per-connection processes.
#[derive(Clone, Debug)]
pub struct ConnectionSupervisor {
    inner: Arc<SupervisorInner>,
}

impl ConnectionSupervisor {
    /// Creates a connection supervisor backed by the configured liminal channels.
    ///
    /// # Errors
    /// Returns [`ServerError`] when channel initialization or scheduler startup fails.
    pub fn from_config(config: &ServerConfig) -> Result<Self, ServerError> {
        Self::with_services(Arc::new(LiminalConnectionServices::from_config(config)?))
    }

    /// Creates a connection supervisor with no configured channels.
    ///
    /// # Errors
    /// Returns [`ServerError`] when scheduler startup fails.
    pub fn new() -> Result<Self, ServerError> {
        Self::with_services(Arc::new(LiminalConnectionServices::empty()))
    }

    /// Creates a connection supervisor using an explicit service adapter.
    ///
    /// # Errors
    /// Returns [`ServerError`] when scheduler startup fails.
    pub fn with_services(services: Arc<dyn ConnectionServices>) -> Result<Self, ServerError> {
        SupervisorInner::new(services).map(|inner| Self {
            inner: Arc::new(inner),
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

    /// Stops the beamr scheduler used by connection processes.
    pub fn shutdown(&self) {
        self.inner.scheduler.shutdown();
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
    fn new(services: Arc<dyn ConnectionServices>) -> Result<Self, ServerError> {
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
        Ok(Self {
            scheduler: Arc::new(scheduler),
            runtime: Arc::new(ConnectionRuntime::new(services)),
        })
    }

    fn spawn_connection(
        self: &Arc<Self>,
        stream: TcpStream,
    ) -> Result<ConnectionHandle, ServerError> {
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
        Ok(ConnectionHandle {
            pid,
            peer_addr,
            supervisor: Arc::clone(self),
        })
    }
}

#[derive(Debug)]
pub(super) struct ConnectionRuntime {
    services: Arc<dyn ConnectionServices>,
    records: Mutex<HashMap<u64, ConnectionRecord>>,
}

impl ConnectionRuntime {
    fn new(services: Arc<dyn ConnectionServices>) -> Self {
        Self {
            services,
            records: Mutex::new(HashMap::new()),
        }
    }

    pub(super) fn services(&self) -> &dyn ConnectionServices {
        self.services.as_ref()
    }

    fn register(&self, pid: u64, peer_addr: Option<SocketAddr>) -> Result<(), ServerError> {
        lock(&self.records, "connection registry")?.insert(pid, ConnectionRecord { peer_addr });
        Ok(())
    }

    pub(super) fn ensure_registered(
        &self,
        pid: u64,
        peer_addr: Option<SocketAddr>,
    ) -> Result<(), ServerError> {
        lock(&self.records, "connection registry")?
            .entry(pid)
            .or_insert(ConnectionRecord { peer_addr });
        Ok(())
    }

    pub(super) fn mark_crashed(&self, pid: u64, reason: ExitReason, peer_addr: Option<SocketAddr>) {
        let removed = self.remove(pid).unwrap_or(ConnectionRecord { peer_addr });
        tracing::warn!(
            connection_pid = pid,
            peer_addr = ?removed.peer_addr,
            reason = ?reason,
            "connection process crashed"
        );
    }

    pub(super) fn finish(&self, pid: u64) {
        self.remove(pid);
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
                let peer_addr = removed.and_then(|record| record.peer_addr);
                tracing::warn!(
                    connection_pid = pid,
                    ?peer_addr,
                    reason = "unknown",
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

    fn active_count(&self) -> usize {
        self.records.lock().map_or(0, |records| records.len())
    }

    fn remove(&self, pid: u64) -> Option<ConnectionRecord> {
        self.records
            .lock()
            .ok()
            .and_then(|mut records| records.remove(&pid))
    }
}

#[derive(Debug, Clone, Copy)]
struct ConnectionRecord {
    peer_addr: Option<SocketAddr>,
}

fn lock<'a, T>(mutex: &'a Mutex<T>, context: &str) -> Result<MutexGuard<'a, T>, ServerError> {
    mutex.lock().map_err(|error| ServerError::ListenerAccept {
        message: format!("{context} unavailable: {error}"),
    })
}
