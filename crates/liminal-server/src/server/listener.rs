use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use super::connection::ConnectionSupervisor;
use crate::ServerError;
use crate::config::types::ServerConfig;

const ACCEPT_IDLE_BACKOFF: Duration = Duration::from_millis(10);
const TRANSIENT_ERROR_BACKOFF: Duration = Duration::from_millis(50);

/// Running main wire-protocol TCP listener.
#[derive(Debug)]
pub struct ServerListener {
    local_addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    worker: Option<JoinHandle<Result<(), ServerError>>>,
    supervisor: ConnectionSupervisor,
}

impl ServerListener {
    /// Binds the configured listen address and starts the accept loop.
    ///
    /// # Errors
    /// Returns [`ServerError::ListenerBind`] when the configured address cannot bind,
    /// or [`ServerError::ListenerAccept`] when listener setup fails after binding.
    pub fn bind(
        config: &ServerConfig,
        supervisor: ConnectionSupervisor,
    ) -> Result<Self, ServerError> {
        bind_listener(config, supervisor)
    }

    /// Returns the bound address for the main listener.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns a clone of the supervisor used for accepted connections.
    #[must_use]
    pub fn supervisor(&self) -> ConnectionSupervisor {
        self.supervisor.clone()
    }

    /// Stops accepting new connections and waits for the accept worker to finish.
    ///
    /// # Errors
    /// Returns [`ServerError`] if the accept worker panicked or returned a fatal error.
    pub fn shutdown(mut self) -> Result<(), ServerError> {
        self.stop_worker()
    }

    /// Stops accepting new connections and waits for the accept worker to finish.
    ///
    /// Existing accepted connections remain supervised so the graceful shutdown
    /// coordinator can drain or force-close them before stopping the scheduler.
    ///
    /// # Errors
    /// Returns [`ServerError`] if the accept worker panicked or returned a fatal error.
    pub fn stop_accepting(&mut self) -> Result<(), ServerError> {
        self.stop_worker()
    }

    fn stop_worker(&mut self) -> Result<(), ServerError> {
        self.shutdown.store(true, Ordering::SeqCst);
        let Some(worker) = self.worker.take() else {
            return Ok(());
        };
        worker.join().map_err(|_| ServerError::ListenerAccept {
            message: "listener accept worker terminated unexpectedly".to_owned(),
        })?
    }
}

impl Drop for ServerListener {
    fn drop(&mut self) {
        if let Err(error) = self.stop_worker() {
            tracing::debug!(%error, "main listener shutdown during drop failed");
        }
    }
}

/// Binds the main TCP listener to `ServerConfig.listen_address`.
///
/// # Errors
/// Returns [`ServerError::ListenerBind`] when the configured address cannot bind.
pub fn bind_listener(
    config: &ServerConfig,
    supervisor: ConnectionSupervisor,
) -> Result<ServerListener, ServerError> {
    let listener =
        TcpListener::bind(config.listen_address).map_err(|source| ServerError::ListenerBind {
            address: config.listen_address,
            source,
        })?;
    listener
        .set_nonblocking(true)
        .map_err(|error| ServerError::ListenerAccept {
            message: format!("failed to configure listener for nonblocking accept: {error}"),
        })?;
    let local_addr = listener
        .local_addr()
        .map_err(|error| ServerError::ListenerAccept {
            message: format!("failed to inspect listener address: {error}"),
        })?;
    tracing::info!(listen_address = %local_addr, "liminal server listener bound");

    let shutdown = Arc::new(AtomicBool::new(false));
    let worker_shutdown = Arc::clone(&shutdown);
    let worker_supervisor = supervisor.clone();
    let worker =
        thread::spawn(move || accept_loop(&listener, &worker_supervisor, &worker_shutdown));

    Ok(ServerListener {
        local_addr,
        shutdown,
        worker: Some(worker),
        supervisor,
    })
}

fn accept_loop(
    listener: &TcpListener,
    supervisor: &ConnectionSupervisor,
    shutdown: &AtomicBool,
) -> Result<(), ServerError> {
    while !shutdown.load(Ordering::SeqCst) {
        let reaped = supervisor.reap_crashed_connections();
        if reaped > 0 {
            tracing::debug!(reaped_connections = reaped, "reaped crashed connections");
        }
        match listener.accept() {
            Ok((stream, peer_addr)) => {
                if let Err(error) = supervisor.spawn_connection(stream) {
                    tracing::warn!(%peer_addr, %error, "failed to spawn connection process");
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(ACCEPT_IDLE_BACKOFF);
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) if is_transient_accept_error(&error) => {
                tracing::warn!(%error, "transient listener accept error");
                thread::sleep(TRANSIENT_ERROR_BACKOFF);
            }
            Err(error) => {
                return Err(ServerError::ListenerAccept {
                    message: format!("listener accept failed: {error}"),
                });
            }
        }
    }
    Ok(())
}

fn is_transient_accept_error(error: &std::io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(code) if code == emfile() || code == enfile()
    )
}

#[cfg(unix)]
const fn emfile() -> i32 {
    24
}

#[cfg(unix)]
const fn enfile() -> i32 {
    23
}

#[cfg(not(unix))]
const fn emfile() -> i32 {
    24
}

#[cfg(not(unix))]
const fn enfile() -> i32 {
    23
}

#[cfg(test)]
mod tests {
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::thread;
    use std::time::{Duration, Instant};

    use super::{ServerListener, emfile, is_transient_accept_error};
    use crate::ServerError;
    use crate::config::types::{ChannelDef, ServerConfig};
    use crate::server::connection::ConnectionSupervisor;

    #[test]
    fn listener_binds_to_configured_address() -> Result<(), Box<dyn std::error::Error>> {
        let address = reserve_loopback_port()?;
        let config = sample_config(address)?;
        let supervisor = ConnectionSupervisor::new()?;

        let listener = ServerListener::bind(&config, supervisor)?;
        let local_addr = listener.local_addr();
        listener.shutdown()?;

        assert_eq!(local_addr, address);
        Ok(())
    }

    #[test]
    fn binding_in_use_port_returns_listener_bind() -> Result<(), Box<dyn std::error::Error>> {
        let occupied = TcpListener::bind(loopback_ephemeral()?)?;
        let address = occupied.local_addr()?;
        let config = sample_config(address)?;
        let supervisor = ConnectionSupervisor::new()?;

        let result = ServerListener::bind(&config, supervisor);

        assert!(matches!(
            result,
            Err(ServerError::ListenerBind { address: failed, .. }) if failed == address
        ));
        drop(occupied);
        Ok(())
    }

    #[test]
    fn accepts_multiple_simultaneous_connections() -> Result<(), Box<dyn std::error::Error>> {
        let address = reserve_loopback_port()?;
        let config = sample_config(address)?;
        let supervisor = ConnectionSupervisor::new()?;
        let listener = ServerListener::bind(&config, supervisor.clone())?;
        let local_addr = listener.local_addr();

        let clients = (0..5)
            .map(|_| TcpStream::connect(local_addr))
            .collect::<Result<Vec<_>, _>>()?;
        wait_for_connections(&supervisor, clients.len())?;

        assert_eq!(supervisor.active_connection_count(), clients.len());
        drop(clients);
        listener.shutdown()?;
        Ok(())
    }

    #[test]
    fn stop_accepting_refuses_new_connections() -> Result<(), Box<dyn std::error::Error>> {
        let address = reserve_loopback_port()?;
        let config = sample_config(address)?;
        let supervisor = ConnectionSupervisor::new()?;
        let mut listener = ServerListener::bind(&config, supervisor.clone())?;
        let local_addr = listener.local_addr();

        let client = TcpStream::connect(local_addr)?;
        wait_for_connections(&supervisor, 1)?;
        listener.stop_accepting()?;

        let result = TcpStream::connect(local_addr);

        assert!(result.is_err());
        assert_eq!(supervisor.active_connection_count(), 1);
        drop(client);
        supervisor.shutdown();
        Ok(())
    }

    #[test]
    fn resource_exhaustion_accept_error_is_transient() {
        let error = std::io::Error::from_raw_os_error(emfile());

        assert!(is_transient_accept_error(&error));
    }

    fn wait_for_connections(
        supervisor: &ConnectionSupervisor,
        expected: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if supervisor.active_connection_count() >= expected {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(10));
        }
        Err(format!(
            "expected {expected} connections, observed {}",
            supervisor.active_connection_count()
        )
        .into())
    }

    fn reserve_loopback_port() -> Result<SocketAddr, Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(loopback_ephemeral()?)?;
        let address = listener.local_addr()?;
        drop(listener);
        Ok(address)
    }

    fn loopback_ephemeral() -> Result<SocketAddr, Box<dyn std::error::Error>> {
        Ok("127.0.0.1:0".parse()?)
    }

    fn sample_config(address: SocketAddr) -> Result<ServerConfig, Box<dyn std::error::Error>> {
        Ok(ServerConfig {
            listen_address: address,
            health_listen_address: reserve_loopback_port()?,
            drain_timeout_ms: 30_000,
            channels: vec![ChannelDef {
                name: "orders".to_owned(),
                schema_ref: None,
                durable: false,
                loaded_schema: None,
            }],
            routing_rules: Vec::new(),
            persistence_path: None,
            cluster: None,
            auth: None,
            services: crate::config::types::ServicesConfig::default(),
            limits: crate::config::types::LimitsConfig::default(),
            participant: None,
            websocket: None,
        })
    }
}
