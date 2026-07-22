use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};

use super::connection::ConnectionSupervisor;
use crate::ServerError;
use crate::config::types::ServerConfig;

/// Running main wire-protocol TCP listener.
///
/// W4 leg 1 (§4.1): the accept worker BLOCKS in `accept` (kernel-parked, zero
/// idle wakes) rather than spinning a non-blocking poll with a backoff sleep.
/// Shutdown wakes the blocked `accept` with an explicit self-connect interrupt
/// to `interrupt_target` (the bound address, loopback-normalised).
#[derive(Debug)]
pub struct ServerListener {
    local_addr: SocketAddr,
    /// Loopback-normalised self-connect target used to interrupt the blocking
    /// `accept` at shutdown.
    interrupt_target: SocketAddr,
    shutdown: Arc<AtomicBool>,
    worker: Option<JoinHandle<Result<(), ServerError>>>,
    supervisor: ConnectionSupervisor,
    /// Count of `accept` calls issued by the worker (test observability for the
    /// zero-idle-wakes oracle: on a silent listener this stays at the single
    /// parked call). The worker always maintains the counter; only the host-side
    /// handle for reading it is test-scoped.
    #[cfg(test)]
    accept_attempts: Arc<AtomicU64>,
    /// Count of connections shed under fd exhaustion via the reserve descriptor
    /// (test observability for the EMFILE shed oracle).
    #[cfg(test)]
    shed_count: Arc<AtomicU64>,
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
        // Explicit cross-platform interrupt: a single self-connect wakes the
        // blocked `accept`. The worker sees the shutdown flag it observed under
        // the same lock ordering and sheds the woken (spurious) socket rather
        // than admitting it — at most one spurious accept per interrupt. If the
        // worker already exited (a real accept then flag check), the listener is
        // gone and this connect fails fast; the join then returns immediately.
        if let Ok(waker) = TcpStream::connect(self.interrupt_target) {
            drop(waker);
        }
        worker.join().map_err(|_| ServerError::ListenerAccept {
            message: "listener accept worker terminated unexpectedly".to_owned(),
        })?
    }

    /// `accept` calls issued by the worker (test observability, oracle 1).
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
    // The listener stays BLOCKING (W4 leg 1): the accept worker kernel-parks in
    // `accept` with zero idle wakes; shutdown wakes it via the self-connect
    // interrupt below.
    let local_addr = listener
        .local_addr()
        .map_err(|error| ServerError::ListenerAccept {
            message: format!("failed to inspect listener address: {error}"),
        })?;
    let interrupt_target = loopback_interrupt_target(local_addr);
    tracing::info!(listen_address = %local_addr, "liminal server listener bound");

    let shutdown = Arc::new(AtomicBool::new(false));
    let accept_attempts = Arc::new(AtomicU64::new(0));
    let shed_count = Arc::new(AtomicU64::new(0));
    let worker_shutdown = Arc::clone(&shutdown);
    let worker_supervisor = supervisor.clone();
    let worker_attempts = Arc::clone(&accept_attempts);
    let worker_shed = Arc::clone(&shed_count);
    let worker = thread::spawn(move || {
        accept_loop(
            &listener,
            &worker_supervisor,
            &worker_shutdown,
            &worker_attempts,
            &worker_shed,
        )
    });

    Ok(ServerListener {
        local_addr,
        interrupt_target,
        shutdown,
        worker: Some(worker),
        supervisor,
        #[cfg(test)]
        accept_attempts,
        #[cfg(test)]
        shed_count,
    })
}

/// Normalises a bound address to a loopback self-connect target: an unspecified
/// bind (`0.0.0.0` / `::`) is reached at the matching loopback (`127.0.0.1` /
/// `::1`) on the same port; a specific address is used as-is. Shared with the
/// sibling WebSocket listener (F2), which uses the same interrupt shape.
pub(crate) fn loopback_interrupt_target(addr: SocketAddr) -> SocketAddr {
    match addr {
        SocketAddr::V4(v4) if v4.ip().is_unspecified() => {
            SocketAddr::from((Ipv4Addr::LOCALHOST, v4.port()))
        }
        SocketAddr::V6(v6) if v6.ip().is_unspecified() => {
            SocketAddr::from((Ipv6Addr::LOCALHOST, v6.port()))
        }
        other => other,
    }
}

fn accept_loop(
    listener: &TcpListener,
    supervisor: &ConnectionSupervisor,
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
                    // or a real peer racing the broadcast. Shed it, never spawn:
                    // no connection slips past the shutdown broadcast (oracle 20).
                    drop(stream);
                    continue;
                }
                if let Err(error) = supervisor.spawn_connection(stream) {
                    tracing::warn!(%peer_addr, %error, "failed to spawn connection process");
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) if is_transient_accept_error(&error) => {
                shed_on_fd_exhaustion(listener, &mut reserve, shed_count, &error);
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

/// Shed-with-spare-fd (§4.1, RULED): under EMFILE/ENFILE the reserve descriptor
/// is released so this one accept has a slot; the connection is accepted and
/// immediately shed loudly (typed log + counter), then the reserve is
/// re-established. No sleep, no retry of the failing call, no listener death;
/// the loop returns to the blocking wait, keeping zero idle wakes while pressure
/// persists and admitting normally once it lifts. Shared with the sibling
/// WebSocket listener (F2), which sheds the same way.
pub(crate) fn shed_on_fd_exhaustion(
    listener: &TcpListener,
    reserve: &mut Option<TcpListener>,
    shed_count: &AtomicU64,
    error: &std::io::Error,
) {
    if reserve.take().is_none() {
        // The reserve was already spent (a prior re-clone failed under sustained
        // pressure). Nothing to free; leave the connection pending for the next
        // wait rather than spin, and try to re-establish the reserve.
        tracing::warn!(%error, "listener fd exhaustion with no reserve descriptor available");
        *reserve = listener.try_clone().ok();
        return;
    }
    match listener.accept() {
        Ok((stream, peer_addr)) => {
            shed_count.fetch_add(1, Ordering::SeqCst);
            tracing::warn!(
                %peer_addr,
                %error,
                "listener fd exhaustion: shedding connection via reserve descriptor"
            );
            drop(stream);
        }
        Err(accept_error) => {
            tracing::warn!(%accept_error, "listener fd exhaustion: accept-with-reserve failed");
        }
    }
    // Re-establish the reserve for the next exhaustion (the shed above freed a
    // descriptor, so this normally succeeds).
    *reserve = listener.try_clone().ok();
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
    use std::io::Write;
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    use liminal::protocol::{Frame, encode, encoded_len};

    use super::{ServerListener, emfile, is_transient_accept_error, shed_on_fd_exhaustion};
    use crate::ServerError;
    use crate::config::types::{ChannelDef, ServerConfig, WebSocketConfig};
    use crate::server::connection::{ConnectionSupervisor, WebSocketListener};

    /// Oracle 1 (W4 leg 1) — on a quiet listener the blocking accept is issued
    /// exactly once (the parked call) and never again: zero repeated accepts,
    /// zero application wakes, contrasted with the retired ~100/s poll.
    #[test]
    fn silent_main_listener_has_zero_application_wakes() -> Result<(), Box<dyn std::error::Error>> {
        let address = reserve_loopback_port()?;
        let config = sample_config(address)?;
        let supervisor = ConnectionSupervisor::new()?;
        let listener = ServerListener::bind(&config, supervisor)?;

        let deadline = Instant::now() + Duration::from_secs(2);
        while listener.accept_attempts() < 1 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        let armed = listener.accept_attempts();
        assert_eq!(
            armed, 1,
            "the blocking accept is issued exactly once when parked"
        );

        thread::sleep(Duration::from_millis(200));
        assert_eq!(
            listener.accept_attempts(),
            armed,
            "a silent listener must not wake or re-accept"
        );
        assert_eq!(listener.shed_count(), 0, "a silent listener sheds nothing");

        listener.shutdown()?;
        Ok(())
    }

    /// Oracle 3 (W4 leg 1) — absence proof over this module's production source:
    /// the retired backoff constants, the non-blocking flip, the per-iteration
    /// reap call, and any accept-path sleep must not appear.
    #[test]
    fn main_listener_source_has_no_accept_backoff_or_reap_poll() {
        const SOURCE: &str = include_str!("listener.rs");
        let production = SOURCE.split("mod tests").next().unwrap_or(SOURCE);
        for forbidden in [
            "ACCEPT_IDLE_BACKOFF",
            "TRANSIENT_ERROR_BACKOFF",
            "reap_crashed_connections",
            "set_nonblocking",
            "thread::sleep",
            "ErrorKind::WouldBlock",
        ] {
            assert!(
                !production.contains(forbidden),
                "retired accept-path source `{forbidden}` reappeared"
            );
        }
    }

    /// Oracle 20 (W4 leg 1) — a socket accepted WHILE shutdown fires (the
    /// self-connect interrupt) is shed, never supervised and never slept: the
    /// post-accept flag recheck drops it, so no connection slips past the
    /// shutdown broadcast.
    #[test]
    fn accepted_socket_racing_shutdown_is_supervised_or_shed_never_slept()
    -> Result<(), Box<dyn std::error::Error>> {
        let address = reserve_loopback_port()?;
        let config = sample_config(address)?;
        let supervisor = ConnectionSupervisor::new()?;
        let mut listener = ServerListener::bind(&config, supervisor.clone())?;

        let deadline = Instant::now() + Duration::from_secs(2);
        while listener.accept_attempts() < 1 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }

        // Shutdown fires: the self-connect interrupt is a socket accepted while the
        // flag is set. The worker sheds it and exits promptly (interrupted, not slept).
        listener.stop_accepting()?;
        assert_eq!(
            supervisor.active_connection_count(),
            0,
            "the socket accepted during shutdown was shed, not supervised"
        );
        assert!(
            listener.accept_attempts() >= 1,
            "the worker parked in a blocking accept before the shutdown interrupt"
        );
        supervisor.shutdown();
        Ok(())
    }

    /// Oracle 24 (W4 leg 1) — under EMFILE/ENFILE the shed-with-spare-fd path
    /// releases the reserve, accepts and sheds the connection loudly (counter),
    /// re-establishes the reserve, and recovers — no sleep, no retry loop. Driven
    /// with a synthetic EMFILE since real fd exhaustion is not deterministic in a
    /// shared test binary.
    #[test]
    fn fd_exhaustion_sheds_loudly_and_recovers_without_spin()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(loopback_ephemeral()?)?;
        let addr = listener.local_addr()?;
        let mut reserve = listener.try_clone().ok();
        assert!(reserve.is_some(), "a reserve descriptor is held");
        let shed = AtomicU64::new(0);

        let client = TcpStream::connect(addr)?;
        let emfile_error = std::io::Error::from_raw_os_error(emfile());
        assert!(is_transient_accept_error(&emfile_error));

        shed_on_fd_exhaustion(&listener, &mut reserve, &shed, &emfile_error);
        assert_eq!(
            shed.load(Ordering::SeqCst),
            1,
            "the pending connection was shed loudly with a counter"
        );
        assert!(
            reserve.is_some(),
            "the reserve descriptor was re-established"
        );
        drop(client);

        // Recovery: a subsequent normal accept admits — the listener never spun or died.
        let client2 = TcpStream::connect(addr)?;
        let (accepted, _peer) = listener.accept()?;
        drop(accepted);
        drop(client2);
        Ok(())
    }

    /// Oracle 5 (W4 leg 1) — shutdown interrupts the blocking accept wait on BOTH
    /// listeners, promptly (no backoff sleep) and with no lost accept: a
    /// connection admitted before shutdown stays supervised.
    #[test]
    fn listener_shutdown_interrupts_accept_wait_without_backoff()
    -> Result<(), Box<dyn std::error::Error>> {
        let tcp_address = reserve_loopback_port()?;
        let tcp_config = sample_config(tcp_address)?;
        let tcp_supervisor = ConnectionSupervisor::new()?;
        let tcp_listener = ServerListener::bind(&tcp_config, tcp_supervisor.clone())?;

        // A connection admitted before shutdown must survive stop (no lost accept).
        let _client = TcpStream::connect(tcp_listener.local_addr())?;
        let deadline = Instant::now() + Duration::from_secs(2);
        while tcp_supervisor.active_connection_count() < 1 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(tcp_supervisor.active_connection_count(), 1);

        let ws_address = reserve_loopback_port()?;
        let ws_config = WebSocketConfig {
            listen_address: ws_address,
            path: "/liminal".to_owned(),
            allowed_origins: Vec::new(),
            ping_interval_ms: None,
        };
        let ws_supervisor = ConnectionSupervisor::new()?;
        let ws_listener = WebSocketListener::bind(&ws_config, ws_supervisor.clone())?;

        // Race shutdown while both accept workers are parked: both interrupt
        // promptly rather than draining a backoff.
        let start = Instant::now();
        tcp_listener.shutdown()?;
        ws_listener.shutdown()?;
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "shutdown must interrupt the accept wait promptly, not sleep-poll"
        );
        assert_eq!(
            tcp_supervisor.active_connection_count(),
            1,
            "a connection admitted before shutdown is not lost"
        );

        tcp_supervisor.shutdown();
        ws_supervisor.shutdown();
        Ok(())
    }

    /// Oracle 7 (W4 leg 1, idle-honesty both-sides) — under an unrelated live
    /// workload the connection's reactor slice counter GROWS while the listener's
    /// accept-attempt counter stays FLAT, proving the test cannot pass by
    /// disabling the reactor.
    #[test]
    fn listener_idle_grows_unrelated_reactor_slices_while_accept_counters_stay_flat()
    -> Result<(), Box<dyn std::error::Error>> {
        let address = reserve_loopback_port()?;
        let config = sample_config(address)?;
        let supervisor = ConnectionSupervisor::new()?;
        let listener = ServerListener::bind(&config, supervisor)?;
        let local_addr = listener.local_addr();
        let supervisor = listener.supervisor();

        let mut client = TcpStream::connect(local_addr)?;
        let deadline = Instant::now() + Duration::from_secs(2);
        let pid = loop {
            if let Some(pid) = supervisor.active_connection_pids().first().copied() {
                break pid;
            }
            if Instant::now() >= deadline {
                return Err("connection did not register".into());
            }
            thread::sleep(Duration::from_millis(5));
        };
        let accepts_after_admit = listener.accept_attempts();
        let slices_before = supervisor.slice_count(pid);

        // Unrelated live workload on the EXISTING connection: each ping drives a slice.
        for _ in 0..5 {
            write_ping(&mut client)?;
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while supervisor.slice_count(pid) <= slices_before && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }

        assert!(
            supervisor.slice_count(pid) > slices_before,
            "unrelated reactor slices must grow under live traffic"
        );
        assert_eq!(
            listener.accept_attempts(),
            accepts_after_admit,
            "the accept counter stays flat while an existing connection is busy"
        );

        drop(client);
        supervisor.shutdown();
        Ok(())
    }

    fn write_ping(stream: &mut TcpStream) -> Result<(), Box<dyn std::error::Error>> {
        let frame = Frame::Ping { flags: 0 };
        let len = encoded_len(&frame).map_err(|error| format!("encoded_len: {error:?}"))?;
        let mut bytes = vec![0_u8; len];
        let written = encode(&frame, &mut bytes).map_err(|error| format!("encode: {error:?}"))?;
        stream.write_all(
            bytes
                .get(..written)
                .ok_or("encoded frame length was invalid")?,
        )?;
        Ok(())
    }

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
