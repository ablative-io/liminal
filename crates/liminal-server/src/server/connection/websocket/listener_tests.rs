//! R1.1 listener lifecycle pins, mirroring the main TCP listener's tests:
//! bind/address inspection, the typed bind error, and stop-accepting.

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::{Duration, Instant};

use super::super::super::ConnectionSupervisor;
use super::WebSocketListener;
use crate::ServerError;
use crate::config::types::WebSocketConfig;

/// Oracle 2 (W4 leg 1) — on a quiet WebSocket listener the blocking accept is
/// issued exactly once (the parked call) and never again: zero repeated accepts,
/// zero application wakes, contrasted with the retired ~100/s poll.
#[test]
fn silent_websocket_listener_has_zero_application_wakes() -> Result<(), Box<dyn std::error::Error>>
{
    let address = reserve_loopback_port()?;
    let supervisor = ConnectionSupervisor::new()?;
    let listener = WebSocketListener::bind(&websocket_config(address), supervisor.clone())?;

    let deadline = Instant::now() + Duration::from_secs(2);
    while listener.accept_attempts() < 1 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    let armed = listener.accept_attempts();
    assert_eq!(
        armed, 1,
        "the blocking accept is issued exactly once when parked"
    );

    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        listener.accept_attempts(),
        armed,
        "a silent websocket listener must not wake or re-accept"
    );
    assert_eq!(listener.shed_count(), 0, "a silent listener sheds nothing");

    listener.shutdown()?;
    supervisor.shutdown();
    Ok(())
}

/// Oracle 4 (W4 leg 1) — absence proof over the WebSocket listener's production
/// source: the retired backoff constants, the non-blocking flip, the
/// per-iteration handshake reap, and any accept-path sleep must not appear.
#[test]
fn websocket_listener_source_has_no_accept_backoff_or_handshake_reap_poll() {
    const SOURCE: &str = include_str!("listener.rs");
    for forbidden in [
        "ACCEPT_IDLE_BACKOFF",
        "TRANSIENT_ERROR_BACKOFF",
        "reap_finished",
        "set_nonblocking",
        "thread::sleep",
        "ErrorKind::WouldBlock",
    ] {
        assert!(
            !SOURCE.contains(forbidden),
            "retired websocket accept-path source `{forbidden}` reappeared"
        );
    }
}

fn websocket_config(address: SocketAddr) -> WebSocketConfig {
    WebSocketConfig {
        listen_address: address,
        path: "/liminal".to_owned(),
        allowed_origins: Vec::new(),
        ping_interval_ms: None,
    }
}

fn reserve_loopback_port() -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    Ok(address)
}

#[test]
fn websocket_listener_binds_to_configured_address() -> Result<(), Box<dyn std::error::Error>> {
    let address = reserve_loopback_port()?;
    let supervisor = ConnectionSupervisor::new()?;
    let listener = WebSocketListener::bind(&websocket_config(address), supervisor.clone())?;
    let local_addr = listener.local_addr();
    listener.shutdown()?;
    supervisor.shutdown();
    assert_eq!(local_addr, address);
    Ok(())
}

#[test]
fn websocket_bind_conflict_returns_typed_listener_bind() -> Result<(), Box<dyn std::error::Error>> {
    let occupied = TcpListener::bind("127.0.0.1:0")?;
    let address = occupied.local_addr()?;
    let supervisor = ConnectionSupervisor::new()?;

    let result = WebSocketListener::bind(&websocket_config(address), supervisor.clone());

    assert!(matches!(
        result,
        Err(ServerError::ListenerBind { address: failed, .. }) if failed == address
    ));
    supervisor.shutdown();
    drop(occupied);
    Ok(())
}

#[test]
fn stop_accepting_refuses_new_sockets() -> Result<(), Box<dyn std::error::Error>> {
    let address = reserve_loopback_port()?;
    let supervisor = ConnectionSupervisor::new()?;
    let mut listener = WebSocketListener::bind(&websocket_config(address), supervisor.clone())?;
    listener.stop_accepting()?;

    let result = TcpStream::connect(address);
    assert!(
        result.is_err(),
        "a stopped websocket listener must refuse new sockets"
    );
    supervisor.shutdown();
    Ok(())
}
