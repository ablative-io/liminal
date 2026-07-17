//! R1.1 listener lifecycle pins, mirroring the main TCP listener's tests:
//! bind/address inspection, the typed bind error, and stop-accepting.

use std::net::{SocketAddr, TcpListener, TcpStream};

use super::super::super::ConnectionSupervisor;
use super::WebSocketListener;
use crate::ServerError;
use crate::config::types::WebSocketConfig;

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
