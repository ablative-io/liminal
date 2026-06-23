use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

use beamr::process::ExitReason;

use super::ConnectionSupervisor;

#[test]
fn spawning_connections_creates_distinct_beamr_processes() -> Result<(), Box<dyn std::error::Error>>
{
    let supervisor = ConnectionSupervisor::new()?;
    let (first_client, first_server) = tcp_pair()?;
    let (second_client, second_server) = tcp_pair()?;

    let first = supervisor.spawn_connection(first_server)?;
    let second = supervisor.spawn_connection(second_server)?;

    assert_ne!(first.pid(), second.pid());
    assert!(first.is_live());
    assert!(second.is_live());
    assert!(supervisor.is_tracked(first.pid()));
    assert!(supervisor.is_tracked(second.pid()));
    assert_eq!(supervisor.active_connection_count(), 2);

    drop(first_client);
    drop(second_client);
    supervisor.shutdown();
    Ok(())
}

#[test]
fn crashing_one_connection_process_does_not_affect_others() -> Result<(), Box<dyn std::error::Error>>
{
    let supervisor = ConnectionSupervisor::new()?;
    let (first_client, first_server) = tcp_pair()?;
    let (second_client, second_server) = tcp_pair()?;
    let first = supervisor.spawn_connection(first_server)?;
    let second = supervisor.spawn_connection(second_server)?;

    supervisor
        .scheduler()
        .terminate_process(first.pid(), ExitReason::Error);
    wait_for_cleanup(&supervisor, first.pid())?;

    assert!(!first.is_live());
    assert!(!supervisor.is_tracked(first.pid()));
    assert!(second.is_live());
    assert!(supervisor.is_tracked(second.pid()));
    assert_eq!(supervisor.active_connection_count(), 1);

    drop(first_client);
    drop(second_client);
    supervisor.shutdown();
    Ok(())
}

fn wait_for_cleanup(
    supervisor: &ConnectionSupervisor,
    crashed_pid: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let reaped = supervisor.reap_crashed_connections();
        if !supervisor.is_tracked(crashed_pid) || reaped > 0 {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(10));
    }
    Err(format!("connection pid {crashed_pid} was not cleaned up").into())
}

fn tcp_pair() -> Result<(TcpStream, TcpStream), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(loopback_ephemeral()?)?;
    let address = listener.local_addr()?;
    let client = TcpStream::connect(address)?;
    let (server, _peer_addr) = listener.accept()?;
    Ok((client, server))
}

fn loopback_ephemeral() -> Result<SocketAddr, Box<dyn std::error::Error>> {
    Ok("127.0.0.1:0".parse()?)
}
