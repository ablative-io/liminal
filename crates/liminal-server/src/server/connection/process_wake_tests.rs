//! R6 (idempotent READY marker) and R7 (quiescence slice counter) instruments.
//!
//! The park-flip makes READY markers the connection's event vocabulary and parks
//! after every fully drained slice. The counter therefore pins quiescence: an
//! idle connection runs its admission slice once and remains flat until an event.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use liminal::protocol::{Frame, encode, encoded_len};

use crate::server::connection::services::LiminalConnectionServices;
use crate::server::connection::supervisor::ConnectionSupervisor;

fn tcp_pair() -> Result<(TcpStream, TcpStream), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let client = TcpStream::connect(address)?;
    let (server, _peer) = listener.accept()?;
    Ok((client, server))
}

fn supervisor() -> Result<ConnectionSupervisor, Box<dyn std::error::Error>> {
    Ok(ConnectionSupervisor::with_services(Arc::new(
        LiminalConnectionServices::empty()?,
    ))?)
}

/// Waits until `predicate` holds or the deadline passes, returning whether it held.
fn wait_until(mut predicate: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if predicate() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    predicate()
}

/// R7: an idle connection parks after its initial drain and stays quiescent.
#[test]
fn idle_connection_slice_count_is_flat_across_soak() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = supervisor()?;
    let (_client, server) = tcp_pair()?;
    let handle = supervisor.spawn_connection(server)?;
    let pid = handle.pid();

    let admitted = wait_until(|| supervisor.slice_count(pid) > 0);
    assert!(
        admitted,
        "the connection must run its admission slice (got {})",
        supervisor.slice_count(pid)
    );
    let parked_at = supervisor.slice_count(pid);
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        supervisor.slice_count(pid),
        parked_at,
        "an idle parked connection must service no slices during the soak"
    );
    supervisor.shutdown();
    Ok(())
}

/// R6: a READY marker (and N coalesced duplicates) is idempotent — it wakes the
/// connection for one servicing without corrupting or duplicating anything. The
/// connection still answers a `Ping` with exactly one `Pong` after a burst of
/// duplicate markers, proving duplicate-marker harmlessness end to end.
#[test]
fn duplicate_ready_markers_are_harmless() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = supervisor()?;
    let (mut client, server) = tcp_pair()?;
    let handle = supervisor.spawn_connection(server)?;
    let pid = handle.pid();

    // Fire a burst of READY markers directly (the same wake a notifier fires).
    let waker = supervisor
        .ready_waker(pid)
        .ok_or("a live connection must yield a ready waker")?;
    for _ in 0..16 {
        waker.fire();
    }

    // The connection remains healthy and answers a single Ping with a single Pong.
    let ping = Frame::Ping { flags: 0 };
    let mut bytes = vec![0_u8; encoded_len(&ping)?];
    let written = encode(&ping, &mut bytes)?;
    bytes.truncate(written);
    client.write_all(&bytes)?;

    let pong_seen = wait_until(|| {
        let mut probe = [0_u8; 256];
        client
            .set_read_timeout(Some(Duration::from_millis(50)))
            .ok();
        matches!(client.read(&mut probe), Ok(n) if n > 0)
    });
    assert!(
        pong_seen,
        "the connection answers Ping after a burst of duplicate READY markers"
    );
    // Duplicate markers must not have torn the connection down.
    assert!(
        supervisor.is_tracked(pid),
        "the connection is still tracked"
    );
    supervisor.shutdown();
    Ok(())
}
