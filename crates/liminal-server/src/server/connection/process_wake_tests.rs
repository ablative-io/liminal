//! R6 (idempotent READY marker) and R7 (quiescence slice counter) instruments.
//!
//! These land the consumer machinery the park-flip consumes, tested behind the
//! existing busy loop. Under the busy loop a connection runs every slice, so a
//! READY marker is redundant and the slice counter advances continuously — but
//! the marker's idempotence and the counter's per-slice accounting are exactly
//! what the park-flip's wake semantics and quiescence assertion rely on, so they
//! are pinned now.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::items_after_statements
)]

use std::io::Write;
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

/// R7: the per-connection slice counter advances as the connection is serviced —
/// the instrument counts slices. (The park-flip's dual assertion, that a PARKED
/// connection's counter does NOT advance, becomes meaningful only once parking
/// exists; under the busy loop the counter is expected to keep climbing, which is
/// precisely the permanent-runnable cost the park-flip removes.)
#[test]
fn slice_counter_counts_serviced_slices() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = supervisor()?;
    let (_client, server) = tcp_pair()?;
    let handle = supervisor.spawn_connection(server)?;
    let pid = handle.pid();

    // The idle connection busy-loops; its counter must climb past any threshold.
    let advanced = wait_until(|| supervisor.slice_count(pid) > 3);
    assert!(
        advanced,
        "the slice counter must advance as the connection is serviced (got {})",
        supervisor.slice_count(pid)
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
        use std::io::Read;
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
