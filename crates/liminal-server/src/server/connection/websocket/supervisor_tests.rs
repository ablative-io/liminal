//! Oracle 21 — `handshake_worker_completion_delivered_not_reap_scanned`
//! (brief §4.1 handshake-stage carve-out, §5 oracle 21).
//!
//! A handshake-stage worker that never registers — a refused or failed upgrade,
//! or a pre-upgrade shutdown — delivers its OWN completion; its record is
//! reclaimed by that delivery, with no `HandshakeSupervisor::reap_finished`
//! join-scan run per accept iteration. This is the F2 exit the W1b fate
//! delivery does not cover. The proof drives the PRODUCTION `begin` path (not a
//! mock supervisor), waits on a runtime completion counter (no sleep, no
//! timing-based assertion), and asserts the registry nets back to empty WITHOUT
//! any accept-loop iteration having run.

use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream};

use super::{AcceptorSettings, HandshakeSupervisor};
use crate::server::connection::ConnectionSupervisor;

fn acceptor_settings() -> AcceptorSettings {
    AcceptorSettings {
        path: "/liminal".to_owned(),
        allowed_origins: Vec::new(),
        ping_interval: None,
        message_bound: 1024,
    }
}

/// Connects a loopback client and returns the accepted server-side stream, its
/// peer address, and the still-open client, so the caller can drive the server
/// worker to a refused or failed handshake exit through the production path.
fn connected_pair(
    listener: &TcpListener,
) -> Result<(TcpStream, SocketAddr, TcpStream), Box<dyn std::error::Error>> {
    let address = listener.local_addr()?;
    let client = TcpStream::connect(address)?;
    let (server, peer) = listener.accept()?;
    Ok((server, peer, client))
}

#[test]
fn handshake_worker_completion_delivered_not_reap_scanned() -> Result<(), Box<dyn std::error::Error>>
{
    let supervisor = ConnectionSupervisor::new()?;
    let handshakes = HandshakeSupervisor::new(supervisor.clone(), acceptor_settings());

    let listener = TcpListener::bind("127.0.0.1:0")?;

    // Worker 1 — FAILED upgrade: the peer drops before sending a request head,
    // so the worker's read hits EOF and the worker exits without ever
    // registering in the shared supervisor.
    let (server_one, peer_one, client_one) = connected_pair(&listener)?;
    drop(client_one);
    handshakes.begin(server_one, Some(peer_one));

    // Worker 2 — REFUSED upgrade: a well-formed request for the WRONG path is
    // refused, so the worker exits without registering.
    let (server_two, peer_two, mut client_two) = connected_pair(&listener)?;
    client_two.write_all(
        b"GET /wrong HTTP/1.1\r\n\
          Host: server.example.com\r\n\
          Connection: Upgrade\r\n\
          Upgrade: websocket\r\n\
          Sec-WebSocket-Version: 13\r\n\
          Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
    )?;
    handshakes.begin(server_two, Some(peer_two));

    // TOLD wait: block until BOTH workers deliver their completion. `begin`
    // returns only after installing each join handle, so once both completions
    // land, both the install and the delivery have occurred for every worker —
    // no timing assumption, no sleep.
    handshakes.wait_for_completions(2);

    // Reclaimed by delivery: the registry netted back to empty WITHOUT any
    // accept-loop iteration or `reap_finished` join-scan running — this test
    // never touches `accept_loop`.
    assert_eq!(
        handshakes.worker_record_count(),
        0,
        "never-registering handshake workers must be reclaimed by their own \
         completion delivery, leaving no worker record"
    );

    // Structural absence: no per-iteration join-scan remains. The WebSocket
    // accept loop no longer calls a handshake reap, and the supervisor holds no
    // `is_finished` liveness sample.
    let accept_loop_source = include_str!("listener.rs");
    assert!(
        !accept_loop_source.contains("reap_finished"),
        "the WebSocket accept loop must not call a handshake reap join-scan"
    );
    let supervisor_source = include_str!("supervisor.rs");
    assert!(
        !supervisor_source.contains("reap_finished"),
        "the handshake supervisor must not retain a reap_finished join-scan"
    );
    assert!(
        !supervisor_source.contains("is_finished"),
        "completion delivery must not sample worker liveness with is_finished"
    );

    drop(client_two);
    handshakes.stop();
    supervisor.shutdown();
    Ok(())
}
