//! Pins for the loopback server side (design §8 step 3).
//!
//! Every pin here drives the PRODUCTION admission path —
//! `ConnectionSupervisor::spawn_loopback_connection` — and reads its answer off
//! the real registry, the real scheduler, and the real `apply_frame` seam. None
//! of them constructs a connection by hand, because the claim being pinned is
//! precisely that an in-process connection is admitted by the same door a
//! socket connection is.

use std::error::Error;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use liminal::protocol::{Frame, decode, encode, encoded_len};

use super::super::{LoopbackClientEnd, LoopbackDuplex};
use crate::config::types::{LimitsConfig, ServerConfig, ServicesConfig};
use crate::server::connection::services::LiminalConnectionServices;
use crate::server::connection::supervisor::{ConnectionRuntime, ConnectionSupervisor};
use crate::server::mount::MountKind;

/// Ring size for the pins: comfortably larger than any frame they exchange, so
/// a pin about admission or wake never accidentally becomes a pin about
/// backpressure (which the duplex's own unit pins already cover).
const PIN_RING_BYTES: usize = 64 * 1024;

/// The bound every pin waits under. A pin that reaches it has FAILED — it is a
/// failure deadline, never a settling delay.
const PIN_DEADLINE: Duration = Duration::from_secs(5);

fn supervisor() -> Result<ConnectionSupervisor, Box<dyn Error>> {
    Ok(ConnectionSupervisor::with_services(Arc::new(
        LiminalConnectionServices::empty()?,
    ))?)
}

fn supervisor_with_token(token: &str) -> Result<ConnectionSupervisor, Box<dyn Error>> {
    Ok(ConnectionSupervisor::with_services_and_auth(
        Arc::new(LiminalConnectionServices::empty()?),
        Some(token.as_bytes().to_vec()),
    )?)
}

fn supervisor_with_max_connections(max: usize) -> Result<ConnectionSupervisor, Box<dyn Error>> {
    let config = ServerConfig {
        listen_address: "127.0.0.1:0".parse()?,
        health_listen_address: "127.0.0.1:1".parse()?,
        drain_timeout_ms: 30_000,
        channels: Vec::new(),
        routing_rules: Vec::new(),
        persistence_path: None,
        cluster: None,
        auth: None,
        services: ServicesConfig::default(),
        limits: LimitsConfig {
            max_connections: max,
            ..LimitsConfig::default()
        },
        participant: None,
        websocket: None,
    };
    Ok(ConnectionSupervisor::from_config(&config)?)
}

fn tcp_pair() -> Result<(TcpStream, TcpStream), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let client = TcpStream::connect(address)?;
    let (server, _peer) = listener.accept()?;
    Ok((client, server))
}

fn write_frame(client: &mut LoopbackClientEnd, frame: &Frame) -> Result<(), Box<dyn Error>> {
    let mut bytes = vec![0_u8; encoded_len(frame)?];
    let written = encode(frame, &mut bytes)?;
    bytes.truncate(written);
    client.write_all(&bytes)?;
    Ok(())
}

/// Reads one complete frame from the client end, parking on the duplex's own
/// condvar rather than sampling. `Ok(None)` is end of file.
fn read_frame(client: &mut LoopbackClientEnd) -> Result<Option<Frame>, Box<dyn Error>> {
    let deadline = Instant::now() + PIN_DEADLINE;
    let mut buffered: Vec<u8> = Vec::new();
    loop {
        match decode(&buffered) {
            Ok((frame, _consumed)) => return Ok(Some(frame)),
            Err(
                liminal::protocol::ProtocolError::IncompleteHeader { .. }
                | liminal::protocol::ProtocolError::TruncatedPayload { .. },
            ) => {}
            Err(error) => return Err(Box::new(error)),
        }
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or("loopback pin timed out waiting for a frame")?;
        let mut chunk = [0_u8; 4096];
        let read = client.read_timeout(&mut chunk, Some(remaining))?;
        if read == 0 {
            return Ok(None);
        }
        buffered.extend_from_slice(chunk.get(..read).unwrap_or(&[]));
    }
}

/// Waits until `predicate` holds, returning whether it did before the failure
/// deadline.
fn wait_until(mut predicate: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + PIN_DEADLINE;
    while Instant::now() < deadline {
        if predicate() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    predicate()
}

fn connect_frame(token: &[u8]) -> Frame {
    Frame::Connect {
        flags: 0,
        min_version: liminal::protocol::ProtocolVersion::new(1, 0),
        max_version: liminal::protocol::ProtocolVersion::new(1, 0),
        auth_token: token.to_vec(),
    }
}

#[test]
fn a_loopback_connection_consumes_an_admission_slot_from_the_one_shared_pool()
-> Result<(), Box<dyn Error>> {
    let supervisor = supervisor_with_max_connections(1)?;
    let (_client, server_end) = LoopbackDuplex::bounded(PIN_RING_BYTES);
    let admitted = supervisor.spawn_loopback_connection(server_end)?;
    assert_eq!(
        supervisor.active_connection_count(),
        1,
        "an in-process connection is a tracked connection like any other"
    );

    // The SAME pool: a socket connect now finds the single slot taken by a
    // loopback. Capacity has no side door either.
    let (_socket_client, socket_server) = tcp_pair()?;
    match supervisor.spawn_connection(socket_server) {
        Err(crate::ServerError::ConnectionLimitReached { limit }) => {
            assert_eq!(limit, 1, "the refusal reports the configured cap");
        }
        other => {
            return Err(format!("expected ConnectionLimitReached, got {other:?}").into());
        }
    }
    drop(admitted);
    supervisor.shutdown();
    Ok(())
}

#[test]
fn a_loopback_connection_at_capacity_is_refused_exactly_like_a_socket_connect()
-> Result<(), Box<dyn Error>> {
    let supervisor = supervisor_with_max_connections(1)?;
    let (_socket_client, socket_server) = tcp_pair()?;
    supervisor.spawn_connection(socket_server)?;

    let (_client, server_end) = LoopbackDuplex::bounded(PIN_RING_BYTES);
    match supervisor.spawn_loopback_connection(server_end) {
        Err(crate::ServerError::ConnectionLimitReached { limit }) => {
            assert_eq!(
                limit, 1,
                "an in-process connect at capacity is refused with the same typed \
                 refusal, reporting the same cap, as a socket connect"
            );
        }
        other => {
            return Err(format!("expected ConnectionLimitReached, got {other:?}").into());
        }
    }
    assert_eq!(
        supervisor.active_connection_count(),
        1,
        "a refused loopback connect leaves no record and leaks no admission"
    );
    supervisor.shutdown();
    Ok(())
}

#[test]
fn a_loopback_connect_with_the_configured_token_is_acked_through_the_real_apply_frame_path()
-> Result<(), Box<dyn Error>> {
    let supervisor = supervisor_with_token("s3cr3t")?;
    let (mut client, server_end) = LoopbackDuplex::bounded(PIN_RING_BYTES);
    let handle = supervisor.spawn_loopback_connection(server_end)?;

    write_frame(&mut client, &connect_frame(b"s3cr3t"))?;
    let answer = read_frame(&mut client)?.ok_or("loopback connect produced end of file")?;
    assert!(
        matches!(answer, Frame::ConnectAck { .. }),
        "a correct token over the loopback is acked by the same handshake a socket \
         reaches, got {answer:?}"
    );
    assert!(
        supervisor.is_tracked(handle.pid()),
        "an acked connection stays open"
    );
    supervisor.shutdown();
    Ok(())
}

#[test]
fn a_loopback_connect_with_a_wrong_token_is_refused_on_its_own_duplex() -> Result<(), Box<dyn Error>>
{
    let supervisor = supervisor_with_token("s3cr3t")?;
    let (mut client, server_end) = LoopbackDuplex::bounded(PIN_RING_BYTES);
    let handle = supervisor.spawn_loopback_connection(server_end)?;
    let pid = handle.pid();

    write_frame(&mut client, &connect_frame(b"wrong"))?;
    let answer = read_frame(&mut client)?.ok_or("loopback refusal produced end of file")?;
    assert!(
        matches!(answer, Frame::ConnectError { .. }),
        "admission is admission: an embedded caller with the wrong token is refused \
         on its own loopback, got {answer:?}"
    );
    assert!(
        wait_until(|| !supervisor.is_tracked(pid)),
        "a refused connection is torn down, not left open"
    );
    supervisor.shutdown();
    Ok(())
}

#[test]
fn a_parked_loopback_connection_is_woken_by_a_client_write_and_answers()
-> Result<(), Box<dyn Error>> {
    let supervisor = supervisor()?;
    let (mut client, server_end) = LoopbackDuplex::bounded(PIN_RING_BYTES);
    let handle = supervisor.spawn_loopback_connection(server_end)?;
    let pid = handle.pid();

    // First: the connection genuinely PARKS. A transport that returned
    // `Continue` to poll its ring would advance this counter forever, so a flat
    // counter across the soak is the no-polling half of the pin — and it is
    // what makes the wake half meaningful, since a spinning connection would
    // answer with or without a wake.
    assert!(
        wait_until(|| supervisor.slice_count(pid) > 0),
        "the connection must run its admission slice"
    );
    let parked_at = supervisor.slice_count(pid);
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        supervisor.slice_count(pid),
        parked_at,
        "an idle loopback connection parks: it must service no slices while nothing \
         has happened, and must never spin on its ring"
    );

    // Then: the write itself is the whole wake. Nothing else touches this
    // connection — no timer, no control, no other traffic — so the answer can
    // only have come from the duplex firing the connection's READY atom.
    write_frame(&mut client, &Frame::Ping { flags: 0 })?;
    let answer = read_frame(&mut client)?.ok_or("woken loopback produced end of file")?;
    assert!(
        matches!(answer, Frame::Pong { .. }),
        "the parked connection was told about the write and answered it, got {answer:?}"
    );
    assert!(
        supervisor.slice_count(pid) > parked_at,
        "the wake ran a slice"
    );
    supervisor.shutdown();
    Ok(())
}

#[test]
fn dropping_the_client_end_tears_the_loopback_connection_down_and_deregisters_it()
-> Result<(), Box<dyn Error>> {
    let supervisor = supervisor()?;
    let (client, server_end) = LoopbackDuplex::bounded(PIN_RING_BYTES);
    let handle = supervisor.spawn_loopback_connection(server_end)?;
    let pid = handle.pid();
    assert!(
        wait_until(|| supervisor.slice_count(pid) > 0),
        "the connection must run its admission slice before the hangup"
    );

    // The client vanishing is the loopback's socket hangup. It fires the same
    // wake a write does, so the parked connection LEARNS about it rather than
    // sitting until some unrelated event happens past it.
    drop(client);

    assert!(
        wait_until(|| !supervisor.is_tracked(pid)),
        "a dropped client end must drive the connection's teardown"
    );
    assert_eq!(
        supervisor.active_connection_count(),
        0,
        "teardown deregisters the record and releases its admission slot"
    );
    supervisor.shutdown();
    Ok(())
}

#[test]
fn the_registry_record_for_a_loopback_connection_carries_no_fd_and_tears_down_without_one()
-> Result<(), Box<dyn Error>> {
    let supervisor = supervisor()?;
    let (client, server_end) = LoopbackDuplex::bounded(PIN_RING_BYTES);
    let handle = supervisor.spawn_loopback_connection(server_end)?;
    let pid = handle.pid();

    assert_eq!(
        supervisor.connection_has_fd_guard(pid),
        Some(false),
        "the fd guard keeps a descriptor alive until readiness deregistration is \
         acknowledged; a loopback registers no readiness and holds no descriptor, so \
         the slot is honestly empty rather than filled with a placeholder"
    );

    // A positive control on the same predicate, so `Some(false)` above is a
    // measurement of the record and not of a broken accessor.
    let (_socket_client, socket_server) = tcp_pair()?;
    let socket_handle = supervisor.spawn_connection(socket_server)?;
    assert_eq!(
        supervisor.connection_has_fd_guard(socket_handle.pid()),
        Some(true),
        "a socket-admitted record does hold its guard"
    );

    drop(client);
    assert!(
        wait_until(|| !supervisor.is_tracked(pid)),
        "teardown completes for a record that never had a guard to release"
    );
    supervisor.shutdown();
    Ok(())
}

#[test]
fn the_mount_on_the_record_is_the_door_that_admitted_the_connection() -> Result<(), Box<dyn Error>>
{
    let supervisor = supervisor()?;
    let (_client, server_end) = LoopbackDuplex::bounded(PIN_RING_BYTES);
    let loopback = supervisor.spawn_loopback_connection(server_end)?;
    let (_socket_client, socket_server) = tcp_pair()?;
    let socket = supervisor.spawn_connection(socket_server)?;

    assert_eq!(
        supervisor.connection_mount(loopback.pid()),
        Some(MountKind::Loopback),
        "the in-process door stamps Loopback"
    );
    assert_eq!(
        supervisor.connection_mount(socket.pid()),
        Some(MountKind::Tcp),
        "the socket door stamps Tcp — the two doors are told apart, so a single \
         hardcoded answer cannot satisfy this pin"
    );
    supervisor.shutdown();
    Ok(())
}

#[test]
fn a_loopback_process_stamps_its_mount_before_any_inbound_byte_is_read()
-> Result<(), Box<dyn Error>> {
    // The stamp is a property of the transport TYPE, so it is already on the
    // connection state the moment the process is built — before the duplex has
    // carried a single byte, and therefore before any client input exists that
    // could have influenced it.
    let runtime = Arc::new(ConnectionRuntime::for_tests(Arc::new(
        LiminalConnectionServices::empty()?,
    )));
    let (_client, server_end) = LoopbackDuplex::bounded(PIN_RING_BYTES);
    let holder = Arc::new(Mutex::new(Some(server_end)));
    let process = super::LoopbackConnectionProcess::from_loopback_holder(runtime, &holder, None);
    assert_eq!(
        process.mount(),
        MountKind::Loopback,
        "the loopback door's stamp is on the state apply_frame builds the handler \
         context from, before the transport has read anything"
    );
    Ok(())
}
