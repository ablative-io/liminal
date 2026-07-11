use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

use beamr::process::ExitReason;
use liminal::protocol::{Frame, SchemaId, decode, encode, encoded_len};

use super::{ConnectionRuntime, ConnectionSupervisor, PushReplyAwaiter};
use crate::server::connection::services::{ConnectionServices, server_error_from_protocol};

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

#[test]
fn force_close_sends_disconnect_and_removes_connection() -> Result<(), Box<dyn std::error::Error>> {
    // The supervisor must carry the "orders" channel so the subscribe below
    // succeeds with a `SubscribeAck` (the empty-services supervisor would reject
    // it with a `SubscribeError`, so the connection would never reach the
    // subscribed state this test forcefully closes).
    let supervisor = supervisor_with_orders_channel()?;
    let (mut client, server) = tcp_pair()?;
    let handle = supervisor.spawn_connection(server)?;
    client.set_read_timeout(Some(Duration::from_secs(2)))?;
    send_subscribe(&mut client)?;
    read_until_subscribe_ack(&mut client)?;

    supervisor.force_close_active_connections();
    let frame = read_frame(&mut client)?;
    wait_for_cleanup(&supervisor, handle.pid())?;

    assert!(matches!(frame, Frame::Disconnect { .. }));
    assert!(!supervisor.is_tracked(handle.pid()));
    supervisor.shutdown();
    Ok(())
}

#[test]
fn notify_shutdown_subscribers_sends_disconnect_to_subscriber()
-> Result<(), Box<dyn std::error::Error>> {
    // R6: a connected subscriber must receive a shutdown notification BEFORE its
    // connection closes. Unlike force-close, the graceful notification only
    // targets connections that hold an active subscription, so the subscribe
    // must genuinely succeed for the Disconnect frame to be sent.
    let supervisor = supervisor_with_orders_channel()?;
    let (mut client, server) = tcp_pair()?;
    let handle = supervisor.spawn_connection(server)?;
    client.set_read_timeout(Some(Duration::from_secs(2)))?;
    send_subscribe(&mut client)?;
    read_until_subscribe_ack(&mut client)?;

    supervisor.notify_shutdown_subscribers();
    let frame = read_frame(&mut client)?;

    assert!(matches!(frame, Frame::Disconnect { .. }));
    // The notification precedes connection close: the process stays tracked
    // (the graceful path does not stop it), and the client still observes the
    // frame on its open stream.
    assert!(supervisor.is_tracked(handle.pid()));
    drop(client);
    supervisor.shutdown();
    Ok(())
}

#[derive(Debug)]
struct FlushFailingServices;

impl ConnectionServices for FlushFailingServices {
    fn publish(
        &self,
        channel: &str,
        envelope: &liminal::protocol::MessageEnvelope,
        idempotency_key: Option<&str>,
    ) -> Result<crate::server::connection::services::PublishOutcome, crate::ServerError> {
        let _ = (channel, envelope, idempotency_key);
        Ok(crate::server::connection::services::PublishOutcome {
            message_id: 1,
            delivered: true,
        })
    }

    fn subscribe(
        &self,
        channel: &str,
        accepted_schemas: &[SchemaId],
    ) -> Result<crate::server::connection::ConnectionSubscription, crate::ServerError> {
        let _ = (channel, accepted_schemas);
        Err(crate::ServerError::ListenerAccept {
            message: "test subscribe unsupported".to_owned(),
        })
    }

    fn unsubscribe(
        &self,
        subscription: crate::server::connection::ConnectionSubscription,
    ) -> Result<(), crate::ServerError> {
        subscription.unsubscribe()
    }

    fn open_conversation(
        &self,
        conversation_id: u64,
        subject: &str,
    ) -> Result<crate::server::connection::ConnectionConversation, crate::ServerError> {
        let _ = (conversation_id, subject);
        Err(crate::ServerError::ListenerAccept {
            message: "test conversation unsupported".to_owned(),
        })
    }

    fn conversation_message(
        &self,
        conversation: &crate::server::connection::ConnectionConversation,
        envelope: &liminal::protocol::MessageEnvelope,
    ) -> Result<(), crate::ServerError> {
        let _ = (conversation, envelope);
        Ok(())
    }

    fn close_conversation(
        &self,
        conversation: crate::server::connection::ConnectionConversation,
    ) -> Result<(), crate::ServerError> {
        drop(conversation);
        Ok(())
    }

    fn flush_durable_state(&self) -> Result<(), crate::ServerError> {
        Err(crate::ServerError::ShutdownFlush {
            message: "test flush failed".to_owned(),
        })
    }
}

#[test]
fn flush_durable_state_propagates_shutdown_flush() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor =
        ConnectionSupervisor::with_services(std::sync::Arc::new(FlushFailingServices))?;

    let result = supervisor.flush_durable_state();

    assert!(matches!(
        result,
        Err(crate::ServerError::ShutdownFlush { .. })
    ));
    supervisor.shutdown();
    Ok(())
}

/// Registers an in-flight server-push reply slot on `runtime` for connection
/// `pid` and returns the awaiter that would be handed back by
/// `push_to_connection`, without needing a live scheduler. This mirrors the real
/// allocate-id / register-slot / build-awaiter sequence so the close-path tests
/// exercise the same `push_replies` bookkeeping the production path uses.
fn outstanding_push(
    runtime: &ConnectionRuntime,
    pid: u64,
) -> Result<PushReplyAwaiter, Box<dyn std::error::Error>> {
    let correlation_id = runtime.next_push_correlation_id();
    let receiver = runtime.register_push(pid, correlation_id)?;
    Ok(PushReplyAwaiter {
        correlation_id,
        receiver,
    })
}

#[test]
fn closing_connection_wakes_outstanding_push_awaiter_promptly()
-> Result<(), Box<dyn std::error::Error>> {
    // Headline: a server push is outstanding (awaiter created, no reply sent), then
    // the connection closes. The awaiter must wake IMMEDIATELY with a disconnected
    // error rather than blocking the full push-reply timeout. We pass a 30s timeout
    // and assert the call returns well under it — proving cancellation, not timeout.
    let runtime = ConnectionRuntime::for_tests(std::sync::Arc::new(FlushFailingServices));
    let pid = 7;
    runtime.register(pid, None)?;
    let awaiter = outstanding_push(&runtime, pid)?;

    // Drive the close path (the unit-level equivalent of the connection process
    // exiting). `finish` removes the record and, via `remove`, cancels the slot.
    runtime.finish(pid);

    let timeout = Duration::from_secs(30);
    let started = Instant::now();
    let result = awaiter.receive(timeout);
    let elapsed = started.elapsed();

    assert!(
        matches!(
            result,
            Err(crate::ServerError::PushReplyDisconnected { .. })
        ),
        "expected a typed PushReplyDisconnected error, got {result:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "awaiter must wake promptly on close, not after the {timeout:?} timeout (took {elapsed:?})"
    );
    Ok(())
}

#[test]
fn mark_crashed_wakes_outstanding_push_awaiter_promptly() -> Result<(), Box<dyn std::error::Error>>
{
    // The crash close route must cancel outstanding pushes just like the graceful
    // `finish` route: a crashing connection is exactly the worker-death case the
    // prompt signal exists for.
    let runtime = ConnectionRuntime::for_tests(std::sync::Arc::new(FlushFailingServices));
    let pid = 11;
    runtime.register(pid, None)?;
    let awaiter = outstanding_push(&runtime, pid)?;

    runtime.mark_crashed(pid, ExitReason::Error, None);

    let started = Instant::now();
    let result = awaiter.receive(Duration::from_secs(30));
    let elapsed = started.elapsed();

    assert!(
        matches!(
            result,
            Err(crate::ServerError::PushReplyDisconnected { .. })
        ),
        "crash close must wake the awaiter with a disconnected error, got {result:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "crash close must wake the awaiter promptly (took {elapsed:?})"
    );
    Ok(())
}

#[test]
fn closing_one_connection_leaves_other_connections_push_intact()
-> Result<(), Box<dyn std::error::Error>> {
    // Per-pid isolation: two connections each hold an outstanding push. Closing one
    // must wake ONLY its awaiter; the other connection's push still resolves
    // normally via `resolve_push`.
    let runtime = ConnectionRuntime::for_tests(std::sync::Arc::new(FlushFailingServices));
    let closing_pid = 21;
    let surviving_pid = 22;
    runtime.register(closing_pid, None)?;
    runtime.register(surviving_pid, None)?;
    let closing_awaiter = outstanding_push(&runtime, closing_pid)?;
    let surviving_awaiter = outstanding_push(&runtime, surviving_pid)?;

    runtime.finish(closing_pid);

    // The closed connection's awaiter wakes disconnected.
    let closing_result = closing_awaiter.receive(Duration::from_secs(30));
    assert!(
        matches!(
            closing_result,
            Err(crate::ServerError::PushReplyDisconnected { .. })
        ),
        "the closed connection's awaiter must wake disconnected, got {closing_result:?}"
    );

    // The surviving connection's slot is untouched and still resolves on a reply.
    let reply = b"surviving-reply".to_vec();
    runtime.resolve_push(surviving_awaiter.correlation_id(), reply.clone());
    let surviving_result = surviving_awaiter.receive(Duration::from_secs(2))?;
    assert_eq!(
        surviving_result, reply,
        "the surviving connection's push must still resolve with its reply"
    );
    Ok(())
}

#[test]
fn resolved_push_then_close_is_a_noop_with_no_double_send() -> Result<(), Box<dyn std::error::Error>>
{
    // Resolved-then-close: resolve a push (awaiter gets the reply), THEN close the
    // connection. The close must not panic or double-send; the already-resolved
    // correlation id was removed by `resolve_push`, so cancellation is a no-op.
    let runtime = ConnectionRuntime::for_tests(std::sync::Arc::new(FlushFailingServices));
    let pid = 31;
    runtime.register(pid, None)?;
    let awaiter = outstanding_push(&runtime, pid)?;

    let reply = b"resolved-reply".to_vec();
    runtime.resolve_push(awaiter.correlation_id(), reply.clone());
    let received = awaiter.receive(Duration::from_secs(2))?;
    assert_eq!(received, reply, "the push must resolve with its reply");

    // Closing afterwards is a no-op for the already-resolved slot.
    runtime.finish(pid);
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

fn supervisor_with_orders_channel() -> Result<ConnectionSupervisor, Box<dyn std::error::Error>> {
    use crate::config::types::{ChannelDef, ServerConfig, ServicesConfig};

    let config = ServerConfig {
        listen_address: "127.0.0.1:0".parse()?,
        health_listen_address: "127.0.0.1:0".parse()?,
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
        services: ServicesConfig::default(),
    };
    Ok(ConnectionSupervisor::from_config(&config)?)
}

/// A channel-free config carrying the given `[services]` profile, for the
/// profile-dispatch tests on the public config constructor.
fn channel_free_config_with_profile(
    profile: &str,
) -> Result<crate::config::types::ServerConfig, Box<dyn std::error::Error>> {
    use crate::config::types::{ServerConfig, ServicesConfig};

    Ok(ServerConfig {
        listen_address: "127.0.0.1:0".parse()?,
        health_listen_address: "127.0.0.1:0".parse()?,
        drain_timeout_ms: 30_000,
        channels: Vec::new(),
        routing_rules: Vec::new(),
        persistence_path: None,
        cluster: None,
        auth: None,
        services: ServicesConfig {
            profile: profile.to_owned(),
        },
    })
}

/// §9 D2 gate on the PUBLIC config constructor (record-by-construction census):
/// a worker-profile config through `ConnectionSupervisor::from_config` builds the
/// front door — the subsystem factory records ZERO extra subsystems and the
/// installed services serve no channel operations — so no full service can be
/// created through this constructor under the worker profile. The recording
/// happens INSIDE the factory that is the construction path's only route to those
/// constructors (a bypass would be a code-review-visible structural violation of
/// the factory seam, not a silently-missing side call). The connection scheduler
/// itself is built for BOTH profiles (the supervisor owns it), so worker mode is
/// EXACTLY that one scheduler with its fixed worker count — asserted against the
/// live scheduler's configured thread count. The full-profile arm is the positive
/// control: the SAME instrument through the SAME constructor records the
/// channel/conversation/haematite schedulers. An OS-level thread census upgrades
/// this when the beamr composition lane's scheduler-inventory API (currently on
/// their branch, not yet consumable from liminal) lands.
#[test]
fn from_config_worker_profile_builds_front_door_with_no_extra_schedulers()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::server::connection::services::SchedulerSubsystem;
    use crate::server::connection::services::subsystem_census::RecordingSubsystems;

    let worker_root = tempfile::tempdir()?;
    let worker_subsystems = RecordingSubsystems::rooted(worker_root.path());
    let supervisor = ConnectionSupervisor::from_config_via(
        &channel_free_config_with_profile("worker-front-door")?,
        &worker_subsystems,
    )?;
    assert!(
        worker_subsystems.recorded().is_empty(),
        "the worker profile must construct no scheduler beyond the connection supervisor's own"
    );
    // The retained connection scheduler runs exactly its fixed worker complement:
    // worker mode = this one scheduler, at this size, and nothing else.
    assert_eq!(
        supervisor.scheduler().thread_count(),
        super::CONNECTION_SCHEDULER_THREADS,
        "the connection scheduler must carry exactly its fixed worker count"
    );
    assert!(
        !supervisor
            .inner
            .runtime
            .services()
            .supports_channel_operations(),
        "the worker profile must install the front-door services, not the full stack"
    );
    assert_eq!(supervisor.active_connection_count(), 0);
    supervisor.shutdown();

    let full_root = tempfile::tempdir()?;
    let full_subsystems = RecordingSubsystems::rooted(full_root.path());
    let full_supervisor = ConnectionSupervisor::from_config_via(
        &channel_free_config_with_profile("full")?,
        &full_subsystems,
    )?;
    assert_eq!(
        full_subsystems.recorded(),
        vec![
            SchedulerSubsystem::ChannelSupervisor,
            SchedulerSubsystem::ConversationSupervisor,
            SchedulerSubsystem::HaematiteStore,
        ],
        "the full profile through the same constructor constructs every subsystem — \
         the positive control proving the census detects them"
    );
    assert!(
        full_supervisor
            .inner
            .runtime
            .services()
            .supports_channel_operations(),
        "the full profile must install the full services"
    );
    full_supervisor.shutdown();
    Ok(())
}

fn send_subscribe(stream: &mut TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let frame = Frame::Subscribe {
        flags: 0,
        stream_id: 1,
        channel: "orders".to_owned(),
        accepted_schemas: Vec::new(),
        max_in_flight: 1,
    };
    write_frame(stream, &frame)
}

fn read_until_subscribe_ack(stream: &mut TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let frame = read_frame(stream)?;
        if matches!(frame, Frame::SubscribeAck { .. }) {
            return Ok(());
        }
    }
    Err("timed out waiting for SubscribeAck".into())
}

fn write_frame(stream: &mut TcpStream, frame: &Frame) -> Result<(), Box<dyn std::error::Error>> {
    let frame_len = encoded_len(frame).map_err(|error| server_error_from_protocol(&error))?;
    let mut bytes = vec![0_u8; frame_len];
    let written = encode(frame, &mut bytes).map_err(|error| server_error_from_protocol(&error))?;
    stream.write_all(
        bytes
            .get(..written)
            .ok_or("encoded frame length was invalid")?,
    )?;
    Ok(())
}

fn read_frame(stream: &mut TcpStream) -> Result<Frame, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut buffer = Vec::new();
    while Instant::now() < deadline {
        let mut chunk = [0_u8; 256];
        match stream.read(&mut chunk) {
            Ok(0) => return Err("connection closed before frame arrived".into()),
            Ok(bytes_read) => buffer.extend_from_slice(&chunk[..bytes_read]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {}
            Err(error) => return Err(error.into()),
        }
        match decode(&buffer) {
            Ok((frame, _consumed)) => return Ok(frame),
            Err(
                liminal::protocol::ProtocolError::IncompleteHeader { .. }
                | liminal::protocol::ProtocolError::TruncatedPayload { .. },
            ) => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(server_error_from_protocol(&error).into()),
        }
    }
    Err("timed out waiting for frame".into())
}
