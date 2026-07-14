use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::thread;
use std::time::{Duration, Instant};

use beamr::process::ExitReason;
use liminal::conversation::ParticipantBehaviour;
use liminal::envelope::Envelope;
use liminal::protocol::{
    CONVERSATION_REPLY_REQUESTED_FLAG, CausalContext, Frame, MessageEnvelope, SchemaId, decode,
    encode, encoded_len,
};

use super::{ConnectionControl, ConnectionRuntime, ConnectionSupervisor, PushReplyAwaiter};
use crate::server::connection::services::{
    ConnectionServices, LiminalConnectionServices, server_error_from_protocol,
};

#[test]
fn connection_scheduler_inventory_has_one_readiness_poll_thread()
-> Result<(), Box<dyn std::error::Error>> {
    let supervisor = ConnectionSupervisor::new()?;
    let inventory = supervisor.scheduler().service_inventory();
    let readiness = inventory
        .iter()
        .find(|entry| entry.service == "readiness")
        .ok_or("connection scheduler readiness inventory line is missing")?;
    assert_eq!(readiness.configured, 1);
    assert_eq!(readiness.actual, 1);
    assert_eq!(readiness.thread_names, ["beamr-readiness-poll"]);
    supervisor.shutdown();
    Ok(())
}

#[test]
fn readable_ping_produces_one_pong_then_reparks() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = ConnectionSupervisor::new()?;
    let (mut client, server) = tcp_pair()?;
    let handle = supervisor.spawn_connection(server)?;
    let pid = handle.pid();
    client.set_read_timeout(Some(Duration::from_secs(2)))?;

    wait_for_slice(&supervisor, pid, 0)?;
    let parked_at = supervisor.slice_count(pid);
    thread::sleep(Duration::from_millis(50));
    assert_eq!(supervisor.slice_count(pid), parked_at);

    write_frame(&mut client, &Frame::Ping { flags: 0 })?;
    assert!(matches!(read_frame(&mut client)?, Frame::Pong { .. }));
    wait_for_slice(&supervisor, pid, parked_at)?;
    let reparks_at = supervisor.slice_count(pid);
    thread::sleep(Duration::from_millis(100));
    assert_eq!(
        supervisor.slice_count(pid),
        reparks_at,
        "readable wake must drain Pong output and repark"
    );
    supervisor.shutdown();
    Ok(())
}

#[test]
fn pre_wait_race_barrier_finds_data_arriving_after_arm() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = ConnectionSupervisor::new()?;
    let (mut client, server) = tcp_pair()?;
    let handle = supervisor.spawn_connection(server)?;
    wait_for_parked(&supervisor, handle.pid())?;
    let (armed, release) = supervisor.install_pre_wait_barrier();
    let waker = supervisor
        .ready_waker(handle.pid())
        .ok_or("live connection must have a READY waker")?;
    waker.fire();
    armed.wait();
    write_frame(&mut client, &Frame::Ping { flags: 0 })?;
    // Keep the process behind the gate until the loopback stack exposes the byte
    // to a nonblocking peek; arrival remains strictly between arm and probe.
    thread::sleep(Duration::from_millis(20));
    release.wait();

    client.set_read_timeout(Some(Duration::from_secs(2)))?;
    assert!(matches!(read_frame(&mut client)?, Frame::Pong { .. }));
    assert_eq!(
        supervisor.pre_wait_probe_hits(),
        1,
        "the post-arm socket probe must observe the staged arrival"
    );
    supervisor.shutdown();
    Ok(())
}

fn wait_for_slice(
    supervisor: &ConnectionSupervisor,
    pid: u64,
    after: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if supervisor.slice_count(pid) > after {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(5));
    }
    Err(format!(
        "connection {pid} did not service a slice after {after}; observed {}",
        supervisor.slice_count(pid)
    )
    .into())
}

fn wait_for_parked(
    supervisor: &ConnectionSupervisor,
    pid: u64,
) -> Result<u64, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let before = supervisor.slice_count(pid);
        thread::sleep(Duration::from_millis(50));
        let after = supervisor.slice_count(pid);
        if before > 0 && before == after {
            return Ok(after);
        }
    }
    Err(format!(
        "connection {pid} did not settle parked; observed {} slices",
        supervisor.slice_count(pid)
    )
    .into())
}

fn conversation_request(conversation_id: u64, stream_id: u32, payload: &[u8]) -> Frame {
    Frame::ConversationMessage {
        flags: CONVERSATION_REPLY_REQUESTED_FLAG,
        stream_id,
        conversation_id,
        envelope: MessageEnvelope::new(
            SchemaId::new([0; SchemaId::WIRE_LEN]),
            CausalContext::independent(),
            payload.to_vec(),
        ),
    }
}

#[test]
fn reply_availability_wakes_a_parked_connection() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = ConnectionSupervisor::new()?;
    let (mut client, server) = tcp_pair()?;
    let handle = supervisor.spawn_connection(server)?;
    client.set_read_timeout(Some(Duration::from_secs(2)))?;

    write_frame(
        &mut client,
        &Frame::ConversationOpen {
            flags: 0,
            stream_id: 4,
            conversation_id: 9,
            subject: "echo".to_owned(),
        },
    )?;
    wait_for_parked(&supervisor, handle.pid())?;

    write_frame(&mut client, &conversation_request(9, 17, b"reply-wake"))?;
    let frame = read_frame(&mut client)?;
    assert!(matches!(
        frame,
        Frame::ConversationMessage {
            stream_id: 17,
            conversation_id: 9,
            envelope,
            ..
        } if envelope.payload == b"reply-wake"
    ));
    supervisor.shutdown();
    Ok(())
}

#[derive(Debug)]
struct SilentBehaviour;

impl ParticipantBehaviour for SilentBehaviour {
    fn process(&self, _request: &Envelope) -> Option<Envelope> {
        None
    }
}

#[test]
fn pending_reply_deadline_is_the_only_wake_under_zero_traffic()
-> Result<(), Box<dyn std::error::Error>> {
    let services = Arc::new(LiminalConnectionServices::empty()?);
    services.register_responder("silent", Arc::new(SilentBehaviour))?;
    let supervisor = ConnectionSupervisor::with_services(services)?;
    let (mut client, server) = tcp_pair()?;
    let handle = supervisor.spawn_connection(server)?;
    client.set_read_timeout(Some(Duration::from_secs(7)))?;

    write_frame(
        &mut client,
        &Frame::ConversationOpen {
            flags: 0,
            stream_id: 3,
            conversation_id: 12,
            subject: "silent".to_owned(),
        },
    )?;
    wait_for_parked(&supervisor, handle.pid())?;

    let started = Instant::now();
    write_frame(&mut client, &conversation_request(12, 23, b"never-replied"))?;
    let frame = read_frame_with_timeout(&mut client, Duration::from_secs(7))?;
    assert!(started.elapsed() >= Duration::from_secs(4));
    assert!(matches!(
        frame,
        Frame::ConversationError {
            stream_id: 23,
            conversation_id: 12,
            message: Some(message),
            ..
        } if message.contains("timed out")
    ));
    supervisor.shutdown();
    Ok(())
}

#[test]
fn server_push_control_wakes_a_parked_process_and_reparks() -> Result<(), Box<dyn std::error::Error>>
{
    let supervisor = ConnectionSupervisor::new()?;
    let (mut client, server) = tcp_pair()?;
    let handle = supervisor.spawn_connection(server)?;
    client.set_read_timeout(Some(Duration::from_secs(2)))?;
    let parked_at = wait_for_parked(&supervisor, handle.pid())?;

    let awaiter = supervisor.push_to_connection(handle.pid(), b"parked-push".to_vec())?;
    let frame = read_frame(&mut client)?;
    let correlation_id = match frame {
        Frame::Push {
            correlation_id,
            payload,
            ..
        } => {
            assert_eq!(payload, b"parked-push");
            correlation_id
        }
        other => return Err(format!("expected Push, got {other:?}").into()),
    };
    assert_eq!(correlation_id, awaiter.correlation_id());
    wait_for_slice(&supervisor, handle.pid(), parked_at)?;

    write_frame(
        &mut client,
        &Frame::new_push_reply(1, correlation_id, b"push-reply".to_vec())?,
    )?;
    assert_eq!(
        awaiter.receive(Duration::from_secs(2))?,
        b"push-reply".to_vec()
    );
    wait_for_parked(&supervisor, handle.pid())?;
    supervisor.shutdown();
    Ok(())
}

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
fn external_kill_then_fd_reuse_survives_old_token_deregister()
-> Result<(), Box<dyn std::error::Error>> {
    let supervisor = ConnectionSupervisor::new()?;
    let (old_client, old_server) = tcp_pair()?;
    let old = supervisor.spawn_connection(old_server)?;
    wait_for_parked(&supervisor, old.pid())?;
    assert_eq!(supervisor.readiness_registration_count(), 1);
    let old_fd = supervisor
        .readiness_fd(old.pid())
        .ok_or("old connection did not publish its readiness fd")?;

    // Build the replacement pair while `old_fd` is still owned by the old process.
    // Once the drop event arrives, the only fd allocation this test performs is the
    // clone loop that claims the newly reusable number.
    let (mut new_client, new_server_original) = tcp_pair()?;
    let old_stream_dropped = supervisor.observe_process_stream_drop(old_fd);
    supervisor
        .scheduler()
        .terminate_process(old.pid(), ExitReason::Error);
    old_stream_dropped
        .recv_timeout(Duration::from_secs(2))
        .map_err(|error| format!("externally killed process did not drop its stream: {error}"))?;
    assert!(!old.is_live());

    // Hold clone descriptors below `old_fd` until the allocator reaches the exact
    // externally-freed number. Full-suite concurrency may leave unrelated lower
    // holes, so a single clone is not deterministic.
    let mut fillers = Vec::new();
    let new_server = loop {
        let candidate = new_server_original.try_clone()?;
        let candidate_fd = candidate.as_raw_fd();
        if candidate_fd == old_fd {
            break candidate;
        }
        if candidate_fd > old_fd {
            return Err(format!("fd allocator skipped reusable descriptor {old_fd}").into());
        }
        fillers.push(candidate);
    };
    drop(new_server_original);
    let new = supervisor.spawn_connection(new_server)?;
    drop(fillers);
    wait_for_parked(&supervisor, new.pid())?;
    assert_eq!(supervisor.readiness_fd(new.pid()), Some(old_fd));
    assert_eq!(supervisor.readiness_registration_count(), 2);

    assert_eq!(supervisor.reap_crashed_connections(), 1);
    assert_eq!(supervisor.readiness_registration_count(), 1);
    new_client.set_read_timeout(Some(Duration::from_secs(2)))?;
    write_frame(&mut new_client, &Frame::Ping { flags: 0 })?;
    assert!(matches!(read_frame(&mut new_client)?, Frame::Pong { .. }));
    assert!(new.is_live());

    drop(old_client);
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
fn shutdown_control_wakes_a_parked_subscriber_without_closing_it()
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
    let parked_at = wait_for_parked(&supervisor, handle.pid())?;

    supervisor.notify_shutdown_subscribers();
    let frame = read_frame(&mut client)?;
    wait_for_slice(&supervisor, handle.pid(), parked_at)?;

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
        _install: Option<liminal::channel::InboxInstall>,
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
    runtime: &std::sync::Arc<ConnectionRuntime>,
    pid: u64,
) -> Result<PushReplyAwaiter, Box<dyn std::error::Error>> {
    outstanding_push_with_deadline(runtime, pid, None)
}

/// Like [`outstanding_push`] but attaches an explicit absolute reply `deadline`,
/// mirroring `push_to_connection_with_deadline`'s slot bookkeeping without a live
/// scheduler.
fn outstanding_push_with_deadline(
    runtime: &std::sync::Arc<ConnectionRuntime>,
    pid: u64,
    deadline: Option<Instant>,
) -> Result<PushReplyAwaiter, Box<dyn std::error::Error>> {
    let correlation_id = runtime.next_push_correlation_id();
    let receiver = runtime.register_push(pid, correlation_id, deadline)?;
    Ok(PushReplyAwaiter {
        correlation_id,
        receiver,
        deadline,
        runtime: std::sync::Arc::downgrade(runtime),
    })
}

/// Reads the next frame off `client`, asserting it is a `Push`, and returns its
/// correlation id (helper for the public push-path e2e tests).
fn expect_push_correlation(client: &mut TcpStream) -> Result<u64, Box<dyn std::error::Error>> {
    match read_frame(client)? {
        Frame::Push { correlation_id, .. } => Ok(correlation_id),
        other => Err(format!("expected Push, got {other:?}").into()),
    }
}

#[test]
fn elapsed_receive_polls_are_benign_rearms() -> Result<(), Box<dyn std::error::Error>> {
    // THE regression pin (unit level). The restored 0.2.3 contract: a caller's
    // poll quantum never changes the protocol outcome. An elapsed `receive` on a
    // no-deadline push returns the timeout variant WITHOUT cancelling the slot,
    // and the slot survives every poll so a later reply is still delivered. The
    // D4-wave cancel-on-timeout (this test's former assertion, `pending_push_count
    // == 0`, now INVERTED) is exactly the regression: it cancelled the slot on the
    // first poll, so an aion re-arm loop saw the sender dropped and reported a
    // false lost-worker at poll+epsilon.
    let runtime = std::sync::Arc::new(ConnectionRuntime::for_tests(std::sync::Arc::new(
        FlushFailingServices,
    )));
    let pid = 41;
    runtime.register(pid, None)?;
    let awaiter = outstanding_push(&runtime, pid)?;
    assert_eq!(runtime.pending_push_count(), 1, "the slot is reserved");

    // Five short elapsed polls, no reply: every poll is a benign timeout and the
    // reserved slot is untouched each time — no cancellation, no disconnect.
    for poll in 0..5 {
        let result = awaiter.receive(Duration::from_millis(10));
        assert!(
            matches!(result, Err(crate::ServerError::PushReplyTimeout { .. })),
            "poll {poll}: an elapsed quantum must be a benign timeout, got {result:?}"
        );
        assert_eq!(
            runtime.pending_push_count(),
            1,
            "poll {poll}: the elapsed quantum must NOT cancel the reserved slot"
        );
    }

    // The reply finally arrives; the SAME awaiter, re-armed, receives it byte-exact
    // and only then is the slot freed. Re-arming after timeout works indefinitely.
    let reply = b"eventual-reply".to_vec();
    runtime.resolve_push(awaiter.correlation_id(), reply.clone());
    assert_eq!(
        awaiter.receive(Duration::from_millis(200))?,
        reply,
        "the reply survives the elapsed polls and is delivered on re-arm"
    );
    assert_eq!(
        runtime.pending_push_count(),
        0,
        "the consumed reply frees the slot"
    );
    Ok(())
}

#[test]
fn closing_connection_wakes_outstanding_push_awaiter_promptly()
-> Result<(), Box<dyn std::error::Error>> {
    // Headline: a server push is outstanding (awaiter created, no reply sent), then
    // the connection closes. The awaiter must wake IMMEDIATELY with a disconnected
    // error rather than blocking the full push-reply timeout. We pass a 30s timeout
    // and assert the call returns well under it — proving cancellation, not timeout.
    let runtime = std::sync::Arc::new(ConnectionRuntime::for_tests(std::sync::Arc::new(
        FlushFailingServices,
    )));
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
    let runtime = std::sync::Arc::new(ConnectionRuntime::for_tests(std::sync::Arc::new(
        FlushFailingServices,
    )));
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
    let runtime = std::sync::Arc::new(ConnectionRuntime::for_tests(std::sync::Arc::new(
        FlushFailingServices,
    )));
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
    let runtime = std::sync::Arc::new(ConnectionRuntime::for_tests(std::sync::Arc::new(
        FlushFailingServices,
    )));
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

/// D4: a connection torn down with conversations still open must close them so no
/// conversation actor or participant leaks. The connection here dies by EOF
/// (client dropped) with two conversations open; the teardown path must return the
/// conversation supervisor's scheduler to its pre-open process count.
#[test]
fn eof_hup_deregisters_readiness_and_closes_open_conversations()
-> Result<(), Box<dyn std::error::Error>> {
    let services = std::sync::Arc::new(LiminalConnectionServices::empty()?);
    let conv_scheduler = services.conversation_supervisor().scheduler();
    let baseline = conv_scheduler.process_table().len();

    let supervisor = ConnectionSupervisor::with_services(services.clone())?;
    let (client, server) = tcp_pair()?;
    let handle = supervisor.spawn_connection(server)?;
    wait_for_parked(&supervisor, handle.pid())?;
    assert_eq!(supervisor.readiness_registration_count(), 1);

    // Open two conversations over the connection; each spawns an actor, a
    // participant, and an exit watcher, so the conversation scheduler gains six
    // processes.
    let mut writer = client;
    write_frame(&mut writer, &conversation_open_frame(1, "subject-1"))?;
    write_frame(&mut writer, &conversation_open_frame(2, "subject-2"))?;
    wait_for_process_count(&conv_scheduler, baseline + 6)?;

    // Abrupt teardown: drop the client so the connection reads EOF and finalizes.
    drop(writer);
    wait_for_cleanup(&supervisor, handle.pid())?;
    assert_eq!(supervisor.readiness_registration_count(), 0);

    // Teardown must have finalized both conversations: zero leaked
    // actors/participants in the process table AND in both lifecycle registries.
    wait_for_process_count(&conv_scheduler, baseline)?;
    assert_eq!(
        services.conversation_supervisor().registered_actor_count(),
        0
    );
    assert_eq!(
        services
            .conversation_supervisor()
            .registered_participant_count(),
        0
    );
    assert_eq!(supervisor.active_connection_count(), 0);

    supervisor.shutdown();
    services.conversation_supervisor().shutdown();
    Ok(())
}

/// D4 churn gate (connections): repeated connect / open-conversation / abrupt-close
/// cycles must leave the conversation scheduler bounded — every cycle returns to
/// baseline rather than accumulating leaked actors/participants.
#[test]
fn connection_churn_greater_than_worker_count_remains_bounded()
-> Result<(), Box<dyn std::error::Error>> {
    let services = std::sync::Arc::new(LiminalConnectionServices::empty()?);
    let conv_scheduler = services.conversation_supervisor().scheduler();
    let baseline = conv_scheduler.process_table().len();
    let supervisor = ConnectionSupervisor::with_services(services.clone())?;

    for cycle in 0..8 {
        let (client, server) = tcp_pair()?;
        let handle = supervisor.spawn_connection(server)?;
        let mut writer = client;
        write_frame(
            &mut writer,
            &conversation_open_frame(cycle, "churn-subject"),
        )?;
        wait_for_process_count(&conv_scheduler, baseline + 3)?;
        drop(writer);
        wait_for_cleanup(&supervisor, handle.pid())?;
        wait_for_process_count(&conv_scheduler, baseline)?;
    }

    assert_eq!(conv_scheduler.process_table().len(), baseline);
    assert_eq!(
        services.conversation_supervisor().registered_actor_count(),
        0
    );
    assert_eq!(
        services
            .conversation_supervisor()
            .registered_participant_count(),
        0
    );
    assert_eq!(supervisor.active_connection_count(), 0);

    supervisor.shutdown();
    services.conversation_supervisor().shutdown();
    Ok(())
}

/// D4 rework minor-2(a): a connection terminated EXTERNALLY (never running
/// another slice — the reap-class route, covered by the handler's `Drop`
/// backstop) with conversations open must still return the conversation
/// scheduler's process table AND both lifecycle registries to baseline. All
/// waits are deadline-bounded, so a hang fails instead of wedging CI.
#[test]
fn externally_terminated_connection_with_open_conversations_returns_to_baseline()
-> Result<(), Box<dyn std::error::Error>> {
    let services = std::sync::Arc::new(LiminalConnectionServices::empty()?);
    let conv_scheduler = services.conversation_supervisor().scheduler();
    let baseline = conv_scheduler.process_table().len();
    let supervisor = ConnectionSupervisor::with_services(services.clone())?;
    let (client, server) = tcp_pair()?;
    let handle = supervisor.spawn_connection(server)?;

    let mut writer = client;
    write_frame(&mut writer, &conversation_open_frame(1, "ext-subject-1"))?;
    write_frame(&mut writer, &conversation_open_frame(2, "ext-subject-2"))?;
    wait_for_process_count(&conv_scheduler, baseline + 6)?;

    // External termination: beamr cleans the process up outside the handler's
    // slice path; only the Drop backstop can release the conversations.
    supervisor
        .scheduler()
        .terminate_process(handle.pid(), ExitReason::Error);
    wait_for_cleanup(&supervisor, handle.pid())?;

    wait_for_process_count(&conv_scheduler, baseline)?;
    assert_eq!(
        services.conversation_supervisor().registered_actor_count(),
        0
    );
    assert_eq!(
        services
            .conversation_supervisor()
            .registered_participant_count(),
        0
    );

    drop(writer);
    supervisor.shutdown();
    services.conversation_supervisor().shutdown();
    Ok(())
}

/// D4 rework minor-2(b) — the blocker's sharpest consequence pinned: releasing a
/// connection's conversations AFTER the conversation scheduler has stopped must
/// return promptly. Finalization terminates processes and clears registries
/// through direct scheduler-state writes, never a request into a conversation
/// slice, so a stopped (or wedged) conversation scheduler cannot hang connection
/// teardown. A watchdog bounds the whole release; a hang FAILS the test.
#[test]
fn connection_cleanup_after_conversation_scheduler_shutdown_is_prompt()
-> Result<(), Box<dyn std::error::Error>> {
    let services = std::sync::Arc::new(LiminalConnectionServices::empty()?);
    let conv_scheduler = services.conversation_supervisor().scheduler();
    let baseline = conv_scheduler.process_table().len();
    let supervisor = ConnectionSupervisor::with_services(services.clone())?;
    let (client, server) = tcp_pair()?;
    let handle = supervisor.spawn_connection(server)?;

    let mut writer = client;
    write_frame(&mut writer, &conversation_open_frame(7, "stopped-subject"))?;
    let conversation_supervisor = services.conversation_supervisor();
    let deadline = Instant::now() + Duration::from_secs(5);
    while conversation_supervisor.registered_actor_count() < 1 {
        if Instant::now() > deadline {
            return Err("conversation was not opened in time".into());
        }
        thread::sleep(Duration::from_millis(10));
    }

    // Stop the conversation scheduler FIRST, then tear the connection down. The
    // release must not depend on any conversation slice ever running again.
    conversation_supervisor.shutdown();

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let scheduler = supervisor.scheduler();
    let pid = handle.pid();
    let watchdog = thread::spawn(move || {
        scheduler.terminate_process(pid, ExitReason::Error);
        done_tx.send(()).ok();
    });
    done_rx
        .recv_timeout(Duration::from_secs(10))
        .map_err(|_| "connection teardown hung against a stopped conversation scheduler")?;
    watchdog
        .join()
        .map_err(|_| "teardown watchdog thread panicked")?;

    // Finalization deregisters everything AND terminates the actor, the
    // participant, and the exit watcher directly (scheduler tombstone writes),
    // so the process table returns to BASELINE even though the stopped
    // scheduler never runs another slice. The handler `Drop` that runs it lands
    // at the busy-looping connection slice's store-back, so the wait is
    // deadline-bounded rather than instant.
    let deadline = Instant::now() + Duration::from_secs(5);
    while conversation_supervisor.registered_actor_count() != 0
        || conversation_supervisor.registered_participant_count() != 0
        || conv_scheduler.process_table().len() != baseline
    {
        if Instant::now() > deadline {
            return Err(format!(
                "residue against a stopped conversation scheduler: actors={} \
                 participants={} table={} (baseline {baseline})",
                conversation_supervisor.registered_actor_count(),
                conversation_supervisor.registered_participant_count(),
                conv_scheduler.process_table().len()
            )
            .into());
        }
        thread::sleep(Duration::from_millis(10));
    }

    drop(writer);
    supervisor.shutdown();
    Ok(())
}

/// Push resolve/expiry race, staged deterministically with the slot-registry
/// mutex as the barrier — DEADLINED awaiter (only the explicit-deadline path
/// consults the registry; a no-deadline receive never touches it, see
/// `no_deadline_receive_never_blocks_on_registry_lock`). The awaiter's deadline
/// falls due while the resolver holds the registry lock, so its `expire_slot`
/// blocks; the resolver then wins the transition (remove + send under the lock,
/// exactly what `resolve_push` does) before releasing it. The awaiter must
/// return the delivered reply — not an expiry for an answer that arrived — and
/// the slot must be gone.
#[test]
fn resolver_winning_the_expiry_race_delivers_the_reply() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = std::sync::Arc::new(ConnectionRuntime::for_tests(std::sync::Arc::new(
        FlushFailingServices,
    )));
    let pid = 51;
    runtime.register(pid, None)?;
    let awaiter = outstanding_push_with_deadline(
        &runtime,
        pid,
        Some(Instant::now() + Duration::from_millis(10)),
    )?;
    let correlation_id = awaiter.correlation_id();
    let reply = b"late-but-won".to_vec();

    // Hold the slot-registry lock: the awaiter's due-deadline `expire_slot`
    // (via `expire_push_if_due`) cannot proceed until the resolver below has
    // finished, so the interleaving is enforced by the mutex, not by sleeps.
    let mut slots = runtime
        .push_replies
        .lock()
        .map_err(|error| format!("slot registry lock poisoned: {error}"))?;

    let awaiter_thread = thread::spawn(move || awaiter.receive(Duration::from_millis(500)));

    // Let the awaiter's deadline fall due; it then blocks on the held lock.
    // (The sleep only makes the block observable — the outcome is lock-ordered
    // and identical even if the awaiter arrives later.)
    thread::sleep(Duration::from_millis(100));

    // The resolver's winning transition, under the same lock `resolve_push`
    // uses: remove the slot, send the payload before releasing.
    let pending = slots
        .remove(&correlation_id)
        .ok_or("the reserved slot must still be present")?;
    pending.sender.send(reply.clone()).ok();
    drop(slots);

    let received = awaiter_thread
        .join()
        .map_err(|_| "awaiter thread panicked")?
        .map_err(|error| format!("awaiter must return the delivered reply, got {error}"))?;
    assert_eq!(received, reply);
    assert_eq!(runtime.pending_push_count(), 0, "the slot must be gone");
    Ok(())
}

/// S2 pin: the no-deadline receive path NEVER touches the slot registry — it is
/// behaviour-compatible with 0.2.3 (one bounded channel wait). Staged by holding
/// the registry lock for the whole call: the receive must still return its
/// benign timeout near its quantum (proven by observing the result WHILE the
/// lock is held), and a later re-arm still gets the lock-ordered reply.
#[test]
fn no_deadline_receive_never_blocks_on_registry_lock() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = std::sync::Arc::new(ConnectionRuntime::for_tests(std::sync::Arc::new(
        FlushFailingServices,
    )));
    let pid = 53;
    runtime.register(pid, None)?;
    let awaiter = outstanding_push(&runtime, pid)?;
    let correlation_id = awaiter.correlation_id();

    // Hold the registry lock across the entire receive call.
    let mut slots = runtime
        .push_replies
        .lock()
        .map_err(|error| format!("slot registry lock poisoned: {error}"))?;

    let (outcome_tx, outcome_rx) = channel();
    let awaiter_thread = thread::spawn(move || {
        let outcome = awaiter.receive(Duration::from_millis(50));
        // Report the variant while the parent still holds the lock.
        outcome_tx
            .send(matches!(
                outcome,
                Err(crate::ServerError::PushReplyTimeout { .. })
            ))
            .ok();
        awaiter
    });

    // Received WHILE the lock is held: if the no-deadline path ever consulted
    // the registry, this bounded wait would elapse instead (clean failure).
    let benign = outcome_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| "no-deadline receive blocked on the held registry lock past its quantum")?;
    assert!(
        benign,
        "the elapsed quantum must be a benign timeout, untouched by the held lock"
    );

    // Lock-ordered reply, exactly as `resolve_push` does, then release.
    let pending = slots
        .remove(&correlation_id)
        .ok_or("the benign timeout must have left the slot reserved")?;
    pending.sender.send(b"lock-ordered".to_vec()).ok();
    drop(slots);

    // A later re-arm still gets the lock-ordered reply.
    let awaiter = awaiter_thread
        .join()
        .map_err(|_| "awaiter thread panicked")?;
    assert_eq!(
        awaiter.receive(Duration::from_millis(200))?,
        b"lock-ordered"
    );
    assert_eq!(runtime.pending_push_count(), 0);
    Ok(())
}

/// Test (b): an explicit reply deadline resolves the slot to a TYPED expiry on
/// the next receive after the deadline, removes the slot, and releases its §5
/// `max_pending_pushes_per_connection` cap admission — proven by a follow-up push
/// admitting at the cap boundary that would have been refused had the expired
/// slot leaked. Deadline evaluation is host-side and lazy: it fires here at the
/// receive touch, waking no process and running no timer.
#[test]
fn explicit_deadline_expires_slot_and_releases_cap() -> Result<(), Box<dyn std::error::Error>> {
    // Cap of one in-flight push per connection: the boundary is trivially
    // observable — a second concurrent push is refused, but a push after the
    // first has EXPIRED must admit.
    let limits = crate::config::types::LimitsConfig {
        max_pending_pushes_per_connection: 1,
        ..crate::config::types::LimitsConfig::default()
    };
    let runtime = std::sync::Arc::new(ConnectionRuntime::for_tests_with_limits(
        std::sync::Arc::new(FlushFailingServices),
        limits,
    ));
    let pid = 61;
    runtime.register(pid, None)?;

    // A push with a very short deadline, already elapsed by the time we poll.
    let deadline = Instant::now() + Duration::from_millis(20);
    let awaiter = outstanding_push_with_deadline(&runtime, pid, Some(deadline))?;
    assert_eq!(
        runtime.pending_push_count(),
        1,
        "the deadlined slot is reserved"
    );

    // At the cap: a second push while the first is still in-flight is refused.
    let refused = outstanding_push_with_deadline(&runtime, pid, None);
    assert!(
        refused.is_err(),
        "a second push at the cap must be refused while the first is in flight"
    );

    // Poll after the deadline: the slot resolves to a typed expiry and is removed.
    thread::sleep(Duration::from_millis(40));
    let result = awaiter.receive(Duration::from_millis(10));
    assert!(
        matches!(result, Err(crate::ServerError::PushReplyExpired { .. })),
        "a passed reply deadline must yield a typed PushReplyExpired, got {result:?}"
    );
    assert_eq!(
        runtime.pending_push_count(),
        0,
        "expiry removes the slot (releasing its cap admission)"
    );

    // The cap admission is released: a follow-up push admits at the boundary.
    let follow_up = outstanding_push_with_deadline(&runtime, pid, None);
    assert!(
        follow_up.is_ok(),
        "the expired slot's cap admission must be released so a follow-up push admits"
    );
    Ok(())
}

/// Test (c) + item 4: a late `PushReply` for a slot that already expired (or that
/// never existed) is a harmless no-op — nothing is delivered, no panic, no
/// desync. Pins `resolve_push`'s missing-slot benign discard for the expiry case.
#[test]
fn late_reply_after_expiry_is_a_harmless_noop() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = std::sync::Arc::new(ConnectionRuntime::for_tests(std::sync::Arc::new(
        FlushFailingServices,
    )));
    let pid = 62;
    runtime.register(pid, None)?;

    let deadline = Instant::now() + Duration::from_millis(10);
    let awaiter = outstanding_push_with_deadline(&runtime, pid, Some(deadline))?;
    let correlation_id = awaiter.correlation_id();
    thread::sleep(Duration::from_millis(30));
    assert!(matches!(
        awaiter.receive(Duration::from_millis(5)),
        Err(crate::ServerError::PushReplyExpired { .. })
    ));
    assert_eq!(runtime.pending_push_count(), 0, "expiry removed the slot");

    // A late reply for the now-removed slot: benign discard, no panic, no desync.
    runtime.resolve_push(correlation_id, b"too-late".to_vec());
    assert_eq!(
        runtime.pending_push_count(),
        0,
        "a late reply must not resurrect or leak a slot"
    );

    // A reply for a correlation id that never had a slot is equally harmless.
    runtime.resolve_push(9_999, b"never-existed".to_vec());
    assert_eq!(runtime.pending_push_count(), 0);
    Ok(())
}

/// Observation-point expiry pin: the deadline is evaluated at observation
/// points, not enforced against the wall clock. A reply that is delivered AFTER
/// the deadline instant but BEFORE any receive observes the slot wins — the
/// payload is returned, not `PushReplyExpired`. This locks the deliberate
/// reply-before-deadline check ordering in `on_quantum_elapsed`/`receive`
/// against a future eager-expiry "fix": the deadline bounds waiting and slot
/// occupancy, it is not a delivery-freshness guarantee.
#[test]
fn reply_delivered_after_deadline_instant_beats_unobserved_expiry()
-> Result<(), Box<dyn std::error::Error>> {
    let runtime = std::sync::Arc::new(ConnectionRuntime::for_tests(std::sync::Arc::new(
        FlushFailingServices,
    )));
    let pid = 65;
    runtime.register(pid, None)?;

    let deadline = Instant::now() + Duration::from_millis(10);
    let awaiter = outstanding_push_with_deadline(&runtime, pid, Some(deadline))?;

    // Let the deadline instant pass with NO observation (no receive call).
    thread::sleep(Duration::from_millis(30));

    // The reply lands after the deadline instant but before any observation.
    let reply = b"late-but-unobserved".to_vec();
    runtime.resolve_push(awaiter.correlation_id(), reply.clone());
    assert_eq!(
        runtime.pending_push_count(),
        0,
        "the resolver consumed the slot — lazy expiry never raced it"
    );

    // The first observation finds the payload: delivered, not Expired.
    let received = awaiter.receive(Duration::from_millis(100))?;
    assert_eq!(
        received, reply,
        "a reply delivered before expiry is observed must win over the deadline"
    );
    Ok(())
}

/// S1 pin (i): identical deadlined pushes under the same schedule produce the
/// SAME terminal outcome regardless of the receive quantum — including the
/// blocker's exact shape: an already-due slot with a reply arriving later must
/// expire under receive(ZERO) and receive(60s) alike, never letting a large
/// quantum extend reply eligibility past the push's deadline.
#[test]
fn same_deadlined_schedule_yields_same_outcome_for_any_quantum()
-> Result<(), Box<dyn std::error::Error>> {
    let runtime = std::sync::Arc::new(ConnectionRuntime::for_tests(std::sync::Arc::new(
        FlushFailingServices,
    )));
    let pid = 66;
    runtime.register(pid, None)?;

    // Schedule A — no reply ever, deadline D: a short-quanta poller and a
    // single-long-quantum caller must both end in Expired.
    let deadline = Instant::now() + Duration::from_millis(40);
    let short_polls = outstanding_push_with_deadline(&runtime, pid, Some(deadline))?;
    let one_long = outstanding_push_with_deadline(&runtime, pid, Some(deadline))?;
    let mut short_terminal = None;
    for _ in 0..200 {
        match short_polls.receive(Duration::from_millis(10)) {
            Err(crate::ServerError::PushReplyTimeout { .. }) => {}
            other => {
                short_terminal = Some(other);
                break;
            }
        }
    }
    assert!(
        matches!(
            short_terminal,
            Some(Err(crate::ServerError::PushReplyExpired { .. }))
        ),
        "short quanta must reach the typed expiry, got {short_terminal:?}"
    );
    let long_outcome = one_long.receive(Duration::from_secs(60));
    assert!(
        matches!(
            long_outcome,
            Err(crate::ServerError::PushReplyExpired { .. })
        ),
        "one long quantum must reach the SAME typed expiry, got {long_outcome:?}"
    );

    // Schedule B (the blocker's scenario): slot already due at entry, no reply
    // in hand, reply arriving later. receive(ZERO) and receive(60s) must agree:
    // both expire; the late reply is a discard for both.
    let due = Instant::now();
    let zero_quantum = outstanding_push_with_deadline(&runtime, pid, Some(due))?;
    let long_quantum = outstanding_push_with_deadline(&runtime, pid, Some(due))?;
    let zero_outcome = zero_quantum.receive(Duration::ZERO);
    assert!(
        matches!(
            zero_outcome,
            Err(crate::ServerError::PushReplyExpired { .. })
        ),
        "receive(ZERO) on a due slot must expire it, got {zero_outcome:?}"
    );
    let started = Instant::now();
    let long_due_outcome = long_quantum.receive(Duration::from_secs(60));
    assert!(
        matches!(
            long_due_outcome,
            Err(crate::ServerError::PushReplyExpired { .. })
        ),
        "receive(60s) on a due slot must expire it identically, got {long_due_outcome:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the due-slot expiry must be prompt, not held for the 60s quantum"
    );
    // The late replies find no slots: discarded for both consumers alike.
    runtime.resolve_push(zero_quantum.correlation_id(), b"too-late".to_vec());
    runtime.resolve_push(long_quantum.correlation_id(), b"too-late".to_vec());
    assert_eq!(runtime.pending_push_count_for(pid), 0);

    // Schedule C — reply resolved before any observation: every quantum
    // delivers the identical payload.
    let far = Instant::now() + Duration::from_secs(60);
    let short_reply = outstanding_push_with_deadline(&runtime, pid, Some(far))?;
    let long_reply = outstanding_push_with_deadline(&runtime, pid, Some(far))?;
    runtime.resolve_push(short_reply.correlation_id(), b"same".to_vec());
    runtime.resolve_push(long_reply.correlation_id(), b"same".to_vec());
    assert_eq!(short_reply.receive(Duration::ZERO)?, b"same");
    assert_eq!(long_reply.receive(Duration::from_secs(60))?, b"same");
    Ok(())
}

/// S1 pin (ii): a deadlined receive is bounded by the deadline, not the caller's
/// quantum — an in-flight long-quantum call returns the typed expiry near the
/// deadline (mid-wait wake), and an already-overdue slot expires at entry.
#[test]
fn overdue_deadlined_receive_returns_expired_promptly() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = std::sync::Arc::new(ConnectionRuntime::for_tests(std::sync::Arc::new(
        FlushFailingServices,
    )));
    let pid = 67;
    runtime.register(pid, None)?;

    // Mid-wait: the deadline falls due DURING a 30s quantum; receive wakes at
    // the deadline and returns the terminal expiry promptly.
    let awaiter = outstanding_push_with_deadline(
        &runtime,
        pid,
        Some(Instant::now() + Duration::from_millis(50)),
    )?;
    let started = Instant::now();
    let outcome = awaiter.receive(Duration::from_secs(30));
    let elapsed = started.elapsed();
    assert!(
        matches!(outcome, Err(crate::ServerError::PushReplyExpired { .. })),
        "a deadline falling due mid-quantum must return the typed expiry, got {outcome:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "expiry must arrive near the 50ms deadline, not the 30s quantum (took {elapsed:?})"
    );
    assert_eq!(runtime.pending_push_count_for(pid), 0);

    // Already overdue at entry: a long quantum returns the expiry immediately.
    let overdue = outstanding_push_with_deadline(&runtime, pid, Some(Instant::now()))?;
    let started = Instant::now();
    let outcome = overdue.receive(Duration::from_secs(30));
    assert!(
        matches!(outcome, Err(crate::ServerError::PushReplyExpired { .. })),
        "an overdue slot must expire at receive entry, got {outcome:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "an entry expiry must not wait out any of the quantum"
    );
    Ok(())
}

/// S3 pin: the close-vs-register race can never strand a slot or its cap
/// admission, staged at each of the three interleavings of `remove`'s
/// record-removal-BEFORE-sweep ordering against registration's INSERT ->
/// CONFIRM -> PUBLISH sequence. Asserts the typed awaiter outcome and the exact
/// per-pid slot count after every winner.
///
/// EVIDENCE HONESTY: this staging drives the runtime seams directly — the slot
/// insert (`register_push` via the test helper) and `confirm_push_registration`
/// — with close's two steps interleaved between them; no control is actually
/// enqueued here. The insert->confirm->PUBLISH ordering and its publication
/// invariant are pinned separately
/// (`close_between_insert_and_confirm_publishes_nothing` and
/// `err_from_public_push_publishes_nothing_after_close`).
#[test]
fn close_racing_push_registration_never_strands_slot_or_cap()
-> Result<(), Box<dyn std::error::Error>> {
    let runtime = std::sync::Arc::new(ConnectionRuntime::for_tests(std::sync::Arc::new(
        FlushFailingServices,
    )));

    // Interleaving A — confirm BEFORE the record removal: the push stands (in
    // production it then publishes); the later close's sweep (which runs after
    // its record removal) reaps the slot and the awaiter disconnects.
    let pid_a = 71;
    runtime.register(pid_a, None)?;
    let awaiter_a = outstanding_push(&runtime, pid_a)?;
    assert!(
        runtime.confirm_push_registration(pid_a, awaiter_a.correlation_id()),
        "a live record must confirm the registration"
    );
    runtime.finish(pid_a);
    assert_eq!(
        runtime.pending_push_count_for(pid_a),
        0,
        "the close sweep must reap the confirmed slot"
    );
    let outcome_a = awaiter_a.receive(Duration::from_secs(5));
    assert!(
        matches!(
            outcome_a,
            Err(crate::ServerError::PushReplyDisconnected { .. })
        ),
        "interleaving A must end disconnected, got {outcome_a:?}"
    );

    // Interleaving B — the exact S3 leak Sol proved, staged INSIDE close's two
    // steps: the record is already removed (close's first step) but the sweep
    // (its second step) has not run when the push inserts its slot. The confirm
    // must observe the missing record and roll the slot back itself — in
    // production this happens BEFORE any control is enqueued (S7), so the `Err`
    // coexists with nothing published.
    let pid_b = 72;
    runtime.register(pid_b, None)?;
    runtime
        .records
        .lock()
        .map_err(|error| format!("records lock poisoned: {error}"))?
        .remove(&pid_b); // close, step 1 of 2 (record removal)
    let awaiter_b = outstanding_push(&runtime, pid_b)?; // push admits its slot
    assert_eq!(runtime.pending_push_count_for(pid_b), 1);
    assert!(
        !runtime.confirm_push_registration(pid_b, awaiter_b.correlation_id()),
        "the confirm must observe the removed record"
    );
    assert_eq!(
        runtime.pending_push_count_for(pid_b),
        0,
        "the failed confirm must roll back its own slot — nothing strands"
    );
    runtime.cancel_pushes_for_connection(pid_b); // close, step 2 (sweep): no-op now
    assert_eq!(runtime.pending_push_count_for(pid_b), 0);
    let outcome_b = awaiter_b.receive(Duration::from_secs(5));
    assert!(
        matches!(
            outcome_b,
            Err(crate::ServerError::PushReplyDisconnected { .. })
        ),
        "interleaving B's awaiter must disconnect, got {outcome_b:?}"
    );

    // Interleaving C — close fully completed before the push registers: the
    // confirm rolls back immediately (and production never publishes).
    let pid_c = 73;
    runtime.register(pid_c, None)?;
    runtime.finish(pid_c);
    let awaiter_c = outstanding_push(&runtime, pid_c)?;
    assert!(!runtime.confirm_push_registration(pid_c, awaiter_c.correlation_id()));
    assert_eq!(runtime.pending_push_count_for(pid_c), 0);
    Ok(())
}

/// S7 pin (publication race, both directions, staged deterministically at the
/// runtime level between registration's steps):
///
/// Err side — close lands between INSERT and CONFIRM: the failed confirm rolls
/// the slot back and (in the production INSERT -> CONFIRM -> PUBLISH order) the
/// control is never enqueued, so `Err` coexists with NO published control and
/// no delivered push. Ok side — close lands AFTER a successful confirm (i.e.
/// after push admission): the sweep reaps the slot, the awaiter reads the
/// truthful DISCONNECTED, a late client reply is the pinned harmless no-op, and
/// zero slots are left. Under the pre-fix order (INSERT -> PUBLISH -> CONFIRM)
/// the Err side could return failure for a push the client had already received
/// and answered — the discarded lock-ordered reply Sol proved.
#[test]
fn close_between_insert_and_confirm_publishes_nothing() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = std::sync::Arc::new(ConnectionRuntime::for_tests(std::sync::Arc::new(
        FlushFailingServices,
    )));

    // Err side: INSERT, then the full close (record removal + sweep), then
    // CONFIRM — the pause point is exactly between registration's two steps.
    let pid = 76;
    runtime.register(pid, None)?;
    let awaiter = outstanding_push(&runtime, pid)?; // INSERT
    runtime.finish(pid); // close runs across the pause (removal, then sweep)
    assert!(
        !runtime.confirm_push_registration(pid, awaiter.correlation_id()),
        "CONFIRM after the close must fail"
    );
    // Production returns Err HERE, before ConnectionControl::Push is built or
    // enqueued: no control exists for this pid and no slot is left.
    assert!(
        !runtime.has_control(pid),
        "a failed confirm must precede any published control"
    );
    assert_eq!(runtime.pending_push_count_for(pid), 0);
    let outcome = awaiter.receive(Duration::from_secs(5));
    assert!(
        matches!(
            outcome,
            Err(crate::ServerError::PushReplyDisconnected { .. })
        ),
        "the rolled-back slot reads disconnected, got {outcome:?}"
    );

    // Ok side: INSERT + successful CONFIRM (push admitted), then the close
    // lands before/while the published Push is answered. The sweep reaps the
    // slot; the client's late reply is a harmless no-op; the awaiter reads the
    // truthful disconnected outcome; zero slots left.
    let pid_ok = 77;
    runtime.register(pid_ok, None)?;
    let admitted = outstanding_push(&runtime, pid_ok)?;
    assert!(runtime.confirm_push_registration(pid_ok, admitted.correlation_id()));
    runtime.finish(pid_ok); // close linearizes AFTER push admission
    runtime.resolve_push(admitted.correlation_id(), b"late-reply".to_vec()); // no-op
    assert_eq!(runtime.pending_push_count_for(pid_ok), 0, "zero slots left");
    let admitted_outcome = admitted.receive(Duration::from_secs(5));
    assert!(
        matches!(
            admitted_outcome,
            Err(crate::ServerError::PushReplyDisconnected { .. })
        ),
        "a push admitted before the close reads the truthful disconnect, got {admitted_outcome:?}"
    );
    Ok(())
}

/// S7 pin (public path, real connection): an `Err` from `push_to_connection`
/// against a connection whose host-side close half already ran (record removed,
/// slots swept — the S7 window at its widest, staged deterministically by
/// running that half directly while the beamr process is still live) publishes
/// NOTHING: the client's socket carries no `Push` frame and zero slots remain.
///
/// EVIDENCE HONESTY: the mid-flight pause (close landing between the public
/// call's own insert and confirm) is not deterministically stageable over the
/// live scheduler without a test-only hook in `push_with_deadline`; that exact
/// interleaving is pinned at the runtime level in
/// `close_between_insert_and_confirm_publishes_nothing`. This test pins the
/// public-API face of the invariant: Err => nothing on the wire.
#[test]
fn err_from_public_push_publishes_nothing_after_close() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = ConnectionSupervisor::new()?;
    let (mut client, server) = tcp_pair()?;
    let handle = supervisor.spawn_connection(server)?;
    let pid = handle.pid();
    client.set_read_timeout(Some(Duration::from_millis(300)))?;
    wait_for_parked(&supervisor, pid)?;

    // Close's host-side half: record removed + pushes swept, process still live.
    supervisor.inner.runtime.finish(pid);

    let result = supervisor.push_to_connection(pid, b"never-published".to_vec());
    assert!(
        matches!(result, Err(crate::ServerError::ListenerAccept { .. })),
        "the push against the closed record must fail typed, got {result:?}"
    );
    assert_eq!(
        supervisor.pending_push_count(),
        0,
        "the failed push must leave no slot"
    );
    // Publication invariant on the wire: the client must receive NO Push frame.
    assert!(
        read_frame(&mut client).is_err(),
        "an Err from push_to_connection must publish nothing to the client"
    );
    supervisor.shutdown();
    Ok(())
}

/// S8 pin (Err side), driving the REAL failed-wake rollback path over a real
/// connection: the process is killed externally (which leaves its host record —
/// so the confirm passes and the push reaches `enqueue_control`), the wake
/// fails against the dead pid, and `remove_control` REMOVES the never-consumed
/// entry — the observation-proven unpublished case. The API must return the
/// typed `Err` with zero slots, an empty control queue, and no `Push` frame on
/// the client socket. (Neither S7 test executed this branch; this one does.)
#[test]
fn failed_wake_rollback_err_publishes_nothing() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = ConnectionSupervisor::new()?;
    let (mut client, server) = tcp_pair()?;
    let handle = supervisor.spawn_connection(server)?;
    let pid = handle.pid();
    client.set_read_timeout(Some(Duration::from_millis(300)))?;
    wait_for_parked(&supervisor, pid)?;

    // External kill: the process leaves the scheduler table but its host record
    // REMAINS until reaped (the fd-reuse pin established this), so the push
    // below passes its confirm and fails at the wake — the exact S8 branch.
    supervisor
        .scheduler()
        .terminate_process(pid, ExitReason::Error);
    let deadline = Instant::now() + Duration::from_secs(2);
    while handle.is_live() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    if handle.is_live() {
        return Err("externally killed process did not exit".into());
    }
    assert!(
        supervisor.is_tracked(pid),
        "the host record must still exist so the confirm passes"
    );

    let result = supervisor.push_to_connection(pid, b"never-consumed".to_vec());
    assert!(
        matches!(result, Err(crate::ServerError::ListenerAccept { .. })),
        "the failed wake with a rolled-back control must fail typed, got {result:?}"
    );
    assert_eq!(
        supervisor.pending_push_count(),
        0,
        "the rollback must leave no slot"
    );
    assert!(
        !supervisor.inner.runtime.has_control(pid),
        "the rollback must leave no queued control"
    );
    // Publication invariant on the wire: nothing reached the (dead) socket.
    assert!(
        read_frame(&mut client).is_err(),
        "an Err from the failed-wake rollback must publish nothing"
    );
    supervisor.shutdown();
    Ok(())
}

/// S8 pin (consumed side), staged deterministically with the insert->wake
/// barrier: a consumer drains the just-queued `Push` control in the window
/// between `push_control` and the wake attempt (exactly what a live
/// control-drain slice does — each control atom drains ALL queued controls),
/// then the wake fails. `remove_control` finds nothing, so the control was
/// PUBLISHED: the API must return `Ok` (admission — never `Err` for a control a
/// consumer took), and the slot lifecycle carries the delivery truth — the
/// close sweep resolves the awaiter to the truthful DISCONNECTED with zero
/// slots left.
#[test]
fn control_consumed_before_failed_wake_reads_ok_then_disconnected()
-> Result<(), Box<dyn std::error::Error>> {
    let supervisor = ConnectionSupervisor::new()?;
    // A pid with a host record (so the confirm passes) that is NOT in the
    // scheduler table (so the wake deterministically fails).
    let pid = 991;
    supervisor.inner.runtime.register(pid, None)?;
    let (armed, release) = supervisor.inner.runtime.install_pre_wake_barrier();

    let pusher_supervisor = supervisor.clone();
    let pusher =
        thread::spawn(move || pusher_supervisor.push_to_connection(pid, b"drained".to_vec()));

    // The pusher has inserted its slot and queued its control, and is now
    // paused BEFORE the wake attempt. Act as the control-drain consumer.
    armed.wait();
    let consumed = supervisor
        .inner
        .runtime
        .pop_control(pid)
        .ok_or("the queued Push control must be visible to the consumer")?;
    assert!(
        matches!(consumed, ConnectionControl::Push { ref payload, .. } if payload == b"drained"),
        "the consumer must have drained the just-queued Push, got {consumed:?}"
    );
    release.wait();

    // The wake now fails; remove_control finds nothing (we consumed it):
    // published — the API returns Ok, never Err for a delivered-side control.
    let awaiter = pusher
        .join()
        .map_err(|_| "pusher thread panicked")?
        .map_err(|error| {
            format!("a consumed control must yield Ok (admission), got Err: {error}")
        })?;
    assert_eq!(
        supervisor.inner.runtime.pending_push_count_for(pid),
        1,
        "the admitted slot survives — Ok promises admission"
    );

    // The slot lifecycle carries the delivery truth: the close sweep resolves
    // the awaiter to the truthful disconnected outcome, zero slots left.
    supervisor.inner.runtime.finish(pid);
    assert_eq!(supervisor.inner.runtime.pending_push_count_for(pid), 0);
    let outcome = awaiter.receive(Duration::from_secs(5));
    assert!(
        matches!(
            outcome,
            Err(crate::ServerError::PushReplyDisconnected { .. })
        ),
        "the consumed-then-closed push must read disconnected, got {outcome:?}"
    );
    supervisor.shutdown();
    Ok(())
}

/// S4 pin: a poisoned slot registry must not kill reclamation or exact cap
/// accounting. Admission stays fail-closed, but expiry, reply delivery, and the
/// close sweep all recover the guard and complete their removals.
// The deliberate panic-while-holding-the-guard IS the poisoning mechanism under
// test; the unwrap can only fail if the fresh lock is somehow already poisoned.
#[allow(clippy::unwrap_used, clippy::panic)]
#[test]
fn poisoned_registry_still_reclaims_and_expires() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = std::sync::Arc::new(ConnectionRuntime::for_tests(std::sync::Arc::new(
        FlushFailingServices,
    )));
    let pid = 74;
    runtime.register(pid, None)?;
    let due = outstanding_push_with_deadline(&runtime, pid, Some(Instant::now()))?;
    let resolved = outstanding_push(&runtime, pid)?;
    let swept = outstanding_push(&runtime, pid)?;
    assert_eq!(runtime.pending_push_count_for(pid), 3);

    // Poison the registry: a panic while holding the lock.
    let poisoner = std::sync::Arc::clone(&runtime);
    let _ = thread::spawn(move || {
        let _guard = poisoner.push_replies.lock().unwrap();
        panic!("deliberately poison the push-slot registry");
    })
    .join();
    assert!(
        runtime.push_replies.is_poisoned(),
        "the registry is poisoned"
    );

    // Admission is fail-closed on a poisoned registry.
    assert!(
        runtime.register_push(pid, 9_999, None).is_err(),
        "admission must refuse a poisoned registry"
    );

    // Expiry recovers the guard: typed Expired, slot removed — NOT a benign
    // timeout from a false slot-absence.
    let due_outcome = due.receive(Duration::from_millis(10));
    assert!(
        matches!(
            due_outcome,
            Err(crate::ServerError::PushReplyExpired { .. })
        ),
        "expiry must complete on a poisoned registry, got {due_outcome:?}"
    );
    assert_eq!(runtime.pending_push_count_for(pid), 2);

    // Reply delivery recovers the guard.
    runtime.resolve_push(resolved.correlation_id(), b"poison-proof".to_vec());
    assert_eq!(
        resolved.receive(Duration::from_millis(200))?,
        b"poison-proof"
    );
    assert_eq!(runtime.pending_push_count_for(pid), 1);

    // The close sweep recovers the guard — close-at-latest reclamation holds.
    runtime.finish(pid);
    assert_eq!(runtime.pending_push_count_for(pid), 0);
    let swept_outcome = swept.receive(Duration::from_secs(5));
    assert!(
        matches!(
            swept_outcome,
            Err(crate::ServerError::PushReplyDisconnected { .. })
        ),
        "the swept awaiter must disconnect, got {swept_outcome:?}"
    );
    Ok(())
}

/// S4 pin: a dead runtime yields the established DISCONNECTED outcome, never a
/// benign healthy-but-slow timeout — for a dropped runtime (map and senders
/// gone) and for the staged Weak-upgrade-failure branch inside `expire_slot`.
#[test]
fn dropped_runtime_yields_disconnected_not_timeout() -> Result<(), Box<dyn std::error::Error>> {
    // Dropped runtime: both the deadlined and the no-deadline awaiter observe
    // their dropped senders as disconnected.
    let runtime = std::sync::Arc::new(ConnectionRuntime::for_tests(std::sync::Arc::new(
        FlushFailingServices,
    )));
    let pid = 75;
    runtime.register(pid, None)?;
    let overdue = outstanding_push_with_deadline(&runtime, pid, Some(Instant::now()))?;
    let plain = outstanding_push(&runtime, pid)?;
    drop(runtime);
    let overdue_outcome = overdue.receive(Duration::from_millis(10));
    assert!(
        matches!(
            overdue_outcome,
            Err(crate::ServerError::PushReplyDisconnected { .. })
        ),
        "a dead runtime must read disconnected on the deadlined path, got {overdue_outcome:?}"
    );
    let plain_outcome = plain.receive(Duration::from_millis(10));
    assert!(
        matches!(
            plain_outcome,
            Err(crate::ServerError::PushReplyDisconnected { .. })
        ),
        "a dead runtime must read disconnected on the plain path, got {plain_outcome:?}"
    );

    // The precise Weak-upgrade-failure branch: a due deadline, a dead Weak, and
    // a sender kept artificially alive so the entry channel check stays Empty.
    // `expire_slot` must classify this as disconnected, not a benign timeout.
    let (sender, receiver) = channel();
    let orphan = PushReplyAwaiter {
        correlation_id: 424_242,
        receiver,
        deadline: Some(Instant::now()),
        runtime: std::sync::Weak::new(),
    };
    let orphan_outcome = orphan.receive(Duration::from_millis(10));
    assert!(
        matches!(
            orphan_outcome,
            Err(crate::ServerError::PushReplyDisconnected { .. })
        ),
        "a failed Weak upgrade at expiry must read disconnected, got {orphan_outcome:?}"
    );
    drop(sender);
    Ok(())
}

/// S5 pin (public API): an extreme deadline duration is refused with this
/// fallible API's typed error — never an `Instant` addition panic — and leaves
/// no slot or cap admission behind; the connection then still serves a sane
/// deadlined push end to end.
#[test]
fn duration_max_deadline_is_refused_without_slot_leak() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = ConnectionSupervisor::new()?;
    let (mut client, server) = tcp_pair()?;
    let handle = supervisor.spawn_connection(server)?;
    let pid = handle.pid();
    client.set_read_timeout(Some(Duration::from_secs(2)))?;
    wait_for_parked(&supervisor, pid)?;

    let refused = supervisor.push_to_connection_with_deadline(pid, b"x".to_vec(), Duration::MAX);
    assert!(
        matches!(refused, Err(crate::ServerError::ListenerAccept { .. })),
        "Duration::MAX must be refused with the typed admission error, got {refused:?}"
    );
    assert_eq!(
        supervisor.pending_push_count(),
        0,
        "a refused deadline must leave no slot behind"
    );

    // The same connection still serves a sane deadlined push.
    let awaiter = supervisor.push_to_connection_with_deadline(
        pid,
        b"ping".to_vec(),
        Duration::from_secs(5),
    )?;
    let correlation_id = expect_push_correlation(&mut client)?;
    write_frame(
        &mut client,
        &Frame::new_push_reply(1, correlation_id, b"pong".to_vec())?,
    )?;
    assert_eq!(awaiter.receive(Duration::from_secs(2))?, b"pong");
    supervisor.shutdown();
    Ok(())
}

/// S6 pin (public API, real connection): a deadlined push whose client never
/// replies expires PROMPTLY under a long receive quantum (the public face of
/// S1(ii)), the slot is reclaimed, and the §5 accounting returns to zero.
#[test]
fn public_deadlined_push_expires_promptly_over_real_connection()
-> Result<(), Box<dyn std::error::Error>> {
    let supervisor = ConnectionSupervisor::new()?;
    let (mut client, server) = tcp_pair()?;
    let handle = supervisor.spawn_connection(server)?;
    let pid = handle.pid();
    client.set_read_timeout(Some(Duration::from_secs(2)))?;
    wait_for_parked(&supervisor, pid)?;

    let awaiter = supervisor.push_to_connection_with_deadline(
        pid,
        b"no-answer".to_vec(),
        Duration::from_millis(60),
    )?;
    let _correlation_id = expect_push_correlation(&mut client)?; // delivered; client stays silent
    let started = Instant::now();
    let outcome = awaiter.receive(Duration::from_secs(30));
    let elapsed = started.elapsed();
    assert!(
        matches!(outcome, Err(crate::ServerError::PushReplyExpired { .. })),
        "an unanswered deadlined push must expire with the typed outcome, got {outcome:?}"
    );
    assert!(
        elapsed < Duration::from_secs(10),
        "the expiry must arrive near the 60ms deadline, not the 30s quantum (took {elapsed:?})"
    );
    assert_eq!(supervisor.pending_push_count(), 0, "the slot is reclaimed");
    supervisor.shutdown();
    Ok(())
}

/// S6 pin (public API, real connection): the observation-point rule end to end —
/// a client reply that lands AFTER the deadline instant but before any host-side
/// observation is delivered byte-exact, not expired.
#[test]
fn public_deadlined_push_reply_after_deadline_instant_still_wins()
-> Result<(), Box<dyn std::error::Error>> {
    let supervisor = ConnectionSupervisor::new()?;
    let (mut client, server) = tcp_pair()?;
    let handle = supervisor.spawn_connection(server)?;
    let pid = handle.pid();
    client.set_read_timeout(Some(Duration::from_secs(2)))?;
    wait_for_parked(&supervisor, pid)?;

    let awaiter = supervisor.push_to_connection_with_deadline(
        pid,
        b"needs-answer".to_vec(),
        Duration::from_millis(40),
    )?;
    let correlation_id = expect_push_correlation(&mut client)?;

    // Let the deadline instant pass with no host-side observation, then reply.
    thread::sleep(Duration::from_millis(100));
    write_frame(
        &mut client,
        &Frame::new_push_reply(1, correlation_id, b"after-the-instant".to_vec())?,
    )?;

    // Wait until the connection process has resolved the slot host-side, so the
    // receive below deterministically observes the delivered reply.
    let wait_deadline = Instant::now() + Duration::from_secs(2);
    while supervisor.pending_push_count() > 0 && Instant::now() < wait_deadline {
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        supervisor.pending_push_count(),
        0,
        "the reply must resolve the slot before any expiry observation"
    );
    assert_eq!(
        awaiter.receive(Duration::from_secs(1))?,
        b"after-the-instant",
        "a reply delivered before expiry is observed must win, even past the deadline instant"
    );
    supervisor.shutdown();
    Ok(())
}

/// Test (a): THE missing pin, over a real connection. A server push is
/// outstanding and the caller polls `receive` in short quanta while the reply
/// arrives only after SEVERAL elapsed quanta (Vesper's aion shape: the false
/// lost-worker fired at poll+epsilon on the first quantum). Every elapsed poll
/// must be a benign timeout, the slot must survive, and the reply must eventually
/// be received byte-exact with NO `PushReplyDisconnected` and no cancellation.
#[test]
fn receive_poll_quantum_never_changes_protocol_outcome() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = ConnectionSupervisor::new()?;
    let (mut client, server) = tcp_pair()?;
    let handle = supervisor.spawn_connection(server)?;
    client.set_read_timeout(Some(Duration::from_secs(2)))?;
    wait_for_parked(&supervisor, handle.pid())?;

    let awaiter = supervisor.push_to_connection(handle.pid(), b"slow-handler".to_vec())?;
    let correlation_id = match read_frame(&mut client)? {
        Frame::Push { correlation_id, .. } => correlation_id,
        other => return Err(format!("expected Push, got {other:?}").into()),
    };

    // Poll FOUR short quanta with no reply on the wire yet: each is a benign
    // re-arm timeout, never a disconnect. (Reply arrives only after these,
    // making the multi-quantum shape explicit — >= 3 elapsed quanta.) Short
    // quanta keep this real-connection test from perturbing the parallel suite.
    for poll in 0..4 {
        let result = awaiter.receive(Duration::from_millis(25));
        assert!(
            matches!(result, Err(crate::ServerError::PushReplyTimeout { .. })),
            "poll {poll}: an elapsed quantum before the reply must be a benign timeout, got {result:?}"
        );
    }

    // The slow handler finally answers; the re-armed awaiter delivers it
    // byte-exact. That the reply arrives at all proves the four elapsed polls did
    // not cancel the slot (a cancelled slot's dropped sender would disconnect).
    write_frame(
        &mut client,
        &Frame::new_push_reply(1, correlation_id, b"slow-reply".to_vec())?,
    )?;
    let reply = loop {
        match awaiter.receive(Duration::from_millis(50)) {
            Ok(payload) => break payload,
            Err(crate::ServerError::PushReplyTimeout { .. }) => {}
            Err(other) => return Err(format!("unexpected error while re-arming: {other:?}").into()),
        }
    };
    assert_eq!(
        reply, b"slow-reply",
        "the slow reply must arrive byte-exact"
    );

    supervisor.shutdown();
    Ok(())
}

/// Test (e): poll-quantum independence. The same push shape, one serviced by a
/// single long `receive(2s)` and one by short `receive(500ms)` quanta with an
/// intervening benign timeout, must produce the SAME outcome: the identical
/// byte-exact reply. Staged deterministically at the runtime level (the test
/// controls when the reply is resolved) so the poll quantum — not any wall-clock
/// race — is the only variable, and with zero TCP descriptors so it adds no
/// concurrent-fd churn to the suite.
#[test]
fn poll_quantum_independence_yields_the_same_outcome() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = std::sync::Arc::new(ConnectionRuntime::for_tests(std::sync::Arc::new(
        FlushFailingServices,
    )));
    let pid = 64;
    runtime.register(pid, None)?;
    let reply = b"same-outcome".to_vec();

    // Strategy A — one long quantum: the reply is already resolved, so a single
    // `receive(2s)` returns it.
    let single_awaiter = outstanding_push(&runtime, pid)?;
    runtime.resolve_push(single_awaiter.correlation_id(), reply.clone());
    let single_payload = single_awaiter.receive(Duration::from_secs(2))?;

    // Strategy B — many short quanta: one elapsed short `receive` is a benign
    // timeout (the slot survives, count unchanged), THEN the reply is resolved and
    // a re-armed quantum delivers it. The quantum length is deliberately small
    // and irrelevant to the outcome — that is exactly what this test pins.
    let split_awaiter = outstanding_push(&runtime, pid)?;
    let first_quantum = split_awaiter.receive(Duration::from_millis(20));
    assert!(
        matches!(
            first_quantum,
            Err(crate::ServerError::PushReplyTimeout { .. })
        ),
        "the pre-reply quantum must be a benign timeout, got {first_quantum:?}"
    );
    assert_eq!(
        runtime.pending_push_count(),
        1,
        "the benign quantum must not cancel the surviving slot"
    );
    runtime.resolve_push(split_awaiter.correlation_id(), reply.clone());
    let split_payload = loop {
        match split_awaiter.receive(Duration::from_millis(20)) {
            Ok(payload) => break payload,
            Err(crate::ServerError::PushReplyTimeout { .. }) => {}
            Err(other) => return Err(format!("unexpected error while re-arming: {other:?}").into()),
        }
    };

    // Same terminal outcome: the identical byte-exact reply, regardless of quantum.
    assert_eq!(single_payload, reply);
    assert_eq!(split_payload, reply);
    assert_eq!(
        single_payload, split_payload,
        "the poll quantum must not change the delivered reply"
    );
    Ok(())
}

/// Test (e), enumeration half (Vesper's aion shape (b)): a handler parked past
/// one poll quantum while a side channel enumerates the in-flight push entry,
/// then releases. The entry must stay continuously enumerable across the polls
/// and the outcome must be unchanged (the reply is delivered).
///
/// NOTE: `ConnectionSupervisor` exposes NO public host-side push-enumeration
/// surface — `pending_push_count` is a `cfg(test)` runtime instrument. This diff
/// deliberately does not add one (that is the console/MCP arc's job), so this
/// coverage rides the test instrument at the runtime level.
#[test]
fn concurrent_enumeration_during_polling_does_not_change_outcome()
-> Result<(), Box<dyn std::error::Error>> {
    let runtime = std::sync::Arc::new(ConnectionRuntime::for_tests(std::sync::Arc::new(
        FlushFailingServices,
    )));
    let pid = 63;
    runtime.register(pid, None)?;
    let awaiter = outstanding_push(&runtime, pid)?;

    // A side channel busy-enumerates the in-flight entry for the whole poll window.
    let stop = Arc::new(AtomicBool::new(false));
    let enum_runtime = Arc::clone(&runtime);
    let enum_stop = Arc::clone(&stop);
    let enumerator = thread::spawn(move || {
        let mut max_seen = 0;
        while !enum_stop.load(Ordering::Acquire) {
            max_seen = max_seen.max(enum_runtime.pending_push_count());
            // Yield between reads: the entry stays continuously enumerable without
            // hot-spinning a core (a busy loop here steals CPU from co-running
            // timing-sensitive tests in the parallel suite).
            thread::yield_now();
        }
        max_seen
    });

    // Park past several quanta: each poll is a benign timeout and the entry stays
    // enumerable (count 1) throughout.
    for poll in 0..3 {
        assert!(
            matches!(
                awaiter.receive(Duration::from_millis(10)),
                Err(crate::ServerError::PushReplyTimeout { .. })
            ),
            "poll {poll}: expected a benign timeout while enumerated"
        );
        assert_eq!(runtime.pending_push_count(), 1);
    }

    // Release: the reply is delivered byte-exact on re-arm — outcome unchanged.
    runtime.resolve_push(awaiter.correlation_id(), b"released".to_vec());
    let delivered = loop {
        match awaiter.receive(Duration::from_millis(50)) {
            Ok(payload) => break payload,
            Err(crate::ServerError::PushReplyTimeout { .. }) => {}
            Err(other) => return Err(format!("unexpected error: {other:?}").into()),
        }
    };
    assert_eq!(delivered, b"released");

    stop.store(true, Ordering::Release);
    let max_seen = enumerator.join().map_err(|_| "enumerator panicked")?;
    assert_eq!(
        max_seen, 1,
        "the in-flight push entry must be continuously enumerable during polling"
    );
    Ok(())
}

fn conversation_open_frame(conversation_id: u64, subject: &str) -> Frame {
    Frame::ConversationOpen {
        flags: 0,
        stream_id: 1,
        conversation_id,
        subject: subject.to_owned(),
    }
}

fn wait_for_process_count(
    scheduler: &beamr::scheduler::Scheduler,
    target: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if scheduler.process_table().len() == target {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(10));
    }
    Err(format!(
        "conversation scheduler process count did not reach {target} (now {})",
        scheduler.process_table().len()
    )
    .into())
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
    use crate::config::types::{ChannelDef, LimitsConfig, ServerConfig, ServicesConfig};

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
        limits: LimitsConfig::default(),
    };
    Ok(ConnectionSupervisor::from_config(&config)?)
}

/// A channel-free config carrying the given `[services]` profile, for the
/// profile-dispatch tests on the public config constructor.
fn channel_free_config_with_profile(
    profile: &str,
) -> Result<crate::config::types::ServerConfig, Box<dyn std::error::Error>> {
    use crate::config::types::{LimitsConfig, ServerConfig, ServicesConfig};

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
        limits: LimitsConfig::default(),
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
    read_frame_with_timeout(stream, Duration::from_secs(2))
}

fn read_frame_with_timeout(
    stream: &mut TcpStream,
    timeout: Duration,
) -> Result<Frame, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
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

/// §5 cap-refusal (behavioural): `max_connections` refuses the connection PAST
/// the cap with the typed [`ServerError::ConnectionLimitReached`] — the listener
/// then drops the accepted stream, so an over-cap peer is refused, not admitted.
#[test]
fn max_connections_refuses_past_the_cap() -> Result<(), Box<dyn std::error::Error>> {
    use crate::config::types::{LimitsConfig, ServerConfig, ServicesConfig};

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
            max_connections: 1,
            ..LimitsConfig::default()
        },
    };
    let supervisor = ConnectionSupervisor::from_config(&config)?;

    let (_c1, s1) = tcp_pair()?;
    let first = supervisor.spawn_connection(s1);
    assert!(
        first.is_ok(),
        "the first connection is admitted under the cap"
    );

    let (_c2, s2) = tcp_pair()?;
    let second = supervisor.spawn_connection(s2);
    match second {
        Err(crate::ServerError::ConnectionLimitReached { limit }) => {
            assert_eq!(limit, 1, "the refusal reports the configured cap");
        }
        other => return Err(format!("expected ConnectionLimitReached, got {other:?}").into()),
    }
    supervisor.shutdown();
    Ok(())
}

/// Review round 1 item 7: the §5 `max_connections` bound holds under CONCURRENT
/// spawns — an atomic admission reservation means N callers racing for the last
/// slot admit EXACTLY one; the rest get the typed `ConnectionLimitReached`. A
/// signed bound that can be transiently exceeded is not a bound.
#[test]
fn concurrent_spawns_at_the_limit_admit_exactly_one() -> Result<(), Box<dyn std::error::Error>> {
    use crate::config::types::{LimitsConfig, ServerConfig, ServicesConfig};

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
            max_connections: 2,
            ..LimitsConfig::default()
        },
    };
    let supervisor = ConnectionSupervisor::from_config(&config)?;

    // Fill all but the last slot.
    let (_hold_client, hold_server) = tcp_pair()?;
    let _held = supervisor.spawn_connection(hold_server)?;

    // Race N threads for the single remaining slot, released by one barrier.
    let racers: usize = 4;
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(racers));
    let mut workers = Vec::new();
    for _ in 0..racers {
        let supervisor = supervisor.clone();
        let barrier = std::sync::Arc::clone(&barrier);
        let (client, server) = tcp_pair()?;
        workers.push(thread::spawn(move || {
            // Keep the client half alive across the spawn attempt.
            let _client = client;
            barrier.wait();
            supervisor
                .spawn_connection(server)
                .map(|handle| handle.pid())
        }));
    }
    let mut admitted = 0_usize;
    let mut refused = 0_usize;
    for worker in workers {
        match worker.join() {
            Ok(Ok(_pid)) => admitted += 1,
            Ok(Err(crate::ServerError::ConnectionLimitReached { limit })) => {
                assert_eq!(limit, 2, "the refusal reports the configured cap");
                refused += 1;
            }
            Ok(Err(other)) => return Err(format!("unexpected spawn error: {other}").into()),
            Err(_) => return Err("racer thread panicked".into()),
        }
    }
    assert_eq!(admitted, 1, "exactly one racer wins the last slot");
    assert_eq!(refused, racers - 1, "every other racer is refused");
    supervisor.shutdown();
    Ok(())
}

/// Review round 1 item 7 (release pairing): a closed connection releases its
/// admission slot, so the cap is a bound on LIVE connections, not a
/// once-per-process quota — spawn at limit 1, close, spawn again succeeds.
#[test]
fn closed_connection_releases_its_admission_slot() -> Result<(), Box<dyn std::error::Error>> {
    use crate::config::types::{LimitsConfig, ServerConfig, ServicesConfig};

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
            max_connections: 1,
            ..LimitsConfig::default()
        },
    };
    let supervisor = ConnectionSupervisor::from_config(&config)?;

    let (_client_one, server_one) = tcp_pair()?;
    let first = supervisor.spawn_connection(server_one)?;
    // The slot is held: a second spawn is refused.
    let (_client_two, server_two) = tcp_pair()?;
    assert!(matches!(
        supervisor.spawn_connection(server_two),
        Err(crate::ServerError::ConnectionLimitReached { .. })
    ));

    // Close the first connection and wait for its record to go away.
    supervisor.force_close_active_connections();
    wait_for_cleanup(&supervisor, first.pid())?;

    // The released slot admits a fresh connection.
    let (_client_three, server_three) = tcp_pair()?;
    let second = supervisor.spawn_connection(server_three);
    assert!(
        second.is_ok(),
        "the admission slot released by the close admits a new connection: {second:?}"
    );
    supervisor.shutdown();
    Ok(())
}
