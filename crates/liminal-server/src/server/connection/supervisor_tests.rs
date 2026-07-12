use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use beamr::process::ExitReason;
use liminal::conversation::ParticipantBehaviour;
use liminal::envelope::Envelope;
use liminal::protocol::{
    CONVERSATION_REPLY_REQUESTED_FLAG, CausalContext, Frame, MessageEnvelope, SchemaId, decode,
    encode, encoded_len,
};

use super::{ConnectionRuntime, ConnectionSupervisor, PushReplyAwaiter};
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

    supervisor
        .scheduler()
        .terminate_process(old.pid(), ExitReason::Error);
    let deadline = Instant::now() + Duration::from_secs(2);
    while old.is_live() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    if old.is_live() {
        return Err("externally killed process did not exit".into());
    }

    // Hold clone descriptors below `old_fd` until the allocator reaches the exact
    // externally-freed number. Full-suite concurrency may leave unrelated lower
    // holes, so a single clone is not deterministic.
    let (mut new_client, new_server_original) = tcp_pair()?;
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
    let correlation_id = runtime.next_push_correlation_id();
    let receiver = runtime.register_push(pid, correlation_id)?;
    Ok(PushReplyAwaiter {
        correlation_id,
        receiver,
        runtime: std::sync::Arc::downgrade(runtime),
    })
}

#[test]
fn timed_out_push_cancels_its_reply_slot() -> Result<(), Box<dyn std::error::Error>> {
    // D4 push-slot leak: a reserved slot whose reply never arrives must be released
    // when its deadline passes, not left until connection close. The connection
    // stays open (no `finish`/`mark_crashed`), so only the deadline can free it.
    let runtime = std::sync::Arc::new(ConnectionRuntime::for_tests(std::sync::Arc::new(
        FlushFailingServices,
    )));
    let pid = 41;
    runtime.register(pid, None)?;
    let awaiter = outstanding_push(&runtime, pid)?;
    assert_eq!(runtime.pending_push_count(), 1, "the slot is reserved");

    let result = awaiter.receive(Duration::from_millis(50));

    assert!(
        matches!(result, Err(crate::ServerError::PushReplyTimeout { .. })),
        "a slow-but-connected worker must yield a typed timeout, got {result:?}"
    );
    assert_eq!(
        runtime.pending_push_count(),
        0,
        "the timed-out slot must be cancelled, not leaked until connection close"
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

/// D4 rework minor-1 pin: the push resolve/timeout race, staged deterministically
/// with the slot-registry mutex as the barrier. The awaiter's deadline expires
/// while the resolver holds the registry lock; the resolver then wins the
/// resolved-vs-cancelled transition (remove + send under the lock, exactly what
/// `resolve_push` does) before releasing it. The awaiter must return the reply —
/// not report a timeout for an answer that arrived — and the slot must be gone.
#[test]
fn resolver_winning_the_timeout_race_delivers_the_reply() -> Result<(), Box<dyn std::error::Error>>
{
    let runtime = std::sync::Arc::new(ConnectionRuntime::for_tests(std::sync::Arc::new(
        FlushFailingServices,
    )));
    let pid = 51;
    runtime.register(pid, None)?;
    let awaiter = outstanding_push(&runtime, pid)?;
    let correlation_id = awaiter.correlation_id();
    let reply = b"late-but-won".to_vec();

    // Hold the slot-registry lock: the awaiter's post-timeout `cancel_push`
    // cannot proceed until the resolver below has finished, so the interleaving
    // is enforced by the mutex, not by sleeps.
    let mut slots = runtime
        .push_replies
        .lock()
        .map_err(|error| format!("slot registry lock poisoned: {error}"))?;

    let awaiter_thread = thread::spawn(move || awaiter.receive(Duration::from_millis(20)));

    // Let the awaiter's deadline expire; it then blocks on the held lock. (The
    // sleep only makes the block observable — the outcome is lock-ordered and
    // identical even if the awaiter arrives later.)
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

/// The other side of the minor-1 transition: when the timeout WINS (slot removed
/// by the awaiter), a late resolve is a discard — the reply is never delivered,
/// not even to a retried receive.
#[test]
fn timeout_winning_the_race_discards_the_late_reply() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = std::sync::Arc::new(ConnectionRuntime::for_tests(std::sync::Arc::new(
        FlushFailingServices,
    )));
    let pid = 52;
    runtime.register(pid, None)?;
    let awaiter = outstanding_push(&runtime, pid)?;

    let result = awaiter.receive(Duration::from_millis(20));
    assert!(
        matches!(result, Err(crate::ServerError::PushReplyTimeout { .. })),
        "with no reply the deadline expiry must report a typed timeout, got {result:?}"
    );
    assert_eq!(
        runtime.pending_push_count(),
        0,
        "the timeout freed the slot"
    );

    // A very late reply finds no slot: discarded, and a retried receive reports
    // the dropped sender (disconnected), never the stale payload.
    runtime.resolve_push(awaiter.correlation_id(), b"too-late".to_vec());
    let retried = awaiter.receive(Duration::from_millis(20));
    assert!(
        matches!(
            retried,
            Err(crate::ServerError::PushReplyDisconnected { .. })
        ),
        "a late reply must be discarded, got {retried:?}"
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
