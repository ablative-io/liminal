//! Integration proof for liminal dogfood seam #2: conversations opened through
//! the server's connection services are REAL supervised beamr conversation
//! actors, and a participant crash is detected structurally via the trapped
//! linked-EXIT mechanism — not by polling, sleeping, or a heartbeat.
//!
//! These tests are impossible to pass against the prior trace-only placeholder
//! (`Conversation::start`), which has no participant process, no beamr link, and
//! therefore nothing to crash and nothing to detect.

use std::error::Error;
use std::time::Duration;

use beamr::process::ExitReason;
use liminal::protocol::{CausalContext, MessageEnvelope, SchemaId};
use liminal_server::server::connection::{ConnectionServices, LiminalConnectionServices};

/// Bound on the event-driven wait for the structural crash signal. The wait
/// itself parks on the actor's exit notifier and is woken the instant the link
/// fires; this is only a hang-guard, never a poll interval.
const CRASH_DETECTION_GUARD: Duration = Duration::from_secs(5);

fn message_envelope(payload: &[u8]) -> MessageEnvelope {
    MessageEnvelope::new(
        SchemaId::new([0xAA; SchemaId::WIRE_LEN]),
        CausalContext::independent(),
        payload.to_vec(),
    )
}

/// Opening a conversation through the server services spawns a REAL supervised
/// conversation actor with a live linked participant process. The trace-only
/// placeholder has no participant, so it would expose no participant PID here.
#[test]
fn open_conversation_spawns_real_supervised_actor_with_participant() -> Result<(), Box<dyn Error>> {
    let services = LiminalConnectionServices::empty()?;

    let conversation = services.open_conversation(7, "supervised-subject")?;

    let participants = conversation.participant_pids();
    assert_eq!(
        participants.len(),
        1,
        "a real supervised conversation links exactly one participant process"
    );
    let participant_pid = participants[0];
    assert!(participant_pid != 0, "participant must be a live beamr pid");

    // The participant is a genuine process on the supervisor's scheduler.
    let scheduler = services.conversation_supervisor().scheduler();
    assert!(
        scheduler.process_table().get(participant_pid).is_some(),
        "the linked participant must be a live process in the scheduler table"
    );

    // No crash yet: detection is quiescent until the link actually fires.
    assert!(
        !conversation.has_detected_crash(),
        "a freshly opened conversation must not report a crash"
    );

    services.close_conversation(conversation)?;
    services.conversation_supervisor().shutdown();
    Ok(())
}

/// A message routed through the services reaches the REAL conversation actor and
/// is accepted while the participant is alive.
#[test]
fn conversation_message_drives_real_actor() -> Result<(), Box<dyn Error>> {
    let services = LiminalConnectionServices::empty()?;
    let conversation = services.open_conversation(11, "msg-subject")?;

    services.conversation_message(&conversation, &message_envelope(b"hello"))?;

    services.close_conversation(conversation)?;
    services.conversation_supervisor().shutdown();
    Ok(())
}

/// Bound on the request-reply round trip. The reply receive parks on the
/// conversation actor and is woken when the participant delivers its reply; this
/// guard only bounds a genuine hang so CI cannot block.
const REPLY_GUARD: Duration = Duration::from_secs(5);

/// THE KEY PROOF (request-reply): a message sent through the server reaches the
/// REAL participant process, which genuinely PROCESSES it (the `EchoBehaviour`)
/// and produces a reply that flows back through the conversation to the caller.
///
/// This PASSES now but FAILS against the old inert `spawn_test_process`
/// participant: that stand-in is a live, linked beamr process (so crash
/// detection works) but it runs NO handler and processes NO messages. With the
/// inert participant the forwarded request is consumed by nothing, no reply is
/// ever delivered into the conversation, and `receive_reply` would exhaust
/// `REPLY_GUARD` and return a timeout error instead of the echoed payload. The
/// distinctive payload below proves an actual round trip through the participant,
/// not an accidental loopback: the reply must carry the exact request bytes that
/// the participant's behaviour echoed back.
#[test]
fn request_reply_message_is_really_processed_by_the_participant() -> Result<(), Box<dyn Error>> {
    let services = LiminalConnectionServices::empty()?;
    let conversation = services.open_conversation(99, "request-reply")?;

    let request = b"request-reply-proof-payload";
    services.conversation_message(&conversation, &message_envelope(request))?;

    // Drain the participant's reply (bounded). A real participant processed the
    // forwarded request and delivered this reply back through the conversation;
    // the inert stand-in would never produce it, so this would time out.
    let reply = conversation.receive_reply(REPLY_GUARD)?;
    assert_eq!(
        reply.payload, request,
        "the participant must genuinely process the request and echo it back; \
         an inert participant produces no reply and this drain would time out"
    );

    services.close_conversation(conversation)?;
    services.conversation_supervisor().shutdown();
    Ok(())
}

/// THE PROOF: open a conversation through the server, then KILL its participant
/// process. The supervised actor traps the participant's EXIT (a beamr process
/// link), which fires the structural crash signal in microseconds. The services
/// surface that crash; no polling/sleep/heartbeat is involved.
///
/// This cannot pass against the old trace-only `Conversation::start`: that
/// placeholder has no participant process to terminate and no link to fire, so
/// `participant_pids()` would be empty, there would be nothing to kill, and
/// `await_crash`/`has_detected_crash` could never become true.
#[test]
fn participant_crash_is_detected_via_structural_linked_exit() -> Result<(), Box<dyn Error>> {
    let services = LiminalConnectionServices::empty()?;
    let conversation = services.open_conversation(42, "crash-subject")?;

    let participants = conversation.participant_pids();
    assert_eq!(participants.len(), 1, "expected one linked participant");
    let participant_pid = participants[0];

    let scheduler = services.conversation_supervisor().scheduler();

    // Kill the linked participant. The conversation actor traps this EXIT and
    // records the crash; the link firing is the detection mechanism.
    scheduler.terminate_process(participant_pid, ExitReason::Error);

    // Event-driven wait: park on the actor's exit notifier, woken the instant
    // the trapped EXIT is processed. The guard only bounds a genuine hang.
    let crash_instant = conversation.await_crash(CRASH_DETECTION_GUARD);
    assert!(
        crash_instant.is_some(),
        "participant crash must be detected via the trapped linked-EXIT signal"
    );

    // Detection is observable through the connection-facing accessor too, and
    // the actor's structural phase has transitioned to Failed (CrashPolicy::Fail
    // applied inside the EXIT handler).
    assert!(
        conversation.has_detected_crash(),
        "crash must remain observable after detection"
    );

    // A message after the crash is rejected honestly rather than silently
    // forwarded into a failed conversation.
    let after_crash = services.conversation_message(&conversation, &message_envelope(b"late"));
    assert!(
        after_crash.is_err(),
        "messages after a participant crash must be rejected, not silently dropped"
    );

    services.close_conversation(conversation)?;
    services.conversation_supervisor().shutdown();
    Ok(())
}
