use std::error::Error;

use beamr::process::ExitReason;

use super::{ConversationActor, ConversationSupervisor};
use crate::channel::ChannelMode;
use crate::conversation::types::{
    ConversationConfig, ConversationPhase, CrashPolicy, ParticipantHealth, ParticipantPid,
};
use crate::envelope::Envelope;

fn test_envelope(payload: &[u8]) -> Envelope {
    Envelope::new(
        payload.to_vec(),
        None,
        crate::channel::SchemaId::new(),
        crate::envelope::PublisherId::default(),
    )
}

/// Drive the actor's command loop until it reflects the participant death, then
/// return the observed state.
///
/// We deliberately assert on the *durable effect* of the trapped exit — the
/// `Failed`/`Dead` transition performed by `handle_participant_exit` — rather
/// than scanning the actor mailbox for the raw `{EXIT, _, _}` tuple. The tuple
/// is a transient artifact: once the scheduler runs the actor's slice it
/// consumes the message (`RemoveMessage`) and applies its effect, so polling
/// for the tuple races consumption and is inherently non-deterministic. The
/// state transition, by contrast, is the observable, monotone outcome the test
/// actually cares about: a participant death must arrive as a *trapped* exit
/// (the actor survives it) and be recorded (not silently dropped or timed out).
///
/// `state()` enqueues a `QueryState` command behind the already-delivered EXIT
/// message in the actor mailbox, so FIFO processing guarantees the snapshot we
/// observe is taken no earlier than the exit was handled. Each successful
/// `state()` call also proves the actor is still alive and responsive — i.e. it
/// trapped the exit rather than being cascade-killed by it, and answered
/// "without timeout".
fn state_after_trapped_participant_death(
    actor: &ConversationActor,
) -> Result<crate::conversation::types::ConversationState, Box<dyn Error>> {
    // Generous bound: the work is a single scheduler slice; this only guards
    // against a genuine hang (the failure the test name warns against).
    for _ in 0..1_000 {
        let state = actor.state()?;
        if state.current_phase == ConversationPhase::Failed {
            return Ok(state);
        }
        std::thread::yield_now();
    }
    Err("actor did not record trapped participant death".into())
}

#[test]
fn actor_is_spawned_linked_and_queryable() -> Result<(), Box<dyn Error>> {
    let supervisor = ConversationSupervisor::new()?;
    let scheduler = supervisor.scheduler();
    let participant = ParticipantPid::new(scheduler.spawn_test_process(false));
    let actor = supervisor.spawn(ConversationConfig::new(
        vec![participant],
        None,
        ChannelMode::Ephemeral,
        CrashPolicy::Fail,
    ))?;
    let actor_pid = actor.pid()?;
    let state = actor.state()?;

    assert!(scheduler.is_linked(actor_pid.get(), participant.get()));
    assert_eq!(state.current_phase, ConversationPhase::Created);
    assert_eq!(state.context.len(), 0);
    supervisor.shutdown();
    Ok(())
}

#[test]
fn handle_progresses_created_active_closed_lifecycle() -> Result<(), Box<dyn Error>> {
    let supervisor = ConversationSupervisor::new()?;
    let actor = supervisor.spawn(ConversationConfig::new(
        Vec::new(),
        None,
        ChannelMode::Ephemeral,
        CrashPolicy::Fail,
    ))?;
    let handle = actor.handle();

    assert_eq!(
        handle.query_state()?.current_phase,
        ConversationPhase::Created
    );
    handle.send(test_envelope(b"hello"))?;
    assert_eq!(
        handle.query_state()?.current_phase,
        ConversationPhase::Active
    );
    assert_eq!(handle.receive()?.payload, b"hello");
    handle.close()?;
    assert_eq!(
        handle.query_state()?.current_phase,
        ConversationPhase::Closed
    );
    supervisor.shutdown();
    Ok(())
}

#[test]
fn participant_death_arrives_as_trapped_exit_without_timeout() -> Result<(), Box<dyn Error>> {
    let supervisor = ConversationSupervisor::new()?;
    let scheduler = supervisor.scheduler();
    let participant = ParticipantPid::new(scheduler.spawn_test_process(false));
    let actor = supervisor.spawn(ConversationConfig::new(
        vec![participant],
        None,
        ChannelMode::Ephemeral,
        CrashPolicy::Fail,
    ))?;
    let actor_pid = actor.pid()?;
    assert!(scheduler.is_linked(actor_pid.get(), participant.get()));

    scheduler.terminate_process(participant.get(), ExitReason::Error);

    // The participant died with an abnormal reason while linked to the actor.
    // Because the actor traps exits, this must arrive as a trapped `{EXIT, _, _}`
    // and be recorded as a participant crash — the actor must NOT be
    // cascade-killed by it. We observe the durable, monotone effect of that
    // trapped exit (Failed/Dead) rather than the transient mailbox tuple, which
    // the actor consumes as soon as it runs (see helper docs).
    let state = state_after_trapped_participant_death(&actor)?;
    assert_eq!(state.current_phase, ConversationPhase::Failed);
    assert_eq!(state.participants[0].health, ParticipantHealth::Dead);

    // The actor itself survived the linked exit (trapped, not killed): its pid
    // is unchanged and still live in the process table.
    assert_eq!(actor.pid()?, actor_pid);
    assert!(scheduler.process_table().get(actor_pid.get()).is_some());

    supervisor.shutdown();
    Ok(())
}

/// Crash-before-register race: a notifier registered AFTER the participant is
/// already recorded dead must fire immediately, replaying the recorded EXIT
/// instant. This is exactly the link→register window the dispatcher cannot
/// otherwise observe. If `register` ignored already-dead state (the bug), the
/// notifier would never fire and `recv_timeout` would elapse, failing the test.
#[test]
fn notify_on_already_dead_participant_fires_immediately() -> Result<(), Box<dyn Error>> {
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    let supervisor = ConversationSupervisor::new()?;
    let scheduler = supervisor.scheduler();
    let participant = ParticipantPid::new(scheduler.spawn_test_process(false));
    let actor = supervisor.spawn(ConversationConfig::new(
        vec![participant],
        None,
        ChannelMode::Ephemeral,
        CrashPolicy::Fail,
    ))?;
    let actor_pid = actor.pid()?;
    assert!(scheduler.is_linked(actor_pid.get(), participant.get()));

    let before_crash = Instant::now();
    // Kill the participant and wait until the actor has RECORDED it dead, so the
    // subsequent registration genuinely observes already-dead state.
    scheduler.terminate_process(participant.get(), ExitReason::Error);
    let state = state_after_trapped_participant_death(&actor)?;
    assert_eq!(state.participants[0].health, ParticipantHealth::Dead);
    let recorded_exit = state.participants[0]
        .exited_at
        .ok_or("EXIT instant must be recorded when a participant is marked dead")?;
    assert!(recorded_exit >= before_crash, "recorded EXIT must be real");

    // Register only now, after death is recorded. The notifier must fire
    // immediately with the recorded instant — not block.
    let (tx, rx) = mpsc::sync_channel::<Instant>(1);
    actor.notify_on_participant_exit(participant, tx)?;
    let replayed = rx
        .recv_timeout(Duration::from_millis(50))
        .map_err(|_| "already-dead registration must replay the EXIT immediately")?;
    assert_eq!(
        replayed, recorded_exit,
        "replayed instant must equal the recorded EXIT instant"
    );

    supervisor.shutdown();
    Ok(())
}

#[test]
fn supervisor_restarts_only_crashed_actor() -> Result<(), Box<dyn Error>> {
    let supervisor = ConversationSupervisor::new()?;
    let first = supervisor.spawn(ConversationConfig::new(
        Vec::new(),
        None,
        ChannelMode::Ephemeral,
        CrashPolicy::Fail,
    ))?;
    let second = supervisor.spawn(ConversationConfig::new(
        Vec::new(),
        None,
        ChannelMode::Ephemeral,
        CrashPolicy::Fail,
    ))?;
    let scheduler = supervisor.scheduler();
    let first_pid = first.pid()?;
    let second_pid = second.pid()?;

    scheduler.terminate_process(first_pid.get(), ExitReason::Error);
    let restarted_pid = first.pid()?;

    assert_ne!(first_pid, restarted_pid);
    assert_eq!(second.pid()?, second_pid);
    assert!(scheduler.process_table().get(second_pid.get()).is_some());
    supervisor.shutdown();
    Ok(())
}
