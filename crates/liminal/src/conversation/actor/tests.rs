use std::error::Error;

use beamr::process::ExitReason;

use std::sync::Arc;

use super::{ConversationActor, ConversationSupervisor};
use crate::channel::ChannelMode;
use crate::conversation::participant::EchoBehaviour;
use crate::conversation::types::{
    ConversationConfig, ConversationContextEntry, ConversationPhase, CrashPolicy,
    ParticipantHealth, ParticipantPid,
};
use crate::envelope::Envelope;
use crate::error::LiminalError;

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

/// Drive the actor's command loop until it has RECORDED `participant` dead
/// (under a non-Fail policy the phase does not change, so we watch the
/// participant status itself), then return the observed state. Same monotone-
/// effect rationale as [`state_after_trapped_participant_death`].
fn state_after_participant_recorded_dead(
    actor: &ConversationActor,
    participant: ParticipantPid,
) -> Result<crate::conversation::types::ConversationState, Box<dyn Error>> {
    for _ in 0..1_000 {
        let state = actor.state()?;
        if state.participants.iter().any(|status| {
            status.participant == participant && status.health == ParticipantHealth::Dead
        }) {
            return Ok(state);
        }
        std::thread::yield_now();
    }
    Err("actor did not record participant death".into())
}

/// Wait until `pid` has left the scheduler's process table, so a subsequent
/// actor boot deterministically discovers the death (rather than racing a
/// still-propagating termination).
fn wait_until_process_gone(
    scheduler: &beamr::scheduler::Scheduler,
    pid: u64,
) -> Result<(), Box<dyn Error>> {
    for _ in 0..1_000 {
        if scheduler.process_table().get(pid).is_none() {
            return Ok(());
        }
        std::thread::yield_now();
    }
    Err("process did not leave the process table".into())
}

fn participant_crash_entries(state: &crate::conversation::types::ConversationState) -> usize {
    state
        .context
        .iter()
        .filter(|entry| matches!(entry, ConversationContextEntry::ParticipantCrashed { .. }))
        .count()
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

/// G3 regression (a): a crash recorded by the live actor must not block a later
/// restart. The trapped EXIT records the participant dead host-side; when the
/// actor process is then killed, boot must prune the dead pid instead of
/// failing on an impossible link — and must NOT re-record the crash (no
/// duplicate context entry, no restamped `exited_at`, no duplicate signal).
#[test]
fn restart_after_recorded_participant_crash_prunes_dead_pid_without_duplicate()
-> Result<(), Box<dyn Error>> {
    let supervisor = ConversationSupervisor::new()?;
    let scheduler = supervisor.scheduler();
    let participant = ParticipantPid::new(scheduler.spawn_test_process(false));
    let actor = supervisor.spawn(ConversationConfig::new(
        vec![participant],
        None,
        ChannelMode::Ephemeral,
        CrashPolicy::RouteToNext,
    ))?;
    let first_pid = actor.pid()?;

    // Kill the participant while the actor is alive: the trapped EXIT records
    // the crash host-side (non-Fail policy: record and continue, not Failed).
    scheduler.terminate_process(participant.get(), ExitReason::Error);
    let state = state_after_participant_recorded_dead(&actor, participant)?;
    assert_ne!(state.current_phase, ConversationPhase::Failed);
    assert_eq!(participant_crash_entries(&state), 1);
    let recorded_exit = state.participants[0]
        .exited_at
        .ok_or("recorded crash must stamp exited_at")?;

    // Kill the actor and force a restart. Before the boot-prune fix this
    // failed: linking the already-dead participant erred out of boot.
    scheduler.terminate_process(first_pid.get(), ExitReason::Error);
    let restarted_pid = actor.pid()?;
    assert_ne!(restarted_pid, first_pid);

    let state = actor.state()?;
    assert_eq!(state.participants[0].health, ParticipantHealth::Dead);
    assert_eq!(
        state.participants[0].exited_at,
        Some(recorded_exit),
        "boot must not restamp an already-recorded EXIT instant"
    );
    assert_eq!(
        participant_crash_entries(&state),
        1,
        "boot must not duplicate the crash record"
    );
    supervisor.shutdown();
    Ok(())
}

/// G3 regression (b): the participant dies while the actor itself is down, so
/// nobody is linked and the EXIT is lost. Under a non-Fail policy the restart's
/// boot must discover the dead pid, record the crash exactly once (context
/// entry, Dead status, notifier replay), and leave the conversation usable.
#[test]
fn boot_records_death_that_occurred_while_actor_was_down_and_continues()
-> Result<(), Box<dyn Error>> {
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    let supervisor = ConversationSupervisor::new()?;
    let scheduler = supervisor.scheduler();
    let participant = ParticipantPid::new(scheduler.spawn_test_process(false));
    let actor = supervisor.spawn(ConversationConfig::new(
        vec![participant],
        None,
        ChannelMode::Ephemeral,
        CrashPolicy::RouteToNext,
    ))?;
    let first_pid = actor.pid()?;

    // Kill the actor FIRST, then the participant: no live actor traps the
    // participant's EXIT, so only boot can discover the death.
    scheduler.terminate_process(first_pid.get(), ExitReason::Error);
    scheduler.terminate_process(participant.get(), ExitReason::Error);
    wait_until_process_gone(&scheduler, participant.get())?;

    let restarted_pid = actor.pid()?;
    assert_ne!(restarted_pid, first_pid);

    let state = actor.state()?;
    assert_ne!(state.current_phase, ConversationPhase::Failed);
    assert_eq!(state.participants[0].health, ParticipantHealth::Dead);
    assert!(
        state.participants[0].exited_at.is_some(),
        "boot-discovered death must stamp exited_at"
    );
    assert_eq!(participant_crash_entries(&state), 1);

    // The boot-recorded death replays to a late notifier registrant, exactly
    // like a trapped EXIT recorded before registration.
    let (notifier, fired) = mpsc::sync_channel::<Instant>(1);
    actor.notify_on_participant_exit(participant, notifier)?;
    fired
        .recv_timeout(Duration::from_millis(50))
        .map_err(|_| "boot-recorded death must replay to a late notifier")?;

    // The conversation keeps operating for the surviving configuration: a send
    // is accepted and activates it.
    let handle = actor.handle();
    handle.send(test_envelope(b"still-usable"))?;
    assert_eq!(
        handle.query_state()?.current_phase,
        ConversationPhase::Active
    );
    supervisor.shutdown();
    Ok(())
}

/// G3 regression (c): same lost-EXIT scenario as (b) but under
/// `CrashPolicy::Fail` — the boot-discovered death must behave like a live
/// trap: the conversation transitions to Failed and a receive reports
/// `ParticipantCrashed` honestly rather than a boot/link error.
#[test]
fn boot_discovered_death_under_fail_policy_fails_conversation_honestly()
-> Result<(), Box<dyn Error>> {
    let supervisor = ConversationSupervisor::new()?;
    let scheduler = supervisor.scheduler();
    let participant = ParticipantPid::new(scheduler.spawn_test_process(false));
    let actor = supervisor.spawn(ConversationConfig::new(
        vec![participant],
        None,
        ChannelMode::Ephemeral,
        CrashPolicy::Fail,
    ))?;
    let first_pid = actor.pid()?;

    scheduler.terminate_process(first_pid.get(), ExitReason::Error);
    scheduler.terminate_process(participant.get(), ExitReason::Error);
    wait_until_process_gone(&scheduler, participant.get())?;

    // The restart itself succeeds — the failure is expressed as conversation
    // state, exactly as a live trapped EXIT would have expressed it.
    let restarted_pid = actor.pid()?;
    assert_ne!(restarted_pid, first_pid);

    let state = actor.state()?;
    assert_eq!(state.current_phase, ConversationPhase::Failed);
    assert_eq!(state.participants[0].health, ParticipantHealth::Dead);
    assert_eq!(participant_crash_entries(&state), 1);

    let received = actor.handle().receive();
    assert!(
        matches!(received, Err(LiminalError::ParticipantCrashed { .. })),
        "receive against the failed conversation must report the crash, got {received:?}"
    );
    supervisor.shutdown();
    Ok(())
}

/// D4 rule-1: closing a conversation must leave NO parked participant — its
/// process is terminated and its runtime registration is dropped — and the
/// actor's own registration is dropped too. Before the fix, close stopped only
/// the actor: the participant stayed parked and registered forever.
#[test]
fn closing_conversation_terminates_and_deregisters_participant() -> Result<(), Box<dyn Error>> {
    let supervisor = ConversationSupervisor::new()?;
    let scheduler = supervisor.scheduler();
    let (actor, participant) = supervisor.spawn_with_participant(
        Arc::new(EchoBehaviour),
        None,
        ChannelMode::Ephemeral,
        CrashPolicy::Fail,
    )?;
    let actor_pid = actor.pid()?;

    // Registered and live before close.
    assert_eq!(supervisor.inner.participant_runtime.registration_count(), 1);
    assert!(scheduler.process_table().get(participant.get()).is_some());

    actor.handle().close()?;

    // Close deregisters the participant AND the actor synchronously (the reply is
    // sent only after `apply_close` runs).
    assert_eq!(
        supervisor.inner.participant_runtime.registration_count(),
        0,
        "close must deregister the participant"
    );
    assert_eq!(
        supervisor.inner.runtime.registration_count(),
        0,
        "close must deregister the actor"
    );
    // Both processes are terminated: no parked participant or actor survives.
    wait_until_process_gone(&scheduler, participant.get())?;
    wait_until_process_gone(&scheduler, actor_pid.get())?;

    supervisor.shutdown();
    Ok(())
}

/// D4 churn gate: repeated open/close cycles must pin BOUNDED registry size. An
/// unbounded leak (the pre-fix behaviour, where neither the participant nor the
/// actor key was ever removed) fails this assertion because the counts grow with
/// the cycle count instead of returning to zero.
#[test]
fn repeated_open_close_pins_bounded_registries() -> Result<(), Box<dyn Error>> {
    let supervisor = ConversationSupervisor::new()?;
    let scheduler = supervisor.scheduler();

    for _ in 0..25 {
        let (actor, participant) = supervisor.spawn_with_participant(
            Arc::new(EchoBehaviour),
            None,
            ChannelMode::Ephemeral,
            CrashPolicy::Fail,
        )?;
        actor.pid()?;
        actor.handle().close()?;
        // Drive each participant fully out of the process table before the next
        // cycle so the scheduler table is bounded too, not just the registries.
        wait_until_process_gone(&scheduler, participant.get())?;
    }

    assert_eq!(
        supervisor.inner.participant_runtime.registration_count(),
        0,
        "participant registry must not grow with open/close churn"
    );
    assert_eq!(
        supervisor.inner.runtime.registration_count(),
        0,
        "actor registry must not grow with open/close churn"
    );

    supervisor.shutdown();
    Ok(())
}
