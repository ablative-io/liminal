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
    assert_eq!(supervisor.registered_participant_count(), 1);
    assert!(scheduler.process_table().get(participant.get()).is_some());

    actor.handle().close()?;

    // Close deregisters the participant AND the actor synchronously (the reply is
    // sent only after `apply_close` runs).
    assert_eq!(
        supervisor.registered_participant_count(),
        0,
        "close must deregister the participant"
    );
    assert_eq!(
        supervisor.registered_actor_count(),
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
        supervisor.registered_participant_count(),
        0,
        "participant registry must not grow with open/close churn"
    );
    assert_eq!(
        supervisor.registered_actor_count(),
        0,
        "actor registry must not grow with open/close churn"
    );

    supervisor.shutdown();
    Ok(())
}

/// Deadline-bounded wait until both lifecycle registries are empty, so a missed
/// exit-driven cleanup FAILS the test instead of wedging it.
fn wait_until_registries_empty(supervisor: &ConversationSupervisor) -> Result<(), Box<dyn Error>> {
    for _ in 0..1_000 {
        if supervisor.registered_actor_count() == 0
            && supervisor.registered_participant_count() == 0
        {
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    Err(format!(
        "registries did not empty: actors={} participants={}",
        supervisor.registered_actor_count(),
        supervisor.registered_participant_count()
    )
    .into())
}

/// D4 rework major-2 pin: a BARE actor exit — the actor process terminated with
/// the core handle retained and NO later restart, close, or any other handle
/// touch — must still remove both registrations, exit-driven (the watcher), not
/// touch-driven. Before the fix the registrations lingered until the next
/// registry touch, which never comes here.
#[test]
fn bare_actor_exit_removes_registrations_without_restart_or_close() -> Result<(), Box<dyn Error>> {
    let supervisor = ConversationSupervisor::new()?;
    let scheduler = supervisor.scheduler();
    let (actor, participant) = supervisor.spawn_with_participant(
        Arc::new(EchoBehaviour),
        None,
        ChannelMode::Ephemeral,
        CrashPolicy::Fail,
    )?;
    let actor_pid = actor.pid()?;
    assert_eq!(supervisor.registered_actor_count(), 1);
    assert_eq!(supervisor.registered_participant_count(), 1);

    scheduler.terminate_process(actor_pid.get(), ExitReason::Error);

    // No handle operation after the kill: cleanup must be driven by the exit
    // itself. The actor handle stays retained (alive) for the whole wait.
    wait_until_registries_empty(&supervisor)?;
    wait_until_process_gone(&scheduler, participant.get())?;

    drop(actor);
    supervisor.shutdown();
    Ok(())
}

/// D4 rework major-3 pin: close on a FAILED conversation is terminal. The close
/// succeeds, the Failed phase is preserved as the diagnostic outcome, and every
/// subsequent handle operation is refused with the typed error — none may
/// respawn the actor through `ensure_running`.
#[test]
fn finalized_conversation_refuses_all_operations_without_respawn() -> Result<(), Box<dyn Error>> {
    let supervisor = ConversationSupervisor::new()?;
    let scheduler = supervisor.scheduler();
    let baseline = scheduler.process_table().len();
    let (actor, participant) = supervisor.spawn_with_participant(
        Arc::new(EchoBehaviour),
        None,
        ChannelMode::Ephemeral,
        CrashPolicy::Fail,
    )?;
    let actor_pid = actor.pid()?;

    // Fail the conversation via a genuine participant crash.
    scheduler.terminate_process(participant.get(), ExitReason::Error);
    let state = state_after_trapped_participant_death(&actor)?;
    assert_eq!(state.current_phase, ConversationPhase::Failed);

    // Closing the failed conversation succeeds and is terminal.
    actor.handle().close()?;

    // The diagnostic outcome is preserved: state queries still answer from the
    // host-side snapshot and the phase remains Failed, not erased to Closed.
    let state = actor.state()?;
    assert_eq!(state.current_phase, ConversationPhase::Failed);
    assert_eq!(state.participants[0].health, ParticipantHealth::Dead);

    // Every operation that would need a live actor is refused with the typed
    // error; none respawns.
    let handle = actor.handle();
    assert!(matches!(
        handle.send(test_envelope(b"late")),
        Err(LiminalError::ConversationFailed { .. })
    ));
    assert!(matches!(
        handle.receive(),
        Err(LiminalError::ConversationFailed { .. })
    ));
    assert!(matches!(
        handle.close(),
        Err(LiminalError::ConversationFailed { .. })
    ));
    assert!(matches!(
        actor.pid(),
        Err(LiminalError::ConversationFailed { .. })
    ));

    // No respawn happened: the old actor process winds down and the scheduler
    // table returns to its pre-spawn baseline (actor, participant, and watcher
    // all gone; nothing new appeared).
    wait_until_process_gone(&scheduler, actor_pid.get())?;
    wait_until_registries_empty(&supervisor)?;
    for _ in 0..1_000 {
        if scheduler.process_table().len() == baseline {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert_eq!(
        scheduler.process_table().len(),
        baseline,
        "a finalized conversation must not respawn any process"
    );

    supervisor.shutdown();
    Ok(())
}

/// D4 round-3 blocker pin (construction ordering, held-gap rendezvous — no
/// sleeps): the actor is killed at the EXACT seam between the watcher arming
/// and the boot enqueue. The spawn attempt must fail with a typed error and
/// roll back EVERYTHING it created — actor, watcher, participant, both
/// registrations, and the (never-admitted or purged) boot command — leaving
/// the scheduler table at baseline. Before the fix this window parked the
/// watcher forever and leaked the registered participant.
#[test]
fn spawn_rolls_back_when_actor_dies_between_watcher_arm_and_boot() -> Result<(), Box<dyn Error>> {
    use std::sync::mpsc;

    let supervisor = ConversationSupervisor::new()?;
    let scheduler = supervisor.scheduler();
    let baseline = scheduler.process_table().len();

    let (actor_tx, actor_rx) = mpsc::channel();
    let (proceed_tx, proceed_rx) = mpsc::channel();
    supervisor
        .inner
        .install_boot_barrier((actor_tx, proceed_rx));

    let spawner = {
        let supervisor = supervisor.clone();
        std::thread::spawn(move || {
            supervisor.spawn_with_participant(
                Arc::new(EchoBehaviour),
                None,
                ChannelMode::Ephemeral,
                CrashPolicy::Fail,
            )
        })
    };

    // Rendezvous: the spawner is now blocked after the watcher armed, before
    // the boot enqueue — the exact window under test. Kill the actor there.
    let actor_pid = actor_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .map_err(|_| "spawner never reached the arm->boot seam")?;
    scheduler.terminate_process(actor_pid.get(), ExitReason::Error);
    wait_until_process_gone(&scheduler, actor_pid.get())?;
    proceed_tx
        .send(())
        .map_err(|_| "spawner abandoned the barrier")?;

    let spawn_result = spawner.join().map_err(|_| "spawner thread panicked")?;
    assert!(
        spawn_result.is_err(),
        "boot against the killed actor must fail the spawn, got Ok"
    );

    // Full transactional rollback: nothing the attempt created survives.
    wait_until_registries_empty(&supervisor)?;
    for _ in 0..1_000 {
        if scheduler.process_table().len() == baseline {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert_eq!(
        scheduler.process_table().len(),
        baseline,
        "rollback must terminate every process the failed attempt created"
    );

    supervisor.shutdown();
    Ok(())
}

/// D4 round-3 blocker pin (a): the boot reply wait is bounded, fails with the
/// typed timeout error, and purges the queued command so it cannot linger for a
/// later actor incarnation. The target is a live-but-inert process, so the
/// enqueue succeeds and ONLY the bound can end the wait.
#[test]
fn boot_wait_is_bounded_and_purges_the_queued_command() -> Result<(), Box<dyn Error>> {
    use super::core::ActorCore;

    let supervisor = ConversationSupervisor::new()?;
    let scheduler = supervisor.scheduler();
    let inert = ParticipantPid::new(scheduler.spawn_test_process(false));
    let core = Arc::new(ActorCore::new(
        Arc::clone(&supervisor.inner),
        ConversationConfig::new(Vec::new(), None, ChannelMode::Ephemeral, CrashPolicy::Fail),
        Vec::new(),
    ));

    let result = core.boot_with_timeout(inert, std::time::Duration::from_millis(100));

    assert!(
        matches!(result, Err(LiminalError::ConversationTimeout { .. })),
        "an unanswered boot must fail with the typed timeout, got {result:?}"
    );
    assert_eq!(
        core.queued_command_count(),
        0,
        "the timed-out boot command must be purged, not left queued"
    );
    supervisor.shutdown();
    Ok(())
}

/// D4 round-3 blocker pin (c, first slice): a watcher spawned for an
/// ALREADY-DEAD target must not park — its armed first slice takes the final
/// liveness probe, observes the empty table slot, cleans up (the dead pid's
/// registration removed), and self-terminates.
#[test]
fn watcher_probe_reaps_already_dead_target_instead_of_parking() -> Result<(), Box<dyn Error>> {
    use super::core::ActorCore;

    let supervisor = ConversationSupervisor::new()?;
    let scheduler = supervisor.scheduler();
    let target = ParticipantPid::new(scheduler.spawn_test_process(false));
    let core = Arc::new(ActorCore::new(
        Arc::clone(&supervisor.inner),
        ConversationConfig::new(Vec::new(), None, ChannelMode::Ephemeral, CrashPolicy::Fail),
        Vec::new(),
    ));
    supervisor
        .inner
        .runtime
        .register(target, Arc::downgrade(&core))?;

    // The target dies BEFORE the watcher exists: no link, no EXIT, only the
    // first-slice probe can observe the death.
    scheduler.terminate_process(target.get(), ExitReason::Error);
    wait_until_process_gone(&scheduler, target.get())?;

    let watcher = supervisor.inner.spawn_watcher(&core, target)?;

    wait_until_process_gone(&scheduler, watcher.get())?;
    wait_until_registries_empty(&supervisor)?;
    supervisor.shutdown();
    Ok(())
}

/// D4 round-3 blocker pin (c, parked): a watcher parked over an UNLINKED live
/// target (the pre-boot-link window) whose target then dies silently must run
/// the same probe on its next wake and self-terminate instead of re-parking —
/// the probe is inseparable from parking on every slice, not just the first.
#[test]
fn parked_unlinked_watcher_wake_probes_and_self_terminates() -> Result<(), Box<dyn Error>> {
    use super::core::ActorCore;

    let supervisor = ConversationSupervisor::new()?;
    let scheduler = supervisor.scheduler();
    let target = ParticipantPid::new(scheduler.spawn_test_process(false));
    let core = Arc::new(ActorCore::new(
        Arc::clone(&supervisor.inner),
        ConversationConfig::new(Vec::new(), None, ChannelMode::Ephemeral, CrashPolicy::Fail),
        Vec::new(),
    ));
    supervisor
        .inner
        .runtime
        .register(target, Arc::downgrade(&core))?;

    // Live target at arm time: the watcher's first-slice probe passes and it
    // parks, unlinked (no boot ran, so no link exists).
    let watcher = supervisor.inner.spawn_watcher(&core, target)?;

    // Silent death: without a link there is no EXIT signal and no wake.
    scheduler.terminate_process(target.get(), ExitReason::Error);
    wait_until_process_gone(&scheduler, target.get())?;

    // Any wake now reaches the probe; a non-EXIT message stands in for the
    // stray wakes a real system produces.
    assert!(
        scheduler.enqueue_atom_message(watcher.get(), supervisor.inner.participant_wakeup_atom),
        "the parked watcher must be wakeable"
    );

    wait_until_process_gone(&scheduler, watcher.get())?;
    wait_until_registries_empty(&supervisor)?;
    supervisor.shutdown();
    Ok(())
}

/// Construction-ordering pin: after a successful open, the actor is genuinely
/// linked to its (already-armed) exit watcher — the observation is installed.
#[test]
fn watcher_is_linked_to_actor_after_open() -> Result<(), Box<dyn Error>> {
    let supervisor = ConversationSupervisor::new()?;
    let scheduler = supervisor.scheduler();
    let (actor, _participant) = supervisor.spawn_with_participant(
        Arc::new(EchoBehaviour),
        None,
        ChannelMode::Ephemeral,
        CrashPolicy::Fail,
    )?;
    let actor_pid = actor.pid()?;
    let watcher = actor
        .core
        .watcher_pid()
        .ok_or("a booted actor must have a recorded watcher")?;

    assert!(
        scheduler.is_linked(actor_pid.get(), watcher.get()),
        "boot must link the actor to its armed watcher"
    );
    supervisor.shutdown();
    Ok(())
}

/// D4 round-3 major-2 pin: finalize racing a respawn. The respawn holds the
/// lifecycle gate through boot (blocked at the test barrier); finalize blocks
/// on that same gate, then cleans up the freshly published actor and watcher —
/// the spawn cannot slip a live pair past a finalization that already returned.
#[test]
fn finalize_racing_respawn_leaves_no_processes_or_registrations() -> Result<(), Box<dyn Error>> {
    use std::sync::mpsc;

    let supervisor = ConversationSupervisor::new()?;
    let scheduler = supervisor.scheduler();
    let baseline = scheduler.process_table().len();
    let actor = supervisor.spawn(ConversationConfig::new(
        Vec::new(),
        None,
        ChannelMode::Ephemeral,
        CrashPolicy::Fail,
    ))?;
    let first_pid = actor.pid()?;

    // Kill the actor so the next handle operation must respawn.
    scheduler.terminate_process(first_pid.get(), ExitReason::Error);
    wait_until_process_gone(&scheduler, first_pid.get())?;

    let (actor_tx, actor_rx) = mpsc::channel();
    let (proceed_tx, proceed_rx) = mpsc::channel();
    supervisor
        .inner
        .install_boot_barrier((actor_tx, proceed_rx));

    let respawner = {
        let actor = actor.clone();
        std::thread::spawn(move || actor.pid())
    };
    // The respawner is inside the gate, blocked at the barrier.
    actor_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .map_err(|_| "respawner never reached the arm->boot seam")?;

    // Finalize concurrently: it must block on the SAME gate the respawner
    // holds, then clean up whatever the respawn published.
    let finalizer = {
        let actor = actor.clone();
        std::thread::spawn(move || actor.finalize())
    };
    proceed_tx
        .send(())
        .map_err(|_| "respawner abandoned the barrier")?;
    respawner
        .join()
        .map_err(|_| "respawner thread panicked")?
        .ok();
    finalizer.join().map_err(|_| "finalizer thread panicked")?;

    // Whatever the interleaving published, finalization wins: no registrations,
    // no processes, and the handle is terminally refused.
    wait_until_registries_empty(&supervisor)?;
    for _ in 0..1_000 {
        if scheduler.process_table().len() == baseline {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert_eq!(
        scheduler.process_table().len(),
        baseline,
        "finalize must clean up an actor published by a racing respawn"
    );
    assert!(matches!(
        actor.pid(),
        Err(LiminalError::ConversationFailed { .. })
    ));
    supervisor.shutdown();
    Ok(())
}

/// D4 round-3 major-3 pin (admission): a command enqueued after finalization —
/// the interleaving where a caller passed `ensure_running` before finalize ran
/// — is rejected at admission with the typed error, never silently queued for
/// an actor that will not process it.
#[test]
fn command_enqueued_after_finalize_is_rejected() -> Result<(), Box<dyn Error>> {
    use std::sync::mpsc;

    use super::queue::QueuedCommandKind;

    let supervisor = ConversationSupervisor::new()?;
    let actor = supervisor.spawn(ConversationConfig::new(
        Vec::new(),
        None,
        ChannelMode::Ephemeral,
        CrashPolicy::Fail,
    ))?;
    let pid = actor.pid()?;
    actor.finalize();

    let (reply, response) = mpsc::sync_channel(1);
    let admitted = actor
        .core
        .enqueue_for_pid(pid, QueuedCommandKind::Receive { reply });

    assert!(
        matches!(admitted, Err(LiminalError::ConversationFailed { .. })),
        "post-finalize admission must be refused with the typed error, got {admitted:?}"
    );
    assert_eq!(actor.core.queued_command_count(), 0);
    drop(response);
    supervisor.shutdown();
    Ok(())
}

/// D4 round-3 major-3 pin (in-flight receive): a receive waiter already parked
/// in the pending queue — its command was popped before finalization — is woken
/// by finalize with the typed error. This pins the UNBOUNDED `receive` path:
/// without the drain (and the publication recheck in `apply_receive`) the
/// caller would block forever.
#[test]
fn blocked_receive_is_released_by_finalize() -> Result<(), Box<dyn Error>> {
    use std::sync::mpsc;

    let supervisor = ConversationSupervisor::new()?;
    let actor = supervisor.spawn(ConversationConfig::new(
        Vec::new(),
        None,
        ChannelMode::Ephemeral,
        CrashPolicy::Fail,
    ))?;
    // Activate with a buffered message, then drain it so the next receive
    // genuinely parks in the pending queue.
    actor.handle().send(test_envelope(b"warm-up"))?;
    assert_eq!(actor.handle().receive()?.payload, b"warm-up");

    let (result_tx, result_rx) = mpsc::channel();
    let receiver_thread = {
        let handle = actor.handle();
        std::thread::spawn(move || {
            let _ = result_tx.send(handle.receive());
        })
    };
    // Deterministic staging: wait (bounded) until the waiter is genuinely
    // parked in the pending queue before finalizing.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while actor.core.pending_receive_count() == 0 {
        if std::time::Instant::now() > deadline {
            return Err("receive waiter never parked".into());
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }

    actor.finalize();

    let received = result_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .map_err(|_| "finalize must wake the blocked receive, not leave it parked")?;
    assert!(
        matches!(received, Err(LiminalError::ConversationFailed { .. })),
        "the released receive must carry the typed closed error, got {received:?}"
    );
    receiver_thread
        .join()
        .map_err(|_| "receiver thread panicked")?;
    supervisor.shutdown();
    Ok(())
}

/// D4 rework major-4 pin: the close-initiated Normal termination of a
/// participant is suppressed as the close's own doing, but an ABNORMAL exit
/// racing the close is recorded with its real reason — and the recorded crash
/// does not reopen the closed conversation (precedence ruling: once close has
/// succeeded, the terminal phase stands).
#[test]
fn abnormal_exit_racing_close_is_recorded_without_reopening_phase() -> Result<(), Box<dyn Error>> {
    use std::time::Instant;

    let supervisor = ConversationSupervisor::new()?;
    let (actor, participant) = supervisor.spawn_with_participant(
        Arc::new(EchoBehaviour),
        None,
        ChannelMode::Ephemeral,
        CrashPolicy::Fail,
    )?;
    actor.pid()?;
    actor.handle().close()?;

    // Suppression half: the close's own Normal termination of the participant is
    // not a crash. Whether or not the actor processed the EXIT before stopping,
    // no crash may be recorded for it.
    let state = actor.state()?;
    assert_eq!(state.current_phase, ConversationPhase::Closed);
    assert_eq!(
        participant_crash_entries(&state),
        0,
        "the close-initiated Normal exit must be suppressed, not recorded as a crash"
    );

    // Racing-crash half, staged deterministically through the host-side
    // recording path the trapped-EXIT handler uses: the participant genuinely
    // crashed (Error) while the close was in flight, so its EXIT carries Error,
    // not the close's Normal.
    actor
        .core
        .record_participant_exit(participant, Instant::now(), Some(ExitReason::Error))?;

    let state = actor.state()?;
    assert_eq!(
        state.current_phase,
        ConversationPhase::Closed,
        "an abnormal exit racing close must not flip the terminal phase"
    );
    assert_eq!(state.participants[0].health, ParticipantHealth::Dead);
    assert_eq!(
        state.participants[0].exit_reason,
        Some(ExitReason::Error),
        "the abnormal exit's real reason must be preserved"
    );
    assert_eq!(
        participant_crash_entries(&state),
        1,
        "the racing crash is recorded exactly once"
    );

    supervisor.shutdown();
    Ok(())
}
