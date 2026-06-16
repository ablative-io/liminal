use std::error::Error;

use beamr::process::ExitReason;

use super::ConversationSupervisor;
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

    scheduler.terminate_process(participant.get(), ExitReason::Error);

    assert_eq!(
        scheduler.has_trapped_exit_message(actor_pid.get(), participant.get()),
        Some(true)
    );
    let state = actor.state()?;
    assert_eq!(state.current_phase, ConversationPhase::Failed);
    assert_eq!(state.participants[0].health, ParticipantHealth::Dead);
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
