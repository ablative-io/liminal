use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use beamr::native::native_process::{NativeContext, NativeHandler, NativeOutcome};
use beamr::process::ExitReason;

use super::{WorkerContext, WorkerEntry, lifecycle_failed};
use crate::aion::channels::ChannelName;
use crate::aion::error::AionSurfaceError;
use crate::channel::ChannelMode;
use crate::conversation::{
    ConversationActor, ConversationConfig, ConversationSupervisor, CrashPolicy, ParticipantPid,
};

#[derive(Debug)]
pub(super) struct WorkerLinkMonitor {
    supervisor: ConversationSupervisor,
    actor: Option<ConversationActor>,
    listener: Option<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
    owned_participant: Option<ParticipantPid>,
}

impl WorkerLinkMonitor {
    const fn new(
        supervisor: ConversationSupervisor,
        actor: ConversationActor,
        listener: JoinHandle<()>,
        stop: Arc<AtomicBool>,
        owned_participant: Option<ParticipantPid>,
    ) -> Self {
        Self {
            supervisor,
            actor: Some(actor),
            listener: Some(listener),
            stop,
            owned_participant,
        }
    }

    pub(super) fn shutdown(&mut self) {
        // Request the listener stop BEFORE joining. The exit-notification sender
        // lives in the actor process on the scheduler, so `actor.close()` drops
        // it only asynchronously; setting `stop` lets the listener's bounded
        // `recv_timeout` loop exit promptly even if that drop is delayed or the
        // EXIT never arrives (graceful shutdown terminates the participant last).
        self.stop.store(true, Ordering::Release);
        if let Some(actor) = self.actor.take() {
            let _ = actor.handle().close();
        }
        if let Some(listener) = self.listener.take() {
            let _ = listener.join();
        }
        if let Some(participant) = self.owned_participant.take() {
            self.supervisor
                .scheduler()
                .terminate_process(participant.get(), ExitReason::Normal);
        }
    }
}

impl Drop for WorkerLinkMonitor {
    fn drop(&mut self) {
        self.shutdown();
    }
}

pub(super) fn spawn_worker_process(
    context: &WorkerContext,
    channel_name: &ChannelName,
) -> Result<ParticipantPid, AionSurfaceError> {
    let supervisor = context.supervisor_for(channel_name)?;
    let factory = Box::new(|| Box::new(IdleWorkerProcess) as Box<dyn NativeHandler>);
    supervisor
        .scheduler()
        .spawn_native(factory)
        .map(ParticipantPid::new)
        .map_err(|error| lifecycle_failed(channel_name, error))
}

pub(super) fn monitor_worker_process(
    context: WorkerContext,
    channel_name: &ChannelName,
    participant: ParticipantPid,
    entry: Weak<WorkerEntry>,
    owned_participant: Option<ParticipantPid>,
) -> Result<WorkerLinkMonitor, AionSurfaceError> {
    let supervisor = context.supervisor_for(channel_name)?;
    let actor = supervisor
        .spawn(ConversationConfig::new(
            vec![participant],
            None,
            ChannelMode::Ephemeral,
            CrashPolicy::RouteToNext,
        ))
        .map_err(|error| lifecycle_failed(channel_name, error))?;
    actor
        .pid()
        .map_err(|error| lifecycle_failed(channel_name, error))?;
    let (exit_tx, exit_rx) = mpsc::sync_channel(1);
    actor
        .notify_on_participant_exit(participant, exit_tx)
        .map_err(|error| lifecycle_failed(channel_name, error))?;
    let stop = Arc::new(AtomicBool::new(false));
    let listener =
        spawn_worker_link_listener(context, entry, exit_rx, Arc::clone(&stop), channel_name)?;
    Ok(WorkerLinkMonitor::new(
        supervisor,
        actor,
        listener,
        stop,
        owned_participant,
    ))
}

fn spawn_worker_link_listener(
    context: WorkerContext,
    entry: Weak<WorkerEntry>,
    exit_rx: mpsc::Receiver<std::time::Instant>,
    stop: Arc<AtomicBool>,
    channel_name: &ChannelName,
) -> Result<JoinHandle<()>, AionSurfaceError> {
    thread::Builder::new()
        .name("aion-worker-link-listener".to_owned())
        .spawn(move || {
            loop {
                match exit_rx.recv_timeout(Duration::from_millis(50)) {
                    // Real participant EXIT: tear down the worker's pool entry.
                    Ok(_) => {
                        if let Some(entry) = entry.upgrade() {
                            entry.drop_subscription();
                            entry.active.store(false, Ordering::Release);
                            let _ = context.remove_inactive(&entry.channel_name);
                        }
                        break;
                    }
                    // Notification sender dropped (actor torn down): nothing more
                    // can arrive, so exit.
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    // Bounded wait: re-check the shutdown flag so `shutdown()`'s
                    // join cannot block on a delayed or never-arriving EXIT.
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        if stop.load(Ordering::Acquire) {
                            break;
                        }
                    }
                }
            }
        })
        .map_err(|error| lifecycle_failed(channel_name, error))
}

#[derive(Debug)]
struct IdleWorkerProcess;

impl NativeHandler for IdleWorkerProcess {
    fn handle(&mut self, context: &mut NativeContext<'_>) -> NativeOutcome {
        let _ = context;
        NativeOutcome::Wait
    }
}
