//! Exit-driven registry cleanup for conversation actor processes.
//!
//! The actor/participant runtime registries are host-side maps keyed by pid, so
//! an actor process that dies without a later handle touch (no restart, no
//! close) would leave its registration — and its conversation's participant
//! registrations — behind forever. The [`ActorExitWatcher`] is a native process
//! spawned alongside each actor process: it traps exits, is linked to the actor
//! during the actor's boot slice, and on the actor's EXIT removes exactly that
//! actor's registration and terminates + deregisters the conversation's
//! participants, then stops. Cleanup is therefore driven by the exit itself,
//! never by polling and never by waiting for the next registry touch.
//!
//! Arming order is load-bearing: trap-exit can only be set from the watcher's
//! own first slice, and a link that exists before the trap is armed would let
//! an abnormal actor exit cascade-kill the watcher unobserved. The supervisor
//! therefore spawns the watcher UNLINKED, waits (bounded, at construction time)
//! for the arm signal from the watcher's first slice, and only then boots the
//! actor — whose boot slice creates the link. An actor death in any later
//! window is delivered as a trapped `{EXIT, actor, reason}` message.

use std::sync::{Weak, mpsc};

use beamr::native::native_process::{NativeContext, NativeHandler, NativeOutcome};
use beamr::process::ExitReason;

use super::beam::exit_source;
use super::{ActorCore, SupervisorInner};
use crate::conversation::types::ParticipantPid;

pub(super) struct ActorExitWatcher {
    core: Weak<ActorCore>,
    supervisor: Weak<SupervisorInner>,
    actor: ParticipantPid,
    /// Present until the first slice arms trap-exit and signals the spawner.
    armed: Option<mpsc::SyncSender<()>>,
}

impl ActorExitWatcher {
    pub(super) const fn new(
        core: Weak<ActorCore>,
        supervisor: Weak<SupervisorInner>,
        actor: ParticipantPid,
        armed: mpsc::SyncSender<()>,
    ) -> Self {
        Self {
            core,
            supervisor,
            actor,
            armed: Some(armed),
        }
    }

    /// Removes the dead actor's registration and releases its participants.
    ///
    /// With the owning core alive, cleanup goes through the core (identity-
    /// checked, exact). With the core already dropped, nothing can restart the
    /// conversation: fall back to the supervisor's registries directly —
    /// dead-`Weak` actor entries and orphaned participant registrations are
    /// removed, and orphaned participant processes terminated.
    fn cleanup(&self) {
        if let Some(core) = self.core.upgrade() {
            core.finalize_after_actor_exit(self.actor);
            return;
        }
        if let Some(supervisor) = self.supervisor.upgrade() {
            supervisor.runtime.deregister_dead(self.actor);
            supervisor
                .participant_runtime
                .reap_orphans(&supervisor.scheduler);
        }
    }

    fn actor_process_gone(&self) -> bool {
        self.supervisor.upgrade().is_none_or(|supervisor| {
            supervisor
                .scheduler
                .process_table()
                .get(self.actor.get())
                .is_none()
        })
    }
}

impl std::fmt::Debug for ActorExitWatcher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ActorExitWatcher")
            .field("actor", &self.actor)
            .finish_non_exhaustive()
    }
}

impl NativeHandler for ActorExitWatcher {
    fn handle(&mut self, ctx: &mut NativeContext<'_>) -> NativeOutcome {
        if let Some(armed) = self.armed.take() {
            ctx.set_trap_exit(true);
            // A dropped receiver means the spawner already gave up (arm-timeout
            // rollback); the watcher still runs, so nothing is lost.
            let _ = armed.try_send(());
        }
        let mut actor_exited = false;
        while let Some(message) = ctx.recv() {
            if let Some((source, _reason)) = exit_source(message) {
                if source == self.actor {
                    actor_exited = true;
                }
            }
        }
        // The liveness re-check covers the window before the boot slice created
        // the link: an actor that died unlinked delivers no EXIT, but any wake
        // of this watcher after that death still observes the empty table slot.
        if actor_exited || self.actor_process_gone() {
            self.cleanup();
            return NativeOutcome::Stop(ExitReason::Normal);
        }
        NativeOutcome::Wait
    }
}
