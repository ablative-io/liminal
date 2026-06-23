use std::sync::{Mutex, mpsc};
use std::time::Instant;

use super::lock;
use crate::conversation::types::{ParticipantHealth, ParticipantPid, ParticipantStatus};
use crate::error::LiminalError;

/// One-shot notifiers waiting on participant EXIT signals.
///
/// A dispatcher registers a notifier keyed by the participant it linked to, then
/// blocks on the matching receiver. When that participant's trapped EXIT signal
/// is processed by the actor, [`ExitNotifierRegistry::signal`] hands every
/// matching waiter the instant the link fired and drops it. This is the
/// event-driven crash-detection path: waiters are woken by the EXIT, never by
/// polling consumer liveness.
#[derive(Debug, Default)]
pub(super) struct ExitNotifierRegistry {
    notifiers: Mutex<Vec<(ParticipantPid, mpsc::SyncSender<Instant>)>>,
}

impl ExitNotifierRegistry {
    /// Registers `notifier` for `participant`'s EXIT, or replays an already-
    /// observed crash immediately.
    ///
    /// `participants` is the actor's recorded participant state, read by the
    /// caller under the state lock and held across this call. If `participant`
    /// is already dead there, the recorded EXIT instant is sent now and nothing
    /// is queued — closing the crash-before-register race; otherwise the
    /// notifier is queued for a future [`ExitNotifierRegistry::signal`]. Exactly
    /// one notification is delivered.
    ///
    /// # Errors
    /// Returns [`LiminalError`] when the registry lock is poisoned.
    pub(super) fn register(
        &self,
        participant: ParticipantPid,
        notifier: mpsc::SyncSender<Instant>,
        participants: &[ParticipantStatus],
    ) -> Result<(), LiminalError> {
        let already_exited_at = participants.iter().find_map(|status| {
            (status.participant == participant && status.health == ParticipantHealth::Dead)
                .then(|| status.exited_at.unwrap_or_else(Instant::now))
        });
        if let Some(exited_at) = already_exited_at {
            // Ignore send errors: a dropped receiver means the caller abandoned
            // the attempt, so there is nothing to wake.
            let _ = notifier.try_send(exited_at);
            return Ok(());
        }
        lock(&self.notifiers, "actor exit notifiers")?.push((participant, notifier));
        Ok(())
    }

    /// Wakes every waiter registered for `participant`, handing each the instant
    /// the EXIT signal was observed, and drops them so each fires once. A waiter
    /// whose receiver was already dropped is silently discarded.
    ///
    /// # Errors
    /// Returns [`LiminalError`] when the registry lock is poisoned.
    pub(super) fn signal(
        &self,
        participant: ParticipantPid,
        observed_at: Instant,
    ) -> Result<(), LiminalError> {
        // Partition under the lock, then send after releasing it so the lock is
        // never held across the wakeups.
        let matched = {
            let mut notifiers = lock(&self.notifiers, "actor exit notifiers")?;
            let mut matched = Vec::new();
            let mut retained = Vec::with_capacity(notifiers.len());
            for (registered, notifier) in notifiers.drain(..) {
                if registered == participant {
                    matched.push(notifier);
                } else {
                    retained.push((registered, notifier));
                }
            }
            *notifiers = retained;
            matched
        };
        for notifier in matched {
            // Ignore send errors: a dropped receiver means the dispatcher
            // already abandoned this attempt, so there is nothing to wake.
            let _ = notifier.try_send(observed_at);
        }
        Ok(())
    }
}
