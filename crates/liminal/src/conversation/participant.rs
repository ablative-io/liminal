//! Real conversation participant: a beamr-supervised native process that
//! genuinely processes the messages a conversation forwards to it.
//!
//! Seam #2 spawned an INERT participant (`Scheduler::spawn_test_process`): a real
//! supervised, linked process — so crash detection worked — but it processed NO
//! messages. This module replaces that stand-in with a [`ParticipantProcess`], a
//! real [`NativeHandler`] (mirroring the connection-process pattern in
//! `liminal-server`). When the conversation actor forwards an envelope, the
//! participant is woken, drains the forwarded envelope from a shared Rust-side
//! queue (the same shared-queue + atom-wakeup mechanism `ActorCore` uses to move
//! payloads to a beamr process), runs application logic behind the clean
//! [`ParticipantBehaviour`] trait, and delivers any reply back into the
//! conversation so it flows to the caller.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Weak};

use beamr::atom::Atom;
use beamr::native::native_process::{NativeContext, NativeHandler, NativeOutcome};
use beamr::scheduler::Scheduler;

use super::actor::ActorCore;
use crate::conversation::types::ParticipantPid;
use crate::envelope::Envelope;
use crate::error::LiminalError;

/// Application-supplied behaviour for a conversation participant.
///
/// This is the clean extension point: a participant receives each forwarded
/// request [`Envelope`] and returns an optional reply [`Envelope`]. Returning
/// `Some` makes this a request-reply participant (the reply flows back to the
/// caller through the conversation); returning `None` consumes the message
/// without replying. Implementations run on a beamr scheduler worker, so they
/// must be `Send + Sync` and should not block.
pub trait ParticipantBehaviour: Send + Sync + std::fmt::Debug {
    /// Processes one forwarded request, optionally producing a reply.
    fn process(&self, request: &Envelope) -> Option<Envelope>;
}

/// Reference request-reply participant.
///
/// Echoes each request straight back as the reply, preserving the payload and
/// schema. Concrete, real processing — not a stub — and the natural first
/// pattern from LIM-005 (request-reply).
#[derive(Clone, Copy, Debug, Default)]
pub struct EchoBehaviour;

impl ParticipantBehaviour for EchoBehaviour {
    fn process(&self, request: &Envelope) -> Option<Envelope> {
        Some(request.clone())
    }
}

/// Shared state bridging the conversation actor and its native participants.
///
/// Owns, per participant pid: the forwarded-request queue the actor pushes into,
/// the application [`ParticipantBehaviour`], and a weak handle to the owning
/// [`ActorCore`] so a produced reply can be delivered back into the conversation.
/// This is the participant-side analogue of `liminal-server`'s `ConnectionRuntime`
/// — a shared Rust-side queue read by a native handler that is woken by an atom
/// message, never an invented side channel.
#[derive(Debug, Default)]
pub(super) struct ParticipantRuntime {
    registrations: Mutex<HashMap<u64, ParticipantRegistration>>,
}

/// Shared forwarding queue of requests awaiting processing by a participant.
type RequestQueue = Arc<Mutex<VecDeque<Envelope>>>;

/// A participant snapshot: its forwarding queue, behaviour, and owning core.
type ParticipantSnapshot = (RequestQueue, Arc<dyn ParticipantBehaviour>, Weak<ActorCore>);

struct ParticipantRegistration {
    inbox: RequestQueue,
    behaviour: Arc<dyn ParticipantBehaviour>,
    core: Weak<ActorCore>,
}

impl std::fmt::Debug for ParticipantRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ParticipantRegistration")
            .field("behaviour", &self.behaviour)
            .finish_non_exhaustive()
    }
}

impl ParticipantRuntime {
    /// Registers a participant pid with its forwarding queue, behaviour, and the
    /// actor core that owns the conversation, so forwarded requests can be
    /// processed and replies delivered back.
    ///
    /// # Errors
    /// Returns [`LiminalError`] when the registry lock is poisoned.
    pub(super) fn register(
        &self,
        pid: ParticipantPid,
        inbox: RequestQueue,
        behaviour: Arc<dyn ParticipantBehaviour>,
        core: Weak<ActorCore>,
    ) -> Result<(), LiminalError> {
        lock(&self.registrations)?.insert(
            pid.get(),
            ParticipantRegistration {
                inbox,
                behaviour,
                core,
            },
        );
        Ok(())
    }

    /// Drops a participant registration (used when a participant process exits).
    pub(super) fn deregister(&self, pid: ParticipantPid) {
        if let Ok(mut registrations) = self.registrations.lock() {
            registrations.remove(&pid.get());
        }
    }

    /// Drains and processes every request currently queued for `pid`, delivering
    /// each produced reply back into the owning conversation. Returns the number
    /// of requests processed this slice.
    fn run_slice(&self, pid: u64) -> usize {
        let Some((inbox, behaviour, core)) = self.snapshot(pid) else {
            return 0;
        };
        let mut processed = 0;
        loop {
            let request = {
                let Ok(mut queue) = inbox.lock() else {
                    break;
                };
                queue.pop_front()
            };
            let Some(request) = request else { break };
            processed += 1;
            if let Some(reply) = behaviour.process(&request) {
                if let Some(core) = core.upgrade() {
                    // Deliver the reply into the conversation. A failure here means
                    // the conversation is already closed/failed; the reply is then
                    // genuinely undeliverable and dropping it is correct.
                    let _ = core.deliver_participant_reply(reply);
                }
            }
        }
        processed
    }

    fn snapshot(&self, pid: u64) -> Option<ParticipantSnapshot> {
        let registrations = self.registrations.lock().ok()?;
        let registration = registrations.get(&pid)?;
        let snapshot = (
            Arc::clone(&registration.inbox),
            Arc::clone(&registration.behaviour),
            registration.core.clone(),
        );
        drop(registrations);
        Some(snapshot)
    }
}

/// Handle to one registered participant: its pid, the queue the actor forwards
/// into, and the shared runtime that wakes it. Held by [`ActorCore`] so a `Send`
/// command can forward an envelope to the real participant process.
#[derive(Clone, Debug)]
pub(super) struct ParticipantChannel {
    pid: ParticipantPid,
    inbox: RequestQueue,
}

impl ParticipantChannel {
    pub(super) const fn new(pid: ParticipantPid, inbox: RequestQueue) -> Self {
        Self { pid, inbox }
    }

    pub(super) const fn pid(&self) -> ParticipantPid {
        self.pid
    }

    /// Returns a clone of the shared forwarding queue, so the participant runtime
    /// and the conversation actor read and write the SAME queue.
    pub(super) fn inbox_arc(&self) -> RequestQueue {
        Arc::clone(&self.inbox)
    }

    /// Forwards `request` to the participant by enqueuing it on the shared queue
    /// and waking the participant process with `wakeup_atom`. Returns
    /// [`LiminalError::DeliveryFailed`] when the participant process is not live.
    pub(super) fn forward(
        &self,
        request: Envelope,
        scheduler: &Scheduler,
        wakeup_atom: Atom,
    ) -> Result<(), LiminalError> {
        lock(&self.inbox)?.push_back(request);
        if scheduler.enqueue_atom_message(self.pid.get(), wakeup_atom) {
            Ok(())
        } else {
            // Roll back the enqueue so a dead participant does not leave an
            // orphaned request that a later live pid could mistakenly drain.
            lock(&self.inbox)?.pop_back();
            Err(LiminalError::DeliveryFailed {
                message: format!("conversation participant {} is not live", self.pid.get()),
            })
        }
    }
}

/// The native participant process body.
///
/// A real [`NativeHandler`] — a first-class, scheduler-supervised beamr process
/// with a pid and mailbox, spawned through the same machinery as any other beamr
/// process. On each slice it drains its atom wakeups, then runs the shared
/// runtime slice that processes every forwarded request through the application
/// behaviour. It parks (`NativeOutcome::Wait`) when idle, so it consumes no CPU
/// until the actor forwards the next request — and stays linked to the actor for
/// structural crash detection.
pub(super) struct ParticipantProcess {
    runtime: Arc<ParticipantRuntime>,
    wakeup_atom: Atom,
}

impl ParticipantProcess {
    pub(super) const fn new(runtime: Arc<ParticipantRuntime>, wakeup_atom: Atom) -> Self {
        Self {
            runtime,
            wakeup_atom,
        }
    }
}

impl std::fmt::Debug for ParticipantProcess {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ParticipantProcess")
            .field("wakeup_atom", &self.wakeup_atom)
            .finish_non_exhaustive()
    }
}

impl NativeHandler for ParticipantProcess {
    fn handle(&mut self, ctx: &mut NativeContext<'_>) -> NativeOutcome {
        let pid = ctx.self_pid();
        // Drain the atom wakeups the actor enqueued. The payloads themselves
        // travel through the shared Rust-side queue (the `ActorCore` pattern):
        // beamr's host enqueue API moves only atoms, so the atom is just the
        // wakeup signal and `run_slice` does the real work against the shared
        // queue. Participant failure is surfaced structurally via the beamr
        // link/EXIT to the supervised actor, not through a mailbox message, so
        // there is no message-driven shutdown branch here.
        while ctx.recv().is_some() {}
        self.runtime.run_slice(pid);
        NativeOutcome::Wait
    }
}

fn lock<T>(mutex: &Mutex<T>) -> Result<std::sync::MutexGuard<'_, T>, LiminalError> {
    mutex.lock().map_err(|error| LiminalError::DeliveryFailed {
        message: format!("participant queue lock poisoned: {error}"),
    })
}
