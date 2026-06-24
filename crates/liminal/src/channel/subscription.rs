//! LIM-002 R2/R3: subscriptions backed by real beamr processes.
//!
//! Each subscription owns a real, scheduler-supervised beamr native process
//! (a [`SubscriberProcess`]) plus the in-memory inbox the channel actor delivers
//! matching envelopes into. The channel actor LINKS to this process's pid on
//! `Subscribe`; when the [`SubscriptionHandle`] is dropped (or the caller
//! unsubscribes) the process is terminated, the link fires an `{EXIT, pid, _}`
//! signal, and the trapping channel actor removes the dead subscriber from its
//! fan-out list. There is NO weak-Arc polling: liveness is observed structurally
//! through the beamr link/EXIT path, exactly as the conversation actor observes
//! its participants (`conversation/actor/beam.rs`).
//!
//! R3 predicates live INSIDE the channel actor process: a [`SubscriptionPredicate`]
//! is a boxed `Fn(&Envelope) -> bool` owned by the actor's subscriber
//! registration and evaluated at delivery time. This mirrors the participant
//! `behaviour` pattern (a boxed trait object the process owns); for an in-memory
//! ephemeral channel there is no need for a serialisable predicate, so a closure
//! the actor holds is the simplest faithful design.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use beamr::native::native_process::{NativeContext, NativeHandler, NativeOutcome};
use beamr::process::ExitReason;
use beamr::scheduler::Scheduler;
use beamr::term::binary_ref::BinaryRef;

use crate::channel::wire::decode_envelope;
use crate::envelope::Envelope;
use crate::error::LiminalError;

/// In-memory inbox a subscriber receives delivered envelopes on.
pub(crate) type SubscriberInbox = Arc<Mutex<VecDeque<Envelope>>>;

/// A delivery predicate evaluated by the channel actor against each published
/// envelope. `None` (no predicate) means deliver everything.
pub(crate) type SubscriptionPredicate = Arc<dyn Fn(&Envelope) -> bool + Send + Sync>;

/// Real beamr native process backing one subscription.
///
/// For LOCAL delivery it is an idle handler (mirroring
/// `aion::worker::link::IdleWorkerProcess`): local envelopes travel through the
/// shared [`SubscriberInbox`] the channel actor writes and
/// [`SubscriptionHandle::try_next`] reads. Its other job is to BE a first-class
/// linkable, killable process whose lifetime equals the subscription's, so the
/// channel actor detects the subscription dying via a real EXIT signal rather
/// than by polling a weak pointer.
///
/// For CROSS-NODE delivery (SRV-005) it is also the landing point for a remote
/// publish: a remote node sends a published envelope, encoded by
/// [`crate::channel::wire::encode_envelope`], as a single beamr binary directly
/// to this process's pid (the pid the cluster registered in the channel's
/// distributed process group). The binary lands in this process's mailbox; the
/// handler decodes it back into an [`Envelope`] and pushes it onto the SAME
/// inbox a local publish would, so a subscriber observes local and remote
/// messages identically. Non-binary wakeups (trapped `{EXIT, _, _}` signals) are
/// drained and ignored.
struct SubscriberProcess {
    inbox: SubscriberInbox,
}

impl NativeHandler for SubscriberProcess {
    fn handle(&mut self, ctx: &mut NativeContext<'_>) -> NativeOutcome {
        // Trapping is set authoritatively at spawn (see `SubscriptionHandle::spawn`)
        // so it holds before the actor ever links — re-assert it here defensively
        // for any future restart of this handler.
        ctx.set_trap_exit(true);
        // Drain every queued wakeup. A binary message is a remote envelope frame
        // (SRV-005) to decode and enqueue; everything else (e.g. a trapped
        // `{EXIT, _, _}` tuple from a crashed actor this subscriber outlives) is
        // ignored. Death is driven only by an explicit `terminate_process` on
        // unsubscribe/handle drop.
        while let Some(message) = ctx.recv() {
            if let Some(binary) = BinaryRef::new(message) {
                self.accept_remote_frame(binary.as_bytes());
            }
        }
        NativeOutcome::Wait
    }
}

impl SubscriberProcess {
    /// Decode a remote envelope frame and push it onto the inbox. A frame that
    /// fails to decode is dropped: a corrupt cross-node payload must never crash
    /// the subscriber or stall delivery of well-formed messages.
    fn accept_remote_frame(&self, bytes: &[u8]) {
        let Ok(envelope) = decode_envelope(bytes) else {
            return;
        };
        if let Ok(mut inbox) = self.inbox.lock() {
            inbox.push_back(envelope);
        }
    }
}

/// The actor-side record of one subscriber: the inbox to deliver into and the
/// optional predicate to gate delivery. Held by the channel actor INSIDE its
/// process, keyed by the subscriber process's pid.
pub(crate) struct SubscriberRegistration {
    pid: u64,
    inbox: SubscriberInbox,
    predicate: Option<SubscriptionPredicate>,
}

impl SubscriberRegistration {
    pub(crate) const fn pid(&self) -> u64 {
        self.pid
    }

    /// Delivers `envelope` to this subscriber when its predicate accepts it (or
    /// it has no predicate). Returns `Err` only when the inbox lock is poisoned.
    pub(crate) fn deliver(&self, envelope: &Envelope) -> Result<(), LiminalError> {
        if let Some(predicate) = self.predicate.as_ref() {
            if !predicate(envelope) {
                return Ok(());
            }
        }
        self.inbox
            .lock()
            .map_err(|error| LiminalError::DeliveryFailed {
                message: format!("subscriber inbox unavailable: {error}"),
            })?
            .push_back(envelope.clone());
        Ok(())
    }
}

impl std::fmt::Debug for SubscriberRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SubscriberRegistration")
            .field("pid", &self.pid)
            .field("has_predicate", &self.predicate.is_some())
            .finish_non_exhaustive()
    }
}

/// Handle returned by channel subscriptions for receiving validated envelopes.
///
/// Owns the subscriber's beamr pid, the shared inbox, and a clone of the
/// scheduler so the process can be terminated when the subscription ends. The
/// handle is the subscription's lifetime: dropping the last clone terminates the
/// subscriber process, whose EXIT prunes the channel actor's fan-out list.
#[derive(Clone)]
pub struct SubscriptionHandle {
    inner: Arc<SubscriptionInner>,
}

struct SubscriptionInner {
    pid: u64,
    inbox: SubscriberInbox,
    scheduler: Arc<Scheduler>,
}

impl SubscriptionHandle {
    /// Spawns a real subscriber process on `scheduler` and returns the handle
    /// plus its actor-side registration record (carrying any predicate).
    ///
    /// # Errors
    /// Returns [`LiminalError::SubscriptionFailed`] when the scheduler cannot
    /// spawn the subscriber process.
    pub(crate) fn spawn(
        scheduler: &Arc<Scheduler>,
        predicate: Option<SubscriptionPredicate>,
    ) -> Result<(Self, SubscriberRegistration), LiminalError> {
        let inbox: SubscriberInbox = Arc::new(Mutex::new(VecDeque::new()));
        let process_inbox = Arc::clone(&inbox);
        let factory = Box::new(move || {
            Box::new(SubscriberProcess {
                inbox: Arc::clone(&process_inbox),
            }) as Box<dyn NativeHandler>
        });
        let pid =
            scheduler
                .spawn_native(factory)
                .map_err(|error| LiminalError::SubscriptionFailed {
                    message: format!("failed to spawn subscriber process: {error:?}"),
                })?;
        // Set trap_exit BEFORE the channel actor links to this pid, so an
        // abnormal actor crash is trapped (delivered as a message the subscriber
        // drains) instead of cascading across the link and killing the
        // subscriber. This makes the subscriber outlive a channel-actor crash so
        // the restarted actor can re-link to it on boot (R2/R4). Setting it
        // host-side (not in the handler's first slice) removes any race against
        // the crash: the flag is in place the instant `subscribe` proceeds.
        scheduler
            .set_trap_exit(pid, true)
            .map_err(|error| LiminalError::SubscriptionFailed {
                message: format!("failed to set trap_exit on subscriber process {pid}: {error:?}"),
            })?;
        let handle = Self {
            inner: Arc::new(SubscriptionInner {
                pid,
                inbox: Arc::clone(&inbox),
                scheduler: Arc::clone(scheduler),
            }),
        };
        let registration = SubscriberRegistration {
            pid,
            inbox,
            predicate,
        };
        Ok((handle, registration))
    }

    /// Returns the beamr pid of the subscriber process this handle owns.
    #[must_use]
    pub(crate) fn pid(&self) -> u64 {
        self.inner.pid
    }

    /// Attempts to receive the next delivered envelope without blocking.
    ///
    /// # Errors
    ///
    /// Returns [`LiminalError::SubscriptionFailed`] when the subscription inbox cannot be read.
    pub fn try_next(&self) -> Result<Option<Envelope>, LiminalError> {
        let mut messages =
            self.inner
                .inbox
                .lock()
                .map_err(|error| LiminalError::SubscriptionFailed {
                    message: format!("subscription inbox unavailable: {error}"),
                })?;
        Ok(messages.pop_front())
    }
}

impl std::fmt::Debug for SubscriptionHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SubscriptionHandle")
            .field("pid", &self.inner.pid)
            .finish_non_exhaustive()
    }
}

impl Drop for SubscriptionInner {
    fn drop(&mut self) {
        // Terminating the subscriber process fires the bidirectional link to the
        // channel actor, which traps the EXIT and removes this subscriber from
        // its fan-out list. This is the real-beamr unsubscribe-on-drop path.
        self.scheduler
            .terminate_process(self.pid, ExitReason::Normal);
    }
}
