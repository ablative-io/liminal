//! Connection-owned conversation resources.
//!
//! A connection process owns a [`ConnectionConversation`] per open conversation.
//! The default implementation ([`LiminalConversationResource`]) wraps a real
//! beamr-backed supervised conversation actor: messages are forwarded over its
//! handle, and a participant crash is surfaced structurally through the actor's
//! trapped linked-EXIT notifier — never by polling, sleeping, or a heartbeat.

use std::sync::{Mutex, mpsc};
use std::time::{Duration, Instant};

use liminal::channel::SchemaId;
use liminal::conversation::{ConversationActor, ConversationPhase, ParticipantPid};
use liminal::envelope::{Envelope, PublisherId};
use liminal::protocol::MessageEnvelope;

use crate::ServerError;

/// Marker for library conversation state owned by a single connection process.
pub trait ConversationResource: std::fmt::Debug + Send {
    /// Delegates one conversation message to the library resource.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the liminal library rejects the conversation message.
    fn message(&self, envelope: &MessageEnvelope) -> Result<(), ServerError>;

    /// Returns the participant PIDs linked to the supervised conversation, if any.
    ///
    /// A trace-only conversation has no participant process and returns an empty
    /// slice; a real supervised conversation returns the linked participant PIDs.
    fn participant_pids(&self) -> Vec<u64>;

    /// Returns true if the conversation has structurally detected a participant
    /// crash via the trapped linked-EXIT path (never by polling/sleeping).
    ///
    /// This is non-blocking: it observes whether the actor's exit notifier has
    /// already fired (the link-EXIT event landed) and falls back to the actor's
    /// structurally-set `Failed` phase. It does not sample liveness.
    fn has_detected_crash(&self) -> bool;

    /// Blocks up to `timeout` waiting for a structural linked-EXIT crash signal,
    /// returning the [`Instant`] the EXIT was observed inside the actor's link
    /// handler, or `None` if no crash is detected within the bound.
    ///
    /// The wait is event-driven (parks on the exit notifier and is woken by the
    /// EXIT handler), not a poll loop. Used by tests to prove real detection.
    fn await_crash(&self, timeout: Duration) -> Option<Instant>;

    /// Releases or finishes the library conversation resource.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the liminal library reports a close failure.
    fn close(self: Box<Self>) -> Result<(), ServerError>;
}

/// Library conversation resource owned by a single connection process.
#[derive(Debug)]
pub struct ConnectionConversation {
    resource: Box<dyn ConversationResource>,
}

impl ConnectionConversation {
    /// Creates an owned conversation resource for one connection process.
    #[must_use]
    pub fn new(resource: Box<dyn ConversationResource>) -> Self {
        Self { resource }
    }

    pub(super) fn message(&self, envelope: &MessageEnvelope) -> Result<(), ServerError> {
        self.resource.message(envelope)
    }

    /// Returns the participant PIDs linked to the supervised conversation.
    #[must_use]
    pub fn participant_pids(&self) -> Vec<u64> {
        self.resource.participant_pids()
    }

    /// Returns true once a participant crash has been structurally detected
    /// through the linked-EXIT mechanism.
    #[must_use]
    pub fn has_detected_crash(&self) -> bool {
        self.resource.has_detected_crash()
    }

    /// Blocks (event-driven) up to `timeout` for a structural crash signal.
    #[must_use]
    pub fn await_crash(&self, timeout: Duration) -> Option<Instant> {
        self.resource.await_crash(timeout)
    }

    pub(super) fn close(self) -> Result<(), ServerError> {
        self.resource.close()
    }
}

/// A real supervised conversation owned by one connection process.
///
/// Wraps a beamr-backed [`ConversationActor`] (a genuine supervised process that
/// traps its participants' EXITs) rather than a trace-only span. Messages are
/// forwarded to the actor over its handle, and a participant crash is surfaced
/// structurally through the link-EXIT notifier — never by polling.
#[derive(Debug)]
pub(super) struct LiminalConversationResource {
    actor: ConversationActor,
    participant: ParticipantPid,
    /// Receives the link-EXIT instant from the actor's trapped-EXIT handler. The
    /// single observed instant is cached in `crash_observed` once drained so the
    /// (one-shot) signal is not lost across repeated observations.
    exit_rx: Mutex<mpsc::Receiver<Instant>>,
    crash_observed: Mutex<Option<Instant>>,
}

impl LiminalConversationResource {
    /// Creates a resource around a booted, crash-armed supervised actor.
    pub(super) const fn new(
        actor: ConversationActor,
        participant: ParticipantPid,
        exit_rx: mpsc::Receiver<Instant>,
    ) -> Self {
        Self {
            actor,
            participant,
            exit_rx: Mutex::new(exit_rx),
            crash_observed: Mutex::new(None),
        }
    }

    /// Returns the cached crash instant or, non-blocking, the one already sent by
    /// the EXIT handler. This reads an already-fired structural event; it never
    /// sleeps or samples participant liveness.
    fn poll_exit_signal(&self) -> Option<Instant> {
        if let Ok(cached) = self.crash_observed.lock() {
            if let Some(instant) = *cached {
                return Some(instant);
            }
        }
        let received = self.exit_rx.lock().map_or(None, |rx| rx.try_recv().ok());
        self.cache(received);
        received
    }

    /// Caches an observed crash instant so the one-shot signal is replayable.
    fn cache(&self, instant: Option<Instant>) {
        if let Some(instant) = instant {
            if let Ok(mut cached) = self.crash_observed.lock() {
                *cached = Some(instant);
            }
        }
    }

    /// True when the actor's structurally-tracked phase is `Failed`, which the
    /// trapped-EXIT handler sets under `CrashPolicy::Fail`. This is a structural
    /// state read, not a liveness sample.
    fn actor_phase_failed(&self) -> bool {
        matches!(
            self.actor.state().map(|state| state.current_phase),
            Ok(ConversationPhase::Failed)
        )
    }
}

impl ConversationResource for LiminalConversationResource {
    fn message(&self, envelope: &MessageEnvelope) -> Result<(), ServerError> {
        // If the participant has already crashed (structural EXIT observed),
        // refuse the message rather than forwarding into a failed conversation.
        if self.poll_exit_signal().is_some() || self.actor_phase_failed() {
            return Err(ServerError::ListenerAccept {
                message: format!(
                    "conversation participant {} crashed; message rejected",
                    self.participant.get()
                ),
            });
        }
        let payload = envelope.payload.clone();
        let message = Envelope::new(payload, None, SchemaId::new(), PublisherId::default());
        self.actor
            .handle()
            .send(message)
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("conversation message delivery failed: {error}"),
            })
    }

    fn participant_pids(&self) -> Vec<u64> {
        vec![self.participant.get()]
    }

    fn has_detected_crash(&self) -> bool {
        self.poll_exit_signal().is_some() || self.actor_phase_failed()
    }

    fn await_crash(&self, timeout: Duration) -> Option<Instant> {
        if let Some(instant) = self.poll_exit_signal() {
            return Some(instant);
        }
        // Event-driven: park on the exit notifier; the actor's trapped-EXIT
        // handler wakes us the instant the participant's link fires. No polling.
        let received = self
            .exit_rx
            .lock()
            .map_or(None, |rx| rx.recv_timeout(timeout).ok());
        self.cache(received);
        received
    }

    fn close(self: Box<Self>) -> Result<(), ServerError> {
        let Self { actor, .. } = *self;
        // A crashed (Failed) conversation cannot transition to Closed; tearing
        // down its handle is sufficient and is not an error.
        if matches!(
            actor.state().map(|state| state.current_phase),
            Ok(ConversationPhase::Failed)
        ) {
            actor.handle().close().ok();
            return Ok(());
        }
        actor
            .handle()
            .close()
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("conversation close failed: {error}"),
            })
    }
}
