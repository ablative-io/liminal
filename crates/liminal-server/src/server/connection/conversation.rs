//! Connection-owned conversation resources.
//!
//! A connection process owns a [`ConnectionConversation`] per open conversation.
//! The default implementation ([`LiminalConversationResource`]) wraps a real
//! beamr-backed supervised conversation actor: messages are forwarded over its
//! handle, and a participant crash is surfaced structurally through the actor's
//! trapped linked-EXIT notifier — never by polling, sleeping, or a heartbeat.

use std::sync::{Mutex, mpsc};
use std::time::{Duration, Instant};

use liminal::LiminalError;
use liminal::channel::SchemaId;
use liminal::conversation::{
    ConversationActor, ConversationPhase, ConversationState, ParticipantPid,
};
use liminal::envelope::{Envelope, PublisherId};
use liminal::protocol::{
    CausalContext as ProtocolCausalContext, MessageEnvelope, SchemaId as ProtocolSchemaId,
};

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

    /// Receives the next reply the participant produced for this conversation,
    /// bounded by `timeout`.
    ///
    /// A real participant processes each forwarded message and delivers a reply
    /// back through the conversation; this drains that reply. A trace-only or
    /// non-replying resource times out.
    ///
    /// # Errors
    /// Returns [`ServerError`] when no reply arrives within `timeout`, the
    /// participant crashed, or the conversation is unavailable.
    fn receive_reply(&self, timeout: Duration) -> Result<MessageEnvelope, ServerError>;

    /// R1(vi)(a): non-blocking drain of one buffered participant reply, if any.
    ///
    /// Replaces the removed in-slice BLOCKING `receive_reply` on the request-reply
    /// path: the connection polls this on its own slice and correlates the reply
    /// through its pending-reply table. Defaulted to `None` for resources with no
    /// live reply queue (trace-only / test stand-ins).
    fn try_receive_reply(&self) -> Option<MessageEnvelope> {
        None
    }

    /// Non-consuming reply availability query for the post-arm race barrier.
    fn has_pending_reply(&self) -> bool {
        false
    }

    /// R1(vi)(a): installs the reply-availability notifier (fired on the reply
    /// queue's empty→non-empty transition and on terminal actor error), captured at
    /// conversation open. Defaulted to a no-op for resources with no reply queue.
    fn register_reply_notifier(&self, _notifier: std::sync::Arc<dyn Fn() + Send + Sync>) {}

    /// Releases or finishes the library conversation resource.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the liminal library reports a close failure.
    fn close(self: Box<Self>) -> Result<(), ServerError>;

    /// Releases the resource without requiring its backing actor to run:
    /// bounded, non-blocking, and idempotent. Connection teardown paths (and
    /// the teardown `Drop` backstop) MUST use this instead of
    /// [`Self::close`] — `close` is a request/reply round trip into the
    /// conversation scheduler, and a teardown that waits on another scheduler
    /// being live re-creates the wedged-worker failure this repair removes.
    /// Deliberately required, not defaulted: every resource author must decide
    /// what teardown-safe release means for their live state (a defaulted
    /// no-op would let a resource that needs real cleanup leak silently). A
    /// resource with no live process behind it implements this as a plain
    /// `drop(self)`.
    fn finalize(self: Box<Self>);
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

    /// Receives the next participant reply for this conversation, bounded by
    /// `timeout`.
    ///
    /// # Errors
    /// Returns [`ServerError`] when no reply arrives in time or the conversation
    /// is unavailable.
    pub fn receive_reply(&self, timeout: Duration) -> Result<MessageEnvelope, ServerError> {
        self.resource.receive_reply(timeout)
    }

    /// R1(vi)(a): non-blocking drain of one buffered participant reply.
    pub(super) fn try_receive_reply(&self) -> Option<MessageEnvelope> {
        self.resource.try_receive_reply()
    }

    pub(super) fn has_pending_reply(&self) -> bool {
        self.resource.has_pending_reply()
    }

    /// R1(vi)(a): installs the reply-availability notifier at conversation open.
    pub(super) fn register_reply_notifier(&self, notifier: std::sync::Arc<dyn Fn() + Send + Sync>) {
        self.resource.register_reply_notifier(notifier);
    }

    pub(super) fn close(self) -> Result<(), ServerError> {
        self.resource.close()
    }

    /// Non-blocking teardown release; see [`ConversationResource::finalize`].
    pub(super) fn finalize(self) {
        self.resource.finalize();
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
        observe_exit(&self.crash_observed, &self.exit_rx, None)
    }

    /// True when the actor's structurally-tracked phase is `Failed`, which the
    /// trapped-EXIT handler sets under `CrashPolicy::Fail`. This is a structural
    /// state read, not a liveness sample.
    fn actor_phase_failed(&self) -> bool {
        state_reads_failed(&self.actor.state())
    }
}

/// Reads the one-shot participant-EXIT signal, caching the observed instant so
/// repeated observations replay it. `wait` bounds a blocking receive; `None`
/// polls without blocking. The single seam behind both [`poll_exit_signal`] and
/// [`await_crash`].
///
/// [`poll_exit_signal`]: LiminalConversationResource::poll_exit_signal
/// [`await_crash`]: LiminalConversationResource::await_crash
/// A DISCONNECTED channel is a crash OBSERVATION, not the absence of one: the
/// sender lives inside the actor's trapped-EXIT handler, so its disconnection
/// means the participant actor is gone. That branch used to collapse to "no
/// crash", which is how a connection kept forwarding conversation messages into
/// a vanished participant.
fn observe_exit(
    crash_observed: &Mutex<Option<Instant>>,
    exit_rx: &Mutex<mpsc::Receiver<Instant>>,
    wait: Option<Duration>,
) -> Option<Instant> {
    if let Ok(cached) = crash_observed.lock()
        && let Some(instant) = *cached
    {
        return Some(instant);
    }
    let received = exit_rx.lock().map_or(None, |rx| {
        wait.map_or_else(
            || match rx.try_recv() {
                Ok(instant) => Some(instant),
                Err(mpsc::TryRecvError::Empty) => None,
                Err(mpsc::TryRecvError::Disconnected) => Some(vanished_actor_instant()),
            },
            |timeout| match rx.recv_timeout(timeout) {
                Ok(instant) => Some(instant),
                Err(mpsc::RecvTimeoutError::Timeout) => None,
                Err(mpsc::RecvTimeoutError::Disconnected) => Some(vanished_actor_instant()),
            },
        )
    });
    if let Some(instant) = received
        && let Ok(mut cached) = crash_observed.lock()
    {
        *cached = Some(instant);
    }
    received
}

/// The observation time for a crash inferred from a disconnected EXIT channel.
/// The handler never got to send its own instant, so this is the earliest moment
/// the host can honestly claim to have seen the crash. Warned once — the cache
/// replays every later observation without re-entering this path.
fn vanished_actor_instant() -> Instant {
    tracing::warn!(
        "conversation exit channel disconnected; the participant actor is gone \
         and is reported as crashed"
    );
    Instant::now()
}

/// Maps one actor state query to "this conversation has structurally failed".
///
/// An `Err` is treated as FAILED, not as healthy. A cleanly closed or finalized
/// conversation still answers `Ok` from its host-side snapshot, and the query
/// blocks on an unbounded `wait_for` rather than a timeout, so an `Err` here is
/// never a clean close and never a transient slow-actor sample — it is
/// `ensure_running` failing, the command queue refusing, or the response channel
/// closing under the query. All three mean the actor is unreachable.
fn state_reads_failed(state: &Result<ConversationState, LiminalError>) -> bool {
    match state {
        Ok(state) => matches!(state.current_phase, ConversationPhase::Failed),
        Err(error) => {
            tracing::warn!(
                %error,
                "conversation actor could not service a state query; treating the \
                 unreachable actor as failed"
            );
            true
        }
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
        // Event-driven: park on the exit notifier; the actor's trapped-EXIT
        // handler wakes us the instant the participant's link fires. No polling.
        observe_exit(&self.crash_observed, &self.exit_rx, Some(timeout))
    }

    fn receive_reply(&self, timeout: Duration) -> Result<MessageEnvelope, ServerError> {
        // The participant produced a reply that the conversation actor delivered
        // back into the conversation; drain it (bounded). This is the reply leg
        // of the request-reply path — proof the participant genuinely processed
        // the forwarded message, not just that it was linked.
        let reply =
            self.actor
                .receive_timeout(timeout)
                .map_err(|error| ServerError::ListenerAccept {
                    message: format!("conversation reply receive failed: {error}"),
                })?;
        Ok(MessageEnvelope::new(
            ProtocolSchemaId::new([0; ProtocolSchemaId::WIRE_LEN]),
            ProtocolCausalContext::independent(),
            reply.payload,
        ))
    }

    fn try_receive_reply(&self) -> Option<MessageEnvelope> {
        // Non-blocking host-side drain of one buffered participant reply, framed
        // as the wire reply envelope (schema/causal metadata are not bridged in
        // v1, matching the removed blocking `receive_reply`).
        let reply = self.actor.try_take_reply()?;
        Some(MessageEnvelope::new(
            ProtocolSchemaId::new([0; ProtocolSchemaId::WIRE_LEN]),
            ProtocolCausalContext::independent(),
            reply.payload,
        ))
    }

    fn has_pending_reply(&self) -> bool {
        self.actor.has_pending_reply()
    }

    fn register_reply_notifier(&self, notifier: std::sync::Arc<dyn Fn() + Send + Sync>) {
        self.actor.register_reply_notifier(notifier);
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

    fn finalize(self: Box<Self>) {
        self.actor.finalize();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, mpsc};
    use std::time::{Duration, Instant};

    use liminal::LiminalError;
    use liminal::channel::ChannelMode;
    use liminal::conversation::{ConversationPhase, ConversationState};

    use super::{observe_exit, state_reads_failed};

    /// A crash-detection fixture: the two mutexes `observe_exit` reads, plus the
    /// live sender the actor's trapped-EXIT handler would own.
    fn exit_channel() -> (
        mpsc::SyncSender<Instant>,
        Mutex<Option<Instant>>,
        Mutex<mpsc::Receiver<Instant>>,
    ) {
        let (tx, rx) = mpsc::sync_channel::<Instant>(1);
        (tx, Mutex::new(None), Mutex::new(rx))
    }

    fn state_with(current_phase: ConversationPhase) -> ConversationState {
        ConversationState {
            current_phase,
            context: Vec::new(),
            deadline: None,
            participants: Vec::new(),
            mode: ChannelMode::Ephemeral,
        }
    }

    /// THE site-2 headline. The exit sender lives inside the actor's trapped-EXIT
    /// handler, so a DISCONNECTED channel means that actor is gone — which is a
    /// crash observation, not the absence of one. Reading `Disconnected` as "no
    /// crash" is what let a connection keep forwarding conversation messages into
    /// a vanished participant.
    #[test]
    fn a_dropped_exit_sender_is_a_crash_observation() {
        let (tx, crash_observed, exit_rx) = exit_channel();
        drop(tx);

        let observed = observe_exit(&crash_observed, &exit_rx, None);

        assert!(
            observed.is_some(),
            "a vanished participant actor must read as a crash, not as no-crash"
        );
    }

    /// The same claim on the blocking leg, which additionally must not burn the
    /// caller's whole timeout before reporting a crash that has already happened.
    #[test]
    fn a_dropped_exit_sender_ends_the_crash_wait_immediately() {
        let (tx, crash_observed, exit_rx) = exit_channel();
        drop(tx);

        let started = Instant::now();
        let observed = observe_exit(&crash_observed, &exit_rx, Some(Duration::from_secs(30)));
        let elapsed = started.elapsed();

        assert!(
            observed.is_some(),
            "a vanished participant actor must read as a crash on the blocking leg too"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "the crash is already observable; the wait must not run to its bound (took {elapsed:?})"
        );
    }

    /// A state query the actor cannot service means the actor is unreachable —
    /// it has no live process to answer, and a cleanly closed or finalized
    /// conversation still answers `Ok` from its host-side snapshot. Reading that
    /// `Err` as "not failed" reports a conversation whose actor is gone as
    /// healthy.
    #[test]
    fn an_unserviceable_state_query_reads_as_failed() {
        let unreachable = Err(LiminalError::ConversationFailed {
            message: "state query response channel closed".to_owned(),
        });

        assert!(
            state_reads_failed(&unreachable),
            "an actor that cannot answer a state query must not read as healthy"
        );
    }

    /// The healthy path is untouched: a live phase is not a failure, and the
    /// structurally-set `Failed` phase still is.
    #[test]
    fn live_phases_still_read_healthy_and_failed_still_reads_failed() {
        assert!(!state_reads_failed(&Ok(state_with(
            ConversationPhase::Active
        ))));
        assert!(state_reads_failed(&Ok(state_with(
            ConversationPhase::Failed
        ))));
    }
}
