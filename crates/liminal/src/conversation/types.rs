use std::sync::Arc;
use std::time::{Duration, Instant};

use beamr::process::ExitReason;
use beamr::process::registry::ProcessHandle;

use crate::channel::ChannelMode;
use crate::envelope::Envelope;
use crate::error::LiminalError;
use crate::tracing::{ConversationSpan, FinishedSpan, TraceContext};

#[derive(Debug)]
pub struct Conversation {
    span: ConversationSpan,
}

impl Conversation {
    #[must_use]
    pub fn start(conversation_id: impl Into<String>) -> Self {
        Self {
            span: ConversationSpan::root(conversation_id),
        }
    }

    #[must_use]
    pub fn spawn_child(&self, conversation_id: impl Into<String>) -> Self {
        Self {
            span: self.span.child(conversation_id),
        }
    }

    #[must_use]
    pub const fn message<Payload>(&self, payload: Payload) -> ConversationMessage<Payload> {
        ConversationMessage::new(payload, self.span.message_context())
    }

    #[must_use]
    pub fn name(&self) -> &str {
        self.span.name()
    }

    #[must_use]
    pub const fn trace_context(&self) -> TraceContext {
        self.span.context()
    }

    #[must_use]
    pub const fn parent_trace_context(&self) -> Option<TraceContext> {
        self.span.parent()
    }

    #[must_use]
    pub fn finish(self) -> FinishedSpan {
        self.span.finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConversationMessage<Payload> {
    payload: Payload,
    trace_context: TraceContext,
}

impl<Payload> ConversationMessage<Payload> {
    const fn new(payload: Payload, trace_context: TraceContext) -> Self {
        Self {
            payload,
            trace_context,
        }
    }

    #[must_use]
    pub const fn trace_context(&self) -> TraceContext {
        self.trace_context
    }

    #[must_use]
    pub const fn payload(&self) -> &Payload {
        &self.payload
    }

    #[must_use]
    pub fn into_payload(self) -> Payload {
        self.payload
    }

    #[must_use]
    pub fn map<NextPayload>(
        self,
        map_payload: impl FnOnce(Payload) -> NextPayload,
    ) -> ConversationMessage<NextPayload> {
        ConversationMessage {
            payload: map_payload(self.payload),
            trace_context: self.trace_context,
        }
    }
}

/// PID of a beamr process participating in a conversation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ParticipantPid(u64);

impl ParticipantPid {
    /// Creates a participant PID wrapper from a raw beamr PID.
    #[must_use]
    pub const fn new(pid: u64) -> Self {
        Self(pid)
    }

    /// Returns the raw beamr PID.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for ParticipantPid {
    fn from(pid: u64) -> Self {
        Self::new(pid)
    }
}

impl From<ProcessHandle> for ParticipantPid {
    fn from(handle: ProcessHandle) -> Self {
        Self::new(handle.pid())
    }
}

/// Policy applied when a linked participant exits unexpectedly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrashPolicy {
    /// Fail the conversation immediately.
    Fail,
    /// Record that routing should select another participant in a later brief.
    RouteToNext,
    /// Record that compensation should run in a later brief.
    Compensate,
}

/// Required configuration for a conversation actor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConversationConfig {
    /// Participant process identifiers linked to the conversation actor.
    pub participants: Vec<ParticipantPid>,
    /// Optional in-memory deadline for the conversation.
    pub timeout: Option<Duration>,
    /// Durability mode marker; durable persistence is implemented elsewhere.
    pub mode: ChannelMode,
    /// Participant-crash policy. This has no default and must be provided.
    pub on_crash: CrashPolicy,
}

impl ConversationConfig {
    /// Creates conversation configuration from all required fields.
    #[must_use]
    pub const fn new(
        participants: Vec<ParticipantPid>,
        timeout: Option<Duration>,
        mode: ChannelMode,
        on_crash: CrashPolicy,
    ) -> Self {
        Self {
            participants,
            timeout,
            mode,
            on_crash,
        }
    }
}

/// Lifecycle phase of a conversation actor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConversationPhase {
    /// Actor exists but no exchange has started.
    Created,
    /// Messages are being exchanged.
    Active,
    /// Normal close has begun.
    Completing,
    /// Normal close finished.
    Closed,
    /// The conversation failed.
    Failed,
}

/// Liveness recorded for one linked participant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantHealth {
    /// The participant has not emitted an EXIT signal.
    Alive,
    /// The participant emitted an EXIT signal.
    Dead,
}

/// Participant liveness snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParticipantStatus {
    /// Participant PID.
    pub participant: ParticipantPid,
    /// Last known participant liveness.
    pub health: ParticipantHealth,
    /// Instant the participant's EXIT signal was observed, set when the
    /// participant is marked dead. Replayed to a late exit-notifier registrant
    /// so a crash that lands before registration is not lost.
    pub exited_at: Option<Instant>,
    /// EXIT reason carried by the participant's trapped exit signal, set when
    /// the participant is marked dead. `None` while alive, or when the death
    /// was discovered without a signal (boot pruning a pid that died while no
    /// actor was linked to it).
    pub exit_reason: Option<ExitReason>,
}

impl ParticipantStatus {
    /// Creates an alive participant status.
    #[must_use]
    pub const fn alive(participant: ParticipantPid) -> Self {
        Self {
            participant,
            health: ParticipantHealth::Alive,
            exited_at: None,
            exit_reason: None,
        }
    }

    /// Marks this participant dead, recording the instant its EXIT was observed
    /// and the reason its exit signal carried (`None` when the death was
    /// discovered without a signal).
    pub const fn mark_dead_at(&mut self, at: Instant, reason: Option<ExitReason>) {
        self.health = ParticipantHealth::Dead;
        self.exited_at = Some(at);
        self.exit_reason = reason;
    }
}

/// Opaque conversation context accumulated by the actor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConversationContextEntry {
    /// Envelope delivered to the conversation.
    Sent(Envelope),
    /// Envelope delivered from the conversation to a receiver.
    Received(Envelope),
    /// Participant crash handled according to the configured policy.
    ParticipantCrashed {
        /// Crashed participant PID.
        participant: ParticipantPid,
        /// Policy applied by the actor.
        policy: CrashPolicy,
    },
}

/// Queryable state snapshot owned by a conversation actor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConversationState {
    /// Current lifecycle phase.
    pub current_phase: ConversationPhase,
    /// Accumulated exchanged messages and lifecycle references.
    pub context: Vec<ConversationContextEntry>,
    /// Optional in-memory deadline.
    pub deadline: Option<Instant>,
    /// Last known participant liveness.
    pub participants: Vec<ParticipantStatus>,
    /// Durability mode marker retained for diagnostics and future resume work.
    pub mode: ChannelMode,
}

impl ConversationState {
    /// Creates initial state for a conversation config.
    #[must_use]
    pub fn from_config(config: &ConversationConfig, now: Instant) -> Self {
        let deadline = config.timeout.map(|timeout| now + timeout);
        let participants = config
            .participants
            .iter()
            .copied()
            .map(ParticipantStatus::alive)
            .collect();

        Self {
            current_phase: ConversationPhase::Created,
            context: Vec::new(),
            deadline,
            participants,
            mode: config.mode,
        }
    }

    /// Transitions Created to Active. Active is accepted as idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`LiminalError::ConversationFailed`] when the state cannot become active.
    pub fn activate(&mut self) -> Result<(), LiminalError> {
        match self.current_phase {
            ConversationPhase::Created => {
                self.current_phase = ConversationPhase::Active;
                Ok(())
            }
            ConversationPhase::Active => Ok(()),
            phase => Err(invalid_transition(phase, ConversationPhase::Active)),
        }
    }

    /// Transitions Active to Completing.
    ///
    /// # Errors
    ///
    /// Returns [`LiminalError::ConversationFailed`] when the state is not active.
    pub fn begin_completing(&mut self) -> Result<(), LiminalError> {
        match self.current_phase {
            ConversationPhase::Active => {
                self.current_phase = ConversationPhase::Completing;
                Ok(())
            }
            ConversationPhase::Completing => Ok(()),
            phase => Err(invalid_transition(phase, ConversationPhase::Completing)),
        }
    }

    /// Transitions Completing to Closed.
    ///
    /// # Errors
    ///
    /// Returns [`LiminalError::ConversationFailed`] when completion has not begun.
    pub fn close(&mut self) -> Result<(), LiminalError> {
        match self.current_phase {
            ConversationPhase::Completing => {
                self.current_phase = ConversationPhase::Closed;
                Ok(())
            }
            ConversationPhase::Closed => Ok(()),
            phase => Err(invalid_transition(phase, ConversationPhase::Closed)),
        }
    }

    /// Transitions any phase to Failed.
    pub const fn fail(&mut self) {
        self.current_phase = ConversationPhase::Failed;
    }

    /// Records an envelope sent into the conversation.
    pub fn record_sent(&mut self, envelope: Envelope) {
        self.context.push(ConversationContextEntry::Sent(envelope));
    }

    /// Records an envelope received from the conversation.
    pub fn record_received(&mut self, envelope: Envelope) {
        self.context
            .push(ConversationContextEntry::Received(envelope));
    }

    /// Records participant crash handling and marks that participant dead,
    /// stamping `exited_at` as the instant the EXIT signal was observed and
    /// `reason` as the signal's exit reason (when one was carried).
    pub fn record_participant_crash(
        &mut self,
        participant: ParticipantPid,
        policy: CrashPolicy,
        exited_at: Instant,
        reason: Option<ExitReason>,
    ) {
        for status in &mut self.participants {
            if status.participant == participant {
                status.mark_dead_at(exited_at, reason);
            }
        }
        self.context
            .push(ConversationContextEntry::ParticipantCrashed {
                participant,
                policy,
            });
    }
}

/// Cloneable handle for interacting with a conversation actor.
#[derive(Clone, Debug)]
pub struct ConversationHandle {
    backend: Arc<dyn ConversationHandleBackend>,
}

impl ConversationHandle {
    pub(crate) fn new(backend: Arc<dyn ConversationHandleBackend>) -> Self {
        Self { backend }
    }

    /// Sends a message envelope to the conversation actor.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when the actor cannot accept or process the message.
    pub fn send(&self, message: impl Into<Envelope>) -> Result<(), LiminalError> {
        self.backend.send(message.into())
    }

    /// Receives the next available envelope from the conversation actor.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when the actor is closed, failed, or unavailable.
    pub fn receive(&self) -> Result<Envelope, LiminalError> {
        self.backend.receive()
    }

    /// Closes the conversation normally.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when the actor cannot close normally.
    pub fn close(&self) -> Result<(), LiminalError> {
        self.backend.close()
    }

    /// Queries the actor state for diagnostics.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when the state cannot be queried.
    pub fn query_state(&self) -> Result<ConversationState, LiminalError> {
        self.backend.query_state()
    }

    /// Returns the current actor PID.
    ///
    /// # Errors
    ///
    /// Returns a [`LiminalError`] when the supervisor cannot inspect the actor.
    pub fn actor_pid(&self) -> Result<ParticipantPid, LiminalError> {
        self.backend.actor_pid()
    }
}

pub(crate) trait ConversationHandleBackend: std::fmt::Debug + Send + Sync {
    fn send(&self, message: Envelope) -> Result<(), LiminalError>;
    fn receive(&self) -> Result<Envelope, LiminalError>;
    fn close(&self) -> Result<(), LiminalError>;
    fn query_state(&self) -> Result<ConversationState, LiminalError>;
    fn actor_pid(&self) -> Result<ParticipantPid, LiminalError>;
}

fn invalid_transition(from: ConversationPhase, to: ConversationPhase) -> LiminalError {
    LiminalError::ConversationFailed {
        message: format!("invalid conversation phase transition from {from:?} to {to:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::{Conversation, ConversationHandle};

    #[test]
    fn starting_conversation_creates_named_span_with_fresh_trace_context() {
        let first = Conversation::start("conversation-1");
        let second = Conversation::start("conversation-2");

        assert_eq!(first.name(), "conversation-1");
        assert_eq!(first.parent_trace_context(), None);
        assert_ne!(first.trace_context().trace_id(), 0);
        assert_ne!(first.trace_context().span_id(), 0);
        assert_ne!(
            first.trace_context().trace_id(),
            second.trace_context().trace_id()
        );
    }

    #[test]
    fn messages_inherit_conversation_trace_context_automatically() {
        let conversation = Conversation::start("conversation");
        let message = conversation.message("payload");

        assert_eq!(message.payload(), &"payload");
        assert_eq!(message.trace_context(), conversation.trace_context());
    }

    #[test]
    fn child_conversation_references_parent_trace_context() {
        let parent = Conversation::start("parent");
        let child = parent.spawn_child("child");

        assert_eq!(child.name(), "child");
        assert_eq!(child.parent_trace_context(), Some(parent.trace_context()));
        assert_eq!(
            child.trace_context().trace_id(),
            parent.trace_context().trace_id()
        );
        assert_ne!(
            child.trace_context().span_id(),
            parent.trace_context().span_id()
        );
    }

    #[test]
    fn message_mapping_preserves_trace_context() {
        let conversation = Conversation::start("conversation");
        let context = conversation.trace_context();
        let mapped = conversation.message(1_u8).map(u16::from);

        assert_eq!(mapped.payload(), &1_u16);
        assert_eq!(mapped.trace_context(), context);
    }

    #[test]
    fn conversation_handle_is_clone_send_sync() {
        fn assert_clone_send_sync<T: Clone + Send + Sync>() {}

        assert_clone_send_sync::<ConversationHandle>();
    }
}
