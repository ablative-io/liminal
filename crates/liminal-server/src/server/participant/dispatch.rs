//! Participant transport-to-semantics dispatch boundary.
//!
//! This module contains no lifecycle rules. The shared protocol crate gates and
//! decodes inbound frames, while an injected semantic handler returns one typed
//! protocol value. The server then performs only generic-frame encoding.

use std::collections::BTreeSet;
use std::sync::Arc;

use liminal::durability::DurableStore;
use liminal::protocol::Frame;
use liminal_protocol::lifecycle::ConnectionConversationTracking;
use liminal_protocol::wire::{
    BindingEpoch, ClientRequest, CodecError, ConnectionIncarnation, ConversationId,
    ObserverRecoveryHandshake, ParticipantId, ServerValue, ValidatedFrameLimit,
};

use super::dispatch_impact::DispatchImpact;
use super::transport::{
    ParticipantIngress, ParticipantSession, encode_server_value, gate_generic_frame,
    normalize_configured_frame_limit,
};
use super::{
    ObserverPublicationTarget, ParticipantOfferedProgress, ParticipantPublication,
    ParticipantPublicationInbox, ParticipantPublicationRegistry,
};

/// Connection-local semantic-conversation dispatch map (contract R-D1: the
/// connection's binding/interest/dispatch maps are bounded by the signed
/// `max_semantic_conversations_per_connection`).
///
/// One value lives in each connection process's state for the connection's
/// lifetime and is dropped with it. A conversation enters the map exactly
/// when a semantic operation for it COMMITS on this connection (the crate's
/// `ConnectionConversationCapacityCommit::newly_tracked` verdict) or when an
/// observer-recovery batch arms its refusal-only recipient; refusals and
/// replays leave the map untouched, exactly as the crate's stage-6 selector
/// leaves its counter unchanged. Growth is therefore bounded by the signed
/// limit the stage-6 selector enforces.
#[derive(Debug, Default)]
pub struct ParticipantConnectionConversations {
    tracked: BTreeSet<ConversationId>,
}

impl ParticipantConnectionConversations {
    /// Stage-6 tracking fact for one conversation on this connection.
    #[must_use]
    pub fn tracking(&self, conversation_id: ConversationId) -> ConnectionConversationTracking {
        if self.tracked.contains(&conversation_id) {
            ConnectionConversationTracking::AlreadyTracked
        } else {
            ConnectionConversationTracking::Untracked
        }
    }

    /// Current connection-conversation occupancy.
    #[must_use]
    pub fn occupied(&self) -> u64 {
        // `usize` fits `u64` on every supported target; if that ever stopped
        // holding, saturating at MAX fails CLOSED (capacity reads as full)
        // rather than silently under-counting occupancy.
        u64::try_from(self.tracked.len()).unwrap_or(u64::MAX)
    }

    /// Installs one conversation slot after a capacity-committing operation.
    pub fn track(&mut self, conversation_id: ConversationId) {
        self.tracked.insert(conversation_id);
    }

    /// Sorted tracked conversations (the observer-recovery preflight's
    /// current-occupancy input).
    #[must_use]
    pub fn tracked_conversations(&self) -> Vec<ConversationId> {
        self.tracked.iter().copied().collect()
    }
}

/// Connection-scoped authority facts supplied to participant semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParticipantConnectionContext {
    connection_incarnation: ConnectionIncarnation,
}

impl ParticipantConnectionContext {
    /// Captures the durably allocated incarnation of the receiving connection.
    #[must_use]
    pub const fn new(connection_incarnation: ConnectionIncarnation) -> Self {
        Self {
            connection_incarnation,
        }
    }

    /// Returns the durably allocated receiving-connection incarnation.
    #[must_use]
    pub const fn connection_incarnation(self) -> ConnectionIncarnation {
        self.connection_incarnation
    }
}

/// Exact terminal classification preserved from a connection's close trigger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionFateClass {
    /// A protocol-level clean Disconnect.
    CleanDisconnect,
    /// An orderly server `ForceClose`.
    ServerShutdown,
    /// EOF or transport loss without clean protocol evidence.
    ConnectionLost,
    /// A terminal protocol/decode refusal after participant binding.
    ProtocolError,
}

/// One durable bounded connection-fate intent delivered to participant semantics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionFateWorkItem {
    /// Durable incarnation-stream Open sequence used by participant source rows.
    pub open_sequence: u64,
    /// Exact connection whose current Bound slots are eligible.
    pub connection_incarnation: ConnectionIncarnation,
    /// Preserved close classification.
    pub class: ConnectionFateClass,
    /// Canonical sorted tracked-conversation snapshot owned by the Open.
    pub tracked_conversations: Vec<ConversationId>,
}

/// Process-wide terminal participant-service latch.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum ParticipantServiceFatal {
    /// A durable Open landed but one listed conversation could not durably finish its fate.
    #[error(
        "connection-fate intent {open_sequence} is incomplete at conversation {conversation_id}"
    )]
    ConnectionFateIntentIncomplete {
        /// Durable incarnation-stream Open sequence.
        open_sequence: u64,
        /// Exact conversation whose non-idempotent completion failed.
        conversation_id: ConversationId,
    },
}

/// Non-wire semantic service failure.
///
/// A failure is terminal to the connection attempt. It is deliberately not
/// convertible to [`ServerValue`], preventing the server from inventing a
/// lifecycle response when the protocol-owned transition did not produce one.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum ParticipantSemanticError {
    /// The complete semantic service is not installed.
    #[error("participant semantic service is unavailable")]
    Unavailable,
    /// Durable state or a protocol invariant prevented semantic completion.
    #[error("participant semantic service failed: {message}")]
    Internal {
        /// Diagnostic text for server logs; never placed on the participant wire.
        message: String,
    },
    /// A process-wide participant fatal has already latched.
    #[error(transparent)]
    ServiceFatal(ParticipantServiceFatal),
}

/// One semantic result paired with every dispatch effect durably installed by
/// the request before it returned.
///
/// The envelope deliberately owns the `Result`: a marker-drain prefix can
/// commit before a later retry fails, and that failure must not erase the
/// prefix's post-commit tell.
#[derive(Debug)]
pub struct ParticipantSemanticOutcome<T> {
    result: Result<T, ParticipantSemanticError>,
    impact: DispatchImpact,
}

impl<T> ParticipantSemanticOutcome<T> {
    /// Wraps a fixture or operation which installed no dispatch effect.
    #[must_use]
    pub const fn unchanged(result: Result<T, ParticipantSemanticError>) -> Self {
        Self {
            result,
            impact: DispatchImpact::Unchanged,
        }
    }

    /// Carries an operation result and its complete request accumulator.
    #[must_use]
    pub const fn new(result: Result<T, ParticipantSemanticError>, impact: DispatchImpact) -> Self {
        Self { result, impact }
    }

    pub(crate) fn into_parts(self) -> (Result<T, ParticipantSemanticError>, DispatchImpact) {
        (self.result, self.impact)
    }

    /// Returns the semantic result when an internal caller has no notification
    /// boundary. Production request dispatch uses the complete envelope; this
    /// projection exists for the trait's legacy direct-call entry point.
    pub(crate) fn into_result(self) -> Result<T, ParticipantSemanticError> {
        self.result
    }
}

/// One connection-fate result paired with every conversation impact committed
/// before the fate operation returned.
#[derive(Debug)]
pub struct ParticipantConnectionFateOutcome {
    result: Result<(), ParticipantSemanticError>,
    impacts: Vec<DispatchImpact>,
}

impl ParticipantConnectionFateOutcome {
    /// Wraps a fixture fate handler which committed no dispatch impact.
    #[must_use]
    pub const fn unchanged(result: Result<(), ParticipantSemanticError>) -> Self {
        Self {
            result,
            impacts: Vec::new(),
        }
    }

    /// Carries a fate result and every committed per-conversation impact.
    #[must_use]
    pub const fn new(
        result: Result<(), ParticipantSemanticError>,
        impacts: Vec<DispatchImpact>,
    ) -> Self {
        Self { result, impacts }
    }

    pub(crate) fn into_parts(self) -> (Result<(), ParticipantSemanticError>, Vec<DispatchImpact>) {
        (self.result, self.impacts)
    }

    pub(crate) fn into_result(self) -> Result<(), ParticipantSemanticError> {
        self.result
    }
}

/// Server-owned adapter from a decoded request to a protocol-owned value.
pub trait ParticipantSemanticHandler: core::fmt::Debug + Send + Sync {
    /// Applies one already authenticated and capability-gated request.
    ///
    /// `conversations` is the receiving connection's semantic-conversation
    /// dispatch map: the handler reads it for the crate's stage-6
    /// connection-conversation capacity facts and installs a slot exactly
    /// when an operation's capacity commit reports `newly_tracked`.
    ///
    /// # Errors
    ///
    /// Returns [`ParticipantSemanticError`] when no protocol value can be
    /// produced. The caller closes rather than fabricating a response.
    ///
    /// Production handlers override this with the signed
    /// `max_semantic_conversations_per_connection`; semantic-only fixtures own
    /// no publication conversations.
    ///
    /// Returns the latched fatal, when participant service must remain stopped.
    fn service_fatal(&self) -> Result<Option<ParticipantServiceFatal>, ParticipantSemanticError> {
        Ok(None)
    }

    /// Atomically latches the post-Open fatal selected by Decision B.
    ///
    /// Implementations must preserve the first fatal and return it on every later call.
    /// The default exists only for semantic fixtures which own no durable intents.
    ///
    /// # Errors
    ///
    /// Returns a semantic service error when the fatal latch cannot be inspected or updated.
    fn latch_connection_fate_intent_incomplete(
        &self,
        open_sequence: u64,
        conversation_id: ConversationId,
    ) -> Result<ParticipantServiceFatal, ParticipantSemanticError> {
        Ok(ParticipantServiceFatal::ConnectionFateIntentIncomplete {
            open_sequence,
            conversation_id,
        })
    }

    /// Applies every matching participant binding named by one durable Open.
    ///
    /// The incarnation-stream lock is not held while this method runs. Each
    /// implementation serializes conversations independently and must return
    /// only after every source and immediately executable specific fate flushes.
    ///
    /// # Errors
    ///
    /// Returns a semantic failure without consuming the Open; startup or the
    /// live fatal path retains it for exact replay.
    fn handle_connection_fate(
        &self,
        work_item: ConnectionFateWorkItem,
    ) -> Result<(), ParticipantSemanticError> {
        drop(work_item);
        Err(ParticipantSemanticError::Unavailable)
    }

    /// Applies connection fate while preserving every committed conversation's
    /// post-flush dispatch effects on both success and failure exits.
    fn handle_connection_fate_with_impact(
        &self,
        work_item: ConnectionFateWorkItem,
    ) -> ParticipantConnectionFateOutcome {
        ParticipantConnectionFateOutcome::unchanged(self.handle_connection_fate(work_item))
    }

    /// Repairs every remaining binding owned by a prior server incarnation.
    ///
    /// Startup calls this after all retained Opens complete and before publishing
    /// the incarnation authority, scheduler, listener, or new admission.
    ///
    /// # Errors
    ///
    /// Returns a semantic failure while startup still owns all publication seams.
    fn repair_unclean_server_restart(
        &self,
        current_server_incarnation: u64,
    ) -> Result<(), ParticipantSemanticError> {
        let _ = current_server_incarnation;
        Ok(())
    }

    /// Reports whether any listed conversation currently contains a Bound slot
    /// owned by this exact connection. Terminal decode funnels use this query to
    /// distinguish bound-only `ProtocolError` from pre-auth/detached/internal paths.
    ///
    /// # Errors
    ///
    /// Returns a semantic failure when exact bound authority cannot be inspected.
    fn connection_has_bound_participant(
        &self,
        connection_incarnation: ConnectionIncarnation,
        conversations: &[ConversationId],
    ) -> Result<bool, ParticipantSemanticError> {
        let _ = connection_incarnation;
        let _ = conversations;
        Ok(false)
    }

    fn publication_conversation_limit(&self) -> u64 {
        0
    }

    /// Resolves all live current bindings with pending durable obligations for
    /// one conversation. Production overrides this; semantic-only fixtures have
    /// no publication source.
    ///
    /// # Errors
    ///
    /// Returns a semantic fault when durable readiness cannot be resolved.
    fn ready_connection_incarnations(
        &self,
        _conversation_id: ConversationId,
    ) -> Result<Vec<ConnectionIncarnation>, ParticipantSemanticError> {
        Ok(Vec::new())
    }

    /// Selects the least durable recipient obligation for this incarnation,
    /// restarting from durable ack when `offered` names an older binding.
    ///
    /// # Errors
    ///
    /// Returns a semantic fault when the durable obligation owner is unavailable.
    fn next_publication(
        &self,
        _connection_incarnation: ConnectionIncarnation,
        _conversation_id: ConversationId,
        _offered: Option<ParticipantOfferedProgress>,
    ) -> Result<Option<ParticipantPublication>, ParticipantSemanticError> {
        Ok(None)
    }

    /// Checks that a held head still belongs to the exact current binding before
    /// it is offered after writable readiness.
    ///
    /// # Errors
    ///
    /// Returns a semantic fault when current binding authority cannot be read.
    fn publication_binding_is_current(
        &self,
        _conversation_id: ConversationId,
        _participant_id: ParticipantId,
        _binding_epoch: BindingEpoch,
    ) -> Result<bool, ParticipantSemanticError> {
        Ok(false)
    }

    /// Re-selects a held publication against current binding, cursor, debt, and
    /// outbox authority before its first offer. Semantic-only handlers retain
    /// the binding-only default; production overrides this with the full locked
    /// dispatch decision.
    ///
    /// # Errors
    ///
    /// Returns a semantic fault when current publication authority cannot be read.
    fn publication_is_current(
        &self,
        publication: &ParticipantPublication,
        offered: Option<ParticipantOfferedProgress>,
    ) -> Result<bool, ParticipantSemanticError> {
        if offered.is_some_and(|progress| progress.binding_epoch != publication.binding_epoch) {
            return Ok(false);
        }
        self.publication_binding_is_current(
            publication.conversation_id(),
            publication.participant_id,
            publication.binding_epoch,
        )
    }

    /// Records exact successful marker enqueue testimony. Non-marker offers are
    /// ignored by production after validating their current binding.
    ///
    /// # Errors
    ///
    /// Returns a semantic fault when exact offer testimony cannot be recorded.
    fn record_publication_offer(
        &self,
        _publication: &ParticipantPublication,
    ) -> Result<(), ParticipantSemanticError> {
        Ok(())
    }

    /// Applies observer recovery with the weak exact-live-connection target
    /// captured by the installed service. Semantic-only handlers delegate to
    /// their ordinary request path and do not own observer publication.
    ///
    /// # Errors
    ///
    /// Returns [`ParticipantSemanticError`] under the same contract as
    /// [`Self::handle`].
    fn handle_observer_recovery(
        &self,
        context: ParticipantConnectionContext,
        conversations: &mut ParticipantConnectionConversations,
        request: ObserverRecoveryHandshake,
        target: Option<ObserverPublicationTarget>,
    ) -> Result<ServerValue, ParticipantSemanticError> {
        drop(target);
        self.handle(
            context,
            conversations,
            ClientRequest::ObserverRecovery(request),
        )
    }

    /// Applies one request and preserves post-commit effects on every exit.
    ///
    /// Semantic-only fixtures default to an empty accumulator. Production
    /// overrides this boundary and returns operation-owned effects.
    fn handle_with_impact(
        &self,
        context: ParticipantConnectionContext,
        conversations: &mut ParticipantConnectionConversations,
        request: ClientRequest,
    ) -> ParticipantSemanticOutcome<ServerValue> {
        ParticipantSemanticOutcome::unchanged(self.handle(context, conversations, request))
    }

    /// Applies one decoded participant request to protocol-owned authority.
    ///
    /// # Errors
    ///
    /// Returns [`ParticipantSemanticError`] when durable or protocol authority
    /// cannot produce a truthful terminal value. The connection fails rather
    /// than fabricating a response.
    fn handle(
        &self,
        context: ParticipantConnectionContext,
        conversations: &mut ParticipantConnectionConversations,
        request: ClientRequest,
    ) -> Result<ServerValue, ParticipantSemanticError>;
}

/// Server-sealed participant activation token installed on a connection
/// supervisor.
///
/// The semantic handler and its durable store form one value so participant
/// capability activation cannot observe one without the other. The supervisor
/// uses the store to durably allocate connection incarnations before spawning a
/// connection process, and the process uses the handler only after that exact
/// incarnation has been carried into its state. The token atomically carries the
/// pair declared by server composition; it does not independently prove storage
/// namespace identity.
///
/// Construction and access are server-private. Until a complete production
/// lifecycle handler exists, external [`ConnectionServices`](crate::server::connection::ConnectionServices)
/// implementations cannot manufacture an activation token or advertise the
/// participant capability.
#[derive(Clone, Debug)]
pub struct InstalledParticipantService {
    handler: Arc<dyn ParticipantSemanticHandler>,
    durable_store: Arc<dyn DurableStore>,
    frame_limit: ValidatedFrameLimit,
    publication_registry: Arc<ParticipantPublicationRegistry>,
}

impl InstalledParticipantService {
    /// Pairs a semantic handler, its declared durable store, and the raw
    /// configured participant wire-frame limit.
    ///
    /// Production construction happens exactly once, in the server's
    /// connection-services layer, from the deployment's `[participant]`
    /// configuration; tests construct it directly with fixture handlers.
    ///
    /// # Errors
    ///
    /// Returns the shared codec error when the configured limit is smaller than
    /// the protocol's minimum complete frame.
    pub(crate) fn new(
        handler: Arc<dyn ParticipantSemanticHandler>,
        durable_store: Arc<dyn DurableStore>,
        configured_wf: u64,
    ) -> Result<Self, CodecError> {
        Ok(Self {
            handler,
            durable_store,
            frame_limit: normalize_configured_frame_limit(configured_wf)?,
            publication_registry: Arc::new(ParticipantPublicationRegistry::default()),
        })
    }

    /// Clones the durable store used by the installed participant service.
    #[must_use]
    pub(crate) fn durable_store(&self) -> Arc<dyn DurableStore> {
        Arc::clone(&self.durable_store)
    }

    /// Returns the normalized configured complete-frame limit advertised by
    /// this installed participant service.
    #[must_use]
    pub(crate) const fn frame_limit(&self) -> ValidatedFrameLimit {
        self.frame_limit
    }

    /// Returns the signed semantic-conversation allowance shared by publication
    /// readiness and connection-held encoded heads.
    #[must_use]
    pub(crate) fn publication_conversation_limit(&self) -> u64 {
        self.handler.publication_conversation_limit()
    }

    /// Creates the strongly connection-owned ready inbox at process spawn.
    #[must_use]
    pub(crate) fn new_publication_inbox(&self) -> ParticipantPublicationInbox {
        ParticipantPublicationInbox::new(self.handler.publication_conversation_limit())
    }

    /// Returns the shared weak publication registry.
    #[must_use]
    pub(crate) fn publication_registry(&self) -> &ParticipantPublicationRegistry {
        &self.publication_registry
    }

    /// Selects one exact durable publication through the installed production
    /// source.
    pub(crate) fn next_publication(
        &self,
        connection_incarnation: ConnectionIncarnation,
        conversation_id: ConversationId,
        offered: Option<ParticipantOfferedProgress>,
    ) -> Result<Option<ParticipantPublication>, ParticipantSemanticError> {
        self.handler
            .next_publication(connection_incarnation, conversation_id, offered)
    }

    pub(crate) fn publication_is_current(
        &self,
        publication: &ParticipantPublication,
        offered: Option<ParticipantOfferedProgress>,
    ) -> Result<bool, ParticipantSemanticError> {
        self.handler.publication_is_current(publication, offered)
    }

    pub(crate) fn record_publication_offer(
        &self,
        publication: &ParticipantPublication,
    ) -> Result<(), ParticipantSemanticError> {
        self.handler.record_publication_offer(publication)
    }

    fn notify_impact(&self, impact: &DispatchImpact) -> Result<(), ParticipantSemanticError> {
        let Some(conversation_id) = impact.conversation_id() else {
            return Ok(());
        };
        for target in impact.target_union() {
            self.publication_registry
                .notify(
                    target.binding_epoch().connection_incarnation,
                    conversation_id,
                )
                .map_err(|error| ParticipantSemanticError::Internal {
                    message: format!("participant publication wake failed: {error}"),
                })?;
        }
        Ok(())
    }
}

impl ParticipantSemanticHandler for InstalledParticipantService {
    fn service_fatal(&self) -> Result<Option<ParticipantServiceFatal>, ParticipantSemanticError> {
        self.handler.service_fatal()
    }

    fn latch_connection_fate_intent_incomplete(
        &self,
        open_sequence: u64,
        conversation_id: ConversationId,
    ) -> Result<ParticipantServiceFatal, ParticipantSemanticError> {
        self.handler
            .latch_connection_fate_intent_incomplete(open_sequence, conversation_id)
    }

    fn publication_conversation_limit(&self) -> u64 {
        self.handler.publication_conversation_limit()
    }

    fn handle_connection_fate(
        &self,
        work_item: ConnectionFateWorkItem,
    ) -> Result<(), ParticipantSemanticError> {
        let outcome = self.handler.handle_connection_fate_with_impact(work_item);
        let (result, impacts) = outcome.into_parts();
        for impact in &impacts {
            self.notify_impact(impact)?;
        }
        result
    }

    fn handle_connection_fate_with_impact(
        &self,
        work_item: ConnectionFateWorkItem,
    ) -> ParticipantConnectionFateOutcome {
        let result = self.handle_connection_fate(work_item);
        ParticipantConnectionFateOutcome::unchanged(result)
    }

    fn repair_unclean_server_restart(
        &self,
        current_server_incarnation: u64,
    ) -> Result<(), ParticipantSemanticError> {
        self.handler
            .repair_unclean_server_restart(current_server_incarnation)
    }

    fn connection_has_bound_participant(
        &self,
        connection_incarnation: ConnectionIncarnation,
        conversations: &[ConversationId],
    ) -> Result<bool, ParticipantSemanticError> {
        self.handler
            .connection_has_bound_participant(connection_incarnation, conversations)
    }

    fn handle(
        &self,
        context: ParticipantConnectionContext,
        conversations: &mut ParticipantConnectionConversations,
        request: ClientRequest,
    ) -> Result<ServerValue, ParticipantSemanticError> {
        if let ClientRequest::ObserverRecovery(request) = request {
            let target = self
                .publication_registry
                .observer_target(context.connection_incarnation())
                .map_err(|error| ParticipantSemanticError::Internal {
                    message: format!("observer publication target failed: {error}"),
                })?;
            return self
                .handler
                .handle_observer_recovery(context, conversations, request, target);
        }
        let outcome = self
            .handler
            .handle_with_impact(context, conversations, request);
        let (result, impact) = outcome.into_parts();
        self.notify_impact(&impact)?;
        result
    }
}

/// Result of dispatching one generic frame through participant transport.
#[derive(Debug)]
pub enum ParticipantDispatch {
    /// The generic frame belongs to another protocol.
    NotParticipant,
    /// Exact encoded response selected by the shared gate or semantic handler.
    Respond(Frame),
    /// Exact crate-owned pre-semantic rejection, followed by connection close.
    RespondThenClose(Frame),
    /// No truthful participant response exists; the connection must fail closed.
    Fatal(ParticipantDispatchError),
}

/// Failure after a generic frame has entered participant dispatch.
#[derive(Debug, thiserror::Error)]
pub enum ParticipantDispatchError {
    /// The preserved generic frame could not represent a canonical participant frame.
    #[error("invalid generic participant frame")]
    InvalidGenericFrame,
    /// The semantic handler could not produce a protocol value.
    #[error(transparent)]
    Semantic(#[from] ParticipantSemanticError),
    /// The crate-produced value could not be encoded into the generic transport.
    #[error("failed to encode participant response: {0:?}")]
    Encode(CodecError),
}

/// Gates, decodes, semantically applies, and encodes one participant frame.
///
/// Transport rejection values originate in `liminal-protocol`; semantic values
/// originate only in `handler`. No lifecycle outcome is constructed here.
#[must_use]
pub fn dispatch_generic_frame(
    frame: &Frame,
    authenticated: bool,
    session: ParticipantSession,
    context: ParticipantConnectionContext,
    conversations: &mut ParticipantConnectionConversations,
    handler: &dyn ParticipantSemanticHandler,
) -> ParticipantDispatch {
    let (value, close_after_response) = match gate_generic_frame(frame, authenticated, session) {
        ParticipantIngress::NotParticipant => return ParticipantDispatch::NotParticipant,
        ParticipantIngress::Rejected(rejection) => {
            (ServerValue::ParticipantTransportRejected(rejection), true)
        }
        ParticipantIngress::InvalidGenericFrame => {
            return ParticipantDispatch::Fatal(ParticipantDispatchError::InvalidGenericFrame);
        }
        ParticipantIngress::Request(request) => {
            match handler.handle(context, conversations, request) {
                Ok(value) => (value, false),
                Err(error) => {
                    return ParticipantDispatch::Fatal(ParticipantDispatchError::Semantic(error));
                }
            }
        }
    };
    match encode_server_value(value) {
        Ok(frame) if close_after_response => ParticipantDispatch::RespondThenClose(frame),
        Ok(frame) => ParticipantDispatch::Respond(frame),
        Err(error) => ParticipantDispatch::Fatal(ParticipantDispatchError::Encode(error)),
    }
}
