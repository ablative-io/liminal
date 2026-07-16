//! Participant transport-to-semantics dispatch boundary.
//!
//! This module contains no lifecycle rules. The shared protocol crate gates and
//! decodes inbound frames, while an injected semantic handler returns one typed
//! protocol value. The server then performs only generic-frame encoding.

use std::sync::Arc;

use liminal::durability::DurableStore;
use liminal::protocol::Frame;
use liminal_protocol::wire::{
    ClientRequest, CodecError, ConnectionIncarnation, ServerValue, ValidatedFrameLimit,
};

#[cfg(test)]
use super::transport::normalize_configured_frame_limit;
use super::transport::{
    ParticipantIngress, ParticipantSession, encode_server_value, gate_generic_frame,
};

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
}

/// Server-owned adapter from a decoded request to a protocol-owned value.
pub trait ParticipantSemanticHandler: core::fmt::Debug + Send + Sync {
    /// Applies one already authenticated and capability-gated request.
    ///
    /// # Errors
    ///
    /// Returns [`ParticipantSemanticError`] when no protocol value can be
    /// produced. The caller closes rather than fabricating a response.
    fn handle(
        &self,
        context: ParticipantConnectionContext,
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
}

impl InstalledParticipantService {
    /// Pairs a semantic test handler, its declared durable store, and the raw
    /// configured participant wire-frame limit.
    ///
    /// # Errors
    ///
    /// Returns the shared codec error when the configured limit is smaller than
    /// the protocol's minimum complete frame.
    #[cfg(test)]
    pub(crate) fn new(
        handler: Arc<dyn ParticipantSemanticHandler>,
        durable_store: Arc<dyn DurableStore>,
        configured_wf: u64,
    ) -> Result<Self, CodecError> {
        Ok(Self {
            handler,
            durable_store,
            frame_limit: normalize_configured_frame_limit(configured_wf)?,
        })
    }

    /// Returns the installed semantic handler.
    #[must_use]
    pub(crate) fn handler(&self) -> &dyn ParticipantSemanticHandler {
        self.handler.as_ref()
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
        ParticipantIngress::Request(request) => match handler.handle(context, request) {
            Ok(value) => (value, false),
            Err(error) => {
                return ParticipantDispatch::Fatal(ParticipantDispatchError::Semantic(error));
            }
        },
    };
    match encode_server_value(value) {
        Ok(frame) if close_after_response => ParticipantDispatch::RespondThenClose(frame),
        Ok(frame) => ParticipantDispatch::Respond(frame),
        Err(error) => ParticipantDispatch::Fatal(ParticipantDispatchError::Encode(error)),
    }
}
