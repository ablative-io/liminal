use std::sync::Arc;

use super::ActorCore;
use crate::conversation::types::{ConversationHandleBackend, ConversationState, ParticipantPid};
use crate::envelope::Envelope;
use crate::error::LiminalError;

/// Adapter that drives a [`ConversationHandle`](crate::conversation::ConversationHandle)
/// against an [`ActorCore`] by delegating each command to the core's submission
/// path. Pure delegation: it owns no state beyond the shared core.
#[derive(Debug)]
pub(super) struct ActorBackend {
    pub(super) core: Arc<ActorCore>,
}

impl ConversationHandleBackend for ActorBackend {
    fn send(&self, message: Envelope) -> Result<(), LiminalError> {
        self.core.submit_send(message)
    }

    fn receive(&self) -> Result<Envelope, LiminalError> {
        self.core.submit_receive()
    }

    fn close(&self) -> Result<(), LiminalError> {
        self.core.submit_close()
    }

    fn query_state(&self) -> Result<ConversationState, LiminalError> {
        self.core.submit_query_state()
    }

    fn actor_pid(&self) -> Result<ParticipantPid, LiminalError> {
        self.core.ensure_running()
    }
}
