//! Leave and ordinary record-admission arms.
//!
//! Both operations consume the conversation's validated claim-frontier
//! authority. This binding acquires it through the A1 whole-conversation
//! cold restore; until that acquisition is wired the arms fail closed with a
//! typed diagnostic — no silent narrowing, no hand-built outcome.

use liminal_protocol::wire::{ConnectionIncarnation, LeaveRequest, RecordAdmission, ServerValue};

use super::ops_bind::OperationFacts;
use super::state::{ConversationAuthority, DurableAppend, StateError};

impl ConversationAuthority {
    /// Applies one terminal Leave request.
    pub(super) fn apply_leave(
        &mut self,
        request: &LeaveRequest,
        receiving_incarnation: ConnectionIncarnation,
        appender: &dyn DurableAppend,
    ) -> Result<ServerValue, StateError> {
        self.ensure_genesis(appender)?;
        let _ = (request, receiving_incarnation);
        Err(StateError::invariant(
            "leave commit requires the claim-frontier authority; the A1 frontier acquisition \
             is not wired for leave in this binding yet",
        ))
    }

    /// Applies one ordinary record admission.
    pub(super) fn apply_record_admission(
        &mut self,
        request: &RecordAdmission,
        operation_facts: &OperationFacts,
        appender: &dyn DurableAppend,
    ) -> Result<ServerValue, StateError> {
        self.ensure_genesis(appender)?;
        let _ = (request, operation_facts);
        Err(StateError::invariant(
            "record admission requires the claim-frontier authority; the A1 frontier \
             acquisition is not wired for records in this binding yet",
        ))
    }
}
