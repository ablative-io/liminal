//! Leave and ordinary record-admission arms.
//!
//! Both operations' COMMIT paths consume the conversation's validated
//! claim-frontier authority, which this binding does not yet acquire (the A1
//! whole-conversation restore capsule for a live conversation is the next
//! unit). Leave therefore classifies every refusal arm through the shared
//! lookup and fails closed only on an authorized Leave; record admission
//! fails closed entirely, because even its refusal classification runs
//! inside the frontier-consuming total selector. Every failure is a typed
//! diagnostic — no silent narrowing, no hand-built outcome.

use liminal_protocol::lifecycle::{
    LeaveLookupResult, LeaveSecretProof, PresentedIdentity, lookup_leave,
};
use liminal_protocol::wire::{
    BindingEpoch, ConnectionIncarnation, LeaveEnvelope, LeaveRequest, LeaveResponse,
    RecordAdmission, ServerValue,
};

use super::facts::{self, Digest};
use super::ops_bind::OperationFacts;
use super::state::{ConversationAuthority, DurableAppend, StateError};

impl ConversationAuthority {
    /// Applies one terminal Leave request.
    ///
    /// Refusal arms are total through the shared lookup; an authorized Leave
    /// fails closed until the claim-frontier acquisition lands.
    pub(super) fn apply_leave(
        &mut self,
        request: &LeaveRequest,
        receiving_incarnation: ConnectionIncarnation,
        appender: &dyn DurableAppend,
    ) -> Result<ServerValue, StateError> {
        self.ensure_genesis(appender)?;
        let envelope = leave_envelope(request);
        let receiving_epoch =
            BindingEpoch::new(receiving_incarnation, request.capability_generation);
        let Some(slot) = self.slots.get(&request.participant_id) else {
            return Ok(LeaveResponse::participant_unknown(envelope).into_server_value());
        };
        let secret_proof = if facts::constant_time_eq(
            &slot.attach_secret.into_bytes(),
            &request.attach_secret.into_bytes(),
        ) {
            LeaveSecretProof::Verified
        } else {
            LeaveSecretProof::Mismatch
        };
        let lookup = lookup_leave(
            PresentedIdentity::<Digest, Digest, Digest>::Live(&slot.member),
            &slot.binding,
            Some(receiving_epoch),
            request,
            secret_proof,
        );
        let response = match lookup {
            LeaveLookupResult::StaleAuthority(value) => LeaveResponse::stale_authority(value),
            LeaveLookupResult::ParticipantUnknown(_) => {
                LeaveResponse::participant_unknown(envelope)
            }
            LeaveLookupResult::NoBinding(_) => LeaveResponse::no_binding(envelope),
            LeaveLookupResult::LeaveCommitted(_)
            | LeaveLookupResult::AttemptTokenBodyConflict(_)
            | LeaveLookupResult::Retired(_) => {
                return Err(StateError::invariant(
                    "tombstone leave arm observed in a binding that mints no tombstones",
                ));
            }
            LeaveLookupResult::AuthorizedBound { .. }
            | LeaveLookupResult::AuthorizedDetached { .. } => {
                return Err(StateError::invariant(
                    "authorized leave requires the claim-frontier authority; the A1 frontier \
                     acquisition is not wired for leave in this binding yet",
                ));
            }
        };
        Ok(response.into_server_value())
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

/// Builds the echo envelope of one Leave request.
const fn leave_envelope(request: &LeaveRequest) -> LeaveEnvelope {
    LeaveEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        leave_attempt_token: request.leave_attempt_token,
    }
}
