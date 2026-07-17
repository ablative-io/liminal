//! Leave and ordinary record-admission arms.
//!
//! Both operations' COMMIT paths consume the conversation's validated
//! claim-frontier authority, which this binding does not yet acquire: the
//! crate exposes no frontier transitions for the attach/detach/ack
//! operations this binding already commits, so a live [`ClaimFrontiers`]
//! value cannot be maintained across the conversation's history, and the
//! whole-conversation live-restore capsule is a separate protocol-crate
//! unit (see the dated amendment in `docs/design/LP-GAP-CLOSURE-GOAL.md`).
//!
//! Until that unit lands, both arms classify every frozen pre-commit stage
//! through crate selectors — lookup stages 2-5 and the stage-6
//! connection-conversation capacity gate — and fail closed ONLY on a fully
//! authorized commit, with a typed diagnostic. No lifecycle outcome is
//! hand-built and no refusal is silently narrowed.
//!
//! [`ClaimFrontiers`]: liminal_protocol::lifecycle::ClaimFrontiers

use liminal_protocol::lifecycle::{
    BindingState, LeaveLookupResult, LeaveSecretProof, PresentedIdentity,
    SemanticConnectionCapacityDecision, classify_record_admission_binding, lookup_leave,
};
use liminal_protocol::wire::{
    BindingEpoch, LeaveEnvelope, LeaveRequest, LeaveResponse, RecordAdmission,
    RecordAdmissionResponse,
};

use super::barrier::{ArmOutcome, OperationFacts};
use super::facts::{self, Digest};
use super::state::{ConversationAuthority, StateError};

impl ConversationAuthority {
    /// Applies one terminal Leave request.
    ///
    /// Refusal arms are total through the shared lookup and the stage-6
    /// capacity gate; an authorized Leave fails closed until the
    /// claim-frontier acquisition lands.
    pub(super) fn apply_leave(
        &self,
        request: &LeaveRequest,
        operation_facts: &OperationFacts,
    ) -> Result<ArmOutcome, StateError> {
        let envelope = leave_envelope(request);
        let receiving_epoch = BindingEpoch::new(
            operation_facts.receiving_incarnation,
            request.capability_generation,
        );
        // A missing slot is presented to the crate's lookup as an ABSENT
        // identity with a detached placeholder binding — the same pattern the
        // ack arms use — so participant-unknown classification has exactly one
        // owner (`lookup_leave`'s `PresentedIdentity::Absent` arm). No stored
        // secret exists for an absent slot, so the proof is `Mismatch`;
        // identity precedence classifies before any secret is consulted.
        let binding_detached = BindingState::Detached;
        let (identity, binding, secret_proof) = self.slots.get(&request.participant_id).map_or(
            (
                PresentedIdentity::<Digest, Digest, Digest>::Absent,
                &binding_detached,
                LeaveSecretProof::Mismatch,
            ),
            |slot| {
                let secret_proof = if facts::constant_time_eq(
                    &slot.attach_secret.into_bytes(),
                    &request.attach_secret.into_bytes(),
                ) {
                    LeaveSecretProof::Verified
                } else {
                    LeaveSecretProof::Mismatch
                };
                (
                    PresentedIdentity::<Digest, Digest, Digest>::Live(&slot.member),
                    &slot.binding,
                    secret_proof,
                )
            },
        );
        let lookup = lookup_leave(
            identity,
            binding,
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
                // Stage 6 (register row 5641) precedes the authorized commit,
                // so an untracked conversation over a full connection map
                // still receives its typed refusal here.
                if let SemanticConnectionCapacityDecision::Respond { limit } =
                    operation_facts.semantic_connection_capacity()
                {
                    return Ok(ArmOutcome::respond(
                        LeaveResponse::connection_conversation_capacity_exceeded(envelope, limit)
                            .into_server_value(),
                    ));
                }
                return Err(StateError::invariant(
                    "authorized leave requires the claim-frontier authority; the live \
                     claim-frontier acquisition is a separate protocol-crate unit (see the \
                     LP-GAP-CLOSURE amendment)",
                ));
            }
        };
        Ok(ArmOutcome::respond(response.into_server_value()))
    }

    /// Applies one ordinary record admission.
    ///
    /// Every frozen pre-commit stage this binding can honestly evaluate runs
    /// through crate selectors: the binding-required lookup rows (stages
    /// 2-5) through [`classify_record_admission_binding`] and the stage-6
    /// connection-conversation capacity gate. A fully authorized record
    /// fails closed BEFORE any durable touch — no genesis, no append, no
    /// registry residue — because the later stages run inside the crate's
    /// frontier-consuming total selector.
    pub(super) fn apply_record_admission(
        &self,
        request: &RecordAdmission,
        operation_facts: &OperationFacts,
    ) -> Result<ArmOutcome, StateError> {
        let receiving_epoch = BindingEpoch::new(
            operation_facts.receiving_incarnation,
            request.capability_generation,
        );
        let binding_detached = BindingState::Detached;
        let (identity, binding) = self.slots.get(&request.participant_id).map_or(
            (
                PresentedIdentity::<Digest, Digest, Digest>::Absent,
                &binding_detached,
            ),
            |slot| {
                (
                    PresentedIdentity::<Digest, Digest, Digest>::Live(&slot.member),
                    &slot.binding,
                )
            },
        );
        if let Some(response) =
            classify_record_admission_binding(identity, binding, receiving_epoch, request)
        {
            return Ok(ArmOutcome::respond(response.into_server_value()));
        }
        // Stage 6 (register row 5641) precedes every later admission stage.
        if let SemanticConnectionCapacityDecision::Respond { limit } =
            operation_facts.semantic_connection_capacity()
        {
            return Ok(ArmOutcome::respond(
                RecordAdmissionResponse::connection_conversation_capacity_exceeded(
                    record_envelope(request),
                    limit,
                )
                .into_server_value(),
            ));
        }
        Err(StateError::invariant(format!(
            "authorized record admission for conversation {} requires the claim-frontier \
             authority; the live claim-frontier acquisition is a separate protocol-crate unit \
             (see the LP-GAP-CLOSURE amendment)",
            self.conversation_id
        )))
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

/// Builds the echo envelope of one ordinary record admission.
const fn record_envelope(
    request: &RecordAdmission,
) -> liminal_protocol::wire::RecordAdmissionEnvelope {
    liminal_protocol::wire::RecordAdmissionEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
    }
}
