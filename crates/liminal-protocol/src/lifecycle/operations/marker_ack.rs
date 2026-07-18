use crate::wire::{
    BindingEpoch, ConversationId, DeliverySeq, Generation, MarkerAck, MarkerAckCommitted,
    MarkerAckEnvelope, MarkerAckResponse, ParticipantId,
};

use super::{
    super::{
        BindingRequiredLookupResult, BindingState, LiveMember, ObserverProgressProjection,
        ParticipantBindingRequest, PresentedIdentity, lookup_binding_required,
        membership::{LiveMemberCursorUpdate, LiveMemberCursorUpdateError},
    },
    marker_proof::{
        MarkerProofDecision, MarkerProofInput, MarkerProofPermit, MarkerProofState,
        select_marker_proof,
    },
};

/// Atomic zero-debt marker-ack commit.
///
/// The retained marker permit proves delivery to the exact authoritative
/// binding epoch. The cursor update is opaque and can be applied only through
/// [`Self::apply_to`]. Nonzero-debt edge completion is deliberately outside
/// this operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkerAckCommit {
    outcome: MarkerAckCommitted,
    proof: MarkerProofPermit,
    cursor_update: LiveMemberCursorUpdate,
}

impl MarkerAckCommit {
    /// Borrows the exact committed wire outcome.
    #[must_use]
    pub const fn outcome(&self) -> &MarkerAckCommitted {
        &self.outcome
    }

    /// Projects the exact committed marker boundary into hard observer progress.
    #[must_use]
    pub const fn observer_progress_projection(&self) -> ObserverProgressProjection {
        let request = self.outcome.request();
        ObserverProgressProjection::new(request.conversation_id, request.marker_delivery_seq)
    }

    /// Returns the exact canonical request selected into this commit.
    #[must_use]
    pub const fn canonical_request(&self) -> MarkerAck {
        let request = self.outcome.request();
        MarkerAck {
            conversation_id: request.conversation_id,
            participant_id: request.participant_id,
            capability_generation: request.capability_generation,
            marker_delivery_seq: request.marker_delivery_seq,
        }
    }

    /// Returns the receiving binding epoch authorized by shared lookup.
    #[must_use]
    pub const fn receiving_binding_epoch(&self) -> BindingEpoch {
        self.proof.proof_binding_epoch()
    }

    /// Returns the exact marker sequence proven offered to the binding.
    #[must_use]
    pub const fn offered_marker_delivery_seq(&self) -> DeliverySeq {
        self.proof.expected_marker_delivery_seq()
    }

    /// Returns the binding epoch on which the marker was proven offered.
    #[must_use]
    pub const fn delivered_binding_epoch(&self) -> BindingEpoch {
        self.proof.proof_binding_epoch()
    }

    /// Returns the durable cursor prestate checked by this transition.
    #[must_use]
    pub const fn from_cursor(&self) -> DeliverySeq {
        self.cursor_update.previous_cursor()
    }

    /// Returns the exact post-transition cursor for replay audit.
    #[must_use]
    pub const fn resulting_cursor(&self) -> DeliverySeq {
        self.cursor_update.resulting_cursor()
    }

    /// Borrows the exact delivered-marker authority retained by this commit.
    #[must_use]
    pub const fn proof(&self) -> &MarkerProofPermit {
        &self.proof
    }

    /// Applies this commit to either its exact old cursor or its already-written
    /// resulting cursor.
    ///
    /// Replaying after a crash is idempotent: the old prestate advances once,
    /// while the exact new prestate returns the same [`MarkerAckCommitted`]
    /// without another mutation.
    ///
    /// # Errors
    ///
    /// Returns [`MarkerAckCommitError`] if the supplied member differs in
    /// conversation, participant, generation, or cursor prestate.
    pub fn apply_to<F>(
        self,
        member: &mut LiveMember<F>,
    ) -> Result<MarkerAckCommitted, MarkerAckCommitError> {
        member
            .apply_cursor_update(self.cursor_update)
            .map_err(MarkerAckCommitError::from_member_error)?;
        Ok(self.outcome)
    }
}

/// Failure while applying an already-selected marker-ack commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarkerAckCommitError {
    /// Commit belongs to another conversation.
    Conversation {
        /// Conversation captured by the commit.
        expected: ConversationId,
        /// Conversation carried by the supplied member.
        actual: ConversationId,
    },
    /// Commit belongs to another participant.
    Participant {
        /// Participant captured by the commit.
        expected: ParticipantId,
        /// Participant carried by the supplied member.
        actual: ParticipantId,
    },
    /// Commit belongs to another credential generation.
    Generation {
        /// Generation captured by the commit.
        expected: Generation,
        /// Generation carried by the supplied member.
        actual: Generation,
    },
    /// A malformed internal update did not strictly advance its source cursor.
    NonAdvancing {
        /// Cursor from which the update was selected.
        from_cursor: DeliverySeq,
        /// Proposed resulting cursor.
        resulting_cursor: DeliverySeq,
    },
    /// Supplied member is neither the exact old nor exact committed prestate.
    CursorPrestate {
        /// Cursor from which the commit was selected.
        expected_from_cursor: DeliverySeq,
        /// Cursor produced by the commit.
        resulting_cursor: DeliverySeq,
        /// Cursor carried by the supplied member.
        actual_cursor: DeliverySeq,
    },
}

impl MarkerAckCommitError {
    const fn from_member_error(error: LiveMemberCursorUpdateError) -> Self {
        match error {
            LiveMemberCursorUpdateError::Conversation { expected, actual } => {
                Self::Conversation { expected, actual }
            }
            LiveMemberCursorUpdateError::Participant { expected, actual } => {
                Self::Participant { expected, actual }
            }
            LiveMemberCursorUpdateError::Generation { expected, actual } => {
                Self::Generation { expected, actual }
            }
            LiveMemberCursorUpdateError::NonAdvancing {
                from_cursor,
                resulting_cursor,
            } => Self::NonAdvancing {
                from_cursor,
                resulting_cursor,
            },
            LiveMemberCursorUpdateError::CursorPrestate {
                expected_from_cursor,
                resulting_cursor,
                actual_cursor,
            } => Self::CursorPrestate {
                expected_from_cursor,
                resulting_cursor,
                actual_cursor,
            },
        }
    }
}

/// Total zero-debt marker-ack decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MarkerAckDecision {
    /// Authority or marker-proof response; membership is unchanged.
    Respond(MarkerAckResponse),
    /// Exact committed response paired with its proof and sole cursor update.
    Commit(MarkerAckCommit),
}

/// Applies the complete zero-debt marker-ack selector.
///
/// Existing shared lookup enforces retired, unknown, stale-authority, and
/// exact-binding precedence. Only authorized requests enter marker proof. The
/// proof snapshot's cursor and proof epoch are derived here from the verified
/// member and binding; callers provide only the remaining durable marker
/// facts. An exact accepted marker at the member cursor returns `AckNoOp`.
/// Every other refusal is nonmutating, while an exact delivered marker returns
/// a replay-safe [`MarkerAckCommit`].
#[must_use]
pub fn apply_marker_ack<EF, V, LF>(
    presented_identity: PresentedIdentity<'_, EF, V, LF>,
    binding: &BindingState,
    receiving_binding_epoch: BindingEpoch,
    request: &MarkerAck,
    marker_state: &MarkerProofState,
) -> MarkerAckDecision {
    let lookup_request = ParticipantBindingRequest::MarkerAck(request.clone());
    let (member, active_binding) = match lookup_binding_required(
        presented_identity,
        binding,
        Some(receiving_binding_epoch),
        &lookup_request,
    ) {
        BindingRequiredLookupResult::Retired(outcome) => {
            return MarkerAckDecision::Respond(MarkerAckResponse::from_retired(outcome));
        }
        BindingRequiredLookupResult::ParticipantUnknown(outcome) => {
            return MarkerAckDecision::Respond(MarkerAckResponse::from_participant_unknown(
                outcome,
            ));
        }
        BindingRequiredLookupResult::StaleAuthority(outcome) => {
            return MarkerAckDecision::Respond(MarkerAckResponse::from_stale_authority(outcome));
        }
        BindingRequiredLookupResult::NoBinding(outcome) => {
            return MarkerAckDecision::Respond(MarkerAckResponse::from_no_binding(outcome));
        }
        BindingRequiredLookupResult::Authorized { member, binding } => (member, binding),
    };

    let exact_state = MarkerProofState::new(
        member.cursor(),
        marker_state.accepted_marker_at_cursor(),
        marker_state.expected_marker_delivery_seq(),
        active_binding.binding_epoch,
        marker_state.delivered_to_proof_epoch(),
    );
    match select_marker_proof(&exact_state, MarkerProofInput::marker_ack(request)) {
        MarkerProofDecision::AckNoOp(outcome) => {
            MarkerAckDecision::Respond(MarkerAckResponse::from_ack_no_op(outcome))
        }
        MarkerProofDecision::MarkerMismatch(outcome) => {
            MarkerAckDecision::Respond(MarkerAckResponse::from_marker_mismatch(outcome))
        }
        MarkerProofDecision::MarkerNotDelivered(outcome) => {
            MarkerAckDecision::Respond(MarkerAckResponse::from_marker_not_delivered(outcome))
        }
        MarkerProofDecision::Permit(proof) => {
            let envelope = marker_ack_envelope(request);
            MarkerAckDecision::Commit(MarkerAckCommit {
                outcome: MarkerAckCommitted::new(envelope),
                proof,
                cursor_update: LiveMemberCursorUpdate::new(
                    request.conversation_id,
                    request.participant_id,
                    request.capability_generation,
                    member.cursor(),
                    request.marker_delivery_seq,
                ),
            })
        }
    }
}

const fn marker_ack_envelope(request: &MarkerAck) -> MarkerAckEnvelope {
    MarkerAckEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        marker_delivery_seq: request.marker_delivery_seq,
    }
}
