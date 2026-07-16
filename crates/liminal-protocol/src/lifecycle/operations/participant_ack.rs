use crate::wire::{
    AckCommitted, AckGap, AckNoOp, AckRegression, BindingEpoch, ConversationId, DeliverySeq,
    Generation, ParticipantAck, ParticipantAckEnvelope, ParticipantId, ServerValue,
};

use super::super::{
    BindingRequiredLookupResult, BindingState, LiveMember, ParticipantBindingRequest,
    PresentedIdentity, lookup_binding_required,
    membership::{LiveMemberCursorUpdate, LiveMemberCursorUpdateError},
};

/// Atomic zero-debt participant-ack commit.
///
/// The cursor update is intentionally opaque. Only [`Self::apply_to`] can
/// validate and apply it to a [`LiveMember`], while [`Self::outcome`] exposes
/// the crate-owned wire success for persistence and response encoding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParticipantAckCommit {
    outcome: AckCommitted,
    cursor_update: LiveMemberCursorUpdate,
}

impl ParticipantAckCommit {
    /// Borrows the exact committed wire outcome.
    #[must_use]
    pub const fn outcome(&self) -> &AckCommitted {
        &self.outcome
    }

    /// Applies this commit to either its exact old cursor or its already-written
    /// resulting cursor.
    ///
    /// Replaying after a crash is idempotent: the old prestate advances once,
    /// while the exact new prestate returns the same [`AckCommitted`] without a
    /// second mutation.
    ///
    /// # Errors
    ///
    /// Returns [`ParticipantAckCommitError`] if the supplied member differs in
    /// conversation, participant, generation, or cursor prestate.
    pub fn apply_to<F>(
        self,
        member: &mut LiveMember<F>,
    ) -> Result<AckCommitted, ParticipantAckCommitError> {
        member
            .apply_cursor_update(self.cursor_update)
            .map_err(ParticipantAckCommitError::from_member_error)?;
        Ok(self.outcome)
    }
}

/// Failure while applying an already-selected participant-ack commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantAckCommitError {
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

impl ParticipantAckCommitError {
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

/// Total zero-debt participant-ack decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParticipantAckDecision {
    /// Exact lookup, regression, no-op, or gap response; membership is unchanged.
    Respond(ServerValue),
    /// Exact committed response paired with its sole cursor update authority.
    Commit(ParticipantAckCommit),
}

/// Applies the complete zero-debt participant-ack selector.
///
/// Existing shared lookup enforces retired, unknown, stale-authority, and
/// exact-binding precedence, including the receiving [`BindingEpoch`]. An
/// authorized request then selects `AckRegression`, `AckNoOp`, `AckGap`, or
/// `AckCommitted` in that exact relation order. Observer, order, sequence, and
/// closure checks are absent because none is selectable for zero-debt ack.
#[must_use]
pub fn apply_participant_ack<EF, V, LF>(
    presented_identity: PresentedIdentity<'_, EF, V, LF>,
    binding: &BindingState,
    receiving_binding_epoch: BindingEpoch,
    request: &ParticipantAck,
    contiguously_available_through: DeliverySeq,
) -> ParticipantAckDecision {
    let lookup_request = ParticipantBindingRequest::ParticipantAck(request.clone());
    let member = match lookup_binding_required(
        presented_identity,
        binding,
        Some(receiving_binding_epoch),
        &lookup_request,
    ) {
        BindingRequiredLookupResult::Retired(outcome) => {
            return ParticipantAckDecision::Respond(ServerValue::Retired(outcome));
        }
        BindingRequiredLookupResult::ParticipantUnknown(outcome) => {
            return ParticipantAckDecision::Respond(ServerValue::ParticipantUnknown(outcome));
        }
        BindingRequiredLookupResult::StaleAuthority(outcome) => {
            return ParticipantAckDecision::Respond(ServerValue::StaleAuthority(outcome));
        }
        BindingRequiredLookupResult::NoBinding(outcome) => {
            return ParticipantAckDecision::Respond(ServerValue::NoBinding(outcome));
        }
        BindingRequiredLookupResult::Authorized { member, .. } => member,
    };

    let current_cursor = member.cursor();
    if request.through_seq < current_cursor
        && let Some(outcome) = AckRegression::new(ack_envelope(request), current_cursor)
    {
        return ParticipantAckDecision::Respond(ServerValue::AckRegression(outcome));
    }
    if request.through_seq == current_cursor {
        return ParticipantAckDecision::Respond(ServerValue::AckNoOp(AckNoOp::participant_ack(
            ack_envelope(request),
        )));
    }
    if request.through_seq > contiguously_available_through
        && let Some(outcome) = AckGap::new(ack_envelope(request), current_cursor)
    {
        return ParticipantAckDecision::Respond(ServerValue::AckGap(outcome));
    }

    let outcome = AckCommitted::new(ack_envelope(request));
    ParticipantAckDecision::Commit(ParticipantAckCommit {
        cursor_update: LiveMemberCursorUpdate::new(
            request.conversation_id,
            request.participant_id,
            request.capability_generation,
            current_cursor,
            request.through_seq,
        ),
        outcome,
    })
}

const fn ack_envelope(request: &ParticipantAck) -> ParticipantAckEnvelope {
    ParticipantAckEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        through_seq: request.through_seq,
    }
}
