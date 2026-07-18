use alloc::boxed::Box;

use crate::wire::{
    AckCommitted, BindingEpoch, ConversationId, DeliverySeq, Generation, ParticipantAck,
    ParticipantAckResponse, ParticipantId,
};

use super::super::{
    BindingRequiredLookupResult, BindingState, CumulativeAckAuthorizationError,
    CumulativeAckOutcome, LiveMember, NonzeroDebtCursorEpisode, ObserverProgressProjection,
    ParticipantBindingRequest, PresentedIdentity, RecipientAckObligations, lookup_binding_required,
    membership::{LiveMemberCursorUpdate, LiveMemberCursorUpdateError},
};

/// Durable episode position supplied while applying an aggregate ack commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NonzeroAckEpisodePosition {
    /// Exact episode from which the commit was selected.
    Before,
    /// Exact episode already produced by the commit.
    Resulting,
}

/// Atomic nonzero-debt participant-ack commit.
///
/// The commit retains both exact episode prestates plus the crate-private
/// membership cursor authority. [`Self::apply_to`] validates the aggregate as
/// one pair before changing either value, so consuming storage can persist the
/// resulting member and episode in one transaction and replay that transaction
/// from either wholly-old or wholly-resulting durable state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NonzeroParticipantAckCommit {
    outcome: AckCommitted,
    from_cursor: DeliverySeq,
    before_episode: NonzeroDebtCursorEpisode,
    resulting_episode: NonzeroDebtCursorEpisode,
    cursor_update: LiveMemberCursorUpdate,
}

impl NonzeroParticipantAckCommit {
    /// Borrows the exact committed wire outcome.
    #[must_use]
    pub const fn outcome(&self) -> &AckCommitted {
        &self.outcome
    }

    /// Projects the exact committed ack boundary into hard observer progress.
    #[must_use]
    pub const fn observer_progress_projection(&self) -> ObserverProgressProjection {
        let request = self.outcome.request();
        ObserverProgressProjection::new(request.conversation_id, request.through_seq)
    }

    /// Borrows the exact episode that must be persisted with the member cursor.
    #[must_use]
    pub const fn resulting_episode(&self) -> &NonzeroDebtCursorEpisode {
        &self.resulting_episode
    }

    /// Applies this aggregate commit from an exact wholly-old or
    /// wholly-resulting durable prestate.
    ///
    /// The old pair advances once. Replaying against the exact resulting pair
    /// returns the identical [`AckCommitted`] without another change. A split
    /// member/episode pair is rejected rather than silently repaired.
    ///
    /// # Errors
    ///
    /// Returns [`NonzeroParticipantAckCommitError`] for a mismatched member,
    /// unrelated episode, or split aggregate prestate. Neither argument is
    /// changed on error.
    pub fn apply_to<F>(
        self,
        member: &mut LiveMember<F>,
        episode: &mut NonzeroDebtCursorEpisode,
    ) -> Result<AckCommitted, NonzeroParticipantAckCommitError> {
        let request = self.outcome.request();
        if member.conversation_id() != request.conversation_id {
            return Err(NonzeroParticipantAckCommitError::Conversation {
                expected: request.conversation_id,
                actual: member.conversation_id(),
            });
        }
        if member.participant_id() != request.participant_id {
            return Err(NonzeroParticipantAckCommitError::Participant {
                expected: request.participant_id,
                actual: member.participant_id(),
            });
        }
        if member.generation() != request.capability_generation {
            return Err(NonzeroParticipantAckCommitError::Generation {
                expected: request.capability_generation,
                actual: member.generation(),
            });
        }

        let (position, expected_cursor) = if *episode == self.before_episode {
            (NonzeroAckEpisodePosition::Before, self.from_cursor)
        } else if *episode == self.resulting_episode {
            (NonzeroAckEpisodePosition::Resulting, request.through_seq)
        } else {
            return Err(NonzeroParticipantAckCommitError::EpisodePrestate);
        };
        if member.cursor() != expected_cursor {
            return Err(NonzeroParticipantAckCommitError::AggregateCursorPrestate {
                episode_position: position,
                expected_cursor,
                actual_cursor: member.cursor(),
            });
        }

        member
            .apply_cursor_update(self.cursor_update)
            .map_err(NonzeroParticipantAckCommitError::from_member_error)?;
        if position == NonzeroAckEpisodePosition::Before {
            *episode = self.resulting_episode;
        }
        Ok(self.outcome)
    }
}

/// Failure while applying an already-selected nonzero-debt ack commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NonzeroParticipantAckCommitError {
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
    /// Supplied episode is neither the exact old nor exact resulting state.
    EpisodePrestate,
    /// Member cursor disagrees with the supplied old/resulting episode state.
    AggregateCursorPrestate {
        /// Which exact episode state was supplied.
        episode_position: NonzeroAckEpisodePosition,
        /// Cursor required by that episode position.
        expected_cursor: DeliverySeq,
        /// Cursor carried by the supplied member.
        actual_cursor: DeliverySeq,
    },
    /// A malformed internal update did not strictly advance its source cursor.
    NonAdvancing {
        /// Cursor from which the update was selected.
        from_cursor: DeliverySeq,
        /// Proposed resulting cursor.
        resulting_cursor: DeliverySeq,
    },
    /// The opaque member update disagreed with the already-validated pair.
    CursorPrestate {
        /// Cursor from which the commit was selected.
        expected_from_cursor: DeliverySeq,
        /// Cursor produced by the commit.
        resulting_cursor: DeliverySeq,
        /// Cursor carried by the supplied member.
        actual_cursor: DeliverySeq,
    },
}

impl NonzeroParticipantAckCommitError {
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

/// Durable aggregate mismatch found after common request authority succeeded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NonzeroParticipantAckInvariantError {
    /// Episode belongs to another conversation.
    Conversation {
        /// Conversation owned by the selected member.
        member: ConversationId,
        /// Conversation owned by the episode.
        episode: ConversationId,
    },
    /// Episode has no entry for the selected permanent participant index.
    ParticipantMissing {
        /// Selected participant identifier/index.
        participant_id: ParticipantId,
    },
    /// Episode entry carries another credential generation.
    Generation {
        /// Current member generation.
        member: Generation,
        /// Generation carried by the episode binding epoch.
        episode: Generation,
    },
    /// Episode entry cursor differs from durable membership.
    Cursor {
        /// Durable member cursor.
        member: DeliverySeq,
        /// Episode entry cursor.
        episode: DeliverySeq,
    },
    /// Episode entry carries another active binding epoch.
    BindingEpoch {
        /// Exact binding authorized by common lookup.
        active: BindingEpoch,
        /// Binding epoch carried by the episode entry.
        episode: BindingEpoch,
    },
    /// Existing episode transition rejected an otherwise-validated aggregate.
    EpisodeTransition(CumulativeAckAuthorizationError),
}

/// Total nonzero-debt participant-ack decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NonzeroParticipantAckDecision {
    /// Authority, regression, no-op, or gap response; aggregate is unchanged.
    Respond(ParticipantAckResponse),
    /// Durable aggregate state is internally inconsistent; nothing changed.
    Invariant(NonzeroParticipantAckInvariantError),
    /// Exact committed response paired with the sole aggregate mutation.
    Commit(Box<NonzeroParticipantAckCommit>),
}

/// Applies the complete nonzero-debt normal-ack selector.
///
/// Shared participant lookup runs first. Only its authorized arm may inspect
/// the episode. The selected member and episode must then agree, in order, on
/// conversation, participant, generation, current cursor, and exact active
/// binding epoch. The existing episode transition remains the sole owner of
/// gap selection, participant-scoped cursor facts, and floor computation.
/// Refusals and invariant errors borrow the prestate and cannot mutate it.
#[must_use]
pub fn apply_nonzero_participant_ack<EF, V, LF>(
    presented_identity: PresentedIdentity<'_, EF, V, LF>,
    binding: &BindingState,
    receiving_binding_epoch: BindingEpoch,
    request: &ParticipantAck,
    contiguously_available_through: DeliverySeq,
    episode: &NonzeroDebtCursorEpisode,
) -> NonzeroParticipantAckDecision {
    apply_nonzero_participant_ack_by_availability(
        presented_identity,
        binding,
        receiving_binding_epoch,
        request,
        &AckAvailability::Contiguous(contiguously_available_through),
        episode,
    )
}

/// Applies the nonzero-debt selector with durable recipient obligations.
///
/// Shared lookup and aggregate validation retain their existing precedence. The
/// episode then requires a forward endpoint to occur in the sealed recipient
/// obligation index; internal conversation sequence gaps remain legal.
#[must_use]
pub fn apply_nonzero_participant_ack_with_obligations<EF, V, LF>(
    presented_identity: PresentedIdentity<'_, EF, V, LF>,
    binding: &BindingState,
    receiving_binding_epoch: BindingEpoch,
    request: &ParticipantAck,
    obligations: &RecipientAckObligations,
    episode: &NonzeroDebtCursorEpisode,
) -> NonzeroParticipantAckDecision {
    apply_nonzero_participant_ack_by_availability(
        presented_identity,
        binding,
        receiving_binding_epoch,
        request,
        &AckAvailability::Obligations(obligations),
        episode,
    )
}

enum AckAvailability<'a> {
    Contiguous(DeliverySeq),
    Obligations(&'a RecipientAckObligations),
}

fn apply_nonzero_participant_ack_by_availability<EF, V, LF>(
    presented_identity: PresentedIdentity<'_, EF, V, LF>,
    binding: &BindingState,
    receiving_binding_epoch: BindingEpoch,
    request: &ParticipantAck,
    availability: &AckAvailability<'_>,
    episode: &NonzeroDebtCursorEpisode,
) -> NonzeroParticipantAckDecision {
    let lookup_request = ParticipantBindingRequest::ParticipantAck(request.clone());
    let (member, active_binding) = match lookup_binding_required(
        presented_identity,
        binding,
        Some(receiving_binding_epoch),
        &lookup_request,
    ) {
        BindingRequiredLookupResult::Retired(outcome) => {
            return NonzeroParticipantAckDecision::Respond(ParticipantAckResponse::from_retired(
                outcome,
            ));
        }
        BindingRequiredLookupResult::ParticipantUnknown(outcome) => {
            return NonzeroParticipantAckDecision::Respond(
                ParticipantAckResponse::from_participant_unknown(outcome),
            );
        }
        BindingRequiredLookupResult::StaleAuthority(outcome) => {
            return NonzeroParticipantAckDecision::Respond(
                ParticipantAckResponse::from_stale_authority(outcome),
            );
        }
        BindingRequiredLookupResult::NoBinding(outcome) => {
            return NonzeroParticipantAckDecision::Respond(
                ParticipantAckResponse::from_no_binding(outcome),
            );
        }
        BindingRequiredLookupResult::Authorized { member, binding } => (member, binding),
    };

    if let Err(error) = validate_aggregate(member, active_binding.binding_epoch, episode) {
        return NonzeroParticipantAckDecision::Invariant(error);
    }

    let mut resulting_episode = episode.clone();
    let selected = match availability {
        AckAvailability::Contiguous(available_through) => resulting_episode.acknowledge(
            member.participant_id(),
            active_binding.binding_epoch,
            request,
            *available_through,
        ),
        AckAvailability::Obligations(obligations) => resulting_episode
            .acknowledge_with_obligations(
                member.participant_id(),
                active_binding.binding_epoch,
                request,
                obligations,
            ),
    };
    let outcome = match selected {
        Ok(outcome) => outcome,
        Err(error) => {
            return NonzeroParticipantAckDecision::Invariant(
                NonzeroParticipantAckInvariantError::EpisodeTransition(error),
            );
        }
    };
    match outcome {
        CumulativeAckOutcome::Committed(outcome) => {
            NonzeroParticipantAckDecision::Commit(Box::new(NonzeroParticipantAckCommit {
                cursor_update: LiveMemberCursorUpdate::new(
                    request.conversation_id,
                    request.participant_id,
                    request.capability_generation,
                    member.cursor(),
                    request.through_seq,
                ),
                outcome,
                from_cursor: member.cursor(),
                before_episode: episode.clone(),
                resulting_episode,
            }))
        }
        CumulativeAckOutcome::NoOp(outcome) => {
            NonzeroParticipantAckDecision::Respond(ParticipantAckResponse::from_ack_no_op(outcome))
        }
        CumulativeAckOutcome::Gap(outcome) => {
            NonzeroParticipantAckDecision::Respond(ParticipantAckResponse::ack_gap(outcome))
        }
        CumulativeAckOutcome::Regression(outcome) => {
            NonzeroParticipantAckDecision::Respond(ParticipantAckResponse::ack_regression(outcome))
        }
    }
}

fn validate_aggregate<F>(
    member: &LiveMember<F>,
    active_binding_epoch: BindingEpoch,
    episode: &NonzeroDebtCursorEpisode,
) -> Result<(), NonzeroParticipantAckInvariantError> {
    if member.conversation_id() != episode.conversation_id() {
        return Err(NonzeroParticipantAckInvariantError::Conversation {
            member: member.conversation_id(),
            episode: episode.conversation_id(),
        });
    }
    let Some(episode_participant) = episode.participant(member.participant_id()) else {
        return Err(NonzeroParticipantAckInvariantError::ParticipantMissing {
            participant_id: member.participant_id(),
        });
    };
    let episode_generation = episode_participant
        .active_binding_epoch()
        .capability_generation;
    if episode_generation != member.generation() {
        return Err(NonzeroParticipantAckInvariantError::Generation {
            member: member.generation(),
            episode: episode_generation,
        });
    }
    if episode_participant.cursor() != member.cursor() {
        return Err(NonzeroParticipantAckInvariantError::Cursor {
            member: member.cursor(),
            episode: episode_participant.cursor(),
        });
    }
    if episode_participant.active_binding_epoch() != active_binding_epoch {
        return Err(NonzeroParticipantAckInvariantError::BindingEpoch {
            active: active_binding_epoch,
            episode: episode_participant.active_binding_epoch(),
        });
    }
    Ok(())
}
