//! Move-coupled closure-debt authority and synchronous delivery seam.

use crate::wire::{AckCommitted, BindingEpoch, DeliverySeq, ParticipantId};

use super::{
    ClosureState, CursorEpisodeBuildError, FrontierBinding, LiveFrontierOwner, LiveMember,
    NonzeroDebtCursorEpisode, NonzeroParticipantAckCommit, NonzeroParticipantAckCommitError,
};

/// Failure to couple a validated live frontier to its complete debt episode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObligationDebtOwnerError {
    /// Validated frontier/observer inputs could not form one exact episode.
    Episode(CursorEpisodeBuildError),
    /// A nonzero commit did not apply to the exact coupled member/episode pair.
    NonzeroAck(NonzeroParticipantAckCommitError),
    /// A nonzero commit was attempted without one coherent Owed owner.
    NonzeroAckState,
}

/// Complete protocol-owned authority while closure debt is nonzero.
#[derive(Debug, PartialEq, Eq)]
pub struct CoupledObligationDebtOwner {
    frontier: LiveFrontierOwner,
    episode: NonzeroDebtCursorEpisode,
}

/// Sole protocol-owned live frontier and obligation-debt dispatch state.
#[derive(Debug, PartialEq, Eq)]
pub enum ObligationDebtDispatchState {
    /// Closure accounting is clear and no cursor episode exists.
    Clear(LiveFrontierOwner),
    /// Closure accounting is owed and one complete episode is move-coupled to it.
    Owed(CoupledObligationDebtOwner),
}

/// Move-only authority that prevents production from reinstalling a bare
/// frontier after beginning a coupled transition.
#[derive(Debug)]
pub struct ObligationDebtDispatchTransition {
    prior_episode: Option<NonzeroDebtCursorEpisode>,
}

impl ObligationDebtDispatchState {
    /// Couples one validated frontier according to its resulting closure state.
    ///
    /// # Errors
    ///
    /// Returns [`ObligationDebtOwnerError`] if an Owed frontier cannot form a
    /// complete episode from the same validated frontier and observer state.
    pub fn from_frontier(
        frontier: LiveFrontierOwner,
        observer_progress: DeliverySeq,
    ) -> Result<Self, ObligationDebtOwnerError> {
        match frontier.closure_accounting().state() {
            ClosureState::Clear => Ok(Self::Clear(frontier)),
            ClosureState::Owed { debt, .. } => {
                let episode = NonzeroDebtCursorEpisode::from_claim_frontiers(
                    frontier.frontiers(),
                    debt,
                    observer_progress,
                )
                .map_err(ObligationDebtOwnerError::Episode)?;
                Ok(Self::Owed(CoupledObligationDebtOwner { frontier, episode }))
            }
        }
    }

    /// Borrows the one live frontier owner.
    #[must_use]
    pub const fn frontier(&self) -> &LiveFrontierOwner {
        match self {
            Self::Clear(frontier) => frontier,
            Self::Owed(coupled) => &coupled.frontier,
        }
    }

    /// Borrows the episode only on the Owed branch.
    #[must_use]
    pub const fn episode(&self) -> Option<&NonzeroDebtCursorEpisode> {
        match self {
            Self::Clear(_) => None,
            Self::Owed(coupled) => Some(&coupled.episode),
        }
    }

    /// Returns one participant's binding-tagged protocol cursor from the live
    /// frontier on both owner variants.
    #[must_use]
    pub fn frontier_participant(
        &self,
        participant_id: ParticipantId,
    ) -> Option<(FrontierBinding, DeliverySeq)> {
        self.frontier()
            .frontiers()
            .active_identities()
            .participants()
            .iter()
            .find(|participant| participant.participant_index() == participant_id)
            .map(|participant| (participant.binding(), participant.cursor()))
    }

    /// Returns one participant's binding-tagged episode cursor on Owed.
    #[must_use]
    pub fn participant(
        &self,
        participant_id: ParticipantId,
    ) -> Option<(FrontierBinding, DeliverySeq)> {
        self.episode()?.participant_binding(participant_id)
    }

    /// Begins one consuming frontier transition while retaining the prior
    /// episode in an unforgeable completion token.
    #[must_use]
    pub fn begin_transition(self) -> (LiveFrontierOwner, ObligationDebtDispatchTransition) {
        match self {
            Self::Clear(frontier) => (
                frontier,
                ObligationDebtDispatchTransition {
                    prior_episode: None,
                },
            ),
            Self::Owed(coupled) => (
                coupled.frontier,
                ObligationDebtDispatchTransition {
                    prior_episode: Some(coupled.episode),
                },
            ),
        }
    }
}

impl ObligationDebtDispatchTransition {
    /// Completes a coupled transition from the operation's exact resulting
    /// frontier. Clear consumes any old episode; Owed reconciles all tagged
    /// participants and surviving facts into one complete episode.
    ///
    /// # Errors
    ///
    /// Returns [`ObligationDebtOwnerError`] if resulting frontier and observer
    /// state cannot form one coherent owner.
    pub fn complete(
        self,
        frontier: LiveFrontierOwner,
        observer_progress: DeliverySeq,
    ) -> Result<ObligationDebtDispatchState, ObligationDebtOwnerError> {
        match frontier.closure_accounting().state() {
            ClosureState::Clear => Ok(ObligationDebtDispatchState::Clear(frontier)),
            ClosureState::Owed { debt, .. } => {
                let episode = self
                    .prior_episode
                    .map_or_else(
                        || {
                            NonzeroDebtCursorEpisode::from_claim_frontiers(
                                frontier.frontiers(),
                                debt,
                                observer_progress,
                            )
                        },
                        |episode| {
                            episode.reconcile_claim_frontiers(
                                frontier.frontiers(),
                                debt,
                                observer_progress,
                            )
                        },
                    )
                    .map_err(ObligationDebtOwnerError::Episode)?;
                Ok(ObligationDebtDispatchState::Owed(
                    CoupledObligationDebtOwner { frontier, episode },
                ))
            }
        }
    }

    /// Completes one nonzero acknowledgement with the exact episode produced by
    /// its sealed commit instead of regenerating cursor facts from frontier
    /// scalars. The member, episode, and frontier therefore cross one protocol
    /// barrier as the same commit.
    ///
    /// # Errors
    ///
    /// Returns [`ObligationDebtOwnerError`] if the transition did not begin from
    /// Owed, the aggregate commit rejects the member/episode pair, or the exact
    /// resulting episode disagrees with the resulting frontier and closure debt.
    pub fn complete_nonzero_ack<F>(
        mut self,
        frontier: LiveFrontierOwner,
        commit: NonzeroParticipantAckCommit,
        member: &mut LiveMember<F>,
        observer_progress: DeliverySeq,
    ) -> Result<(ObligationDebtDispatchState, AckCommitted), ObligationDebtOwnerError> {
        let ClosureState::Owed { debt, .. } = frontier.closure_accounting().state() else {
            return Err(ObligationDebtOwnerError::NonzeroAckState);
        };
        let mut episode = self
            .prior_episode
            .take()
            .ok_or(ObligationDebtOwnerError::NonzeroAckState)?;
        let outcome = commit
            .apply_to(member, &mut episode)
            .map_err(ObligationDebtOwnerError::NonzeroAck)?;
        if episode.debt() != debt {
            return Err(ObligationDebtOwnerError::NonzeroAckState);
        }
        let reconciled = episode
            .clone()
            .reconcile_claim_frontiers(frontier.frontiers(), debt, observer_progress)
            .map_err(ObligationDebtOwnerError::Episode)?;
        if reconciled != episode {
            return Err(ObligationDebtOwnerError::NonzeroAckState);
        }
        Ok((
            ObligationDebtDispatchState::Owed(CoupledObligationDebtOwner { frontier, episode }),
            outcome,
        ))
    }
}

/// Internal scheduling deferral selected by the completed Leg 2 decision body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DebtDispatchDeferral {
    /// No durable recipient obligation follows the reconciled cursor.
    NoObligation,
    /// The participant has no exact current binding.
    NoCurrentBinding,
    /// The least testified endpoint is beyond the active debt high watermark.
    BeyondDebtHighWatermark,
}

/// Typed disagreement that must fail dispatch rather than fabricate a wire value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DebtDispatchInvariant {
    /// Coupled closure and episode authority disagree.
    CoupledState,
    /// Binding or cursor authority differs across the locked snapshot.
    ParticipantAuthority,
    /// Outbox selection did not advance beyond the reconciled cursor.
    OutboxSelection,
    /// The two nonzero acknowledgement selectors diverged.
    ScalarDivergence,
}

/// Total synchronous result at the participant delivery-decision seam.
#[derive(Debug, PartialEq, Eq)]
pub enum ObligationDebtDispatchDecision<T> {
    /// The selected durable obligation may proceed to publication construction.
    Permit(T),
    /// Current debt authority does not permit work yet.
    Defer(DebtDispatchDeferral),
    /// Locked authority is internally inconsistent.
    Invariant(DebtDispatchInvariant),
}

/// Executes the protocol-owned dispatch decision under the conversation owner.
///
/// The caller supplies the already-reconciled cursor and a one-shot read of the
/// least durable recipient obligation strictly after it. Clear authority permits
/// any advancing selection. Owed authority additionally requires exact agreement
/// between frontier and episode participant state, then treats endpoint testimony
/// and the episode high watermark as the complete eligibility domain: the
/// physical retention floor is deliberately not consulted.
pub fn decide_obligation_debt_dispatch<T>(
    state: &ObligationDebtDispatchState,
    participant_id: ParticipantId,
    binding_epoch: BindingEpoch,
    dispatch_after: DeliverySeq,
    select_next: impl FnOnce(ParticipantId, BindingEpoch, DeliverySeq) -> Option<(DeliverySeq, T)>,
) -> ObligationDebtDispatchDecision<T> {
    let frontier_participant = state
        .frontier()
        .frontiers()
        .active_identities()
        .participants()
        .iter()
        .find(|participant| participant.participant_index() == participant_id)
        .copied();
    let Some(frontier_participant) = frontier_participant else {
        return ObligationDebtDispatchDecision::Invariant(
            DebtDispatchInvariant::ParticipantAuthority,
        );
    };
    let FrontierBinding::Bound(frontier_epoch) = frontier_participant.binding() else {
        return ObligationDebtDispatchDecision::Defer(DebtDispatchDeferral::NoCurrentBinding);
    };
    if frontier_epoch != binding_epoch || frontier_participant.cursor() > dispatch_after {
        return ObligationDebtDispatchDecision::Invariant(
            DebtDispatchInvariant::ParticipantAuthority,
        );
    }

    let owed_high_watermark = match state {
        ObligationDebtDispatchState::Clear(frontier) => {
            if !matches!(frontier.closure_accounting().state(), ClosureState::Clear) {
                return ObligationDebtDispatchDecision::Invariant(
                    DebtDispatchInvariant::CoupledState,
                );
            }
            None
        }
        ObligationDebtDispatchState::Owed(coupled) => {
            let ClosureState::Owed { debt, .. } = coupled.frontier.closure_accounting().state()
            else {
                return ObligationDebtDispatchDecision::Invariant(
                    DebtDispatchInvariant::CoupledState,
                );
            };
            if coupled.episode.debt() != debt {
                return ObligationDebtDispatchDecision::Invariant(
                    DebtDispatchInvariant::CoupledState,
                );
            }
            if coupled.episode.participant_binding(participant_id)
                != Some((
                    FrontierBinding::Bound(binding_epoch),
                    frontier_participant.cursor(),
                ))
            {
                return ObligationDebtDispatchDecision::Invariant(
                    DebtDispatchInvariant::ParticipantAuthority,
                );
            }
            Some(coupled.episode.candidate_high_watermark())
        }
    };

    classify_selected_obligation(
        dispatch_after,
        owed_high_watermark,
        select_next(participant_id, binding_epoch, dispatch_after),
    )
}

fn classify_selected_obligation<T>(
    dispatch_after: DeliverySeq,
    owed_high_watermark: Option<DeliverySeq>,
    selected: Option<(DeliverySeq, T)>,
) -> ObligationDebtDispatchDecision<T> {
    let Some((delivery_seq, selected)) = selected else {
        return ObligationDebtDispatchDecision::Defer(DebtDispatchDeferral::NoObligation);
    };
    if delivery_seq <= dispatch_after {
        return ObligationDebtDispatchDecision::Invariant(DebtDispatchInvariant::OutboxSelection);
    }
    if owed_high_watermark.is_some_and(|high_watermark| delivery_seq > high_watermark) {
        return ObligationDebtDispatchDecision::Defer(
            DebtDispatchDeferral::BeyondDebtHighWatermark,
        );
    }
    ObligationDebtDispatchDecision::Permit(selected)
}

#[cfg(test)]
mod tests {
    use alloc::vec;

    use crate::{
        algebra::WideResourceVector,
        wire::{BindingEpoch, ConnectionIncarnation, Generation},
    };

    use super::{
        super::{BoundParticipantCursor, ClosureDebt, NonzeroDebtCursorEpisode},
        DebtDispatchDeferral, DebtDispatchInvariant, ObligationDebtDispatchDecision,
        classify_selected_obligation,
    };

    fn binding_epoch() -> BindingEpoch {
        BindingEpoch::new(
            ConnectionIncarnation::new(1, 1),
            Generation::new(1).unwrap_or(Generation::ONE),
        )
    }

    #[test]
    fn nonzero_debt_permits_testified_below_floor_and_defers_above_high_watermark() {
        let cursor = 0;
        let high_watermark = 100;
        let physical_floor = 25;
        let capacity_floor = 25;
        let below_floor_endpoint = 10;
        let above_high_watermark = high_watermark + 1;
        let episode = NonzeroDebtCursorEpisode::new(
            1,
            ClosureDebt::new(WideResourceVector::new(1, 1))
                .unwrap_or_else(|| unreachable!("fixture debt is nonzero")),
            high_watermark,
            high_watermark,
            physical_floor,
            capacity_floor,
            vec![BoundParticipantCursor::new(0, binding_epoch(), cursor)],
        )
        .unwrap_or_else(|error| unreachable!("fixture episode must be valid: {error:?}"));

        assert_eq!(episode.candidate_high_watermark(), high_watermark);
        assert_eq!(episode.observer_progress(), high_watermark);
        assert_eq!(episode.cap_floor(), capacity_floor);
        assert_eq!(episode.retained_suffix_start(), Some(25));
        assert!(!episode.retains(below_floor_endpoint));
        assert_eq!(
            classify_selected_obligation(
                cursor,
                Some(episode.candidate_high_watermark()),
                Some((below_floor_endpoint, below_floor_endpoint)),
            ),
            ObligationDebtDispatchDecision::Permit(below_floor_endpoint)
        );
        assert_eq!(
            classify_selected_obligation(
                cursor,
                Some(episode.candidate_high_watermark()),
                Some((above_high_watermark, above_high_watermark)),
            ),
            ObligationDebtDispatchDecision::Defer(DebtDispatchDeferral::BeyondDebtHighWatermark)
        );
        assert_eq!(
            classify_selected_obligation::<u64>(
                cursor,
                Some(episode.candidate_high_watermark()),
                None,
            ),
            ObligationDebtDispatchDecision::Defer(DebtDispatchDeferral::NoObligation)
        );
    }

    #[test]
    fn debt_dispatch_invariant_never_falls_back_or_fabricates_wire_refusal() {
        let cursor = 7;
        let selected_payload = "durable publication";
        let decision =
            classify_selected_obligation(cursor, Some(100), Some((cursor, selected_payload)));

        assert_eq!(
            decision,
            ObligationDebtDispatchDecision::Invariant(DebtDispatchInvariant::OutboxSelection)
        );
        assert!(!matches!(
            decision,
            ObligationDebtDispatchDecision::Permit(_)
        ));
        assert!(!matches!(
            decision,
            ObligationDebtDispatchDecision::Defer(_)
        ));
    }
}
