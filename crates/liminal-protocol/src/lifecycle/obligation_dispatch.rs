//! Move-coupled closure-debt authority and synchronous delivery seam.

use crate::wire::{BindingEpoch, DeliverySeq, ParticipantId};

use super::{
    ClosureState, CursorEpisodeBuildError, FrontierBinding, LiveFrontierOwner,
    NonzeroDebtCursorEpisode,
};

/// Failure to couple a validated live frontier to its complete debt episode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObligationDebtOwnerError {
    /// Validated frontier/observer inputs could not form one exact episode.
    Episode(CursorEpisodeBuildError),
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
}

/// Internal scheduling deferral selected by the completed Leg 2 decision body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DebtDispatchDeferral {
    /// No durable recipient obligation follows the reconciled cursor.
    NoObligation,
    /// The participant has no exact current binding.
    NoCurrentBinding,
    /// The least testified endpoint is beyond the active candidate watermark.
    BeyondCandidateHighWatermark,
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

/// Executes the protocol-owned dispatch seam under the conversation owner.
///
/// Leg 1 deliberately installs the typed seam as record-and-permit: the
/// operation records the complete locked inputs in its type boundary and invokes
/// the existing least-obligation selection exactly once. Leg 2 fills the
/// decision body with the ruled debt selector without moving this call site.
pub fn decide_obligation_debt_dispatch<T>(
    state: &ObligationDebtDispatchState,
    participant_id: ParticipantId,
    binding_epoch: BindingEpoch,
    dispatch_after: DeliverySeq,
    select_next: impl FnOnce(ParticipantId, BindingEpoch, DeliverySeq) -> Option<T>,
) -> ObligationDebtDispatchDecision<Option<T>> {
    match state {
        ObligationDebtDispatchState::Clear(_) | ObligationDebtDispatchState::Owed(_) => {
            ObligationDebtDispatchDecision::Permit(select_next(
                participant_id,
                binding_epoch,
                dispatch_after,
            ))
        }
    }
}
