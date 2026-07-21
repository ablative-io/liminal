use alloc::boxed::Box;

use crate::{algebra::floor_transition, wire::DeliverySeq};

use super::{LiveFrontierError, LiveFrontierOwner, live_frontier::BindingFateOwnerPlan};
use crate::lifecycle::{
    CommittedDiedTerminal, Event, FrontierBinding, ObserverProgressProjection, OrdinaryBindingFate,
    RecoveredBindingFate, SealedBindingFateIntent, SealedBindingFateToken,
};

/// Closed terminal input accepted by protocol-owned binding-fate measurement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindingFateTerminal {
    /// Ordinary fate consumes the exact committed Died terminal.
    Ordinary(CommittedDiedTerminal),
    /// Recovered fate after committed Died receives no Died terminal.
    Recovered,
    /// Recovered fate after pending Died preserves exact finalizer authority.
    RecoveredAndReserveFinalizer,
}

/// Protocol-produced binding fate after measuring the post-release floor.
#[derive(Debug, PartialEq, Eq)]
pub enum MeasuredBindingFate {
    /// Ordinary no-marker fate with exact Died provenance.
    Ordinary(OrdinaryBindingFate),
    /// Fenced recovered fate with no Died terminal input.
    Recovered(RecoveredBindingFate),
}

impl MeasuredBindingFate {
    /// Returns the measured floor carried by either closed fate class.
    #[must_use]
    pub const fn resulting_floor(&self) -> DeliverySeq {
        match self {
            Self::Ordinary(fate) => fate.resulting_floor(),
            Self::Recovered(fate) => fate.resulting_floor(),
        }
    }

    /// Projects the protocol-measured floor for observer routing.
    #[must_use]
    pub const fn observer_progress_projection(&self) -> ObserverProgressProjection {
        match self {
            Self::Ordinary(fate) => fate.observer_progress_projection(),
            Self::Recovered(fate) => fate.observer_progress_projection(),
        }
    }
}

/// Successful protocol measurement retaining the coupled frontier owner.
#[derive(Debug, PartialEq, Eq)]
pub struct PreparedBindingFate {
    owner: LiveFrontierOwner,
    fate: MeasuredBindingFate,
    event: Event,
}

impl PreparedBindingFate {
    /// Borrows the measured fate.
    #[must_use]
    pub const fn fate(&self) -> &MeasuredBindingFate {
        &self.fate
    }

    /// Returns the internally minted binding-fate event.
    #[must_use]
    pub const fn event(&self) -> Event {
        self.event
    }

    /// Consumes the prepared transition into the measured next owner and fate.
    #[must_use]
    pub fn into_parts(self) -> (LiveFrontierOwner, MeasuredBindingFate, Event) {
        (self.owner, self.fate, self.event)
    }
}

/// Floor authority measured before a Pending-Died enclosing finalizer commits.
#[derive(Debug, PartialEq, Eq)]
pub struct PendingDiedOrdinaryFinalizer {
    resulting_floor: DeliverySeq,
    authority: FinalizerAuthority,
}

#[derive(Debug, PartialEq, Eq)]
struct FinalizerAuthority;

/// Ordinary fate and unchanged owner prepared for an enclosing finalizer.
#[derive(Debug, PartialEq, Eq)]
pub struct PreparedPendingDiedOrdinaryFinalizer {
    owner: LiveFrontierOwner,
    fate: OrdinaryBindingFate,
    finalizer: PendingDiedOrdinaryFinalizer,
}

impl PreparedPendingDiedOrdinaryFinalizer {
    /// Returns the unchanged owner, measured fate, and one-use floor authority.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        LiveFrontierOwner,
        OrdinaryBindingFate,
        PendingDiedOrdinaryFinalizer,
    ) {
        (self.owner, self.fate, self.finalizer)
    }
}

impl LiveFrontierOwner {
    /// Applies a measured Ordinary floor after its enclosing finalizer transition.
    ///
    /// # Errors
    ///
    /// Returns [`LiveFrontierError`] if retained charges, the measured floor,
    /// marker precedence, or closure accounting disagree with the post-finalizer owner.
    pub fn complete_pending_died_ordinary_finalizer(
        self,
        finalizer: PendingDiedOrdinaryFinalizer,
    ) -> Result<Self, LiveFrontierError> {
        let PendingDiedOrdinaryFinalizer {
            resulting_floor,
            authority,
        } = finalizer;
        let FinalizerAuthority = authority;
        self.install_finalized_binding_fate_floor(resulting_floor)
    }
}

/// Typed reason protocol-owned binding-fate measurement refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindingFateMeasurementError {
    /// The sealed token has no unique ordinary or recovered authority.
    Token,
    /// The token names a different conversation.
    Conversation,
    /// The token's participant is absent from the coupled frontier.
    Participant,
    /// The token's binding epoch or cursor disagrees with the coupled frontier.
    Binding,
    /// Ordinary/recovered terminal input disagrees with the token class.
    Terminal,
    /// Hard observer progress exceeds the candidate high watermark.
    ObserverProgress,
    /// The measured checked floor is outside the delivery-sequence domain.
    ResultingFloor,
    /// The coupled frontier, retained charges, or closure baseline refused installation.
    OwnerTransition,
}

/// Refused measurement preserving every move-only input for serial retry.
#[derive(Debug, PartialEq, Eq)]
pub struct BindingFateMeasurementRefused {
    owner: LiveFrontierOwner,
    token: SealedBindingFateToken,
    terminal: BindingFateTerminal,
    error: BindingFateMeasurementError,
}

impl BindingFateMeasurementRefused {
    /// Returns the typed refusal reason.
    #[must_use]
    pub const fn error(&self) -> BindingFateMeasurementError {
        self.error
    }

    /// Recovers every unchanged input for a same-lock serial retry.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        LiveFrontierOwner,
        SealedBindingFateToken,
        BindingFateTerminal,
    ) {
        (self.owner, self.token, self.terminal)
    }
}

struct ValidatedBindingFateMeasurement {
    participant_id: crate::wire::ParticipantId,
    binding_epoch: crate::wire::BindingEpoch,
    resulting_floor: DeliverySeq,
    owner_plan: BindingFateOwnerPlan,
}

impl LiveFrontierOwner {
    /// Consumes one sealed fate token after measuring its real post-release floor.
    ///
    /// The server supplies only hard observer progress and the closed terminal
    /// class. The participant, binding epoch, current retained floor, candidate
    /// high watermark, and remaining member cursors all come from protocol-owned
    /// state. Recovered internally mints its event and accepts no terminal.
    ///
    /// # Errors
    ///
    /// Returns every input unchanged when authority, terminal class, observer
    /// progress, or checked floor validation fails.
    pub fn prepare_binding_fate(
        self,
        token: SealedBindingFateToken,
        terminal: BindingFateTerminal,
        hard_observer_progress: DeliverySeq,
    ) -> Result<PreparedBindingFate, Box<BindingFateMeasurementRefused>> {
        let measurement = match validate_binding_fate_measurement(
            &self,
            &token,
            terminal,
            hard_observer_progress,
        ) {
            Ok(measurement) => measurement,
            Err(error) => return refusal(self, token, terminal, error),
        };
        let event = Event::binding_fate_observed(
            measurement.participant_id,
            measurement.binding_epoch,
            measurement.resulting_floor,
        );
        let fate = match terminal {
            BindingFateTerminal::Ordinary(terminal) => token
                .ordinary_binding_fate(terminal, measurement.resulting_floor)
                .map(MeasuredBindingFate::Ordinary),
            BindingFateTerminal::Recovered | BindingFateTerminal::RecoveredAndReserveFinalizer => {
                token
                    .recovered_binding_fate_measured(measurement.resulting_floor)
                    .map(MeasuredBindingFate::Recovered)
            }
        };
        match fate {
            Ok(fate) => {
                let owner = self.install_binding_fate_transition(
                    measurement.owner_plan,
                    measurement.resulting_floor,
                );
                Ok(PreparedBindingFate { owner, fate, event })
            }
            Err(token) => Err(Box::new(BindingFateMeasurementRefused {
                owner: self,
                token: *token,
                terminal,
                error: BindingFateMeasurementError::Terminal,
            })),
        }
    }

    /// Measures Ordinary without installing its owner transition before an enclosing finalizer.
    ///
    /// # Errors
    ///
    /// Returns every move-only input unchanged when the token, terminal,
    /// observer progress, measured floor, or pre-finalizer owner transition
    /// is inconsistent.
    pub fn prepare_pending_died_ordinary_finalizer(
        self,
        token: SealedBindingFateToken,
        terminal: CommittedDiedTerminal,
        hard_observer_progress: DeliverySeq,
    ) -> Result<PreparedPendingDiedOrdinaryFinalizer, Box<BindingFateMeasurementRefused>> {
        let terminal_input = BindingFateTerminal::Ordinary(terminal);
        let measurement = match validate_binding_fate_measurement(
            &self,
            &token,
            terminal_input,
            hard_observer_progress,
        ) {
            Ok(measurement) => measurement,
            Err(error) => {
                return Err(Box::new(BindingFateMeasurementRefused {
                    owner: self,
                    token,
                    terminal: terminal_input,
                    error,
                }));
            }
        };
        let resulting_floor = measurement.resulting_floor;
        match token.ordinary_binding_fate(terminal, resulting_floor) {
            Ok(fate) => Ok(PreparedPendingDiedOrdinaryFinalizer {
                owner: self,
                fate,
                finalizer: PendingDiedOrdinaryFinalizer {
                    resulting_floor,
                    authority: FinalizerAuthority,
                },
            }),
            Err(token) => Err(Box::new(BindingFateMeasurementRefused {
                owner: self,
                token: *token,
                terminal: terminal_input,
                error: BindingFateMeasurementError::Terminal,
            })),
        }
    }
}

fn validate_binding_fate_measurement(
    owner: &LiveFrontierOwner,
    token: &SealedBindingFateToken,
    terminal: BindingFateTerminal,
    hard_observer_progress: DeliverySeq,
) -> Result<ValidatedBindingFateMeasurement, BindingFateMeasurementError> {
    let Some(context) = token.measurement_context() else {
        return Err(BindingFateMeasurementError::Token);
    };
    if context.conversation_id != owner.frontiers().conversation_id() {
        return Err(BindingFateMeasurementError::Conversation);
    }
    let Some(participant) = owner
        .frontiers()
        .active_identities()
        .participants()
        .iter()
        .find(|participant| participant.participant_index() == context.participant_id)
    else {
        return Err(BindingFateMeasurementError::Participant);
    };
    if participant.cursor() != context.cursor
        || participant.binding() != FrontierBinding::Bound(context.binding_epoch)
            && participant.binding() != FrontierBinding::Detached(context.binding_epoch)
    {
        return Err(BindingFateMeasurementError::Binding);
    }
    let terminal_matches = match (token.intent(), terminal) {
        (Some(SealedBindingFateIntent::Ordinary), BindingFateTerminal::Ordinary(died)) => {
            died.conversation_id() == context.conversation_id
                && died.participant_id() == context.participant_id
                && died.binding_epoch() == context.binding_epoch
        }
        (
            Some(SealedBindingFateIntent::Recovered { .. }),
            BindingFateTerminal::Recovered | BindingFateTerminal::RecoveredAndReserveFinalizer,
        ) => true,
        _ => false,
    };
    if !terminal_matches {
        return Err(BindingFateMeasurementError::Terminal);
    }
    let candidate_high_watermark = owner.frontiers().sequence().ledger().high_watermark();
    if hard_observer_progress > candidate_high_watermark {
        return Err(BindingFateMeasurementError::ObserverProgress);
    }
    let minimum_remaining_cursor = owner
        .frontiers()
        .active_identities()
        .participants()
        .iter()
        .filter(|participant| participant.participant_index() != context.participant_id)
        .map(|participant| participant.cursor())
        .min();
    let measured = floor_transition(
        owner.frontiers().retained_floor(),
        minimum_remaining_cursor,
        candidate_high_watermark,
        hard_observer_progress,
        owner.frontiers().retained_floor(),
    );
    let Ok(resulting_floor) = DeliverySeq::try_from(measured.resulting_floor) else {
        return Err(BindingFateMeasurementError::ResultingFloor);
    };
    let owner_plan = owner
        .prepare_binding_fate_transition(
            context.participant_id,
            context.binding_epoch,
            context.cursor,
            resulting_floor,
            terminal == BindingFateTerminal::RecoveredAndReserveFinalizer,
        )
        .map_err(|_| BindingFateMeasurementError::OwnerTransition)?;
    Ok(ValidatedBindingFateMeasurement {
        participant_id: context.participant_id,
        binding_epoch: context.binding_epoch,
        resulting_floor,
        owner_plan,
    })
}

fn refusal(
    owner: LiveFrontierOwner,
    token: SealedBindingFateToken,
    terminal: BindingFateTerminal,
    error: BindingFateMeasurementError,
) -> Result<PreparedBindingFate, Box<BindingFateMeasurementRefused>> {
    Err(Box::new(BindingFateMeasurementRefused {
        owner,
        token,
        terminal,
        error,
    }))
}
