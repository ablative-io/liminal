use super::{
    ClientParticipantAggregate, LostAuthorityTestimony,
    barrier::LostAuthorityResolutionRefusalReason,
};
use crate::outcome::{ReconnectDelayResult, ReconnectRequiredEvent, ReconnectState};

/// Established-connection transport fate authorizing one reconnect attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EstablishedConnectionTransportFate {
    /// The established transport was lost.
    Lost,
}

/// Proved online transition authorizing one reconnect attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProvedOnlineTransition {
    /// Product state proved a fresh online transition.
    ProvedOnline,
}

/// Explicit caller action authorizing one reconnect attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExplicitReconnectAction {
    /// The caller explicitly requested a real connection attempt.
    ReconnectNow,
}

/// Closed fresh-event classes that may mint one reconnect permit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconnectFreshEvent {
    /// Established connection received typed transport fate.
    TransportFate(EstablishedConnectionTransportFate),
    /// Product state proved an online transition.
    OnlineTransition(ProvedOnlineTransition),
    /// Caller explicitly requested a reconnect.
    ExplicitCallerAction(ExplicitReconnectAction),
}

impl ReconnectFreshEvent {
    const fn required_event(self) -> ReconnectRequiredEvent {
        match self {
            Self::TransportFate(_) => ReconnectRequiredEvent::TransportFate,
            Self::OnlineTransition(_) => ReconnectRequiredEvent::OnlineTransition,
            Self::ExplicitCallerAction(_) => ReconnectRequiredEvent::ExplicitCallerAction,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ReconnectMachineState {
    Parked,
    Permit {
        authorization: u64,
        event: ReconnectFreshEvent,
        issued: bool,
    },
    Attempt {
        authorization: u64,
        event: ReconnectFreshEvent,
    },
    Online,
}

/// Non-cloneable reconnect producer and single-attempt state shell.
///
/// The brief keeps this state inside [`ClientParticipantAggregate`], rather
/// than exposing a fresh public constructor, so persisted authorization and the
/// root participant facts cannot be independently recombined.
#[derive(Debug, PartialEq, Eq)]
pub struct ReconnectAggregate {
    pub(super) state: ReconnectMachineState,
    pub(super) next_authorization: u64,
    pub(super) lost: Option<LostAuthorityTestimony>,
}

impl ReconnectAggregate {
    pub(super) const fn new() -> Self {
        Self {
            state: ReconnectMachineState::Parked,
            next_authorization: 0,
            lost: None,
        }
    }

    /// Reports reconnect state without exposing permit identity.
    #[must_use]
    pub const fn state(&self) -> ReconnectState {
        match self.state {
            ReconnectMachineState::Parked => ReconnectState::Parked,
            ReconnectMachineState::Permit { .. } => ReconnectState::PermitOutstanding,
            ReconnectMachineState::Attempt { .. } => ReconnectState::AttemptInProgress,
            ReconnectMachineState::Online => ReconnectState::Online,
        }
    }
}

/// Sealed, non-cloneable authority for one real connection attempt.
///
/// ```compile_fail
/// use liminal_protocol::client::ReconnectAttemptPermit;
/// fn clone_is_forbidden(permit: ReconnectAttemptPermit) {
///     let _duplicate = permit.clone();
/// }
/// ```
///
/// ```compile_fail
/// use liminal_protocol::client::{
///     ClientParticipantAggregate, ReconnectAttemptPermit, redeem_attempt,
/// };
/// fn moved_reuse_is_forbidden(
///     aggregate: ClientParticipantAggregate,
///     permit: ReconnectAttemptPermit,
/// ) {
///     let _started = redeem_attempt(aggregate, permit);
///     let _reuse = permit;
/// }
/// ```
#[derive(Debug, PartialEq, Eq)]
pub struct ReconnectAttemptPermit {
    authorization: u64,
    event: ReconnectFreshEvent,
}

impl ReconnectAttemptPermit {
    /// Returns the fresh event that authorized this attempt.
    #[must_use]
    pub const fn event(&self) -> ReconnectFreshEvent {
        self.event
    }
}

/// Reason a fresh event did not mint another permit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconnectPermitRefusalReason {
    /// A permit or attempt is already outstanding.
    AuthorizationOutstanding,
    /// The monotonic authorization counter is exhausted.
    AuthorizationExhausted,
}

/// Fresh-event refusal with unchanged aggregate and retained event.
#[derive(Debug, PartialEq, Eq)]
pub struct ReconnectPermitRefusal {
    aggregate: ClientParticipantAggregate,
    event: ReconnectFreshEvent,
    reason: ReconnectPermitRefusalReason,
    result: ReconnectDelayResult,
}

impl ReconnectPermitRefusal {
    /// Returns the closed refusal reason.
    #[must_use]
    pub const fn reason(&self) -> ReconnectPermitRefusalReason {
        self.reason
    }

    /// Returns the total legacy-named reconnect result with no delay arm.
    #[must_use]
    pub const fn result(&self) -> ReconnectDelayResult {
        self.result
    }

    /// Releases the unchanged aggregate and retained fresh event.
    #[must_use]
    pub fn into_parts(self) -> (ClientParticipantAggregate, ReconnectFreshEvent) {
        (self.aggregate, self.event)
    }
}

/// Complete fresh-event permit decision.
#[derive(Debug, PartialEq, Eq)]
pub enum ReconnectPermitDecision {
    /// One fresh event minted one one-use permit.
    Permitted {
        /// Aggregate retaining the matching authorization.
        aggregate: ClientParticipantAggregate,
        /// One-use attempt permit.
        permit: ReconnectAttemptPermit,
        /// Total legacy-named result, now event-based and timer-free.
        result: ReconnectDelayResult,
    },
    /// Existing authority was retained without mutation.
    Refused(ReconnectPermitRefusal),
}

/// Records established-connection transport fate and mints at most one permit.
#[must_use]
pub const fn record_transport_fate(
    aggregate: ClientParticipantAggregate,
    fate: EstablishedConnectionTransportFate,
) -> ReconnectPermitDecision {
    record_fresh_event(aggregate, ReconnectFreshEvent::TransportFate(fate))
}

/// Records a proved online transition and mints at most one permit.
#[must_use]
pub const fn record_online_transition(
    aggregate: ClientParticipantAggregate,
    transition: ProvedOnlineTransition,
) -> ReconnectPermitDecision {
    record_fresh_event(aggregate, ReconnectFreshEvent::OnlineTransition(transition))
}

/// Records explicit caller action and mints at most one permit.
#[must_use]
pub const fn record_explicit_reconnect(
    aggregate: ClientParticipantAggregate,
    action: ExplicitReconnectAction,
) -> ReconnectPermitDecision {
    record_fresh_event(aggregate, ReconnectFreshEvent::ExplicitCallerAction(action))
}

const fn record_fresh_event(
    mut aggregate: ClientParticipantAggregate,
    event: ReconnectFreshEvent,
) -> ReconnectPermitDecision {
    if !matches!(
        aggregate.reconnect.state,
        ReconnectMachineState::Parked | ReconnectMachineState::Online
    ) {
        let state = aggregate.reconnect.state();
        return ReconnectPermitDecision::Refused(ReconnectPermitRefusal {
            aggregate,
            event,
            reason: ReconnectPermitRefusalReason::AuthorizationOutstanding,
            result: ReconnectDelayResult::ReconnectNotArmed {
                state,
                required_event: event.required_event(),
            },
        });
    }
    let Some(authorization) = aggregate.reconnect.next_authorization.checked_add(1) else {
        let state = aggregate.reconnect.state();
        return ReconnectPermitDecision::Refused(ReconnectPermitRefusal {
            aggregate,
            event,
            reason: ReconnectPermitRefusalReason::AuthorizationExhausted,
            result: ReconnectDelayResult::ReconnectNotArmed {
                state,
                required_event: event.required_event(),
            },
        });
    };
    aggregate.reconnect.next_authorization = authorization;
    aggregate.reconnect.state = ReconnectMachineState::Permit {
        authorization,
        event,
        issued: true,
    };
    ReconnectPermitDecision::Permitted {
        aggregate,
        permit: ReconnectAttemptPermit {
            authorization,
            event,
        },
        result: ReconnectDelayResult::ReconnectArmed {
            event: event.required_event(),
        },
    }
}

/// Decision for releasing a validated cold-restored permit exactly once.
#[derive(Debug, PartialEq, Eq)]
pub enum RecoveredReconnectPermitDecision {
    /// Restore authority released one permit and marked it issued.
    Recovered {
        /// Resulting aggregate.
        aggregate: ClientParticipantAggregate,
        /// One-use restored permit.
        permit: ReconnectAttemptPermit,
    },
    /// No unissued restored permit exists.
    NotAvailable {
        /// Unchanged aggregate.
        aggregate: ClientParticipantAggregate,
        /// Current reconnect state.
        state: ReconnectState,
    },
}

/// Releases an unissued permit created only by a committed cold record.
///
/// A record whose own issuance bit is true is never re-minted: restore
/// testifies that loss instead, and only [`resolve_lost_reconnect_authority`]
/// consumes the serialized testimony to abandon the lost process-local
/// capability.
#[must_use]
pub const fn recover_reconnect_permit(
    mut aggregate: ClientParticipantAggregate,
) -> RecoveredReconnectPermitDecision {
    match aggregate.reconnect.state {
        ReconnectMachineState::Permit {
            authorization,
            event,
            issued: false,
        } => {
            aggregate.reconnect.state = ReconnectMachineState::Permit {
                authorization,
                event,
                issued: true,
            };
            RecoveredReconnectPermitDecision::Recovered {
                aggregate,
                permit: ReconnectAttemptPermit {
                    authorization,
                    event,
                },
            }
        }
        ReconnectMachineState::Parked
        | ReconnectMachineState::Permit { .. }
        | ReconnectMachineState::Attempt { .. }
        | ReconnectMachineState::Online => {
            let state = aggregate.reconnect.state();
            RecoveredReconnectPermitDecision::NotAvailable { aggregate, state }
        }
    }
}

/// Complete decision for resolving restored reconnect-authority loss.
///
/// This is the sole recovery path for the reconnect-domain testimony minted by
/// validated cold restore (`LP-CLIENT-GOAL` pieces 3 and 4, r2, 2026-07-18).
/// It takes no fate parameter: reconnect process-fate values are not publicly
/// constructible, so a caller can never terminalize an issued permit or
/// in-progress attempt whose live authority still exists.
#[derive(Debug, PartialEq, Eq)]
pub enum LostReconnectAuthorityDecision {
    /// The consumed testimony parked the producer without minting a
    /// replacement; only a later fresh event authorizes a new real attempt.
    Recorded {
        /// Resulting aggregate parked without timer or replacement authority.
        aggregate: ClientParticipantAggregate,
        /// Consumed take-once testimony.
        testimony: LostAuthorityTestimony,
    },
    /// No pending testimony existed; the aggregate is unchanged.
    Refused {
        /// Unchanged aggregate.
        aggregate: ClientParticipantAggregate,
        /// Closed refusal reason.
        reason: LostAuthorityResolutionRefusalReason,
    },
}

/// Consumes the pending reconnect-domain lost-authority testimony exactly once.
///
/// A second call after consumption returns a typed refusal without mutation.
#[must_use]
pub const fn resolve_lost_reconnect_authority(
    mut aggregate: ClientParticipantAggregate,
) -> LostReconnectAuthorityDecision {
    match aggregate.reconnect.lost.take() {
        Some(testimony) => {
            aggregate.reconnect.state = ReconnectMachineState::Parked;
            LostReconnectAuthorityDecision::Recorded {
                aggregate,
                testimony,
            }
        }
        None => LostReconnectAuthorityDecision::Refused {
            aggregate,
            reason: LostAuthorityResolutionRefusalReason::NoPendingTestimony,
        },
    }
}

/// Sealed in-progress authority held while the binding opens a real connection.
#[derive(Debug, PartialEq, Eq)]
pub struct ReconnectInProgressAttempt {
    authorization: u64,
    event: ReconnectFreshEvent,
}

impl ReconnectInProgressAttempt {
    /// Returns the fresh event authorizing the real connection attempt.
    #[must_use]
    pub const fn event(&self) -> ReconnectFreshEvent {
        self.event
    }
}

/// Reason a permit redemption was refused unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconnectAttemptRefusalReason {
    /// Aggregate has no matching outstanding permit.
    NoPermit,
    /// Permit belongs to an older or different authorization.
    StalePermit,
    /// A restore already testified the issued authority destroyed; only the
    /// pending testimony can resolve it (r2, 2026-07-18).
    LostAuthorityPending,
}

/// Complete permit redemption decision.
#[derive(Debug, PartialEq, Eq)]
pub enum ReconnectAttemptDecision {
    /// Permit became a sealed in-progress attempt before transport open.
    Started {
        /// Resulting aggregate.
        aggregate: ClientParticipantAggregate,
        /// One-use in-progress attempt authority.
        attempt: ReconnectInProgressAttempt,
    },
    /// Aggregate and permit were returned unchanged.
    Refused {
        /// Unchanged aggregate.
        aggregate: ClientParticipantAggregate,
        /// Retained stale or unmatched permit.
        permit: ReconnectAttemptPermit,
        /// Closed refusal reason.
        reason: ReconnectAttemptRefusalReason,
    },
}

/// Redeems one fresh permit into one in-progress real connection attempt.
#[must_use]
pub fn redeem_attempt(
    mut aggregate: ClientParticipantAggregate,
    permit: ReconnectAttemptPermit,
) -> ReconnectAttemptDecision {
    if aggregate.reconnect.lost.is_some() {
        return ReconnectAttemptDecision::Refused {
            aggregate,
            permit,
            reason: ReconnectAttemptRefusalReason::LostAuthorityPending,
        };
    }
    match aggregate.reconnect.state {
        ReconnectMachineState::Permit {
            authorization,
            event,
            ..
        } if authorization == permit.authorization && event == permit.event => {
            aggregate.reconnect.state = ReconnectMachineState::Attempt {
                authorization,
                event,
            };
            ReconnectAttemptDecision::Started {
                aggregate,
                attempt: ReconnectInProgressAttempt {
                    authorization,
                    event,
                },
            }
        }
        ReconnectMachineState::Permit { .. } => ReconnectAttemptDecision::Refused {
            aggregate,
            permit,
            reason: ReconnectAttemptRefusalReason::StalePermit,
        },
        ReconnectMachineState::Parked
        | ReconnectMachineState::Attempt { .. }
        | ReconnectMachineState::Online => ReconnectAttemptDecision::Refused {
            aggregate,
            permit,
            reason: ReconnectAttemptRefusalReason::NoPermit,
        },
    }
}

/// Typed fate of one real connection attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconnectAttemptFate {
    /// The binding proved the connection online.
    Connected,
    /// The real connection attempt failed and parks without timer authority.
    Failed,
}

/// Reason an attempt fate was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconnectAttemptFateRefusalReason {
    /// Aggregate has no matching in-progress attempt.
    NoAttempt,
    /// Attempt belongs to an older authorization.
    StaleAttempt,
    /// A restore already testified the in-progress authority destroyed; only
    /// the pending testimony can resolve it (r2, 2026-07-18).
    LostAuthorityPending,
}

/// Complete attempt-fate decision.
#[derive(Debug, PartialEq, Eq)]
pub enum ReconnectAttemptFateDecision {
    /// Typed fate returned the aggregate to online or parked state.
    Recorded(ClientParticipantAggregate),
    /// Aggregate, attempt, and fate were retained unchanged.
    Refused {
        /// Unchanged aggregate.
        aggregate: ClientParticipantAggregate,
        /// Retained in-progress authority.
        attempt: ReconnectInProgressAttempt,
        /// Retained typed fate.
        fate: ReconnectAttemptFate,
        /// Closed refusal reason.
        reason: ReconnectAttemptFateRefusalReason,
    },
}

/// Records typed success or failure for one in-progress real attempt.
#[must_use]
pub fn record_attempt_fate(
    mut aggregate: ClientParticipantAggregate,
    attempt: ReconnectInProgressAttempt,
    fate: ReconnectAttemptFate,
) -> ReconnectAttemptFateDecision {
    if aggregate.reconnect.lost.is_some() {
        return ReconnectAttemptFateDecision::Refused {
            aggregate,
            attempt,
            fate,
            reason: ReconnectAttemptFateRefusalReason::LostAuthorityPending,
        };
    }
    match aggregate.reconnect.state {
        ReconnectMachineState::Attempt {
            authorization,
            event,
        } if authorization == attempt.authorization && event == attempt.event => {
            aggregate.reconnect.state = match fate {
                ReconnectAttemptFate::Connected => ReconnectMachineState::Online,
                ReconnectAttemptFate::Failed => ReconnectMachineState::Parked,
            };
            ReconnectAttemptFateDecision::Recorded(aggregate)
        }
        ReconnectMachineState::Attempt { .. } => ReconnectAttemptFateDecision::Refused {
            aggregate,
            attempt,
            fate,
            reason: ReconnectAttemptFateRefusalReason::StaleAttempt,
        },
        ReconnectMachineState::Parked
        | ReconnectMachineState::Permit { .. }
        | ReconnectMachineState::Online => ReconnectAttemptFateDecision::Refused {
            aggregate,
            attempt,
            fate,
            reason: ReconnectAttemptFateRefusalReason::NoAttempt,
        },
    }
}
