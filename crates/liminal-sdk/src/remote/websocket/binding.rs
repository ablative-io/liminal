//! R2.2 typed-fate wiring: socket facts enter the client unit; policy never
//! exits it.
//!
//! The binding owns one [`ClientParticipantAggregate`] and mediates every real
//! WebSocket socket open against the client unit's sealed reconnect machinery:
//! a socket may open only after a one-use permit has become a
//! [`ReconnectInProgressAttempt`]; open completion is passed as
//! [`ReconnectAttemptFate::Connected`]/[`ReconnectAttemptFate::Failed`];
//! established loss is passed as [`EstablishedConnectionTransportFate::Lost`];
//! and a detach-send loss is passed to the typed detach replay fate. The
//! aggregate — never this binding, never the adapter — decides whether an
//! attempt, replay, park, refusal, or terminal result exists. A WebSocket
//! close code, I/O failure, or protocol violation is diagnostic data only and
//! never selects an aggregate transition.

use liminal_protocol::client::{
    ClientParticipantAggregate, ClientResponseCorrelation, DetachReplayRefusalReason,
    DetachTransportFate, DetachTransportFateDecision, EstablishedConnectionTransportFate,
    ExplicitReconnectAction, ReconnectAttemptDecision, ReconnectAttemptFate,
    ReconnectAttemptFateDecision, ReconnectAttemptFateRefusalReason, ReconnectAttemptPermit,
    ReconnectAttemptRefusalReason, ReconnectFreshEvent, ReconnectInProgressAttempt,
    ReconnectPermitDecision, ReconnectPermitRefusalReason, record_attempt_fate,
    record_explicit_reconnect, record_transport_fate, redeem_attempt, transport_fate,
};
use liminal_protocol::outcome::ReconnectState;

use super::core::TransportTerminal;

/// Decision for one open request.
#[derive(Debug, PartialEq, Eq)]
pub enum OpenRequestDecision {
    /// The aggregate authorized exactly one real socket open.
    Authorized {
        /// The fresh event that authorized this attempt.
        event: ReconnectFreshEvent,
    },
    /// The open was refused typed with unchanged authority state.
    Refused(OpenRequestRefusal),
}

/// Closed refusal classes for an open request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenRequestRefusal {
    /// A real open is already authorized and in progress.
    OpenAlreadyInProgress,
    /// The aggregate refused to mint a fresh permit.
    Permit(ReconnectPermitRefusalReason),
    /// The aggregate refused to redeem the held permit.
    Redemption(ReconnectAttemptRefusalReason),
}

/// Outcome of recording an open-completion socket fate.
#[derive(Debug, PartialEq, Eq)]
pub enum AttemptFateOutcome {
    /// The aggregate consumed the typed fate.
    Recorded {
        /// Resulting reconnect state (`Online` or `Parked`).
        state: ReconnectState,
    },
    /// The fate was refused typed with unchanged authority state.
    Refused(AttemptFateRefusal),
}

/// Closed refusal classes for an open-completion fate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttemptFateRefusal {
    /// No authorized open is in progress; the fate has no attempt to consume.
    NoOpenInProgress,
    /// The aggregate refused the fate for the held attempt.
    Fate(ReconnectAttemptFateRefusalReason),
}

/// Outcome of recording an established-connection loss.
#[derive(Debug, PartialEq, Eq)]
pub enum LossRecordOutcome {
    /// `Lost` was recorded; the minted one-use permit is retained for the
    /// next open request. No timer, no automatic retry.
    PermitRetained,
    /// The loss report was refused typed with unchanged authority state.
    Refused(LossRecordRefusal),
}

/// Closed refusal classes for an established-loss report.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LossRecordRefusal {
    /// No established connection exists at the aggregate (not `Online`).
    NotEstablished,
    /// The aggregate refused to mint the loss permit.
    Permit(ReconnectPermitRefusalReason),
}

/// Outcome of reporting a detach-send transport loss.
#[derive(Debug, PartialEq, Eq)]
pub enum DetachLossOutcome {
    /// The typed fate consumed the send authority and parked replay.
    Parked,
    /// The report was refused typed with unchanged replay state.
    Refused(DetachReplayRefusalReason),
}

/// Socket-fact conduit into one [`ClientParticipantAggregate`].
///
/// The binding retains at most one un-redeemed permit (minted by an
/// established loss) and at most one in-progress attempt (minted by an open
/// request); both are the client unit's sealed one-use authorities, so "one
/// real open per fresh authorization" holds by construction.
#[derive(Debug)]
pub struct WebSocketAuthorityBinding {
    aggregate: ClientParticipantAggregate,
    permit: Option<ReconnectAttemptPermit>,
    attempt: Option<ReconnectInProgressAttempt>,
    last_loss_diagnostic: Option<TransportTerminal>,
}

impl WebSocketAuthorityBinding {
    /// Creates a binding over a fresh, unbound client aggregate.
    #[must_use]
    pub const fn new() -> Self {
        Self::with_aggregate(ClientParticipantAggregate::new())
    }

    /// Creates a binding over an existing client aggregate (for example one
    /// restored from a committed resume record or prepared by the caller).
    #[must_use]
    pub const fn with_aggregate(aggregate: ClientParticipantAggregate) -> Self {
        Self {
            aggregate,
            permit: None,
            attempt: None,
            last_loss_diagnostic: None,
        }
    }

    /// Borrows the owned aggregate for inspection.
    #[must_use]
    pub const fn aggregate(&self) -> &ClientParticipantAggregate {
        &self.aggregate
    }

    /// Reports the aggregate's reconnect state.
    #[must_use]
    pub const fn reconnect_state(&self) -> ReconnectState {
        self.aggregate.reconnect().state()
    }

    /// The diagnostic terminal recorded with the most recent established loss.
    ///
    /// Diagnostics only: the terminal variant never selected the transition.
    #[must_use]
    pub const fn last_loss_diagnostic(&self) -> Option<&TransportTerminal> {
        self.last_loss_diagnostic.as_ref()
    }

    /// Requests authority for exactly one real socket open.
    ///
    /// A permit retained from an established loss is redeemed first; otherwise
    /// the request is recorded as the explicit caller action fresh event. On
    /// `Authorized` the caller must perform exactly one real open and then
    /// report its completion through [`connection_established`] or
    /// [`open_failed`].
    ///
    /// [`connection_established`]: Self::connection_established
    /// [`open_failed`]: Self::open_failed
    pub fn request_open(&mut self) -> OpenRequestDecision {
        if self.attempt.is_some() {
            return OpenRequestDecision::Refused(OpenRequestRefusal::OpenAlreadyInProgress);
        }
        let aggregate = self.take_aggregate();
        let (aggregate, permit) = match self.permit.take() {
            Some(retained) => (aggregate, retained),
            None => {
                match record_explicit_reconnect(aggregate, ExplicitReconnectAction::ReconnectNow) {
                    ReconnectPermitDecision::Permitted {
                        aggregate, permit, ..
                    } => (aggregate, permit),
                    ReconnectPermitDecision::Refused(refusal) => {
                        let reason = refusal.reason();
                        let (aggregate, _event) = refusal.into_parts();
                        self.aggregate = aggregate;
                        return OpenRequestDecision::Refused(OpenRequestRefusal::Permit(reason));
                    }
                }
            }
        };
        match redeem_attempt(aggregate, permit) {
            ReconnectAttemptDecision::Started { aggregate, attempt } => {
                let event = attempt.event();
                self.aggregate = aggregate;
                self.attempt = Some(attempt);
                OpenRequestDecision::Authorized { event }
            }
            ReconnectAttemptDecision::Refused {
                aggregate,
                permit,
                reason,
            } => {
                self.aggregate = aggregate;
                self.permit = Some(permit);
                OpenRequestDecision::Refused(OpenRequestRefusal::Redemption(reason))
            }
        }
    }

    /// Passes the socket's successful open as the typed `Connected` fate.
    pub fn connection_established(&mut self) -> AttemptFateOutcome {
        self.record_open_fate(ReconnectAttemptFate::Connected)
    }

    /// Passes the socket's failed open as the typed `Failed` fate, which
    /// parks the aggregate without minting any retry authority.
    pub fn open_failed(&mut self) -> AttemptFateOutcome {
        self.record_open_fate(ReconnectAttemptFate::Failed)
    }

    fn record_open_fate(&mut self, fate: ReconnectAttemptFate) -> AttemptFateOutcome {
        let Some(attempt) = self.attempt.take() else {
            return AttemptFateOutcome::Refused(AttemptFateRefusal::NoOpenInProgress);
        };
        let aggregate = self.take_aggregate();
        match record_attempt_fate(aggregate, attempt, fate) {
            ReconnectAttemptFateDecision::Recorded(aggregate) => {
                let state = aggregate.reconnect().state();
                self.aggregate = aggregate;
                AttemptFateOutcome::Recorded { state }
            }
            ReconnectAttemptFateDecision::Refused {
                aggregate,
                attempt,
                reason,
                ..
            } => {
                self.aggregate = aggregate;
                self.attempt = Some(attempt);
                AttemptFateOutcome::Refused(AttemptFateRefusal::Fate(reason))
            }
        }
    }

    /// Passes an established-connection terminal as the typed `Lost` fate.
    ///
    /// `terminal` is retained as diagnostics only; every variant reaches the
    /// identical aggregate decision. On success the minted one-use permit is
    /// retained for the next [`request_open`] — the binding never opens on its
    /// own and never re-arms a timer.
    ///
    /// [`request_open`]: Self::request_open
    pub fn established_terminal(&mut self, terminal: &TransportTerminal) -> LossRecordOutcome {
        if self.reconnect_state() != ReconnectState::Online {
            return LossRecordOutcome::Refused(LossRecordRefusal::NotEstablished);
        }
        let aggregate = self.take_aggregate();
        match record_transport_fate(aggregate, EstablishedConnectionTransportFate::Lost) {
            ReconnectPermitDecision::Permitted {
                aggregate, permit, ..
            } => {
                self.aggregate = aggregate;
                self.permit = Some(permit);
                self.last_loss_diagnostic = Some(*terminal);
                LossRecordOutcome::PermitRetained
            }
            ReconnectPermitDecision::Refused(refusal) => {
                let reason = refusal.reason();
                let (aggregate, _event) = refusal.into_parts();
                self.aggregate = aggregate;
                LossRecordOutcome::Refused(LossRecordRefusal::Permit(reason))
            }
        }
    }

    /// Passes a detach-send transport loss to the typed detach replay fate,
    /// consuming the outstanding send authority and parking replay.
    pub fn detach_send_lost(
        &mut self,
        correlation: ClientResponseCorrelation,
    ) -> DetachLossOutcome {
        let aggregate = self.take_aggregate();
        match transport_fate(
            aggregate,
            correlation,
            DetachTransportFate::ResponseUnavailable,
        ) {
            DetachTransportFateDecision::Parked(applied) => {
                self.aggregate = applied.into_aggregate();
                DetachLossOutcome::Parked
            }
            DetachTransportFateDecision::Refused(refusal) => {
                let reason = refusal.reason();
                let (aggregate, _input) = refusal.into_parts();
                self.aggregate = aggregate;
                DetachLossOutcome::Refused(reason)
            }
        }
    }

    /// Takes the owned aggregate for a consuming client-unit call; every
    /// decision arm reinstalls the returned aggregate before returning.
    const fn take_aggregate(&mut self) -> ClientParticipantAggregate {
        core::mem::replace(&mut self.aggregate, ClientParticipantAggregate::new())
    }
}

impl Default for WebSocketAuthorityBinding {
    fn default() -> Self {
        Self::new()
    }
}
