//! Client-side binding for the shared participant protocol.
//!
//! Wire decoding and lifecycle facts remain owned by `liminal-protocol`. This
//! module records only SDK-local delivery state: the latest typed binding and
//! whether the crate-owned detach replay authority may be sent after a fresh
//! external event.

use liminal_protocol::outcome::{AuthoritySuperseded, SdkDetachReplayAuthority};
use liminal_protocol::wire::{
    AttachBound, BindingEpoch, BindingStateView, ClientRequest, ConversationId, DetachEnvelope,
    DetachStaleAuthority, Generation, InboundGateContext, InboundGateError, ParticipantFrame,
    ParticipantId, ReceiptReplay, ServerPush, ServerValue, StaleAuthority, gate_inbound,
};

/// Participant identity and binding epoch proven by a protocol outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BoundParticipant {
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    generation: Generation,
    binding_epoch: BindingEpoch,
}

impl BoundParticipant {
    /// Conversation containing the bound participant.
    #[must_use]
    pub const fn conversation_id(self) -> ConversationId {
        self.conversation_id
    }

    /// Permanently assigned participant identity.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        self.participant_id
    }

    /// Current credential generation.
    #[must_use]
    pub const fn generation(self) -> Generation {
        self.generation
    }

    /// Current connection-scoped binding epoch.
    #[must_use]
    pub const fn binding_epoch(self) -> BindingEpoch {
        self.binding_epoch
    }

    const fn from_attach(value: &AttachBound) -> Self {
        Self {
            conversation_id: value.conversation_id(),
            participant_id: value.participant_id(),
            generation: value.capability_generation(),
            binding_epoch: value.origin_binding_epoch(),
        }
    }
}

/// Application-visible participant lifecycle state derived from typed outcomes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ParticipantClientState {
    /// No successful binding outcome has been received.
    #[default]
    Unattached,
    /// An enrollment or credential attach proved a current binding.
    Bound(BoundParticipant),
    /// A detach outcome proved that no current binding remains.
    Detached,
    /// A Leave outcome permanently retired the participant.
    Left,
}

/// Fresh external event that may authorize one detach replay attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DetachReplayEvent {
    /// Fate of an established connection proved that its response was lost.
    EstablishedConnectionFate,
    /// The transport proved a fresh transition to online.
    ProvedOnlineTransition,
    /// The caller explicitly requested one attempt.
    ExplicitCallerAction,
}

/// SDK-local detach replay status.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DetachReplayStatus {
    /// No detach has unknown durability.
    #[default]
    None,
    /// The exact request is parked until a fresh external event.
    Parked,
    /// One event-authorized attempt is currently in flight.
    InFlight,
    /// A newer matching attach consumed the crate-owned authority.
    AuthoritySuperseded,
    /// A newer Leave was durably written by the caller.
    LeaveSuperseded,
    /// A typed terminal detach status was received.
    Terminal,
}

/// Action selected after an external replay event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DetachReplayAction {
    /// Send this exact write-ahead request once.
    Send(DetachEnvelope),
    /// No replay authority is currently eligible to send.
    None,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ReplayState {
    None,
    Parked(SdkDetachReplayAuthority),
    InFlight(SdkDetachReplayAuthority),
    Superseded(AuthoritySuperseded),
    LeaveSuperseded,
    Terminal,
}

/// Crash-safe typed state needed to resume one participant session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParticipantResumeState {
    client_state: ParticipantClientState,
    replay_authority: Option<SdkDetachReplayAuthority>,
}

/// State transition caused by one decoded server outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantTransition {
    /// The outcome is surfaced without changing client binding state.
    Unchanged,
    /// Enrollment or attach established a binding.
    Bound,
    /// A detach committed and ended the binding.
    Detached,
    /// Leave permanently retired the participant.
    Left,
    /// A newer matching attach consumed old detach replay authority.
    AuthoritySuperseded(AuthoritySuperseded),
    /// `DetachInProgress` terminalized replay attempts.
    DetachInProgress,
    /// `TerminalizedDetachCell` terminalized replay attempts.
    TerminalizedDetachCell,
}

/// One exact server value plus its SDK-local lifecycle effect.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParticipantOutcome {
    value: ServerValue,
    transition: ParticipantTransition,
}

impl ParticipantOutcome {
    /// Borrows the exact protocol outcome without flattening its variant.
    #[must_use]
    pub const fn value(&self) -> &ServerValue {
        &self.value
    }

    /// Returns the client lifecycle transition selected by the typed outcome.
    #[must_use]
    pub const fn transition(&self) -> ParticipantTransition {
        self.transition
    }

    /// Returns the exact protocol outcome.
    #[must_use]
    pub fn into_value(self) -> ServerValue {
        self.value
    }
}

/// Exhaustive result of decoding a server participant frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParticipantReceive {
    /// One of all 37 registered server values.
    Outcome(ParticipantOutcome),
    /// One of both registered server pushes.
    Push(ServerPush),
    /// Directionally invalid request retained as a typed diagnostic.
    UnexpectedClientRequest(ClientRequest),
    /// The crate-owned inbound gate rejected the frame.
    Rejected(InboundGateError),
}

/// Client-side receive lifecycle for one participant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParticipantLifecycle {
    state: ParticipantClientState,
    replay: ReplayState,
}

impl ParticipantLifecycle {
    /// Creates an unattached participant lifecycle.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: ParticipantClientState::Unattached,
            replay: ReplayState::None,
        }
    }

    /// Restores typed client state after a crash.
    #[must_use]
    pub fn resume(state: ParticipantResumeState) -> Self {
        Self {
            state: state.client_state,
            replay: state
                .replay_authority
                .map_or(ReplayState::None, ReplayState::Parked),
        }
    }

    /// Returns durable state suitable for a caller-owned store.
    ///
    /// An in-flight request becomes parked: after a crash, only a new external
    /// event may authorize replay.
    #[must_use]
    pub fn crash_state(&self) -> ParticipantResumeState {
        ParticipantResumeState {
            client_state: self.state,
            replay_authority: self.active_authority().cloned(),
        }
    }

    /// Returns the current participant binding state.
    #[must_use]
    pub const fn state(&self) -> ParticipantClientState {
        self.state
    }

    /// Returns the current detach replay status.
    #[must_use]
    pub const fn detach_replay_status(&self) -> DetachReplayStatus {
        match self.replay {
            ReplayState::None => DetachReplayStatus::None,
            ReplayState::Parked(_) => DetachReplayStatus::Parked,
            ReplayState::InFlight(_) => DetachReplayStatus::InFlight,
            ReplayState::Superseded(_) => DetachReplayStatus::AuthoritySuperseded,
            ReplayState::LeaveSuperseded => DetachReplayStatus::LeaveSuperseded,
            ReplayState::Terminal => DetachReplayStatus::Terminal,
        }
    }

    /// Records the exact write-ahead detach request as replayable.
    pub const fn record_detach(&mut self, request: DetachEnvelope) {
        self.replay = ReplayState::Parked(SdkDetachReplayAuthority::new(request));
    }

    /// Records that a newer Leave request is durable and cancels old detach replay.
    pub const fn record_leave_durable(&mut self) {
        if self.active_authority().is_some() {
            self.replay = ReplayState::LeaveSuperseded;
        }
    }

    /// Consumes one fresh event and selects at most one exact replay request.
    #[must_use]
    pub fn on_replay_event(&mut self, event: DetachReplayEvent) -> DetachReplayAction {
        let _ = event;
        let replay = core::mem::replace(&mut self.replay, ReplayState::None);
        match replay {
            ReplayState::Parked(authority) => {
                let request = authority.request().clone();
                self.replay = ReplayState::InFlight(authority);
                DetachReplayAction::Send(request)
            }
            other => {
                self.replay = other;
                DetachReplayAction::None
            }
        }
    }

    /// Parks a failed in-flight attempt without arming a timer.
    pub const fn replay_attempt_failed(&mut self) {
        let replay = core::mem::replace(&mut self.replay, ReplayState::None);
        self.replay = match replay {
            ReplayState::InFlight(authority) => ReplayState::Parked(authority),
            other => other,
        };
    }

    /// Decodes a complete server frame with the crate-owned inbound gate.
    #[must_use]
    pub fn receive(&mut self, bytes: &[u8], context: InboundGateContext) -> ParticipantReceive {
        match gate_inbound(bytes, context) {
            Ok(frame) => self.receive_frame(frame),
            Err(error) => ParticipantReceive::Rejected(error),
        }
    }

    /// Applies one already-decoded crate wire frame.
    #[must_use]
    pub fn receive_frame(&mut self, frame: ParticipantFrame) -> ParticipantReceive {
        match frame {
            ParticipantFrame::ServerValue(value) => {
                ParticipantReceive::Outcome(self.receive_outcome(value))
            }
            ParticipantFrame::ServerPush(value) => ParticipantReceive::Push(value),
            ParticipantFrame::ClientRequest(value) => {
                ParticipantReceive::UnexpectedClientRequest(value)
            }
        }
    }

    fn receive_outcome(&mut self, value: ServerValue) -> ParticipantOutcome {
        let transition = match &value {
            ServerValue::EnrollBound(bound)
            | ServerValue::Bound(ReceiptReplay::Enrollment(bound)) => {
                self.state = ParticipantClientState::Bound(BoundParticipant {
                    conversation_id: bound.conversation_id(),
                    participant_id: bound.participant_id(),
                    generation: bound.capability_generation(),
                    binding_epoch: bound.origin_binding_epoch(),
                });
                ParticipantTransition::Bound
            }
            ServerValue::AttachBound(bound)
            | ServerValue::Bound(ReceiptReplay::CredentialAttach(bound)) => {
                let authority_transition = self.supersede_detach(bound);
                self.state = ParticipantClientState::Bound(BoundParticipant::from_attach(bound));
                authority_transition.unwrap_or(ParticipantTransition::Bound)
            }
            ServerValue::DetachCommitted(_) => {
                self.replay = ReplayState::Terminal;
                self.state = ParticipantClientState::Detached;
                ParticipantTransition::Detached
            }
            ServerValue::DetachInProgress(_) => {
                self.replay = ReplayState::Terminal;
                ParticipantTransition::DetachInProgress
            }
            ServerValue::StaleAuthority(StaleAuthority::Detach(
                DetachStaleAuthority::TerminalizedDetachCell(cell),
            )) => {
                self.replay = ReplayState::Terminal;
                self.state = match cell.binding_state() {
                    BindingStateView::Bound {
                        current_binding_epoch,
                    } => ParticipantClientState::Bound(BoundParticipant {
                        conversation_id: cell.conversation_id(),
                        participant_id: cell.participant_id(),
                        generation: cell.current_generation(),
                        binding_epoch: current_binding_epoch,
                    }),
                    BindingStateView::Detached => ParticipantClientState::Detached,
                };
                ParticipantTransition::TerminalizedDetachCell
            }
            ServerValue::LeaveCommitted(_) | ServerValue::Retired(_) => {
                self.replay = ReplayState::Terminal;
                self.state = ParticipantClientState::Left;
                ParticipantTransition::Left
            }
            _ => ParticipantTransition::Unchanged,
        };
        ParticipantOutcome { value, transition }
    }

    fn supersede_detach(&mut self, attach: &AttachBound) -> Option<ParticipantTransition> {
        let replay = core::mem::replace(&mut self.replay, ReplayState::None);
        match replay {
            ReplayState::Parked(authority) | ReplayState::InFlight(authority) => {
                match authority.supersede(attach) {
                    Ok(superseded) => {
                        self.replay = ReplayState::Superseded(superseded);
                        Some(ParticipantTransition::AuthoritySuperseded(superseded))
                    }
                    Err(authority) => {
                        self.replay = ReplayState::Parked(authority);
                        None
                    }
                }
            }
            other => {
                self.replay = other;
                None
            }
        }
    }

    const fn active_authority(&self) -> Option<&SdkDetachReplayAuthority> {
        match &self.replay {
            ReplayState::Parked(authority) | ReplayState::InFlight(authority) => Some(authority),
            ReplayState::None
            | ReplayState::Superseded(_)
            | ReplayState::LeaveSuperseded
            | ReplayState::Terminal => None,
        }
    }
}

impl Default for ParticipantLifecycle {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
