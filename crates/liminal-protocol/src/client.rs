//! Transport-agnostic client participant state and sealed effects.
//!
//! The client aggregate owns correlation, detach replay, and the durability
//! barrier for one outstanding operation. The authorized round-3 order is
//! commit-seal, persist the committed resume record, then release executable
//! authority; pending has no durable form.
//!
//! # `LP-CLIENT-GOAL` API-shape rationale
//!
//! The Phase 1 brief requires authority-safe shapes rather than caller-owned
//! state parts. The resulting public shape is deliberate:
//!
//! 1. Replay and reconnect transitions consume and return the root aggregate so
//!    their facts are persisted atomically; standalone state-part recomposition
//!    is not representable.
//! 2. [`ReconnectAggregate`] has no public constructor because fresh detached
//!    reconnect state would separate permit identity from participant facts.
//! 3. [`recover_reconnect_permit`] exists because an unissued committed cold
//!    record must release its permit once without making permits cloneable.
//! 4. Only the commit seal exposes a durable record; pending has no encoder. The
//!    authorized round-3 order is commit-seal, persist committed `LPCR`, release.
//! 5. The retained name [`crate::outcome::ReconnectDelayResult`] carries an
//!    event, never a delay: the brief explicitly supersedes timer scheduling.
//! 6. A different retained detach is refused while live, but terminal or
//!    attach-superseded replay yields only to an exact newer-generation detach.
//! 7. Terminalized-detach fixture construction remains `cfg(test)` only; wire
//!    authority construction is not weakened in production.
//! 8. Detach recording is atomic with [`record_operation`], so no public raw
//!    envelope-to-replay mint or second caller-owned persistence step exists.
//! 9. [`DetachTransportFate`] is closed to response unavailability because this
//!    protocol crate owns no socket, runtime, or transport handle.
//! 10. [`DetachReplayOutcome`] is exhaustive so a generic server value cannot be
//!     relabeled by a caller as one of the three terminal detach outcomes.
//! 11. Reconnect fresh-event producers are separate typed functions rather than
//!     a generic event-injection seam, limiting minting to the brief's classes.
//! 12. [`recover_expected_operation`] is the one-use post-restore counterpart to
//!     the pending-to-commit barrier; detach recovery also marks replay in flight.
//! 13. Replay inspection returns `None` only for its named Empty state; terminal
//!     payloads remain lossless and distinct.
//! 14. There is no speculative persistence format. A pending-window crash means
//!     the operation did not happen and restart may record it again.
//! 15. `RecordAdmission` responses are always ambiguous because wire identity is
//!     insufficient; `ObserverRecovery` compares its echoed list. A sealed
//!     transport-context upgrade belongs to the later SDK leg.
//! 16. Issued permit loss and interrupted attempts have explicit typed process
//!     fates; restore neither silently re-mints nor strands those states.
//! 17. Detached bindings retain the attach secret because the complete client
//!     record must remain capable of a later credential attach after restart.
//! 18. Expected recovery and replay-start atomically share one detach issuance
//!     bit, guaranteeing one first-send authority in either call order.
//!
//! # Exhaustive constructible-state audit
//!
//! Every state reachable through decode, restore, or apply is listed here.
//!
//! | Owned fact / state | Producers | Typed exits or documented terminal reason |
//! |---|---|---|
//! | Binding `Unbound` | new, restore | enrollment/attach → `Bound`; observer recovery; mismatched participant request refuses |
//! | Binding `Bound` | enrollment/attach, restore | detach → `Detached`; attach rotates; Leave/Retired → `Left` |
//! | Binding `Detached` | detach commit, restore | exact-secret attach → `Bound`; Leave/Retired → `Left` |
//! | Binding `Left` | Leave/Retired, restore | permanent terminal; inbound/outbound return `AlreadyDead` |
//! | Expected `None` | new, exact response/fate, abort, restore | one [`record_operation`] admission |
//! | Expected unissued non-detach | committed `LPCR`, restore | [`recover_expected_operation`] → issued once |
//! | Expected issued non-detach | release/recovery, restore | exact inbound; correlation + response-unavailable; process-lost exit |
//! | Expected unissued detach + replay `Parked` | committed `LPCR`, restore | expected recovery or replay start atomically marks issued |
//! | Expected issued detach + active replay | either first-send path, restore | exact inbound; replay fate/start; duplicate recovery refuses |
//! | Replay `Empty` | new, abort, restore | pending detach → `Parked` |
//! | Replay `Parked` | committed detach/fate, restore | matching expected required; transport start → `InFlight` |
//! | Replay `InFlight` | first-send path, restore | fate → `Parked`; outcome terminal; attach/Leave supersession |
//! | Replay `Superseded` | matching newer attach, restore | terminal for old generation; newer-generation detach may replace |
//! | Replay `LeaveSuperseded` | Leave/Retired, restore | terminal because binding is permanently `Left` |
//! | Replay terminal: three payload arms | exact outcome, restore | lossless terminal; newer-generation detach may replace |
//! | Reconnect `Parked` | new/failure/interruption, restore | typed fresh event → permit |
//! | Reconnect permit unissued | committed restore testimony | one [`recover_reconnect_permit`] → issued |
//! | Reconnect permit issued | fresh event/recovery, restore | held permit → attempt; process-lost → `Parked`; never re-minted |
//! | Reconnect attempt | permit redemption, restore | held fate → `Online`/`Parked`; process-lost → `Parked` |
//! | Reconnect `Online` | successful attempt, restore | later typed fresh event → permit |

use crate::wire::{ClientRequest, Generation, ParticipantAckEnvelope};

/// Coarse client binding state without exposing credential-bearing state parts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientBindingStatus {
    /// No participant binding has been established.
    Unbound,
    /// A live binding and attach credential are retained.
    Bound,
    /// The most recently correlated detach completed.
    Detached,
    /// A correlated durable Leave permanently retired the participant.
    Left,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ClientBindingState {
    Unbound,
    Bound {
        conversation_id: u64,
        participant_id: u64,
        generation: Generation,
        attach_secret: crate::wire::AttachSecret,
        binding_epoch: crate::wire::BindingEpoch,
    },
    Detached {
        conversation_id: u64,
        participant_id: u64,
        generation: Generation,
        attach_secret: crate::wire::AttachSecret,
    },
    Left {
        conversation_id: u64,
        participant_id: u64,
        generation: Generation,
    },
}

impl ClientBindingState {
    const fn status(&self) -> ClientBindingStatus {
        match self {
            Self::Unbound => ClientBindingStatus::Unbound,
            Self::Bound { .. } => ClientBindingStatus::Bound,
            Self::Detached { .. } => ClientBindingStatus::Detached,
            Self::Left { .. } => ClientBindingStatus::Left,
        }
    }

    const fn is_left(&self) -> bool {
        matches!(self, Self::Left { .. })
    }

    fn matches_ack(&self, request: &ParticipantAckEnvelope) -> bool {
        match self {
            Self::Bound {
                conversation_id,
                participant_id,
                generation,
                ..
            } => {
                *conversation_id == request.conversation_id
                    && *participant_id == request.participant_id
                    && *generation == request.capability_generation
            }
            Self::Unbound | Self::Detached { .. } | Self::Left { .. } => false,
        }
    }

    fn accepts_request(&self, request: &ClientRequest) -> bool {
        if self.is_left() {
            return false;
        }
        match request {
            ClientRequest::Enrollment(_) => matches!(self, Self::Unbound),
            ClientRequest::CredentialAttach(value) => {
                matches!(self, Self::Unbound)
                    || self.matches_credential(
                        value.conversation_id,
                        value.participant_id,
                        value.capability_generation,
                        value.attach_secret,
                        true,
                    )
            }
            ClientRequest::Detach(value) => self.matches_identity(
                value.conversation_id,
                value.participant_id,
                value.capability_generation,
                false,
            ),
            ClientRequest::ParticipantAck(value) => self.matches_identity(
                value.conversation_id,
                value.participant_id,
                value.capability_generation,
                false,
            ),
            ClientRequest::Leave(value) => self.matches_credential(
                value.conversation_id,
                value.participant_id,
                value.capability_generation,
                value.attach_secret,
                true,
            ),
            ClientRequest::MarkerAck(value) => self.matches_identity(
                value.conversation_id,
                value.participant_id,
                value.capability_generation,
                false,
            ),
            ClientRequest::RecordAdmission(value) => self.matches_identity(
                value.conversation_id,
                value.participant_id,
                value.capability_generation,
                false,
            ),
            ClientRequest::ObserverRecovery(_) => true,
        }
    }

    fn matches_identity(
        &self,
        conversation: u64,
        participant: u64,
        generation_value: Generation,
        allow_detached: bool,
    ) -> bool {
        match self {
            Self::Bound {
                conversation_id,
                participant_id,
                generation,
                ..
            } => {
                (*conversation_id, *participant_id, *generation)
                    == (conversation, participant, generation_value)
            }
            Self::Detached {
                conversation_id,
                participant_id,
                generation,
                ..
            } if allow_detached => {
                (*conversation_id, *participant_id, *generation)
                    == (conversation, participant, generation_value)
            }
            Self::Unbound | Self::Detached { .. } | Self::Left { .. } => false,
        }
    }

    fn matches_credential(
        &self,
        conversation: u64,
        participant: u64,
        generation_value: Generation,
        presented_secret: crate::wire::AttachSecret,
        allow_detached: bool,
    ) -> bool {
        match self {
            Self::Bound { attach_secret, .. } => {
                *attach_secret == presented_secret
                    && self.matches_identity(
                        conversation,
                        participant,
                        generation_value,
                        allow_detached,
                    )
            }
            Self::Detached { attach_secret, .. } if allow_detached => {
                *attach_secret == presented_secret
                    && self.matches_identity(conversation, participant, generation_value, true)
            }
            Self::Unbound | Self::Detached { .. } | Self::Left { .. } => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ExpectedOperationState {
    pub(super) request: ClientRequest,
    pub(super) issued: bool,
    pub(super) authorization: u64,
}

/// Non-cloneable client participant state shell.
///
/// Its expected operation, credential-bearing binding, replay request, and
/// reconnect state are private so callers must delegate every decision. This
/// brief-required root ownership prevents callers from recombining independently
/// persisted authorities into a state the crate never validated.
#[derive(Debug, PartialEq, Eq)]
pub struct ClientParticipantAggregate {
    pub(super) binding: ClientBindingState,
    pub(super) expected: Option<ExpectedOperationState>,
    pub(super) next_operation_authorization: u64,
    pub(super) detach_replay: SdkDetachReplayAggregate,
    pub(super) reconnect: ReconnectAggregate,
}

impl ClientParticipantAggregate {
    /// Creates a fresh unbound client aggregate.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            binding: ClientBindingState::Unbound,
            expected: None,
            next_operation_authorization: 0,
            detach_replay: SdkDetachReplayAggregate::new(),
            reconnect: ReconnectAggregate::new(),
        }
    }

    /// Reports binding status without exposing credential-bearing state.
    #[must_use]
    pub const fn binding_status(&self) -> ClientBindingStatus {
        self.binding.status()
    }

    /// Reports whether one write-ahead operation is outstanding.
    #[must_use]
    pub const fn has_expected_operation(&self) -> bool {
        self.expected.is_some()
    }

    /// Borrows the detach replay aggregate for status inspection.
    #[must_use]
    pub const fn detach_replay(&self) -> &SdkDetachReplayAggregate {
        &self.detach_replay
    }

    /// Borrows the reconnect aggregate for status inspection.
    #[must_use]
    pub const fn reconnect(&self) -> &ReconnectAggregate {
        &self.reconnect
    }
}

impl Default for ClientParticipantAggregate {
    fn default() -> Self {
        Self::new()
    }
}

mod barrier;
mod correlation;
mod inbound;
mod reconnect;
mod replay;
mod resume;
mod resume_decode;
mod resume_encode;

pub use barrier::*;
pub use inbound::{
    ClientCorrelatedInboundDecision, ClientCorrelatedInboundRefusal, ClientInboundApplied,
    ClientInboundDecision, ClientInboundRefusal, ClientInboundRefusalReason,
    decide_correlated_inbound, decide_inbound,
};
pub use reconnect::{
    EstablishedConnectionTransportFate, ExplicitReconnectAction, InterruptedReconnectAttemptFate,
    IssuedReconnectPermitFate, ProvedOnlineTransition, ReconnectAggregate,
    ReconnectAttemptDecision, ReconnectAttemptFate, ReconnectAttemptFateDecision,
    ReconnectAttemptFateRefusalReason, ReconnectAttemptPermit, ReconnectAttemptRefusalReason,
    ReconnectFreshEvent, ReconnectInProgressAttempt, ReconnectPermitDecision,
    ReconnectPermitRefusal, ReconnectPermitRefusalReason, ReconnectRestoreExitDecision,
    ReconnectRestoreExitRefusalReason, RecoveredReconnectPermitDecision, record_attempt_fate,
    record_explicit_reconnect, record_interrupted_attempt_fate, record_issued_permit_fate,
    record_online_transition, record_transport_fate, recover_reconnect_permit, redeem_attempt,
};
pub use replay::{
    ApplyAttachDecision, ApplyDetachOutcomeDecision, ApplyLeaveDecision, DetachReplayApplied,
    DetachReplayOutcome, DetachReplayRefusal, DetachReplayRefusalReason, DetachReplayStatus,
    DetachReplayTerminal, DetachTransportAttempt, DetachTransportAttemptDecision,
    DetachTransportFate, DetachTransportFateDecision, SdkDetachReplayAggregate, apply_attach,
    apply_detach_outcome, apply_leave_durable, transport_attempt_started, transport_fate,
};
pub use resume::{
    ClientResumeRecord, ClientResumeRecordDecodeError, ClientResumeRecordEncodeError,
    ClientResumeRecordSection, ClientResumeRestoreError,
};

#[cfg(test)]
mod resume_tests;
#[cfg(test)]
mod review_tests;
#[cfg(test)]
mod round3_tests;
#[cfg(test)]
mod tests;
