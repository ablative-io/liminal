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
//!     insufficient; `ObserverRecovery` compares its echoed list. As the M7
//!     companion rule, both tokenless classes resolve as typed abandoned on every
//!     restore and are never re-released. A later sealed-transport-context SDK leg
//!     may add outbound attempt tokens and lift this restriction.
//! 16. Issued permit loss and interrupted attempts have explicit typed process
//!     fates; restore neither silently re-mints nor strands those states.
//! 17. Detached bindings retain the attach secret because the complete client
//!     record must remain capable of a later credential attach after restart.
//! 18. Expected recovery and replay-start atomically share one detach issuance
//!     bit, guaranteeing one first-send authority in either call order.
//!
//! # Exhaustive constructible-state audit
//!
//! Every state accepted by restore or reachable through a public apply path is
//! listed here. The exhaustive conservation property test mechanically covers
//! the **live authority** and **typed consumption** columns across 358 valid paths
//! from a 3-operation × 10-transition alphabet through depth 7.
//!
//! | Owned state | Restore/apply acceptance | Live authority | Typed consumption / exit |
//! |---|---|---:|---|
//! | Binding `Unbound` | new, restore | 0 | enrollment/attach → `Bound`; identity-bound requests refuse |
//! | Binding `Bound` | enrollment/attach, restore | 0 | detach result → `Detached`; Leave/Retired → `Left` |
//! | Binding `Detached` | detach result, restore | 0 | exact-secret attach → `Bound`; Leave/Retired → `Left` |
//! | Binding `Left` | correlated Leave/Retired, restore | 0 | permanent; inbound/outbound return `AlreadyDead` |
//! | Expected `None` | new, abort, consumed outcome/fate, restore | 0 | one [`record_operation`] admission |
//! | Tokenless expected, live process only | record/commit/release | 0 or 1 | exact response remains conservative; fate consumes; every restore emits typed `TokenlessAfterCrash` abandonment and clears it |
//! | Token-bearing non-detach, unissued | committed `LPCR`, restore | 0 | [`recover_expected_operation`] issues once |
//! | Token-bearing non-detach, issued | release/recovery, restore | 1 live, or 1 lost testimony after crash | correlated outcome/fate, or typed `ProcessLost` |
//! | Expected detach + replay `Parked` | commit, live fate, restore only when exact and unissued | 0 | recovery or replay start issues exactly one effect |
//! | Expected detach + replay `InFlight` | release/start, restore only when exact and issued | 1 live, or 1 lost testimony after crash | correlated outcome/fate; typed `ProcessLost` parks exact-token replay |
//! | Expected detach + replay `Empty`, `Superseded`, `LeaveSuperseded`, or terminal | never accepted | 0 | typed `ExpectedDetachActiveReplayMismatch` restore refusal |
//! | Active replay without exact expected detach | never accepted | 0 | typed `ActiveReplayExpectedDetachMismatch` restore refusal |
//! | Replay `Empty` | new, abort, restore without expected detach | 0 | admitted detach records `Parked` atomically |
//! | Replay `Superseded` | authority-consuming matching attach, restore | 0 | old generation terminal; exact newer generation may replace |
//! | Replay `LeaveSuperseded` | authority-consuming matching durable Leave, restore | 0 | proves only that matching Leave/retirement superseded replay; public [`apply_leave_durable`] does **not** change binding to `Left` |
//! | Replay terminal (three exact payload arms) | authority-consuming exact outcome, restore | 0 | lossless terminal; exact newer-generation detach may replace |
//! | Reconnect `Parked` | new/failure/interruption, restore | 0 | typed fresh event → permit |
//! | Reconnect permit unissued | committed restore testimony | 0 | one [`recover_reconnect_permit`] issue |
//! | Reconnect permit issued | event/recovery, restore | 1 or lost testimony | held permit → attempt; typed process loss → `Parked` |
//! | Reconnect attempt | permit redemption, restore | 1 or lost testimony | held fate → `Online`/`Parked`; typed process loss → `Parked` |
//! | Reconnect `Online` | successful fate, restore | 0 | later typed fresh event → permit |

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

/// Why a persisted expected operation was deliberately not re-released.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestoredExpectedOperationAbandonmentReason {
    /// The operation class has no outbound attempt token and cannot be proven
    /// unsent after a crash.
    TokenlessAfterCrash,
}

/// Typed restore resolution for an operation that cannot safely be re-issued.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoredExpectedOperationAbandonment {
    request: ClientRequest,
    reason: RestoredExpectedOperationAbandonmentReason,
}

impl RestoredExpectedOperationAbandonment {
    /// Borrows the exact operation the restore boundary abandoned.
    #[must_use]
    pub const fn request(&self) -> &ClientRequest {
        &self.request
    }

    /// Reports the closed abandonment reason.
    #[must_use]
    pub const fn reason(&self) -> RestoredExpectedOperationAbandonmentReason {
        self.reason
    }

    /// Consumes the resolution into the request callers may explicitly re-record.
    #[must_use]
    pub fn into_request(self) -> ClientRequest {
        self.request
    }
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
    pub(super) restored_abandonment: Option<RestoredExpectedOperationAbandonment>,
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
            restored_abandonment: None,
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

    /// Takes the typed tokenless-operation resolution produced by cold restore.
    ///
    /// The expected slot is already empty when this value exists; taking the
    /// event cannot mint or release executable authority.
    #[must_use]
    pub const fn take_restored_operation_abandonment(
        &mut self,
    ) -> Option<RestoredExpectedOperationAbandonment> {
        self.restored_abandonment.take()
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
mod authority_property_tests;
#[cfg(test)]
mod resume_tests;
#[cfg(test)]
mod review_tests;
#[cfg(test)]
mod round3_tests;
#[cfg(test)]
mod round4_tests;
#[cfg(test)]
mod tests;
