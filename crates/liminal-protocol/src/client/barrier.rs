//! Brief-mandated write-ahead operation barrier and sealed response authority.

use super::{ClientParticipantAggregate, DetachReplayStatus, ExpectedOperationState, replay};
use crate::wire::ClientRequest;

/// Sealed, non-cloneable authority to execute exactly the committed operation.
#[derive(Debug, PartialEq, Eq)]
pub struct ExpectedParticipantOperation {
    request: ClientRequest,
    authorization: u64,
}

impl ExpectedParticipantOperation {
    /// Borrows the exact request released by the durability barrier.
    #[must_use]
    pub const fn request(&self) -> &ClientRequest {
        &self.request
    }

    /// Consumes the send authority into its exact request and a non-cloneable
    /// response-correlation capability.
    ///
    /// Returning both values from the advertised send path prevents successful
    /// request extraction from silently discarding the only typed handle that
    /// can terminalize an issued operation after transport loss.
    #[must_use]
    pub fn into_request(self) -> (ClientRequest, ClientResponseCorrelation) {
        (
            self.request,
            ClientResponseCorrelation {
                authorization: self.authorization,
            },
        )
    }
}

/// Non-cloneable local correlation for responses that omit request body identity.
#[derive(Debug, PartialEq, Eq)]
pub struct ClientResponseCorrelation {
    pub(super) authorization: u64,
}

/// Reason an operation could not enter the write-ahead slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientOperationRecordRefusalReason {
    /// Another write-ahead operation remains outstanding.
    OutstandingOperation,
    /// A different detach replay request remains retained.
    DetachReplayOutstanding,
    /// The retained same-envelope replay status is not compatible with a fresh
    /// first send; re-recording would revive expected-detach authority over an
    /// inactive replay (r2, 2026-07-18).
    DetachReplayIncompatible,
    /// A pending tokenless abandonment must be taken before another tokenless
    /// operation is recorded (r2, 2026-07-18).
    AbandonmentPending,
    /// The request is not legal for the retained binding identity or state.
    BindingMismatch,
    /// A durable Leave or retirement made the participant permanently dead.
    AlreadyDead,
    /// The wire response cannot distinguish this request from an older request.
    AmbiguousCorrelation,
    /// The monotonic local response-correlation counter is exhausted.
    AuthorizationExhausted,
}

/// Unchanged aggregate and request refused before persistence.
#[derive(Debug, PartialEq, Eq)]
pub struct ClientOperationRecordRefusal {
    aggregate: ClientParticipantAggregate,
    request: ClientRequest,
    reason: ClientOperationRecordRefusalReason,
}

impl ClientOperationRecordRefusal {
    /// Returns the closed refusal reason.
    #[must_use]
    pub const fn reason(&self) -> ClientOperationRecordRefusalReason {
        self.reason
    }

    /// Recovers the unchanged aggregate and refused request.
    #[must_use]
    pub fn into_parts(self) -> (ClientParticipantAggregate, ClientRequest) {
        (self.aggregate, self.request)
    }
}

/// Continuous acknowledgement that bypasses the write-ahead slot.
#[derive(Debug, PartialEq, Eq)]
pub struct ClientContinuousOperation {
    aggregate: ClientParticipantAggregate,
    operation: ExpectedParticipantOperation,
}

impl ClientContinuousOperation {
    /// Releases the unchanged aggregate and executable acknowledgement.
    #[must_use]
    pub fn into_parts(self) -> (ClientParticipantAggregate, ExpectedParticipantOperation) {
        (self.aggregate, self.operation)
    }
}

/// Pending decision whose successor and executable parts remain unreachable.
///
/// Round-3's authorized order is commit-seal, persist the committed `LPCR`, then
/// release. Pending has no persistence API: a crash here means the operation did
/// not happen and the caller may record it again after restart.
///
/// ```compile_fail
/// use liminal_protocol::client::ClientPendingOperationRecord;
/// fn speculative_persistence_is_forbidden(pending: ClientPendingOperationRecord) {
///     let _bytes = pending.encode_resume_record();
/// }
/// ```
#[derive(Debug, PartialEq, Eq)]
pub struct ClientPendingOperationRecord {
    pub(super) successor: ClientParticipantAggregate,
    operation: ExpectedParticipantOperation,
    prior_replay: Option<replay::DetachReplayState>,
}

impl ClientPendingOperationRecord {
    /// Seals the speculative decision as committed without yet releasing its
    /// successor aggregate or executable authority.
    #[must_use]
    pub fn commit(self) -> ClientOperationCommit {
        ClientOperationCommit {
            aggregate: self.successor,
            operation: self.operation,
        }
    }

    /// Aborts the speculative successor and returns the unchanged aggregate and
    /// refused request.
    #[must_use]
    pub fn abort(mut self) -> (ClientParticipantAggregate, ClientRequest) {
        self.successor.expected = None;
        self.successor.next_operation_authorization =
            self.operation.authorization.saturating_sub(1);
        if let Some(prior_replay) = self.prior_replay {
            self.successor.detach_replay.state = prior_replay;
        }
        (self.successor, self.operation.request)
    }
}

/// Committed operation decision that still seals aggregate and execution authority.
///
/// This implements the authorized round-3 mandate's governing order:
/// commit-seal, persist the committed `LPCR` exposed by this value, then call
/// [`Self::into_parts`] to release authority.
#[derive(Debug, PartialEq, Eq)]
pub struct ClientOperationCommit {
    pub(super) aggregate: ClientParticipantAggregate,
    operation: ExpectedParticipantOperation,
}

impl ClientOperationCommit {
    /// Releases the committed aggregate and one-use expected operation.
    #[must_use]
    pub fn into_parts(mut self) -> (ClientParticipantAggregate, ExpectedParticipantOperation) {
        if let Some(expected) = self.aggregate.expected.as_mut() {
            expected.issued = true;
        }
        if matches!(self.operation.request, ClientRequest::Detach(_)) {
            self.aggregate.detach_replay.mark_initial_attempt_started();
        }
        (self.aggregate, self.operation)
    }
}

/// Decision for releasing a committed cold-restored operation exactly once.
#[derive(Debug, PartialEq, Eq)]
pub enum RecoveredExpectedOperationDecision {
    /// The unissued committed operation was released and marked issued.
    Recovered {
        /// Resulting aggregate.
        aggregate: ClientParticipantAggregate,
        /// One-use exact operation authority.
        operation: ExpectedParticipantOperation,
    },
    /// No unissued committed operation is available.
    NotAvailable {
        /// Unchanged aggregate.
        aggregate: ClientParticipantAggregate,
        /// Whether an expected operation is retained but already issued.
        already_issued: bool,
    },
}

/// Releases an unissued operation from a validated committed cold restore.
///
/// Detach recovery atomically marks its initial replay attempt in flight, so
/// the generic expected-operation authority is the sole first-send authority.
#[must_use]
pub fn recover_expected_operation(
    mut aggregate: ClientParticipantAggregate,
) -> RecoveredExpectedOperationDecision {
    let Some(expected) = aggregate.expected.as_mut() else {
        return RecoveredExpectedOperationDecision::NotAvailable {
            aggregate,
            already_issued: false,
        };
    };
    if expected.issued {
        return RecoveredExpectedOperationDecision::NotAvailable {
            aggregate,
            already_issued: true,
        };
    }
    expected.issued = true;
    let request = expected.request.clone();
    let authorization = expected.authorization;
    if matches!(request, ClientRequest::Detach(_)) {
        aggregate.detach_replay.mark_initial_attempt_started();
    }
    RecoveredExpectedOperationDecision::Recovered {
        aggregate,
        operation: ExpectedParticipantOperation {
            request,
            authorization,
        },
    }
}

/// Reason a lost-authority resolution was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LostAuthorityResolutionRefusalReason {
    /// No serialized lost-authority testimony is pending in this domain.
    NoPendingTestimony,
}

/// Complete decision for resolving a restored operation-authority loss.
///
/// This is the sole recovery path for the operation-domain testimony minted by
/// validated cold restore (`LP-CLIENT-GOAL` piece 4, r2, 2026-07-18). It takes
/// no fate parameter: the serialized testimony inside the aggregate is the
/// consumed value, so a process-loss fate is not publicly constructible and
/// cannot terminalize an operation whose live correlation still exists.
#[derive(Debug, PartialEq, Eq)]
pub enum LostOperationAuthorityDecision {
    /// The consumed testimony terminalized the lost non-detach operation.
    Recorded {
        /// Resulting aggregate with an empty expected-operation slot.
        aggregate: ClientParticipantAggregate,
        /// Exact operation whose issued authority was destroyed.
        request: ClientRequest,
        /// Consumed take-once testimony.
        testimony: super::LostAuthorityTestimony,
    },
    /// The consumed testimony parked the exact-token detach for replay.
    DetachParked {
        /// Resulting aggregate with no live send effect.
        aggregate: ClientParticipantAggregate,
        /// Exact detach retained for replay.
        request: ClientRequest,
        /// Consumed take-once testimony.
        testimony: super::LostAuthorityTestimony,
    },
    /// No pending testimony existed; the aggregate is unchanged.
    Refused {
        /// Unchanged aggregate.
        aggregate: ClientParticipantAggregate,
        /// Closed refusal reason.
        reason: LostAuthorityResolutionRefusalReason,
    },
}

/// Consumes the pending operation-domain lost-authority testimony exactly once.
///
/// A second call after consumption returns a typed refusal without mutation,
/// so the take-once atom can never resolve the same loss twice.
#[must_use]
pub fn resolve_lost_operation_authority(
    mut aggregate: ClientParticipantAggregate,
) -> LostOperationAuthorityDecision {
    let Some(expected) = aggregate.expected.as_mut() else {
        return LostOperationAuthorityDecision::Refused {
            aggregate,
            reason: LostAuthorityResolutionRefusalReason::NoPendingTestimony,
        };
    };
    let Some(testimony) = expected.lost.take() else {
        return LostOperationAuthorityDecision::Refused {
            aggregate,
            reason: LostAuthorityResolutionRefusalReason::NoPendingTestimony,
        };
    };
    if matches!(expected.request, ClientRequest::Detach(_)) {
        expected.issued = false;
        let request = expected.request.clone();
        if let replay::DetachReplayState::Recorded { status, .. } =
            &mut aggregate.detach_replay.state
        {
            *status = DetachReplayStatus::Parked;
        }
        return LostOperationAuthorityDecision::DetachParked {
            aggregate,
            request,
            testimony,
        };
    }
    let request = expected.request.clone();
    aggregate.expected = None;
    LostOperationAuthorityDecision::Recorded {
        aggregate,
        request,
        testimony,
    }
}

/// Typed transport fate for an issued non-detach operation awaiting a response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpectedOperationTransportFate {
    /// The established transport ended before a correlated response arrived.
    ResponseUnavailable,
}

/// Reason an expected-operation transport fate was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpectedOperationFateRefusalReason {
    /// No issued expected operation is retained.
    NoIssuedOperation,
    /// Correlation belongs to an older operation.
    StaleCorrelation,
    /// Detach must use its lossless replay-specific transport fate.
    DetachUsesReplayFate,
    /// A restore already testified this issued authority destroyed; only the
    /// pending testimony can resolve it (r2, 2026-07-18).
    LostAuthorityPending,
}

/// Complete typed fate decision for an issued non-detach operation.
#[derive(Debug, PartialEq, Eq)]
pub enum ExpectedOperationFateDecision {
    /// The lost response terminalized the expectation without inventing a response.
    Recorded {
        /// Resulting aggregate with an empty expected-operation slot.
        aggregate: ClientParticipantAggregate,
        /// Exact request whose response became unavailable.
        request: ClientRequest,
        /// Consumed transport fate.
        fate: ExpectedOperationTransportFate,
    },
    /// Aggregate, correlation, and fate are unchanged.
    Refused {
        /// Unchanged aggregate.
        aggregate: ClientParticipantAggregate,
        /// Retained local correlation.
        correlation: ClientResponseCorrelation,
        /// Retained typed fate.
        fate: ExpectedOperationTransportFate,
        /// Closed refusal reason.
        reason: ExpectedOperationFateRefusalReason,
    },
}

/// Records transport loss for the exact issued non-detach operation.
///
/// This is the typed lifecycle exit for an issued expectation when no inbound
/// value can arrive. Detach is refused because its exact replay state owns the
/// corresponding lossless fate transition.
#[must_use]
pub fn record_expected_operation_fate(
    mut aggregate: ClientParticipantAggregate,
    correlation: ClientResponseCorrelation,
    fate: ExpectedOperationTransportFate,
) -> ExpectedOperationFateDecision {
    let Some(expected) = aggregate.expected.as_ref() else {
        return ExpectedOperationFateDecision::Refused {
            aggregate,
            correlation,
            fate,
            reason: ExpectedOperationFateRefusalReason::NoIssuedOperation,
        };
    };
    if !expected.issued {
        return ExpectedOperationFateDecision::Refused {
            aggregate,
            correlation,
            fate,
            reason: ExpectedOperationFateRefusalReason::NoIssuedOperation,
        };
    }
    if expected.lost.is_some() {
        return ExpectedOperationFateDecision::Refused {
            aggregate,
            correlation,
            fate,
            reason: ExpectedOperationFateRefusalReason::LostAuthorityPending,
        };
    }
    if expected.authorization != correlation.authorization {
        return ExpectedOperationFateDecision::Refused {
            aggregate,
            correlation,
            fate,
            reason: ExpectedOperationFateRefusalReason::StaleCorrelation,
        };
    }
    if matches!(expected.request, ClientRequest::Detach(_)) {
        return ExpectedOperationFateDecision::Refused {
            aggregate,
            correlation,
            fate,
            reason: ExpectedOperationFateRefusalReason::DetachUsesReplayFate,
        };
    }
    let expected = aggregate.expected.take();
    match expected {
        Some(expected) => ExpectedOperationFateDecision::Recorded {
            aggregate,
            request: expected.request,
            fate,
        },
        None => ExpectedOperationFateDecision::Refused {
            aggregate,
            correlation,
            fate,
            reason: ExpectedOperationFateRefusalReason::NoIssuedOperation,
        },
    }
}

/// Complete write-ahead admission decision.
#[derive(Debug, PartialEq, Eq)]
pub enum ClientOperationRecordDecision {
    /// Durability must precede commit and execution.
    Pending(ClientPendingOperationRecord),
    /// Continuous acknowledgements execute without occupying the slot.
    Continuous(ClientContinuousOperation),
    /// The one permitted slot is already occupied.
    Refused(ClientOperationRecordRefusal),
}

/// Records one operation behind the client durability barrier.
///
/// Continuous acknowledgements bypass the write-ahead slot. Every other request
/// is rejected while an expected operation exists; the crate never queues or
/// silently replaces it.
#[must_use]
pub fn record_operation(
    mut aggregate: ClientParticipantAggregate,
    request: ClientRequest,
) -> ClientOperationRecordDecision {
    if !aggregate.binding.accepts_request(&request) {
        let reason = if aggregate.binding.is_left() {
            ClientOperationRecordRefusalReason::AlreadyDead
        } else {
            ClientOperationRecordRefusalReason::BindingMismatch
        };
        return ClientOperationRecordDecision::Refused(ClientOperationRecordRefusal {
            aggregate,
            request,
            reason,
        });
    }
    if matches!(request, ClientRequest::ParticipantAck(_)) {
        return ClientOperationRecordDecision::Continuous(ClientContinuousOperation {
            aggregate,
            operation: ExpectedParticipantOperation {
                request,
                authorization: 0,
            },
        });
    }
    if aggregate.expected.is_some() {
        return ClientOperationRecordDecision::Refused(ClientOperationRecordRefusal {
            aggregate,
            request,
            reason: ClientOperationRecordRefusalReason::OutstandingOperation,
        });
    }
    if aggregate.restored_abandonment.is_some()
        && matches!(request, ClientRequest::ObserverRecovery(_))
    {
        return ClientOperationRecordDecision::Refused(ClientOperationRecordRefusal {
            aggregate,
            request,
            reason: ClientOperationRecordRefusalReason::AbandonmentPending,
        });
    }
    let Some(authorization) = aggregate.next_operation_authorization.checked_add(1) else {
        return ClientOperationRecordDecision::Refused(ClientOperationRecordRefusal {
            aggregate,
            request,
            reason: ClientOperationRecordRefusalReason::AuthorizationExhausted,
        });
    };
    let prior_replay = match admit_detach_replay(&mut aggregate, &request) {
        Ok(prior_replay) => prior_replay,
        Err(reason) => {
            return ClientOperationRecordDecision::Refused(ClientOperationRecordRefusal {
                aggregate,
                request,
                reason,
            });
        }
    };
    aggregate.next_operation_authorization = authorization;
    aggregate.expected = Some(ExpectedOperationState {
        request: request.clone(),
        issued: false,
        authorization,
        lost: None,
    });
    ClientOperationRecordDecision::Pending(ClientPendingOperationRecord {
        successor: aggregate,
        operation: ExpectedParticipantOperation {
            request,
            authorization,
        },
        prior_replay,
    })
}

/// Atomically admits a detach into the replay lifecycle during recording.
///
/// Re-recording the retained same envelope is admitted only over a `Parked`
/// replay: a superseded, Leave-superseded, terminal, or in-flight replay
/// status is not compatible with a fresh first send and refuses with a typed
/// reason instead of reviving expected-detach authority over an inactive
/// replay (r2, 2026-07-18).
fn admit_detach_replay(
    aggregate: &mut ClientParticipantAggregate,
    request: &ClientRequest,
) -> Result<Option<replay::DetachReplayState>, ClientOperationRecordRefusalReason> {
    let ClientRequest::Detach(value) = request else {
        return Ok(None);
    };
    let envelope = crate::wire::DetachEnvelope {
        conversation_id: value.conversation_id,
        participant_id: value.participant_id,
        capability_generation: value.capability_generation,
        detach_attempt_token: value.detach_attempt_token,
    };
    match &aggregate.detach_replay.state {
        replay::DetachReplayState::Empty => {
            let prior = aggregate.detach_replay.state.clone();
            aggregate.detach_replay.state = replay::DetachReplayState::Recorded {
                request: envelope,
                status: DetachReplayStatus::Parked,
            };
            Ok(Some(prior))
        }
        replay::DetachReplayState::Recorded {
            request: retained,
            status,
        } if retained == &envelope => {
            if matches!(status, DetachReplayStatus::Parked) {
                Ok(Some(aggregate.detach_replay.state.clone()))
            } else {
                Err(ClientOperationRecordRefusalReason::DetachReplayIncompatible)
            }
        }
        replay::DetachReplayState::Recorded { .. }
            if aggregate.detach_replay.can_replace_with(&envelope) =>
        {
            let prior = aggregate.detach_replay.state.clone();
            aggregate.detach_replay.state = replay::DetachReplayState::Recorded {
                request: envelope,
                status: DetachReplayStatus::Parked,
            };
            Ok(Some(prior))
        }
        replay::DetachReplayState::Recorded { .. } => {
            Err(ClientOperationRecordRefusalReason::DetachReplayOutstanding)
        }
    }
}
