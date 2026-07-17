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

/// Typed process fate for an issued operation restored without its process-local handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IssuedExpectedOperationFate {
    /// Process termination lost the issued operation and response correlation.
    ProcessLost,
}

/// Reason issued-operation process-loss recovery was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IssuedExpectedOperationFateRefusalReason {
    /// No issued expected operation is retained.
    NoIssuedOperation,
    /// Detach must remain governed by its lossless replay lifecycle.
    DetachUsesReplay,
}

/// Complete decision for an issued operation whose process-local authority was lost.
#[derive(Debug, PartialEq, Eq)]
pub enum IssuedExpectedOperationFateDecision {
    /// Process loss cleared the non-detach expected slot.
    Recorded {
        /// Resulting aggregate.
        aggregate: ClientParticipantAggregate,
        /// Exact operation terminalized as process-lost.
        request: ClientRequest,
        /// Consumed process fate.
        fate: IssuedExpectedOperationFate,
    },
    /// Aggregate and fate were retained unchanged.
    Refused {
        /// Unchanged aggregate.
        aggregate: ClientParticipantAggregate,
        /// Retained process fate.
        fate: IssuedExpectedOperationFate,
        /// Closed refusal reason.
        reason: IssuedExpectedOperationFateRefusalReason,
    },
}

/// Records process loss for a restored issued non-detach operation.
#[must_use]
pub fn record_issued_expected_operation_fate(
    mut aggregate: ClientParticipantAggregate,
    fate: IssuedExpectedOperationFate,
) -> IssuedExpectedOperationFateDecision {
    let Some(expected) = aggregate.expected.as_ref() else {
        return IssuedExpectedOperationFateDecision::Refused {
            aggregate,
            fate,
            reason: IssuedExpectedOperationFateRefusalReason::NoIssuedOperation,
        };
    };
    if !expected.issued {
        return IssuedExpectedOperationFateDecision::Refused {
            aggregate,
            fate,
            reason: IssuedExpectedOperationFateRefusalReason::NoIssuedOperation,
        };
    }
    if matches!(expected.request, ClientRequest::Detach(_)) {
        return IssuedExpectedOperationFateDecision::Refused {
            aggregate,
            fate,
            reason: IssuedExpectedOperationFateRefusalReason::DetachUsesReplay,
        };
    }
    match aggregate.expected.take() {
        Some(expected) => IssuedExpectedOperationFateDecision::Recorded {
            aggregate,
            request: expected.request,
            fate,
        },
        None => IssuedExpectedOperationFateDecision::Refused {
            aggregate,
            fate,
            reason: IssuedExpectedOperationFateRefusalReason::NoIssuedOperation,
        },
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
    let Some(authorization) = aggregate.next_operation_authorization.checked_add(1) else {
        return ClientOperationRecordDecision::Refused(ClientOperationRecordRefusal {
            aggregate,
            request,
            reason: ClientOperationRecordRefusalReason::AuthorizationExhausted,
        });
    };
    let mut prior_replay = None;
    if let ClientRequest::Detach(value) = &request {
        let envelope = crate::wire::DetachEnvelope {
            conversation_id: value.conversation_id,
            participant_id: value.participant_id,
            capability_generation: value.capability_generation,
            detach_attempt_token: value.detach_attempt_token,
        };
        match &aggregate.detach_replay.state {
            replay::DetachReplayState::Empty => {
                prior_replay = Some(aggregate.detach_replay.state.clone());
                aggregate.detach_replay.state = replay::DetachReplayState::Recorded {
                    request: envelope,
                    status: DetachReplayStatus::Parked,
                };
            }
            replay::DetachReplayState::Recorded {
                request: retained, ..
            } if retained == &envelope => {}
            replay::DetachReplayState::Recorded { .. }
                if aggregate.detach_replay.can_replace_with(&envelope) =>
            {
                prior_replay = Some(aggregate.detach_replay.state.clone());
                aggregate.detach_replay.state = replay::DetachReplayState::Recorded {
                    request: envelope,
                    status: DetachReplayStatus::Parked,
                };
            }
            replay::DetachReplayState::Recorded { .. } => {
                return ClientOperationRecordDecision::Refused(ClientOperationRecordRefusal {
                    aggregate,
                    request,
                    reason: ClientOperationRecordRefusalReason::DetachReplayOutstanding,
                });
            }
        }
    }
    aggregate.next_operation_authorization = authorization;
    aggregate.expected = Some(ExpectedOperationState {
        request: request.clone(),
        issued: false,
        authorization,
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
