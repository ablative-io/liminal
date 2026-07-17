use super::{ClientParticipantAggregate, ClientResponseCorrelation};
use crate::wire::{
    AttachBound, DetachCommitted, DetachEnvelope, DetachInProgress, LeaveCommitted,
    TerminalizedDetachCell,
};

/// Closed, lossless detach replay status vocabulary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DetachReplayStatus {
    /// The exact detach is durably parked for a transport attempt.
    Parked,
    /// A transport attempt is outstanding.
    InFlight,
    /// A matching newer attach permanently superseded the old detach.
    Superseded,
    /// A matching durable Leave permanently superseded the old detach.
    LeaveSuperseded,
    /// A typed server result terminalized replay.
    Terminal(DetachReplayTerminal),
}

/// Typed terminal detach replay outcomes retained without projection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DetachReplayTerminal {
    /// Exact committed detach result.
    DetachCommitted(DetachCommitted),
    /// Exact competing-pending result.
    DetachInProgress(DetachInProgress),
    /// Exact terminalized old-cell authority result.
    TerminalizedDetachCell(TerminalizedDetachCell),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum DetachReplayState {
    Empty,
    Recorded {
        request: DetachEnvelope,
        status: DetachReplayStatus,
    },
}

/// Non-cloneable owner of the exact detach replay envelope and lifecycle.
#[derive(Debug, PartialEq, Eq)]
pub struct SdkDetachReplayAggregate {
    pub(super) state: DetachReplayState,
}

impl SdkDetachReplayAggregate {
    pub(super) const fn new() -> Self {
        Self {
            state: DetachReplayState::Empty,
        }
    }

    /// Borrows the exact retained detach request, absent only before first record.
    #[must_use]
    pub const fn request(&self) -> Option<&DetachEnvelope> {
        match &self.state {
            DetachReplayState::Empty => None,
            DetachReplayState::Recorded { request, .. } => Some(request),
        }
    }

    /// Borrows the lossless replay status, absent only before first record.
    #[must_use]
    pub const fn status(&self) -> Option<&DetachReplayStatus> {
        match &self.state {
            DetachReplayState::Empty => None,
            DetachReplayState::Recorded { status, .. } => Some(status),
        }
    }

    pub(super) const fn mark_initial_attempt_started(&mut self) {
        if let DetachReplayState::Recorded { status, .. } = &mut self.state {
            if matches!(status, DetachReplayStatus::Parked) {
                *status = DetachReplayStatus::InFlight;
            }
        }
    }

    pub(super) fn can_replace_with(&self, request: &DetachEnvelope) -> bool {
        match &self.state {
            DetachReplayState::Recorded {
                request: retained,
                status,
            } => {
                retained.conversation_id == request.conversation_id
                    && retained.participant_id == request.participant_id
                    && request.capability_generation > retained.capability_generation
                    && matches!(
                        status,
                        DetachReplayStatus::Superseded | DetachReplayStatus::Terminal(_)
                    )
            }
            DetachReplayState::Empty => false,
        }
    }

    pub(super) fn apply_attach(&mut self, attach: &AttachBound) -> bool {
        let DetachReplayState::Recorded { request, status } = &mut self.state else {
            return false;
        };
        if attach.conversation_id() == request.conversation_id
            && attach.participant_id() == request.participant_id
            && attach.request_generation() == request.capability_generation
            && attach.capability_generation() > request.capability_generation
        {
            *status = DetachReplayStatus::Superseded;
            true
        } else {
            false
        }
    }

    pub(super) fn apply_leave(&mut self, leave: &LeaveCommitted) -> bool {
        let DetachReplayState::Recorded { request, status } = &mut self.state else {
            return false;
        };
        if leave.conversation_id() == request.conversation_id
            && leave.participant_id() == request.participant_id
            && leave.presented_generation() == request.capability_generation
        {
            *status = DetachReplayStatus::LeaveSuperseded;
            true
        } else {
            false
        }
    }

    pub(super) fn apply_retired(
        &mut self,
        conversation_id: u64,
        participant_id: u64,
        retired_generation: crate::wire::Generation,
    ) -> bool {
        let DetachReplayState::Recorded { request, status } = &mut self.state else {
            return false;
        };
        if request.conversation_id == conversation_id
            && request.participant_id == participant_id
            && retired_generation >= request.capability_generation
        {
            *status = DetachReplayStatus::LeaveSuperseded;
            true
        } else {
            false
        }
    }

    pub(super) fn apply_detach_committed(&mut self, value: &DetachCommitted) -> bool {
        let DetachReplayState::Recorded { request, status } = &mut self.state else {
            return false;
        };
        if detach_committed_matches(request, value) {
            *status =
                DetachReplayStatus::Terminal(DetachReplayTerminal::DetachCommitted(value.clone()));
            true
        } else {
            false
        }
    }

    pub(super) fn apply_detach_in_progress(&mut self, value: &DetachInProgress) -> bool {
        let DetachReplayState::Recorded { request, status } = &mut self.state else {
            return false;
        };
        if detach_in_progress_matches(request, value) {
            *status =
                DetachReplayStatus::Terminal(DetachReplayTerminal::DetachInProgress(value.clone()));
            true
        } else {
            false
        }
    }

    pub(super) fn apply_terminalized_detach_cell(
        &mut self,
        value: &TerminalizedDetachCell,
    ) -> bool {
        let DetachReplayState::Recorded { request, status } = &mut self.state else {
            return false;
        };
        if terminalized_matches(request, value) {
            *status = DetachReplayStatus::Terminal(DetachReplayTerminal::TerminalizedDetachCell(
                value.clone(),
            ));
            true
        } else {
            false
        }
    }
}

/// Reason a detach replay input was refused unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DetachReplayRefusalReason {
    /// Replay is already active and cannot be silently replaced.
    AlreadyRecorded,
    /// The requested transition is not legal in the current replay status.
    InvalidStatus,
    /// The typed input does not match the retained exact detach request.
    ForeignInput,
}

/// Applied detach replay transition.
#[derive(Debug, PartialEq, Eq)]
pub struct DetachReplayApplied {
    aggregate: ClientParticipantAggregate,
}

impl DetachReplayApplied {
    /// Releases the resulting client aggregate.
    #[must_use]
    pub fn into_aggregate(self) -> ClientParticipantAggregate {
        self.aggregate
    }
}

/// Refused detach replay transition with unchanged aggregate and input.
#[derive(Debug, PartialEq, Eq)]
pub struct DetachReplayRefusal<T> {
    aggregate: ClientParticipantAggregate,
    input: T,
    reason: DetachReplayRefusalReason,
}

impl<T> DetachReplayRefusal<T> {
    /// Returns the closed refusal reason.
    #[must_use]
    pub const fn reason(&self) -> DetachReplayRefusalReason {
        self.reason
    }

    /// Releases the unchanged aggregate and refused typed input.
    #[must_use]
    pub fn into_parts(self) -> (ClientParticipantAggregate, T) {
        (self.aggregate, self.input)
    }
}

/// Sealed effect authorizing one transport send of the exact detach.
#[derive(Debug, PartialEq, Eq)]
pub struct DetachTransportAttempt {
    request: DetachEnvelope,
    authorization: u64,
}

impl DetachTransportAttempt {
    /// Borrows the exact detach to send.
    #[must_use]
    pub const fn request(&self) -> &DetachEnvelope {
        &self.request
    }

    /// Consumes this one-use send effect into the exact wire envelope and its
    /// lifecycle correlation. The correlation must be consumed by outcome,
    /// transport fate, or typed abandonment before another attempt can start.
    #[must_use]
    pub const fn into_request(self) -> (DetachEnvelope, ClientResponseCorrelation) {
        (
            self.request,
            ClientResponseCorrelation {
                authorization: self.authorization,
            },
        )
    }
}

/// Decision for starting a detach transport attempt.
#[derive(Debug, PartialEq, Eq)]
pub enum DetachTransportAttemptDecision {
    /// Replay moved from parked to in-flight and released one send effect.
    Started {
        /// Resulting aggregate.
        aggregate: ClientParticipantAggregate,
        /// One-use exact send effect.
        attempt: DetachTransportAttempt,
    },
    /// Replay stayed unchanged.
    Refused(DetachReplayRefusal<()>),
}

/// Moves a parked detach to in-flight and releases its exact send effect.
///
/// If the matching committed expected detach was still unissued, this path
/// atomically marks it issued. Consequently the inverse restore order
/// (`transport_attempt_started` before `recover_expected_operation`) cannot
/// release a second initial-send authority.
#[must_use]
pub fn transport_attempt_started(
    mut aggregate: ClientParticipantAggregate,
) -> DetachTransportAttemptDecision {
    let request = match &aggregate.detach_replay.state {
        DetachReplayState::Recorded {
            request,
            status: DetachReplayStatus::Parked,
        } => request.clone(),
        DetachReplayState::Empty | DetachReplayState::Recorded { .. } => {
            return DetachTransportAttemptDecision::Refused(DetachReplayRefusal {
                aggregate,
                input: (),
                reason: DetachReplayRefusalReason::InvalidStatus,
            });
        }
    };
    let expected_matches = aggregate.expected.as_ref().is_some_and(|expected| {
        expected.authorization != 0
            && matches!(&expected.request, crate::wire::ClientRequest::Detach(value)
            if value.conversation_id == request.conversation_id
                && value.participant_id == request.participant_id
                && value.capability_generation == request.capability_generation
                && value.detach_attempt_token == request.detach_attempt_token)
    });
    if !expected_matches {
        return DetachTransportAttemptDecision::Refused(DetachReplayRefusal {
            aggregate,
            input: (),
            reason: DetachReplayRefusalReason::InvalidStatus,
        });
    }
    let authorization = aggregate
        .expected
        .as_ref()
        .map_or(0, |expected| expected.authorization);
    if let Some(expected) = aggregate.expected.as_mut() {
        expected.issued = true;
    }
    if let DetachReplayState::Recorded { status, .. } = &mut aggregate.detach_replay.state {
        *status = DetachReplayStatus::InFlight;
    }
    DetachTransportAttemptDecision::Started {
        aggregate,
        attempt: DetachTransportAttempt {
            request,
            authorization,
        },
    }
}

/// Typed transport fate for an outstanding detach send.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DetachTransportFate {
    /// The transport failed before a semantic response was obtained.
    ResponseUnavailable,
}

/// Decision for returning an in-flight detach to parked replay.
#[derive(Debug, PartialEq, Eq)]
pub enum DetachTransportFateDecision {
    /// Typed fate consumed the exact send authority and parked replay.
    Parked(DetachReplayApplied),
    /// No matching in-flight attempt existed; state, authority, and fate are retained.
    Refused(DetachReplayRefusal<(ClientResponseCorrelation, DetachTransportFate)>),
}

/// Consumes the outstanding send authority with typed transport fate.
///
/// This is the live-process `InFlight -> Parked` path. It marks the matching
/// expected detach unissued before allowing another attempt, so the consumed
/// correlation and a replacement effect can never coexist.
#[must_use]
pub fn transport_fate(
    mut aggregate: ClientParticipantAggregate,
    correlation: ClientResponseCorrelation,
    fate: DetachTransportFate,
) -> DetachTransportFateDecision {
    if detach_authority_matches(&aggregate, &correlation) {
        if let DetachReplayState::Recorded { status, .. } = &mut aggregate.detach_replay.state {
            *status = DetachReplayStatus::Parked;
        }
        if let Some(expected) = aggregate.expected.as_mut() {
            expected.issued = false;
        }
        return DetachTransportFateDecision::Parked(DetachReplayApplied { aggregate });
    }
    DetachTransportFateDecision::Refused(DetachReplayRefusal {
        aggregate,
        input: (correlation, fate),
        reason: DetachReplayRefusalReason::InvalidStatus,
    })
}

/// Decision for applying a matching newer attach to replay.
#[derive(Debug, PartialEq, Eq)]
pub enum ApplyAttachDecision {
    /// Matching attach superseded the old detach.
    Superseded(DetachReplayApplied),
    /// Non-matching attach was retained and replay state stayed exact.
    Refused(DetachReplayRefusal<(AttachBound, ClientResponseCorrelation)>),
}

/// Applies attach supersession without treating attach as transport fate.
#[must_use]
pub fn apply_attach(
    mut aggregate: ClientParticipantAggregate,
    attach: AttachBound,
    correlation: ClientResponseCorrelation,
) -> ApplyAttachDecision {
    if detach_authority_matches(&aggregate, &correlation)
        && aggregate.detach_replay.apply_attach(&attach)
    {
        aggregate.expected = None;
        ApplyAttachDecision::Superseded(DetachReplayApplied { aggregate })
    } else {
        ApplyAttachDecision::Refused(DetachReplayRefusal {
            aggregate,
            input: (attach, correlation),
            reason: DetachReplayRefusalReason::ForeignInput,
        })
    }
}

/// Decision for applying a durable Leave to replay.
#[derive(Debug, PartialEq, Eq)]
pub enum ApplyLeaveDecision {
    /// Matching Leave superseded the old detach.
    Superseded(DetachReplayApplied),
    /// Non-matching Leave was retained with unchanged replay.
    Refused(DetachReplayRefusal<(LeaveCommitted, ClientResponseCorrelation)>),
}

/// Applies durable Leave supersession.
#[must_use]
pub fn apply_leave_durable(
    mut aggregate: ClientParticipantAggregate,
    leave: LeaveCommitted,
    correlation: ClientResponseCorrelation,
) -> ApplyLeaveDecision {
    if detach_authority_matches(&aggregate, &correlation)
        && aggregate.detach_replay.apply_leave(&leave)
    {
        aggregate.expected = None;
        ApplyLeaveDecision::Superseded(DetachReplayApplied { aggregate })
    } else {
        ApplyLeaveDecision::Refused(DetachReplayRefusal {
            aggregate,
            input: (leave, correlation),
            reason: DetachReplayRefusalReason::ForeignInput,
        })
    }
}

/// Typed terminal detach outcome accepted by replay.
#[derive(Debug, PartialEq, Eq)]
pub enum DetachReplayOutcome {
    /// Stable committed detach.
    DetachCommitted(DetachCommitted),
    /// Different token found a pending detach.
    DetachInProgress(DetachInProgress),
    /// Exact old token resolved to a terminalized cell.
    TerminalizedDetachCell(TerminalizedDetachCell),
}

/// Decision for terminalizing detach replay.
#[derive(Debug, PartialEq, Eq)]
pub enum ApplyDetachOutcomeDecision {
    /// Exact typed outcome terminalized replay.
    Terminal(DetachReplayApplied),
    /// Non-matching outcome was retained with unchanged replay.
    Refused(DetachReplayRefusal<(DetachReplayOutcome, ClientResponseCorrelation)>),
}

/// Validates a typed detach outcome against the retained exact request.
#[must_use]
pub fn apply_detach_outcome(
    mut aggregate: ClientParticipantAggregate,
    outcome: DetachReplayOutcome,
    correlation: ClientResponseCorrelation,
) -> ApplyDetachOutcomeDecision {
    if !detach_authority_matches(&aggregate, &correlation) {
        return ApplyDetachOutcomeDecision::Refused(DetachReplayRefusal {
            aggregate,
            input: (outcome, correlation),
            reason: DetachReplayRefusalReason::InvalidStatus,
        });
    }
    let applied = match &outcome {
        DetachReplayOutcome::DetachCommitted(value) => {
            aggregate.detach_replay.apply_detach_committed(value)
        }
        DetachReplayOutcome::DetachInProgress(value) => {
            aggregate.detach_replay.apply_detach_in_progress(value)
        }
        DetachReplayOutcome::TerminalizedDetachCell(value) => aggregate
            .detach_replay
            .apply_terminalized_detach_cell(value),
    };
    if applied {
        aggregate.expected = None;
        ApplyDetachOutcomeDecision::Terminal(DetachReplayApplied { aggregate })
    } else {
        ApplyDetachOutcomeDecision::Refused(DetachReplayRefusal {
            aggregate,
            input: (outcome, correlation),
            reason: DetachReplayRefusalReason::ForeignInput,
        })
    }
}

fn detach_authority_matches(
    aggregate: &ClientParticipantAggregate,
    correlation: &ClientResponseCorrelation,
) -> bool {
    let Some(expected) = aggregate.expected.as_ref() else {
        return false;
    };
    if !expected.issued || expected.authorization != correlation.authorization {
        return false;
    }
    let DetachReplayState::Recorded {
        request,
        status: DetachReplayStatus::InFlight,
    } = &aggregate.detach_replay.state
    else {
        return false;
    };
    matches!(&expected.request, crate::wire::ClientRequest::Detach(value)
        if value.conversation_id == request.conversation_id
            && value.participant_id == request.participant_id
            && value.capability_generation == request.capability_generation
            && value.detach_attempt_token == request.detach_attempt_token)
}

fn detach_committed_matches(request: &DetachEnvelope, value: &DetachCommitted) -> bool {
    value.conversation_id() == request.conversation_id
        && value.participant_id() == request.participant_id
        && value.capability_generation() == request.capability_generation
        && value.detach_attempt_token() == request.detach_attempt_token
}

fn detach_in_progress_matches(request: &DetachEnvelope, value: &DetachInProgress) -> bool {
    let expected_generation = request.capability_generation;
    let presented_generation = value.presented_generation;
    let expected_token = request.detach_attempt_token;
    let presented_token = value.presented_token;
    value.conversation_id == request.conversation_id
        && value.participant_id == request.participant_id
        && presented_generation == expected_generation
        && presented_token == expected_token
}

fn terminalized_matches(request: &DetachEnvelope, value: &TerminalizedDetachCell) -> bool {
    value.conversation_id() == request.conversation_id
        && value.participant_id() == request.participant_id
        && value.capability_generation() == request.capability_generation
        && value.detach_attempt_token() == request.detach_attempt_token
}
