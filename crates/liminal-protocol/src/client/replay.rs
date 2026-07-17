use super::ClientParticipantAggregate;
use crate::wire::{
    AttachBound, DetachCommitted, DetachEnvelope, DetachInProgress, LeaveCommitted,
    TerminalizedDetachCell,
};

/// Closed, lossless detach replay status vocabulary.
#[derive(Debug, PartialEq, Eq)]
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
#[derive(Debug, PartialEq, Eq)]
pub enum DetachReplayTerminal {
    /// Exact committed detach result.
    DetachCommitted(DetachCommitted),
    /// Exact competing-pending result.
    DetachInProgress(DetachInProgress),
    /// Exact terminalized old-cell authority result.
    TerminalizedDetachCell(TerminalizedDetachCell),
}

#[derive(Debug, PartialEq, Eq)]
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

/// Decision for recording an exact detach replay request.
#[derive(Debug, PartialEq, Eq)]
pub enum RecordDetachDecision {
    /// The request is durably parked.
    Recorded(DetachReplayApplied),
    /// Existing replay authority was preserved.
    Refused(DetachReplayRefusal<DetachEnvelope>),
}

/// Records the exact detach envelope without exposing replay state parts.
#[must_use]
pub const fn record_detach(
    mut aggregate: ClientParticipantAggregate,
    request: DetachEnvelope,
) -> RecordDetachDecision {
    if matches!(aggregate.detach_replay.state, DetachReplayState::Empty) {
        aggregate.detach_replay.state = DetachReplayState::Recorded {
            request,
            status: DetachReplayStatus::Parked,
        };
        RecordDetachDecision::Recorded(DetachReplayApplied { aggregate })
    } else {
        RecordDetachDecision::Refused(DetachReplayRefusal {
            aggregate,
            input: request,
            reason: DetachReplayRefusalReason::AlreadyRecorded,
        })
    }
}

/// Sealed effect authorizing one transport send of the exact detach.
#[derive(Debug, PartialEq, Eq)]
pub struct DetachTransportAttempt {
    request: DetachEnvelope,
}

impl DetachTransportAttempt {
    /// Borrows the exact detach to send.
    #[must_use]
    pub const fn request(&self) -> &DetachEnvelope {
        &self.request
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
#[must_use]
pub fn transport_attempt_started(
    mut aggregate: ClientParticipantAggregate,
) -> DetachTransportAttemptDecision {
    let request = match &mut aggregate.detach_replay.state {
        DetachReplayState::Recorded { request, status }
            if matches!(status, DetachReplayStatus::Parked) =>
        {
            *status = DetachReplayStatus::InFlight;
            request.clone()
        }
        DetachReplayState::Empty | DetachReplayState::Recorded { .. } => {
            return DetachTransportAttemptDecision::Refused(DetachReplayRefusal {
                aggregate,
                input: (),
                reason: DetachReplayRefusalReason::InvalidStatus,
            });
        }
    };
    DetachTransportAttemptDecision::Started {
        aggregate,
        attempt: DetachTransportAttempt { request },
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
    /// Typed fate parked the exact request for replay.
    Parked(DetachReplayApplied),
    /// No in-flight attempt existed; state and fate are retained.
    Refused(DetachReplayRefusal<DetachTransportFate>),
}

/// Applies typed transport fate; this is the only `InFlight -> Parked` path.
#[must_use]
pub const fn transport_fate(
    mut aggregate: ClientParticipantAggregate,
    fate: DetachTransportFate,
) -> DetachTransportFateDecision {
    match &mut aggregate.detach_replay.state {
        DetachReplayState::Recorded { status, .. }
            if matches!(status, DetachReplayStatus::InFlight) =>
        {
            let _ = fate;
            *status = DetachReplayStatus::Parked;
            DetachTransportFateDecision::Parked(DetachReplayApplied { aggregate })
        }
        DetachReplayState::Empty | DetachReplayState::Recorded { .. } => {
            DetachTransportFateDecision::Refused(DetachReplayRefusal {
                aggregate,
                input: fate,
                reason: DetachReplayRefusalReason::InvalidStatus,
            })
        }
    }
}

/// Decision for applying a matching newer attach to replay.
#[derive(Debug, PartialEq, Eq)]
pub enum ApplyAttachDecision {
    /// Matching attach superseded the old detach.
    Superseded(DetachReplayApplied),
    /// Non-matching attach was retained and replay state stayed exact.
    Refused(DetachReplayRefusal<AttachBound>),
}

/// Applies attach supersession without treating attach as transport fate.
#[must_use]
pub fn apply_attach(
    mut aggregate: ClientParticipantAggregate,
    attach: AttachBound,
) -> ApplyAttachDecision {
    if aggregate.detach_replay.apply_attach(&attach) {
        ApplyAttachDecision::Superseded(DetachReplayApplied { aggregate })
    } else {
        ApplyAttachDecision::Refused(DetachReplayRefusal {
            aggregate,
            input: attach,
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
    Refused(DetachReplayRefusal<LeaveCommitted>),
}

/// Applies durable Leave supersession.
#[must_use]
pub fn apply_leave_durable(
    mut aggregate: ClientParticipantAggregate,
    leave: LeaveCommitted,
) -> ApplyLeaveDecision {
    if aggregate.detach_replay.apply_leave(&leave) {
        ApplyLeaveDecision::Superseded(DetachReplayApplied { aggregate })
    } else {
        ApplyLeaveDecision::Refused(DetachReplayRefusal {
            aggregate,
            input: leave,
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
    Refused(DetachReplayRefusal<DetachReplayOutcome>),
}

/// Validates a typed detach outcome against the retained exact request.
#[must_use]
pub fn apply_detach_outcome(
    mut aggregate: ClientParticipantAggregate,
    outcome: DetachReplayOutcome,
) -> ApplyDetachOutcomeDecision {
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
        ApplyDetachOutcomeDecision::Terminal(DetachReplayApplied { aggregate })
    } else {
        ApplyDetachOutcomeDecision::Refused(DetachReplayRefusal {
            aggregate,
            input: outcome,
            reason: DetachReplayRefusalReason::ForeignInput,
        })
    }
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
