use crate::wire::{
    AttachEnvelope, BindingEpoch, DeliverySeq, DetachEnvelope, EnrollmentEnvelope, LeaveEnvelope,
    ObserverBackpressure, ObserverBackpressureState, RecordAdmissionEnvelope,
};

/// Operations whose candidate floor is checked against hard observer retention.
///
/// The variants carry every operation-specific field required by the frozen
/// `ObserverBackpressure` register. Normal and marker acknowledgements are
/// deliberately absent because they can never return this outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObserverCheckedOperation {
    /// Participant enrollment.
    Enrollment(EnrollmentEnvelope),
    /// Credential attach or supersession.
    CredentialAttach(AttachEnvelope),
    /// Initial detach of the exact committed binding epoch.
    InitialDetach {
        /// Exact detach request envelope.
        request: DetachEnvelope,
        /// Binding epoch the detach would terminalize.
        committed_binding_epoch: BindingEpoch,
    },
    /// Terminal Leave.
    Leave {
        /// Exact Leave request envelope.
        request: LeaveEnvelope,
        /// Whether an earlier binding-terminal cell already exists.
        prior_terminal_cell_exists: bool,
    },
    /// Ordinary record admission.
    RecordAdmission(RecordAdmissionEnvelope),
}

/// Opaque proof that one protocol-computed candidate floor passed stage 11.
///
/// Construction is private. A consuming server can persist and commit the
/// exact checked values, but cannot create a permit without running the shared
/// protocol selector.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObserverFloorPermit {
    observer_progress: DeliverySeq,
    cap_floor: u128,
}

impl ObserverFloorPermit {
    /// Returns the observer progress against which the floor was checked.
    #[must_use]
    pub const fn observer_progress(self) -> DeliverySeq {
        self.observer_progress
    }

    /// Returns the exact protocol-computed candidate capacity floor.
    #[must_use]
    pub const fn cap_floor(self) -> u128 {
        self.cap_floor
    }
}

/// Stage-11 observer hard-retention decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObserverFloorDecision {
    /// `cap_floor <= observer_progress + 1`; commit may continue with this proof.
    Eligible(ObserverFloorPermit),
    /// The candidate would remove a sequence above hard observer progress.
    Respond(ObserverBackpressure),
}

/// Applies the observer hard-retention selector to a protocol-computed floor.
///
/// The comparison is widened before checked one-past progress is formed.
/// Equality passes. A strict excess returns the exact operation-specific
/// [`ObserverBackpressure`] with an initial refusal epoch equal to current
/// observer progress.
#[must_use]
pub fn check_observer_floor(
    operation: ObserverCheckedOperation,
    observer_progress: DeliverySeq,
    cap_floor: u128,
) -> ObserverFloorDecision {
    let observer_limit = u128::from(observer_progress) + 1;
    if cap_floor <= observer_limit {
        return ObserverFloorDecision::Eligible(ObserverFloorPermit {
            observer_progress,
            cap_floor,
        });
    }

    let state = ObserverBackpressureState::initial(observer_progress);
    let response = match operation {
        ObserverCheckedOperation::Enrollment(request) => {
            ObserverBackpressure::Enrollment { request, state }
        }
        ObserverCheckedOperation::CredentialAttach(request) => {
            ObserverBackpressure::CredentialAttach { request, state }
        }
        ObserverCheckedOperation::InitialDetach {
            request,
            committed_binding_epoch,
        } => ObserverBackpressure::Detach {
            request,
            committed_binding_epoch,
            state,
        },
        ObserverCheckedOperation::Leave {
            request,
            prior_terminal_cell_exists,
        } => ObserverBackpressure::Leave {
            request,
            state,
            prior_terminal_cell_exists,
        },
        ObserverCheckedOperation::RecordAdmission(request) => {
            ObserverBackpressure::RecordAdmission { request, state }
        }
    };
    ObserverFloorDecision::Respond(response)
}
