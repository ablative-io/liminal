mod binding_fate;
mod binding_terminal;
mod enrollment_operation;
mod live_frontier;
mod marker_ack;
mod marker_drain;
mod marker_proof;
mod nonzero_participant_ack;
pub(in crate::lifecycle) mod ordinary_record_projection;
mod participant_ack;
mod record_admission;

#[cfg(test)]
mod binding_fate_tests;
#[cfg(test)]
mod binding_terminal_tests;
#[cfg(test)]
mod enrollment_operation_tests;
#[cfg(test)]
mod marker_ack_tests;
#[cfg(test)]
mod marker_drain_tests;
#[cfg(test)]
mod marker_proof_tests;
#[cfg(test)]
mod nonzero_participant_ack_tests;
#[cfg(test)]
mod ordinary_record_projection_tests;
#[cfg(test)]
mod participant_ack_tests;
#[cfg(test)]
mod record_admission_selector_acceptance_tests;
#[cfg(test)]
mod record_admission_tests;

pub use binding_fate::{
    BindingFateMeasurementError, BindingFateMeasurementRefused, BindingFateTerminal,
    MeasuredBindingFate, PendingDiedOrdinaryFinalizer, PreparedBindingFate,
    PreparedPendingDiedOrdinaryFinalizer,
};
pub use binding_terminal::{
    BindingTerminalAdmission, BindingTerminalAdmitError, BindingTerminalAdmitRefused,
    BindingTerminalCandidateCharge, BindingTerminalCauseClass, BindingTerminalCommit,
    BindingTerminalEncoding, BindingTerminalPending, BindingTerminalPrepareError,
    BindingTerminalPrepareRefused, CandidateTerminalKey, PreparedBindingTerminal,
};
pub use enrollment_operation::{
    InitialEnrollmentCommitValues, InitialEnrollmentOperationCommit,
    InitialEnrollmentOperationDecision, InitialEnrollmentOperationFault,
    InitialEnrollmentOperationInput, ReceiptDeadlineError, ReceiptDeadlines,
    apply_initial_enrollment,
};
pub use live_frontier::{
    AttachFrontierCharges, FencedAttachMintRefusalReason, FencedMarkerSourceExpectation,
    FencedMarkerSourceRetentionRefused, LiveFrontierCommit, LiveFrontierError, LiveFrontierFailure,
    LiveFrontierOwner, LiveFrontierResult, LiveLeaveCommit, LiveLeaveError,
    MintFencedAttachRefused, MintFencedAttachResult, MintedFencedAttach,
    RetainedFencedMarkerSource, apply_attach_frontier, apply_detach_frontier,
    apply_enrollment_frontier, apply_marker_ack_frontier, apply_nonzero_participant_ack_frontier,
    apply_participant_ack_frontier, commit_pending_leave_frontier, commit_settled_leave_frontier,
};
pub use marker_ack::{MarkerAckCommit, MarkerAckCommitError, MarkerAckDecision, apply_marker_ack};
pub use marker_drain::{
    MarkerDeliveryProjection, MarkerDrainCommit, MarkerDrainError, drain_next_marker,
};
pub use marker_proof::{
    MarkerProofDecision, MarkerProofInput, MarkerProofPermit, MarkerProofState, select_marker_proof,
};
pub use nonzero_participant_ack::{
    NonzeroAckEpisodePosition, NonzeroParticipantAckCommit, NonzeroParticipantAckCommitError,
    NonzeroParticipantAckDecision, NonzeroParticipantAckInvariantError,
    apply_nonzero_participant_ack, apply_nonzero_participant_ack_with_obligations,
};
pub use ordinary_record_projection::{
    OrdinaryProjectionError, OrdinaryProjectionLimits, OrdinaryRecordDrainFirst,
    OrdinaryRecordProjectionDecision, OrdinaryRecordProjectionFailure,
    OrdinaryRecordProjectionInput, ProjectedOrdinaryRecord, RetainedRecordCharge,
};
pub use participant_ack::{
    ParticipantAckCommit, ParticipantAckCommitError, ParticipantAckDecision, apply_participant_ack,
    apply_participant_ack_with_obligations,
};
pub use record_admission::{
    CommittedOrdinaryRecord, RecordAdmissionCommit, RecordAdmissionDecision,
    RecordAdmissionDrainFirst, RecordAdmissionFailure, RecordAdmissionFault,
    RecordAdmissionPersistenceParts, RecordAdmissionPrestate, RecordAdmissionRefusal,
    UnchangedRecordAdmission, apply_record_admission, classify_record_admission_binding,
};
