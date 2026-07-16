//! Typed participant lifecycle state and transitions.
//!
//! The deviations here are mandated by `docs/design/LP-EXTRACTION-GOAL.md`:
//! detach cells retain a terminalized token after attach so the old binding
//! epoch remains producible, and cursor-progress facts are keyed per
//! participant rather than stored in the contract's fixed occurrence array.
//! The array cannot represent two participants advancing over the same retained
//! suffix, so completion ordering is instead enforced by typed transitions and
//! tests.

mod admission;
mod attach;
mod binding;
mod closure_accounting;
mod cursor_facts;
mod detach;
mod edge;
mod enrollment;
mod enrollment_closure;
mod lookup;
mod membership;
mod observer_recovery;
mod operations;
mod storage;

#[cfg(test)]
mod attach_tests;
#[cfg(test)]
mod binding_tests;
#[cfg(test)]
mod closure_accounting_tests;
#[cfg(test)]
mod cursor_facts_tests;
#[cfg(test)]
mod detach_tests;
#[cfg(test)]
mod edge_tests;
#[cfg(test)]
mod enrollment_closure_tests;
#[cfg(test)]
mod enrollment_tests;
#[cfg(test)]
mod lookup_tests;
#[cfg(test)]
mod membership_tests;
#[cfg(test)]
mod observer_recovery_tests;
#[cfg(test)]
mod storage_tests;

pub use admission::{
    BindingSlotDecision, BindingSlotOccupancy, CapacityCounter, CapacityCounterInvariantError,
    ConnectionConversationCapacityCommit, ConnectionConversationTracking,
    CredentialAttachCapacityCommit, CredentialAttachCapacityCounters,
    CredentialAttachCapacityDecision, EnrollmentCapacityCommit, EnrollmentCapacityCounters,
    EnrollmentCapacityDecision, FreshParticipantCapacityCounter,
    FreshParticipantCapacityCounterInvariantError, ObserverCheckedOperation, ObserverFloorDecision,
    ObserverFloorPermit, OrderAdmissionError, OrderAllocation, OrderClaims,
    OrderClaimsInvariantError, OrderHigh, OrderLedger, OrderLedgerInvariantError,
    RecordSizeDecision, RecordSizePermit, RecoverySequenceReserve,
    ResultingEnrollmentCapacityCounters, ResultingOrderClaims, ResultingSequenceState,
    SemanticConnectionCapacityDecision, SequenceAdmission, SequenceAdmissionError, SequenceClaims,
    SequenceLedger, SequenceLedgerInvariantError, admit_sequence, allocate_order,
    check_observer_floor, check_record_size, select_credential_attach_binding_slot,
    select_credential_attach_capacity, select_enrollment_binding_slot, select_enrollment_capacity,
    select_semantic_connection_capacity,
};
pub use attach::{
    AttachCommit, AttachCommitError, AttachCommitParameters, AttachTransition,
    AttachVerificationError, VerifiedAttachCommit, commit_attach,
};
pub use binding::{
    ActiveBinding, AdmissionOrder, BindingState, BindingTerminalDisposition, BindingTerminalKind,
    CommittedBindingTerminal, CommittedBindingTerminalPosition, CommittedDetachedTerminal,
    CommittedDiedTerminal, DetachedBindingTransition, DiedBindingTransition,
    PendingBindingTerminalPosition, PendingDetachedFinalization, PendingDiedFinalization,
    PendingFinalization,
};
pub use closure_accounting::{
    ClosureAccounting, ClosureAccountingError, RecoveryFenceDecision, RecoveryFencePermit,
    RemainingClosureDecision, RemainingClosurePermit, RequiredCapacityPlan,
    RequiredCapacityPlanError, check_recovery_fence, check_remaining_closure,
};
pub use cursor_facts::{
    BoundParticipantCursor, CumulativeAckAuthorizationError, CumulativeAckOutcome,
    CursorEpisodeBuildError, CursorFactEncodeError, CursorProgressFact, CursorProgressFacts,
    CursorProgressKey, NonzeroDebtCursorEpisode,
};
pub use detach::{
    CommittedDetach, CommittedDetachTransition, DetachCell, DetachCommitError, DetachReplayError,
    DetachVerificationError, EmptyDetach, PendingDetach, PendingDetachTransition,
    PendingDrainDecision, PendingReplay, PendingReplayError, PendingReplayRequest,
    TerminalizedDetach, VerifiedCommittedDetach, VerifiedDetachRequest, VerifiedPendingDetach,
    VerifiedTerminalizedDetach, commit_detach, complete_pending_detach, start_blocked_detach,
};
pub use edge::{
    ClosureDebt, ClosureState, CursorFateSuccessor, CursorProgressContinuous, CursorProgressMarker,
    DebtCompletion, DetachedAttachRefusal, DetachedCredentialRecovery, DetachedCursorRelease,
    DetachedMarkerRelease, Event, FencedAttachCommit, KClaimBackedDetachedLeave, LeaveOnlyEdge,
    MarkerDelivery, ObserverProjection, OrdinaryBindingAuthority, OrdinaryBindingFate,
    OrdinaryDetachedAttachAdmission, ParticipantCursorProgress, PendingRecoveredCursorRelease,
    PhysicalCompaction, ProjectionCompactionSuccessor, RecoveredBindingFate,
    RecoveredBindingFateTransition, RecoveredCursorRelease, StoredEdge,
};
pub use enrollment::{
    AllocatedParticipantSlot, AttachedLifecycleRecord, AttachedRecordPosition, EnrollmentCommit,
    EnrollmentCommitError, EnrollmentCommitParameters, ParticipantSlotAllocationError,
    ParticipantSlotAllocatorProof, commit_enrollment,
};
pub use enrollment_closure::{
    InitialEnrollmentClosureError, InitialEnrollmentClosureInput,
    InitialEnrollmentClosureProjection, PlannedEnrollmentMarker, RecoveryQuartetStatus,
    project_initial_enrollment_closure,
};
pub use lookup::{
    AttachSecretProof, BindingRequiredLookupResult, CredentialAttachLiveReceipt,
    CredentialAttachLookupResult, CredentialAttachProvenance, CredentialAttachTokenPhase,
    DetachLookupContext, DetachLookupResult, DetachTokenResolution, EnrollmentLiveReceipt,
    EnrollmentLookupResult, EnrollmentProvenance, EnrollmentTokenPhase, LeaveLookupResult,
    LeaveSecretProof, ParticipantBindingRequest, PresentedIdentity, ResolvedIdentity,
    lookup_binding_required, lookup_credential_attach, lookup_detach, lookup_enrollment,
    lookup_leave,
};
pub use membership::{
    EnrollmentFingerprint, IdentityState, LeaveCommitError, LeaveCommitParameters,
    LeaveFingerprint, LeaveVerificationError, LiveMember, LiveMemberRestore,
    MembershipInvariantError, NoInterveningTuplePlannerProof, NoInterveningTupleProof,
    NoInterveningTupleProofError, PendingLeaveCommitParameters, RetiredIdentity, RetirementError,
    VerifiedLeaveRequest, commit_leave, commit_pending_leave,
};
pub use observer_recovery::{
    ObserverRecoveryArm, ObserverRecoveryCommit, ObserverRecoveryDecision, apply_observer_recovery,
};
pub use operations::{
    InitialEnrollmentCommitValues, InitialEnrollmentOperationCommit,
    InitialEnrollmentOperationDecision, InitialEnrollmentOperationFault,
    InitialEnrollmentOperationInput, MarkerAckCommit, MarkerAckCommitError, MarkerAckDecision,
    MarkerProofDecision, MarkerProofInput, MarkerProofPermit, MarkerProofState,
    NonzeroAckEpisodePosition, NonzeroParticipantAckCommit, NonzeroParticipantAckCommitError,
    NonzeroParticipantAckDecision, NonzeroParticipantAckInvariantError, ParticipantAckCommit,
    ParticipantAckCommitError, ParticipantAckDecision, ReceiptDeadlineError, ReceiptDeadlines,
    apply_initial_enrollment, apply_marker_ack, apply_nonzero_participant_ack,
    apply_participant_ack, select_marker_proof,
};
pub use storage::{
    BindingFateTerminalRestore, BindingStateRestore, ClosureStateRestore,
    CommittedBindingTerminalRestore, CursorEpisodeRestore, DebtCompletionRestore,
    DetachCellRestore, DetachedCredentialRecoveryRestore, DetachedCursorReleaseProvenanceRestore,
    DetachedMarkerReleaseRestore, FencedAttachCommitRestore, LeaveCommittedRestore,
    LiveIdentityRestore, MarkerCursorProgressRestore, MarkerDeliveryRestore,
    OrdinaryBindingAuthorityRestore, OrdinaryBindingFateRestore, ParticipantLifecycleRestore,
    PendingFinalizationRestore, PendingRecoveredCursorReleaseRestore, RecoveredBindingFateRestore,
    RecoveredStorageCompletionRestore, RestoredBindingFateTerminal, RestoredParticipantLifecycle,
    RetiredIdentityRestore, StorageRestoreError, StoredEdgeRestore,
};
