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
mod aggregate_commit;
mod attach;
mod binding;
mod claim_frontier;
mod closure_accounting;
mod conversation;
mod conversation_codec;
mod cursor_facts;
mod detach;
mod edge;
mod enrollment;
mod enrollment_closure;
mod incarnation;
mod lookup;
mod membership;
mod observer_recovery;
mod operation_event;
mod operations;
mod storage;

#[cfg(test)]
mod aggregate_commit_tests;
#[cfg(test)]
mod attach_tests;
#[cfg(test)]
mod binding_tests;
#[cfg(test)]
mod claim_frontier_tests;
#[cfg(test)]
mod closure_accounting_tests;
#[cfg(test)]
mod conversation_tests;
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
mod incarnation_tests;
#[cfg(test)]
mod lookup_tests;
#[cfg(test)]
mod membership_tests;
#[cfg(test)]
mod observer_recovery_tests;
#[cfg(test)]
mod observer_recovery_transaction_tests;
#[cfg(test)]
mod operation_event_tests;
#[cfg(test)]
mod storage_tests;
#[cfg(test)]
mod test_support;

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
    ResultingEnrollmentCapacityCounters, SemanticConnectionCapacityDecision, SequenceAdmission,
    SequenceAdmissionError, SequenceClaims, SequenceLedger, SequenceLedgerInvariantError,
    check_observer_floor, check_record_size, select_credential_attach_binding_slot,
    select_credential_attach_capacity, select_enrollment_binding_slot, select_enrollment_capacity,
    select_semantic_connection_capacity,
};
use admission::{admit_sequence, allocate_order};
pub use aggregate_commit::{
    AggregateOperationCommit, AggregateOperationDecision, AggregateOperationFault,
    AggregateOperationFaultReason, AggregateOperationRefusal, AggregateOperationResult,
    decide_attached_operation, decide_detached_operation, decide_enrolled_operation,
    decide_left_operation, decide_nonzero_debt_ack_operation,
    decide_ordinary_binding_fate_operation, decide_recovered_binding_fate_operation,
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
pub use claim_frontier::{
    ActiveIdentityRanks, BindingTerminalOwner, ClaimFrontierCounter, ClaimFrontierError,
    ClaimFrontierInvalidReason, ClaimFrontiers, ClaimFrontiersRestore, ExitProductRange,
    ExitProductRangeRestore, FrontierBinding, FrontierParticipant, HistoricalCausalFactRestore,
    HistoricalMarkerDeliveryFactRestore, ImmutableOrderCandidateMajor,
    ImmutableOrderCandidateMajorRestore, ImmutableSequenceCandidate,
    InitialEnrollmentFrontierCommit, InitialEnrollmentFrontierError,
    InitialEnrollmentFrontierFailure, MarkerCandidateAuthority, MarkerProvenance,
    MarkerSequenceOwner, MovableOrderClaim, MovableSequenceClaim, OrderClaimFrontier,
    OrderClaimFrontierRestore, OrderDirectOwner, PrepareLeaveAuthorityError,
    RecoveryClaimProvenance, RecoveryOrderActiveBindingRestore, RecoveryOrderBlock,
    RecoveryOrderBlockRestore, RecoverySequenceBlock, RecoverySequenceBlockRestore,
    RecoverySequenceTerminalRestore, ReplacementTerminalProductRange,
    ReplacementTerminalProductRangeRestore, RetainedCausalRecord, RetainedCausalRecordKind,
    SequenceClaimFrontier, SequenceClaimFrontierRestore, SequenceDirectOwner, SequenceProductClass,
    SequenceProductRanges, SequenceProductRangesRestore, TerminalProductRange,
    TerminalProductRangeRestore, TerminalProductSource,
};
pub use closure_accounting::{
    ClosureAccounting, ClosureAccountingError, RecoveryFenceDecision, RecoveryFencePermit,
    RemainingClosureDecision, RemainingClosurePermit, RequiredCapacityPlan,
    RequiredCapacityPlanError, check_recovery_fence, check_remaining_closure,
};
pub use conversation::{
    ConversationCommit, ConversationDecision, ConversationEvent, ConversationEventDecodeError,
    ConversationGenesis, ConversationRefusal, ConversationRefusalReason, ConversationReplayError,
    ConversationReplayFailure, ParticipantConversation,
};
pub use cursor_facts::{
    BoundParticipantCursor, CumulativeAckAuthorizationError, CumulativeAckOutcome,
    CursorEpisodeBuildError, CursorFactEncodeError, CursorProgressFact, CursorProgressFacts,
    CursorProgressKey, NonzeroDebtCursorEpisode, RecipientAckObligations,
    RecipientAckObligationsContextError, RecipientAckObligationsError,
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
    AllocatedParticipantSlot, AttachedLifecycleRecord, AttachedRecordPosition, BindingOrigin,
    EnrollmentCommit, EnrollmentCommitError, EnrollmentCommitParameters,
    ParticipantSlotAllocationError, ParticipantSlotAllocatorProof, commit_enrollment,
};
pub use enrollment_closure::{
    InitialEnrollmentClosureError, InitialEnrollmentClosureInput,
    InitialEnrollmentClosureProjection, PlannedEnrollmentMarker, RecoveryQuartetStatus,
    project_initial_enrollment_closure,
};
pub use incarnation::{
    ConnectionIncarnationAllocation, ConnectionIncarnationAllocationDecision,
    ConnectionIncarnationAllocator, ConnectionIncarnationAllocatorRestore,
    ConnectionIncarnationAllocatorRestoreError, ConnectionOrdinalExhaustion,
    ConnectionOrdinalExhaustionCommit, ConnectionOrdinalExhaustionReplay,
    DurableIncarnationReferences, DurableIncarnationReferencesError, ServerIncarnationExhaustion,
    ServerIncarnationFsyncIntent, ServerIncarnationStartupDecision,
    allocate_connection_incarnation, prepare_server_incarnation_startup,
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
    EnrollmentFingerprint, IdentityState, LeaveCommit, LeaveCommitError, LeaveCommitParameters,
    LeaveFingerprint, LeaveVerificationError, LiveMember, LiveMemberRestore,
    MembershipInvariantError, PendingLeaveCommitParameters, PreparedLeaveAuthority,
    RetiredIdentity, RetirementError, VerifiedLeaveRequest, commit_leave, commit_pending_leave,
};
pub use observer_recovery::{
    ObserverProgressAdvanceDecision, ObserverProgressAdvanceError,
    ObserverProgressAdvanceTransaction, ObserverProgressTrackDecision, ObserverProgressTrackError,
    ObserverProgressTrackTransaction, ObserverRecoveryAggregate,
    ObserverRecoveryAggregateRestoreError, ObserverRecoveryArm, ObserverRecoveryCommit,
    ObserverRecoveryDecision, ObserverRecoveryTransaction, ObserverRecoveryTransactionDecision,
};
pub use operation_event::{
    AttachedOperation, BindingFateOperation, ConversationOperation, DetachedOperation,
    EnrolledOperation, LeftOperation, NonzeroDebtAckOperation,
};
pub use operations::{
    AttachFrontierCharges, CommittedOrdinaryRecord, InitialEnrollmentCommitValues,
    InitialEnrollmentOperationCommit, InitialEnrollmentOperationDecision,
    InitialEnrollmentOperationFault, InitialEnrollmentOperationInput, LiveFrontierCommit,
    LiveFrontierError, LiveFrontierFailure, LiveFrontierOwner, LiveFrontierResult, LiveLeaveCommit,
    LiveLeaveError, MarkerAckCommit, MarkerAckCommitError, MarkerAckDecision,
    MarkerDeliveryProjection, MarkerDrainCommit, MarkerDrainError, MarkerProofDecision,
    MarkerProofInput, MarkerProofPermit, MarkerProofState, NonzeroAckEpisodePosition,
    NonzeroParticipantAckCommit, NonzeroParticipantAckCommitError, NonzeroParticipantAckDecision,
    NonzeroParticipantAckInvariantError, OrdinaryProjectionError, OrdinaryProjectionLimits,
    OrdinaryRecordDrainFirst, OrdinaryRecordProjectionDecision, OrdinaryRecordProjectionFailure,
    OrdinaryRecordProjectionInput, ParticipantAckCommit, ParticipantAckCommitError,
    ParticipantAckDecision, ProjectedOrdinaryRecord, ReceiptDeadlineError, ReceiptDeadlines,
    RecordAdmissionCommit, RecordAdmissionDecision, RecordAdmissionDrainFirst,
    RecordAdmissionFailure, RecordAdmissionFault, RecordAdmissionPersistenceParts,
    RecordAdmissionPrestate, RecordAdmissionRefusal, RetainedRecordCharge,
    UnchangedRecordAdmission, apply_attach_frontier, apply_detach_frontier,
    apply_enrollment_frontier, apply_initial_enrollment, apply_marker_ack,
    apply_marker_ack_frontier, apply_nonzero_participant_ack,
    apply_nonzero_participant_ack_frontier, apply_nonzero_participant_ack_with_obligations,
    apply_participant_ack, apply_participant_ack_frontier, apply_participant_ack_with_obligations,
    apply_record_admission, classify_record_admission_binding, commit_pending_leave_frontier,
    commit_settled_leave_frontier, drain_next_marker, select_marker_proof,
};
pub use storage::{
    BindingFateTerminalRestore, BindingStateRestore, ClosureStateRestore,
    CommittedBindingTerminalRestore, ConversationStateRestoreError, CursorEpisodeRestore,
    DebtCompletionRestore, DetachCellRestore, DetachedCredentialRecoveryRestore,
    DetachedCursorReleaseProvenanceRestore, DetachedMarkerReleaseRestore,
    FencedAttachCommitRestore, LeaveCommittedRestore, LiveIdentityRestore,
    MarkerCursorProgressRestore, MarkerDeliveryRestore, OrdinaryBindingAuthorityRestore,
    OrdinaryBindingFateRestore, ParticipantConversationRestore, ParticipantConversationState,
    ParticipantLifecycleRestore, PendingFinalizationRestore, PendingRecoveredCursorReleaseRestore,
    RecoveredBindingFateRestore, RecoveredStorageCompletionRestore, RestoredBindingFateTerminal,
    RestoredParticipantLifecycle, RetiredIdentityRestore, StorageRestoreError, StoredEdgeRestore,
};
