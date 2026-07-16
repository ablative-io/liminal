//! Typed participant lifecycle state and transitions.
//!
//! The deviations here are mandated by `docs/design/LP-EXTRACTION-GOAL.md`:
//! detach cells retain a terminalized token after attach so the old binding
//! epoch remains producible, and cursor-progress facts are keyed per
//! participant rather than stored in the contract's fixed occurrence array.
//! The array cannot represent two participants advancing over the same retained
//! suffix, so completion ordering is instead enforced by typed transitions and
//! tests.

mod attach;
mod binding;
mod cursor_facts;
mod detach;
mod edge;
mod enrollment;
mod lookup;
mod membership;

#[cfg(test)]
mod attach_tests;
#[cfg(test)]
mod binding_tests;
#[cfg(test)]
mod cursor_facts_tests;
#[cfg(test)]
mod detach_tests;
#[cfg(test)]
mod edge_tests;
#[cfg(test)]
mod enrollment_tests;
#[cfg(test)]
mod lookup_tests;
#[cfg(test)]
mod membership_tests;

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
