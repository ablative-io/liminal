//! Typed participant lifecycle state and transitions.
//!
//! The deviations here are mandated by `docs/design/LP-EXTRACTION-GOAL.md`:
//! detach cells retain a terminalized token after attach so the old binding
//! epoch remains producible, and cursor-progress facts are keyed per
//! participant rather than stored in the contract's fixed occurrence array.
//! The array cannot represent two participants advancing over the same retained
//! suffix, so completion ordering is instead enforced by typed transitions and
//! tests.

mod binding;
mod cursor_facts;
mod detach;
mod edge;
mod lookup;
mod membership;

#[cfg(test)]
mod cursor_facts_tests;
#[cfg(test)]
mod detach_tests;
#[cfg(test)]
mod edge_tests;
#[cfg(test)]
mod lookup_tests;

pub use binding::{
    ActiveBinding, AdmissionOrder, BindingState, BindingTerminalKind, PendingFinalization,
};
pub use cursor_facts::{
    CursorFactEncodeError, CursorProgressFact, CursorProgressFacts, CursorProgressKey,
};
pub use detach::{
    CommittedDetach, DetachCell, DetachReplayError, DetachVerificationError, EmptyDetach,
    PendingDetach, TerminalizedDetach, VerifiedCommittedDetach, VerifiedDetachRequest,
    VerifiedPendingDetach, VerifiedTerminalizedDetach, commit_detach, complete_pending_detach,
    start_blocked_detach,
};
pub use edge::{
    ClosureDebt, ClosureState, CursorFateSuccessor, CursorProgressContinuous, CursorProgressMarker,
    DebtCompletion, DetachedAttachRefusal, DetachedCredentialRecovery, DetachedCursorRelease,
    DetachedMarkerRelease, Event, KClaimBackedDetachedLeave, LeaveOnlyEdge, MarkerDelivery,
    ObserverProjection, ParticipantCursorProgress, PhysicalCompaction,
    ProjectionCompactionSuccessor, StoredEdge,
};
pub use lookup::{
    DetachLookupResult, LeaveLookupResult, LeaveSecretProof, PresentedIdentity, lookup_detach,
    lookup_leave,
};
pub use membership::{
    EnrollmentFingerprint, IdentityState, LeaveFingerprint, LiveMember, RetiredIdentity,
    RetirementError,
};
