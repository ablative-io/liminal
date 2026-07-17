//! Participant outcomes that are deliberately outside the network registry.
//!
//! Semantic wire outcomes are re-exported from [`crate::wire`]. Local SDK,
//! startup/configuration, accepted-socket, and internal-recovery outcomes live
//! here because the frozen contract explicitly excludes them from the stable
//! `0x0100..=0x0124` server registry.
//!
//! `docs/design/LP-EXTRACTION-GOAL.md` Fix 2 removes the fixed occurrence-array
//! machinery. Consequently the occurrence-array-only retention and corruption
//! reason arms are intentionally absent from the startup/internal enums below.

mod internal;
mod keepalive;
mod local;
mod parking;
mod startup;

#[cfg(test)]
mod internal_tests;
#[cfg(test)]
mod keepalive_tests;
#[cfg(test)]
mod local_tests;
#[cfg(test)]
mod parking_tests;
#[cfg(test)]
mod startup_tests;

pub use internal::{
    BindingRecoveryCommitted, BindingRecoveryFinalization, CandidatePhase, ClaimCounter,
    ParticipantStateCorrupt, ParticipantStateCorruptReason, UncleanServerRestartCause,
};
pub use keepalive::{
    AcceptedSocketKeepaliveReason, KeepaliveCertificationFailed, KeepaliveField, KeepaliveOption,
    KeepalivePhase, KeepaliveReadbackMismatch, NumericKeepaliveOption, PlatformName,
    StartupKeepaliveReason,
};
pub use local::{
    CredentialRecoveryLost, ParkOrderCounter, ReconnectDelayResult, ReconnectRequiredEvent,
    ReconnectState, RecordAdmissionOperation, RecordAdmissionUnknown,
    SdkObserverParkCapacityExceeded, SdkParkOrderExhausted, SdkParticipantRequestTooLarge,
};
pub use parking::{
    CheckedMultiplyOverflow, CheckedOperation, HandshakeSizeOperands, ParkingLimitField,
    ParkingShapeViolation, ParticipantParkingConfigurationInvalid,
    ParticipantRecoveryHandshakeTooLarge, RecoveryHandshakeDimension,
    SdkParkingCapacityIncompatible,
};
pub use startup::{
    CapabilityLimitField, ConnectionIncarnationExhausted,
    ParticipantCapabilityConfigurationInvalid, ParticipantRetentionCapacityInvalid,
};

pub use crate::wire::{
    AckCommitted, AckGap, AckNoOp, AckRegression, AttachBound, AttachMarkerProof,
    AttemptTokenBodyConflict, BindingRequiredEnvelope, BindingStateView, ClosureCapacityReason,
    ClosureCheckedEnvelope, ClosureRefusalReason, ClosureSnapshot, CommonStaleAuthorityEnvelope,
    ConnectionConversationBindingOccupied, ConnectionConversationCapacityExceeded,
    ConversationOrderExhausted, ConversationSequenceExhausted, DetachCommitted, DetachInProgress,
    DetachStaleAuthority, DetachedCause, DiedCause, EnrollBound, EnrollmentKnown,
    EnrollmentReceiptCapacityScope, IdentityCapacityExceeded, InvalidObserverEpoch,
    InvalidObserverEpochList, LeaveCommitted, LeaveStaleAuthority, MarkerAckCommitted,
    MarkerAckProof, MarkerClosureCapacityExceeded, MarkerMismatch, MarkerMismatchBody,
    MarkerNotDelivered, MarkerProofRequest, NoBinding, ObserverBackpressure,
    ObserverBackpressureState, ObserverProgressStatus, ObserverRecoveryAccepted,
    OrderAllocatingEnvelope, ParticipantDelivery, ParticipantRecord, ParticipantReferenceEnvelope,
    ParticipantTransportRejected, ParticipantUnknown, ReceiptCapacityExceeded, ReceiptExpired,
    ReceiptReplay, RecordCommitted, RecordTooLarge, Retired, SequenceAllocatingEnvelope,
    SequenceBudget, ServerPush, ServerValue, StaleAuthority, StaleOrUnknownReceipt,
    TerminalizedDetachCell, TransportRejectionReason,
};
