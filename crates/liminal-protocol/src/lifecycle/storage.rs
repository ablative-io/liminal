//! Validated durable-state restoration for participant lifecycle typestates.
//!
//! Storage serialization necessarily crosses the crate's compile-time state
//! boundary. These capsules retain the predecessor data needed to rebuild
//! opaque authorities, then validate every identity, generation, epoch, and
//! paired-state invariant before returning executable lifecycle values.

use alloc::vec::Vec;

use crate::algebra::WideResourceVector;
use crate::outcome::ParticipantStateCorruptReason;
use crate::wire::{
    AttachSecret, BindingEpoch, CloseCause, ConversationId, DeliverySeq, DetachAttemptToken,
    Generation, LeaveAttemptToken, LeaveCommitted, ObserverEpoch, ParticipantId, TransactionOrder,
};

use super::{
    ActiveBinding, AdmissionOrder, BindingOrigin, BindingState, BoundParticipantCursor,
    ClosureDebt, ClosureState, CommittedBindingTerminal, DebtCompletion, DetachCell,
    DetachedCredentialRecovery, DetachedMarkerRelease, EnrollmentFingerprint, Event,
    FencedAttachCommit, IdentityState, LeaveFingerprint, LiveMember, MarkerDelivery,
    NonzeroDebtCursorEpisode, ObserverProjection, OrdinaryBindingAuthority, OrdinaryBindingFate,
    ParticipantCursorProgress, PendingFinalization, PendingRecoveredCursorRelease,
    PhysicalCompaction, RecoveredBindingFate, RecoveredBindingFateTransition, RetiredIdentity,
    StoredEdge,
    binding::{restore_committed_terminal, restore_pending_finalization},
    claim_frontier::{MarkerRecordOccurrence, ValidatedMarkerRecord},
    cursor_facts::{CursorProgressFact, CursorProgressKey},
};

#[cfg(test)]
use super::{
    ClaimFrontiers, ClaimFrontiersRestore, EmptyDetach, FrontierBinding, LiveMemberRestore,
    OrderLedger, SequenceLedger,
    claim_frontier::{
        HistoricalCausalAuthority, MarkerRecordRequest, ValidatedConversationHistory,
    },
    detach::{
        restore_committed_detach, restore_pending_detach, restore_terminalized_detach,
        validate_pending_pair,
    },
};

/// A durable lifecycle capsule failed a protocol invariant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageRestoreError {
    /// A raw committed binding terminal has an impossible cause or restart suffix.
    CommittedBindingTerminal,
    /// A pending terminal has an impossible cause or restart suffix.
    PendingFinalization,
    /// A binding slot names another live identity or generation.
    BindingAuthority,
    /// Live membership disagrees with its decoded terminal identity or generation.
    MembershipInvariant,
    /// A raw permanent Leave result is internally inconsistent.
    LeaveResult,
    /// A retired identity disagrees with its permanent Leave result.
    RetiredIdentity,
    /// A detach-cell variant is internally inconsistent.
    DetachCell,
    /// Binding, membership, terminal history, and detach cell do not form one state.
    DetachBindingPair,
    /// Cursor-episode floor, participant, or fact state is inconsistent.
    CursorEpisode,
    /// A closure state paired a zero debt vector with a stored edge.
    ClosureDebt,
    /// A stored edge disagrees with the predecessor provenance in its capsule.
    StoredEdgeProvenance,
}

/// Failure while jointly restoring claim frontiers and their current closure edge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConversationStateRestoreError {
    /// Numeric, causal, or cross-counter claim-frontier validation failed.
    ClaimFrontier(ParticipantStateCorruptReason),
    /// Raw lifecycle storage disagreed with its typed predecessor authority.
    Storage(StorageRestoreError),
}

/// Crate-internal jointly restored claim frontiers and exact current closure state.
///
/// This value can execute restored edge authority. It is deliberately absent
/// from the public storage surface: external callers may deserialize inert
/// restore data, but only protocol-owned replay may promote it to executable
/// state.
#[derive(Debug, PartialEq, Eq)]
#[cfg(test)]
pub(super) struct RestoredConversationState {
    frontiers: ClaimFrontiers,
    closure: ClosureState,
}

/// Complete crate-internal participant-conversation snapshot.
///
/// This component form is intentionally not public API. Allowing a caller to
/// combine a member restored from one history with a binding-origin capsule
/// emitted by another would turn a valid producer proof into spliceable
/// executable authority. Public cold restoration must instead replay one
/// consuming protocol event history, which owns every predecessor and
/// successor together.
#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ParticipantConversationRestore<EF, V, LF, D> {
    /// Complete live and retired participant states.
    pub participants: Vec<ParticipantLifecycleRestore<EF, V, LF, D>>,
    /// Coupled raw claim frontiers.
    pub frontiers: ClaimFrontiersRestore,
    /// Aggregate delivery-sequence ledger.
    pub sequence_ledger: SequenceLedger,
    /// Aggregate transaction-order ledger.
    pub order_ledger: OrderLedger,
    /// Current closure state.
    pub closure: ClosureStateRestore,
}

/// Fully validated crate-internal participant-conversation state.
///
/// This state can execute restored edge authority, so only the protocol-owned
/// replay layer may produce or consume it.
#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ParticipantConversationState<EF, V, LF, D> {
    participants: Vec<RestoredParticipantLifecycle<EF, V, LF, D>>,
    frontiers: ClaimFrontiers,
    closure: ClosureState,
}

#[cfg(test)]
impl<EF, V, LF, D> ParticipantConversationState<EF, V, LF, D> {
    /// Borrows every restored participant and tombstone.
    #[must_use]
    pub(super) fn participants(&self) -> &[RestoredParticipantLifecycle<EF, V, LF, D>] {
        &self.participants
    }

    /// Borrows the finalized claim frontiers.
    #[must_use]
    pub(super) const fn frontiers(&self) -> &ClaimFrontiers {
        &self.frontiers
    }

    /// Returns the exact closure state.
    #[must_use]
    pub(super) const fn closure(&self) -> ClosureState {
        self.closure
    }
}

#[cfg(test)]
impl RestoredConversationState {
    /// Borrows the fully finalized coupled claim frontiers.
    #[must_use]
    pub(crate) const fn frontiers(&self) -> &ClaimFrontiers {
        &self.frontiers
    }

    /// Returns the exact restored closure debt and stored edge.
    #[must_use]
    pub(crate) const fn closure(&self) -> ClosureState {
        self.closure
    }

    /// Consumes the aggregate into the two values persisted by a server binding.
    #[must_use]
    pub(crate) fn into_parts(self) -> (ClaimFrontiers, ClosureState) {
        (self.frontiers, self.closure)
    }
}

/// Restores claim ownership and its marker-derived edge as one provenance unit.
///
/// Numeric/candidate validation runs first. At most one non-cloneable retained
/// marker token is then consumed to restore the raw closure edge, after which
/// frontier recovery claims are finalized against that exact typed edge.
/// This standalone form intentionally rejects raw compacted causal history and
/// ordinary-binding cursor authority; those require protocol-owned event replay
/// to establish owned lifecycle provenance first. It is crate-private because
/// accepting caller-authored snapshots here would upgrade inert storage bytes
/// into executable edge authority.
///
/// # Errors
///
/// Returns [`ConversationStateRestoreError::ClaimFrontier`] for malformed
/// numeric/candidate ownership or [`ConversationStateRestoreError::Storage`]
/// for a missing, ambiguous, cross-conversation, or context-mismatched marker
/// record and every other stored-edge provenance failure.
#[cfg(test)]
pub(super) fn restore_conversation_state(
    frontier_restore: ClaimFrontiersRestore,
    sequence_ledger: SequenceLedger,
    order_ledger: OrderLedger,
    closure_restore: &ClosureStateRestore,
) -> Result<RestoredConversationState, ConversationStateRestoreError> {
    let history = ValidatedConversationHistory::empty();
    restore_conversation_with_history(
        frontier_restore,
        sequence_ledger,
        order_ledger,
        closure_restore,
        &history,
    )
}

#[cfg(test)]
fn restore_conversation_with_history(
    frontier_restore: ClaimFrontiersRestore,
    sequence_ledger: SequenceLedger,
    order_ledger: OrderLedger,
    closure_restore: &ClosureStateRestore,
    history: &ValidatedConversationHistory,
) -> Result<RestoredConversationState, ConversationStateRestoreError> {
    let mut prevalidated = ClaimFrontiers::prevalidate_with_history(
        frontier_restore,
        sequence_ledger,
        order_ledger,
        history,
    )
    .map_err(ConversationStateRestoreError::ClaimFrontier)?;
    let marker_request = closure_restore.marker_record_request();
    let ordinary_request = closure_restore.ordinary_binding_request();
    let closure = match (marker_request, ordinary_request) {
        (Some(marker_request), None) => {
            let record = prevalidated.take_marker_record(marker_request).ok_or(
                ConversationStateRestoreError::Storage(StorageRestoreError::StoredEdgeProvenance),
            )?;
            (*closure_restore)
                .restore_with_marker_record(prevalidated.conversation_id(), record)
                .map_err(ConversationStateRestoreError::Storage)?
        }
        (None, Some(binding)) => {
            let origin = history
                .ordinary_origin(
                    binding.conversation_id,
                    binding.participant_id,
                    binding.binding_epoch,
                )
                .ok_or(ConversationStateRestoreError::Storage(
                    StorageRestoreError::StoredEdgeProvenance,
                ))?;
            (*closure_restore)
                .restore_with_binding_origin(origin)
                .map_err(ConversationStateRestoreError::Storage)?
        }
        (None, None) => (*closure_restore)
            .restore()
            .map_err(ConversationStateRestoreError::Storage)?,
        (Some(_), Some(_)) => {
            return Err(ConversationStateRestoreError::Storage(
                StorageRestoreError::StoredEdgeProvenance,
            ));
        }
    };
    let current_edge = match closure {
        ClosureState::Clear => None,
        ClosureState::Owed { edge, .. } => Some(edge),
    };
    let frontiers = prevalidated
        .finish(current_edge)
        .map_err(ConversationStateRestoreError::ClaimFrontier)?;
    Ok(RestoredConversationState { frontiers, closure })
}

/// Raw durable fields for one committed binding terminal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommittedBindingTerminalRestore {
    /// Binding authority that ended.
    pub binding: ActiveBinding,
    /// Cause stored with the terminal.
    pub cause: CloseCause,
    /// Immutable admission major.
    pub transaction_order: TransactionOrder,
    /// Durable lifecycle delivery sequence.
    pub delivery_seq: DeliverySeq,
}

impl CommittedBindingTerminalRestore {
    /// Validates and rebuilds the cause-partitioned committed terminal.
    ///
    /// # Errors
    ///
    /// Returns [`StorageRestoreError::CommittedBindingTerminal`] for an
    /// impossible cause/suffix combination.
    pub fn restore(self) -> Result<CommittedBindingTerminal, StorageRestoreError> {
        restore_committed_terminal(
            self.binding,
            self.cause,
            self.transaction_order,
            self.delivery_seq,
        )
        .ok_or(StorageRestoreError::CommittedBindingTerminal)
    }
}

/// Raw durable fields for one pending binding terminal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PendingFinalizationRestore {
    /// Binding authority that already ended.
    pub binding: ActiveBinding,
    /// Cause stored with the pending terminal.
    pub cause: CloseCause,
    /// Immutable reserved admission major.
    pub transaction_order: TransactionOrder,
}

impl PendingFinalizationRestore {
    /// Validates and rebuilds the cause-partitioned pending terminal.
    ///
    /// # Errors
    ///
    /// Returns [`StorageRestoreError::PendingFinalization`] when the cause
    /// cannot be pending or an unclean-restart suffix names another server.
    pub fn restore(self) -> Result<PendingFinalization, StorageRestoreError> {
        restore_pending_finalization(self.binding, self.cause, self.transaction_order)
            .ok_or(StorageRestoreError::PendingFinalization)
    }
}

/// Durable binding-fate terminal provenance, whether appended or still pending.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindingFateTerminalRestore {
    /// Terminal appended in the binding-fate transaction.
    Committed(CommittedBindingTerminalRestore),
    /// Binding fate committed but its terminal append remains pending.
    Pending(PendingFinalizationRestore),
}

/// Validated binding-fate terminal provenance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestoredBindingFateTerminal {
    /// Cause-partitioned committed terminal.
    Committed(CommittedBindingTerminal),
    /// Cause-partitioned pending terminal.
    Pending(PendingFinalization),
}

impl BindingFateTerminalRestore {
    /// Validates and rebuilds either durable terminal disposition.
    ///
    /// # Errors
    ///
    /// Returns [`StorageRestoreError`] for an impossible cause or restart suffix.
    pub fn restore(self) -> Result<RestoredBindingFateTerminal, StorageRestoreError> {
        match self {
            Self::Committed(value) => value.restore().map(RestoredBindingFateTerminal::Committed),
            Self::Pending(value) => value.restore().map(RestoredBindingFateTerminal::Pending),
        }
    }
}

impl RestoredBindingFateTerminal {
    const fn participant_id(self) -> ParticipantId {
        match self {
            Self::Committed(value) => value.participant_id(),
            Self::Pending(value) => value.participant_id(),
        }
    }

    const fn conversation_id(self) -> ConversationId {
        match self {
            Self::Committed(value) => value.conversation_id(),
            Self::Pending(value) => value.conversation_id(),
        }
    }

    const fn binding_epoch(self) -> BindingEpoch {
        match self {
            Self::Committed(value) => value.binding_epoch(),
            Self::Pending(value) => value.binding_epoch(),
        }
    }
}

/// Durable binding-slot representation with raw pending-terminal fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindingStateRestore {
    /// No binding authority or pending terminal.
    Detached,
    /// Live binding authority.
    Bound(ActiveBinding),
    /// Ended authority whose terminal append remains pending.
    PendingFinalization(PendingFinalizationRestore),
}

impl BindingStateRestore {
    #[cfg(test)]
    fn restore_for<EF>(self, member: &LiveMember<EF>) -> Result<BindingState, StorageRestoreError> {
        let state = match self {
            Self::Detached => BindingState::Detached,
            Self::Bound(binding) => BindingState::Bound(binding),
            Self::PendingFinalization(raw) => BindingState::PendingFinalization(raw.restore()?),
        };
        let authority_matches = match state {
            BindingState::Detached => true,
            BindingState::Bound(binding) => {
                binding.participant_id == member.participant_id()
                    && binding.conversation_id == member.conversation_id()
                    && binding.binding_epoch.capability_generation == member.generation()
            }
            BindingState::PendingFinalization(pending) => {
                pending.participant_id() == member.participant_id()
                    && pending.conversation_id() == member.conversation_id()
                    && pending.binding_epoch().capability_generation == member.generation()
            }
        };
        if authority_matches {
            Ok(state)
        } else {
            Err(StorageRestoreError::BindingAuthority)
        }
    }
}

/// Raw durable fields for the latest committed terminal retained by membership.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveIdentityRestore<EF> {
    /// Permanent participant identity/index.
    pub participant_id: ParticipantId,
    /// Owning conversation.
    pub conversation_id: ConversationId,
    /// Current credential generation.
    pub generation: Generation,
    /// Current attach secret.
    pub attach_secret: AttachSecret,
    /// Durable cumulative participant cursor.
    pub cursor: DeliverySeq,
    /// Permanent enrollment-token fingerprint.
    pub enrollment_fingerprint: EnrollmentFingerprint<EF>,
    /// Most recent committed binding terminal, if any.
    pub latest_terminal: Option<CommittedBindingTerminalRestore>,
}

impl<EF> LiveIdentityRestore<EF> {
    #[cfg(test)]
    fn restore(self) -> Result<LiveMember<EF>, StorageRestoreError> {
        let latest_terminal = self
            .latest_terminal
            .map(CommittedBindingTerminalRestore::restore)
            .transpose()?;
        LiveMember::restore(LiveMemberRestore {
            participant_id: self.participant_id,
            conversation_id: self.conversation_id,
            generation: self.generation,
            attach_secret: self.attach_secret,
            cursor: self.cursor,
            enrollment_fingerprint: self.enrollment_fingerprint,
            latest_terminal,
        })
        .map_err(|_| StorageRestoreError::MembershipInvariant)
    }
}

/// Raw fields of the canonical permanent `LeaveCommitted` result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaveCommittedRestore {
    /// Conversation containing the participant.
    pub conversation_id: ConversationId,
    /// Committing Leave token.
    pub leave_attempt_token: LeaveAttemptToken,
    /// Retired participant.
    pub participant_id: ParticipantId,
    /// Permanent retired generation.
    pub retired_generation: Generation,
    /// Active binding ended in the Leave transaction, if any.
    pub ended_binding_epoch: Option<BindingEpoch>,
    /// Earlier binding-terminal delivery sequence, if any.
    pub prior_terminal_delivery_seq: Option<DeliverySeq>,
    /// Durable `Left` delivery sequence.
    pub left_delivery_seq: DeliverySeq,
}

impl LeaveCommittedRestore {
    /// Rebuilds the canonical terminal Leave result.
    ///
    /// # Errors
    ///
    /// Returns [`StorageRestoreError::LeaveResult`] for an epoch-generation
    /// mismatch or a terminal sequence not strictly before `Left`.
    pub fn restore(self) -> Result<LeaveCommitted, StorageRestoreError> {
        LeaveCommitted::new(
            self.conversation_id,
            self.leave_attempt_token,
            self.participant_id,
            self.retired_generation,
            self.ended_binding_epoch,
            self.prior_terminal_delivery_seq,
            self.left_delivery_seq,
        )
        .ok_or(StorageRestoreError::LeaveResult)
    }
}

/// Complete raw durable tombstone fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetiredIdentityRestore<EF, V, LF> {
    /// Permanent participant identity/index.
    pub participant_id: ParticipantId,
    /// Conversation containing the tombstone.
    pub conversation_id: ConversationId,
    /// Permanent retired generation.
    pub retired_generation: Generation,
    /// Permanent enrollment-token fingerprint.
    pub enrollment_fingerprint: EnrollmentFingerprint<EF>,
    /// Permanent committing Leave token.
    pub leave_attempt_token: LeaveAttemptToken,
    /// Stored non-reversible Leave-request verifier.
    pub leave_request_verifier: V,
    /// Stored canonical Leave fingerprint.
    pub leave_fingerprint: LeaveFingerprint<LF>,
    /// Immutable transaction-order major of the permanent `Left` record.
    pub left_transaction_order: TransactionOrder,
    /// Complete canonical committed result.
    pub committed_result: LeaveCommittedRestore,
}

impl<EF, V, LF> RetiredIdentityRestore<EF, V, LF> {
    #[cfg(test)]
    fn restore(self) -> Result<RetiredIdentity<EF, V, LF>, StorageRestoreError> {
        let result = self.committed_result.restore()?;
        RetiredIdentity::restore(
            self.participant_id,
            self.conversation_id,
            self.retired_generation,
            self.enrollment_fingerprint,
            self.leave_attempt_token,
            self.leave_request_verifier,
            self.leave_fingerprint,
            self.left_transaction_order,
            result,
        )
        .map_err(|_| StorageRestoreError::RetiredIdentity)
    }
}

/// Exact four-state durable detach replay cell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DetachCellRestore<V> {
    /// No detach replay state.
    Empty,
    /// Accepted detach whose terminal append remains pending.
    Pending {
        /// Stable attempt token.
        token: DetachAttemptToken,
        /// Cell owner.
        participant_id: ParticipantId,
        /// Request generation.
        request_generation: Generation,
        /// Non-reversible exact-request verifier.
        request_verifier: V,
        /// Binding epoch ended by detach.
        committed_binding_epoch: BindingEpoch,
        /// Immutable binding-terminal admission position.
        admission_order: AdmissionOrder,
        /// Observer refusal epoch.
        refused_epoch: ObserverEpoch,
    },
    /// Committed detach retaining its exact terminal sequence.
    Committed {
        /// Stable attempt token.
        token: DetachAttemptToken,
        /// Cell owner.
        participant_id: ParticipantId,
        /// Request generation.
        request_generation: Generation,
        /// Non-reversible exact-request verifier.
        request_verifier: V,
        /// Binding epoch ended by detach.
        committed_binding_epoch: BindingEpoch,
        /// Committed `Detached` delivery sequence.
        detached_delivery_seq: DeliverySeq,
    },
    /// Post-attach replay state retaining the old binding epoch.
    Terminalized {
        /// Stable old detach token.
        token: DetachAttemptToken,
        /// Cell owner.
        participant_id: ParticipantId,
        /// Old request generation.
        request_generation: Generation,
        /// Non-reversible exact-request verifier.
        request_verifier: V,
        /// Old binding epoch ended by detach.
        committed_binding_epoch: BindingEpoch,
    },
}

impl<V> DetachCellRestore<V> {
    #[cfg(test)]
    fn restore(self) -> Result<DetachCell<V>, StorageRestoreError> {
        match self {
            Self::Empty => Ok(DetachCell::Empty(EmptyDetach)),
            Self::Pending {
                token,
                participant_id,
                request_generation,
                request_verifier,
                committed_binding_epoch,
                admission_order,
                refused_epoch,
            } => restore_pending_detach(
                token,
                participant_id,
                request_generation,
                request_verifier,
                committed_binding_epoch,
                admission_order,
                refused_epoch,
            )
            .map(DetachCell::Pending)
            .ok_or(StorageRestoreError::DetachCell),
            Self::Committed {
                token,
                participant_id,
                request_generation,
                request_verifier,
                committed_binding_epoch,
                detached_delivery_seq,
            } => restore_committed_detach(
                token,
                participant_id,
                request_generation,
                request_verifier,
                committed_binding_epoch,
                detached_delivery_seq,
            )
            .map(DetachCell::Committed)
            .ok_or(StorageRestoreError::DetachCell),
            Self::Terminalized {
                token,
                participant_id,
                request_generation,
                request_verifier,
                committed_binding_epoch,
            } => restore_terminalized_detach(
                token,
                participant_id,
                request_generation,
                request_verifier,
                committed_binding_epoch,
            )
            .map(DetachCell::Terminalized)
            .ok_or(StorageRestoreError::DetachCell),
        }
    }
}

/// Complete event-replayed participant state, with tombstone precedence in the type.
#[allow(
    clippy::large_enum_variant,
    reason = "the live storage capsule remains inline so its atomic slots cannot be restored separately"
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParticipantLifecycleRestore<EF, V, LF, D> {
    /// Live identity and its atomically paired binding and detach slots.
    Live {
        /// Membership and terminal history.
        identity: LiveIdentityRestore<EF>,
        /// Binding slot.
        binding: BindingStateRestore,
        /// Current or last binding's producer-emitted origin capsule.
        binding_origin: Option<BindingOrigin>,
        /// Four-state detach replay cell.
        detach_cell: DetachCellRestore<D>,
    },
    /// Permanent tombstone; no live binding or detach slot can accompany it.
    Retired(RetiredIdentityRestore<EF, V, LF>),
}

/// Validated runtime participant state restored from durable data.
#[allow(
    clippy::large_enum_variant,
    reason = "the validated live capsule remains inline as one atomic lifecycle result"
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RestoredParticipantLifecycle<EF, V, LF, D> {
    /// Valid live membership and its paired slots.
    Live {
        /// Validated live member.
        member: LiveMember<EF>,
        /// Validated binding state.
        binding: BindingState,
        /// Validated current or last binding origin, when a binding has existed.
        binding_origin: Option<BindingOrigin>,
        /// Validated detach cell.
        detach_cell: DetachCell<D>,
    },
    /// Permanent retired identity with no remaining live slots.
    Retired(RetiredIdentity<EF, V, LF>),
}

impl<EF, V, LF, D> ParticipantLifecycleRestore<EF, V, LF, D> {
    /// Validates one complete atomic participant snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`StorageRestoreError`] when any raw state is invalid or the
    /// membership, binding, terminal history, and detach-cell variants disagree.
    #[cfg(test)]
    pub(super) fn restore(
        self,
    ) -> Result<RestoredParticipantLifecycle<EF, V, LF, D>, StorageRestoreError> {
        match self {
            Self::Retired(identity) => identity
                .restore()
                .map(RestoredParticipantLifecycle::Retired),
            Self::Live {
                identity,
                binding,
                binding_origin,
                detach_cell,
            } => {
                let member = identity.restore()?;
                let binding = binding.restore_for(&member)?;
                let binding_origin = binding_origin
                    .map(|origin| validate_binding_origin(origin, &member, binding))
                    .transpose()?;
                let origin_required = !matches!(binding, BindingState::Detached)
                    || member.latest_terminal().is_some();
                if origin_required != binding_origin.is_some() {
                    return Err(StorageRestoreError::BindingAuthority);
                }
                let detach_cell = detach_cell.restore()?;
                validate_live_pair(&member, binding, &detach_cell)?;
                Ok(RestoredParticipantLifecycle::Live {
                    member,
                    binding,
                    binding_origin,
                    detach_cell,
                })
            }
        }
    }
}

#[cfg(test)]
fn validate_binding_origin<EF>(
    origin: BindingOrigin,
    member: &LiveMember<EF>,
    binding_state: BindingState,
) -> Result<BindingOrigin, StorageRestoreError> {
    let expected_epoch = match binding_state {
        BindingState::Bound(current) => Some(current.binding_epoch),
        BindingState::PendingFinalization(pending) => Some(pending.binding_epoch()),
        BindingState::Detached => member
            .latest_terminal()
            .map(CommittedBindingTerminal::binding_epoch),
    };
    let attached = origin.attached();
    if origin.participant_id() != member.participant_id()
        || origin.conversation_id() != member.conversation_id()
        || expected_epoch != Some(origin.binding_epoch())
        || attached.participant_id() != member.participant_id()
        || attached.conversation_id() != member.conversation_id()
    {
        Err(StorageRestoreError::BindingAuthority)
    } else {
        Ok(origin)
    }
}

impl<EF, V, LF, D> RestoredParticipantLifecycle<EF, V, LF, D> {
    /// Consumes the restored snapshot into the crate's identity and optional live slots.
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        IdentityState<EF, V, LF>,
        Option<BindingState>,
        Option<DetachCell<D>>,
    ) {
        match self {
            Self::Live {
                member,
                binding,
                binding_origin: _,
                detach_cell,
            } => (
                IdentityState::Live(member),
                Some(binding),
                Some(detach_cell),
            ),
            Self::Retired(identity) => (IdentityState::Retired(identity), None, None),
        }
    }
}

#[cfg(test)]
impl<EF, V, LF, D> ParticipantConversationRestore<EF, V, LF, D> {
    /// Restores participant lifecycle, sealed history/origins, frontiers, and
    /// closure as one total conversation snapshot.
    ///
    /// # Errors
    ///
    /// Returns a storage error when participant snapshots or binding origins
    /// disagree, and a claim-frontier error when the exact participant-derived
    /// history does not back raw causal rows and marker ownership.
    pub fn restore(
        self,
    ) -> Result<ParticipantConversationState<EF, V, LF, D>, ConversationStateRestoreError> {
        let participants = self
            .participants
            .into_iter()
            .map(ParticipantLifecycleRestore::restore)
            .collect::<Result<Vec<_>, _>>()
            .map_err(ConversationStateRestoreError::Storage)?;
        validate_participant_frontier_projection(
            &participants,
            self.frontiers.conversation_id,
            &self.frontiers.active_identities,
        )?;
        let history = validated_conversation_history(&participants)?;
        let restored = restore_conversation_with_history(
            self.frontiers,
            self.sequence_ledger,
            self.order_ledger,
            &self.closure,
            &history,
        )?;
        let (frontiers, closure) = restored.into_parts();
        Ok(ParticipantConversationState {
            participants,
            frontiers,
            closure,
        })
    }
}

#[cfg(test)]
fn validated_conversation_history<EF, V, LF, D>(
    participants: &[RestoredParticipantLifecycle<EF, V, LF, D>],
) -> Result<ValidatedConversationHistory, ConversationStateRestoreError> {
    let mut causal_authorities = Vec::new();
    let mut binding_origins = Vec::new();
    let mut seen = Vec::new();
    for participant in participants {
        let participant_id = match participant {
            RestoredParticipantLifecycle::Live {
                member,
                binding_origin,
                ..
            } => {
                if let Some(terminal) = member.latest_terminal() {
                    causal_authorities
                        .push(HistoricalCausalAuthority::from_committed_terminal(terminal));
                }
                if let Some(origin) = binding_origin {
                    binding_origins.push(*origin);
                }
                member.participant_id()
            }
            RestoredParticipantLifecycle::Retired(retired) => {
                causal_authorities.push(HistoricalCausalAuthority::from_retired(retired));
                retired.participant_id()
            }
        };
        if seen.contains(&participant_id) {
            return Err(ConversationStateRestoreError::Storage(
                StorageRestoreError::MembershipInvariant,
            ));
        }
        seen.push(participant_id);
    }
    Ok(ValidatedConversationHistory::new(
        causal_authorities,
        binding_origins,
    ))
}

#[cfg(test)]
fn validate_participant_frontier_projection<EF, V, LF, D>(
    participants: &[RestoredParticipantLifecycle<EF, V, LF, D>],
    conversation_id: ConversationId,
    frontier: &[super::FrontierParticipant],
) -> Result<(), ConversationStateRestoreError> {
    let mut projected = Vec::new();
    for participant in participants {
        match participant {
            RestoredParticipantLifecycle::Live {
                member, binding, ..
            } => {
                if member.conversation_id() != conversation_id {
                    return Err(ConversationStateRestoreError::Storage(
                        StorageRestoreError::MembershipInvariant,
                    ));
                }
                let binding = match binding {
                    BindingState::Bound(binding) => FrontierBinding::Bound(binding.binding_epoch),
                    BindingState::PendingFinalization(pending) => {
                        FrontierBinding::Detached(pending.binding_epoch())
                    }
                    BindingState::Detached => {
                        let Some(terminal) = member.latest_terminal() else {
                            return Err(ConversationStateRestoreError::Storage(
                                StorageRestoreError::BindingAuthority,
                            ));
                        };
                        FrontierBinding::Detached(terminal.binding_epoch())
                    }
                };
                projected.push(super::FrontierParticipant::new(
                    member.participant_id(),
                    member.cursor(),
                    binding,
                ));
            }
            RestoredParticipantLifecycle::Retired(retired) => {
                if retired.conversation_id() != conversation_id {
                    return Err(ConversationStateRestoreError::Storage(
                        StorageRestoreError::MembershipInvariant,
                    ));
                }
            }
        }
    }
    projected.sort_by_key(|participant| participant.participant_index());
    if projected == frontier {
        Ok(())
    } else {
        Err(ConversationStateRestoreError::Storage(
            StorageRestoreError::MembershipInvariant,
        ))
    }
}

#[allow(clippy::suspicious_operation_groupings)]
#[cfg(test)]
fn validate_live_pair<EF, D>(
    member: &LiveMember<EF>,
    binding: BindingState,
    detach_cell: &DetachCell<D>,
) -> Result<(), StorageRestoreError> {
    match detach_cell {
        DetachCell::Empty(_) => Ok(()),
        DetachCell::Pending(cell) => {
            if cell.participant_id() != member.participant_id()
                || cell.request_generation() != member.generation()
                || validate_pending_pair(binding, cell, Some(member.conversation_id())).is_err()
            {
                Err(StorageRestoreError::DetachBindingPair)
            } else {
                Ok(())
            }
        }
        DetachCell::Committed(cell) => {
            let terminal_matches = member.latest_terminal().is_some_and(|terminal| {
                terminal.participant_id() == cell.participant_id()
                    && terminal.conversation_id() == member.conversation_id()
                    && terminal.binding_epoch() == cell.committed_binding_epoch()
                    && terminal.delivery_seq() == cell.detached_delivery_seq()
                    && terminal.detached_cause()
                        == Some(crate::wire::DetachedCause::CleanDeregister)
            });
            if cell.participant_id() != member.participant_id()
                || cell.request_generation() != member.generation()
                || binding != BindingState::Detached
                || !terminal_matches
            {
                Err(StorageRestoreError::DetachBindingPair)
            } else {
                Ok(())
            }
        }
        DetachCell::Terminalized(cell) => {
            if cell.participant_id() != member.participant_id()
                || cell.request_generation().get() >= member.generation().get()
            {
                Err(StorageRestoreError::DetachBindingPair)
            } else {
                Ok(())
            }
        }
    }
}

/// Complete raw state for one participant-scoped nonzero-debt cursor episode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CursorEpisodeRestore {
    /// Owning conversation.
    pub conversation_id: ConversationId,
    /// Raw componentwise closure debt, validated nonzero during restoration.
    pub debt: WideResourceVector,
    /// Durable hard-observer progress `o`.
    pub observer_progress: DeliverySeq,
    /// Candidate high watermark `H'`.
    pub candidate_high_watermark: DeliverySeq,
    /// Current durable floor `F`.
    pub current_floor: u128,
    /// Current append-free class capacity floor.
    pub cap_floor: u128,
    /// Bound participant cursors keyed by their embedded permanent ids.
    pub participants: Vec<BoundParticipantCursor>,
    /// Variable facts keyed by `(participant_index, boundary)`.
    pub facts: Vec<(CursorProgressKey, CursorProgressFact)>,
}

impl CursorEpisodeRestore {
    /// Validates and rebuilds one cursor episode and all variable facts.
    ///
    /// # Errors
    ///
    /// Returns [`StorageRestoreError::CursorEpisode`] for an invalid floor,
    /// duplicate/unknown participant, duplicate fact, out-of-range boundary, or
    /// fact state inconsistent with the participant cursor.
    pub fn restore(self) -> Result<NonzeroDebtCursorEpisode, StorageRestoreError> {
        let debt = ClosureDebt::new(self.debt).ok_or(StorageRestoreError::ClosureDebt)?;
        NonzeroDebtCursorEpisode::restore(
            self.conversation_id,
            debt,
            self.observer_progress,
            self.candidate_high_watermark,
            self.current_floor,
            self.cap_floor,
            self.participants,
            self.facts,
        )
        .ok_or(StorageRestoreError::CursorEpisode)
    }
}

/// Raw predecessor fields for an ordinary-attach binding authority.
///
/// Raw fields are not executable provenance. Standalone restoration is absent:
/// total participant-conversation restore must first prove an exact unfenced
/// binding-origin capsule.
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::OrdinaryBindingAuthorityRestore;
///
/// fn bypass(raw: OrdinaryBindingAuthorityRestore) {
///     let _ = raw.restore();
/// }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrdinaryBindingAuthorityRestore {
    /// Exact binding installed by ordinary attach.
    pub binding: ActiveBinding,
    /// Durable no-marker cursor carried through the attach.
    pub through_seq: DeliverySeq,
}

impl OrdinaryBindingAuthorityRestore {
    fn restore_with_origin(
        self,
        origin: &BindingOrigin,
    ) -> Result<OrdinaryBindingAuthority, StorageRestoreError> {
        if !origin.is_unfenced()
            || origin.conversation_id() != self.binding.conversation_id
            || origin.participant_id() != self.binding.participant_id
            || origin.binding_epoch() != self.binding.binding_epoch
        {
            return Err(StorageRestoreError::StoredEdgeProvenance);
        }
        Ok(OrdinaryBindingAuthority::new(
            self.binding,
            self.through_seq,
        ))
    }
}

/// Raw predecessor fields proving exact marker delivery.
///
/// A retained-record authority is mandatory and cannot be constructed by a
/// storage binding. Raw edge fields alone therefore fail at compile time.
///
/// ```compile_fail
/// use liminal_protocol::{
///     lifecycle::MarkerDeliveryRestore,
///     wire::BindingEpoch,
/// };
///
/// fn raw_restore(epoch: BindingEpoch) {
///     let raw = MarkerDeliveryRestore {
///         participant_id: 7,
///         binding_epoch: epoch,
///         marker_delivery_seq: 11,
///     };
///     let _ = raw.restore_bound(1);
/// }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarkerDeliveryRestore {
    /// Participant that received the marker.
    pub participant_id: ParticipantId,
    /// Exact receiving binding epoch.
    pub binding_epoch: BindingEpoch,
    /// Exact delivered marker sequence.
    pub marker_delivery_seq: DeliverySeq,
}

impl MarkerDeliveryRestore {
    /// Rebuilds a live marker-delivery witness after exact record-field matching.
    ///
    /// # Errors
    ///
    /// Returns [`StorageRestoreError::StoredEdgeProvenance`] when the authority
    /// belongs to another conversation, names a detached target, or disagrees
    /// with any raw edge field.
    #[cfg(test)]
    pub(super) fn restore_bound(
        self,
        conversation_id: ConversationId,
        record_authority: ValidatedMarkerRecord,
    ) -> Result<MarkerDelivery, StorageRestoreError> {
        let restored = self.restore_with_target(
            conversation_id,
            &record_authority,
            MarkerRecordTarget::Bound,
            MarkerRecordOccurrence::Undelivered,
        );
        record_authority.consume();
        restored
    }

    /// Exercises the delivered detached-record gate for adversarial tests.
    #[cfg(test)]
    pub(super) fn restore_detached_delivered_for_test(
        self,
        conversation_id: ConversationId,
        record_authority: ValidatedMarkerRecord,
    ) -> Result<MarkerDelivery, StorageRestoreError> {
        let restored = self.restore_with_target(
            conversation_id,
            &record_authority,
            MarkerRecordTarget::Detached,
            MarkerRecordOccurrence::Delivered,
        );
        record_authority.consume();
        restored
    }

    fn restore_detached(
        self,
        conversation_id: ConversationId,
        record_authority: &ValidatedMarkerRecord,
    ) -> Result<MarkerDelivery, StorageRestoreError> {
        self.restore_with_target(
            conversation_id,
            record_authority,
            MarkerRecordTarget::Detached,
            MarkerRecordOccurrence::Undelivered,
        )
    }

    fn restore_with_target(
        self,
        conversation_id: ConversationId,
        record_authority: &ValidatedMarkerRecord,
        target: MarkerRecordTarget,
        occurrence: MarkerRecordOccurrence,
    ) -> Result<MarkerDelivery, StorageRestoreError> {
        if record_authority.conversation_id() != conversation_id
            || !target.matches(record_authority.target_binding(), self.binding_epoch)
            || record_authority.occurrence() != occurrence
        {
            return Err(StorageRestoreError::StoredEdgeProvenance);
        }
        let delivery = MarkerDelivery::from_validated_record(record_authority);
        if delivery.participant_id() != self.participant_id
            || delivery.binding_epoch() != self.binding_epoch
            || delivery.marker_delivery_seq() != self.marker_delivery_seq
        {
            return Err(StorageRestoreError::StoredEdgeProvenance);
        }
        Ok(delivery)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MarkerRecordTarget {
    Bound,
    Detached,
}

impl MarkerRecordTarget {
    fn matches(self, target: super::FrontierBinding, epoch: BindingEpoch) -> bool {
        matches!(
            (self, target),
            (Self::Bound, super::FrontierBinding::Bound(actual))
                | (Self::Detached, super::FrontierBinding::Detached(actual))
                if actual == epoch
        )
    }
}

#[derive(Debug)]
#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "record authority is constructible only inside crate-owned restore tests"
    )
)]
enum MarkerRestoreAuthority<'a> {
    Absent,
    Record {
        conversation_id: ConversationId,
        record: &'a ValidatedMarkerRecord,
    },
}

impl MarkerRestoreAuthority<'_> {
    const fn require_absent(&self) -> Result<(), StorageRestoreError> {
        match self {
            Self::Absent => Ok(()),
            Self::Record { .. } => Err(StorageRestoreError::StoredEdgeProvenance),
        }
    }

    const fn require_record(
        &self,
    ) -> Result<(ConversationId, &ValidatedMarkerRecord), StorageRestoreError> {
        match self {
            Self::Record {
                conversation_id,
                record,
            } if record.conversation_id() == *conversation_id => Ok((*conversation_id, record)),
            Self::Absent | Self::Record { .. } => Err(StorageRestoreError::StoredEdgeProvenance),
        }
    }

    fn record_for(
        &self,
        expected_conversation_id: ConversationId,
    ) -> Result<&ValidatedMarkerRecord, StorageRestoreError> {
        let (conversation_id, record) = self.require_record()?;
        if conversation_id == expected_conversation_id {
            Ok(record)
        } else {
            Err(StorageRestoreError::StoredEdgeProvenance)
        }
    }
}

/// Marker-backed cursor provenance retaining its exact delivery predecessor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarkerCursorProgressRestore {
    /// Conversation key under which the cursor witness is stored.
    pub conversation_id: ConversationId,
    /// Cursor witness participant.
    pub participant_id: ParticipantId,
    /// Cursor witness binding epoch.
    pub binding_epoch: BindingEpoch,
    /// Required cumulative cursor boundary.
    pub through_seq: DeliverySeq,
    /// Exact marker accepted by this cursor witness.
    pub marker_delivery_seq: DeliverySeq,
    /// Exact predecessor delivery proof.
    pub delivery: MarkerDeliveryRestore,
}

impl MarkerCursorProgressRestore {
    fn restore_with_debt(
        self,
        debt: ClosureDebt,
        record_authority: &ValidatedMarkerRecord,
        target: MarkerRecordTarget,
    ) -> Result<ParticipantCursorProgress, StorageRestoreError> {
        if self.participant_id != self.delivery.participant_id
            || self.binding_epoch != self.delivery.binding_epoch
            || self.marker_delivery_seq != self.delivery.marker_delivery_seq
            || self.through_seq != self.marker_delivery_seq
        {
            return Err(StorageRestoreError::StoredEdgeProvenance);
        }
        let marker = self.delivery.restore_with_target(
            self.conversation_id,
            record_authority,
            target,
            MarkerRecordOccurrence::Delivered,
        )?;
        let state = marker
            .delivered(
                debt,
                Event::marker_delivered(
                    self.participant_id,
                    self.binding_epoch,
                    self.marker_delivery_seq,
                ),
            )
            .map_err(|_| StorageRestoreError::StoredEdgeProvenance)?;
        match state {
            ClosureState::Owed {
                edge: StoredEdge::ParticipantCursorProgress(progress),
                ..
            } => Ok(progress),
            ClosureState::Clear | ClosureState::Owed { .. } => {
                Err(StorageRestoreError::StoredEdgeProvenance)
            }
        }
    }
}

/// Marker-backed detached credential-recovery provenance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DetachedCredentialRecoveryRestore {
    /// Detached participant.
    pub participant_id: ParticipantId,
    /// Delivered recovery marker.
    pub marker_delivery_seq: DeliverySeq,
    /// Dead binding epoch that received the marker.
    pub prior_binding_epoch: BindingEpoch,
    /// Floor measured by the binding-fate transaction that selected DCR.
    pub resulting_floor: DeliverySeq,
    /// Exact committed or pending terminal proving the binding fate occurred.
    pub terminal: BindingFateTerminalRestore,
    /// Exact marker-backed cursor predecessor.
    pub progress: MarkerCursorProgressRestore,
}

impl DetachedCredentialRecoveryRestore {
    fn restore_with_debt(
        self,
        debt: ClosureDebt,
        record_authority: &ValidatedMarkerRecord,
    ) -> Result<DetachedCredentialRecovery, StorageRestoreError> {
        if self.participant_id != self.progress.participant_id
            || self.marker_delivery_seq != self.progress.marker_delivery_seq
            || self.prior_binding_epoch != self.progress.binding_epoch
        {
            return Err(StorageRestoreError::StoredEdgeProvenance);
        }
        let terminal = self.terminal.restore()?;
        if terminal.participant_id() != self.participant_id
            || terminal.binding_epoch() != self.prior_binding_epoch
            || terminal.conversation_id() != self.progress.conversation_id
        {
            return Err(StorageRestoreError::StoredEdgeProvenance);
        }
        let progress = self.progress.restore_with_debt(
            debt,
            record_authority,
            MarkerRecordTarget::Detached,
        )?;
        let successor = progress
            .binding_fate(
                debt,
                Event::binding_fate_observed(
                    self.participant_id,
                    self.prior_binding_epoch,
                    self.resulting_floor,
                ),
            )
            .map_err(|_| StorageRestoreError::StoredEdgeProvenance)?;
        match successor.into_stored_edge() {
            StoredEdge::DetachedCredentialRecovery(edge) => Ok(edge),
            _ => Err(StorageRestoreError::StoredEdgeProvenance),
        }
    }
}

/// Undelivered-marker release provenance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DetachedMarkerReleaseRestore {
    /// Conversation key under which the release is stored.
    pub conversation_id: ConversationId,
    /// Detached participant.
    pub participant_id: ParticipantId,
    /// Undelivered marker sequence.
    pub marker_delivery_seq: DeliverySeq,
    /// Dead binding epoch.
    pub last_dead_binding_epoch: BindingEpoch,
    /// Floor measured by the binding-fate transaction that selected DMR.
    pub resulting_floor: DeliverySeq,
    /// Exact committed or pending terminal proving the binding fate occurred.
    pub terminal: BindingFateTerminalRestore,
    /// Exact undelivered marker predecessor.
    pub delivery: MarkerDeliveryRestore,
}

impl DetachedMarkerReleaseRestore {
    fn restore_with_debt(
        self,
        debt: ClosureDebt,
        record_authority: &ValidatedMarkerRecord,
    ) -> Result<DetachedMarkerRelease, StorageRestoreError> {
        if self.participant_id != self.delivery.participant_id
            || self.marker_delivery_seq != self.delivery.marker_delivery_seq
            || self.last_dead_binding_epoch != self.delivery.binding_epoch
        {
            return Err(StorageRestoreError::StoredEdgeProvenance);
        }
        let terminal = self.terminal.restore()?;
        if terminal.participant_id() != self.participant_id
            || terminal.binding_epoch() != self.last_dead_binding_epoch
            || terminal.conversation_id() != self.conversation_id
        {
            return Err(StorageRestoreError::StoredEdgeProvenance);
        }
        let marker = self
            .delivery
            .restore_detached(self.conversation_id, record_authority)?;
        let state = marker
            .binding_fate(
                debt,
                Event::binding_fate_observed(
                    self.participant_id,
                    self.last_dead_binding_epoch,
                    self.resulting_floor,
                ),
            )
            .map_err(|_| StorageRestoreError::StoredEdgeProvenance)?;
        match state {
            ClosureState::Owed {
                edge: StoredEdge::DetachedMarkerRelease(edge),
                ..
            } => Ok(edge),
            ClosureState::Clear | ClosureState::Owed { .. } => {
                Err(StorageRestoreError::StoredEdgeProvenance)
            }
        }
    }
}

/// Exact ordinary-binding fate provenance for cursor release.
///
/// Raw fields are deliberately not independently restorable. The participant's
/// total snapshot must first prove its unfenced binding origin.
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::OrdinaryBindingFateRestore;
///
/// fn bypass(raw: OrdinaryBindingFateRestore) {
///     let _ = raw.restore();
/// }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrdinaryBindingFateRestore {
    /// Ordinary-attach authority that owned the no-marker cursor.
    pub authority: OrdinaryBindingAuthorityRestore,
    /// Exact committed `Died` terminal for that authority.
    pub terminal: CommittedBindingTerminalRestore,
    /// Floor measured in the binding-fate transaction.
    pub resulting_floor: DeliverySeq,
}

impl OrdinaryBindingFateRestore {
    /// Validates the ordinary attach and exact committed-death provenance.
    ///
    /// # Errors
    ///
    /// Returns [`StorageRestoreError::StoredEdgeProvenance`] unless the terminal
    /// is a `Died` terminal for the exact participant, conversation, and epoch.
    fn restore_with_origin(
        self,
        origin: &BindingOrigin,
    ) -> Result<OrdinaryBindingFate, StorageRestoreError> {
        let authority = self.authority.restore_with_origin(origin)?;
        let terminal = self.terminal.restore()?;
        let CommittedBindingTerminal::Died(terminal) = terminal else {
            return Err(StorageRestoreError::StoredEdgeProvenance);
        };
        authority
            .binding_fate(terminal, self.resulting_floor)
            .map_err(|_| StorageRestoreError::StoredEdgeProvenance)
    }
}

/// Raw durable successor class accepted by detached Leave or fenced attach.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DebtCompletionRestore {
    /// Debt cleared completely.
    Clear,
    /// Nonzero debt with an independent observer-projection successor.
    ObserverProjection {
        /// Raw nonzero debt vector.
        debt: WideResourceVector,
        /// Projection boundary.
        through_seq: DeliverySeq,
    },
    /// Nonzero debt with an independent physical-compaction successor.
    PhysicalCompaction {
        /// Raw nonzero debt vector.
        debt: WideResourceVector,
        /// First sequence in the compaction range.
        from_floor: DeliverySeq,
        /// Inclusive compaction boundary.
        through_seq: DeliverySeq,
    },
}

impl DebtCompletionRestore {
    /// Validates and rebuilds the restricted clear/OP/PC successor.
    ///
    /// # Errors
    ///
    /// Returns [`StorageRestoreError`] for zero debt or an inverted compaction range.
    pub fn restore(self) -> Result<DebtCompletion, StorageRestoreError> {
        match self {
            Self::Clear => Ok(DebtCompletion::clear()),
            Self::ObserverProjection { debt, through_seq } => {
                let debt = ClosureDebt::new(debt).ok_or(StorageRestoreError::ClosureDebt)?;
                Ok(DebtCompletion::observer_projection(
                    debt,
                    ObserverProjection::new(through_seq),
                ))
            }
            Self::PhysicalCompaction {
                debt,
                from_floor,
                through_seq,
            } => {
                let debt = ClosureDebt::new(debt).ok_or(StorageRestoreError::ClosureDebt)?;
                let edge = PhysicalCompaction::new(from_floor, through_seq)
                    .ok_or(StorageRestoreError::StoredEdgeProvenance)?;
                Ok(DebtCompletion::physical_compaction(debt, edge))
            }
        }
    }
}

/// Complete predecessor and event fields for a committed fenced attach.
///
/// Raw fields remain serializable, but their executable restoration entry point
/// is crate-private. Public callers cannot combine them with an occurrence token
/// obtained from another transition.
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::FencedAttachCommitRestore;
///
/// fn bypass() {
///     let _ = FencedAttachCommitRestore::restore;
/// }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FencedAttachCommitRestore {
    /// Exact DCR predecessor.
    pub predecessor: DetachedCredentialRecoveryRestore,
    /// Raw nonzero debt carried by the DCR predecessor.
    pub predecessor_debt: WideResourceVector,
    /// Event participant; must equal the DCR owner.
    pub participant_id: ParticipantId,
    /// Event marker; must equal the DCR recovery marker.
    pub marker_delivery_seq: DeliverySeq,
    /// Event prior epoch; must equal the DCR prior epoch.
    pub prior_binding_epoch: BindingEpoch,
    /// Newly committed immediately-next binding epoch.
    pub new_binding_epoch: BindingEpoch,
    /// Floor measured by the fenced-attach transaction.
    pub resulting_floor: DeliverySeq,
    /// Restricted post-attach closure state.
    pub successor: DebtCompletionRestore,
}

impl FencedAttachCommitRestore {
    /// Replays validation from the exact DCR predecessor to the fenced commit.
    ///
    /// # Errors
    ///
    /// Returns [`StorageRestoreError`] for any participant, marker, epoch,
    /// generation, debt, or successor mismatch.
    #[cfg(test)]
    pub(super) fn restore(
        self,
        record_authority: ValidatedMarkerRecord,
    ) -> Result<FencedAttachCommit, StorageRestoreError> {
        let restored = self.restore_with_record(&record_authority);
        record_authority.consume();
        restored
    }

    fn restore_with_record(
        self,
        record_authority: &ValidatedMarkerRecord,
    ) -> Result<FencedAttachCommit, StorageRestoreError> {
        let debt =
            ClosureDebt::new(self.predecessor_debt).ok_or(StorageRestoreError::ClosureDebt)?;
        let predecessor = self.predecessor.restore_with_debt(debt, record_authority)?;
        predecessor
            .fenced_attach(
                debt,
                Event::fenced_recovery_committed(
                    self.participant_id,
                    self.marker_delivery_seq,
                    self.prior_binding_epoch,
                    self.new_binding_epoch,
                    self.resulting_floor,
                ),
                self.successor.restore()?,
            )
            .map_err(|_| StorageRestoreError::StoredEdgeProvenance)
    }
}

/// Complete fenced-attach predecessor for a recovered binding-fate authority.
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::RecoveredBindingFateRestore;
///
/// fn bypass() {
///     let _ = RecoveredBindingFateRestore::restore;
/// }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveredBindingFateRestore {
    /// Exact fenced-attach commit that installed the recovered epoch.
    pub fenced_attach: FencedAttachCommitRestore,
    /// Fate event participant.
    pub participant_id: ParticipantId,
    /// Fate event epoch.
    pub binding_epoch: BindingEpoch,
    /// Floor measured in the fate transaction.
    pub resulting_floor: DeliverySeq,
}

impl RecoveredBindingFateRestore {
    /// Replays the fenced commit and validates exact recovered-epoch fate.
    ///
    /// # Errors
    ///
    /// Returns [`StorageRestoreError::StoredEdgeProvenance`] for a wrong
    /// participant/epoch or a fenced attach whose successor was already clear.
    #[cfg(test)]
    pub(super) fn restore(
        self,
        record_authority: ValidatedMarkerRecord,
    ) -> Result<RecoveredBindingFate, StorageRestoreError> {
        let restored = self.restore_with_record(&record_authority);
        record_authority.consume();
        restored
    }

    fn restore_with_record(
        self,
        record_authority: &ValidatedMarkerRecord,
    ) -> Result<RecoveredBindingFate, StorageRestoreError> {
        let commit = self.fenced_attach.restore_with_record(record_authority)?;
        commit
            .recovered_binding_fate(Event::binding_fate_observed(
                self.participant_id,
                self.binding_epoch,
                self.resulting_floor,
            ))
            .map_err(|_| StorageRestoreError::StoredEdgeProvenance)
    }
}

/// Durable latent cursor-release suffix while an OP/PC predecessor remains stored.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PendingRecoveredCursorReleaseRestore {
    /// Exact fenced-attach and recovered-fate predecessor.
    pub fate: RecoveredBindingFateRestore,
    /// Raw nonzero debt remaining after the fate transaction.
    pub resulting_debt: WideResourceVector,
}

impl PendingRecoveredCursorReleaseRestore {
    /// Rebuilds the latent suffix only when the exact OP/PC remains current.
    ///
    /// # Errors
    ///
    /// Returns [`StorageRestoreError`] if provenance mismatches or the fate
    /// covers physical compaction immediately instead of leaving it pending.
    #[cfg(test)]
    pub(super) fn restore(
        self,
        record_authority: ValidatedMarkerRecord,
    ) -> Result<PendingRecoveredCursorRelease, StorageRestoreError> {
        let restored = self.restore_with_record(&record_authority);
        record_authority.consume();
        restored
    }

    fn restore_with_record(
        self,
        record_authority: &ValidatedMarkerRecord,
    ) -> Result<PendingRecoveredCursorRelease, StorageRestoreError> {
        let authority = self.fate.restore_with_record(record_authority)?;
        let predecessor_state = authority.predecessor_state();
        let resulting_debt =
            ClosureDebt::new(self.resulting_debt).ok_or(StorageRestoreError::ClosureDebt)?;
        let transition = match predecessor_state {
            ClosureState::Owed {
                debt,
                edge: StoredEdge::ObserverProjection(edge),
            } => edge.apply_recovered_binding_fate(debt, resulting_debt, authority),
            ClosureState::Owed {
                debt,
                edge: StoredEdge::PhysicalCompaction(edge),
            } => edge.apply_recovered_binding_fate(debt, resulting_debt, authority),
            ClosureState::Clear | ClosureState::Owed { .. } => {
                return Err(StorageRestoreError::StoredEdgeProvenance);
            }
        }
        .map_err(|_| StorageRestoreError::StoredEdgeProvenance)?;
        match transition {
            RecoveredBindingFateTransition::PendingStorage(pending) => Ok(pending),
            RecoveredBindingFateTransition::DetachedCursorRelease(_) => {
                Err(StorageRestoreError::StoredEdgeProvenance)
            }
        }
    }
}

/// Exact completion event consuming a latent OP/PC predecessor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveredStorageCompletionRestore {
    /// Observer projection completed through this boundary.
    ObserverProjection {
        /// Completion boundary.
        through_seq: DeliverySeq,
        /// Remaining debt, or `None` when closure clears.
        resulting_debt: Option<WideResourceVector>,
    },
    /// Physical compaction completed this exact range.
    PhysicalCompaction {
        /// First compacted sequence.
        from_floor: DeliverySeq,
        /// Inclusive compaction boundary.
        through_seq: DeliverySeq,
        /// Resulting first-retained floor.
        resulting_floor: DeliverySeq,
        /// Remaining debt, or `None` when closure clears.
        resulting_debt: Option<WideResourceVector>,
    },
}

impl RecoveredStorageCompletionRestore {
    fn restore(
        self,
        pending: PendingRecoveredCursorRelease,
    ) -> Result<ClosureState, StorageRestoreError> {
        let current = pending.current_state();
        match (current, self) {
            (
                ClosureState::Owed {
                    edge: StoredEdge::ObserverProjection(edge),
                    ..
                },
                Self::ObserverProjection {
                    through_seq,
                    resulting_debt,
                },
            ) => edge
                .complete_after_recovered_binding_fate(
                    Event::projection_completed(through_seq),
                    optional_debt(resulting_debt)?,
                    pending,
                )
                .map_err(|_| StorageRestoreError::StoredEdgeProvenance),
            (
                ClosureState::Owed {
                    edge: StoredEdge::PhysicalCompaction(edge),
                    ..
                },
                Self::PhysicalCompaction {
                    from_floor,
                    through_seq,
                    resulting_floor,
                    resulting_debt,
                },
            ) => {
                let event = Event::compaction_completed(from_floor, through_seq, resulting_floor)
                    .ok_or(StorageRestoreError::StoredEdgeProvenance)?;
                edge.complete_after_recovered_binding_fate(
                    event,
                    optional_debt(resulting_debt)?,
                    pending,
                )
                .map_err(|_| StorageRestoreError::StoredEdgeProvenance)
            }
            _ => Err(StorageRestoreError::StoredEdgeProvenance),
        }
    }
}

/// Provenance alternatives capable of constructing `DetachedCursorRelease`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DetachedCursorReleaseProvenanceRestore {
    /// Direct release from an exact ordinary binding death.
    Ordinary(OrdinaryBindingFateRestore),
    /// Recovered fate immediately covered physical compaction.
    RecoveredDirect {
        /// Exact fenced-attach recovered fate.
        fate: RecoveredBindingFateRestore,
        /// Raw nonzero debt carried by the release.
        resulting_debt: WideResourceVector,
    },
    /// Recovered fate remained latent until exact OP/PC completion.
    RecoveredAfterStorage {
        /// Exact latent suffix capsule.
        pending: PendingRecoveredCursorReleaseRestore,
        /// Exact storage completion consuming the predecessor.
        completion: RecoveredStorageCompletionRestore,
    },
}

impl DetachedCursorReleaseProvenanceRestore {
    #[cfg(test)]
    const fn ordinary_binding_request(self) -> Option<ActiveBinding> {
        match self {
            Self::Ordinary(fate) => Some(fate.authority.binding),
            Self::RecoveredDirect { .. } | Self::RecoveredAfterStorage { .. } => None,
        }
    }

    #[cfg(test)]
    const fn marker_record_request(self) -> Option<MarkerRecordRequest> {
        match self {
            Self::Ordinary(_) => None,
            Self::RecoveredDirect { fate, .. } => Some(MarkerRecordRequest::recovered(
                fate.participant_id,
                fate.fenced_attach.marker_delivery_seq,
                fate.fenced_attach.prior_binding_epoch,
                fate.fenced_attach.new_binding_epoch,
            )),
            Self::RecoveredAfterStorage { pending, .. } => Some(MarkerRecordRequest::recovered(
                pending.fate.participant_id,
                pending.fate.fenced_attach.marker_delivery_seq,
                pending.fate.fenced_attach.prior_binding_epoch,
                pending.fate.fenced_attach.new_binding_epoch,
            )),
        }
    }

    fn restore_state(
        self,
        debt: ClosureDebt,
        marker_authority: &MarkerRestoreAuthority<'_>,
        ordinary_origin: Option<&BindingOrigin>,
    ) -> Result<ClosureState, StorageRestoreError> {
        match self {
            Self::Ordinary(provenance) => {
                marker_authority.require_absent()?;
                let origin = ordinary_origin.ok_or(StorageRestoreError::StoredEdgeProvenance)?;
                Ok(provenance
                    .restore_with_origin(origin)?
                    .into_direct_state(debt))
            }
            Self::RecoveredDirect {
                fate,
                resulting_debt,
            } => {
                let (_, record_authority) = marker_authority.require_record()?;
                let authority = fate.restore_with_record(record_authority)?;
                let predecessor = authority.predecessor_state();
                let resulting_debt =
                    ClosureDebt::new(resulting_debt).ok_or(StorageRestoreError::ClosureDebt)?;
                let transition = match predecessor {
                    ClosureState::Owed {
                        debt: predecessor_debt,
                        edge: StoredEdge::PhysicalCompaction(edge),
                    } => edge.apply_recovered_binding_fate(
                        predecessor_debt,
                        resulting_debt,
                        authority,
                    ),
                    ClosureState::Clear | ClosureState::Owed { .. } => {
                        return Err(StorageRestoreError::StoredEdgeProvenance);
                    }
                }
                .map_err(|_| StorageRestoreError::StoredEdgeProvenance)?;
                match transition {
                    RecoveredBindingFateTransition::DetachedCursorRelease(release)
                        if release.debt() == debt =>
                    {
                        Ok(release.into_state())
                    }
                    RecoveredBindingFateTransition::PendingStorage(_)
                    | RecoveredBindingFateTransition::DetachedCursorRelease(_) => {
                        Err(StorageRestoreError::StoredEdgeProvenance)
                    }
                }
            }
            Self::RecoveredAfterStorage {
                pending,
                completion,
            } => {
                let (_, record_authority) = marker_authority.require_record()?;
                let state = completion.restore(pending.restore_with_record(record_authority)?)?;
                match state {
                    ClosureState::Owed {
                        debt: restored_debt,
                        edge: StoredEdge::DetachedCursorRelease(_),
                    } if restored_debt == debt => Ok(state),
                    ClosureState::Clear | ClosureState::Owed { .. } => {
                        Err(StorageRestoreError::StoredEdgeProvenance)
                    }
                }
            }
        }
    }
}

/// Exact seven-kind stored-edge representation with provenance where required.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoredEdgeRestore {
    /// Observer projection.
    ObserverProjection {
        /// Projection boundary.
        through_seq: DeliverySeq,
    },
    /// Physical compaction.
    PhysicalCompaction {
        /// First compacted sequence.
        from_floor: DeliverySeq,
        /// Inclusive compaction boundary.
        through_seq: DeliverySeq,
    },
    /// Planned exact marker delivery.
    MarkerDelivery(MarkerDeliveryRestore),
    /// Continuous cursor witness carrying ordinary-attach provenance.
    ParticipantCursorProgressContinuous {
        /// Stored edge participant.
        participant_id: ParticipantId,
        /// Stored edge binding epoch.
        binding_epoch: BindingEpoch,
        /// Stored required cursor boundary.
        through_seq: DeliverySeq,
        /// Exact ordinary-attach predecessor.
        authority: OrdinaryBindingAuthorityRestore,
    },
    /// Marker-backed cursor witness carrying delivery provenance.
    ParticipantCursorProgressMarker(MarkerCursorProgressRestore),
    /// Detached credential recovery carrying marker-ack provenance.
    DetachedCredentialRecovery(DetachedCredentialRecoveryRestore),
    /// Detached undelivered-marker release carrying delivery provenance.
    DetachedMarkerRelease(DetachedMarkerReleaseRestore),
    /// Detached cursor release carrying ordinary or fenced provenance.
    DetachedCursorRelease {
        /// Stored edge participant.
        participant_id: ParticipantId,
        /// Stored edge dead binding epoch.
        last_dead_binding_epoch: BindingEpoch,
        /// Provenance path that alone can construct the release.
        provenance: DetachedCursorReleaseProvenanceRestore,
    },
}

impl StoredEdgeRestore {
    #[cfg(test)]
    const fn ordinary_binding_request(self) -> Option<ActiveBinding> {
        match self {
            Self::ParticipantCursorProgressContinuous { authority, .. } => Some(authority.binding),
            Self::DetachedCursorRelease { provenance, .. } => provenance.ordinary_binding_request(),
            Self::ObserverProjection { .. }
            | Self::PhysicalCompaction { .. }
            | Self::MarkerDelivery(_)
            | Self::ParticipantCursorProgressMarker(_)
            | Self::DetachedCredentialRecovery(_)
            | Self::DetachedMarkerRelease(_) => None,
        }
    }

    #[cfg(test)]
    const fn marker_record_request(self) -> Option<MarkerRecordRequest> {
        match self {
            Self::MarkerDelivery(value) => Some(MarkerRecordRequest::planned(
                value.participant_id,
                value.marker_delivery_seq,
                super::FrontierBinding::Bound(value.binding_epoch),
            )),
            Self::ParticipantCursorProgressMarker(value) => Some(MarkerRecordRequest::delivered(
                value.participant_id,
                value.marker_delivery_seq,
                super::FrontierBinding::Bound(value.binding_epoch),
            )),
            Self::DetachedCredentialRecovery(value) => Some(MarkerRecordRequest::delivered(
                value.participant_id,
                value.marker_delivery_seq,
                super::FrontierBinding::Detached(value.prior_binding_epoch),
            )),
            Self::DetachedMarkerRelease(value) => Some(MarkerRecordRequest::planned(
                value.participant_id,
                value.marker_delivery_seq,
                super::FrontierBinding::Detached(value.last_dead_binding_epoch),
            )),
            Self::DetachedCursorRelease { provenance, .. } => provenance.marker_record_request(),
            Self::ObserverProjection { .. }
            | Self::PhysicalCompaction { .. }
            | Self::ParticipantCursorProgressContinuous { .. } => None,
        }
    }

    fn restore_with_debt(
        self,
        debt: ClosureDebt,
        marker_authority: &MarkerRestoreAuthority<'_>,
        ordinary_origin: Option<&BindingOrigin>,
    ) -> Result<StoredEdge, StorageRestoreError> {
        match self {
            Self::ObserverProjection { through_seq } => {
                marker_authority.require_absent()?;
                Ok(StoredEdge::ObserverProjection(ObserverProjection::new(
                    through_seq,
                )))
            }
            Self::PhysicalCompaction {
                from_floor,
                through_seq,
            } => {
                marker_authority.require_absent()?;
                PhysicalCompaction::new(from_floor, through_seq)
                    .map(StoredEdge::PhysicalCompaction)
                    .ok_or(StorageRestoreError::StoredEdgeProvenance)
            }
            Self::MarkerDelivery(value) => {
                let (conversation_id, record_authority) = marker_authority.require_record()?;
                value
                    .restore_with_target(
                        conversation_id,
                        record_authority,
                        MarkerRecordTarget::Bound,
                        MarkerRecordOccurrence::Undelivered,
                    )
                    .map(StoredEdge::MarkerDelivery)
            }
            Self::ParticipantCursorProgressContinuous {
                participant_id,
                binding_epoch,
                through_seq,
                authority,
            } => {
                marker_authority.require_absent()?;
                let origin = ordinary_origin.ok_or(StorageRestoreError::StoredEdgeProvenance)?;
                ParticipantCursorProgress::restore_continuous(
                    authority.restore_with_origin(origin)?,
                    participant_id,
                    binding_epoch,
                    through_seq,
                )
                .map(StoredEdge::ParticipantCursorProgress)
                .ok_or(StorageRestoreError::StoredEdgeProvenance)
            }
            Self::ParticipantCursorProgressMarker(value) => {
                let record_authority = marker_authority.record_for(value.conversation_id)?;
                value
                    .restore_with_debt(debt, record_authority, MarkerRecordTarget::Bound)
                    .map(StoredEdge::ParticipantCursorProgress)
            }
            Self::DetachedCredentialRecovery(value) => {
                let record_authority =
                    marker_authority.record_for(value.progress.conversation_id)?;
                value
                    .restore_with_debt(debt, record_authority)
                    .map(StoredEdge::DetachedCredentialRecovery)
            }
            Self::DetachedMarkerRelease(value) => {
                let record_authority = marker_authority.record_for(value.conversation_id)?;
                value
                    .restore_with_debt(debt, record_authority)
                    .map(StoredEdge::DetachedMarkerRelease)
            }
            Self::DetachedCursorRelease {
                participant_id,
                last_dead_binding_epoch,
                provenance,
            } => {
                let state = provenance.restore_state(debt, marker_authority, ordinary_origin)?;
                match state {
                    ClosureState::Owed {
                        edge: StoredEdge::DetachedCursorRelease(edge),
                        ..
                    } if edge.participant_id() == participant_id
                        && edge.last_dead_binding_epoch() == last_dead_binding_epoch =>
                    {
                        Ok(StoredEdge::DetachedCursorRelease(edge))
                    }
                    ClosureState::Clear | ClosureState::Owed { .. } => {
                        Err(StorageRestoreError::StoredEdgeProvenance)
                    }
                }
            }
        }
    }
}

/// Raw closure state; a stored edge always carries a raw nonzero debt vector.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClosureStateRestore {
    /// No edge and zero debt.
    Clear,
    /// One exact stored edge and raw componentwise debt.
    Owed {
        /// Raw debt, validated nonzero during restoration.
        debt: WideResourceVector,
        /// Exact edge and required predecessor provenance.
        edge: StoredEdgeRestore,
    },
}

impl ClosureStateRestore {
    #[cfg(test)]
    const fn ordinary_binding_request(self) -> Option<ActiveBinding> {
        match self {
            Self::Clear => None,
            Self::Owed { edge, .. } => edge.ordinary_binding_request(),
        }
    }

    #[cfg(test)]
    const fn marker_record_request(self) -> Option<MarkerRecordRequest> {
        match self {
            Self::Clear => None,
            Self::Owed { edge, .. } => edge.marker_record_request(),
        }
    }

    /// Validates and rebuilds one closure state.
    ///
    /// # Errors
    ///
    /// Returns [`StorageRestoreError`] for zero owed debt, an invalid range, or
    /// any opaque edge whose supplied predecessor provenance does not match.
    pub fn restore(self) -> Result<ClosureState, StorageRestoreError> {
        self.restore_with_authority(&MarkerRestoreAuthority::Absent, None)
    }

    /// Restores exactly one marker-derived edge after claim/log prevalidation.
    ///
    /// The marker token is conversation-bound and fixes whether its current
    /// target is live (`MarkerDelivery`/marker PCP) or detached (DMR/DCR and
    /// recovered cursor-release history).
    ///
    /// # Errors
    ///
    /// Returns [`StorageRestoreError::StoredEdgeProvenance`] when the raw edge
    /// is not marker-derived, the token belongs to another conversation, or
    /// the retained record has the wrong target state, epoch, participant, or
    /// delivery sequence.
    #[cfg(test)]
    pub(super) fn restore_with_marker_record(
        self,
        conversation_id: ConversationId,
        record: ValidatedMarkerRecord,
    ) -> Result<ClosureState, StorageRestoreError> {
        let restored = self.restore_with_authority(
            &MarkerRestoreAuthority::Record {
                conversation_id,
                record: &record,
            },
            None,
        );
        record.consume();
        restored
    }

    #[cfg(test)]
    fn restore_with_binding_origin(
        self,
        origin: &BindingOrigin,
    ) -> Result<ClosureState, StorageRestoreError> {
        self.restore_with_authority(&MarkerRestoreAuthority::Absent, Some(origin))
    }

    fn restore_with_authority(
        self,
        marker_authority: &MarkerRestoreAuthority<'_>,
        ordinary_origin: Option<&BindingOrigin>,
    ) -> Result<ClosureState, StorageRestoreError> {
        match self {
            Self::Clear => {
                marker_authority.require_absent()?;
                Ok(ClosureState::Clear)
            }
            Self::Owed { debt, edge } => {
                let debt = ClosureDebt::new(debt).ok_or(StorageRestoreError::ClosureDebt)?;
                Ok(ClosureState::Owed {
                    debt,
                    edge: edge.restore_with_debt(debt, marker_authority, ordinary_origin)?,
                })
            }
        }
    }
}

fn optional_debt(
    raw: Option<WideResourceVector>,
) -> Result<Option<ClosureDebt>, StorageRestoreError> {
    raw.map(|value| ClosureDebt::new(value).ok_or(StorageRestoreError::ClosureDebt))
        .transpose()
}
