//! Validated durable-state restoration for participant lifecycle typestates.
//!
//! Storage serialization necessarily crosses the crate's compile-time state
//! boundary. These capsules retain the predecessor data needed to rebuild
//! opaque authorities, then validate every identity, generation, epoch, and
//! paired-state invariant before returning executable lifecycle values.

use alloc::vec::Vec;

use crate::algebra::WideResourceVector;
use crate::wire::{
    AttachSecret, BindingEpoch, CloseCause, ConversationId, DeliverySeq, DetachAttemptToken,
    Generation, LeaveAttemptToken, LeaveCommitted, ObserverEpoch, ParticipantId, TransactionOrder,
};

use super::{
    ActiveBinding, AdmissionOrder, BindingState, BoundParticipantCursor, ClosureDebt, ClosureState,
    CommittedBindingTerminal, DebtCompletion, DetachCell, DetachedCredentialRecovery,
    DetachedMarkerRelease, EmptyDetach, EnrollmentFingerprint, Event, FencedAttachCommit,
    IdentityState, LeaveFingerprint, LiveMember, LiveMemberRestore, MarkerDelivery,
    NonzeroDebtCursorEpisode, ObserverProjection, OrdinaryBindingAuthority, OrdinaryBindingFate,
    ParticipantCursorProgress, PendingFinalization, PendingRecoveredCursorRelease,
    PhysicalCompaction, RecoveredBindingFate, RecoveredBindingFateTransition, RetiredIdentity,
    StoredEdge,
    binding::{restore_committed_terminal, restore_pending_finalization},
    cursor_facts::{CursorProgressFact, CursorProgressKey},
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
    /// Complete canonical committed result.
    pub committed_result: LeaveCommittedRestore,
}

impl<EF, V, LF> RetiredIdentityRestore<EF, V, LF> {
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

/// Complete participant durable state, with tombstone precedence in the type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParticipantLifecycleRestore<EF, V, LF, D> {
    /// Live identity and its atomically paired binding and detach slots.
    Live {
        /// Membership and terminal history.
        identity: LiveIdentityRestore<EF>,
        /// Binding slot.
        binding: BindingStateRestore,
        /// Four-state detach replay cell.
        detach_cell: DetachCellRestore<D>,
    },
    /// Permanent tombstone; no live binding or detach slot can accompany it.
    Retired(RetiredIdentityRestore<EF, V, LF>),
}

/// Validated runtime participant state restored from durable data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RestoredParticipantLifecycle<EF, V, LF, D> {
    /// Valid live membership and its paired slots.
    Live {
        /// Validated live member.
        member: LiveMember<EF>,
        /// Validated binding state.
        binding: BindingState,
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
    pub fn restore(
        self,
    ) -> Result<RestoredParticipantLifecycle<EF, V, LF, D>, StorageRestoreError> {
        match self {
            Self::Retired(identity) => identity
                .restore()
                .map(RestoredParticipantLifecycle::Retired),
            Self::Live {
                identity,
                binding,
                detach_cell,
            } => {
                let member = identity.restore()?;
                let binding = binding.restore_for(&member)?;
                let detach_cell = detach_cell.restore()?;
                validate_live_pair(&member, binding, &detach_cell)?;
                Ok(RestoredParticipantLifecycle::Live {
                    member,
                    binding,
                    detach_cell,
                })
            }
        }
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

#[allow(clippy::suspicious_operation_groupings)]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrdinaryBindingAuthorityRestore {
    /// Exact binding installed by ordinary attach.
    pub binding: ActiveBinding,
    /// Durable no-marker cursor carried through the attach.
    pub through_seq: DeliverySeq,
}

impl OrdinaryBindingAuthorityRestore {
    /// Rebuilds the opaque ordinary-attach authority from its complete fields.
    #[must_use]
    pub const fn restore(self) -> OrdinaryBindingAuthority {
        OrdinaryBindingAuthority::new(self.binding, self.through_seq)
    }
}

/// Raw predecessor fields proving exact marker delivery.
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
    /// Rebuilds the marker-delivery witness.
    #[must_use]
    pub const fn restore(self) -> MarkerDelivery {
        MarkerDelivery::new(
            self.participant_id,
            self.binding_epoch,
            self.marker_delivery_seq,
        )
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
    ) -> Result<ParticipantCursorProgress, StorageRestoreError> {
        if self.participant_id != self.delivery.participant_id
            || self.binding_epoch != self.delivery.binding_epoch
            || self.marker_delivery_seq != self.delivery.marker_delivery_seq
            || self.through_seq != self.marker_delivery_seq
        {
            return Err(StorageRestoreError::StoredEdgeProvenance);
        }
        let marker = self.delivery.restore();
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
        let progress = self.progress.restore_with_debt(debt)?;
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
        let marker = self.delivery.restore();
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
    pub fn restore(self) -> Result<OrdinaryBindingFate, StorageRestoreError> {
        let authority = self.authority.restore();
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
    pub fn restore(self) -> Result<FencedAttachCommit, StorageRestoreError> {
        let debt =
            ClosureDebt::new(self.predecessor_debt).ok_or(StorageRestoreError::ClosureDebt)?;
        let predecessor = self.predecessor.restore_with_debt(debt)?;
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
    pub fn restore(self) -> Result<RecoveredBindingFate, StorageRestoreError> {
        let commit = self.fenced_attach.restore()?;
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
    pub fn restore(self) -> Result<PendingRecoveredCursorRelease, StorageRestoreError> {
        let authority = self.fate.restore()?;
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
    fn restore_state(self, debt: ClosureDebt) -> Result<ClosureState, StorageRestoreError> {
        match self {
            Self::Ordinary(provenance) => Ok(provenance.restore()?.into_direct_state(debt)),
            Self::RecoveredDirect {
                fate,
                resulting_debt,
            } => {
                let authority = fate.restore()?;
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
                let state = completion.restore(pending.restore()?)?;
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
    fn restore_with_debt(self, debt: ClosureDebt) -> Result<StoredEdge, StorageRestoreError> {
        match self {
            Self::ObserverProjection { through_seq } => Ok(StoredEdge::ObserverProjection(
                ObserverProjection::new(through_seq),
            )),
            Self::PhysicalCompaction {
                from_floor,
                through_seq,
            } => PhysicalCompaction::new(from_floor, through_seq)
                .map(StoredEdge::PhysicalCompaction)
                .ok_or(StorageRestoreError::StoredEdgeProvenance),
            Self::MarkerDelivery(value) => Ok(StoredEdge::MarkerDelivery(value.restore())),
            Self::ParticipantCursorProgressContinuous {
                participant_id,
                binding_epoch,
                through_seq,
                authority,
            } => ParticipantCursorProgress::restore_continuous(
                authority.restore(),
                participant_id,
                binding_epoch,
                through_seq,
            )
            .map(StoredEdge::ParticipantCursorProgress)
            .ok_or(StorageRestoreError::StoredEdgeProvenance),
            Self::ParticipantCursorProgressMarker(value) => value
                .restore_with_debt(debt)
                .map(StoredEdge::ParticipantCursorProgress),
            Self::DetachedCredentialRecovery(value) => value
                .restore_with_debt(debt)
                .map(StoredEdge::DetachedCredentialRecovery),
            Self::DetachedMarkerRelease(value) => value
                .restore_with_debt(debt)
                .map(StoredEdge::DetachedMarkerRelease),
            Self::DetachedCursorRelease {
                participant_id,
                last_dead_binding_epoch,
                provenance,
            } => {
                let state = provenance.restore_state(debt)?;
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
    /// Validates and rebuilds one closure state.
    ///
    /// # Errors
    ///
    /// Returns [`StorageRestoreError`] for zero owed debt, an invalid range, or
    /// any opaque edge whose supplied predecessor provenance does not match.
    pub fn restore(self) -> Result<ClosureState, StorageRestoreError> {
        match self {
            Self::Clear => Ok(ClosureState::Clear),
            Self::Owed { debt, edge } => {
                let debt = ClosureDebt::new(debt).ok_or(StorageRestoreError::ClosureDebt)?;
                Ok(ClosureState::Owed {
                    debt,
                    edge: edge.restore_with_debt(debt)?,
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
