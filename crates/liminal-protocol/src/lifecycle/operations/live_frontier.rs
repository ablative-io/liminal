//! Move-only executable ownership for live lifecycle frontier transitions.
//!
//! Storage supplies canonical encoded row charges, but every participant,
//! binding, cursor, retained row, and aggregate-claim transition is derived
//! inside the protocol from an existing sealed operation commit.

use alloc::{boxed::Box, vec, vec::Vec};

use crate::{algebra::ResourceVector, wire::RecordAdmission};

use super::super::{
    AttachCommit, AttachTransition, BindingState, ClaimFrontiers, ClosureAccounting, ClosureDebt,
    CommittedDetachTransition, DebtCompletion, DetachCell, DetachedCredentialRecovery, Event,
    FencedAttachCommit, FrontierBinding, FrontierParticipant, IdentityState,
    InitialEnrollmentFrontierCommit, LeaveCommitError, LeaveCommitParameters, LiveMember,
    MarkerAckCommit, NonzeroParticipantAckCommit, ObserverProgressProjection, OrderLedger,
    ParticipantAckCommit, PendingFinalization, PendingLeaveCommitParameters,
    PrepareLeaveAuthorityError, RetainedCausalRecord, RetainedCausalRecordKind, SequenceLedger,
    StoredEdge, VerifiedLeaveRequest,
    claim_frontier::{BindingTerminalOwner, FencedMarkerSourceRecord, LiveFrontierTransitionError},
    commit_leave, commit_pending_leave,
};
use super::{
    InitialEnrollmentOperationCommit, MarkerDeliveryProjection, MarkerDrainCommit,
    RecordAdmissionPersistenceParts, RetainedRecordCharge, UnchangedRecordAdmission,
};

mod binding_fate_transition;
mod ledger;
mod state;
pub(super) use binding_fate_transition::BindingFateOwnerPlan;
use ledger::{
    detach_order, detach_sequence, detached_attach_order, detached_attach_sequence,
    enrollment_order, enrollment_sequence, superseding_attach_order, superseding_attach_sequence,
};
use state::{
    accounting_after_fenced_attach, accounting_after_leave, accounting_after_marker_ack,
    accounting_after_rows, retained_attached, retained_terminal,
};

/// Complete executable frontier, closure-accounting, and keyed-retention owner.
///
/// The owner is intentionally move-only. It is the only live mutation input and
/// never exposes a constructor from independent frontier/accounting components.
/// Frontier, closure, retained charges, and participant history therefore cannot
/// be cloned or recombined from different owners:
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::LiveFrontierOwner;
///
/// fn clone_frontier(owner: &LiveFrontierOwner) -> LiveFrontierOwner {
///     owner.clone()
/// }
/// ```
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::LiveFrontierOwner;
///
/// fn splice(left: &mut LiveFrontierOwner, right: LiveFrontierOwner) {
///     left.frontiers = right.frontiers;
///     left.closure_accounting = right.closure_accounting;
///     left.retained_charges = right.retained_charges;
/// }
/// ```
#[derive(Debug, PartialEq, Eq)]
pub struct LiveFrontierOwner {
    frontiers: ClaimFrontiers,
    closure_accounting: ClosureAccounting,
    retained_charges: Vec<RetainedRecordCharge>,
    retained_record_limit: u64,
}

impl LiveFrontierOwner {
    /// Acquires live ownership from the protocol's atomic initial-enrollment result.
    #[must_use]
    pub fn from_initial_enrollment<F>(
        initial: InitialEnrollmentFrontierCommit<F>,
        retained_record_limit: u64,
    ) -> (InitialEnrollmentOperationCommit<F>, Self) {
        let (operation, frontiers, closure_accounting, attached_charge) =
            initial.into_conversation_parts();
        let attached = operation.enrollment().attached;
        let retained_charges = vec![RetainedRecordCharge::new(
            attached.delivery_seq(),
            attached.admission_order(),
            attached_charge,
        )];
        (
            operation,
            Self {
                frontiers,
                closure_accounting,
                retained_charges,
                retained_record_limit,
            },
        )
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(in crate::lifecycle) const fn from_test_parts(
        frontiers: ClaimFrontiers,
        closure_accounting: ClosureAccounting,
        retained_charges: Vec<RetainedRecordCharge>,
        retained_record_limit: u64,
    ) -> Self {
        Self {
            frontiers,
            closure_accounting,
            retained_charges,
            retained_record_limit,
        }
    }

    /// Test-support-only checked capacity extension applied after a real
    /// terminal selector has already chosen Pending. It cannot affect that
    /// disposition; the caller supplies the exact encoded finalizer charges.
    ///
    /// # Errors
    ///
    /// Returns an error if exact capacity arithmetic or reconstruction fails.
    #[cfg(any(test, feature = "test-support"))]
    pub fn with_pending_finalizer_test_capacity(
        mut self,
        finalizer_rows: u64,
        finalizer_charge: ResourceVector,
    ) -> Result<Self, &'static str> {
        let retained_count = u64::try_from(self.retained_charges.len())
            .map_err(|_| "retained record count exceeds u64")?;
        self.retained_record_limit = retained_count
            .checked_add(finalizer_rows)
            .ok_or("pending finalizer retained-record capacity overflow")?;
        let current = self.closure_accounting;
        let configured = current.configured_cap();
        let configured = ResourceVector::new(
            configured
                .entries
                .checked_add(finalizer_charge.entries)
                .ok_or("pending finalizer entry capacity overflow")?,
            configured
                .bytes
                .checked_add(finalizer_charge.bytes)
                .ok_or("pending finalizer byte capacity overflow")?,
        );
        self.closure_accounting = ClosureAccounting::try_new(
            current.state(),
            current.marker_capacity_credits(),
            current.marker_anchors(),
            current.edge_sequence_claims(),
            current.edge_order_position_claims(),
            current.edge_k_remaining(),
            current.baseline(),
            configured,
            current.episode_churn_used(),
            current.episode_churn_limit(),
        )
        .map_err(|_| "pending finalizer closure capacity extension refused")?;
        Ok(self)
    }

    /// Borrows the coupled claim frontiers.
    #[must_use]
    pub const fn frontiers(&self) -> &ClaimFrontiers {
        &self.frontiers
    }

    /// Returns complete current closure accounting.
    #[must_use]
    pub const fn closure_accounting(&self) -> ClosureAccounting {
        self.closure_accounting
    }

    /// Borrows canonical keyed charges for the retained suffix.
    #[must_use]
    pub fn retained_charges(&self) -> &[RetainedRecordCharge] {
        &self.retained_charges
    }

    /// Returns the signed retained causal-row cap.
    #[must_use]
    pub const fn retained_record_limit(&self) -> u64 {
        self.retained_record_limit
    }

    /// Consumes the complete owner for `RecordAdmission`, Leave, or persistence.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ClaimFrontiers,
        ClosureAccounting,
        Vec<RetainedRecordCharge>,
        u64,
    ) {
        (
            self.frontiers,
            self.closure_accounting,
            self.retained_charges,
            self.retained_record_limit,
        )
    }

    /// Restores the exact owner returned by a non-committing admission and
    /// recovers the same request for a same-lock retry.
    #[must_use]
    pub fn from_unchanged_record_admission<EF, V, LF>(
        unchanged: UnchangedRecordAdmission<'_, EF, V, LF>,
        retained_record_limit: u64,
    ) -> (Self, RecordAdmission, ResourceVector) {
        let (prestate, encoded_record_charge) = unchanged.into_parts();
        let (request, frontiers, closure_accounting, retained_charges) =
            prestate.into_live_owner_parts();
        (
            Self {
                frontiers,
                closure_accounting,
                retained_charges,
                retained_record_limit,
            },
            request,
            encoded_record_charge,
        )
    }

    /// Acquires the exact owner from the complete sealed successful
    /// `RecordAdmission` persistence authority.
    #[must_use]
    pub fn from_record_admission_persistence(
        persistence: RecordAdmissionPersistenceParts,
        retained_record_limit: u64,
    ) -> Self {
        Self {
            frontiers: persistence.frontiers,
            closure_accounting: persistence.accounting,
            retained_charges: persistence.retained_charges,
            retained_record_limit,
        }
    }

    /// Acquires the exact post-drain owner and durable marker successor.
    #[must_use]
    pub fn from_marker_drain(
        commit: MarkerDrainCommit,
        retained_record_limit: u64,
    ) -> (Self, StoredEdge, MarkerDeliveryProjection) {
        let (frontiers, closure_accounting, retained_charges, successor, projection) =
            commit.into_parts();
        (
            Self {
                frontiers,
                closure_accounting,
                retained_charges,
                retained_record_limit,
            },
            successor,
            projection,
        )
    }

    pub(super) fn commit_binding_terminal_candidate(
        self,
        active_binding: super::super::ActiveBinding,
        admission_order: super::super::AdmissionOrder,
        delivery_seq: crate::wire::DeliverySeq,
        charge: RetainedRecordCharge,
    ) -> Result<Self, Box<(Self, LiveFrontierError)>> {
        let mut active = self.frontiers.active_identities().participants().to_vec();
        let Some(participant) = active
            .iter_mut()
            .find(|participant| participant.participant_index() == active_binding.participant_id)
        else {
            return Err(Box::new((self, LiveFrontierError::Authority)));
        };
        if participant.binding() != FrontierBinding::Bound(active_binding.binding_epoch) {
            return Err(Box::new((self, LiveFrontierError::Authority)));
        }
        *participant = FrontierParticipant::new(
            participant.participant_index(),
            participant.cursor(),
            FrontierBinding::Detached(active_binding.binding_epoch),
        );
        let row = RetainedCausalRecord {
            delivery_seq,
            admission_order,
            kind: RetainedCausalRecordKind::BindingTerminal(super::super::BindingTerminalOwner {
                participant_index: active_binding.participant_id,
                binding_epoch: active_binding.binding_epoch,
            }),
        };
        let Some(sequence) = detach_sequence(self.frontiers.sequence().ledger(), delivery_seq)
        else {
            return Err(Box::new((self, LiveFrontierError::Frontier)));
        };
        let Some(order) = detach_order(
            self.frontiers.order().ledger(),
            admission_order.transaction_order(),
        ) else {
            return Err(Box::new((self, LiveFrontierError::Frontier)));
        };
        match transition(self, (), active, &[row], vec![charge], sequence, order) {
            Ok(committed) => {
                let ((), owner) = committed.into_parts();
                Ok(owner)
            }
            Err(failure) => {
                let error = failure.error();
                let ((), owner) = failure.into_parts();
                Err(Box::new((owner, error)))
            }
        }
    }

    pub(super) fn pend_binding_terminal_candidate(
        self,
        active_binding: super::super::ActiveBinding,
        admission_order: super::super::AdmissionOrder,
        delivery_seq: crate::wire::DeliverySeq,
    ) -> Result<Self, Box<(Self, LiveFrontierError)>> {
        let Some(order) = detach_order(
            self.frontiers.order().ledger(),
            admission_order.transaction_order(),
        ) else {
            return Err(Box::new((self, LiveFrontierError::Frontier)));
        };
        let Self {
            frontiers,
            closure_accounting,
            retained_charges,
            retained_record_limit,
        } = self;
        match frontiers.apply_pending_binding_terminal(
            active_binding.participant_id,
            active_binding.binding_epoch,
            delivery_seq,
            admission_order,
            order,
        ) {
            Ok(frontiers) => Ok(Self {
                frontiers,
                closure_accounting,
                retained_charges,
                retained_record_limit,
            }),
            Err(failure) => {
                let (frontiers, error) = *failure;
                Err(Box::new((
                    Self {
                        frontiers,
                        closure_accounting,
                        retained_charges,
                        retained_record_limit,
                    },
                    map_frontier_error(error),
                )))
            }
        }
    }

    /// Commits the exact first pending binding-terminal candidate as its own
    /// candidate transaction — the R-A2 candidate-lane terminal drain.
    ///
    /// The caller supplies only the pending finalization authority it already
    /// owns and the canonical keyed terminal charge; the candidate itself, its
    /// delivery sequence, and its already-allocated order major are derived
    /// from the coupled frontiers. Retention transitions through the same
    /// accounting the live transitions use, and the resulting owner is exactly
    /// the poststate an immediately-committed terminal would have produced:
    /// the participant frontier stays detached at its dead epoch.
    ///
    /// The retained-row CAP is deliberately not re-checked here: the terminal
    /// pended exactly because the cap could not admit its row at fate time
    /// ([`PreparedBindingTerminal::admit`](super::PreparedBindingTerminal::admit)),
    /// and pending DEFERRED that exact reserved row rather than discarding it.
    /// R-A2 prescribes the drain transaction's retention transition
    /// unconditionally — an accepted terminal fate can never become
    /// unsequencable — so the drain honors the deferred reservation even while
    /// the suffix rests at its cap. The overage is bounded by the
    /// open-candidate count, itself bounded by the contract's identity-slot
    /// candidate bound.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owner when the first candidate is absent or not
    /// this pending terminal, the charge does not name the candidate's exact
    /// keyed row, or the resulting retention/closure accounting is invalid.
    pub fn drain_pending_terminal(
        self,
        pending: PendingFinalization,
        terminal_charge: RetainedRecordCharge,
    ) -> Result<DrainedPendingTerminal, Box<PendingTerminalDrainRefused>> {
        let Some(expected_sequence) = self
            .frontiers
            .sequence()
            .ledger()
            .high_watermark()
            .checked_add(1)
        else {
            return drain_refusal(self, LiveFrontierError::Frontier);
        };
        if terminal_charge.delivery_seq() != expected_sequence
            || terminal_charge.admission_order() != pending.admission_order()
            || terminal_charge.encoded_charge().entries != 1
        {
            return drain_refusal(self, LiveFrontierError::RetainedCharge);
        }
        if self
            .retained_charges
            .len()
            .checked_add(1)
            .and_then(|len| u64::try_from(len).ok())
            .is_none()
        {
            return drain_refusal(self, LiveFrontierError::RetainedRecordLimit);
        }
        let Some(accounting) = accounting_after_rows(self.closure_accounting, &[terminal_charge])
        else {
            return drain_refusal(self, LiveFrontierError::ClosureAccounting);
        };
        let Self {
            frontiers,
            closure_accounting,
            mut retained_charges,
            retained_record_limit,
        } = self;
        let expected_owner = BindingTerminalOwner {
            participant_index: pending.participant_id(),
            binding_epoch: pending.binding_epoch(),
        };
        let (frontiers, record) = match frontiers
            .drain_first_binding_terminal(expected_owner, pending.admission_order())
        {
            Ok(drained) => drained,
            Err(failure) => {
                let (frontiers, error) = *failure;
                return drain_refusal(
                    Self {
                        frontiers,
                        closure_accounting,
                        retained_charges,
                        retained_record_limit,
                    },
                    map_frontier_error(error),
                );
            }
        };
        retained_charges.push(terminal_charge);
        let projection =
            ObserverProgressProjection::new(pending.conversation_id(), record.delivery_seq);
        Ok(DrainedPendingTerminal {
            owner: Self {
                frontiers,
                closure_accounting: accounting,
                retained_charges,
                retained_record_limit,
            },
            projection,
        })
    }

    /// Retains this move-only owner and the exact recovery while its durable
    /// marker source row is read and validated.
    ///
    /// # Errors
    /// Returns the unchanged owner and recovery when the fully restored frontier
    /// does not contain their exact delivered marker occurrence.
    pub fn retain_fenced_marker_source(
        self,
        recovery: DetachedCredentialRecovery,
    ) -> Result<RetainedFencedMarkerSource, Box<FencedMarkerSourceRetentionRefused>> {
        let Some(source) = self.frontiers.fenced_marker_source(recovery) else {
            return Err(Box::new(FencedMarkerSourceRetentionRefused {
                owner: self,
                recovery,
            }));
        };
        Ok(RetainedFencedMarkerSource {
            owner: self,
            recovery,
            expectation: FencedMarkerSourceExpectation { source },
        })
    }

    /// Consumes the complete owner and the exact descriptive inputs to mint one
    /// fenced attach proof from the selected retained marker occurrence.
    ///
    /// This is the sole public production mint. The caller cannot supply a raw
    /// marker token: the owner removes it from its fully validated frontiers.
    /// Refusal returns this owner and every input unchanged after reinstalling
    /// that same occurrence authority, allowing serial retry but no fork.
    #[must_use]
    pub fn mint_fenced_attach(
        mut self,
        marker_source_sequence: u64,
        recovery: DetachedCredentialRecovery,
        debt: ClosureDebt,
        event: Event,
        successor: DebtCompletion,
    ) -> MintFencedAttachResult {
        let Some(record) = self.frontiers.take_fenced_marker_record(recovery) else {
            return MintFencedAttachResult::MintRefused(Box::new(MintFencedAttachRefused {
                owner: self,
                marker_source_sequence,
                recovery,
                debt,
                event,
                successor,
                reason: FencedAttachMintRefusalReason::MarkerAuthority,
            }));
        };
        match recovery.fenced_attach(record, debt, event, successor) {
            Ok(proof) => MintFencedAttachResult::Minted(Box::new(MintedFencedAttach {
                owner_without_marker_authority: self,
                proof,
            })),
            Err(refusal) => {
                self.frontiers
                    .reinstall_fenced_marker_record((*refusal).into_record());
                MintFencedAttachResult::MintRefused(Box::new(MintFencedAttachRefused {
                    owner: self,
                    marker_source_sequence,
                    recovery,
                    debt,
                    event,
                    successor,
                    reason: FencedAttachMintRefusalReason::ProofInputs,
                }))
            }
        }
    }
}

/// Complete atomic candidate-lane terminal drain commit.
///
/// The transitioned owner and the protocol-produced observer projection share
/// one sealed predecessor and cannot be recombined from unrelated drains.
#[derive(Debug, PartialEq, Eq)]
pub struct DrainedPendingTerminal {
    owner: LiveFrontierOwner,
    projection: ObserverProgressProjection,
}

impl DrainedPendingTerminal {
    /// Consumes the atomic drain into its owner and exact typed projection.
    #[must_use]
    pub fn into_parts(self) -> (LiveFrontierOwner, ObserverProgressProjection) {
        (self.owner, self.projection)
    }
}

/// Failed candidate-lane terminal drain retaining the unchanged owner.
#[derive(Debug, PartialEq, Eq)]
pub struct PendingTerminalDrainRefused {
    owner: LiveFrontierOwner,
    error: LiveFrontierError,
}

impl PendingTerminalDrainRefused {
    /// Returns the exact typed refusal.
    #[must_use]
    pub const fn error(&self) -> LiveFrontierError {
        self.error
    }

    /// Recovers the unchanged complete owner.
    #[must_use]
    pub fn into_owner(self) -> LiveFrontierOwner {
        self.owner
    }
}

fn drain_refusal(
    owner: LiveFrontierOwner,
    error: LiveFrontierError,
) -> Result<DrainedPendingTerminal, Box<PendingTerminalDrainRefused>> {
    Err(Box::new(PendingTerminalDrainRefused { owner, error }))
}

/// Exact protocol-recomputed marker facts a durable source row must match.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FencedMarkerSourceExpectation {
    source: FencedMarkerSourceRecord,
}

impl FencedMarkerSourceExpectation {
    /// Returns the conversation owning the durable marker source.
    #[must_use]
    pub const fn conversation_id(self) -> u64 {
        self.source.conversation_id
    }

    /// Returns the marker's durable delivery sequence.
    #[must_use]
    pub const fn marker_delivery_seq(self) -> u64 {
        self.source.delivery_seq
    }

    /// Returns the marker's immutable causal key.
    #[must_use]
    pub const fn admission_order(self) -> super::super::AdmissionOrder {
        self.source.admission_order
    }

    /// Returns the permanent marker owner.
    #[must_use]
    pub const fn participant_id(self) -> u64 {
        self.source.participant_id
    }

    /// Returns the marker's immutable provenance.
    #[must_use]
    pub const fn provenance(self) -> super::super::MarkerProvenance {
        self.source.provenance
    }

    /// Returns the historically validated delivery target.
    #[must_use]
    pub const fn target_binding(self) -> FrontierBinding {
        self.source.target_binding
    }
}

/// Move-only owner/recovery pair held across one bounded durable source read.
#[derive(Debug, PartialEq, Eq)]
pub struct RetainedFencedMarkerSource {
    owner: LiveFrontierOwner,
    recovery: DetachedCredentialRecovery,
    expectation: FencedMarkerSourceExpectation,
}

impl RetainedFencedMarkerSource {
    /// Returns the protocol-recomputed facts the durable row must match.
    #[must_use]
    pub const fn expectation(&self) -> FencedMarkerSourceExpectation {
        self.expectation
    }

    /// Returns the unchanged owner and recovery after source validation.
    #[must_use]
    pub fn into_parts(self) -> (LiveFrontierOwner, DetachedCredentialRecovery) {
        (self.owner, self.recovery)
    }
}

/// Refused source retention with the owner and recovery unchanged.
#[derive(Debug, PartialEq, Eq)]
pub struct FencedMarkerSourceRetentionRefused {
    owner: LiveFrontierOwner,
    recovery: DetachedCredentialRecovery,
}

impl FencedMarkerSourceRetentionRefused {
    /// Returns the unchanged owner and recovery after retention refusal.
    #[must_use]
    pub fn into_parts(self) -> (LiveFrontierOwner, DetachedCredentialRecovery) {
        (self.owner, self.recovery)
    }
}

/// Exact reason the owner could not mint a fenced attach proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FencedAttachMintRefusalReason {
    /// The validated frontier did not own the selected delivered marker record,
    /// or that record authority was already consumed.
    MarkerAuthority,
    /// Marker authority existed, but recovery/event/successor inputs disagreed.
    ProofInputs,
}

/// Successful one-use fenced proof mint.
#[derive(Debug, PartialEq, Eq)]
pub struct MintedFencedAttach {
    owner_without_marker_authority: LiveFrontierOwner,
    proof: FencedAttachCommit,
}

impl MintedFencedAttach {
    /// Consumes the result into the owner with spent marker authority and the
    /// sole proof admitted to the downstream by-value chain.
    #[must_use]
    pub fn into_parts(self) -> (LiveFrontierOwner, FencedAttachCommit) {
        (self.owner_without_marker_authority, self.proof)
    }
}

/// Failed one-use fenced proof mint with unchanged retry authority and inputs.
#[derive(Debug, PartialEq, Eq)]
pub struct MintFencedAttachRefused {
    owner: LiveFrontierOwner,
    marker_source_sequence: u64,
    recovery: DetachedCredentialRecovery,
    debt: ClosureDebt,
    event: Event,
    successor: DebtCompletion,
    reason: FencedAttachMintRefusalReason,
}

impl MintFencedAttachRefused {
    /// Returns the typed refusal cause.
    #[must_use]
    pub const fn reason(&self) -> FencedAttachMintRefusalReason {
        self.reason
    }

    /// Consumes the refusal into the unchanged owner and all original inputs.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        LiveFrontierOwner,
        u64,
        DetachedCredentialRecovery,
        ClosureDebt,
        Event,
        DebtCompletion,
    ) {
        (
            self.owner,
            self.marker_source_sequence,
            self.recovery,
            self.debt,
            self.event,
            self.successor,
        )
    }
}

/// Complete result of the sole production fenced proof mint.
#[derive(Debug, PartialEq, Eq)]
pub enum MintFencedAttachResult {
    /// Exactly one marker authority was spent and one proof was minted.
    Minted(Box<MintedFencedAttach>),
    /// No proof was minted; the same authority and inputs are serially retryable.
    MintRefused(Box<MintFencedAttachRefused>),
}

/// Complete move-only settled Leave result: tombstone and executable owner.
#[derive(Debug, PartialEq, Eq)]
pub struct LiveLeaveCommit<EF, V, LF> {
    identity: IdentityState<EF, V, LF>,
    owner: LiveFrontierOwner,
}

impl<EF, V, LF> LiveLeaveCommit<EF, V, LF> {
    /// Projects permanent Leave's exact protocol-committed `Left` sequence.
    #[must_use]
    pub const fn observer_progress_projection(&self) -> Option<ObserverProgressProjection> {
        let IdentityState::Retired(retired) = &self.identity else {
            return None;
        };
        let committed = retired.committed_result();
        Some(ObserverProgressProjection::new(
            committed.conversation_id(),
            committed.left_delivery_seq(),
        ))
    }

    /// Consumes the atomic result into its inseparable tombstone and owner.
    #[must_use]
    pub fn into_parts(self) -> (IdentityState<EF, V, LF>, LiveFrontierOwner) {
        (self.identity, self.owner)
    }
}

/// Typed failure of the protocol-owned settled Leave live transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LiveLeaveError {
    /// Claim-frontier Leave authority could not be prepared.
    Prepare(PrepareLeaveAuthorityError),
    /// Membership retirement rejected inconsistent authority.
    Commit(LeaveCommitError),
    /// Canonical Left-row charge did not name the protocol-produced row.
    RetainedCharge,
    /// Resulting retained-row count exceeded the signed cap.
    RetainedRecordLimit,
    /// Resulting closure accounting exceeded configured capacity.
    ClosureAccounting,
    /// Leave did not produce a retired identity.
    Identity,
}

/// Commits settled bound or detached Leave through one complete live owner.
///
/// # Errors
///
/// Returns [`LiveLeaveError`] when preparation or retirement authority is
/// inconsistent, the caller's keyed Left charge does not match the committed
/// row, or the resulting retention/closure accounting exceeds its authority.
pub fn commit_settled_leave_frontier<EF, V, LF, D>(
    owner: LiveFrontierOwner,
    member: LiveMember<EF>,
    binding: BindingState,
    detach_cell: DetachCell<D>,
    verified: VerifiedLeaveRequest<V, LF>,
    left_delivery_seq: u64,
    left_charge: RetainedRecordCharge,
) -> Result<LiveLeaveCommit<EF, V, LF>, LiveLeaveError> {
    let LiveFrontierOwner {
        frontiers,
        closure_accounting,
        mut retained_charges,
        retained_record_limit,
    } = owner;
    let retired_marker_charge =
        retired_marker_charge(&frontiers, &retained_charges, member.participant_id())?;
    let authority = frontiers
        .prepare_settled_leave_authority(&member, binding)
        .map_err(LiveLeaveError::Prepare)?;
    let commit = commit_leave(
        member,
        binding,
        detach_cell,
        verified,
        authority,
        LeaveCommitParameters { left_delivery_seq },
    )
    .map_err(LiveLeaveError::Commit)?;
    let (identity, frontiers) = commit.into_parts();
    let IdentityState::Retired(retired) = &identity else {
        return Err(LiveLeaveError::Identity);
    };
    if left_charge.delivery_seq() != retired.committed_result().left_delivery_seq()
        || left_charge.admission_order() != retired.left_admission_order()
        || left_charge.encoded_charge().entries != 1
    {
        return Err(LiveLeaveError::RetainedCharge);
    }
    retained_charges.push(left_charge);
    retained_charges.sort_unstable_by_key(|charge| charge.delivery_seq());
    let retained_len = u64::try_from(frontiers.retained_records().len())
        .map_err(|_| LiveLeaveError::RetainedRecordLimit)?;
    if retained_len > retained_record_limit
        || retained_charges.len() != frontiers.retained_records().len()
    {
        return Err(LiveLeaveError::RetainedRecordLimit);
    }
    let closure_accounting =
        accounting_after_leave(closure_accounting, &[left_charge], retired_marker_charge)
            .ok_or(LiveLeaveError::ClosureAccounting)?;
    Ok(LiveLeaveCommit {
        identity,
        owner: LiveFrontierOwner {
            frontiers,
            closure_accounting,
            retained_charges,
            retained_record_limit,
        },
    })
}

/// Commits a pending binding terminal immediately before Leave through one
/// complete live owner.
///
/// # Errors
///
/// Returns [`LiveLeaveError`] when pending preparation or retirement authority
/// is inconsistent, either caller charge does not match its protocol-produced
/// row, or resulting retention/closure accounting exceeds its authority.
pub fn commit_pending_leave_frontier<EF, V, LF, D>(
    owner: LiveFrontierOwner,
    member: LiveMember<EF>,
    pending: PendingFinalization,
    detach_cell: DetachCell<D>,
    verified: VerifiedLeaveRequest<V, LF>,
    parameters: PendingLeaveCommitParameters,
    charges: [RetainedRecordCharge; 2],
) -> Result<LiveLeaveCommit<EF, V, LF>, LiveLeaveError> {
    let [terminal_charge, left_charge] = charges;
    let terminal_delivery_seq = parameters.terminal_delivery_seq;
    let LiveFrontierOwner {
        frontiers,
        closure_accounting,
        mut retained_charges,
        retained_record_limit,
    } = owner;
    let retired_marker_charge =
        retired_marker_charge(&frontiers, &retained_charges, member.participant_id())?;
    let authority = frontiers
        .prepare_pending_leave_authority(&member, pending)
        .map_err(LiveLeaveError::Prepare)?;
    let commit = commit_pending_leave(
        member,
        pending,
        detach_cell,
        verified,
        authority,
        parameters,
    )
    .map_err(LiveLeaveError::Commit)?;
    let (identity, frontiers) = commit.into_parts();
    let IdentityState::Retired(retired) = &identity else {
        return Err(LiveLeaveError::Identity);
    };
    if retired.committed_result().prior_terminal_delivery_seq() != Some(terminal_delivery_seq)
        || terminal_charge.delivery_seq() != terminal_delivery_seq
        || terminal_charge.admission_order() != pending.admission_order()
        || terminal_charge.encoded_charge().entries != 1
        || left_charge.delivery_seq() != retired.committed_result().left_delivery_seq()
        || left_charge.admission_order() != retired.left_admission_order()
        || left_charge.encoded_charge().entries != 1
    {
        return Err(LiveLeaveError::RetainedCharge);
    }
    retained_charges.extend([terminal_charge, left_charge]);
    retained_charges.sort_unstable_by_key(|charge| charge.delivery_seq());
    let retained_len = u64::try_from(frontiers.retained_records().len())
        .map_err(|_| LiveLeaveError::RetainedRecordLimit)?;
    if retained_len > retained_record_limit {
        return Err(LiveLeaveError::RetainedRecordLimit);
    }
    if retained_charges.len() != frontiers.retained_records().len() {
        return Err(LiveLeaveError::RetainedCharge);
    }
    let closure_accounting = accounting_after_leave(
        closure_accounting,
        &[terminal_charge, left_charge],
        retired_marker_charge,
    )
    .ok_or(LiveLeaveError::ClosureAccounting)?;
    Ok(LiveLeaveCommit {
        identity,
        owner: LiveFrontierOwner {
            frontiers,
            closure_accounting,
            retained_charges,
            retained_record_limit,
        },
    })
}

fn retired_marker_charge(
    frontiers: &ClaimFrontiers,
    retained_charges: &[RetainedRecordCharge],
    participant_id: crate::wire::ParticipantId,
) -> Result<Option<RetainedRecordCharge>, LiveLeaveError> {
    let marker_sequence = frontiers
        .retained_marker_records()
        .iter()
        .find_map(|record| {
            matches!(
                record.kind,
                RetainedCausalRecordKind::CompactionMarker {
                    participant_index,
                    ..
                } if participant_index == participant_id
            )
            .then_some(record.delivery_seq)
        });
    let Some(marker_sequence) = marker_sequence else {
        return Ok(None);
    };
    retained_charges
        .iter()
        .copied()
        .find(|charge| charge.delivery_seq() == marker_sequence)
        .map(Some)
        .ok_or(LiveLeaveError::RetainedCharge)
}

/// Exact charges for a credential attach's one or two retained rows.
#[derive(Debug, PartialEq, Eq)]
pub struct AttachFrontierCharges {
    terminal: Option<RetainedRecordCharge>,
    attached: RetainedRecordCharge,
    seal: LiveTransitionInputSeal,
}

#[derive(Debug, PartialEq, Eq)]
enum LiveTransitionInputSeal {
    Validated,
}

impl AttachFrontierCharges {
    /// Couples the canonical `Attached` charge with an optional terminal charge.
    #[must_use]
    pub const fn new(
        terminal: Option<RetainedRecordCharge>,
        attached: RetainedRecordCharge,
    ) -> Self {
        Self {
            terminal,
            attached,
            seal: LiveTransitionInputSeal::Validated,
        }
    }

    const fn into_parts(self) -> (Option<RetainedRecordCharge>, RetainedRecordCharge) {
        let Self {
            terminal,
            attached,
            seal,
        } = self;
        match seal {
            LiveTransitionInputSeal::Validated => (terminal, attached),
        }
    }
}

/// A typed lifecycle commit paired with its complete post-transition owner.
#[derive(Debug, PartialEq, Eq)]
pub struct LiveFrontierCommit<T> {
    operation: T,
    owner: LiveFrontierOwner,
}

impl<T> LiveFrontierCommit<T> {
    /// Borrows the exact typed lifecycle commit.
    #[must_use]
    pub const fn operation(&self) -> &T {
        &self.operation
    }

    /// Borrows the complete post-transition owner.
    #[must_use]
    pub const fn owner(&self) -> &LiveFrontierOwner {
        &self.owner
    }

    /// Consumes the atomic transition for durability publication.
    #[must_use]
    pub fn into_parts(self) -> (T, LiveFrontierOwner) {
        (self.operation, self.owner)
    }
}

/// Failed live transition retaining the unchanged complete owner and operation.
#[derive(Debug, PartialEq, Eq)]
pub struct LiveFrontierFailure<T> {
    error: LiveFrontierError,
    operation: T,
    owner: LiveFrontierOwner,
}

impl<T> LiveFrontierFailure<T> {
    /// Returns the exact typed transition failure.
    #[must_use]
    pub const fn error(&self) -> LiveFrontierError {
        self.error
    }

    /// Recovers the unchanged owner and intact operation commit.
    #[must_use]
    pub fn into_parts(self) -> (T, LiveFrontierOwner) {
        (self.operation, self.owner)
    }
}

/// Failure selected while coupling a sealed lifecycle commit to live ownership.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LiveFrontierError {
    /// Commit and live owner name different authority.
    Authority,
    /// A mandatory immutable/recovery transition has precedence.
    Precedence,
    /// Canonical keyed row charges differ from the commit-derived retained rows.
    RetainedCharge,
    /// The retained causal-row cap would be exceeded.
    RetainedRecordLimit,
    /// Aggregate claim arithmetic or exact owner reconstruction failed.
    Frontier,
    /// Resulting closure accounting is invalid or outside its signed capacity.
    ClosureAccounting,
}

/// Result of coupling any typed lifecycle commit to live frontier ownership.
pub type LiveFrontierResult<T> = Result<LiveFrontierCommit<T>, Box<LiveFrontierFailure<T>>>;

/// Applies a subsequent enrollment to the complete live owner.
///
/// # Errors
///
/// Returns a failure retaining the unchanged owner and intact enrollment commit.
pub fn apply_enrollment_frontier<F>(
    owner: LiveFrontierOwner,
    operation: super::super::EnrollmentCommit<F>,
    attached_charge: RetainedRecordCharge,
) -> LiveFrontierResult<super::super::EnrollmentCommit<F>> {
    let attached = operation.attached;
    if attached.conversation_id() != owner.frontiers.conversation_id() {
        return failure(owner, operation, LiveFrontierError::Authority);
    }
    let participant_id = attached.participant_id();
    let mut active = owner.frontiers.active_identities().participants().to_vec();
    if active
        .iter()
        .any(|participant| participant.participant_index() == participant_id)
    {
        return failure(owner, operation, LiveFrontierError::Authority);
    }
    active.push(FrontierParticipant::new(
        participant_id,
        operation.member.cursor(),
        FrontierBinding::Bound(attached.binding_epoch()),
    ));
    active.sort_unstable_by_key(|participant| participant.participant_index());
    let rows = [retained_attached(attached)];
    let Some(sequence) =
        enrollment_sequence(owner.frontiers.sequence().ledger(), attached.delivery_seq())
    else {
        return failure(owner, operation, LiveFrontierError::Frontier);
    };
    let Some(order) = enrollment_order(
        owner.frontiers.order().ledger(),
        attached.admission_order().transaction_order(),
    ) else {
        return failure(owner, operation, LiveFrontierError::Frontier);
    };
    transition(
        owner,
        operation,
        active,
        &rows,
        vec![attached_charge],
        sequence,
        order,
    )
}

/// Applies credential attach to the complete live owner.
///
/// # Errors
///
/// Returns a failure retaining the unchanged owner, intact attach commit, and
/// exact reason the commit could not enter the frontier.
pub fn apply_attach_frontier<F, V>(
    owner: LiveFrontierOwner,
    operation: AttachCommit<F, V>,
    charges: AttachFrontierCharges,
) -> LiveFrontierResult<AttachCommit<F, V>> {
    let (terminal_charge, attached_charge) = charges.into_parts();
    let attached = operation.attached;
    if attached.conversation_id() != owner.frontiers.conversation_id() {
        return failure(owner, operation, LiveFrontierError::Authority);
    }
    let mut active = owner.frontiers.active_identities().participants().to_vec();
    let Some(participant) = active
        .iter_mut()
        .find(|participant| participant.participant_index() == attached.participant_id())
    else {
        return failure(owner, operation, LiveFrontierError::Authority);
    };
    *participant = FrontierParticipant::new(
        participant.participant_index(),
        operation.member.cursor(),
        FrontierBinding::Bound(attached.binding_epoch()),
    );
    let current_sequence = owner.frontiers.sequence().ledger();
    let current_order = owner.frontiers.order().ledger();
    let (rows, keyed_charges, sequence, order) = match operation.transition {
        AttachTransition::Detached => {
            if terminal_charge.is_some() {
                return failure(owner, operation, LiveFrontierError::RetainedCharge);
            }
            let Some(sequence) =
                detached_attach_sequence(current_sequence, attached.delivery_seq())
            else {
                return failure(owner, operation, LiveFrontierError::Frontier);
            };
            let Some(order) = detached_attach_order(
                current_order,
                attached.admission_order().transaction_order(),
            ) else {
                return failure(owner, operation, LiveFrontierError::Frontier);
            };
            (
                vec![retained_attached(attached)],
                vec![attached_charge],
                sequence,
                order,
            )
        }
        AttachTransition::Superseded { terminal } => {
            let Some(terminal_charge) = terminal_charge else {
                return failure(owner, operation, LiveFrontierError::RetainedCharge);
            };
            let rows = vec![
                retained_terminal(terminal.into()),
                retained_attached(attached),
            ];
            let Some(sequence) = superseding_attach_sequence(current_sequence, &rows) else {
                return failure(owner, operation, LiveFrontierError::Frontier);
            };
            let Some(order) = superseding_attach_order(
                current_order,
                attached.admission_order().transaction_order(),
            ) else {
                return failure(owner, operation, LiveFrontierError::Frontier);
            };
            (
                rows,
                vec![terminal_charge, attached_charge],
                sequence,
                order,
            )
        }
        AttachTransition::FencedRecovery {
            prior_binding_epoch,
            composed_terminal,
            next_closure_state,
        } => {
            return apply_fenced_attach_frontier(
                owner,
                operation,
                terminal_charge,
                attached_charge,
                prior_binding_epoch,
                composed_terminal,
                next_closure_state,
            );
        }
    };
    transition(
        owner,
        operation,
        active,
        &rows,
        keyed_charges,
        sequence,
        order,
    )
}

/// Applies a committed detach terminal to the complete live owner.
///
/// # Errors
///
/// Returns a failure retaining the unchanged owner and intact detach commit.
pub fn apply_detach_frontier<EF, V>(
    owner: LiveFrontierOwner,
    operation: CommittedDetachTransition<EF, V>,
    terminal_charge: RetainedRecordCharge,
) -> LiveFrontierResult<CommittedDetachTransition<EF, V>> {
    let terminal = operation.terminal();
    if terminal.conversation_id() != owner.frontiers.conversation_id() {
        return failure(owner, operation, LiveFrontierError::Authority);
    }
    let mut active = owner.frontiers.active_identities().participants().to_vec();
    let Some(participant) = active
        .iter_mut()
        .find(|participant| participant.participant_index() == terminal.participant_id())
    else {
        return failure(owner, operation, LiveFrontierError::Authority);
    };
    *participant = FrontierParticipant::new(
        participant.participant_index(),
        operation.member().cursor(),
        FrontierBinding::Detached(terminal.binding_epoch()),
    );
    let row = retained_terminal(terminal.into());
    let Some(sequence) = detach_sequence(owner.frontiers.sequence().ledger(), row.delivery_seq)
    else {
        return failure(owner, operation, LiveFrontierError::Frontier);
    };
    let Some(order) = detach_order(
        owner.frontiers.order().ledger(),
        row.admission_order.transaction_order(),
    ) else {
        return failure(owner, operation, LiveFrontierError::Frontier);
    };
    transition(
        owner,
        operation,
        active,
        &[row],
        vec![terminal_charge],
        sequence,
        order,
    )
}

/// Applies a zero-debt participant acknowledgement cursor transition.
///
/// # Errors
///
/// Returns a failure retaining the unchanged owner and intact ack commit.
pub fn apply_participant_ack_frontier(
    mut owner: LiveFrontierOwner,
    operation: ParticipantAckCommit,
) -> LiveFrontierResult<ParticipantAckCommit> {
    let request = operation.outcome().request();
    let Some(current) = owner
        .frontiers
        .active_identities()
        .participants()
        .iter()
        .find(|participant| participant.participant_index() == request.participant_id)
        .copied()
    else {
        return failure(owner, operation, LiveFrontierError::Authority);
    };
    let participant = FrontierParticipant::new(
        request.participant_id,
        request.through_seq,
        current.binding(),
    );
    owner.frontiers = match owner.frontiers.apply_live_identity(participant) {
        Ok(frontiers) => frontiers,
        Err(frontier_failure) => {
            let (frontiers, error) = *frontier_failure;
            owner.frontiers = frontiers;
            return failure(owner, operation, map_frontier_error(error));
        }
    };
    Ok(LiveFrontierCommit { operation, owner })
}

/// Applies a nonzero-debt participant acknowledgement cursor transition.
///
/// The episode and member remain owned by the sealed aggregate commit; this
/// transition consumes the same exact acknowledged cursor into the coupled
/// claim-frontier participant rank.
///
/// # Errors
///
/// Returns a failure retaining the unchanged owner and intact aggregate commit.
pub fn apply_nonzero_participant_ack_frontier(
    mut owner: LiveFrontierOwner,
    operation: NonzeroParticipantAckCommit,
) -> LiveFrontierResult<NonzeroParticipantAckCommit> {
    let request = operation.outcome().request();
    let Some(current) = owner
        .frontiers
        .active_identities()
        .participants()
        .iter()
        .find(|participant| participant.participant_index() == request.participant_id)
        .copied()
    else {
        return failure(owner, operation, LiveFrontierError::Authority);
    };
    let participant = FrontierParticipant::new(
        request.participant_id,
        request.through_seq,
        current.binding(),
    );
    owner.frontiers = match owner.frontiers.apply_live_identity(participant) {
        Ok(frontiers) => frontiers,
        Err(frontier_failure) => {
            let (frontiers, error) = *frontier_failure;
            owner.frontiers = frontiers;
            return failure(owner, operation, map_frontier_error(error));
        }
    };
    Ok(LiveFrontierCommit { operation, owner })
}

/// Applies a zero-debt marker acknowledgement cursor transition.
///
/// # Errors
///
/// Returns a failure retaining the unchanged owner and intact marker-ack commit.
pub fn apply_marker_ack_frontier(
    mut owner: LiveFrontierOwner,
    operation: MarkerAckCommit,
) -> LiveFrontierResult<MarkerAckCommit> {
    let request = operation.outcome().request();
    if !owner
        .frontiers
        .retained_marker_records()
        .iter()
        .any(|record| {
            record.delivery_seq == request.marker_delivery_seq
                && matches!(
                    record.kind,
                    RetainedCausalRecordKind::CompactionMarker { participant_index, .. }
                        if participant_index == request.participant_id
                )
        })
    {
        return failure(owner, operation, LiveFrontierError::Authority);
    }
    let Some(current) = owner
        .frontiers
        .active_identities()
        .participants()
        .iter()
        .find(|participant| participant.participant_index() == request.participant_id)
        .copied()
    else {
        return failure(owner, operation, LiveFrontierError::Authority);
    };
    let Some(accounting) = accounting_after_marker_ack(owner.closure_accounting) else {
        return failure(owner, operation, LiveFrontierError::ClosureAccounting);
    };
    let participant = FrontierParticipant::new(
        request.participant_id,
        request.marker_delivery_seq,
        current.binding(),
    );
    owner.frontiers = match owner.frontiers.apply_live_identity(participant) {
        Ok(frontiers) => frontiers,
        Err(frontier_failure) => {
            let (frontiers, error) = *frontier_failure;
            owner.frontiers = frontiers;
            return failure(owner, operation, map_frontier_error(error));
        }
    };
    owner.closure_accounting = accounting;
    Ok(LiveFrontierCommit { operation, owner })
}

fn apply_fenced_attach_frontier<F, V>(
    owner: LiveFrontierOwner,
    operation: AttachCommit<F, V>,
    terminal_charge: Option<RetainedRecordCharge>,
    attached_charge: RetainedRecordCharge,
    prior_binding_epoch: crate::wire::BindingEpoch,
    composed_terminal: Option<super::super::CommittedBindingTerminal>,
    next_closure_state: super::super::ClosureState,
) -> LiveFrontierResult<AttachCommit<F, V>> {
    let attached = operation.attached;
    let (rows, charges) = match (composed_terminal, terminal_charge) {
        (None, None) => (vec![retained_attached(attached)], vec![attached_charge]),
        (Some(terminal), Some(terminal_charge)) => (
            vec![retained_terminal(terminal), retained_attached(attached)],
            vec![terminal_charge, attached_charge],
        ),
        (None, Some(_)) | (Some(_), None) => {
            return failure(owner, operation, LiveFrontierError::RetainedCharge);
        }
    };
    let participant = FrontierParticipant::new(
        attached.participant_id(),
        operation.member.cursor(),
        FrontierBinding::Bound(attached.binding_epoch()),
    );
    fenced_attach_transition(
        owner,
        operation,
        participant,
        prior_binding_epoch,
        next_closure_state,
        &rows,
        charges,
    )
}

fn fenced_attach_transition<T>(
    mut owner: LiveFrontierOwner,
    operation: T,
    participant: FrontierParticipant,
    prior_binding_epoch: crate::wire::BindingEpoch,
    next_closure_state: super::super::ClosureState,
    rows: &[RetainedCausalRecord],
    charges: Vec<RetainedRecordCharge>,
) -> LiveFrontierResult<T> {
    if rows.len() != charges.len()
        || rows.iter().zip(&charges).any(|(row, charge)| {
            row.delivery_seq != charge.delivery_seq()
                || row.admission_order != charge.admission_order()
                || charge.encoded_charge().entries != 1
        })
    {
        return failure(owner, operation, LiveFrontierError::RetainedCharge);
    }
    let resulting_len = owner
        .frontiers
        .retained_records()
        .len()
        .checked_add(rows.len());
    if resulting_len
        .and_then(|len| u64::try_from(len).ok())
        .is_none_or(|len| len > owner.retained_record_limit)
    {
        return failure(owner, operation, LiveFrontierError::RetainedRecordLimit);
    }
    let Some(accounting) =
        accounting_after_fenced_attach(owner.closure_accounting, &charges, next_closure_state)
    else {
        return failure(owner, operation, LiveFrontierError::ClosureAccounting);
    };
    owner.frontiers =
        match owner
            .frontiers
            .apply_live_fenced_attach(participant, prior_binding_epoch, rows)
        {
            Ok(frontiers) => frontiers,
            Err(frontier_failure) => {
                let (frontiers, error) = *frontier_failure;
                owner.frontiers = frontiers;
                return failure(owner, operation, map_frontier_error(error));
            }
        };
    owner.retained_charges.extend(charges);
    owner
        .retained_charges
        .sort_unstable_by_key(|charge| charge.delivery_seq());
    owner.closure_accounting = accounting;
    Ok(LiveFrontierCommit { operation, owner })
}

fn transition<T>(
    mut owner: LiveFrontierOwner,
    operation: T,
    active: Vec<FrontierParticipant>,
    rows: &[RetainedCausalRecord],
    charges: Vec<RetainedRecordCharge>,
    sequence: SequenceLedger,
    order: OrderLedger,
) -> LiveFrontierResult<T> {
    if rows.len() != charges.len()
        || rows.iter().zip(&charges).any(|(row, charge)| {
            row.delivery_seq != charge.delivery_seq()
                || row.admission_order != charge.admission_order()
                || charge.encoded_charge().entries != 1
        })
    {
        return failure(owner, operation, LiveFrontierError::RetainedCharge);
    }
    let resulting_len = owner
        .frontiers
        .retained_records()
        .len()
        .checked_add(rows.len());
    if resulting_len
        .and_then(|len| u64::try_from(len).ok())
        .is_none_or(|len| len > owner.retained_record_limit)
    {
        return failure(owner, operation, LiveFrontierError::RetainedRecordLimit);
    }
    let Some(accounting) = accounting_after_rows(owner.closure_accounting, &charges) else {
        return failure(owner, operation, LiveFrontierError::ClosureAccounting);
    };
    owner.frontiers = match owner
        .frontiers
        .apply_live_transition(active, rows, sequence, order)
    {
        Ok(frontiers) => frontiers,
        Err(frontier_failure) => {
            let (frontiers, error) = *frontier_failure;
            owner.frontiers = frontiers;
            return failure(owner, operation, map_frontier_error(error));
        }
    };
    owner.retained_charges.extend(charges);
    owner.closure_accounting = accounting;
    Ok(LiveFrontierCommit { operation, owner })
}

const fn map_frontier_error(error: LiveFrontierTransitionError) -> LiveFrontierError {
    match error {
        LiveFrontierTransitionError::Authority => LiveFrontierError::Authority,
        LiveFrontierTransitionError::Precedence => LiveFrontierError::Precedence,
        LiveFrontierTransitionError::RecordPosition
        | LiveFrontierTransitionError::Exhausted
        | LiveFrontierTransitionError::ResultingFrontier => LiveFrontierError::Frontier,
    }
}

fn failure<T, U>(
    owner: LiveFrontierOwner,
    operation: T,
    error: LiveFrontierError,
) -> Result<U, Box<LiveFrontierFailure<T>>> {
    Err(Box::new(LiveFrontierFailure {
        error,
        operation,
        owner,
    }))
}
