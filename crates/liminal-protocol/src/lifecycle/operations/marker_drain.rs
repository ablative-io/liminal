//! Consuming mandatory marker-candidate drain.
//!
//! The operation owns the only fresh `MarkerDelivery` producer. It consumes the
//! coupled claim frontiers, delegates the exact H/M/order-key transition to the
//! sealed frontier core, and returns one opaque atomic commit containing the
//! resulting frontiers, exact current closure state, planned marker successor,
//! and retained-record state.

use alloc::vec::Vec;

use super::super::{
    ClaimFrontiers, ClosureAccounting, ClosureState, Event, MarkerDelivery, ObserverProjection,
    StoredEdge,
    claim_frontier::{MarkerDrainCoreError, ValidatedMarkerRecord},
};
use super::RetainedRecordCharge;

/// Exact invariant fault selected by mandatory marker drain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarkerDrainError {
    /// No immutable candidate is currently owed.
    NoCandidate,
    /// A binding terminal has global precedence over marker work.
    BindingTerminalFirst,
    /// The first marker does not own exactly `H + 1`.
    SequenceNotNext,
    /// The current closure edge cannot coexist with this marker append.
    CurrentEdgeMismatch,
    /// Marker drain attempted to allocate rather than reuse its causal major.
    CausalMajorNotAllocated,
    /// Cross-counter validation promised an order key that is now absent.
    MissingOrderCandidate,
    /// Consuming `M` did not yield a valid post-append sequence ledger.
    ResultingLedger,
    /// Frontier core returned candidate and retained-record authorities that disagree.
    AuthorityMismatch,
    /// Canonical marker-row charge does not name the selected retained row.
    MarkerChargeKey,
    /// Every retained durable row has exactly one entry of charge.
    MarkerEntryCharge,
    /// The successor closure accounting failed validation.
    ResultingAccounting,
}

/// Complete atomic marker-drain commit.
///
/// No field can be supplied independently: the claim-frontier transition,
/// executable successor and retained marker record share one sealed predecessor
/// and must be persisted together. The retained-record authority is never
/// exposed independently of the selected successor: doing so would let an
/// undelivered DMR append masquerade as delivered DCR recovery provenance.
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::MarkerDrainCommit;
///
/// fn splice(commit: MarkerDrainCommit) {
///     let _ = commit.record();
/// }
/// ```
#[derive(Debug, PartialEq, Eq)]
pub struct MarkerDrainCommit {
    frontiers: ClaimFrontiers,
    closure_accounting: ClosureAccounting,
    retained_charges: Vec<RetainedRecordCharge>,
    marker_successor: StoredEdge,
    record: ValidatedMarkerRecord,
}

impl MarkerDrainCommit {
    /// Borrows the resulting coupled sequence/order frontiers.
    #[must_use]
    pub const fn frontiers(&self) -> &ClaimFrontiers {
        &self.frontiers
    }

    /// Returns the exact closure state after the append occurrence.
    #[must_use]
    pub const fn closure(&self) -> ClosureState {
        self.closure_accounting.state()
    }

    /// Returns the complete closure accounting after the append occurrence.
    #[must_use]
    pub const fn closure_accounting(&self) -> ClosureAccounting {
        self.closure_accounting
    }

    /// Borrows exact keyed charges for the complete retained suffix.
    #[must_use]
    pub fn retained_charges(&self) -> &[RetainedRecordCharge] {
        &self.retained_charges
    }

    /// Returns the exact marker edge selected once any strict OP/PC completes.
    ///
    /// A bound target selects [`StoredEdge::MarkerDelivery`]; a target whose
    /// epoch already died selects [`StoredEdge::DetachedMarkerRelease`].
    #[must_use]
    pub const fn marker_successor(&self) -> StoredEdge {
        self.marker_successor
    }

    /// Consumes the opaque transaction into its persistable protocol values.
    ///
    /// The retained marker is already present in the returned frontiers. Its
    /// executable validation token remains coupled to this commit and is
    /// deliberately consumed here rather than returned as a fourth value.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ClaimFrontiers,
        ClosureAccounting,
        Vec<RetainedRecordCharge>,
        StoredEdge,
    ) {
        self.record.consume();
        (
            self.frontiers,
            self.closure_accounting,
            self.retained_charges,
            self.marker_successor,
        )
    }

    /// Extracts the occurrence token for crate-internal adversarial tests.
    #[cfg(test)]
    pub(super) fn into_record_for_test(self) -> ValidatedMarkerRecord {
        self.record
    }
}

/// Consumes the globally first marker candidate into one atomic durable commit.
///
/// This operation never accepts a raw candidate token, sequence, participant,
/// epoch, marker count, or order key. All are derived from the coupled
/// frontiers, and the candidate authority is consumed while deriving the exact
/// bound-delivery or detached-release successor. An already-current OP/PC
/// remains strict through its typed marker-append transition.
///
/// # Errors
///
/// Returns [`MarkerDrainError`] when the mandatory prefix is absent, belongs to
/// a binding terminal, is not the exact next sequence, targets a detached
/// binding, conflicts with the current closure edge, lacks its
/// already-allocated causal order key, or cannot produce a valid resulting
/// sequence ledger.
pub fn drain_next_marker(
    frontiers: ClaimFrontiers,
    current_accounting: ClosureAccounting,
    mut retained_charges: Vec<RetainedRecordCharge>,
    marker_charge: RetainedRecordCharge,
) -> Result<MarkerDrainCommit, MarkerDrainError> {
    let core = frontiers.drain_next_marker_core().map_err(map_core_error)?;
    let (frontiers, candidate, record) = core.into_parts();
    let candidate_conversation_id = candidate.conversation_id();
    let candidate_participant = candidate.participant_id();
    let candidate_sequence = candidate.delivery_seq();
    let candidate_target = candidate.target_binding();
    let candidate_provenance = candidate.provenance();
    let marker_successor = MarkerDelivery::successor_from_validated_candidate(candidate);
    if record.conversation_id() != candidate_conversation_id
        || record.participant_id() != candidate_participant
        || record.delivery_seq() != candidate_sequence
        || record.target_binding() != candidate_target
        || record.provenance() != candidate_provenance
    {
        return Err(MarkerDrainError::AuthorityMismatch);
    }
    if marker_charge.delivery_seq() != record.delivery_seq()
        || marker_charge.admission_order() != record.admission_order()
    {
        return Err(MarkerDrainError::MarkerChargeKey);
    }
    if marker_charge.encoded_charge().entries != 1 {
        return Err(MarkerDrainError::MarkerEntryCharge);
    }
    let closure = apply_marker_append(current_accounting.state(), candidate_sequence)?;
    let closure_accounting = ClosureAccounting::try_new(
        closure,
        current_accounting.marker_capacity_credits(),
        current_accounting.marker_anchors(),
        current_accounting.edge_sequence_claims(),
        current_accounting.edge_order_position_claims(),
        current_accounting.edge_k_remaining(),
        current_accounting.baseline(),
        current_accounting.configured_cap(),
        current_accounting.episode_churn_used(),
        current_accounting.episode_churn_limit(),
    )
    .map_err(|_| MarkerDrainError::ResultingAccounting)?;
    retained_charges.push(marker_charge);
    if retained_charges.len() != frontiers.retained_records().len()
        || !retained_charges
            .iter()
            .zip(frontiers.retained_records())
            .all(|(charge, retained)| {
                charge.delivery_seq() == retained.delivery_seq
                    && charge.admission_order() == retained.admission_order
            })
    {
        return Err(MarkerDrainError::MarkerChargeKey);
    }
    Ok(MarkerDrainCommit {
        frontiers,
        closure_accounting,
        retained_charges,
        marker_successor,
        record,
    })
}

fn apply_marker_append(
    current: ClosureState,
    marker_delivery_seq: u64,
) -> Result<ClosureState, MarkerDrainError> {
    let event = Event::marker_appended(marker_delivery_seq, marker_delivery_seq);
    match current {
        ClosureState::Clear => Ok(ClosureState::Clear),
        ClosureState::Owed {
            debt,
            edge: StoredEdge::ObserverProjection(projection),
        } => {
            let successor = projection
                .later_projection_after_marker(
                    &event,
                    debt,
                    ObserverProjection::new(marker_delivery_seq),
                )
                .ok_or(MarkerDrainError::CurrentEdgeMismatch)?;
            projection
                .marker_appended(debt, event, successor)
                .map_err(|_| MarkerDrainError::CurrentEdgeMismatch)
        }
        ClosureState::Owed {
            debt,
            edge: StoredEdge::PhysicalCompaction(compaction),
        } => compaction
            .marker_appended(debt, event)
            .map_err(|_| MarkerDrainError::CurrentEdgeMismatch),
        ClosureState::Owed { .. } => Err(MarkerDrainError::CurrentEdgeMismatch),
    }
}

const fn map_core_error(error: MarkerDrainCoreError) -> MarkerDrainError {
    match error {
        MarkerDrainCoreError::NoCandidate => MarkerDrainError::NoCandidate,
        MarkerDrainCoreError::BindingTerminalFirst => MarkerDrainError::BindingTerminalFirst,
        MarkerDrainCoreError::SequenceNotNext => MarkerDrainError::SequenceNotNext,
        MarkerDrainCoreError::CausalMajorNotAllocated => MarkerDrainError::CausalMajorNotAllocated,
        MarkerDrainCoreError::MissingOrderCandidate => MarkerDrainError::MissingOrderCandidate,
        MarkerDrainCoreError::ResultingLedger => MarkerDrainError::ResultingLedger,
    }
}
