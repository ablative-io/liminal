//! Bounded durable marker-source association for fenced Attached rows.

use std::sync::Arc;

use liminal::durability::DurableStore;
use liminal_protocol::lifecycle::{
    FencedMarkerSourceExpectation, LiveFrontierOwner, MarkerSequenceOwner,
    RetainedFencedMarkerSource,
};

use super::log::{
    DecodedStoredOperation, OperationLog, StoredMarkerDrain, StoredOperationV2, StoredOperationV3,
};

/// Typed reason an exact marker source cannot authorize later mint inputs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum MarkerSourceRefusalReason {
    /// The point read or durable row decode failed.
    #[error("durable marker source could not be read or decoded")]
    DurableRead,
    /// No durable row exists at the selected sequence.
    #[error("marker source sequence does not exist")]
    Missing,
    /// The selected durable row is not a marker source.
    #[error("marker source sequence does not select MarkerDrained")]
    WrongOperation,
    /// Canonical marker identity bytes disagree with restored frontier truth.
    #[error("durable marker body differs from the frontier-validated record")]
    MarkerBody,
    /// Marker delivery sequence disagrees.
    #[error("durable marker delivery sequence differs from the frontier-validated record")]
    DeliverySequence,
    /// Marker transaction order disagrees.
    #[error("durable marker transaction order differs from the frontier-validated record")]
    TransactionOrder,
    /// Marker candidate phase disagrees.
    #[error("durable marker candidate phase differs from the frontier-validated record")]
    CandidatePhase,
    /// Marker participant disagrees.
    #[error("durable marker participant differs from the frontier-validated record")]
    Participant,
    /// Measured marker row charge disagrees.
    #[error("durable marker retained charge differs from its measured canonical bytes")]
    RetainedCharge,
}

/// Refused association retaining the move-only owner/recovery pair.
#[derive(Debug)]
pub struct MarkerSourceRefused {
    retained: RetainedFencedMarkerSource,
    marker_source_sequence: u64,
    reason: MarkerSourceRefusalReason,
}

impl MarkerSourceRefused {
    /// Returns the exact typed refusal.
    #[must_use]
    pub const fn reason(&self) -> MarkerSourceRefusalReason {
        self.reason
    }

    /// Returns unchanged retained inputs and the selected sequence.
    #[must_use]
    pub fn into_parts(self) -> (RetainedFencedMarkerSource, u64) {
        (self.retained, self.marker_source_sequence)
    }
}

/// Source-associated inputs which still contain no one-use marker token.
#[derive(Debug)]
pub struct ValidatedFencedMarkerInputs {
    retained: RetainedFencedMarkerSource,
    marker_source_sequence: u64,
}

impl ValidatedFencedMarkerInputs {
    /// Returns unchanged owner/recovery plus the validated source sequence.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        LiveFrontierOwner,
        liminal_protocol::lifecycle::DetachedCredentialRecovery,
        u64,
    ) {
        let (owner, recovery) = self.retained.into_parts();
        (owner, recovery, self.marker_source_sequence)
    }
}

/// Point-reads and validates one durable marker source before the retained owner
/// may proceed to its private one-use authority mint.
///
/// # Errors
/// Returns a typed refusal containing the unchanged retained inputs when the row
/// is absent, unreadable, has the wrong kind, or disagrees with frontier truth.
pub async fn validate_marker_source(
    store: Arc<dyn DurableStore>,
    retained: RetainedFencedMarkerSource,
    marker_source_sequence: u64,
) -> Result<ValidatedFencedMarkerInputs, Box<MarkerSourceRefused>> {
    let expectation = retained.expectation();
    let log = OperationLog::new(store, expectation.conversation_id());
    let result = read_and_validate(&log, marker_source_sequence, expectation).await;
    match result {
        Ok(()) => Ok(ValidatedFencedMarkerInputs {
            retained,
            marker_source_sequence,
        }),
        Err(reason) => Err(Box::new(MarkerSourceRefused {
            retained,
            marker_source_sequence,
            reason,
        })),
    }
}

async fn read_and_validate(
    log: &OperationLog,
    marker_source_sequence: u64,
    expectation: FencedMarkerSourceExpectation,
) -> Result<(), MarkerSourceRefusalReason> {
    let operation = log
        .read_at(marker_source_sequence)
        .await
        .map_err(|_| MarkerSourceRefusalReason::DurableRead)?
        .ok_or(MarkerSourceRefusalReason::Missing)?
        .operation;
    let row = match operation {
        DecodedStoredOperation::V2(StoredOperationV2::MarkerDrained { row })
        | DecodedStoredOperation::V3(StoredOperationV3::MarkerDrained { row }) => row,
        DecodedStoredOperation::V2(_) | DecodedStoredOperation::V3(_) => {
            return Err(MarkerSourceRefusalReason::WrongOperation);
        }
    };
    validate_row(&row, expectation)
}

fn validate_row(
    row: &StoredMarkerDrain,
    expectation: FencedMarkerSourceExpectation,
) -> Result<(), MarkerSourceRefusalReason> {
    let canonical_marker = canonical_marker_bytes(expectation);
    if row.marker != canonical_marker {
        return Err(MarkerSourceRefusalReason::MarkerBody);
    }
    let charge = row.retained_charge;
    let order = expectation.admission_order();
    if charge.delivery_seq != expectation.marker_delivery_seq() {
        return Err(MarkerSourceRefusalReason::DeliverySequence);
    }
    if charge.transaction_order != order.transaction_order() {
        return Err(MarkerSourceRefusalReason::TransactionOrder);
    }
    if charge.candidate_phase != order.candidate_phase() as u8 {
        return Err(MarkerSourceRefusalReason::CandidatePhase);
    }
    if charge.participant_id != expectation.participant_id() {
        return Err(MarkerSourceRefusalReason::Participant);
    }
    let marker_bytes = u64::try_from(canonical_marker.len())
        .map_err(|_| MarkerSourceRefusalReason::RetainedCharge)?;
    if charge.charge.entries != 1 || charge.charge.bytes != marker_bytes {
        return Err(MarkerSourceRefusalReason::RetainedCharge);
    }
    Ok(())
}

fn canonical_marker_bytes(expectation: FencedMarkerSourceExpectation) -> Vec<u8> {
    format!(
        "MarkerCandidateAuthority {{ delivery_seq: {:?}, admission_order: {:?}, target_binding: {:?}, provenance: {:?}, current_owner: {:?} }}",
        expectation.marker_delivery_seq(),
        expectation.admission_order(),
        expectation.target_binding(),
        expectation.provenance(),
        MarkerSequenceOwner::Marker,
    )
    .into_bytes()
}
