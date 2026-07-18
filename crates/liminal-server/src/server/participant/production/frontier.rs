//! Canonical retained-row charges and executable frontier configuration.
//!
//! Storage owns canonical byte framing; protocol transitions own every causal
//! fact. This module is the narrow seam between them: it encodes protocol-
//! produced row identities and supplies the resulting keyed charge to the
//! move-only frontier transition.

use liminal_protocol::algebra::{ResourceVector, WideResourceVector};
use liminal_protocol::lifecycle::{
    ClosureAccounting, ClosureState, InitialEnrollmentClosureInput, OrderClaims, OrderHigh,
    OrderLedger, SequenceClaims, SequenceLedger,
};
use liminal_protocol::wire::{BindingEpoch, DeliverySeq, ParticipantId, TransactionOrder};
use serde::Serialize;

use crate::config::types::ParticipantConfig;

use super::log::{StoredAttachAllocation, StoredBindingEpoch, StoredEnrollmentAllocation};
use super::state::StateError;

/// Canonical payload of one retained lifecycle row.
///
/// The tagged JSON representation is the v2 storage schema's charge authority.
/// It deliberately contains no configurable maximum and no server-selected
/// causal fact.
#[derive(Serialize)]
#[serde(rename_all = "snake_case", tag = "row")]
enum CanonicalLifecycleRow {
    Attached {
        conversation_id: u64,
        participant_id: ParticipantId,
        binding_epoch: StoredBindingEpoch,
        admission_order: TransactionOrder,
        delivery_seq: DeliverySeq,
    },
    BindingTerminal {
        conversation_id: u64,
        participant_id: ParticipantId,
        binding_epoch: StoredBindingEpoch,
        admission_order: TransactionOrder,
        delivery_seq: DeliverySeq,
    },
}

/// Returns the exact canonical charge of an `Attached` retained row.
pub(super) fn attached_charge(
    conversation_id: u64,
    allocation: &StoredEnrollmentAllocation,
) -> Result<ResourceVector, StateError> {
    lifecycle_row_charge(&CanonicalLifecycleRow::Attached {
        conversation_id,
        participant_id: allocation.participant_id,
        binding_epoch: allocation.origin_epoch,
        admission_order: allocation.attached_order,
        delivery_seq: allocation.attached_seq,
    })
}

/// Returns the exact canonical charge of a credential-attach `Attached` row.
pub(super) fn credential_attached_charge(
    conversation_id: u64,
    participant_id: ParticipantId,
    allocation: &StoredAttachAllocation,
) -> Result<ResourceVector, StateError> {
    lifecycle_row_charge(&CanonicalLifecycleRow::Attached {
        conversation_id,
        participant_id,
        binding_epoch: allocation.binding_epoch,
        admission_order: allocation.attached_order,
        delivery_seq: allocation.attached_seq,
    })
}

/// Returns the exact canonical charge of a binding-terminal retained row.
pub(super) fn terminal_charge(
    conversation_id: u64,
    participant_id: ParticipantId,
    binding_epoch: BindingEpoch,
    admission_order: TransactionOrder,
    delivery_seq: DeliverySeq,
) -> Result<ResourceVector, StateError> {
    lifecycle_row_charge(&CanonicalLifecycleRow::BindingTerminal {
        conversation_id,
        participant_id,
        binding_epoch: binding_epoch.into(),
        admission_order,
        delivery_seq,
    })
}

fn lifecycle_row_charge(row: &CanonicalLifecycleRow) -> Result<ResourceVector, StateError> {
    let bytes = serde_json::to_vec(row).map_err(super::log::OperationLogError::from)?;
    let bytes = u64::try_from(bytes.len())
        .map_err(|_| StateError::invariant("canonical lifecycle row length exceeds u64"))?;
    Ok(ResourceVector::new(1, bytes))
}

/// Builds the protocol's initial clear-conversation closure input exclusively
/// from signed deployment values and the canonical first `Attached` row.
pub(super) fn initial_closure_input(
    config: &ParticipantConfig,
    allocation: &StoredEnrollmentAllocation,
    attached_charge: ResourceVector,
) -> Result<InitialEnrollmentClosureInput, StateError> {
    let mandatory = ResourceVector::new(
        config.mandatory_transaction_bound_entries,
        config.mandatory_transaction_bound_bytes,
    );
    if attached_charge.entries > mandatory.entries || attached_charge.bytes > mandatory.bytes {
        return Err(StateError::invariant(format!(
            "canonical Attached row charge {attached_charge:?} exceeds signed mandatory bound {mandatory:?}"
        )));
    }
    let marker_max = ResourceVector::new(
        config.max_generated_marker_entries,
        config.max_generated_marker_bytes,
    );
    let baseline = WideResourceVector::new(
        u128::from(config.identity_slots) * u128::from(marker_max.entries),
        u128::from(config.identity_slots) * u128::from(marker_max.bytes),
    );
    let accounting = ClosureAccounting::try_new(
        ClosureState::Clear,
        0,
        0,
        0,
        0,
        ResourceVector::default(),
        baseline,
        ResourceVector::new(
            config.retained_capacity_entries,
            config.retained_capacity_bytes,
        ),
        0,
        config.closure_episode_churn_limit,
    )
    .map_err(|error| {
        StateError::invariant(format!(
            "signed initial closure accounting is invalid: {error:?}"
        ))
    })?;
    let order =
        OrderLedger::try_new(OrderHigh::Empty, OrderClaims::default()).map_err(|error| {
            StateError::invariant(format!("empty order ledger is invalid: {error:?}"))
        })?;
    let sequence = SequenceLedger::try_new(0, SequenceClaims::default()).map_err(|error| {
        StateError::invariant(format!("empty sequence ledger is invalid: {error:?}"))
    })?;
    Ok(InitialEnrollmentClosureInput::new(
        accounting,
        config.identity_slots,
        mandatory,
        ResourceVector::new(
            config.full_recovery_claim_entries,
            config.full_recovery_claim_bytes,
        ),
        marker_max,
        attached_charge,
        allocation.participant_id,
        allocation.origin_epoch.to_epoch()?,
        order,
        sequence,
        u128::from(allocation.attached_seq),
        0,
    ))
}
