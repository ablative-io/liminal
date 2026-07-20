//! Frozen participant operation schema-v2 grammar.
//!
//! These types are decode-only in production. Their serialized field order and
//! option shapes are historical bytes and must never be changed or re-encoded.

use liminal_protocol::wire::{DeliverySeq, TransactionOrder};
use serde::{Deserialize, Serialize};

use super::facts::Digest;
use super::log::{
    StoredAck, StoredAttachRequest, StoredBindingEpoch, StoredDetachRequest,
    StoredEnrollmentAllocation, StoredEnrollmentRequest, StoredLeave, StoredMarkerDrain,
    StoredRecordAdmission, StoredU128,
};

/// Complete replayable inputs in the frozen v2 operation grammar.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", tag = "operation")]
pub(super) enum StoredOperationV2 {
    Genesis {
        event: Vec<u8>,
    },
    Enrolled {
        request: StoredEnrollmentRequest,
        allocation: StoredEnrollmentAllocation,
        event: Vec<u8>,
    },
    Attached {
        request: StoredAttachRequest,
        secret_verified: bool,
        allocation: StoredAttachAllocationV2,
        event: Vec<u8>,
    },
    Detached {
        request: StoredDetachRequest,
        verifier: Digest,
        receiving_epoch: StoredBindingEpoch,
        terminal_order: TransactionOrder,
        terminal_seq: DeliverySeq,
        event: Vec<u8>,
    },
    ZeroDebtAck {
        request: StoredAck,
        receiving_epoch: StoredBindingEpoch,
        contiguously_available_through: DeliverySeq,
    },
    MarkerDrained {
        row: StoredMarkerDrain,
    },
    RecordAdmission {
        row: StoredRecordAdmission,
    },
    Left {
        row: StoredLeave,
    },
}

/// Server allocations in the frozen v2 Attached option grammar.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub(super) struct StoredAttachAllocationV2 {
    pub(super) binding_epoch: StoredBindingEpoch,
    pub(super) attach_secret: [u8; 32],
    pub(super) attached_order: TransactionOrder,
    pub(super) attached_seq: DeliverySeq,
    pub(super) receipt_expires_at: StoredU128,
    pub(super) provenance_expires_at: StoredU128,
    /// Admitted wall-clock read of the committing operation.
    pub(super) admitted_now_ms: u64,
    /// Optional superseded terminal sequence used as v2 mode evidence.
    pub(super) superseded_terminal_seq: Option<DeliverySeq>,
}
