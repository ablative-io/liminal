//! Durable transition-input log for one participant conversation.
//!
//! Every entry stores the *inputs* of one committed operation together with
//! the canonical shell event bytes minted for it (when the operation is one
//! of the six shell-event lifecycle operations). Replay re-runs the same
//! protocol transitions from the stored inputs and re-mints the same shell
//! event under the aggregate durability barrier, then cross-checks the
//! re-minted canonical bytes against the stored bytes — so the server never
//! grows a second implementation of lifecycle rules and byte drift between
//! live and replayed decisions fails loudly.

use std::sync::Arc;

use liminal::durability::{DurabilityError, DurableStore};
use liminal_protocol::wire::{
    AttachAttemptToken, AttachSecret, BindingEpoch, ConnectionIncarnation, CredentialAttachRequest,
    DeliverySeq, DetachAttemptToken, DetachRequest, EnrollmentRequest, EnrollmentToken, Generation,
    ParticipantAck, ParticipantId, TransactionOrder,
};
use serde::{Deserialize, Serialize};

use super::facts::Digest;

/// Stream-key prefix for production participant conversation logs.
const STREAM_PREFIX: &str = "liminal:participant-production:";
/// Durable page size used during replay reads.
pub(super) const READ_BATCH_SIZE: usize = 64;
/// Stored-entry schema version.
const SCHEMA_VERSION: u8 = 1;

/// Failure to encode, decode, append, or read one durable log entry.
#[derive(Debug, thiserror::Error)]
pub enum OperationLogError {
    /// The underlying durable store rejected an operation.
    #[error(transparent)]
    Durability(#[from] DurabilityError),
    /// The bounded synchronous durability bridge rejected a suspending backend.
    #[error(transparent)]
    Bridge(#[from] liminal::durability::bridge::BridgeError),
    /// A durable entry could not be serialized or deserialized.
    #[error("participant production log serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    /// A durable entry uses an unsupported schema version.
    #[error("unsupported participant production log schema version {0}")]
    SchemaVersion(u8),
    /// The durable stream was not contiguous at the expected sequence.
    #[error("participant production log expected sequence {expected}, found {actual}")]
    Sequence {
        /// Next sequence required by replay.
        expected: u64,
        /// Sequence read from durable storage.
        actual: u64,
    },
    /// The store assigned a different sequence than the optimistic head.
    #[error("participant production log append expected {expected}, got {actual}")]
    AssignedSequence {
        /// Optimistic-concurrency sequence supplied to the store.
        expected: u64,
        /// Assigned sequence returned by the store.
        actual: u64,
    },
    /// A stored numeric field violated a protocol domain (zero generation).
    #[error("participant production log entry carries zero generation")]
    ZeroGeneration,
    /// A durable row the protocol aggregate itself refuses to replay.
    #[error("durable participant row at sequence {sequence} was refused during restore")]
    CorruptRow {
        /// Durable sequence of the refused row.
        sequence: u64,
    },
}

/// Append-only handle over one conversation's production operation stream.
#[derive(Debug)]
pub(super) struct OperationLog {
    store: Arc<dyn DurableStore>,
    stream_key: String,
}

impl OperationLog {
    /// Creates a stateless log handle over one durable conversation stream.
    pub(super) fn new(store: Arc<dyn DurableStore>, conversation_id: u64) -> Self {
        Self {
            store,
            stream_key: format!("{STREAM_PREFIX}{conversation_id}"),
        }
    }

    /// Reads one replay page starting at `from_sequence`.
    pub(super) async fn read_page(
        &self,
        from_sequence: u64,
    ) -> Result<Vec<(u64, StoredOperation)>, OperationLogError> {
        let entries = self
            .store
            .read_from(&self.stream_key, from_sequence, READ_BATCH_SIZE)
            .await?;
        let mut decoded = Vec::with_capacity(entries.len());
        for entry in entries {
            let stored: StoredEntry = serde_json::from_slice(&entry.payload)?;
            if stored.schema_version != SCHEMA_VERSION {
                return Err(OperationLogError::SchemaVersion(stored.schema_version));
            }
            decoded.push((entry.sequence, stored.operation));
        }
        Ok(decoded)
    }

    /// Appends one operation at the exact optimistic head, then flushes.
    ///
    /// The flush is the durability barrier the caller's pending shell commit
    /// waits behind: nothing is published until these bytes are durable.
    pub(super) async fn append(
        &self,
        operation: &StoredOperation,
        expected_sequence: u64,
    ) -> Result<(), OperationLogError> {
        let payload = serde_json::to_vec(&StoredEntry {
            schema_version: SCHEMA_VERSION,
            operation: operation.clone(),
        })?;
        let assigned = self
            .store
            .append(&self.stream_key, payload, expected_sequence)
            .await?;
        if assigned != expected_sequence {
            return Err(OperationLogError::AssignedSequence {
                expected: expected_sequence,
                actual: assigned,
            });
        }
        self.store.flush().await?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredEntry {
    schema_version: u8,
    operation: StoredOperation,
}

/// Complete replayable inputs of one committed operation.
///
/// `event` fields carry the exact canonical shell-event bytes appended for
/// the six shell-event lifecycle operations; operations that mint no shell
/// event (zero-debt cursor updates) carry none.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", tag = "operation")]
pub(super) enum StoredOperation {
    /// Shell genesis validation (event ordinal zero).
    Genesis {
        /// Canonical genesis event bytes.
        event: Vec<u8>,
    },
    /// Committed initial enrollment for one fresh participant slot.
    Enrolled {
        /// Wire request inputs.
        request: StoredEnrollmentRequest,
        /// Server allocations consumed by the transition.
        allocation: StoredEnrollmentAllocation,
        /// Canonical `Enrolled` shell event bytes.
        event: Vec<u8>,
    },
    /// Committed ordinary detached attach (Fix 1 terminalization inside).
    Attached {
        /// Wire request inputs.
        request: StoredAttachRequest,
        /// Whether the presented secret verified at receive time.
        secret_verified: bool,
        /// Server allocations consumed by the transition.
        allocation: StoredAttachAllocation,
        /// Canonical `Attached` shell event bytes.
        event: Vec<u8>,
    },
    /// Committed immediate detach (one non-decomposable transaction).
    Detached {
        /// Wire request inputs.
        request: StoredDetachRequest,
        /// Canonical non-secret request verifier.
        verifier: Digest,
        /// Binding epoch of the receiving connection.
        receiving_epoch: StoredBindingEpoch,
        /// Assigned terminal transaction order.
        terminal_order: TransactionOrder,
        /// Assigned terminal delivery sequence.
        terminal_seq: DeliverySeq,
        /// Canonical `Detached` shell event bytes.
        event: Vec<u8>,
    },
    /// Committed zero-debt cumulative acknowledgement (no shell event).
    ZeroDebtAck {
        /// Wire request inputs.
        request: StoredAck,
        /// Binding epoch of the receiving connection.
        receiving_epoch: StoredBindingEpoch,
        /// Contiguously available sequence fact at commit time.
        contiguously_available_through: DeliverySeq,
    },
}

/// Stored enrollment request fields.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub(super) struct StoredEnrollmentRequest {
    pub(super) conversation_id: u64,
    pub(super) token: [u8; 16],
}

impl From<&EnrollmentRequest> for StoredEnrollmentRequest {
    fn from(request: &EnrollmentRequest) -> Self {
        Self {
            conversation_id: request.conversation_id,
            token: request.enrollment_token.into_bytes(),
        }
    }
}

impl StoredEnrollmentRequest {
    pub(super) const fn to_request(self) -> EnrollmentRequest {
        EnrollmentRequest {
            conversation_id: self.conversation_id,
            enrollment_token: EnrollmentToken::new(self.token),
        }
    }
}

/// Server allocations committed with one enrollment transition.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub(super) struct StoredEnrollmentAllocation {
    pub(super) participant_id: ParticipantId,
    pub(super) identity_limit: u64,
    pub(super) attach_secret: [u8; 32],
    pub(super) origin_epoch: StoredBindingEpoch,
    pub(super) attached_order: TransactionOrder,
    pub(super) attached_seq: DeliverySeq,
    pub(super) receipt_expires_at: StoredU128,
    pub(super) provenance_expires_at: StoredU128,
    pub(super) enrollment_fingerprint: Digest,
}

/// Stored credential-attach request fields.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub(super) struct StoredAttachRequest {
    pub(super) conversation_id: u64,
    pub(super) participant_id: ParticipantId,
    pub(super) capability_generation: u64,
    pub(super) attach_secret: [u8; 32],
    pub(super) token: [u8; 16],
    pub(super) accept_marker_delivery_seq: Option<DeliverySeq>,
}

impl From<&CredentialAttachRequest> for StoredAttachRequest {
    fn from(request: &CredentialAttachRequest) -> Self {
        Self {
            conversation_id: request.conversation_id,
            participant_id: request.participant_id,
            capability_generation: request.capability_generation.get(),
            attach_secret: request.attach_secret.into_bytes(),
            token: request.attach_attempt_token.into_bytes(),
            accept_marker_delivery_seq: request.accept_marker_delivery_seq,
        }
    }
}

impl StoredAttachRequest {
    pub(super) fn to_request(self) -> Result<CredentialAttachRequest, OperationLogError> {
        Ok(CredentialAttachRequest {
            conversation_id: self.conversation_id,
            participant_id: self.participant_id,
            capability_generation: stored_generation(self.capability_generation)?,
            attach_secret: AttachSecret::new(self.attach_secret),
            attach_attempt_token: AttachAttemptToken::new(self.token),
            accept_marker_delivery_seq: self.accept_marker_delivery_seq,
        })
    }
}

/// Server allocations committed with one credential attach.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub(super) struct StoredAttachAllocation {
    pub(super) binding_epoch: StoredBindingEpoch,
    pub(super) attach_secret: [u8; 32],
    pub(super) attached_order: TransactionOrder,
    pub(super) attached_seq: DeliverySeq,
    pub(super) receipt_expires_at: StoredU128,
    pub(super) provenance_expires_at: StoredU128,
    /// Admitted wall-clock read of the committing operation. Replay derives
    /// the replaced receipt's exact terminal reason (`Superseded` vs
    /// `Deadline`) from this stored fact, never from replay-time clocks.
    pub(super) admitted_now_ms: u64,
}

/// Stored detach request fields.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub(super) struct StoredDetachRequest {
    pub(super) conversation_id: u64,
    pub(super) participant_id: ParticipantId,
    pub(super) capability_generation: u64,
    pub(super) token: [u8; 16],
}

impl From<&DetachRequest> for StoredDetachRequest {
    fn from(request: &DetachRequest) -> Self {
        Self {
            conversation_id: request.conversation_id,
            participant_id: request.participant_id,
            capability_generation: request.capability_generation.get(),
            token: request.detach_attempt_token.into_bytes(),
        }
    }
}

impl StoredDetachRequest {
    pub(super) fn to_request(self) -> Result<DetachRequest, OperationLogError> {
        Ok(DetachRequest {
            conversation_id: self.conversation_id,
            participant_id: self.participant_id,
            capability_generation: stored_generation(self.capability_generation)?,
            detach_attempt_token: DetachAttemptToken::new(self.token),
        })
    }
}

/// Stored cumulative-ack request fields.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub(super) struct StoredAck {
    pub(super) conversation_id: u64,
    pub(super) participant_id: ParticipantId,
    pub(super) capability_generation: u64,
    pub(super) through_seq: DeliverySeq,
}

impl From<&ParticipantAck> for StoredAck {
    fn from(request: &ParticipantAck) -> Self {
        Self {
            conversation_id: request.conversation_id,
            participant_id: request.participant_id,
            capability_generation: request.capability_generation.get(),
            through_seq: request.through_seq,
        }
    }
}

impl StoredAck {
    pub(super) fn to_request(self) -> Result<ParticipantAck, OperationLogError> {
        Ok(ParticipantAck {
            conversation_id: self.conversation_id,
            participant_id: self.participant_id,
            capability_generation: stored_generation(self.capability_generation)?,
            through_seq: self.through_seq,
        })
    }
}

/// Stored binding-epoch fields.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub(super) struct StoredBindingEpoch {
    pub(super) server_incarnation: u64,
    pub(super) connection_ordinal: u64,
    pub(super) capability_generation: u64,
}

impl From<BindingEpoch> for StoredBindingEpoch {
    fn from(epoch: BindingEpoch) -> Self {
        Self {
            server_incarnation: epoch.connection_incarnation.server_incarnation,
            connection_ordinal: epoch.connection_incarnation.connection_ordinal,
            capability_generation: epoch.capability_generation.get(),
        }
    }
}

impl StoredBindingEpoch {
    pub(super) fn to_epoch(self) -> Result<BindingEpoch, OperationLogError> {
        Ok(BindingEpoch::new(
            ConnectionIncarnation::new(self.server_incarnation, self.connection_ordinal),
            stored_generation(self.capability_generation)?,
        ))
    }
}

/// Big-endian byte capsule for `u128` deadlines.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub(super) struct StoredU128(pub(super) [u8; 16]);

impl StoredU128 {
    pub(super) const fn get(self) -> u128 {
        u128::from_be_bytes(self.0)
    }
}

impl From<u128> for StoredU128 {
    fn from(value: u128) -> Self {
        Self(value.to_be_bytes())
    }
}

fn stored_generation(value: u64) -> Result<Generation, OperationLogError> {
    Generation::new(value).ok_or(OperationLogError::ZeroGeneration)
}
