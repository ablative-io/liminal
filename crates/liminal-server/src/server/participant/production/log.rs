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
    LeaveAttemptToken, LeaveRequest, ParticipantAck, ParticipantId, RecordAdmission,
    RecordAdmissionAttemptToken, TransactionOrder,
};
use serde::{Deserialize, Serialize};

use super::facts::Digest;
pub(super) use super::log_error::FencedAttachProofRefusal;

/// Stream-key prefix for production participant conversation logs.
pub(super) const STREAM_PREFIX: &str = "liminal:participant-production:";
/// Durable page size used during replay reads.
pub(super) const READ_BATCH_SIZE: usize = 64;
/// Stored-entry schema version.
pub(super) const SCHEMA_VERSION: u8 = 3;
/// Frozen historical schema version accepted only in the contiguous prefix.
pub(super) const SCHEMA_VERSION_V2: u8 = 2;

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
    /// A durable stream regressed from its v3 suffix back to v2.
    #[error(
        "participant production log schema regressed at sequence {sequence}: previous {previous}, actual {actual}"
    )]
    SchemaVersionTransition {
        sequence: u64,
        previous: u8,
        actual: u8,
    },
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
    /// Frozen v2 marker-bearing Attached bytes cannot supply a fenced proof.
    #[error("v2 Attached row at sequence {sequence} has no durable fenced proof")]
    V2AttachedFencedProofUnavailable { sequence: u64 },
    /// Frozen v2 Attached option fields disagree with replay prestate.
    #[error("v2 Attached row at sequence {sequence} has contradictory mode evidence")]
    V2AttachedModeMismatch { sequence: u64 },
    /// A schema-v3 Attached row carries non-canonical or contradictory fenced proof facts.
    #[error("v3 fenced Attached proof at sequence {sequence} was refused: {reason}")]
    FencedAttachProof {
        sequence: u64,
        reason: FencedAttachProofRefusal,
    },
    /// A decoded v3 fate row reached apply before its later-leg replay owner exists.
    #[error("v3 fate row at sequence {sequence} requires the W1b fate replay leg")]
    V3FateReplayUnavailable { sequence: u64 },
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

    /// Reads one replay page while carrying the one-way schema phase.
    pub(super) async fn read_page(
        &self,
        from_sequence: u64,
        mut phase: OperationSchemaPhase,
    ) -> Result<DecodedOperationPage, OperationLogError> {
        let entries = self
            .store
            .read_from(&self.stream_key, from_sequence, READ_BATCH_SIZE)
            .await?;
        let mut rows = Vec::with_capacity(entries.len());
        for entry in entries {
            let version: StoredEntryVersion = serde_json::from_slice(&entry.payload)?;
            let operation = match version.schema_version {
                SCHEMA_VERSION_V2 if phase == OperationSchemaPhase::V2Prefix => {
                    let stored: StoredEntryV2 = serde_json::from_slice(&entry.payload)?;
                    DecodedStoredOperation::V2(stored.operation)
                }
                SCHEMA_VERSION_V2 => {
                    return Err(OperationLogError::SchemaVersionTransition {
                        sequence: entry.sequence,
                        previous: SCHEMA_VERSION,
                        actual: SCHEMA_VERSION_V2,
                    });
                }
                SCHEMA_VERSION => {
                    phase = OperationSchemaPhase::V3Suffix;
                    let stored: StoredEntryV3 = serde_json::from_slice(&entry.payload)?;
                    stored.operation.validate_durable(entry.sequence)?;
                    DecodedStoredOperation::V3(stored.operation)
                }
                actual => return Err(OperationLogError::SchemaVersion(actual)),
            };
            rows.push(DecodedOperation {
                sequence: entry.sequence,
                schema_version: version.schema_version,
                operation,
            });
        }
        Ok(DecodedOperationPage {
            rows,
            next_phase: phase,
        })
    }

    /// Reads one replay page starting at `from_sequence`.
    #[cfg(test)]
    pub(super) async fn read_v2_page(
        &self,
        from_sequence: u64,
    ) -> Result<Vec<(u64, StoredOperationV2)>, OperationLogError> {
        let entries = self
            .store
            .read_from(&self.stream_key, from_sequence, READ_BATCH_SIZE)
            .await?;
        let mut decoded = Vec::with_capacity(entries.len());
        for entry in entries {
            let version: StoredEntryVersion = serde_json::from_slice(&entry.payload)?;
            if version.schema_version != SCHEMA_VERSION_V2 {
                return Err(OperationLogError::SchemaVersion(version.schema_version));
            }
            let stored: StoredEntryV2 = serde_json::from_slice(&entry.payload)?;
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
        operation.validate_durable(expected_sequence)?;
        let payload = serde_json::to_vec(&StoredEntryV3 {
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

/// One-way schema phase carried across every bounded replay page.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum OperationSchemaPhase {
    #[default]
    V2Prefix,
    V3Suffix,
}

/// One decoded versioned operation row.
#[derive(Clone, Debug)]
pub(super) struct DecodedOperation {
    pub(super) sequence: u64,
    pub(super) schema_version: u8,
    pub(super) operation: DecodedStoredOperation,
}

/// One bounded decoded page plus the phase supplied to the next page.
#[derive(Clone, Debug)]
pub(super) struct DecodedOperationPage {
    pub(super) rows: Vec<DecodedOperation>,
    pub(super) next_phase: OperationSchemaPhase,
}

/// Version-specific operation payload retained until contextual v2 migration.
#[derive(Clone, Debug)]
pub(super) enum DecodedStoredOperation {
    V2(StoredOperationV2),
    V3(StoredOperationV3),
}

#[derive(Clone, Copy, Debug, Deserialize)]
struct StoredEntryVersion {
    schema_version: u8,
}

/// Frozen v2 entry envelope. Its field names and representation never change.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredEntryV2 {
    schema_version: u8,
    operation: StoredOperationV2,
}

/// Canonical v3 entry envelope used for every new append.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredEntryV3 {
    schema_version: u8,
    operation: StoredOperationV3,
}

pub(super) use super::log_v2::*;
pub(super) use super::log_v3::*;

/// Canonical operation type accepted by all production append entry points.
pub(super) type StoredOperation = StoredOperationV3;

/// Scalar resource vector in the canonical v2 row schema.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(super) struct StoredResourceVector {
    pub(super) entries: u64,
    pub(super) bytes: u64,
}

/// One keyed retained-row charge in the canonical v2 row schema.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(super) struct StoredRetainedCharge {
    pub(super) delivery_seq: DeliverySeq,
    pub(super) transaction_order: TransactionOrder,
    pub(super) candidate_phase: u8,
    pub(super) participant_id: ParticipantId,
    pub(super) charge: StoredResourceVector,
}

/// Durable marker drain written before the same-lock `RecordAdmission` retry.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(super) struct StoredMarkerDrain {
    pub(super) marker: Vec<u8>,
    pub(super) retained_charge: StoredRetainedCharge,
    pub(super) resulting_retained_charges: Vec<StoredRetainedCharge>,
    pub(super) successor: Vec<u8>,
}

/// Durable payload-bearing `RecordAdmission` request.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(super) struct StoredRecordAdmissionRequest {
    pub(super) conversation_id: u64,
    pub(super) participant_id: ParticipantId,
    pub(super) capability_generation: u64,
    pub(super) token: [u8; 16],
    pub(super) payload: Vec<u8>,
}

impl From<&RecordAdmission> for StoredRecordAdmissionRequest {
    fn from(request: &RecordAdmission) -> Self {
        Self {
            conversation_id: request.conversation_id,
            participant_id: request.participant_id,
            capability_generation: request.capability_generation.get(),
            token: request.record_admission_attempt_token.into_bytes(),
            payload: request.payload.clone(),
        }
    }
}

impl StoredRecordAdmissionRequest {
    pub(super) fn into_request(self) -> Result<RecordAdmission, OperationLogError> {
        Ok(RecordAdmission {
            conversation_id: self.conversation_id,
            participant_id: self.participant_id,
            capability_generation: stored_generation(self.capability_generation)?,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new(self.token),
            payload: self.payload,
        })
    }
}

/// Atomic ordinary-record poststate persisted in one append/flush transaction.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(super) struct StoredRecordAdmission {
    pub(super) request: StoredRecordAdmissionRequest,
    pub(super) receiving_epoch: StoredBindingEpoch,
    pub(super) transaction_order: TransactionOrder,
    pub(super) delivery_seq: DeliverySeq,
    pub(super) encoded_record_charge: StoredResourceVector,
    pub(super) resulting_connection_count: u64,
    pub(super) newly_tracked: bool,
    pub(super) resulting_retained_charges: Vec<StoredRetainedCharge>,
    pub(super) resulting_closure_accounting: Vec<u8>,
}

/// Durable payload-bearing Leave request.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(super) struct StoredLeaveRequest {
    pub(super) conversation_id: u64,
    pub(super) participant_id: ParticipantId,
    pub(super) capability_generation: u64,
    pub(super) attach_secret: [u8; 32],
    pub(super) token: [u8; 16],
}

impl From<&LeaveRequest> for StoredLeaveRequest {
    fn from(request: &LeaveRequest) -> Self {
        Self {
            conversation_id: request.conversation_id,
            participant_id: request.participant_id,
            capability_generation: request.capability_generation.get(),
            attach_secret: request.attach_secret.into_bytes(),
            token: request.leave_attempt_token.into_bytes(),
        }
    }
}

impl StoredLeaveRequest {
    pub(super) fn into_request(self) -> Result<LeaveRequest, OperationLogError> {
        Ok(LeaveRequest {
            conversation_id: self.conversation_id,
            participant_id: self.participant_id,
            capability_generation: stored_generation(self.capability_generation)?,
            attach_secret: AttachSecret::new(self.attach_secret),
            leave_attempt_token: LeaveAttemptToken::new(self.token),
        })
    }
}

/// Exact Leave tombstone inputs and causal allocation.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(super) struct StoredLeave {
    pub(super) request: StoredLeaveRequest,
    pub(super) request_verifier: Digest,
    pub(super) receiving_epoch: StoredBindingEpoch,
    pub(super) left_transaction_order: TransactionOrder,
    pub(super) left_delivery_seq: DeliverySeq,
    pub(super) ended_binding_epoch: Option<StoredBindingEpoch>,
    pub(super) prior_terminal_delivery_seq: Option<DeliverySeq>,
}

/// Stored enrollment request fields.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
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
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
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
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
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

/// Stored detach request fields.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
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
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
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
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
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
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
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
