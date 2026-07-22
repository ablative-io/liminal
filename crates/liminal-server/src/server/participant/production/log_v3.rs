//! Canonical participant operation schema-v3 durable row grammar.

use liminal_protocol::wire::{DeliverySeq, ParticipantId, TransactionOrder};
use serde::{Deserialize, Serialize};

use super::facts::Digest;
use super::fenced_attach_codec::FencedAttachProofContext;
use super::log::{
    FencedAttachProofRefusal, OperationLogError, StoredAck, StoredAttachAllocationV2,
    StoredAttachRequest, StoredBindingEpoch, StoredDetachRequest, StoredEnrollmentAllocation,
    StoredEnrollmentRequest, StoredLeave, StoredLeaveRequest, StoredMarkerDrain,
    StoredRecordAdmission, StoredU128,
};

/// Canonical schema-v3 operation grammar.
///
/// The four fate variants are deliberately distinct durable tags. Existing
/// participant operations join this enum when the v3 entry envelope is wired;
/// keeping the fate grammar typed here prevents aliases or shape inference.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "operation")]
pub(super) enum StoredOperationV3 {
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
        allocation: StoredAttachAllocation,
        mode: Box<StoredAttachModeV3>,
        event: Vec<u8>,
    },
    /// One exact binding-death source.
    Died {
        row: StoredDied,
    },
    /// One exact clean or orderly-detach source.
    Detached {
        row: StoredDetached,
    },
    /// One ordinary binding-fate completion.
    Ordinary {
        row: StoredOrdinaryFate,
        /// Canonical `BindingFateOperation::from_ordinary` bytes.
        event: Vec<u8>,
    },
    /// One recovered binding-fate completion.
    Recovered {
        row: StoredRecoveredFate,
        /// Canonical `BindingFateOperation::from_recovered` bytes.
        event: Vec<u8>,
    },
    ZeroDebtAck {
        request: StoredAck,
        receiving_epoch: StoredBindingEpoch,
        contiguously_available_through: DeliverySeq,
    },
    NonzeroDebtAck {
        request: StoredAck,
        receiving_epoch: StoredBindingEpoch,
        contiguously_available_through: DeliverySeq,
        event: Vec<u8>,
    },
    MarkerDrained {
        row: StoredMarkerDrain,
    },
    RecordAdmission {
        row: StoredRecordAdmission,
    },
    Left {
        row: StoredLeaveV3,
    },
}

impl StoredOperationV3 {
    /// Validates closed v3 mode evidence immediately after decode and before
    /// append. Fenced proof bytes never enter replay as an unvalidated shape.
    pub(super) fn validate_durable(&self, sequence: u64) -> Result<(), OperationLogError> {
        let Self::Attached {
            request,
            allocation,
            mode,
            ..
        } = self
        else {
            return Ok(());
        };
        let refusal = match mode.as_ref() {
            StoredAttachModeV3::Ordinary => request
                .accept_marker_delivery_seq
                .is_some()
                .then_some(FencedAttachProofRefusal::OrdinaryRequestMarker),
            StoredAttachModeV3::Superseding {
                terminal_transaction_order,
                ..
            } => {
                if request.accept_marker_delivery_seq.is_some() {
                    Some(FencedAttachProofRefusal::SupersedingRequestMarker)
                } else if *terminal_transaction_order != allocation.attached_order {
                    Some(FencedAttachProofRefusal::SupersedingTerminalOrder)
                } else {
                    None
                }
            }
            StoredAttachModeV3::Fenced {
                prior_binding_epoch,
                marker_delivery_seq,
                proof,
                composed_terminal,
                ..
            } => {
                let context = FencedAttachProofContext {
                    conversation_id: request.conversation_id,
                    participant_id: request.participant_id,
                    request_marker_delivery_seq: request.accept_marker_delivery_seq,
                    prior_binding_epoch: *prior_binding_epoch,
                    marker_delivery_seq: *marker_delivery_seq,
                    new_binding_epoch: allocation.binding_epoch,
                };
                proof.decode(context).err().or_else(|| {
                    composed_terminal.as_ref().and_then(|terminal| {
                        terminal
                            .validate_local(sequence, allocation.attached_order)
                            .err()
                    })
                })
            }
        };
        refusal.map_or(Ok(()), |reason| {
            Err(OperationLogError::FencedAttachProof { sequence, reason })
        })
    }
}

/// Common allocation shared by all schema-v3 Attached modes.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(super) struct StoredAttachAllocation {
    pub(super) binding_epoch: StoredBindingEpoch,
    pub(super) attach_secret: [u8; 32],
    pub(super) attached_order: TransactionOrder,
    pub(super) attached_seq: DeliverySeq,
    pub(super) receipt_expires_at: StoredU128,
    pub(super) provenance_expires_at: StoredU128,
    pub(super) admitted_now_ms: u64,
}

/// Exact binding prestate used to migrate frozen v2 Attached option evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum V2AttachedPrestate {
    Detached,
    Bound { binding_epoch: StoredBindingEpoch },
    Other,
}

/// Losslessly maps one frozen v2 Attached payload into the mandatory v3 mode.
pub(super) fn migrate_v2_attached(
    request: StoredAttachRequest,
    secret_verified: bool,
    allocation: StoredAttachAllocationV2,
    event: Vec<u8>,
    prestate: V2AttachedPrestate,
    sequence: u64,
) -> Result<StoredOperationV3, OperationLogError> {
    if request.accept_marker_delivery_seq.is_some() {
        return Err(OperationLogError::V2AttachedFencedProofUnavailable { sequence });
    }
    let mode = match (prestate, allocation.superseded_terminal_seq) {
        (V2AttachedPrestate::Detached, None) => StoredAttachModeV3::Ordinary,
        (
            V2AttachedPrestate::Bound {
                binding_epoch: prior_binding_epoch,
            },
            Some(terminal_delivery_seq),
        ) => StoredAttachModeV3::Superseding {
            prior_binding_epoch,
            terminal_transaction_order: allocation.attached_order,
            terminal_delivery_seq,
        },
        _ => return Err(OperationLogError::V2AttachedModeMismatch { sequence }),
    };
    Ok(StoredOperationV3::Attached {
        request,
        secret_verified,
        allocation: StoredAttachAllocation {
            binding_epoch: allocation.binding_epoch,
            attach_secret: allocation.attach_secret,
            attached_order: allocation.attached_order,
            attached_seq: allocation.attached_seq,
            receipt_expires_at: allocation.receipt_expires_at,
            provenance_expires_at: allocation.provenance_expires_at,
            admitted_now_ms: allocation.admitted_now_ms,
        },
        mode: Box::new(mode),
        event,
    })
}

/// Mandatory closed mode of one schema-v3 Attached row.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub(super) enum StoredAttachModeV3 {
    Ordinary,
    Superseding {
        prior_binding_epoch: StoredBindingEpoch,
        terminal_transaction_order: TransactionOrder,
        terminal_delivery_seq: DeliverySeq,
    },
    Fenced {
        prior_binding_epoch: StoredBindingEpoch,
        marker_delivery_seq: DeliverySeq,
        marker_source_sequence: u64,
        proof: StoredFencedAttachProof,
        composed_terminal: Option<StoredComposedTerminal>,
    },
}

/// Complete fixed-size fenced proof payload persisted by Attached v3.
///
/// The predecessor and successor use canonical protocol storage bytes. Marker
/// identity remains the row source plus delivery sequence above.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(super) struct StoredFencedAttachProof {
    pub(super) detached_credential_recovery: Vec<u8>,
    pub(super) predecessor_debt: Vec<u8>,
    pub(super) fenced_resulting_floor: DeliverySeq,
    pub(super) successor: Vec<u8>,
}

/// Closed class of a terminal composed into a fenced Attached transition.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum StoredComposedTerminalKind {
    Died,
    Detached,
}

/// Exact v3 Leave tombstone inputs plus closed pending-finalizer ownership.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(super) struct StoredLeaveV3 {
    pub(super) request: StoredLeaveRequest,
    pub(super) request_verifier: Digest,
    pub(super) receiving_epoch: StoredBindingEpoch,
    pub(super) left_transaction_order: TransactionOrder,
    pub(super) left_delivery_seq: DeliverySeq,
    pub(super) ended_binding_epoch: Option<StoredBindingEpoch>,
    pub(super) prior_terminal_delivery_seq: Option<DeliverySeq>,
    pub(super) pending_source_sequence: Option<u64>,
    pub(super) finalizer_presentation: StoredFinalizerPresentation,
}

impl From<StoredLeave> for StoredLeaveV3 {
    fn from(row: StoredLeave) -> Self {
        Self {
            request: row.request,
            request_verifier: row.request_verifier,
            receiving_epoch: row.receiving_epoch,
            left_transaction_order: row.left_transaction_order,
            left_delivery_seq: row.left_delivery_seq,
            ended_binding_epoch: row.ended_binding_epoch,
            prior_terminal_delivery_seq: row.prior_terminal_delivery_seq,
            pending_source_sequence: None,
            finalizer_presentation: StoredFinalizerPresentation::PresentEnclosing,
        }
    }
}

/// Explicit occurrence-presentation ownership of a composed finalizer.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "presentation")]
pub(super) enum StoredFinalizerPresentation {
    PresentEnclosing,
    ConsumeRecoveredReservation { recovered_source_sequence: u64 },
}

/// Complete terminal audit composed into one fenced Attached row.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(super) struct StoredComposedTerminal {
    pub(super) kind: StoredComposedTerminalKind,
    pub(super) cause: StoredComposedTerminalCause,
    pub(super) transaction_order: TransactionOrder,
    pub(super) delivery_seq: DeliverySeq,
    pub(super) pending_source_sequence: u64,
    pub(super) presentation: StoredFinalizerPresentation,
}

impl StoredComposedTerminal {
    fn validate_local(
        &self,
        attached_source_sequence: u64,
        attached_order: TransactionOrder,
    ) -> Result<(), FencedAttachProofRefusal> {
        let cause_matches = matches!(
            (self.kind, self.cause),
            (
                StoredComposedTerminalKind::Detached,
                StoredComposedTerminalCause::CleanDeregister
                    | StoredComposedTerminalCause::ServerShutdown
            ) | (
                StoredComposedTerminalKind::Died,
                StoredComposedTerminalCause::ConnectionLost
                    | StoredComposedTerminalCause::ProcessKilled
                    | StoredComposedTerminalCause::ProtocolError
                    | StoredComposedTerminalCause::UncleanServerRestart { .. }
            )
        );
        if !cause_matches {
            return Err(FencedAttachProofRefusal::ComposedTerminalKindCause);
        }
        if self.transaction_order.checked_add(1) != Some(attached_order) {
            return Err(FencedAttachProofRefusal::ComposedTerminalOrder);
        }
        if self.pending_source_sequence >= attached_source_sequence {
            return Err(FencedAttachProofRefusal::ComposedPendingSourceOrder);
        }
        if let StoredFinalizerPresentation::ConsumeRecoveredReservation {
            recovered_source_sequence,
        } = self.presentation
        {
            if self.kind != StoredComposedTerminalKind::Died {
                return Err(FencedAttachProofRefusal::ComposedRecoveredReservationKind);
            }
            if recovered_source_sequence >= attached_source_sequence {
                return Err(FencedAttachProofRefusal::ComposedRecoveredSourceOrder);
            }
        }
        Ok(())
    }
}

/// Cause audit whose class must agree with `StoredComposedTerminal::kind`.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "cause")]
pub(super) enum StoredComposedTerminalCause {
    CleanDeregister,
    ServerShutdown,
    ConnectionLost,
    ProcessKilled,
    ProtocolError,
    UncleanServerRestart { prior_server_incarnation: u64 },
}

/// Closed Died cause set in the participant v3 schema.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "cause")]
pub(super) enum StoredDiedCause {
    ConnectionLost,
    ProcessKilled,
    ProtocolError,
    UncleanServerRestart { prior_server_incarnation: u64 },
}

/// Closed Detached cause set in the participant v3 schema.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum StoredDetachedCause {
    CleanDeregister,
    ServerShutdown,
}

/// Exact committed-or-pending terminal disposition persisted by fate sources.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "disposition")]
pub(super) enum StoredTerminalDisposition {
    Committed { terminal_seq: DeliverySeq },
    Pending,
}

/// Positive durable authority for a Died row's one specific-fate completion.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "fate")]
pub(super) enum StoredSpecificFateIntent {
    Ordinary {
        attached_source_sequence: u64,
    },
    Recovered {
        attached_source_sequence: u64,
        prior_binding_epoch: StoredBindingEpoch,
        marker_delivery_seq: DeliverySeq,
    },
}

/// Complete exact v3 Died source row.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(super) struct StoredDied {
    pub(super) participant_id: ParticipantId,
    pub(super) binding_epoch: StoredBindingEpoch,
    pub(super) cause: StoredDiedCause,
    pub(super) terminal_order: TransactionOrder,
    pub(super) disposition: StoredTerminalDisposition,
    pub(super) connection_intent_sequence: Option<u64>,
    pub(super) specific_fate_intent: Option<StoredSpecificFateIntent>,
}

/// Closed source authority for one exact v3 Detached row.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "source")]
pub(super) enum StoredDetachedSource {
    ExplicitRequestCommitted {
        request: StoredDetachRequest,
        secret_verified: bool,
        verifier: Digest,
        receiving_epoch: StoredBindingEpoch,
        event: Vec<u8>,
    },
    ExplicitRequestPending {
        request: StoredDetachRequest,
        secret_verified: bool,
        verifier: Digest,
        receiving_epoch: StoredBindingEpoch,
        observer_baseline: DeliverySeq,
    },
    ConnectionClose {
        connection_intent_sequence: u64,
    },
}

/// Complete exact v3 Detached source row.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(super) struct StoredDetached {
    pub(super) participant_id: ParticipantId,
    pub(super) binding_epoch: StoredBindingEpoch,
    pub(super) cause: StoredDetachedCause,
    pub(super) terminal_order: TransactionOrder,
    pub(super) disposition: StoredTerminalDisposition,
    pub(super) source: StoredDetachedSource,
}

/// Exact lower terminal source consumed by one ordinary fate.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "source")]
pub(super) enum StoredOrdinaryTerminalSource {
    DiedCommitted {
        died_source_sequence: u64,
    },
    PendingDiedFinalized {
        died_source_sequence: u64,
        finalizer: StoredPendingDiedFinalizer,
    },
}

/// Closed lower finalizer source for immutable Pending Died history.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "finalizer")]
pub(super) enum StoredPendingDiedFinalizer {
    Left { source_sequence: u64 },
    FencedAttached { source_sequence: u64 },
}

/// Redundant exact audit of the committed Died terminal consumed by Ordinary.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(super) struct StoredCommittedTerminalAudit {
    pub(super) cause: StoredDiedCause,
    pub(super) transaction_order: TransactionOrder,
    pub(super) terminal_seq: DeliverySeq,
}

/// Complete exact v3 Ordinary binding-fate row.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(super) struct StoredOrdinaryFate {
    pub(super) participant_id: ParticipantId,
    pub(super) last_dead_binding_epoch: StoredBindingEpoch,
    pub(super) ordinary_attached_source_sequence: u64,
    pub(super) terminal_source: StoredOrdinaryTerminalSource,
    pub(super) committed_terminal_audit: StoredCommittedTerminalAudit,
    pub(super) resulting_floor: DeliverySeq,
}

/// Durable occurrence-presentation ownership of one Recovered fate.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum StoredRecoveredPresentation {
    DiedCommittedOwns,
    RecoveredOwnsAndReservesFinalizer,
}

/// Complete exact v3 Recovered binding-fate row.
///
/// Marker identity is intentionally only the lower fenced-Attached source plus
/// its delivery sequence. There is no marker digest field or digest function.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(super) struct StoredRecoveredFate {
    pub(super) participant_id: ParticipantId,
    pub(super) last_dead_binding_epoch: StoredBindingEpoch,
    pub(super) died_source_sequence: u64,
    pub(super) fenced_attached_source_sequence: u64,
    pub(super) prior_binding_epoch: StoredBindingEpoch,
    pub(super) marker_delivery_seq: DeliverySeq,
    pub(super) resulting_floor: DeliverySeq,
    pub(super) presentation: StoredRecoveredPresentation,
}
