//! Canonical durable codec for schema-v3 fenced Attached proofs.

use liminal_protocol::algebra::WideResourceVector;
use liminal_protocol::lifecycle::{
    ActiveBinding, BindingFateTerminalRestore, ClosureDebt, CommittedBindingTerminalRestore,
    DebtCompletion, DebtCompletionRestore, DetachedCredentialRecovery,
    DetachedCredentialRecoveryRestore, Event, MarkerCursorProgressRestore, MarkerDeliveryRestore,
    PendingFinalizationRestore,
};
use liminal_protocol::wire::{CloseCause, DeliverySeq, ParticipantId, TransactionOrder};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::log::{
    FencedAttachProofRefusal, OperationLogError, StoredBindingEpoch, StoredFencedAttachProof,
    StoredU128,
};
use super::log_v3::StoredComposedTerminalCause;

/// Enclosing Attached facts against which proof redundancies are checked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct FencedAttachProofContext {
    pub(super) conversation_id: u64,
    pub(super) participant_id: ParticipantId,
    pub(super) request_marker_delivery_seq: Option<DeliverySeq>,
    pub(super) prior_binding_epoch: StoredBindingEpoch,
    pub(super) marker_delivery_seq: DeliverySeq,
    pub(super) new_binding_epoch: StoredBindingEpoch,
}

/// Fully decoded, redundantly validated inputs retained for source association
/// and the later consuming proof mint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct DecodedFencedAttachProof {
    pub(super) detached_credential_recovery: StoredDetachedCredentialRecovery,
    pub(super) predecessor_debt: StoredWideResourceVector,
    pub(super) fenced_resulting_floor: DeliverySeq,
    pub(super) successor: StoredDebtCompletion,
}

/// Validated descriptive inputs consumed by the frontier owner's sole proof mint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct FencedAttachMintInputs {
    pub(super) recovery: DetachedCredentialRecovery,
    pub(super) predecessor_debt: ClosureDebt,
    pub(super) event: Event,
    pub(super) successor: DebtCompletion,
}

/// Canonical complete `DetachedCredentialRecovery` description.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredDetachedCredentialRecovery {
    pub(super) conversation_id: u64,
    pub(super) participant_id: ParticipantId,
    pub(super) marker_delivery_seq: DeliverySeq,
    pub(super) prior_binding_epoch: StoredBindingEpoch,
    pub(super) resulting_floor: DeliverySeq,
    pub(super) terminal: StoredRecoveryTerminal,
    pub(super) progress: StoredMarkerCursorProgress,
}

/// Exact committed or pending terminal in the DCR provenance chain.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "disposition")]
pub(super) enum StoredRecoveryTerminal {
    Committed {
        binding: StoredProofBinding,
        cause: StoredComposedTerminalCause,
        transaction_order: TransactionOrder,
        delivery_seq: DeliverySeq,
    },
    Pending {
        binding: StoredProofBinding,
        cause: StoredComposedTerminalCause,
        transaction_order: TransactionOrder,
    },
}

impl StoredRecoveryTerminal {
    const fn binding(&self) -> &StoredProofBinding {
        match self {
            Self::Committed { binding, .. } | Self::Pending { binding, .. } => binding,
        }
    }
}

/// Binding owner repeated by terminal provenance.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredProofBinding {
    pub(super) conversation_id: u64,
    pub(super) participant_id: ParticipantId,
    pub(super) binding_epoch: StoredBindingEpoch,
}

/// Complete marker-backed cursor predecessor repeated by DCR.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredMarkerCursorProgress {
    pub(super) conversation_id: u64,
    pub(super) participant_id: ParticipantId,
    pub(super) binding_epoch: StoredBindingEpoch,
    pub(super) through_seq: DeliverySeq,
    pub(super) marker_delivery_seq: DeliverySeq,
    pub(super) delivery: StoredMarkerDelivery,
}

/// Exact marker-delivery predecessor repeated by cursor progress.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredMarkerDelivery {
    pub(super) participant_id: ParticipantId,
    pub(super) binding_epoch: StoredBindingEpoch,
    pub(super) marker_delivery_seq: DeliverySeq,
}

/// Lossless canonical widened debt scalar.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredWideResourceVector {
    pub(super) entries: StoredU128,
    pub(super) bytes: StoredU128,
}

impl StoredWideResourceVector {
    const fn is_zero(self) -> bool {
        self.entries.get() == 0 && self.bytes.get() == 0
    }

    const fn into_protocol(self) -> WideResourceVector {
        WideResourceVector::new(self.entries.get(), self.bytes.get())
    }
}

/// Closed successor accepted by fenced attach restoration.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "successor")]
pub(super) enum StoredDebtCompletion {
    Clear,
    ObserverProjection {
        debt: StoredWideResourceVector,
        through_seq: DeliverySeq,
    },
    PhysicalCompaction {
        debt: StoredWideResourceVector,
        from_floor: DeliverySeq,
        through_seq: DeliverySeq,
    },
}

impl StoredDebtCompletion {
    const fn validate(self) -> Result<(), FencedAttachProofRefusal> {
        match self {
            Self::ObserverProjection { debt, .. } if debt.is_zero() => {
                Err(FencedAttachProofRefusal::SuccessorDebtZero)
            }
            Self::PhysicalCompaction { debt, .. } if debt.is_zero() => {
                Err(FencedAttachProofRefusal::SuccessorDebtZero)
            }
            Self::PhysicalCompaction {
                from_floor,
                through_seq,
                ..
            } if from_floor > through_seq => {
                Err(FencedAttachProofRefusal::SuccessorCompactionRange)
            }
            Self::Clear | Self::ObserverProjection { .. } | Self::PhysicalCompaction { .. } => {
                Ok(())
            }
        }
    }

    const fn into_restore(self) -> DebtCompletionRestore {
        match self {
            Self::Clear => DebtCompletionRestore::Clear,
            Self::ObserverProjection { debt, through_seq } => {
                DebtCompletionRestore::ObserverProjection {
                    debt: debt.into_protocol(),
                    through_seq,
                }
            }
            Self::PhysicalCompaction {
                debt,
                from_floor,
                through_seq,
            } => DebtCompletionRestore::PhysicalCompaction {
                debt: debt.into_protocol(),
                from_floor,
                through_seq,
            },
        }
    }
}

impl StoredFencedAttachProof {
    /// Canonically encodes the complete durable proof descriptions.
    pub(super) fn encode(
        detached_credential_recovery: &StoredDetachedCredentialRecovery,
        predecessor_debt: StoredWideResourceVector,
        fenced_resulting_floor: DeliverySeq,
        successor: StoredDebtCompletion,
    ) -> Result<Self, OperationLogError> {
        Ok(Self {
            detached_credential_recovery: canonical_bytes(&detached_credential_recovery)?,
            predecessor_debt: canonical_bytes(&predecessor_debt)?,
            fenced_resulting_floor,
            successor: canonical_bytes(&successor)?,
        })
    }

    /// Decodes canonical proof bytes and validates all redundant facts against
    /// the enclosing Attached request, mode, and common allocation.
    pub(super) fn decode(
        &self,
        context: FencedAttachProofContext,
    ) -> Result<DecodedFencedAttachProof, FencedAttachProofRefusal> {
        let recovery = decode_canonical(
            &self.detached_credential_recovery,
            FencedAttachProofRefusal::DetachedCredentialRecoveryMalformed,
            FencedAttachProofRefusal::DetachedCredentialRecoveryNonCanonical,
        )?;
        let predecessor_debt: StoredWideResourceVector = decode_canonical(
            &self.predecessor_debt,
            FencedAttachProofRefusal::PredecessorDebtMalformed,
            FencedAttachProofRefusal::PredecessorDebtNonCanonical,
        )?;
        let successor: StoredDebtCompletion = decode_canonical(
            &self.successor,
            FencedAttachProofRefusal::SuccessorMalformed,
            FencedAttachProofRefusal::SuccessorNonCanonical,
        )?;
        validate_recovery(context, &recovery)?;
        if predecessor_debt.is_zero() {
            return Err(FencedAttachProofRefusal::PredecessorDebtZero);
        }
        successor.validate()?;
        let canonical = Self::encode(
            &recovery,
            predecessor_debt,
            self.fenced_resulting_floor,
            successor,
        )
        .map_err(|_| FencedAttachProofRefusal::DetachedCredentialRecoveryMalformed)?;
        if canonical != *self {
            return Err(FencedAttachProofRefusal::DetachedCredentialRecoveryNonCanonical);
        }
        Ok(DecodedFencedAttachProof {
            detached_credential_recovery: recovery,
            predecessor_debt,
            fenced_resulting_floor: self.fenced_resulting_floor,
            successor,
        })
    }
}

impl DecodedFencedAttachProof {
    /// Rebuilds harmless copyable descriptions after complete codec validation.
    pub(super) fn into_mint_inputs(
        self,
        new_binding_epoch: StoredBindingEpoch,
    ) -> Result<FencedAttachMintInputs, FencedAttachProofRefusal> {
        let recovery = self
            .detached_credential_recovery
            .into_restore()?
            .restore_description()
            .map_err(|_| FencedAttachProofRefusal::DetachedCredentialRecoveryMalformed)?;
        let predecessor_debt = ClosureDebt::new(self.predecessor_debt.into_protocol())
            .ok_or(FencedAttachProofRefusal::PredecessorDebtZero)?;
        let successor = self
            .successor
            .into_restore()
            .restore()
            .map_err(|_| FencedAttachProofRefusal::SuccessorMalformed)?;
        let new_binding_epoch = new_binding_epoch
            .to_epoch()
            .map_err(|_| FencedAttachProofRefusal::NewBindingGenerationMismatch)?;
        Ok(FencedAttachMintInputs {
            recovery,
            predecessor_debt,
            event: Event::fenced_recovery_committed(
                recovery.participant_id(),
                recovery.marker_delivery_seq(),
                recovery.prior_binding_epoch(),
                new_binding_epoch,
                self.fenced_resulting_floor,
            ),
            successor,
        })
    }
}

impl StoredDetachedCredentialRecovery {
    fn into_restore(self) -> Result<DetachedCredentialRecoveryRestore, FencedAttachProofRefusal> {
        let restore_binding = |stored: StoredProofBinding| {
            Ok(ActiveBinding {
                conversation_id: stored.conversation_id,
                participant_id: stored.participant_id,
                binding_epoch: stored
                    .binding_epoch
                    .to_epoch()
                    .map_err(|_| FencedAttachProofRefusal::RecoveryPriorEpochMismatch)?,
            })
        };
        let terminal = match self.terminal {
            StoredRecoveryTerminal::Committed {
                binding,
                cause,
                transaction_order,
                delivery_seq,
            } => BindingFateTerminalRestore::Committed(CommittedBindingTerminalRestore {
                binding: restore_binding(binding)?,
                cause: cause.into_protocol(),
                transaction_order,
                delivery_seq,
            }),
            StoredRecoveryTerminal::Pending {
                binding,
                cause,
                transaction_order,
            } => BindingFateTerminalRestore::Pending(PendingFinalizationRestore {
                binding: restore_binding(binding)?,
                cause: cause.into_protocol(),
                transaction_order,
            }),
        };
        Ok(DetachedCredentialRecoveryRestore {
            participant_id: self.participant_id,
            marker_delivery_seq: self.marker_delivery_seq,
            prior_binding_epoch: self
                .prior_binding_epoch
                .to_epoch()
                .map_err(|_| FencedAttachProofRefusal::RecoveryPriorEpochMismatch)?,
            resulting_floor: self.resulting_floor,
            terminal,
            progress: MarkerCursorProgressRestore {
                conversation_id: self.progress.conversation_id,
                participant_id: self.progress.participant_id,
                binding_epoch: self
                    .progress
                    .binding_epoch
                    .to_epoch()
                    .map_err(|_| FencedAttachProofRefusal::ProgressEpochMismatch)?,
                through_seq: self.progress.through_seq,
                marker_delivery_seq: self.progress.marker_delivery_seq,
                delivery: MarkerDeliveryRestore {
                    participant_id: self.progress.delivery.participant_id,
                    binding_epoch: self
                        .progress
                        .delivery
                        .binding_epoch
                        .to_epoch()
                        .map_err(|_| FencedAttachProofRefusal::DeliveryEpochMismatch)?,
                    marker_delivery_seq: self.progress.delivery.marker_delivery_seq,
                },
            },
        })
    }
}

impl StoredComposedTerminalCause {
    const fn into_protocol(self) -> CloseCause {
        match self {
            Self::CleanDeregister => CloseCause::CleanDeregister,
            Self::ServerShutdown => CloseCause::ServerShutdown,
            Self::ConnectionLost => CloseCause::ConnectionLost,
            Self::ProcessKilled => CloseCause::ProcessKilled,
            Self::ProtocolError => CloseCause::ProtocolError,
            Self::UncleanServerRestart {
                prior_server_incarnation,
            } => CloseCause::UncleanServerRestart {
                prior_server_incarnation,
            },
        }
    }
}

fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, OperationLogError> {
    serde_json::to_vec(value).map_err(OperationLogError::from)
}

fn decode_canonical<T: DeserializeOwned + Serialize>(
    bytes: &[u8],
    malformed: FencedAttachProofRefusal,
    noncanonical: FencedAttachProofRefusal,
) -> Result<T, FencedAttachProofRefusal> {
    let value: T = serde_json::from_slice(bytes).map_err(|_| malformed)?;
    let encoded = serde_json::to_vec(&value).map_err(|_| malformed)?;
    if encoded != bytes {
        return Err(noncanonical);
    }
    Ok(value)
}

fn validate_recovery(
    context: FencedAttachProofContext,
    recovery: &StoredDetachedCredentialRecovery,
) -> Result<(), FencedAttachProofRefusal> {
    if context.request_marker_delivery_seq != Some(context.marker_delivery_seq) {
        return Err(FencedAttachProofRefusal::RequestMarkerMismatch);
    }
    if recovery.conversation_id != context.conversation_id {
        return Err(FencedAttachProofRefusal::RecoveryConversationMismatch);
    }
    if recovery.participant_id != context.participant_id {
        return Err(FencedAttachProofRefusal::RecoveryParticipantMismatch);
    }
    if recovery.marker_delivery_seq != context.marker_delivery_seq {
        return Err(FencedAttachProofRefusal::RecoveryMarkerMismatch);
    }
    if recovery.prior_binding_epoch != context.prior_binding_epoch {
        return Err(FencedAttachProofRefusal::RecoveryPriorEpochMismatch);
    }
    let Some(expected_generation) = context
        .prior_binding_epoch
        .capability_generation
        .checked_add(1)
    else {
        return Err(FencedAttachProofRefusal::NewBindingGenerationMismatch);
    };
    if context.new_binding_epoch.capability_generation != expected_generation {
        return Err(FencedAttachProofRefusal::NewBindingGenerationMismatch);
    }
    validate_progress(recovery)?;
    let terminal = recovery.terminal.binding();
    if terminal.conversation_id != recovery.conversation_id {
        return Err(FencedAttachProofRefusal::TerminalConversationMismatch);
    }
    if terminal.participant_id != recovery.participant_id {
        return Err(FencedAttachProofRefusal::TerminalParticipantMismatch);
    }
    if terminal.binding_epoch != recovery.prior_binding_epoch {
        return Err(FencedAttachProofRefusal::TerminalEpochMismatch);
    }
    Ok(())
}

fn validate_progress(
    recovery: &StoredDetachedCredentialRecovery,
) -> Result<(), FencedAttachProofRefusal> {
    let progress = &recovery.progress;
    if progress.conversation_id != recovery.conversation_id {
        return Err(FencedAttachProofRefusal::ProgressConversationMismatch);
    }
    if progress.participant_id != recovery.participant_id {
        return Err(FencedAttachProofRefusal::ProgressParticipantMismatch);
    }
    if progress.binding_epoch != recovery.prior_binding_epoch {
        return Err(FencedAttachProofRefusal::ProgressEpochMismatch);
    }
    if progress.marker_delivery_seq != recovery.marker_delivery_seq {
        return Err(FencedAttachProofRefusal::ProgressMarkerMismatch);
    }
    if progress.through_seq != progress.marker_delivery_seq {
        return Err(FencedAttachProofRefusal::ProgressThroughMismatch);
    }
    if progress.delivery.participant_id != progress.participant_id {
        return Err(FencedAttachProofRefusal::DeliveryParticipantMismatch);
    }
    if progress.delivery.binding_epoch != progress.binding_epoch {
        return Err(FencedAttachProofRefusal::DeliveryEpochMismatch);
    }
    if progress.delivery.marker_delivery_seq != progress.marker_delivery_seq {
        return Err(FencedAttachProofRefusal::DeliveryMarkerMismatch);
    }
    Ok(())
}
