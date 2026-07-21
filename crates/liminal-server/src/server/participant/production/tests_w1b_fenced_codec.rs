use std::error::Error;

use super::fenced_attach_codec::{
    FencedAttachProofContext, StoredDebtCompletion, StoredDetachedCredentialRecovery,
    StoredMarkerCursorProgress, StoredMarkerDelivery, StoredProofBinding, StoredRecoveryTerminal,
    StoredWideResourceVector,
};
use super::log::{
    FencedAttachProofRefusal, OperationLogError, StoredAttachAllocation, StoredAttachModeV3,
    StoredAttachRequest, StoredBindingEpoch, StoredComposedTerminal, StoredComposedTerminalKind,
    StoredFencedAttachProof, StoredFinalizerPresentation, StoredOperation, StoredU128,
};
use super::log_v3::StoredComposedTerminalCause;

const CONVERSATION: u64 = 41;
const PARTICIPANT: u64 = 3;
const MARKER: u64 = 17;
const PRIOR: StoredBindingEpoch = StoredBindingEpoch {
    server_incarnation: 7,
    connection_ordinal: 8,
    capability_generation: 11,
};
const NEXT: StoredBindingEpoch = StoredBindingEpoch {
    server_incarnation: 9,
    connection_ordinal: 10,
    capability_generation: 12,
};

const fn wide(entries: u128, bytes: u128) -> StoredWideResourceVector {
    StoredWideResourceVector {
        entries: StoredU128(entries.to_be_bytes()),
        bytes: StoredU128(bytes.to_be_bytes()),
    }
}

fn recovery() -> StoredDetachedCredentialRecovery {
    StoredDetachedCredentialRecovery {
        conversation_id: CONVERSATION,
        participant_id: PARTICIPANT,
        marker_delivery_seq: MARKER,
        prior_binding_epoch: PRIOR,
        resulting_floor: 13,
        terminal: StoredRecoveryTerminal::Committed {
            binding: StoredProofBinding {
                conversation_id: CONVERSATION,
                participant_id: PARTICIPANT,
                binding_epoch: PRIOR,
            },
            cause: StoredComposedTerminalCause::ConnectionLost,
            transaction_order: 14,
            delivery_seq: 15,
        },
        progress: StoredMarkerCursorProgress {
            conversation_id: CONVERSATION,
            participant_id: PARTICIPANT,
            binding_epoch: PRIOR,
            through_seq: MARKER,
            marker_delivery_seq: MARKER,
            delivery: StoredMarkerDelivery {
                participant_id: PARTICIPANT,
                binding_epoch: PRIOR,
                marker_delivery_seq: MARKER,
            },
        },
    }
}

const fn context() -> FencedAttachProofContext {
    FencedAttachProofContext {
        conversation_id: CONVERSATION,
        participant_id: PARTICIPANT,
        request_marker_delivery_seq: Some(MARKER),
        prior_binding_epoch: PRIOR,
        marker_delivery_seq: MARKER,
        new_binding_epoch: NEXT,
    }
}

fn proof_with(recovery: &StoredDetachedCredentialRecovery) -> StoredFencedAttachProof {
    StoredFencedAttachProof::encode(
        recovery,
        wide(1, 2),
        18,
        StoredDebtCompletion::PhysicalCompaction {
            debt: wide(2, 3),
            from_floor: 4,
            through_seq: 5,
        },
    )
    .unwrap_or_else(|error| unreachable!("test proof encoding failed: {error}"))
}

fn fenced_operation(proof: StoredFencedAttachProof) -> StoredOperation {
    StoredOperation::Attached {
        request: StoredAttachRequest {
            conversation_id: CONVERSATION,
            participant_id: PARTICIPANT,
            capability_generation: NEXT.capability_generation,
            attach_secret: [1; 32],
            token: [2; 16],
            accept_marker_delivery_seq: Some(MARKER),
        },
        secret_verified: true,
        allocation: StoredAttachAllocation {
            binding_epoch: NEXT,
            attach_secret: [3; 32],
            attached_order: 20,
            attached_seq: 21,
            receipt_expires_at: StoredU128(22_u128.to_be_bytes()),
            provenance_expires_at: StoredU128(23_u128.to_be_bytes()),
            admitted_now_ms: 24,
        },
        mode: Box::new(StoredAttachModeV3::Fenced {
            prior_binding_epoch: PRIOR,
            marker_delivery_seq: MARKER,
            marker_source_sequence: 6,
            proof,
            composed_terminal: None,
        }),
        event: vec![4, 5],
    }
}

#[test]
fn attached_v3_closed_modes_round_trip_complete_fenced_proof() -> Result<(), Box<dyn Error>> {
    let proof = proof_with(&recovery());
    let decoded = proof.decode(context())?;
    assert_eq!(decoded.detached_credential_recovery, recovery());
    assert_eq!(decoded.predecessor_debt, wide(1, 2));
    assert_eq!(decoded.fenced_resulting_floor, 18);
    assert_eq!(
        decoded.successor,
        StoredDebtCompletion::PhysicalCompaction {
            debt: wide(2, 3),
            from_floor: 4,
            through_seq: 5,
        }
    );

    let modes = [
        StoredAttachModeV3::Ordinary,
        StoredAttachModeV3::Superseding {
            prior_binding_epoch: PRIOR,
            terminal_transaction_order: 20,
            terminal_delivery_seq: 19,
        },
        match fenced_operation(proof.clone()) {
            StoredOperation::Attached { mode, .. } => *mode,
            _ => unreachable!("helper always constructs Attached"),
        },
    ];
    for mode in modes {
        let mut operation = fenced_operation(proof.clone());
        let StoredOperation::Attached {
            request,
            mode: stored_mode,
            ..
        } = &mut operation
        else {
            unreachable!("helper always constructs Attached");
        };
        if !matches!(mode, StoredAttachModeV3::Fenced { .. }) {
            request.accept_marker_delivery_seq = None;
        }
        **stored_mode = mode;
        operation.validate_durable(30)?;
        let bytes = serde_json::to_vec(&operation)?;
        let restored: StoredOperation = serde_json::from_slice(&bytes)?;
        restored.validate_durable(30)?;
        assert_eq!(serde_json::to_vec(&restored)?, bytes);
    }
    super::tests_w1b_marker_source::assert_exact_source_association()?;
    Ok(())
}

#[test]
fn composed_terminal_decode_validates_kind_cause_order_source_and_presentation() {
    let mut operation = fenced_operation(proof_with(&recovery()));
    let StoredOperation::Attached {
        allocation, mode, ..
    } = &mut operation
    else {
        unreachable!("helper always constructs Attached");
    };
    let Some(pending_terminal_order) = allocation.attached_order.checked_sub(1) else {
        unreachable!("fixture Attached order has a predecessor");
    };
    let StoredAttachModeV3::Fenced {
        composed_terminal, ..
    } = mode.as_mut()
    else {
        unreachable!("helper always constructs Fenced");
    };
    *composed_terminal = Some(StoredComposedTerminal {
        kind: StoredComposedTerminalKind::Died,
        cause: StoredComposedTerminalCause::ConnectionLost,
        transaction_order: pending_terminal_order,
        delivery_seq: 19,
        pending_source_sequence: 5,
        presentation: StoredFinalizerPresentation::PresentEnclosing,
    });
    assert!(operation.validate_durable(30).is_ok());

    let assert_refusal = |mutate: fn(&mut StoredComposedTerminal), expected| {
        let mut candidate = operation.clone();
        let StoredOperation::Attached { mode, .. } = &mut candidate else {
            unreachable!("helper always constructs Attached");
        };
        let StoredAttachModeV3::Fenced {
            composed_terminal: Some(terminal),
            ..
        } = mode.as_mut()
        else {
            unreachable!("helper always carries composed terminal");
        };
        mutate(terminal);
        assert!(matches!(
            candidate.validate_durable(30),
            Err(OperationLogError::FencedAttachProof { reason, .. }) if reason == expected
        ));
    };
    assert_refusal(
        |terminal| terminal.kind = StoredComposedTerminalKind::Detached,
        FencedAttachProofRefusal::ComposedTerminalKindCause,
    );
    assert_refusal(
        |terminal| {
            let Some(attached_order) = terminal.transaction_order.checked_add(1) else {
                unreachable!("fixture pending terminal order has a successor");
            };
            terminal.transaction_order = attached_order;
        },
        FencedAttachProofRefusal::ComposedTerminalOrder,
    );
    assert_refusal(
        |terminal| terminal.pending_source_sequence = 30,
        FencedAttachProofRefusal::ComposedPendingSourceOrder,
    );
    assert_refusal(
        |terminal| {
            terminal.presentation = StoredFinalizerPresentation::ConsumeRecoveredReservation {
                recovered_source_sequence: 30,
            };
        },
        FencedAttachProofRefusal::ComposedRecoveredSourceOrder,
    );
    assert_refusal(
        |terminal| {
            terminal.kind = StoredComposedTerminalKind::Detached;
            terminal.cause = StoredComposedTerminalCause::CleanDeregister;
            terminal.presentation = StoredFinalizerPresentation::ConsumeRecoveredReservation {
                recovered_source_sequence: 6,
            };
        },
        FencedAttachProofRefusal::ComposedRecoveredReservationKind,
    );
}

#[test]
fn fenced_attach_proof_refuses_every_redundant_field_mismatch() {
    assert_recovery_refusal(
        |v| v.conversation_id += 1,
        FencedAttachProofRefusal::RecoveryConversationMismatch,
    );
    assert_recovery_refusal(
        |v| v.participant_id += 1,
        FencedAttachProofRefusal::RecoveryParticipantMismatch,
    );
    assert_recovery_refusal(
        |v| v.marker_delivery_seq += 1,
        FencedAttachProofRefusal::RecoveryMarkerMismatch,
    );
    assert_recovery_refusal(
        |v| v.prior_binding_epoch.connection_ordinal += 1,
        FencedAttachProofRefusal::RecoveryPriorEpochMismatch,
    );
    assert_recovery_refusal(
        |v| v.progress.conversation_id += 1,
        FencedAttachProofRefusal::ProgressConversationMismatch,
    );
    assert_recovery_refusal(
        |v| v.progress.participant_id += 1,
        FencedAttachProofRefusal::ProgressParticipantMismatch,
    );
    assert_recovery_refusal(
        |v| v.progress.binding_epoch.connection_ordinal += 1,
        FencedAttachProofRefusal::ProgressEpochMismatch,
    );
    assert_recovery_refusal(
        |v| v.progress.marker_delivery_seq += 1,
        FencedAttachProofRefusal::ProgressMarkerMismatch,
    );
    assert_recovery_refusal(
        |v| v.progress.through_seq += 1,
        FencedAttachProofRefusal::ProgressThroughMismatch,
    );
    assert_recovery_refusal(
        |v| v.progress.delivery.participant_id += 1,
        FencedAttachProofRefusal::DeliveryParticipantMismatch,
    );
    assert_recovery_refusal(
        |v| v.progress.delivery.binding_epoch.connection_ordinal += 1,
        FencedAttachProofRefusal::DeliveryEpochMismatch,
    );
    assert_recovery_refusal(
        |v| v.progress.delivery.marker_delivery_seq += 1,
        FencedAttachProofRefusal::DeliveryMarkerMismatch,
    );
    assert_recovery_refusal(
        |v| terminal_binding(&mut v.terminal).conversation_id += 1,
        FencedAttachProofRefusal::TerminalConversationMismatch,
    );
    assert_recovery_refusal(
        |v| terminal_binding(&mut v.terminal).participant_id += 1,
        FencedAttachProofRefusal::TerminalParticipantMismatch,
    );
    assert_recovery_refusal(
        |v| {
            terminal_binding(&mut v.terminal)
                .binding_epoch
                .connection_ordinal += 1;
        },
        FencedAttachProofRefusal::TerminalEpochMismatch,
    );

    let mut wrong_context = context();
    wrong_context.request_marker_delivery_seq = Some(MARKER + 1);
    assert_eq!(
        proof_with(&recovery()).decode(wrong_context),
        Err(FencedAttachProofRefusal::RequestMarkerMismatch)
    );
    wrong_context = context();
    wrong_context.new_binding_epoch.capability_generation += 1;
    assert_eq!(
        proof_with(&recovery()).decode(wrong_context),
        Err(FencedAttachProofRefusal::NewBindingGenerationMismatch)
    );
}

fn assert_recovery_refusal(
    mutate: impl FnOnce(&mut StoredDetachedCredentialRecovery),
    expected: FencedAttachProofRefusal,
) {
    let mut value = recovery();
    mutate(&mut value);
    assert_eq!(proof_with(&value).decode(context()), Err(expected));
}

const fn terminal_binding(terminal: &mut StoredRecoveryTerminal) -> &mut StoredProofBinding {
    match terminal {
        StoredRecoveryTerminal::Committed { binding, .. }
        | StoredRecoveryTerminal::Pending { binding, .. } => binding,
    }
}

#[test]
fn fenced_attach_proof_refuses_noncanonical_malformed_debt_and_successor_bytes() {
    let valid = proof_with(&recovery());
    let mut proof = valid.clone();
    proof.detached_credential_recovery.push(b' ');
    assert_eq!(
        proof.decode(context()),
        Err(FencedAttachProofRefusal::DetachedCredentialRecoveryNonCanonical)
    );
    proof = valid;
    proof.predecessor_debt = b"not-json".to_vec();
    assert_eq!(
        proof.decode(context()),
        Err(FencedAttachProofRefusal::PredecessorDebtMalformed)
    );
    proof =
        StoredFencedAttachProof::encode(&recovery(), wide(0, 0), 18, StoredDebtCompletion::Clear)
            .unwrap_or_else(|error| unreachable!("test proof encoding failed: {error}"));
    assert_eq!(
        proof.decode(context()),
        Err(FencedAttachProofRefusal::PredecessorDebtZero)
    );
    proof = StoredFencedAttachProof::encode(
        &recovery(),
        wide(1, 1),
        18,
        StoredDebtCompletion::ObserverProjection {
            debt: wide(0, 0),
            through_seq: 2,
        },
    )
    .unwrap_or_else(|error| unreachable!("test proof encoding failed: {error}"));
    assert_eq!(
        proof.decode(context()),
        Err(FencedAttachProofRefusal::SuccessorDebtZero)
    );
    proof = StoredFencedAttachProof::encode(
        &recovery(),
        wide(1, 1),
        18,
        StoredDebtCompletion::PhysicalCompaction {
            debt: wide(1, 1),
            from_floor: 3,
            through_seq: 2,
        },
    )
    .unwrap_or_else(|error| unreachable!("test proof encoding failed: {error}"));
    assert_eq!(
        proof.decode(context()),
        Err(FencedAttachProofRefusal::SuccessorCompactionRange)
    );
}
