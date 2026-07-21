use std::error::Error;
use std::sync::Arc;

use liminal::durability::{bridge::block_on, open_ephemeral};
use liminal_protocol::algebra::{ResourceVector, WideResourceVector};
use liminal_protocol::lifecycle::LiveFrontierOwner;
use liminal_protocol::lifecycle::test_support_external::{
    ExecutablePendingFencedAttach, executable_pending_fenced_attach, executable_recovered_attach,
};
use liminal_protocol::wire::{
    AttachAttemptToken, AttachSecret, BindingEpoch, ClientRequest, ConversationId,
    CredentialAttachRequest, EnrollmentRequest, EnrollmentToken, ParticipantId, ServerValue,
};

use crate::server::participant::{
    ConnectionFateClass, ConnectionFateWorkItem, ParticipantConnectionConversations,
};

use super::ProductionParticipantHandler;
use super::barrier::CommitMode;
use super::fenced_attach_codec::{
    StoredDebtCompletion, StoredDetachedCredentialRecovery, StoredMarkerCursorProgress,
    StoredMarkerDelivery, StoredProofBinding, StoredRecoveryTerminal, StoredWideResourceVector,
};
use super::log::{
    DecodedStoredOperation, OperationLog, OperationLogError, StoredAttachAllocation,
    StoredAttachModeV3, StoredComposedTerminal, StoredComposedTerminalCause,
    StoredComposedTerminalKind, StoredFencedAttachProof, StoredFinalizerPresentation,
    StoredMarkerDrain, StoredOperation, StoredResourceVector, StoredRetainedCharge, StoredU128,
};
use super::marker_source::canonical_marker_bytes;
use super::outbox_projection::project_attached_records;
use super::state::{DurableAppend, PendingBindingFate};
use super::tests::{dispatch_tracked, test_participant_config};

pub(super) struct FencedAppender<'a> {
    pub(super) log: &'a OperationLog,
}

impl DurableAppend for FencedAppender<'_> {
    fn append(
        &self,
        operation: &StoredOperation,
        expected_sequence: u64,
    ) -> Result<(), OperationLogError> {
        block_on(self.log.append(operation, expected_sequence))?
    }
}

struct ReservedSetup {
    handler: ProductionParticipantHandler,
    log: OperationLog,
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    recovered_epoch: BindingEpoch,
    died_source_sequence: u64,
    recovered_source_sequence: u64,
    recovered_progress: u64,
    next_log_sequence: u64,
}

pub(super) struct FencedInputs {
    pub(super) request: CredentialAttachRequest,
    pub(super) allocation: StoredAttachAllocation,
    pub(super) mode: StoredAttachModeV3,
}

#[test]
pub(super) fn pending_died_recovered_reservation_makes_fenced_attach_finalizer_non_presenting()
-> Result<(), Box<dyn Error>> {
    let setup = reserved_setup()?;
    commit_fenced_finalizer(&setup)
}

const fn stored_wide(value: WideResourceVector) -> StoredWideResourceVector {
    StoredWideResourceVector {
        entries: StoredU128(value.entries.to_be_bytes()),
        bytes: StoredU128(value.bytes.to_be_bytes()),
    }
}

pub(super) fn marker_source(
    owner: LiveFrontierOwner,
    recovery: liminal_protocol::lifecycle::DetachedCredentialRecovery,
) -> Result<(LiveFrontierOwner, StoredMarkerDrain), Box<dyn Error>> {
    let retained = owner
        .retain_fenced_marker_source(recovery)
        .map_err(|_| "pending fenced frontier refused its recovery marker")?;
    let expectation = retained.expectation();
    let marker = canonical_marker_bytes(expectation);
    let marker_bytes = u64::try_from(marker.len())?;
    let order = expectation.admission_order();
    let row = StoredMarkerDrain {
        marker,
        retained_charge: StoredRetainedCharge {
            delivery_seq: expectation.marker_delivery_seq(),
            transaction_order: order.transaction_order(),
            candidate_phase: order.candidate_phase() as u8,
            participant_id: expectation.participant_id(),
            charge: StoredResourceVector {
                entries: 1,
                bytes: marker_bytes,
            },
        },
        resulting_retained_charges: vec![],
        successor: vec![],
    };
    let (owner, returned) = retained.into_parts();
    assert_eq!(returned, recovery);
    Ok((owner, row))
}

fn reserved_setup() -> Result<ReservedSetup, Box<dyn Error>> {
    let recovered = executable_recovered_attach()?;
    let conversation_id = recovered.member.conversation_id();
    let participant_id = recovered.member.participant_id();
    let recovered_epoch = recovered.recovered_binding_epoch;
    let connection_incarnation = recovered_epoch.connection_incarnation;
    let store = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let mut conversations = ParticipantConnectionConversations::default();
    let enrolled = dispatch_tracked(
        &handler,
        connection_incarnation,
        &mut conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([83; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt) = enrolled else {
        return Err(format!("fenced reservation enrollment did not bind: {enrolled:?}").into());
    };
    assert_eq!(receipt.participant_id(), participant_id);
    let log = OperationLog::new(Arc::clone(&handler.store), conversation_id);
    let appender = FencedAppender { log: &log };
    let cell = handler.cell(conversation_id)?;
    let (died_source_sequence, recovered_progress, next_log_sequence) = {
        let mut owner = cell
            .lock()
            .map_err(|_| "fenced reservation owner lock was poisoned")?;
        let authority = owner
            .as_mut()
            .ok_or("fenced reservation owner was unavailable")?;
        let attached_source_sequence = authority
            .next_log_sequence
            .checked_sub(1)
            .ok_or("fenced reservation Attached source underflow")?;
        authority.frontier = Some(recovered.owner);
        authority.next_seq = recovered.next_terminal_sequence;
        authority.next_order = recovered.next_terminal_order;
        let slot = authority
            .slots
            .get_mut(&participant_id)
            .ok_or("fenced reservation participant slot is absent")?;
        slot.member = recovered.member;
        slot.binding = recovered.binding;
        slot.cell = recovered.detach_cell;
        slot.binding_fate = Some(PendingBindingFate {
            attached_source_sequence,
            token: recovered.fate_token,
        });
        let died = authority.next_log_sequence;
        authority
            .prepare_connection_fate_transaction(&ConnectionFateWorkItem {
                open_sequence: 41,
                connection_incarnation,
                class: ConnectionFateClass::ConnectionLost,
                tracked_conversations: conversations.tracked_conversations(),
            })
            .complete(authority, &appender)?;
        let values = (
            died,
            authority.observer_progress,
            authority.next_log_sequence,
        );
        drop(owner);
        values
    };
    Ok(ReservedSetup {
        handler,
        log,
        conversation_id,
        participant_id,
        recovered_epoch,
        died_source_sequence,
        recovered_source_sequence: died_source_sequence
            .checked_add(1)
            .ok_or("fenced Recovered source overflow")?,
        recovered_progress,
        next_log_sequence,
    })
}

pub(super) fn fenced_inputs(
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    prior_binding_epoch: BindingEpoch,
    died_source_sequence: u64,
    presentation: StoredFinalizerPresentation,
    fixture: &ExecutablePendingFencedAttach,
    marker_source_sequence: u64,
) -> Result<FencedInputs, Box<dyn Error>> {
    let request = CredentialAttachRequest {
        conversation_id,
        participant_id,
        capability_generation: prior_binding_epoch.capability_generation,
        attach_secret: fixture.attach_secret,
        attach_attempt_token: AttachAttemptToken::new([89; 16]),
        accept_marker_delivery_seq: Some(fixture.marker_delivery_seq),
    };
    let allocation = StoredAttachAllocation {
        binding_epoch: fixture.recovered_binding_epoch.into(),
        attach_secret: AttachSecret::new([91; 32]).into_bytes(),
        attached_order: fixture.attached_order,
        attached_seq: fixture.attached_seq,
        receipt_expires_at: StoredU128(100_u128.to_be_bytes()),
        provenance_expires_at: StoredU128(200_u128.to_be_bytes()),
        admitted_now_ms: 0,
    };
    let prior = prior_binding_epoch.into();
    let recovery = StoredDetachedCredentialRecovery {
        conversation_id,
        participant_id,
        marker_delivery_seq: fixture.marker_delivery_seq,
        prior_binding_epoch: prior,
        resulting_floor: fixture.marker_delivery_seq,
        terminal: StoredRecoveryTerminal::Pending {
            binding: StoredProofBinding {
                conversation_id,
                participant_id,
                binding_epoch: prior,
            },
            cause: StoredComposedTerminalCause::ConnectionLost,
            transaction_order: fixture.terminal_order,
        },
        progress: StoredMarkerCursorProgress {
            conversation_id,
            participant_id,
            binding_epoch: prior,
            through_seq: fixture.marker_delivery_seq,
            marker_delivery_seq: fixture.marker_delivery_seq,
            delivery: StoredMarkerDelivery {
                participant_id,
                binding_epoch: prior,
                marker_delivery_seq: fixture.marker_delivery_seq,
            },
        },
    };
    let proof = StoredFencedAttachProof::encode(
        &recovery,
        stored_wide(fixture.predecessor_debt),
        fixture.fenced_resulting_floor,
        StoredDebtCompletion::ObserverProjection {
            debt: stored_wide(fixture.predecessor_debt),
            through_seq: fixture.fenced_resulting_floor,
        },
    )?;
    let mode = StoredAttachModeV3::Fenced {
        prior_binding_epoch: prior,
        marker_delivery_seq: fixture.marker_delivery_seq,
        marker_source_sequence,
        proof,
        composed_terminal: Some(StoredComposedTerminal {
            kind: StoredComposedTerminalKind::Died,
            cause: StoredComposedTerminalCause::ConnectionLost,
            transaction_order: fixture.terminal_order,
            delivery_seq: fixture.terminal_delivery_seq,
            pending_source_sequence: died_source_sequence,
            presentation,
        }),
    };
    Ok(FencedInputs {
        request,
        allocation,
        mode,
    })
}

pub(super) fn extend_finalizer_capacity(
    authority: &mut super::state::ConversationAuthority,
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    prior_binding_epoch: BindingEpoch,
    terminal_order: u64,
    terminal_delivery_seq: u64,
    allocation: &StoredAttachAllocation,
) -> Result<(), Box<dyn Error>> {
    let terminal = super::frontier::terminal_charge(
        conversation_id,
        participant_id,
        prior_binding_epoch,
        terminal_order,
        terminal_delivery_seq,
    )?;
    let attached =
        super::frontier::credential_attached_charge(conversation_id, participant_id, allocation)?;
    let charge = ResourceVector::new(
        terminal
            .entries
            .checked_add(attached.entries)
            .ok_or("fenced finalizer entry charge overflow")?,
        terminal
            .bytes
            .checked_add(attached.bytes)
            .ok_or("fenced finalizer byte charge overflow")?,
    );
    let frontier = authority
        .frontier
        .take()
        .ok_or("fenced finalizer frontier disappeared")?
        .with_pending_finalizer_test_capacity(2, charge)?;
    authority.frontier = Some(frontier);
    Ok(())
}

fn commit_fenced_finalizer(setup: &ReservedSetup) -> Result<(), Box<dyn Error>> {
    let fixture = executable_pending_fenced_attach()?;
    assert_eq!(fixture.prior_binding_epoch, setup.recovered_epoch);
    let marker_source_sequence = setup.next_log_sequence;
    let inputs = fenced_inputs(
        setup.conversation_id,
        setup.participant_id,
        setup.recovered_epoch,
        setup.died_source_sequence,
        StoredFinalizerPresentation::ConsumeRecoveredReservation {
            recovered_source_sequence: setup.recovered_source_sequence,
        },
        &fixture,
        marker_source_sequence,
    )?;
    let (frontier, marker_row) = marker_source(fixture.owner, fixture.recovery)?;
    block_on(setup.log.append(
        &StoredOperation::MarkerDrained { row: marker_row },
        marker_source_sequence,
    ))??;
    let cell = setup.handler.cell(setup.conversation_id)?;
    let attached_source_sequence = {
        let mut owner = cell
            .lock()
            .map_err(|_| "fenced finalizer owner lock was poisoned")?;
        let authority = owner
            .as_mut()
            .ok_or("fenced finalizer owner was unavailable")?;
        authority.frontier = Some(frontier);
        authority.next_seq = fixture.terminal_delivery_seq;
        authority.next_order = fixture.terminal_order;
        authority.next_log_sequence = marker_source_sequence
            .checked_add(1)
            .ok_or("fenced Attached source overflow")?;
        extend_finalizer_capacity(
            authority,
            setup.conversation_id,
            setup.participant_id,
            setup.recovered_epoch,
            fixture.terminal_order,
            fixture.terminal_delivery_seq,
            &inputs.allocation,
        )?;
        let slot = authority
            .slots
            .get_mut(&setup.participant_id)
            .ok_or("fenced finalizer slot disappeared")?;
        slot.member = fixture.member;
        slot.binding = fixture.binding;
        slot.cell = fixture.detach_cell;
        slot.attach_secret = fixture.attach_secret;
        let attached_source = authority.next_log_sequence;
        authority.attach_commit(
            &inputs.request,
            &inputs.allocation,
            &inputs.mode,
            Arc::clone(&setup.handler.store),
            CommitMode::Live(&FencedAppender { log: &setup.log }),
        )?;
        assert_eq!(authority.observer_progress, setup.recovered_progress);
        let records =
            project_attached_records(setup.participant_id, &inputs.allocation, &inputs.mode, None)?;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, fixture.attached_seq);
        drop(owner);
        attached_source
    };
    let attached = block_on(setup.log.read_at(attached_source_sequence))??
        .ok_or("reservation-consuming fenced Attached row is absent")?;
    let DecodedStoredOperation::V3(StoredOperation::Attached { mode, .. }) = attached.operation
    else {
        return Err("reservation fixture expected fenced Attached".into());
    };
    assert_eq!(*mode, inputs.mode);
    Ok(())
}
