use std::error::Error;
use std::sync::Arc;

use liminal::durability::{bridge::block_on, open_ephemeral};
use liminal_protocol::algebra::ResourceVector;
use liminal_protocol::lifecycle::test_support_external::{
    ExecutableRecoveredAttach, executable_recovered_attach,
};
use liminal_protocol::lifecycle::{BindingState, CapacityCounter, ConnectionConversationTracking};
use liminal_protocol::wire::{
    AttachSecret, BindingEpoch, ClientRequest, ConnectionIncarnation, ConversationId,
    EnrollmentRequest, EnrollmentToken, LeaveAttemptToken, LeaveRequest, ParticipantId,
    ServerValue,
};

use crate::server::participant::{
    ConnectionFateClass, ConnectionFateWorkItem, ParticipantConnectionConversations,
};

use super::ProductionParticipantHandler;
use super::barrier::{OperationFacts, ReceiptCapacityLimits};
use super::log::{
    DecodedStoredOperation, OperationLog, OperationLogError, StoredFinalizerPresentation,
    StoredLeaveV3, StoredOperation,
};
use super::state::{ConversationAuthority, DurableAppend, PendingBindingFate};
use super::tests::{dispatch_tracked, test_participant_config};

struct PendingFinalizerAppender<'a> {
    log: &'a OperationLog,
}

impl DurableAppend for PendingFinalizerAppender<'_> {
    fn append(
        &self,
        operation: &StoredOperation,
        expected_sequence: u64,
    ) -> Result<(), OperationLogError> {
        block_on(self.log.append(operation, expected_sequence))?
    }
}

struct ReservationSeed {
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    recovered_epoch: BindingEpoch,
    attach_secret: AttachSecret,
    attached_source_sequence: u64,
    died_source_sequence: u64,
    recovered_progress: u64,
}

struct ReservationProof {
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    recovered_epoch: BindingEpoch,
    attach_secret: AttachSecret,
    attached_source_sequence: u64,
    died_source_sequence: u64,
    recovered_source_sequence: u64,
    recovered_progress: u64,
    died_operation: StoredOperation,
    recovered_operation: StoredOperation,
    left: StoredLeaveV3,
}

#[test]
pub(super) fn pending_died_recovered_reservation_makes_leave_finalizer_non_presenting()
-> Result<(), Box<dyn Error>> {
    let proof = run_live_reservation()?;
    replay_cold_reservation(&proof)
}

fn enroll(
    handler: &ProductionParticipantHandler,
    connection_incarnation: ConnectionIncarnation,
    conversation_id: ConversationId,
    participant_id: ParticipantId,
) -> Result<(ParticipantConnectionConversations, AttachSecret), Box<dyn Error>> {
    let mut conversations = ParticipantConnectionConversations::default();
    let enrolled = dispatch_tracked(
        handler,
        connection_incarnation,
        &mut conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([73; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt) = enrolled else {
        return Err(format!("reservation fixture enrollment did not bind: {enrolled:?}").into());
    };
    assert_eq!(receipt.participant_id(), participant_id);
    Ok((conversations, receipt.attach_secret()))
}

fn install_recovered_fixture(
    authority: &mut ConversationAuthority,
    recovered: ExecutableRecoveredAttach,
    participant_id: ParticipantId,
    attached_source_sequence: u64,
    attach_secret: AttachSecret,
) -> Result<(), Box<dyn Error>> {
    authority.replace_frontier_for_test(recovered.owner)?;
    authority.next_seq = recovered.next_terminal_sequence;
    authority.next_order = recovered.next_terminal_order;
    let slot = authority
        .slots
        .get_mut(&participant_id)
        .ok_or("reservation participant slot is absent")?;
    slot.member = recovered.member;
    slot.binding = recovered.binding;
    slot.cell = recovered.detach_cell;
    slot.attach_secret = attach_secret;
    slot.binding_fate = Some(PendingBindingFate {
        attached_source_sequence,
        token: recovered.fate_token,
    });
    Ok(())
}

fn extend_finalizer_capacity(
    authority: &mut ConversationAuthority,
    conversation_id: ConversationId,
    participant_id: ParticipantId,
) -> Result<(), Box<dyn Error>> {
    let slot = authority
        .slots
        .get(&participant_id)
        .ok_or("reservation slot disappeared after Recovered")?;
    let BindingState::PendingFinalization(pending) = slot.binding else {
        return Err("reservation Died disposition was not Pending".into());
    };
    let terminal_charge = super::frontier::terminal_charge(
        conversation_id,
        participant_id,
        pending.binding_epoch(),
        pending.admission_order().transaction_order(),
        authority.next_seq,
    )?;
    let left_charge = super::frontier::left_record_charge();
    let finalizer_charge = ResourceVector::new(
        terminal_charge
            .entries
            .checked_add(left_charge.entries)
            .ok_or("reservation finalizer entry charge overflow")?,
        terminal_charge
            .bytes
            .checked_add(left_charge.bytes)
            .ok_or("reservation finalizer byte charge overflow")?,
    );
    let finalizer_rows = u64::try_from([terminal_charge, left_charge].len())?;
    let frontier = authority
        .take_frontier()?
        .with_pending_finalizer_test_capacity(finalizer_rows, finalizer_charge)?;
    authority.install_frontier(frontier)?;
    Ok(())
}

fn operation_facts(
    connection_incarnation: ConnectionIncarnation,
) -> Result<OperationFacts, Box<dyn Error>> {
    let config = test_participant_config();
    let connection_capacity =
        CapacityCounter::try_new(config.max_semantic_conversations_per_connection, 0)
            .map_err(|error| format!("reservation connection capacity is invalid: {error:?}"))?;
    Ok(OperationFacts {
        receiving_incarnation: connection_incarnation,
        now_ms: 0,
        identity_slots: config.identity_slots,
        attach_receipt_ttl_ms: config.attach_receipt_ttl_ms,
        receipt_provenance_ttl_ms: config.receipt_provenance_ttl_ms,
        receipt_limits: ReceiptCapacityLimits {
            identity_server: config.max_retired_identity_slots_server,
            live_receipts_server: config.max_live_attach_receipts_server,
            live_receipts_per_participant: config.max_live_attach_receipts_per_participant,
            provenance_server: config.max_receipt_provenance_server,
            provenance_per_conversation: config.max_receipt_provenance_per_conversation,
            provenance_per_participant: config.max_receipt_provenance_per_participant,
        },
        connection_tracking: ConnectionConversationTracking::Untracked,
        connection_capacity,
    })
}

fn run_live_reservation() -> Result<ReservationProof, Box<dyn Error>> {
    let recovered = executable_recovered_attach()?;
    let conversation_id = recovered.member.conversation_id();
    let participant_id = recovered.member.participant_id();
    let recovered_epoch = recovered.recovered_binding_epoch;
    let connection_incarnation = recovered_epoch.connection_incarnation;
    let store = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let (conversations, attach_secret) = enroll(
        &handler,
        connection_incarnation,
        conversation_id,
        participant_id,
    )?;
    let log = OperationLog::new(Arc::clone(&handler.store), conversation_id);
    let appender = PendingFinalizerAppender { log: &log };
    let cell = handler.cell(conversation_id)?;
    let (attached_source_sequence, died_source_sequence, recovered_progress) = {
        let mut owner = cell
            .lock()
            .map_err(|_| "reservation fixture owner lock was poisoned")?;
        let authority = owner
            .as_mut()
            .ok_or("reservation fixture owner was unavailable")?;
        let attached = authority
            .next_log_sequence
            .checked_sub(1)
            .ok_or("reservation Attached source sequence underflow")?;
        let died = authority.next_log_sequence;
        install_recovered_fixture(
            authority,
            recovered,
            participant_id,
            attached,
            attach_secret,
        )?;
        let work_item = ConnectionFateWorkItem {
            open_sequence: 31,
            connection_incarnation,
            class: ConnectionFateClass::ConnectionLost,
            tracked_conversations: conversations.tracked_conversations(),
        };
        authority
            .prepare_connection_fate_transaction(&work_item)
            .complete(authority, &appender)?;
        extend_finalizer_capacity(authority, conversation_id, participant_id)?;
        let recovered_progress = authority.observer_progress;
        authority.apply_leave(
            &LeaveRequest {
                conversation_id,
                participant_id,
                capability_generation: recovered_epoch.capability_generation,
                attach_secret,
                leave_attempt_token: LeaveAttemptToken::new([79; 16]),
            },
            &operation_facts(connection_incarnation)?,
            &appender,
        )?;
        assert_eq!(authority.observer_progress, recovered_progress);
        assert!(authority.retired.contains_key(&participant_id));
        drop(owner);
        (attached, died, recovered_progress)
    };
    read_live_proof(
        &log,
        &ReservationSeed {
            conversation_id,
            participant_id,
            recovered_epoch,
            attach_secret,
            attached_source_sequence,
            died_source_sequence,
            recovered_progress,
        },
    )
}

fn read_live_proof(
    log: &OperationLog,
    seed: &ReservationSeed,
) -> Result<ReservationProof, Box<dyn Error>> {
    let recovered_source_sequence = seed
        .died_source_sequence
        .checked_add(1)
        .ok_or("reservation Recovered source sequence overflow")?;
    let left_source_sequence = recovered_source_sequence
        .checked_add(1)
        .ok_or("reservation Left source sequence overflow")?;
    let died = block_on(log.read_at(seed.died_source_sequence))??
        .ok_or("reservation Died row is absent")?;
    let DecodedStoredOperation::V3(died_operation @ StoredOperation::Died { .. }) = died.operation
    else {
        return Err("reservation fixture expected a Died operation".into());
    };
    let recovered = block_on(log.read_at(recovered_source_sequence))??
        .ok_or("reservation Recovered row is absent")?;
    let DecodedStoredOperation::V3(recovered_operation @ StoredOperation::Recovered { .. }) =
        recovered.operation
    else {
        return Err("reservation fixture expected a Recovered operation".into());
    };
    let left =
        block_on(log.read_at(left_source_sequence))??.ok_or("reserved Left row is absent")?;
    let DecodedStoredOperation::V3(StoredOperation::Left { row }) = left.operation else {
        return Err("reservation fixture expected a Left row".into());
    };
    assert_eq!(row.pending_source_sequence, Some(seed.died_source_sequence));
    assert!(matches!(
        row.finalizer_presentation,
        StoredFinalizerPresentation::ConsumeRecoveredReservation {
            recovered_source_sequence: source
        } if source == recovered_source_sequence
    ));
    Ok(ReservationProof {
        conversation_id: seed.conversation_id,
        participant_id: seed.participant_id,
        recovered_epoch: seed.recovered_epoch,
        attach_secret: seed.attach_secret,
        attached_source_sequence: seed.attached_source_sequence,
        died_source_sequence: seed.died_source_sequence,
        recovered_source_sequence,
        recovered_progress: seed.recovered_progress,
        died_operation,
        recovered_operation,
        left: row,
    })
}

fn replay_cold_reservation(proof: &ReservationProof) -> Result<(), Box<dyn Error>> {
    let cold_recovered = executable_recovered_attach()?;
    let cold_store = Arc::new(open_ephemeral(1)?);
    let cold = ProductionParticipantHandler::new(cold_store, test_participant_config())?;
    let connection_incarnation = proof.recovered_epoch.connection_incarnation;
    enroll(
        &cold,
        connection_incarnation,
        proof.conversation_id,
        proof.participant_id,
    )?;
    let cold_cell = cold.cell(proof.conversation_id)?;
    {
        let mut cold_owner = cold_cell
            .lock()
            .map_err(|_| "cold reservation owner lock was poisoned")?;
        let cold_authority = cold_owner
            .as_mut()
            .ok_or("cold reservation owner was unavailable")?;
        install_recovered_fixture(
            cold_authority,
            cold_recovered,
            proof.participant_id,
            proof.attached_source_sequence,
            proof.attach_secret,
        )?;
        cold_authority.route_fate_occurrence(&proof.died_operation, proof.died_source_sequence)?;
        let StoredOperation::Died { row: died_row } = &proof.died_operation else {
            return Err("routed cold operation stopped being Died".into());
        };
        cold_authority.replay_died_source(died_row, proof.died_source_sequence)?;
        cold_authority
            .replay_specific_fate(&proof.recovered_operation, proof.recovered_source_sequence)?;
        extend_finalizer_capacity(cold_authority, proof.conversation_id, proof.participant_id)?;
        cold_authority.replay_leave(&proof.left)?;
        assert_eq!(cold_authority.observer_progress, proof.recovered_progress);
        assert!(cold_authority.retired.contains_key(&proof.participant_id));
        drop(cold_owner);
    }
    Ok(())
}
