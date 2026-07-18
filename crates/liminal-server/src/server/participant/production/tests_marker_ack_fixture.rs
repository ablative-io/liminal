use std::error::Error;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal::durability::{DurableStore, open_ephemeral};
use liminal_protocol::lifecycle::{CapacityCounter, ConnectionConversationTracking};
use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, EnrollBound, EnrollmentRequest, EnrollmentToken,
    Generation, LeaveAttemptToken, LeaveRequest, ParticipantAck, ParticipantDelivery,
    ParticipantId, ParticipantRecord, RecordAdmission, RecordAdmissionAttemptToken,
    RecordCommitted, ServerValue,
};

use super::ProductionParticipantHandler;
use super::barrier::{OperationFacts, ReceiptCapacityLimits};
use super::log::{OperationLog, OperationLogError, StoredOperation};
use super::outbox_log::{OutboxLog, OutboxRow, ProducedBatch, ProducedSourceKind, ProjectedRecord};
use super::state::{ConversationAuthority, DurableAppend};
use super::tests::{dispatch, test_participant_config};
use crate::config::types::ParticipantConfig;

struct FixtureAppender<'a> {
    log: &'a OperationLog,
}

impl DurableAppend for FixtureAppender<'_> {
    fn append(
        &self,
        operation: &StoredOperation,
        expected_sequence: u64,
    ) -> Result<(), OperationLogError> {
        block_on(self.log.append(operation, expected_sequence))?
    }
}

pub(super) struct MarkerFixture {
    pub(super) handler: ProductionParticipantHandler,
    pub(super) store: Arc<dyn DurableStore>,
    pub(super) target_connection: ConnectionIncarnation,
    pub(super) target_participant: ParticipantId,
    pub(super) record_connection: ConnectionIncarnation,
    pub(super) record_participant: ParticipantId,
    pub(super) catchup_connection: ConnectionIncarnation,
    pub(super) catchup_participant: ParticipantId,
    pub(super) catchup_through_seq: u64,
    pub(super) marker_delivery: ParticipantDelivery,
}

struct FixtureMembers {
    first_connection: ConnectionIncarnation,
    second_connection: ConnectionIncarnation,
    first: EnrollBound,
    second: EnrollBound,
}

pub(super) fn marker_fixture_config() -> ParticipantConfig {
    let mut config = test_participant_config();
    // After retiring the transient peer, three ordinary commits generate marker
    // debt and the fourth drains it, preserving the original two-member fixture.
    config.retained_capacity_entries = 14;
    config.retained_capacity_bytes = 65_536;
    config.max_retained_record_rows = 16;
    config.max_ordinary_record_bytes = 58;
    config
}

fn marker_fixture_facts(
    connection: ConnectionIncarnation,
    config: &ParticipantConfig,
) -> Result<OperationFacts, Box<dyn Error>> {
    let connection_capacity =
        CapacityCounter::try_new(config.max_semantic_conversations_per_connection, 0)
            .map_err(|error| format!("marker fixture connection capacity is invalid: {error:?}"))?;
    Ok(OperationFacts {
        receiving_incarnation: connection,
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

fn append_fixture_outbox_row(
    authority: &mut ConversationAuthority,
    outbox_log: &OutboxLog,
    row: OutboxRow,
) -> Result<(), Box<dyn Error>> {
    let extension_sequence = authority
        .outbox
        .as_ref()
        .ok_or("marker fixture outbox owner is absent")?
        .next_extension_sequence();
    block_on(outbox_log.append(&row, extension_sequence))??;
    authority
        .outbox
        .as_mut()
        .ok_or("marker fixture outbox owner disappeared")?
        .apply_row(extension_sequence, row)?;
    Ok(())
}

fn enroll_members(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
) -> Result<FixtureMembers, Box<dyn Error>> {
    let first_connection = ConnectionIncarnation::new(0xA7, 1);
    let second_connection = ConnectionIncarnation::new(0xA7, 2);
    let first = dispatch(
        handler,
        first_connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0xA1; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(first) = first else {
        return Err(format!("first marker fixture enrollment failed: {first:?}").into());
    };
    let second = dispatch(
        handler,
        second_connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0xA2; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(second) = second else {
        return Err(format!("second marker fixture enrollment failed: {second:?}").into());
    };
    Ok(FixtureMembers {
        first_connection,
        second_connection,
        first,
        second,
    })
}

fn ack_members_through(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    members: &FixtureMembers,
    through_seq: u64,
) -> Result<(), Box<dyn Error>> {
    for (connection, participant_id) in [
        (members.first_connection, members.first.participant_id()),
        (members.second_connection, members.second.participant_id()),
    ] {
        let outcome = dispatch(
            handler,
            connection,
            ClientRequest::ParticipantAck(ParticipantAck {
                conversation_id,
                participant_id,
                capability_generation: Generation::ONE,
                through_seq,
            }),
        )?;
        assert!(matches!(outcome, ServerValue::AckCommitted(_)));
    }
    Ok(())
}

fn ack_marker_prefix(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    members: &FixtureMembers,
) -> Result<(), Box<dyn Error>> {
    let third_connection = ConnectionIncarnation::new(0xA7, 3);
    let third = dispatch(
        handler,
        third_connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0xA0; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(third) = third else {
        return Err(format!("third marker fixture enrollment failed: {third:?}").into());
    };
    ack_members_through(handler, conversation_id, members, 3)?;

    let left = dispatch(
        handler,
        third_connection,
        ClientRequest::Leave(LeaveRequest {
            conversation_id,
            participant_id: third.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: third.attach_secret(),
            leave_attempt_token: LeaveAttemptToken::new([0xA6; 16]),
        }),
    )?;
    assert!(matches!(left, ServerValue::LeaveCommitted(_)));
    ack_members_through(handler, conversation_id, members, 4)
}

fn commit_fixture_record(
    authority: &mut ConversationAuthority,
    operation_log: &OperationLog,
    config: &ParticipantConfig,
    connection: ConnectionIncarnation,
    request: &RecordAdmission,
    expected_rows: u64,
) -> Result<(u64, RecordCommitted), Box<dyn Error>> {
    let source_sequence = authority.next_log_sequence;
    let outcome = authority.apply_record_admission(
        request,
        &marker_fixture_facts(connection, config)?,
        config,
        &FixtureAppender { log: operation_log },
    )?;
    let ServerValue::RecordCommitted(record) = outcome.value else {
        return Err(format!("marker fixture record did not commit: {:?}", outcome.value).into());
    };
    assert_eq!(
        authority.next_log_sequence,
        source_sequence + expected_rows,
        "record at source {source_sequence} appended an unexpected row count"
    );
    Ok((source_sequence, record))
}

fn project_fixture_ordinary(
    authority: &mut ConversationAuthority,
    outbox_log: &OutboxLog,
    source_sequence: u64,
    record: &RecordCommitted,
    request: &RecordAdmission,
    members: &FixtureMembers,
) -> Result<(), Box<dyn Error>> {
    let projected = ProjectedRecord::try_new(
        request.conversation_id,
        record.delivery_seq(),
        ParticipantRecord::OrdinaryRecord {
            sender_participant_id: members.first.participant_id(),
            payload: request.payload.clone(),
        },
        vec![members.second.participant_id()],
        Some(members.first.participant_id()),
    )?;
    append_fixture_outbox_row(
        authority,
        outbox_log,
        OutboxRow::Produced(ProducedBatch::new(
            source_sequence,
            ProducedSourceKind::RecordAdmission,
            vec![projected],
        )),
    )
}

fn commit_fixture_ack(
    authority: &mut ConversationAuthority,
    operation_log: &OperationLog,
    outbox_log: &OutboxLog,
    config: &ParticipantConfig,
    conversation_id: u64,
    members: &FixtureMembers,
    through_seq: u64,
) -> Result<(), Box<dyn Error>> {
    let source_log_sequence = authority.next_log_sequence;
    let request = ParticipantAck {
        conversation_id,
        participant_id: members.second.participant_id(),
        capability_generation: Generation::ONE,
        through_seq,
    };
    let outcome = authority.apply_ack(
        &request,
        &marker_fixture_facts(members.second_connection, config)?,
        &FixtureAppender { log: operation_log },
    )?;
    if !matches!(outcome.value, ServerValue::AckCommitted(_)) {
        return Err(format!(
            "marker fixture ordinary ack did not commit: {:?}",
            outcome.value
        )
        .into());
    }
    append_fixture_outbox_row(
        authority,
        outbox_log,
        OutboxRow::AckAdvanced {
            source_log_sequence,
            participant_id: members.second.participant_id(),
            through_seq,
        },
    )
}

fn record_request(conversation_id: u64, participant_id: u64, token: u8) -> RecordAdmission {
    RecordAdmission {
        conversation_id,
        participant_id,
        capability_generation: Generation::ONE,
        record_admission_attempt_token: RecordAdmissionAttemptToken::new([token; 16]),
        payload: vec![token],
    }
}

fn drive_marker_drain(
    handler: &ProductionParticipantHandler,
    store: Arc<dyn DurableStore>,
    config: &ParticipantConfig,
    conversation_id: u64,
    members: &FixtureMembers,
) -> Result<
    (
        ConnectionIncarnation,
        ParticipantId,
        ParticipantDelivery,
        u64,
    ),
    Box<dyn Error>,
> {
    let operation_log = OperationLog::new(Arc::clone(&store), conversation_id);
    let outbox_log = OutboxLog::new(store, conversation_id);
    let cell = handler.cell(conversation_id)?;
    let mut owner = cell
        .lock()
        .map_err(|_| "marker fixture conversation owner lock was poisoned")?;
    let authority = owner
        .as_mut()
        .ok_or("marker fixture conversation owner was absent")?;

    for token in [0xA3, 0xA4, 0xA5] {
        let request = record_request(conversation_id, members.first.participant_id(), token);
        let (source, record) = commit_fixture_record(
            authority,
            &operation_log,
            config,
            members.first_connection,
            &request,
            1,
        )?;
        assert!(authority.last_marker_projection.take().is_none());
        project_fixture_ordinary(authority, &outbox_log, source, &record, &request, members)?;
        commit_fixture_ack(
            authority,
            &operation_log,
            &outbox_log,
            config,
            conversation_id,
            members,
            record.delivery_seq(),
        )?;
    }

    let request = record_request(conversation_id, members.first.participant_id(), 0xA8);
    let (marker_source, record) = commit_fixture_record(
        authority,
        &operation_log,
        config,
        members.first_connection,
        &request,
        2,
    )?;
    let marker = authority
        .last_marker_projection
        .take()
        .ok_or("drain admission did not surrender a marker projection")?;
    let target = match marker.record {
        ParticipantRecord::HistoryCompacted {
            affected_participant_id,
            ..
        } => affected_participant_id,
        ref other => return Err(format!("marker projection was not a marker: {other:?}").into()),
    };
    let target_connection = if target == members.first.participant_id() {
        members.first_connection
    } else if target == members.second.participant_id() {
        members.second_connection
    } else {
        return Err("marker targeted an unknown participant".into());
    };

    let marker_record = ProjectedRecord::try_new(
        conversation_id,
        marker.delivery_seq,
        marker.record.clone(),
        vec![
            members.first.participant_id(),
            members.second.participant_id(),
        ],
        None,
    )?;
    append_fixture_outbox_row(
        authority,
        &outbox_log,
        OutboxRow::Produced(ProducedBatch::new(
            marker_source,
            ProducedSourceKind::MarkerDrained,
            vec![marker_record],
        )),
    )?;
    project_fixture_ordinary(
        authority,
        &outbox_log,
        marker_source + 1,
        &record,
        &request,
        members,
    )?;
    drop(owner);
    Ok((target_connection, target, marker, record.delivery_seq()))
}

pub(super) fn prepare_marker_fixture() -> Result<MarkerFixture, Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let config = marker_fixture_config();
    let conversation_id = 0xA7;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), config)?;
    let members = enroll_members(&handler, conversation_id)?;
    ack_marker_prefix(&handler, conversation_id, &members)?;
    let (target_connection, target_participant, marker_delivery, catchup_through_seq) =
        drive_marker_drain(
            &handler,
            Arc::clone(&store),
            &config,
            conversation_id,
            &members,
        )?;
    Ok(MarkerFixture {
        handler,
        store,
        target_connection,
        target_participant,
        record_connection: members.first_connection,
        record_participant: members.first.participant_id(),
        catchup_connection: members.second_connection,
        catchup_participant: members.second.participant_id(),
        catchup_through_seq,
        marker_delivery,
    })
}

pub(super) fn marker_protocol_snapshot(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    participant_id: ParticipantId,
) -> Result<(u64, String), Box<dyn Error>> {
    let cell = handler.cell(conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "marker snapshot owner lock was poisoned")?;
    let authority = owner.as_ref().ok_or("marker snapshot owner was absent")?;
    let cursor = authority
        .slots
        .get(&participant_id)
        .ok_or("marker snapshot participant was absent")?
        .member
        .cursor();
    let frontier = format!("{:?}", authority.frontier);
    drop(owner);
    Ok((cursor, frontier))
}
