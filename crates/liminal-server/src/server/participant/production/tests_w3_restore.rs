//! Eleven-oracle W3 acceptance additions (items three through eleven).

use std::error::Error;
use std::sync::Arc;

use liminal::durability::DurableStore;
use liminal::durability::bridge::block_on;
use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, InvalidObserverEpoch, ObserverRecoveryHandshake,
    ObserverRefusal, ParticipantDelivery, ParticipantRecord, ServerValue,
};

use crate::server::participant::{ParticipantOfferedProgress, ParticipantSemanticHandler};

use super::ProductionParticipantHandler;
use super::log::OperationLog;
use super::outbox::{ConversationOutbox, ConversationOutboxLimits};
use super::outbox_log::{
    OUTBOX_SCHEMA_VERSION, OUTBOX_STREAM_PREFIX, OutboxLog, OutboxLogError, OutboxRow,
    ProducedBatch, ProducedSourceKind, ProjectedRecord, RestorePageAccountingGuard,
    UNIT2_OUTBOX_RESTORE_BATCH_ROWS, encode_row,
};
use super::outbox_replay::{RestoreError, RestoreError::Extension};
use super::state::ConversationAuthority;
use super::tests::{dispatch, test_participant_config};
use super::tests_marker_ack::{commit_exact_marker_ack, record_exact_marker_offer};
use super::tests_marker_ack_fixture::{
    marker_fixture_config, marker_protocol_snapshot, prepare_marker_fixture,
};
use super::tests_w3_restore_fixture::{
    CONVERSATION, CursorFaultArm, CutPageStore, OutboxAppendFaultStore, PhaseAwareStore,
    ShortPageStore, append_payload, duplicate_extension_to, enrollment, extension_key, new_store,
    operation_key, seed_enrollment, stream_payloads,
};

fn more_than_two_pages() -> Result<usize, Box<dyn Error>> {
    UNIT2_OUTBOX_RESTORE_BATCH_ROWS
        .checked_mul(2)
        .and_then(|rows| rows.checked_add(1))
        .ok_or_else(|| "W3 fixture row count overflowed".into())
}

fn clear_owner(handler: &ProductionParticipantHandler) -> Result<(), Box<dyn Error>> {
    let cell = handler.cell(CONVERSATION)?;
    let mut owner = cell.lock().map_err(|_| "W3 owner lock poisoned")?;
    *owner = None;
    drop(owner);
    Ok(())
}

fn owner_is_present(handler: &ProductionParticipantHandler) -> Result<bool, Box<dyn Error>> {
    let cell = handler.cell(CONVERSATION)?;
    let owner = cell.lock().map_err(|_| "W3 owner lock poisoned")?;
    let present = owner.is_some();
    drop(owner);
    Ok(present)
}

fn restore_limits() -> Result<ConversationOutboxLimits, Box<dyn Error>> {
    let config = test_participant_config();
    Ok(ConversationOutboxLimits::try_new(
        config.max_retained_record_rows,
        config.identity_slots,
    )?)
}

fn ordered_pushes(
    handler: &ProductionParticipantHandler,
    connection: ConnectionIncarnation,
    conversation_id: u64,
) -> Result<Vec<ParticipantDelivery>, Box<dyn Error>> {
    let mut offered = None;
    let mut pushes = Vec::new();
    let limit = usize::try_from(marker_fixture_config().max_retained_record_rows)?;
    for _ in 0..limit {
        let Some(publication) = handler.next_publication(connection, conversation_id, offered)?
        else {
            break;
        };
        offered = Some(ParticipantOfferedProgress {
            binding_epoch: publication.binding_epoch,
            through_seq: publication.delivery_seq(),
        });
        pushes.push(publication.delivery);
    }
    Ok(pushes)
}

#[test]
fn outbox_restore_peak_decoded_rows_never_exceeds_one_page() -> Result<(), Box<dyn Error>> {
    let store = new_store()?;
    let handler = seed_enrollment(Arc::clone(&store))?;
    drop(handler);
    let row_count = more_than_two_pages()?;
    duplicate_extension_to(&store, row_count)?;

    let accounting = RestorePageAccountingGuard::start();
    let restored =
        ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config())?;
    let snapshot = accounting.snapshot();
    assert!(snapshot.validation_pages > 2);
    assert!(snapshot.application_pages > 2);
    assert!(snapshot.validation_peak_rows <= UNIT2_OUTBOX_RESTORE_BATCH_ROWS);
    assert!(snapshot.application_peak_rows <= UNIT2_OUTBOX_RESTORE_BATCH_ROWS);
    assert_eq!(snapshot.validation_current_rows, 0);
    assert_eq!(snapshot.application_current_rows, 0);
    assert!(!snapshot.cursor_overlap_observed);
    assert!(owner_is_present(&restored)?);
    assert_eq!(stream_payloads(&store, &extension_key())?.len(), row_count);
    Ok(())
}

#[test]
fn midstream_outbox_decode_failure_preserves_typed_error_and_publishes_no_state()
-> Result<(), Box<dyn Error>> {
    let store = new_store()?;
    let handler = seed_enrollment(Arc::clone(&store))?;
    duplicate_extension_to(&store, UNIT2_OUTBOX_RESTORE_BATCH_ROWS)?;
    append_payload(
        &store,
        &extension_key(),
        vec![OUTBOX_SCHEMA_VERSION],
        u64::try_from(UNIT2_OUTBOX_RESTORE_BATCH_ROWS)?,
    )?;
    let before = stream_payloads(&store, &extension_key())?;
    clear_owner(&handler)?;

    let log = OutboxLog::new(Arc::clone(&store), CONVERSATION);
    let Err(typed) = block_on(log.restore_cursor().validate_all())? else {
        return Err("validation unexpectedly accepted a malformed suffix".into());
    };
    assert!(matches!(
        typed,
        OutboxLogError::UnexpectedEnd { field: "row_kind" }
    ));
    let operation_log = OperationLog::new(Arc::clone(&store), CONVERSATION);
    let error = handler
        .replay_and_repair(CONVERSATION, &operation_log)
        .err()
        .ok_or("production restore unexpectedly accepted a malformed suffix")?;
    let message = error.to_string();
    assert!(message.contains(
        "participant Unit 2 extension log failed: Unit 2 extension row ended before row_kind"
    ));
    assert!(!message.contains("participant production operation failed"));
    assert!(!owner_is_present(&handler)?);
    assert_eq!(stream_payloads(&store, &extension_key())?, before);
    Ok(())
}

#[test]
fn restore_retained_authority_counts_are_measured_and_ledgered() -> Result<(), Box<dyn Error>> {
    let participant_id = 7_u64;
    let pair_count = UNIT2_OUTBOX_RESTORE_BATCH_ROWS
        .checked_add(1)
        .ok_or("retained fixture pair count overflowed")?;
    let mut rows = Vec::with_capacity(
        pair_count
            .checked_mul(2)
            .ok_or("retained fixture row capacity overflowed")?,
    );
    let mut physical_sequence = 0_u64;
    for index in 0..pair_count {
        let index = u64::try_from(index)?;
        let delivery_seq = index.checked_add(1).ok_or("delivery sequence overflowed")?;
        let produced_source = index.checked_mul(2).ok_or("source sequence overflowed")?;
        let ack_source = produced_source
            .checked_add(1)
            .ok_or("ack source overflowed")?;
        let record = ProjectedRecord::try_new(
            CONVERSATION,
            delivery_seq,
            ParticipantRecord::OrdinaryRecord {
                sender_participant_id: participant_id
                    .checked_add(1)
                    .ok_or("sender participant overflowed")?,
                payload: index.to_be_bytes().to_vec(),
            },
            vec![participant_id],
            participant_id.checked_add(1),
        )?;
        rows.push((
            physical_sequence,
            OutboxRow::Produced(ProducedBatch::new(
                produced_source,
                ProducedSourceKind::RecordAdmission,
                vec![record],
            )),
        ));
        physical_sequence = physical_sequence
            .checked_add(1)
            .ok_or("physical overflowed")?;
        rows.push((
            physical_sequence,
            OutboxRow::AckAdvanced {
                source_log_sequence: ack_source,
                participant_id,
                through_seq: delivery_seq,
            },
        ));
        physical_sequence = physical_sequence
            .checked_add(1)
            .ok_or("physical overflowed")?;
    }
    assert!(
        rows.len()
            > more_than_two_pages()?
                .checked_sub(1)
                .ok_or("page underflow")?
    );
    let owner = ConversationOutbox::restore(CONVERSATION, rows, restore_limits()?)?;
    assert_eq!(owner.live_record_count(), 0);
    assert_eq!(owner.live_recipient_obligation_count(), 0);
    assert_eq!(owner.charged_bytes(), 0);

    // "Retained owned bytes" means logical deep-owned element bytes: map keys,
    // tuple scalars, set elements, and canonical Vec payload lengths. It excludes
    // allocator/node overhead and spare capacity so W7 reruns are comparable.
    let measured = owner.retained_authority_measurements()?;
    assert_eq!(measured.source_batch_count, pair_count);
    assert_eq!(measured.ack_source_count, pair_count);
    assert_eq!(measured.obligation_participant_count, 1);
    assert_eq!(measured.obligation_sequence_count, pair_count);
    assert!(measured.source_batch_owned_bytes > 0);
    assert!(measured.ack_source_owned_bytes > 0);
    assert!(measured.all_obligations_owned_bytes > 0);
    eprintln!(
        "{{\"w7_retained\":{{\"source_batches\":{{\"count\":{},\"owned_bytes\":{}}},\"ack_sources\":{{\"count\":{},\"owned_bytes\":{}}},\"all_obligations\":{{\"participants\":{},\"sequences\":{},\"owned_bytes\":{}}}}}}}",
        measured.source_batch_count,
        measured.source_batch_owned_bytes,
        measured.ack_source_count,
        measured.ack_source_owned_bytes,
        measured.obligation_participant_count,
        measured.obligation_sequence_count,
        measured.all_obligations_owned_bytes,
    );
    Ok(())
}

#[test]
fn observer_recovery_restores_absent_owner_page_wise_with_exact_errors()
-> Result<(), Box<dyn Error>> {
    let valid_store = new_store()?;
    let valid = seed_enrollment(Arc::clone(&valid_store))?;
    duplicate_extension_to(&valid_store, more_than_two_pages()?)?;
    clear_owner(&valid)?;
    let accounting = RestorePageAccountingGuard::start();
    let value = dispatch(
        &valid,
        ConnectionIncarnation::new(CONVERSATION, 2),
        ClientRequest::ObserverRecovery(ObserverRecoveryHandshake {
            observer_refusals: vec![ObserverRefusal {
                conversation_id: CONVERSATION,
                refused_epoch: 1,
            }],
        }),
    )?;
    assert!(matches!(
        value,
        ServerValue::InvalidObserverEpoch(InvalidObserverEpoch::EpochAhead { .. })
    ));
    assert!(owner_is_present(&valid)?);
    let snapshot = accounting.snapshot();
    assert!(snapshot.validation_pages > 2 && snapshot.application_pages > 2);
    assert!(!snapshot.cursor_overlap_observed);
    drop(accounting);

    let malformed_store = new_store()?;
    let malformed = seed_enrollment(Arc::clone(&malformed_store))?;
    duplicate_extension_to(&malformed_store, UNIT2_OUTBOX_RESTORE_BATCH_ROWS)?;
    append_payload(
        &malformed_store,
        &extension_key(),
        vec![OUTBOX_SCHEMA_VERSION],
        u64::try_from(UNIT2_OUTBOX_RESTORE_BATCH_ROWS)?,
    )?;
    clear_owner(&malformed)?;
    let before = stream_payloads(&malformed_store, &extension_key())?;
    let error = dispatch(
        &malformed,
        ConnectionIncarnation::new(CONVERSATION, 3),
        ClientRequest::ObserverRecovery(ObserverRecoveryHandshake {
            observer_refusals: vec![ObserverRefusal {
                conversation_id: CONVERSATION,
                refused_epoch: 1,
            }],
        }),
    )
    .err()
    .ok_or("malformed observer recovery unexpectedly classified")?;
    let message = error.to_string();
    assert!(message.contains(
        "participant Unit 2 extension log failed: Unit 2 extension row ended before row_kind"
    ));
    assert!(!message.contains("participant production operation failed"));
    assert!(!owner_is_present(&malformed)?);
    assert_eq!(stream_payloads(&malformed_store, &extension_key())?, before);
    Ok(())
}

#[test]
fn later_page_decode_failure_takes_precedence_over_earlier_semantic_conflict()
-> Result<(), Box<dyn Error>> {
    let store = new_store()?;
    let handler = seed_enrollment(Arc::clone(&store))?;
    let valid_payload = stream_payloads(&store, &extension_key())?
        .first()
        .cloned()
        .ok_or("seed extension missing")?;
    let conflict = encode_row(&OutboxRow::AckAdvanced {
        source_log_sequence: 1,
        participant_id: 0,
        through_seq: 1,
    })?;
    append_payload(&store, &extension_key(), conflict, 1)?;
    let mut sequence = 2_u64;
    let page = u64::try_from(UNIT2_OUTBOX_RESTORE_BATCH_ROWS)?;
    while sequence < page {
        append_payload(&store, &extension_key(), valid_payload.clone(), sequence)?;
        sequence = sequence
            .checked_add(1)
            .ok_or("dual-fault sequence overflowed")?;
    }
    append_payload(&store, &extension_key(), vec![OUTBOX_SCHEMA_VERSION], page)?;
    clear_owner(&handler)?;
    let log = OperationLog::new(Arc::clone(&store), CONVERSATION);
    let error = handler
        .replay_and_repair(CONVERSATION, &log)
        .err()
        .ok_or("dual-fault restore unexpectedly succeeded")?;
    let message = error.to_string();
    assert!(message.contains(
        "participant Unit 2 extension log failed: Unit 2 extension row ended before row_kind"
    ));
    assert!(!message.contains("projection at physical sequence"));
    assert!(!message.contains("participant production operation failed"));
    assert!(!owner_is_present(&handler)?);
    Ok(())
}

#[test]
fn same_base_head_group_straddling_page_cut_replays_without_repair() -> Result<(), Box<dyn Error>> {
    let fixture = prepare_marker_fixture()?;
    let conversation_id = fixture.marker_delivery.conversation_id;
    let extension_key = format!("{OUTBOX_STREAM_PREFIX}{conversation_id}");
    let outbox_log = OutboxLog::new(Arc::clone(&fixture.store), conversation_id);
    let rows_before_group = block_on(outbox_log.read_all())??;
    let (_, expected_projection) = rows_before_group
        .last()
        .cloned()
        .ok_or("marker fixture extension was empty")?;
    if !matches!(expected_projection, OutboxRow::Produced(_)) {
        return Err("marker group did not start with its exact Produced row".into());
    }
    let group_head = expected_projection
        .base_log_head()
        .ok_or("marker group Produced boundary overflowed")?;
    let group_start = rows_before_group
        .len()
        .checked_sub(1)
        .ok_or("marker group start underflowed")?;

    // Extend the one same-head group beyond a page with exact idempotent copies
    // of its Produced projection. The two MarkerAck commits below keep this
    // unchanged base head and receive distinct physical extension sequences.
    {
        let cell = fixture.handler.cell(conversation_id)?;
        let mut owner = cell
            .lock()
            .map_err(|_| "marker group owner lock poisoned")?;
        let authority = owner.as_mut().ok_or("marker group owner was absent")?;
        let outbox = authority
            .outbox
            .as_mut()
            .ok_or("marker group outbox was absent")?;
        for _ in 0..UNIT2_OUTBOX_RESTORE_BATCH_ROWS {
            let physical_sequence = outbox.next_extension_sequence();
            block_on(outbox_log.append(&expected_projection, physical_sequence))??;
            outbox.apply_row(physical_sequence, expected_projection.clone())?;
        }
        drop(owner);
    }

    record_exact_marker_offer(&fixture)?;
    let stored_marker = commit_exact_marker_ack(&fixture)?;
    assert_eq!(stored_marker.base_log_head, group_head);

    let rows = block_on(outbox_log.read_all())??;
    let Some((physical, OutboxRow::MarkerAckCommitted(marker))) = rows.last() else {
        return Err("same-head group did not end in MarkerAckCommitted".into());
    };
    assert_eq!(marker.base_log_head, group_head);
    assert_eq!(marker.extension_sequence, *physical);

    let before = stream_payloads(&fixture.store, &extension_key)?;
    let live_target = marker_protocol_snapshot(
        &fixture.handler,
        conversation_id,
        fixture.record_participant,
    )?;
    let live_catchup = marker_protocol_snapshot(
        &fixture.handler,
        conversation_id,
        fixture.catchup_participant,
    )?;
    let live_target_pushes =
        ordered_pushes(&fixture.handler, fixture.record_connection, conversation_id)?;
    let live_catchup_pushes = ordered_pushes(
        &fixture.handler,
        fixture.catchup_connection,
        conversation_id,
    )?;

    for cut in 1..UNIT2_OUTBOX_RESTORE_BATCH_ROWS {
        let cut_store: Arc<dyn DurableStore> = Arc::new(CutPageStore::new(
            Arc::clone(&fixture.store),
            u64::try_from(group_start)?,
            cut,
        ));
        let restored = ProductionParticipantHandler::new(cut_store, marker_fixture_config())?;
        assert_eq!(
            marker_protocol_snapshot(&restored, conversation_id, fixture.record_participant)?,
            live_target
        );
        assert_eq!(
            marker_protocol_snapshot(&restored, conversation_id, fixture.catchup_participant)?,
            live_catchup
        );
        assert_eq!(
            ordered_pushes(&restored, fixture.record_connection, conversation_id)?,
            live_target_pushes
        );
        assert_eq!(
            ordered_pushes(&restored, fixture.catchup_connection, conversation_id)?,
            live_catchup_pushes
        );
        assert!(
            restored
                .observer
                .lock()
                .map_err(|_| "observer owner lock poisoned")?
                .is_some()
        );
        assert_eq!(stream_payloads(&fixture.store, &extension_key)?, before);
    }
    Ok(())
}

#[test]
fn short_nonempty_pages_with_remaining_rows_do_not_terminate_restore() -> Result<(), Box<dyn Error>>
{
    let inner = new_store()?;
    let seeded = seed_enrollment(Arc::clone(&inner))?;
    drop(seeded);
    let row_count = more_than_two_pages()?;
    duplicate_extension_to(&inner, row_count)?;
    let expected_bytes = stream_payloads(&inner, &extension_key())?;

    let short = Arc::new(ShortPageStore::new(Arc::clone(&inner)));
    let short_store: Arc<dyn DurableStore> = short.clone();
    let restored = ProductionParticipantHandler::new(short_store, test_participant_config())?;
    assert!(owner_is_present(&restored)?);
    assert_eq!(short.empty_reads()?, (1, 1));
    assert_eq!(stream_payloads(&inner, &extension_key())?, expected_bytes);
    drop(restored);

    let production_shape =
        ProductionParticipantHandler::new(Arc::clone(&inner), test_participant_config())?;
    assert!(owner_is_present(&production_shape)?);
    assert_eq!(stream_payloads(&inner, &extension_key())?, expected_bytes);
    Ok(())
}

fn repair_then_later_failure(
    aggregate_reference: bool,
) -> Result<(String, Vec<Vec<u8>>, bool), Box<dyn Error>> {
    let inner = new_store()?;
    let faults = Arc::new(OutboxAppendFaultStore::new(Arc::clone(&inner)));
    let store: Arc<dyn DurableStore> = faults.clone();
    let handler = seed_enrollment(Arc::clone(&store))?;
    faults.set_fail(true);
    assert!(
        dispatch(
            &handler,
            ConnectionIncarnation::new(CONVERSATION, 2),
            enrollment(2),
        )
        .is_err()
    );
    faults.set_fail(false);
    assert!(!owner_is_present(&handler)?);
    let base = stream_payloads(&store, &operation_key())?;
    let later_failure = base.first().cloned().ok_or("base stream missing genesis")?;
    append_payload(
        &store,
        &operation_key(),
        later_failure,
        u64::try_from(base.len())?,
    )?;
    let before = stream_payloads(&store, &extension_key())?;
    assert_eq!(before.len(), 1);
    let log = OperationLog::new(Arc::clone(&store), CONVERSATION);
    let result = if aggregate_reference {
        handler.replay_aggregate_reference(CONVERSATION, &log)
    } else {
        handler.replay_and_repair(CONVERSATION, &log)
    };
    let error = result
        .err()
        .ok_or("later invalid base unexpectedly replayed")?;
    let after = stream_payloads(&store, &extension_key())?;
    Ok((error.to_string(), after, owner_is_present(&handler)?))
}

#[test]
fn missing_tail_repair_persists_before_later_base_failure_byte_identically()
-> Result<(), Box<dyn Error>> {
    let reference = repair_then_later_failure(true)?;
    let w3 = repair_then_later_failure(false)?;
    assert_eq!(w3.0, reference.0);
    assert_eq!(w3.1, reference.1);
    assert_eq!(w3.1.len(), 2);
    assert!(!reference.2 && !w3.2);
    assert!(w3.0.contains("participant production operation failed"));
    Ok(())
}

fn direct_w3_error(store: Arc<dyn DurableStore>) -> Result<RestoreError, Box<dyn Error>> {
    let outbox_log = OutboxLog::new(Arc::clone(&store), CONVERSATION);
    block_on(outbox_log.restore_cursor().validate_all())??;
    let operation_log = OperationLog::new(store, CONVERSATION);
    block_on(ConversationAuthority::replay(
        CONVERSATION,
        &operation_log,
        &outbox_log,
        &test_participant_config(),
        restore_limits()?,
    ))?
    .err()
    .ok_or_else(|| "armed direct W3 restore unexpectedly succeeded".into())
}

#[test]
fn pass_two_cursor_failure_maps_through_extension_log_error_and_drops_staged_state()
-> Result<(), Box<dyn Error>> {
    let inner = new_store()?;
    let seeded = seed_enrollment(Arc::clone(&inner))?;
    drop(seeded);
    let phase = Arc::new(PhaseAwareStore::new(
        Arc::clone(&inner),
        CursorFaultArm::PassTwoNonEof,
    ));
    let store: Arc<dyn DurableStore> = phase.clone();
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config())?;
    let before = stream_payloads(&inner, &extension_key())?;
    let log = OperationLog::new(Arc::clone(&store), CONVERSATION);

    for arm in [
        CursorFaultArm::PassTwoNonEof,
        CursorFaultArm::PassTwoEmptyEof,
    ] {
        phase.reset_reference(arm)?;
        assert!(
            handler
                .replay_aggregate_reference(CONVERSATION, &log)
                .is_ok()
        );

        phase.reset_w3(arm)?;
        let typed = direct_w3_error(Arc::clone(&store))?;
        assert!(matches!(typed, Extension(OutboxLogError::Durability(_))));

        clear_owner(&handler)?;
        phase.reset_w3(arm)?;
        let error = handler
            .replay_and_repair(CONVERSATION, &log)
            .err()
            .ok_or("armed production W3 restore unexpectedly succeeded")?;
        let message = error.to_string();
        assert!(message.contains("participant Unit 2 extension log failed:"));
        assert!(!message.contains("participant production operation failed"));
        assert!(!owner_is_present(&handler)?);
        assert_eq!(stream_payloads(&inner, &extension_key())?, before);
    }
    Ok(())
}
