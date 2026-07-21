//! Full-server crash-cut acceptance for durable Leave discharge reconciliation.

use std::error::Error;
use std::path::Path;
use std::sync::Arc;

use liminal::durability::DurableStore;
use liminal::durability::bridge::block_on;
use liminal_protocol::wire::{
    ClientRequest, EnrollBound, Generation, LeaveAttemptToken, LeaveRequest, RecordAdmission,
    RecordAdmissionAttemptToken, ServerValue,
};

use super::e2e_cold_all_shapes_fixture::{
    BoundClosePath, ColdMember, TypedFateSource, ack_through, decoded_history,
    decoded_history_from_store, expected_bound_close_fate, semantic_rows_and_typed_fate_suffix,
};
use super::e2e_leave_regression::{CONVERSATION, enroll_three};
use super::e2e_tests::{OutboxOwnerFacts, SocketFixture, SocketPeer};
use super::log::{READ_BATCH_SIZE, STREAM_PREFIX, StoredOperation};
use super::outbox_log::{
    OUTBOX_STREAM_PREFIX, OutboxRow, ProducedSourceKind, UNIT2_OUTBOX_RESTORE_BATCH_ROWS,
};
use super::tests::{open_disk_store_for_tests, test_participant_config};
use super::tests_outbox_barrier_fixture::OutboxBarrierKind;
use super::tests_outbox_log::measured_fixed_outbox_overhead;

#[derive(Clone, Debug, PartialEq, Eq)]
struct StreamBytes {
    row_count: usize,
    head: u64,
    payloads: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DurableHistoryBytes {
    base: StreamBytes,
    extension: StreamBytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LeaveCrashCut {
    AfterBothFlushes,
    BetweenBaseFlushAndExtensionAppend,
}

#[derive(Debug)]
struct LeaveCrashCutEvidence {
    crash: DurableHistoryBytes,
    restored: DurableHistoryBytes,
    restored_owner: OutboxOwnerFacts,
}

struct RuledTeardownEvidence {
    durable: DurableHistoryBytes,
    live_owner: Option<OutboxOwnerFacts>,
    expected_fate_suffix: [TypedFateSource; 2],
}

fn stream_bytes(
    store: &Arc<dyn DurableStore>,
    stream_key: &str,
    page_rows: usize,
) -> Result<StreamBytes, Box<dyn Error>> {
    let mut payloads = Vec::new();
    let mut head = 0_u64;
    loop {
        let entries = block_on(store.read_from(stream_key, head, page_rows))??;
        let page_len = entries.len();
        if page_len == 0 {
            break;
        }
        for entry in entries {
            assert_eq!(
                entry.sequence, head,
                "item 30 durable stream was not physically contiguous"
            );
            payloads.push(entry.payload);
            head = head
                .checked_add(1)
                .ok_or("item 30 durable stream head overflowed")?;
        }
        if page_len < page_rows {
            break;
        }
    }
    let row_count = payloads.len();
    assert_eq!(u64::try_from(row_count)?, head);
    Ok(StreamBytes {
        row_count,
        head,
        payloads,
    })
}

fn durable_history_bytes_from_store(
    store: &Arc<dyn DurableStore>,
) -> Result<DurableHistoryBytes, Box<dyn Error>> {
    Ok(DurableHistoryBytes {
        base: stream_bytes(
            store,
            &format!("{STREAM_PREFIX}{CONVERSATION}"),
            READ_BATCH_SIZE,
        )?,
        extension: stream_bytes(
            store,
            &format!("{OUTBOX_STREAM_PREFIX}{CONVERSATION}"),
            UNIT2_OUTBOX_RESTORE_BATCH_ROWS,
        )?,
    })
}

fn durable_history_bytes(data_dir: &Path) -> Result<DurableHistoryBytes, Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = open_disk_store_for_tests(data_dir)?;
    durable_history_bytes_from_store(&store)
}

fn assert_left_source_and_audit_rows(
    data_dir: &Path,
    expected_fate_suffix: &[super::e2e_cold_all_shapes_fixture::TypedFateSource],
    expected_extension_batches: usize,
) -> Result<(), Box<dyn Error>> {
    let (base, extension) = decoded_history(data_dir, CONVERSATION)?;
    assert_left_source_and_audit_history(
        base,
        &extension,
        expected_fate_suffix,
        expected_extension_batches,
    )
}

fn assert_left_source_and_audit_history(
    base: Vec<(u64, StoredOperation)>,
    extension: &[(u64, OutboxRow)],
    expected_fate_suffix: &[super::e2e_cold_all_shapes_fixture::TypedFateSource],
    expected_extension_batches: usize,
) -> Result<(), Box<dyn Error>> {
    let (base, fate_suffix) = semantic_rows_and_typed_fate_suffix(base)?;
    assert_eq!(fate_suffix, expected_fate_suffix);
    let (left_source_sequence, _) = base
        .iter()
        .rev()
        .find(|(_, row)| matches!(row, StoredOperation::Left { .. }))
        .ok_or("item 30 crash cut omitted the durable v2 Left source row")?;
    assert!(matches!(
        base.last(),
        Some((sequence, StoredOperation::Left { .. })) if sequence == left_source_sequence
    ));
    let matching_batches = extension
        .iter()
        .filter(|(_, row)| {
            matches!(
                row,
                OutboxRow::Produced(batch)
                    if batch.source_log_sequence() == *left_source_sequence
                        && batch.source_kind() == ProducedSourceKind::Left
            )
        })
        .count();
    assert_eq!(
        matching_batches, expected_extension_batches,
        "item 30 Left source/audit census drifted"
    );
    Ok(())
}

fn restore_and_assert_idempotency(
    data_dir: &Path,
    cut: LeaveCrashCut,
    expected_fate_suffix: &[super::e2e_cold_all_shapes_fixture::TypedFateSource],
    crash: &DurableHistoryBytes,
    live_after_flushes: Option<OutboxOwnerFacts>,
    leaver_id: u64,
    signed_outbox_bound: u64,
) -> Result<(DurableHistoryBytes, OutboxOwnerFacts), Box<dyn Error>> {
    // SocketFixture constructs ProductionParticipantHandler before installing
    // the service, supervisor, or first socket. Thus this returned fixture proves
    // reconciliation completed before authority could be published to transport.
    let config = test_participant_config();
    let first_restore = SocketFixture::start_replay_gated_with_config(data_dir, config)?;
    let first_owner = first_restore.outbox_owner_facts(CONVERSATION, leaver_id)?;
    assert_eq!(first_owner.next_live_obligation, None);
    assert!(first_owner.charged_bytes <= signed_outbox_bound);
    if let Some(live) = live_after_flushes {
        assert_eq!(first_owner, live);
    }
    first_restore.stop();
    let restored = durable_history_bytes(data_dir)?;
    assert_left_source_and_audit_rows(data_dir, expected_fate_suffix, 1)?;

    match cut {
        LeaveCrashCut::AfterBothFlushes | LeaveCrashCut::BetweenBaseFlushAndExtensionAppend => {
            assert_eq!(&restored, crash);
        }
    }

    let second_restore = SocketFixture::start_replay_gated_with_config(data_dir, config)?;
    let second_owner = second_restore.outbox_owner_facts(CONVERSATION, leaver_id)?;
    assert_eq!(second_owner, first_owner);
    assert_eq!(second_owner.next_live_obligation, None);
    assert!(second_owner.charged_bytes <= signed_outbox_bound);
    second_restore.stop();
    let restored_again = durable_history_bytes(data_dir)?;

    // R5's concrete idempotency oracle: a second restore changes neither the
    // extension row count nor its optimistic head, and no source/audit byte moves.
    assert_eq!(
        restored_again.extension.row_count,
        restored.extension.row_count
    );
    assert_eq!(restored_again.extension.head, restored.extension.head);
    assert_eq!(
        restored_again.extension.payloads,
        restored.extension.payloads
    );
    assert_eq!(restored_again.base, restored.base);
    Ok((restored, first_owner))
}

fn arm_leave_crash_cut(server: &SocketFixture, cut: LeaveCrashCut) -> Result<(), Box<dyn Error>> {
    match cut {
        LeaveCrashCut::AfterBothFlushes => server.arm_outbox_barriers([
            OutboxBarrierKind::OperationFlush,
            OutboxBarrierKind::OutboxFlush,
        ]),
        LeaveCrashCut::BetweenBaseFlushAndExtensionAppend => {
            server.arm_outbox_barriers([OutboxBarrierKind::OperationFlush])?;
            server.fail_next_outbox_append()
        }
    }
}

fn observe_live_leave_cut(
    server: &SocketFixture,
    cut: LeaveCrashCut,
    leave_result: &Result<ServerValue, String>,
    leaver_id: u64,
    signed_outbox_bound: u64,
) -> Result<Option<OutboxOwnerFacts>, Box<dyn Error>> {
    match cut {
        LeaveCrashCut::AfterBothFlushes => {
            assert!(matches!(leave_result, Ok(ServerValue::LeaveCommitted(_))));
            let facts = server.outbox_owner_facts(CONVERSATION, leaver_id)?;
            assert_eq!(facts.next_live_obligation, None);
            assert!(facts.charged_bytes <= signed_outbox_bound);
            Ok(Some(facts))
        }
        LeaveCrashCut::BetweenBaseFlushAndExtensionAppend => {
            assert!(
                leave_result.is_err(),
                "item 30 barrier-2 append fault fabricated a terminal response"
            );
            Ok(None)
        }
    }
}

fn capture_semantic_cut(
    server: &SocketFixture,
    cut: LeaveCrashCut,
) -> Result<DurableHistoryBytes, Box<dyn Error>> {
    let store = server.durable_store();
    let crash = durable_history_bytes_from_store(&store)?;
    let (cut_base, cut_extension) = decoded_history_from_store(store, CONVERSATION)?;
    let expected_pre_teardown_fate_suffix = Vec::new();
    assert_left_source_and_audit_history(
        cut_base,
        &cut_extension,
        &expected_pre_teardown_fate_suffix,
        usize::from(cut == LeaveCrashCut::AfterBothFlushes),
    )?;
    Ok(crash)
}

fn perform_ruled_teardown(
    server: SocketFixture,
    observer_socket: SocketPeer,
    sender: &EnrollBound,
    observer: &EnrollBound,
    leaver_id: u64,
    capture_live_owner: bool,
    data_dir: &Path,
) -> Result<RuledTeardownEvidence, Box<dyn Error>> {
    let expected_fate_suffix = [
        expected_bound_close_fate(
            observer.participant_id(),
            observer.origin_binding_epoch(),
            BoundClosePath::DroppedSocket,
        ),
        expected_bound_close_fate(
            sender.participant_id(),
            sender.origin_binding_epoch(),
            BoundClosePath::ServerStop,
        ),
    ];
    server.arm_outbox_barriers([OutboxBarrierKind::OperationFlush])?;
    observer_socket.shutdown_transport()?;
    server.wait_for_outbox_barrier(OutboxBarrierKind::OperationFlush)?;
    server.release_outbox_barrier(OutboxBarrierKind::OperationFlush)?;
    drop(observer_socket);
    server.force_close_and_wait();
    let live_owner = if capture_live_owner {
        Some(server.outbox_owner_facts(CONVERSATION, leaver_id)?)
    } else {
        None
    };
    server.stop();
    let teardown = durable_history_bytes(data_dir)?;
    assert_left_source_and_audit_rows(data_dir, &expected_fate_suffix, 1)?;
    Ok(RuledTeardownEvidence {
        durable: teardown,
        live_owner,
        expected_fate_suffix,
    })
}

fn run_leave_crash_cut(cut: LeaveCrashCut) -> Result<LeaveCrashCutEvidence, Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let config = test_participant_config();
    let (_, fixed_outbox_overhead) = measured_fixed_outbox_overhead(&config)?;
    let signed_outbox_bound = config
        .retained_capacity_bytes
        .checked_add(fixed_outbox_overhead)
        .ok_or("item 30 signed outbox bound overflowed")?;

    let mut server = SocketFixture::start_replay_gated_with_barriers(&data_dir, config)?;
    let mut observer_socket = server.spawn_peer()?;
    let mut leaver_socket = server.spawn_peer()?;
    let (sender, observer, leaver) =
        enroll_three(&mut server, &mut observer_socket, &mut leaver_socket)?;

    let sentinel_payload = vec![0, u8::MAX, 0xA5, 0];
    let record = server.request(ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: CONVERSATION,
        participant_id: sender.participant_id(),
        capability_generation: Generation::ONE,
        record_admission_attempt_token: RecordAdmissionAttemptToken::new([0x30; 16]),
        payload: sentinel_payload,
    }))?;
    let ServerValue::RecordCommitted(record) = record else {
        return Err(format!("item 30 sentinel record did not commit: {record:?}").into());
    };
    ack_through(
        &mut observer_socket,
        CONVERSATION,
        ColdMember::enrolled(&observer),
        record.delivery_seq(),
    )?;

    let before_leave = server.outbox_owner_facts(CONVERSATION, leaver.participant_id())?;
    assert_eq!(
        before_leave.next_live_obligation,
        Some(record.delivery_seq())
    );
    assert!(before_leave.charged_bytes <= signed_outbox_bound);

    arm_leave_crash_cut(&server, cut)?;
    let leave = LeaveRequest {
        conversation_id: CONVERSATION,
        participant_id: leaver.participant_id(),
        capability_generation: Generation::ONE,
        attach_secret: leaver.attach_secret(),
        leave_attempt_token: LeaveAttemptToken::new([0x31; 16]),
    };
    let leave_thread = std::thread::spawn(move || {
        leaver_socket
            .request(ClientRequest::Leave(leave))
            .map_err(|error| error.to_string())
    });

    server.wait_for_outbox_barrier(OutboxBarrierKind::OperationFlush)?;
    server.release_outbox_barrier(OutboxBarrierKind::OperationFlush)?;
    if cut == LeaveCrashCut::AfterBothFlushes {
        server.wait_for_outbox_barrier(OutboxBarrierKind::OutboxFlush)?;
        server.release_outbox_barrier(OutboxBarrierKind::OutboxFlush)?;
    }
    let leave_result = leave_thread
        .join()
        .map_err(|_| "item 30 Leave socket thread panicked")?;

    let live_after_flushes = observe_live_leave_cut(
        &server,
        cut,
        &leave_result,
        leaver.participant_id(),
        signed_outbox_bound,
    )?;

    // Preserve the original cut before mandatory first-touch repair of cut (b).
    let crash = capture_semantic_cut(&server, cut)?;

    // Derive §10.1's ordered suffix from observed Bound receipts and close paths.
    let capture_live_owner = live_after_flushes.is_some();
    let RuledTeardownEvidence {
        durable: teardown,
        live_owner: live_after_fate_suffix,
        expected_fate_suffix,
    } = perform_ruled_teardown(
        server,
        observer_socket,
        &sender,
        &observer,
        leaver.participant_id(),
        capture_live_owner,
        &data_dir,
    )?;

    let (restored, restored_owner) = restore_and_assert_idempotency(
        &data_dir,
        cut,
        &expected_fate_suffix,
        &teardown,
        live_after_fate_suffix,
        leaver.participant_id(),
        signed_outbox_bound,
    )?;

    Ok(LeaveCrashCutEvidence {
        crash,
        restored,
        restored_owner,
    })
}

#[test]
fn leave_discharge_replays_deterministically_across_the_commit_boundary()
-> Result<(), Box<dyn Error>> {
    let after_both = run_leave_crash_cut(LeaveCrashCut::AfterBothFlushes)?;
    let between = run_leave_crash_cut(LeaveCrashCut::BetweenBaseFlushAndExtensionAppend)?;

    assert_eq!(
        between.crash.extension.row_count + 1,
        after_both.crash.extension.row_count
    );
    assert_eq!(
        between.crash.extension.head + 1,
        after_both.crash.extension.head
    );
    assert_eq!(
        between.restored.extension, after_both.restored.extension,
        "item 30 reconciliation did not recreate cut (a)'s byte-identical extension history"
    );
    assert_eq!(between.restored_owner, after_both.restored_owner);
    assert_eq!(between.restored_owner.next_live_obligation, None);
    Ok(())
}
