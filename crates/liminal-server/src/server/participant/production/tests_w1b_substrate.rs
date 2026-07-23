//! W1b leg-1 durable schema, phase, and frozen-v2 migration oracles.

use std::error::Error;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal::durability::{DurableStore, open_ephemeral};
use serde::Serialize;

use super::log::*;
use super::ops_session_replay::validate_operation_schema;
use super::state::StateError;
use super::tests::test_participant_config;

const CONVERSATION: u64 = 0xF1_B1;
const V2_GENESIS: &[u8] = br#"{"schema_version":2,"operation":{"operation":"genesis","event":[]}}"#;

fn store() -> Result<Arc<dyn DurableStore>, Box<dyn Error>> {
    Ok(Arc::new(open_ephemeral(1)?))
}

fn append_payload(
    store: &Arc<dyn DurableStore>,
    sequence: u64,
    payload: &[u8],
) -> Result<(), Box<dyn Error>> {
    let stream_key = format!("{STREAM_PREFIX}{CONVERSATION}");
    assert_eq!(
        block_on(store.append(&stream_key, payload.to_vec(), sequence))??,
        sequence
    );
    block_on(store.flush())??;
    Ok(())
}

const fn epoch(seed: u64) -> StoredBindingEpoch {
    StoredBindingEpoch {
        server_incarnation: seed,
        connection_ordinal: seed + 1,
        capability_generation: seed + 2,
    }
}

const fn detach_request(marker: u8) -> StoredDetachRequest {
    StoredDetachRequest {
        conversation_id: CONVERSATION,
        participant_id: 7,
        capability_generation: 9,
        token: [marker; 16],
    }
}

fn died(cause: StoredDiedCause, disposition: StoredTerminalDisposition) -> StoredOperation {
    StoredOperation::Died {
        row: StoredDied {
            participant_id: 7,
            binding_epoch: epoch(10),
            cause,
            terminal_order: 21,
            disposition,
            connection_intent_sequence: Some(34),
            specific_fate_intent: Some(StoredSpecificFateIntent::Recovered {
                attached_source_sequence: 13,
                prior_binding_epoch: epoch(4),
                marker_delivery_seq: 8,
            }),
            drained: None,
        },
    }
}

fn died_rows() -> Vec<StoredOperation> {
    vec![
        died(
            StoredDiedCause::ConnectionLost,
            StoredTerminalDisposition::Committed { terminal_seq: 22 },
        ),
        died(
            StoredDiedCause::ProcessKilled,
            StoredTerminalDisposition::Pending,
        ),
        died(
            StoredDiedCause::ProtocolError,
            StoredTerminalDisposition::Pending,
        ),
        died(
            StoredDiedCause::UncleanServerRestart {
                prior_server_incarnation: 9,
            },
            StoredTerminalDisposition::Committed { terminal_seq: 23 },
        ),
    ]
}

fn detached_rows() -> Vec<StoredOperation> {
    let request = detach_request(3);
    vec![
        StoredOperation::Detached {
            row: StoredDetached {
                participant_id: 7,
                binding_epoch: epoch(10),
                cause: StoredDetachedCause::CleanDeregister,
                terminal_order: 30,
                disposition: StoredTerminalDisposition::Committed { terminal_seq: 31 },
                source: StoredDetachedSource::ExplicitRequestCommitted {
                    request,
                    secret_verified: true,
                    verifier: [4; 32],
                    receiving_epoch: epoch(10),
                    event: vec![1, 2, 3],
                },
            },
        },
        StoredOperation::Detached {
            row: StoredDetached {
                participant_id: 7,
                binding_epoch: epoch(10),
                cause: StoredDetachedCause::CleanDeregister,
                terminal_order: 32,
                disposition: StoredTerminalDisposition::Pending,
                source: StoredDetachedSource::ExplicitRequestPending {
                    request,
                    secret_verified: true,
                    verifier: [5; 32],
                    receiving_epoch: epoch(10),
                    observer_baseline: 18,
                },
            },
        },
        StoredOperation::Detached {
            row: StoredDetached {
                participant_id: 7,
                binding_epoch: epoch(10),
                cause: StoredDetachedCause::ServerShutdown,
                terminal_order: 33,
                disposition: StoredTerminalDisposition::Pending,
                source: StoredDetachedSource::ConnectionClose {
                    connection_intent_sequence: 12,
                },
            },
        },
    ]
}

const fn terminal_audit() -> StoredCommittedTerminalAudit {
    StoredCommittedTerminalAudit {
        cause: StoredDiedCause::ProtocolError,
        transaction_order: 55,
        terminal_seq: 56,
    }
}

fn ordinary_rows() -> Vec<StoredOperation> {
    let cases = [
        (
            StoredOrdinaryTerminalSource::DiedCommitted {
                died_source_sequence: 6,
            },
            41,
            vec![9, 8],
        ),
        (
            StoredOrdinaryTerminalSource::PendingDiedFinalized {
                died_source_sequence: 6,
                finalizer: StoredPendingDiedFinalizer::Left { source_sequence: 8 },
            },
            42,
            vec![7, 6],
        ),
        (
            StoredOrdinaryTerminalSource::PendingDiedFinalized {
                died_source_sequence: 6,
                finalizer: StoredPendingDiedFinalizer::FencedAttached { source_sequence: 9 },
            },
            43,
            vec![5, 4],
        ),
    ];
    cases
        .into_iter()
        .map(
            |(terminal_source, resulting_floor, event)| StoredOperation::Ordinary {
                row: StoredOrdinaryFate {
                    participant_id: 7,
                    last_dead_binding_epoch: epoch(10),
                    ordinary_attached_source_sequence: 4,
                    terminal_source,
                    committed_terminal_audit: terminal_audit(),
                    resulting_floor,
                },
                event,
            },
        )
        .collect()
}

fn recovered_rows() -> Vec<StoredOperation> {
    [
        (
            StoredRecoveredPresentation::DiedCommittedOwns,
            44,
            vec![3, 2],
        ),
        (
            StoredRecoveredPresentation::RecoveredOwnsAndReservesFinalizer,
            45,
            vec![1, 0],
        ),
    ]
    .into_iter()
    .map(
        |(presentation, resulting_floor, event)| StoredOperation::Recovered {
            row: StoredRecoveredFate {
                participant_id: 7,
                last_dead_binding_epoch: epoch(10),
                died_source_sequence: 6,
                fenced_attached_source_sequence: 9,
                prior_binding_epoch: epoch(4),
                marker_delivery_seq: 14,
                resulting_floor,
                presentation,
            },
            event,
        },
    )
    .collect()
}

fn round_trip_fate_rows(rows: Vec<StoredOperation>) -> Result<(), Box<dyn Error>> {
    for row in rows {
        let encoded = serde_json::to_vec(&row)?;
        assert!(!String::from_utf8_lossy(&encoded).contains("digest"));
        let decoded: StoredOperation = serde_json::from_slice(&encoded)?;
        assert_eq!(decoded, row);
        assert_eq!(serde_json::to_vec(&decoded)?, encoded);
    }
    Ok(())
}

#[test]
fn died_stored_operation_round_trips_and_replays_committed_and_pending()
-> Result<(), Box<dyn Error>> {
    round_trip_fate_rows(died_rows())
}

#[test]
fn detached_stored_operation_round_trips_request_disconnect_and_shutdown()
-> Result<(), Box<dyn Error>> {
    round_trip_fate_rows(detached_rows())
}

#[test]
fn ordinary_stored_operation_round_trips_and_replays_measured_fate() -> Result<(), Box<dyn Error>> {
    round_trip_fate_rows(ordinary_rows())
}

#[test]
fn recovered_stored_operation_round_trips_without_died_terminal() -> Result<(), Box<dyn Error>> {
    for row in recovered_rows() {
        let StoredOperation::Recovered { row: recovered, .. } = &row else {
            return Err("Recovered fixture emitted another operation".into());
        };
        assert_eq!(recovered.died_source_sequence, 6);
        assert!(matches!(
            recovered.presentation,
            StoredRecoveredPresentation::DiedCommittedOwns
                | StoredRecoveredPresentation::RecoveredOwnsAndReservesFinalizer
        ));
        round_trip_fate_rows(vec![row])?;
    }
    Ok(())
}

const fn attach_request(marker: Option<u64>) -> StoredAttachRequest {
    StoredAttachRequest {
        conversation_id: CONVERSATION,
        participant_id: 7,
        capability_generation: 8,
        attach_secret: [2; 32],
        token: [3; 16],
        accept_marker_delivery_seq: marker,
    }
}

const fn v2_attach_allocation(terminal: Option<u64>) -> StoredAttachAllocationV2 {
    StoredAttachAllocationV2 {
        binding_epoch: epoch(20),
        attach_secret: [4; 32],
        attached_order: 30,
        attached_seq: 31,
        receipt_expires_at: StoredU128([5; 16]),
        provenance_expires_at: StoredU128([6; 16]),
        admitted_now_ms: 40,
        superseded_terminal_seq: terminal,
    }
}

#[derive(Serialize)]
struct FrozenV2Entry<'a> {
    schema_version: u8,
    operation: &'a StoredOperationV2,
}

#[test]
fn attached_v2_mapping_is_lossless_and_marker_rows_refuse_without_proof()
-> Result<(), Box<dyn Error>> {
    let ordinary_v2 = StoredOperationV2::Attached {
        request: attach_request(None),
        secret_verified: true,
        allocation: v2_attach_allocation(None),
        event: vec![7, 8],
    };
    let frozen_before = serde_json::to_vec(&FrozenV2Entry {
        schema_version: SCHEMA_VERSION_V2,
        operation: &ordinary_v2,
    })?;
    assert_eq!(
        frozen_before.as_slice(),
        include_bytes!("fixtures/w1b_attached_v2_ordinary.json")
    );
    let StoredOperationV2::Attached {
        request,
        secret_verified,
        allocation,
        event,
    } = ordinary_v2.clone()
    else {
        return Err("ordinary v2 fixture changed shape".into());
    };
    let ordinary = migrate_v2_attached(
        request,
        secret_verified,
        allocation,
        event,
        V2AttachedPrestate::Detached,
        11,
    )?;
    let StoredOperation::Attached {
        allocation, mode, ..
    } = ordinary
    else {
        return Err("ordinary v2 mapping did not produce Attached v3".into());
    };
    assert_eq!(allocation.attached_order, 30);
    assert_eq!(allocation.attached_seq, 31);
    assert!(matches!(mode.as_ref(), StoredAttachModeV3::Ordinary));
    assert_eq!(
        serde_json::to_vec(&FrozenV2Entry {
            schema_version: SCHEMA_VERSION_V2,
            operation: &ordinary_v2,
        })?,
        frozen_before
    );

    let superseding = migrate_v2_attached(
        attach_request(None),
        true,
        v2_attach_allocation(Some(29)),
        vec![9],
        V2AttachedPrestate::Bound {
            binding_epoch: epoch(12),
        },
        12,
    )?;
    let StoredOperation::Attached { mode, .. } = superseding else {
        return Err("superseding v2 mapping did not produce Attached v3".into());
    };
    assert!(matches!(
        mode.as_ref(),
        StoredAttachModeV3::Superseding {
            prior_binding_epoch,
            terminal_transaction_order: 30,
            terminal_delivery_seq: 29,
        } if *prior_binding_epoch == epoch(12)
    ));

    let marker_refusal = migrate_v2_attached(
        attach_request(Some(17)),
        true,
        v2_attach_allocation(None),
        vec![10],
        V2AttachedPrestate::Detached,
        13,
    );
    assert!(matches!(
        marker_refusal,
        Err(OperationLogError::V2AttachedFencedProofUnavailable { sequence: 13 })
    ));
    assert!(matches!(
        migrate_v2_attached(
            attach_request(None),
            true,
            v2_attach_allocation(Some(29)),
            Vec::new(),
            V2AttachedPrestate::Detached,
            14,
        ),
        Err(OperationLogError::V2AttachedModeMismatch { sequence: 14 })
    ));
    Ok(())
}

#[test]
fn old_v2_reader_refuses_v3_fate_row_with_typed_schema_version() -> Result<(), Box<dyn Error>> {
    let store = store()?;
    let log = OperationLog::new(store, CONVERSATION);
    block_on(log.append(
        &died(
            StoredDiedCause::ConnectionLost,
            StoredTerminalDisposition::Pending,
        ),
        0,
    ))??;
    assert!(matches!(
        block_on(log.read_v2_page(0)),
        Ok(Err(OperationLogError::SchemaVersion(3)))
    ));
    Ok(())
}

#[test]
fn v3_reader_accepts_v2_prefix_and_refuses_v2_after_v3() -> Result<(), Box<dyn Error>> {
    let accepted_store = store()?;
    append_payload(&accepted_store, 0, V2_GENESIS)?;
    let accepted_log = OperationLog::new(Arc::clone(&accepted_store), CONVERSATION);
    block_on(accepted_log.append(&StoredOperation::Genesis { event: vec![1] }, 1))??;
    let accepted = block_on(accepted_log.read_page(0, OperationSchemaPhase::V2Prefix))??;
    assert_eq!(accepted.rows.len(), 2);
    assert_eq!(accepted.rows[0].schema_version, SCHEMA_VERSION_V2);
    assert_eq!(accepted.rows[1].schema_version, SCHEMA_VERSION);
    assert_eq!(accepted.next_phase, OperationSchemaPhase::V3Suffix);

    let refused_store = store()?;
    append_payload(&refused_store, 0, V2_GENESIS)?;
    let refused_log = OperationLog::new(Arc::clone(&refused_store), CONVERSATION);
    block_on(refused_log.append(&StoredOperation::Genesis { event: vec![1] }, 1))??;
    append_payload(&refused_store, 2, V2_GENESIS)?;
    assert!(matches!(
        block_on(refused_log.read_page(0, OperationSchemaPhase::V2Prefix)),
        Ok(Err(OperationLogError::SchemaVersionTransition {
            sequence: 2,
            previous: 3,
            actual: 2,
        }))
    ));
    Ok(())
}

#[test]
fn fenced_attach_linearity_ui_contract() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/trybuild/fenced_descriptions_remain_copy.rs");
    cases.compile_fail("tests/trybuild/fenced_raw_mint_is_private.rs");
    cases.compile_fail("tests/trybuild/fenced_owner_cannot_mint_twice.rs");
    cases.compile_fail("tests/trybuild/fenced_proof_cannot_clone.rs");
    cases.compile_fail("tests/trybuild/fenced_proof_cannot_copy.rs");
    cases.compile_fail("tests/trybuild/fenced_proof_cannot_reuse_after_verify.rs");
    cases.compile_fail("tests/trybuild/fenced_proof_fate_method_is_private.rs");
    cases.compile_fail("tests/trybuild/fenced_attach_commit_cannot_split_twice.rs");
    cases.compile_fail("tests/trybuild/validated_marker_record_cannot_clone.rs");
    cases.compile_fail("tests/trybuild/validated_marker_record_cannot_copy.rs");
    cases.compile_fail("tests/trybuild/validated_marker_record_cannot_feed_two_recoveries.rs");
}

#[test]
fn v2_after_v3_across_operation_page_boundary_refuses_before_apply() -> Result<(), Box<dyn Error>> {
    let store = store()?;
    let final_slot = u64::try_from(
        READ_BATCH_SIZE
            .checked_sub(1)
            .ok_or("read batch has no final slot")?,
    )?;
    for sequence in 0..final_slot {
        append_payload(&store, sequence, V2_GENESIS)?;
    }
    let log = OperationLog::new(Arc::clone(&store), CONVERSATION);
    block_on(log.append(&StoredOperation::Genesis { event: vec![3] }, final_slot))??;
    let next_page = final_slot.checked_add(1).ok_or("page sequence overflow")?;
    append_payload(&store, next_page, V2_GENESIS)?;

    let first = block_on(log.read_page(0, OperationSchemaPhase::V2Prefix))??;
    assert_eq!(first.rows.len(), READ_BATCH_SIZE);
    assert_eq!(first.next_phase, OperationSchemaPhase::V3Suffix);
    assert!(matches!(
        block_on(log.read_page(next_page, first.next_phase)),
        Ok(Err(OperationLogError::SchemaVersionTransition {
            sequence,
            previous: 3,
            actual: 2,
        })) if sequence == next_page
    ));
    assert!(matches!(
        block_on(validate_operation_schema(&log, test_participant_config().identity_slots)),
        Ok(Err(StateError::Log(OperationLogError::SchemaVersionTransition {
            sequence,
            previous: 3,
            actual: 2,
        }))) if sequence == next_page
    ));
    Ok(())
}

fn one_payload_error(payloads: &[&[u8]]) -> Result<OperationLogError, Box<dyn Error>> {
    let store = store()?;
    for (index, payload) in payloads.iter().enumerate() {
        append_payload(&store, u64::try_from(index)?, payload)?;
    }
    let log = OperationLog::new(store, CONVERSATION);
    match block_on(log.read_page(0, OperationSchemaPhase::V2Prefix)) {
        Ok(Err(error)) => Ok(error),
        Ok(Ok(_)) => Err("malformed version fixture unexpectedly decoded".into()),
        Err(error) => Err(error.into()),
    }
}

#[test]
fn missing_unknown_malformed_and_mixed_operation_versions_refuse_before_publication()
-> Result<(), Box<dyn Error>> {
    let missing = one_payload_error(&[br#"{"operation":{"operation":"genesis","event":[]}}"#])?;
    assert!(matches!(missing, OperationLogError::Serialization(_)));

    let unknown = one_payload_error(&[
        br#"{"schema_version":9,"operation":{"operation":"genesis","event":[]}}"#,
    ])?;
    assert!(matches!(unknown, OperationLogError::SchemaVersion(9)));

    let malformed = one_payload_error(&[b"not-json"])?;
    assert!(matches!(malformed, OperationLogError::Serialization(_)));

    let mixed = one_payload_error(&[
        br#"{"schema_version":3,"operation":{"operation":"genesis","event":[]}}"#,
        V2_GENESIS,
    ])?;
    assert!(matches!(
        mixed,
        OperationLogError::SchemaVersionTransition {
            sequence: 1,
            previous: 3,
            actual: 2,
        }
    ));
    Ok(())
}
