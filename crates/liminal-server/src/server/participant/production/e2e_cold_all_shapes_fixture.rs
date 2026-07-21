//! Durable-row and socket helpers for the full mapped-shape cold-reopen oracle.

use std::collections::BTreeSet;
use std::error::Error;
use std::path::Path;
use std::sync::Arc;

use liminal::durability::DurableStore;
use liminal::durability::bridge::block_on;
use liminal_protocol::wire::{
    AttachSecret, BindingEpoch, ClientRequest, EnrollBound, Generation, ParticipantAck,
    ParticipantDelivery, ParticipantId, ParticipantRecord, ServerValue,
};

use super::e2e_tests::SocketFixture;
use super::log::{
    DecodedStoredOperation, OperationLog, OperationSchemaPhase, StoredAttachModeV3,
    StoredBindingEpoch, StoredDetachedCause, StoredDetachedSource, StoredDiedCause,
    StoredOperation,
};
use super::outbox_log::{OutboxLog, OutboxRow, ProducedSourceKind};
use super::tests::open_disk_store_for_tests;

pub(super) const ALL_SHAPES_CONVERSATION: u64 = 0x24_01;
pub(super) const MARKER_INTERLEAVING_CONVERSATION: u64 = 0x24_02;

type DecodedHistory = (Vec<(u64, StoredOperation)>, Vec<(u64, OutboxRow)>);
type ClassifiedCrashCut = (Vec<(u64, StoredOperation)>, Vec<TypedFateSource>);

#[derive(Clone, Copy)]
pub(super) struct ColdMember {
    pub(super) participant_id: u64,
    pub(super) generation: Generation,
    pub(super) secret: AttachSecret,
}

impl ColdMember {
    pub(super) fn enrolled(bound: &EnrollBound) -> Self {
        Self {
            participant_id: bound.participant_id(),
            generation: Generation::ONE,
            secret: bound.attach_secret(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BoundClosePath {
    DroppedSocket,
    ServerStop,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TypedFateSource {
    DiedConnectionClose {
        participant_id: ParticipantId,
        binding_epoch: StoredBindingEpoch,
        cause: StoredDiedCause,
    },
    DiedWithoutConnectionIntent {
        participant_id: ParticipantId,
        binding_epoch: StoredBindingEpoch,
        cause: StoredDiedCause,
    },
    DetachedConnectionClose {
        participant_id: ParticipantId,
        binding_epoch: StoredBindingEpoch,
        cause: StoredDetachedCause,
    },
    DetachedExplicitRequest {
        participant_id: ParticipantId,
        binding_epoch: StoredBindingEpoch,
        cause: StoredDetachedCause,
    },
    Ordinary,
    Recovered,
}

pub(super) fn expected_bound_close_fate(
    participant_id: ParticipantId,
    binding_epoch: BindingEpoch,
    path: BoundClosePath,
) -> TypedFateSource {
    match path {
        BoundClosePath::DroppedSocket => TypedFateSource::DiedConnectionClose {
            participant_id,
            binding_epoch: binding_epoch.into(),
            cause: StoredDiedCause::ConnectionLost,
        },
        BoundClosePath::ServerStop => TypedFateSource::DetachedConnectionClose {
            participant_id,
            binding_epoch: binding_epoch.into(),
            cause: StoredDetachedCause::ServerShutdown,
        },
    }
}

pub(super) fn semantic_rows_and_typed_fate_suffix(
    mut base: Vec<(u64, StoredOperation)>,
) -> Result<ClassifiedCrashCut, Box<dyn Error>> {
    let final_semantic_index = base
        .iter()
        .rposition(|(_, operation)| is_semantic_operation(operation))
        .ok_or("durable history contained no semantic operation")?;
    let suffix_start = final_semantic_index
        .checked_add(1)
        .ok_or("semantic cut index overflowed")?;
    let suffix = base
        .split_off(suffix_start)
        .into_iter()
        .map(|(_, operation)| match operation {
            StoredOperation::Died { row } if row.connection_intent_sequence.is_some() => {
                Ok(TypedFateSource::DiedConnectionClose {
                    participant_id: row.participant_id,
                    binding_epoch: row.binding_epoch,
                    cause: row.cause,
                })
            }
            StoredOperation::Died { row } => Ok(TypedFateSource::DiedWithoutConnectionIntent {
                participant_id: row.participant_id,
                binding_epoch: row.binding_epoch,
                cause: row.cause,
            }),
            StoredOperation::Detached { row }
                if matches!(row.source, StoredDetachedSource::ConnectionClose { .. }) =>
            {
                Ok(TypedFateSource::DetachedConnectionClose {
                    participant_id: row.participant_id,
                    binding_epoch: row.binding_epoch,
                    cause: row.cause,
                })
            }
            StoredOperation::Detached { row } => Ok(TypedFateSource::DetachedExplicitRequest {
                participant_id: row.participant_id,
                binding_epoch: row.binding_epoch,
                cause: row.cause,
            }),
            StoredOperation::Ordinary { .. } => Ok(TypedFateSource::Ordinary),
            StoredOperation::Recovered { .. } => Ok(TypedFateSource::Recovered),
            operation => {
                Err(format!("non-fate row followed the final semantic cut: {operation:?}").into())
            }
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    base.retain(|(_, operation)| is_semantic_operation(operation));
    Ok((base, suffix))
}

fn is_semantic_operation(operation: &StoredOperation) -> bool {
    matches!(
        operation,
        StoredOperation::Genesis { .. }
            | StoredOperation::Enrolled { .. }
            | StoredOperation::Attached { .. }
            | StoredOperation::ZeroDebtAck { .. }
            | StoredOperation::MarkerDrained { .. }
            | StoredOperation::RecordAdmission { .. }
            | StoredOperation::Left { .. }
    )
}

pub(super) fn expect_enrolled(
    value: ServerValue,
    label: &str,
) -> Result<EnrollBound, Box<dyn Error>> {
    let ServerValue::EnrollBound(bound) = value else {
        return Err(format!("{label} enrollment did not bind: {value:?}").into());
    };
    Ok(bound)
}

pub(super) fn decoded_history(
    data_dir: &Path,
    conversation_id: u64,
) -> Result<DecodedHistory, Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = open_disk_store_for_tests(data_dir)?;
    decoded_history_from_store(store, conversation_id)
}

pub(super) fn decoded_history_from_store(
    store: Arc<dyn DurableStore>,
    conversation_id: u64,
) -> Result<DecodedHistory, Box<dyn Error>> {
    let log = OperationLog::new(Arc::clone(&store), conversation_id);
    let mut base = Vec::new();
    let mut next = 0_u64;
    let mut phase = OperationSchemaPhase::V2Prefix;
    loop {
        let page = block_on(log.read_page(next, phase))??;
        phase = page.next_phase;
        if page.rows.is_empty() {
            break;
        }
        next = page
            .rows
            .last()
            .and_then(|decoded| decoded.sequence.checked_add(1))
            .ok_or("base history sequence overflowed")?;
        for decoded in page.rows {
            let DecodedStoredOperation::V3(operation) = decoded.operation else {
                return Err("new all-shapes fixture unexpectedly decoded a v2 row".into());
            };
            base.push((decoded.sequence, operation));
        }
    }
    let extension = block_on(OutboxLog::new(store, conversation_id).read_all())??;
    Ok((base, extension))
}

pub(super) fn expected_live_deliveries(
    conversation_id: u64,
    participant_id: u64,
    extension: &[(u64, OutboxRow)],
) -> Vec<ParticipantDelivery> {
    let mut ack_through = 0_u64;
    let mut retired = false;
    let mut projected = Vec::new();
    for (_, row) in extension {
        match row {
            OutboxRow::Produced(batch) => {
                for record in batch.ordered_records() {
                    if matches!(
                        record.body(),
                        ParticipantRecord::Left {
                            affected_participant_id,
                            ..
                        } if *affected_participant_id == participant_id
                    ) {
                        retired = true;
                        projected.clear();
                    }
                    if record.recipients().contains(&participant_id) {
                        projected.push(ParticipantDelivery {
                            conversation_id,
                            delivery_seq: record.delivery_seq(),
                            record: record.body().clone(),
                        });
                    }
                }
            }
            OutboxRow::AckAdvanced {
                participant_id: acked,
                through_seq,
                ..
            } if *acked == participant_id => ack_through = *through_seq,
            OutboxRow::AckAdvanced { .. } | OutboxRow::MarkerAckCommitted(_) => {}
        }
    }
    if retired {
        Vec::new()
    } else {
        projected
            .into_iter()
            .filter(|delivery| delivery.delivery_seq > ack_through)
            .collect()
    }
}

pub(super) fn assert_decoded_source_census(
    base: &[(u64, StoredOperation)],
    extension: &[(u64, OutboxRow)],
) -> Result<(), Box<dyn Error>> {
    let mut base_shapes = BTreeSet::new();
    for (_, row) in base {
        base_shapes.insert(match row {
            StoredOperation::Genesis { .. } => "genesis",
            StoredOperation::Enrolled { .. } => "enrolled",
            StoredOperation::Attached { mode, .. }
                if matches!(mode.as_ref(), StoredAttachModeV3::Superseding { .. }) =>
            {
                "superseding_attach"
            }
            StoredOperation::Attached { .. } => "ordinary_attach",
            StoredOperation::Detached { .. } => "detached",
            StoredOperation::ZeroDebtAck { .. } => "zero_debt_ack",
            StoredOperation::MarkerDrained { .. } => "marker_drained",
            StoredOperation::RecordAdmission { .. } => "record_admission",
            StoredOperation::Left { .. } => "left",
            StoredOperation::Died { .. }
            | StoredOperation::Ordinary { .. }
            | StoredOperation::Recovered { .. } => "w1b_fate",
        });
    }
    assert!(base_shapes.contains("genesis"));
    assert!(base_shapes.contains("enrolled"));

    let mut saw_ack = false;
    for (physical_sequence, row) in extension {
        match row {
            OutboxRow::Produced(batch) => {
                let source = base
                    .iter()
                    .find(|(sequence, _)| *sequence == batch.source_log_sequence())
                    .ok_or("Produced row did not name a decoded v2 source")?;
                let matching = matches!(
                    (batch.source_kind(), &source.1),
                    (
                        ProducedSourceKind::Enrolled,
                        StoredOperation::Enrolled { .. }
                    ) | (
                        ProducedSourceKind::Attached,
                        StoredOperation::Attached { .. }
                    ) | (
                        ProducedSourceKind::Detached,
                        StoredOperation::Detached { .. }
                    ) | (
                        ProducedSourceKind::MarkerDrained,
                        StoredOperation::MarkerDrained { .. }
                    ) | (
                        ProducedSourceKind::RecordAdmission,
                        StoredOperation::RecordAdmission { .. }
                    ) | (ProducedSourceKind::Left, StoredOperation::Left { .. })
                );
                assert!(matching, "Produced row/source kind drifted");
            }
            OutboxRow::AckAdvanced {
                source_log_sequence,
                ..
            } => {
                saw_ack = true;
                assert!(matches!(
                    base.iter()
                        .find(|(sequence, _)| sequence == source_log_sequence),
                    Some((_, StoredOperation::ZeroDebtAck { .. }))
                ));
            }
            OutboxRow::MarkerAckCommitted(stored) => {
                assert_eq!(stored.extension_sequence, *physical_sequence);
            }
        }
    }
    assert!(saw_ack);
    Ok(())
}

pub(super) fn ack_through(
    socket: &mut impl ColdRequest,
    conversation_id: u64,
    member: ColdMember,
    through_seq: u64,
) -> Result<(), Box<dyn Error>> {
    let outcome = socket.cold_request(ClientRequest::ParticipantAck(ParticipantAck {
        conversation_id,
        participant_id: member.participant_id,
        capability_generation: member.generation,
        through_seq,
    }))?;
    if !matches!(
        outcome,
        ServerValue::AckCommitted(_) | ServerValue::AckNoOp(_)
    ) {
        return Err(format!("ack through {through_seq} did not commit: {outcome:?}").into());
    }
    Ok(())
}

pub(super) trait ColdRequest {
    fn cold_request(&mut self, request: ClientRequest) -> Result<ServerValue, Box<dyn Error>>;
}

impl ColdRequest for SocketFixture {
    fn cold_request(&mut self, request: ClientRequest) -> Result<ServerValue, Box<dyn Error>> {
        self.request(request)
    }
}

impl ColdRequest for super::e2e_tests::SocketPeer {
    fn cold_request(&mut self, request: ClientRequest) -> Result<ServerValue, Box<dyn Error>> {
        self.request(request)
    }
}
