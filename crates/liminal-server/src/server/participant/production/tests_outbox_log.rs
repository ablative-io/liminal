//! Unit 2 extension schema, canonical codec, and Q4 measurements.

use std::error::Error;
use std::io;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal::durability::{DurableStore, open_ephemeral};
use liminal_protocol::wire::{
    BindingEpoch, ClientRequest, ConnectionIncarnation, DetachedCause, DiedCause, Generation,
    ParticipantDelivery, ParticipantFrame, ParticipantRecord, RecordAdmission,
    RecordAdmissionAttemptToken, ServerPush, encoded_len,
};

use crate::config::types::ParticipantConfig;

use super::frontier::ordinary_record_charge;
use super::outbox_log::{
    OUTBOX_SCHEMA_VERSION, OUTBOX_STREAM_PREFIX, OutboxLog, OutboxLogError, OutboxRow,
    ProducedBatch, ProducedSourceKind, ProjectedRecord, StoredMarkerAckCommitted,
    UNIT2_OUTBOX_RESTORE_BATCH_ROWS, decode_row, encode_row,
};
use super::tests::test_participant_config;

const CONVERSATION: u64 = 0xF0_C2;
const SOURCE_SEQUENCE: u64 = 17;
const DELIVERY_SEQUENCE: u64 = 29;

#[test]
fn unit2_outbox_restore_batch_rows_has_the_signed_name_and_value() {
    assert_eq!(UNIT2_OUTBOX_RESTORE_BATCH_ROWS, 64);
}

fn generation(value: u64) -> Result<Generation, Box<dyn Error>> {
    Generation::new(value).ok_or_else(|| io::Error::other("test generation must be nonzero").into())
}

fn epoch() -> Result<BindingEpoch, Box<dyn Error>> {
    Ok(BindingEpoch::new(
        ConnectionIncarnation::new(u64::MAX, u64::MAX),
        generation(u64::MAX)?,
    ))
}

fn recipients() -> Vec<u64> {
    vec![u64::MAX - 3, u64::MAX - 2, u64::MAX - 1, u64::MAX]
}

fn projected(
    body: ParticipantRecord,
    sender: Option<u64>,
) -> Result<ProjectedRecord, Box<dyn Error>> {
    Ok(ProjectedRecord::try_new(
        CONVERSATION,
        DELIVERY_SEQUENCE,
        body,
        recipients(),
        sender,
    )?)
}

fn produced(source_kind: ProducedSourceKind, record: ProjectedRecord) -> OutboxRow {
    OutboxRow::Produced(ProducedBatch::new(
        SOURCE_SEQUENCE,
        source_kind,
        vec![record],
    ))
}

fn record_admission(payload: Vec<u8>) -> Result<RecordAdmission, Box<dyn Error>> {
    Ok(RecordAdmission {
        conversation_id: CONVERSATION,
        participant_id: u64::MAX - 4,
        capability_generation: generation(u64::MAX)?,
        record_admission_attempt_token: RecordAdmissionAttemptToken::new([u8::MAX; 16]),
        payload,
    })
}

fn max_record_admission_payload(config: &ParticipantConfig) -> Result<usize, Box<dyn Error>> {
    let empty = ParticipantFrame::ClientRequest(ClientRequest::RecordAdmission(record_admission(
        Vec::new(),
    )?));
    let fixed = encoded_len(&empty)
        .map_err(|error| io::Error::other(format!("request codec failed: {error:?}")))?;
    let limit = usize::try_from(config.wire_frame_limit)?;
    limit
        .checked_sub(fixed)
        .ok_or_else(|| io::Error::other("signed frame limit cannot hold RecordAdmission").into())
}

pub(super) fn measured_fixed_outbox_overhead(
    config: &ParticipantConfig,
) -> Result<(u64, u64), Box<dyn Error>> {
    let payload = vec![u8::MAX; max_record_admission_payload(config)?];
    let mut maximum_fixed_per_record = 0_u64;
    for (_, row) in all_record_rows(payload)? {
        let encoded = encode_row(&row)?;
        let OutboxRow::Produced(batch) = row else {
            return Err(io::Error::other("measurement row was not Produced").into());
        };
        let Some(record) = batch.ordered_records().first() else {
            return Err(io::Error::other("measurement batch was empty").into());
        };
        let payload_length = match record.body() {
            ParticipantRecord::OrdinaryRecord { payload, .. } => payload.len(),
            _ => 0,
        };
        let fixed = encoded
            .len()
            .checked_sub(payload_length)
            .ok_or_else(|| io::Error::other("encoded row shorter than its raw payload"))?;
        maximum_fixed_per_record = maximum_fixed_per_record.max(u64::try_from(fixed)?);
    }
    let fixed_metadata_term = maximum_fixed_per_record
        .checked_mul(config.max_retained_record_rows)
        .ok_or_else(|| io::Error::other("signed fixed outbox metadata term overflowed"))?;
    Ok((maximum_fixed_per_record, fixed_metadata_term))
}

fn all_record_rows(payload: Vec<u8>) -> Result<Vec<(&'static str, OutboxRow)>, Box<dyn Error>> {
    let binding_epoch = epoch()?;
    let sender = u64::MAX - 4;
    Ok(vec![
        (
            "ordinary_record",
            produced(
                ProducedSourceKind::RecordAdmission,
                projected(
                    ParticipantRecord::OrdinaryRecord {
                        sender_participant_id: sender,
                        payload,
                    },
                    Some(sender),
                )?,
            ),
        ),
        (
            "attached",
            produced(
                ProducedSourceKind::Attached,
                projected(
                    ParticipantRecord::Attached {
                        affected_participant_id: sender,
                        binding_epoch,
                    },
                    Some(sender),
                )?,
            ),
        ),
        (
            "detached",
            produced(
                ProducedSourceKind::Detached,
                projected(
                    ParticipantRecord::Detached {
                        affected_participant_id: sender,
                        binding_epoch,
                        cause: DetachedCause::ServerShutdown,
                    },
                    Some(sender),
                )?,
            ),
        ),
        (
            "died",
            produced(
                ProducedSourceKind::Detached,
                projected(
                    ParticipantRecord::Died {
                        affected_participant_id: sender,
                        binding_epoch,
                        cause: DiedCause::UncleanServerRestart {
                            prior_server_incarnation: u64::MAX,
                        },
                    },
                    Some(sender),
                )?,
            ),
        ),
        (
            "left",
            produced(
                ProducedSourceKind::Left,
                projected(
                    ParticipantRecord::Left {
                        affected_participant_id: sender,
                        ended_binding_epoch: Some(binding_epoch),
                    },
                    Some(sender),
                )?,
            ),
        ),
        (
            "history_compacted",
            produced(
                ProducedSourceKind::MarkerDrained,
                projected(
                    ParticipantRecord::HistoryCompacted {
                        affected_participant_id: u64::MAX,
                        abandoned_after: u64::MAX,
                        abandoned_through: u64::MAX,
                        physical_floor_at_decision: u64::MAX,
                    },
                    None,
                )?,
            ),
        ),
    ])
}

#[test]
fn schema_v1_codec_round_trips_every_row_and_record_kind() -> Result<(), Box<dyn Error>> {
    let payload = vec![0, 1, 2, 3, u8::MAX];
    for (_, row) in all_record_rows(payload)? {
        let encoded = encode_row(&row)?;
        assert_eq!(encoded.first(), Some(&OUTBOX_SCHEMA_VERSION));
        assert_eq!(decode_row(&encoded)?, row);
    }

    let ack = OutboxRow::AckAdvanced {
        source_log_sequence: SOURCE_SEQUENCE,
        participant_id: u64::MAX,
        through_seq: DELIVERY_SEQUENCE,
    };
    assert_eq!(decode_row(&encode_row(&ack)?)?, ack);

    let marker = OutboxRow::MarkerAckCommitted(StoredMarkerAckCommitted {
        request: liminal_protocol::wire::MarkerAck {
            conversation_id: CONVERSATION,
            participant_id: u64::MAX,
            capability_generation: generation(u64::MAX)?,
            marker_delivery_seq: DELIVERY_SEQUENCE,
        },
        receiving_binding_epoch: epoch()?,
        offered_marker_delivery_seq: DELIVERY_SEQUENCE,
        delivered_binding_epoch: epoch()?,
        from_cursor: DELIVERY_SEQUENCE - 1,
        resulting_cursor: DELIVERY_SEQUENCE,
        base_log_head: SOURCE_SEQUENCE,
        extension_sequence: 3,
    });
    assert_eq!(decode_row(&encode_row(&marker)?)?, marker);
    Ok(())
}

#[test]
fn extension_schema_version_refuses_before_projection() -> Result<(), Box<dyn Error>> {
    assert!(matches!(
        decode_row(&[]),
        Err(OutboxLogError::MissingSchemaVersion)
    ));
    assert!(matches!(
        decode_row(&[0]),
        Err(OutboxLogError::SchemaVersion(0))
    ));
    assert!(matches!(
        decode_row(&[OUTBOX_SCHEMA_VERSION, 99]),
        Err(OutboxLogError::UnknownTag {
            domain: "row-kind",
            value: 99
        })
    ));
    assert!(matches!(
        decode_row(&[OUTBOX_SCHEMA_VERSION, 0]),
        Err(OutboxLogError::UnexpectedEnd { .. })
    ));

    let row = OutboxRow::AckAdvanced {
        source_log_sequence: 0,
        participant_id: 1,
        through_seq: 2,
    };
    let mut drifted = encode_row(&row)?;
    drifted.push(0);
    assert!(matches!(
        decode_row(&drifted),
        Err(OutboxLogError::TrailingBytes { remaining: 1 })
    ));
    Ok(())
}

#[test]
fn mixed_extension_stream_refuses_without_returning_rows() -> Result<(), Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let log = OutboxLog::new(Arc::clone(&store), CONVERSATION);
    let row = OutboxRow::AckAdvanced {
        source_log_sequence: 0,
        participant_id: 1,
        through_seq: 2,
    };
    block_on(log.append(&row, 0))??;
    let key = format!("{OUTBOX_STREAM_PREFIX}{CONVERSATION}");
    let assigned = block_on(store.append(&key, vec![OUTBOX_SCHEMA_VERSION + 1], 1))??;
    assert_eq!(assigned, 1);
    block_on(store.flush())??;

    assert!(matches!(
        block_on(log.read_all())?,
        Err(OutboxLogError::MixedSchemaVersions {
            expected: OUTBOX_SCHEMA_VERSION,
            actual: 2
        })
    ));
    Ok(())
}

#[test]
fn canonical_outbox_encoder_prints_checked_q4_measurements() -> Result<(), Box<dyn Error>> {
    let config = test_participant_config();
    let payload_bytes = max_record_admission_payload(&config)?;
    let payload = vec![u8::MAX; payload_bytes];
    let rows = all_record_rows(payload)?;
    let mut maximum_encoded_push = 0_u64;
    let mut maximum_fixed_per_record = 0_u64;

    for (name, row) in rows {
        let encoded = encode_row(&row)?;
        let OutboxRow::Produced(batch) = &row else {
            return Err(io::Error::other("measurement row was not Produced").into());
        };
        let Some(record) = batch.ordered_records().first() else {
            return Err(io::Error::other("measurement batch was empty").into());
        };
        let payload_length = match record.body() {
            ParticipantRecord::OrdinaryRecord { payload, .. } => payload.len(),
            _ => 0,
        };
        let fixed = encoded
            .len()
            .checked_sub(payload_length)
            .ok_or_else(|| io::Error::other("encoded row shorter than its raw payload"))?;
        let fixed = u64::try_from(fixed)?;
        maximum_fixed_per_record = maximum_fixed_per_record.max(fixed);
        maximum_encoded_push = maximum_encoded_push.max(record.encoded_push_bytes());
        println!(
            "MEASURED_Q4_RECORD_KIND_{name}_OUTBOX_BYTES={} FIXED_BYTES={fixed} PUSH_BYTES={}",
            encoded.len(),
            record.encoded_push_bytes()
        );
        assert_eq!(decode_row(&encoded)?, row);
    }

    let (measured_fixed_per_record, fixed_metadata_term) = measured_fixed_outbox_overhead(&config)?;
    assert_eq!(maximum_fixed_per_record, measured_fixed_per_record);
    let signed_capacity = config
        .retained_capacity_bytes
        .checked_add(fixed_metadata_term)
        .ok_or_else(|| io::Error::other("signed live outbox byte recommendation overflowed"))?;
    assert!(signed_capacity >= config.retained_capacity_bytes);
    assert!(maximum_encoded_push <= config.wire_frame_limit);
    assert_eq!(recipients().len(), usize::try_from(config.identity_slots)?);
    println!("MEASURED_Q4_FIXED_OUTBOX_OVERHEAD_BYTES={fixed_metadata_term}");
    println!("MEASURED_Q4_MAXIMUM_ENCODED_PUSH_BYTES={maximum_encoded_push}");
    Ok(())
}

#[test]
fn outbox_bound_domination_inequalities_hold_at_the_codec() -> Result<(), Box<dyn Error>> {
    let config = test_participant_config();
    let maximum_payload = vec![u8::MAX; max_record_admission_payload(&config)?];
    let payload_bytes = u64::try_from(maximum_payload.len())?;
    let core_retained_bytes =
        ordinary_record_charge(&record_admission(maximum_payload.clone())?)?.bytes;
    let core_retained_overhead = core_retained_bytes
        .checked_sub(payload_bytes)
        .ok_or_else(|| io::Error::other("core retained charge was smaller than its payload"))?;
    let one_identity_recipient = std::iter::once(u64::MAX).collect::<Vec<_>>();
    let mut ordinary_push_overhead = None;
    let mut maximum_nonordinary_push = None;
    let mut minimum_per_row_fixed_outbox_term = None;

    for (name, row) in all_record_rows(maximum_payload)? {
        let OutboxRow::Produced(batch) = row else {
            return Err(io::Error::other("domination measurement row was not Produced").into());
        };
        let Some(record) = batch.ordered_records().first() else {
            return Err(io::Error::other("domination measurement batch was empty").into());
        };
        let canonical_push =
            ParticipantFrame::ServerPush(ServerPush::ParticipantDelivery(ParticipantDelivery {
                conversation_id: CONVERSATION,
                delivery_seq: record.delivery_seq(),
                record: record.body().clone(),
            }));
        let push_bytes = u64::try_from(
            encoded_len(&canonical_push)
                .map_err(|error| io::Error::other(format!("push codec failed: {error:?}")))?,
        )?;
        assert_eq!(push_bytes, record.encoded_push_bytes());

        let raw_payload_bytes = match record.body() {
            ParticipantRecord::OrdinaryRecord { payload, .. } => u64::try_from(payload.len())?,
            _ => u64::default(),
        };
        if matches!(record.body(), ParticipantRecord::OrdinaryRecord { .. }) {
            ordinary_push_overhead =
                Some(push_bytes.checked_sub(raw_payload_bytes).ok_or_else(|| {
                    io::Error::other("ordinary push was smaller than its payload")
                })?);
        } else {
            maximum_nonordinary_push = Some(
                maximum_nonordinary_push.map_or(push_bytes, |maximum: u64| maximum.max(push_bytes)),
            );
        }

        let one_identity_record = ProjectedRecord::try_new(
            CONVERSATION,
            record.delivery_seq(),
            record.body().clone(),
            one_identity_recipient.clone(),
            record.sender(),
        )?;
        let one_identity_row = OutboxRow::Produced(ProducedBatch::new(
            batch.source_log_sequence(),
            batch.source_kind(),
            vec![one_identity_record],
        ));
        let fixed_at_one_identity = u64::try_from(encode_row(&one_identity_row)?.len())?
            .checked_sub(raw_payload_bytes)
            .ok_or_else(|| {
                io::Error::other("one-identity outbox row was smaller than its payload")
            })?;
        minimum_per_row_fixed_outbox_term = Some(
            minimum_per_row_fixed_outbox_term.map_or(fixed_at_one_identity, |maximum: u64| {
                maximum.max(fixed_at_one_identity)
            }),
        );
        println!(
            "MEASURED_ROW4_RECORD_KIND_{name}_PUSH_BYTES={push_bytes} FIXED_OUTBOX_F1_BYTES={fixed_at_one_identity}"
        );
    }

    let ordinary_push_overhead = ordinary_push_overhead
        .ok_or_else(|| io::Error::other("ordinary push measurement was absent"))?;
    let maximum_nonordinary_push = maximum_nonordinary_push
        .ok_or_else(|| io::Error::other("non-ordinary push measurement was absent"))?;
    let minimum_per_row_fixed_outbox_term = minimum_per_row_fixed_outbox_term
        .ok_or_else(|| io::Error::other("one-identity fixed outbox measurement was absent"))?;

    // This is the tripwire for the structural proof that the signed
    // UNIT2_MAX_LIVE_OUTBOX_PAYLOAD_BYTES bound is dominated by the core caps. If
    // either strict inequality fails, that proof is dead and production needs a
    // comparator; fail loudly here rather than silently losing the guarantee.
    assert!(
        ordinary_push_overhead < core_retained_overhead,
        "ordinary push overhead {ordinary_push_overhead} no longer falls strictly below core retained overhead {core_retained_overhead}"
    );
    assert!(
        maximum_nonordinary_push < minimum_per_row_fixed_outbox_term,
        "maximum non-ordinary push {maximum_nonordinary_push} no longer falls strictly below fixed outbox F(1) {minimum_per_row_fixed_outbox_term}"
    );
    println!(
        "MEASURED_ROW4_ORDINARY_PUSH_OVERHEAD_BYTES={ordinary_push_overhead} CORE_RETAINED_OVERHEAD_BYTES={core_retained_overhead} MAXIMUM_NONORDINARY_PUSH_BYTES={maximum_nonordinary_push} MINIMUM_FIXED_OUTBOX_F1_BYTES={minimum_per_row_fixed_outbox_term}"
    );
    Ok(())
}
