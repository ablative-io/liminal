//! The one canonical binary encoder/decoder for Unit 2 extension schema v1.

mod io;

use liminal_protocol::wire::{
    BindingEpoch, ConnectionIncarnation, DetachedCause, DiedCause, Generation, MarkerAck,
    ParticipantDelivery, ParticipantFrame, ParticipantRecord, ServerPush, encoded_len,
};

use super::{
    OUTBOX_SCHEMA_VERSION, OutboxLogError, OutboxRow, ProducedBatch, ProducedSourceKind,
    ProjectedRecord, StoredMarkerAckCommitted,
};
use io::{Decoder, Encoder};

const ROW_PRODUCED: u8 = 0;
const ROW_ACK_ADVANCED: u8 = 1;
const ROW_MARKER_ACK_COMMITTED: u8 = 2;

/// Encodes exactly one schema-v1 row.
pub(in crate::server::participant::production) fn encode_row(
    row: &OutboxRow,
) -> Result<Vec<u8>, OutboxLogError> {
    let mut encoder = Encoder::new();
    encoder.u8(OUTBOX_SCHEMA_VERSION);
    match row {
        OutboxRow::Produced(batch) => {
            encoder.u8(ROW_PRODUCED);
            encoder.u64(batch.source_log_sequence);
            encoder.u8(source_kind_tag(batch.source_kind));
            encoder.length("ordered_records", batch.ordered_records.len())?;
            for record in &batch.ordered_records {
                encode_projected_record(&mut encoder, record)?;
            }
        }
        OutboxRow::AckAdvanced {
            source_log_sequence,
            participant_id,
            through_seq,
        } => {
            encoder.u8(ROW_ACK_ADVANCED);
            encoder.u64(*source_log_sequence);
            encoder.u64(*participant_id);
            encoder.u64(*through_seq);
        }
        OutboxRow::MarkerAckCommitted(row) => {
            encoder.u8(ROW_MARKER_ACK_COMMITTED);
            encode_marker_ack(&mut encoder, &row.request);
            encode_epoch(&mut encoder, row.receiving_binding_epoch);
            encoder.u64(row.offered_marker_delivery_seq);
            encode_epoch(&mut encoder, row.delivered_binding_epoch);
            encoder.u64(row.from_cursor);
            encoder.u64(row.resulting_cursor);
            encoder.u64(row.base_log_head);
            encoder.u64(row.extension_sequence);
        }
    }
    Ok(encoder.finish())
}

/// Decodes exactly one schema-v1 row and rejects every trailing byte.
pub(in crate::server::participant::production) fn decode_row(
    payload: &[u8],
) -> Result<OutboxRow, OutboxLogError> {
    let version = payload
        .first()
        .copied()
        .ok_or(OutboxLogError::MissingSchemaVersion)?;
    if version != OUTBOX_SCHEMA_VERSION {
        return Err(OutboxLogError::SchemaVersion(version));
    }
    let mut decoder = Decoder::new(payload);
    let decoded_version = decoder.u8("schema_version")?;
    if decoded_version != version {
        return Err(OutboxLogError::SchemaVersion(decoded_version));
    }
    let row = match decoder.u8("row_kind")? {
        ROW_PRODUCED => {
            let source_log_sequence = decoder.u64("source_log_sequence")?;
            let source_kind = decode_source_kind(decoder.u8("source_kind")?)?;
            let record_count = decoder.length("ordered_records")?;
            let mut ordered_records = Vec::with_capacity(record_count);
            for _ in 0..record_count {
                ordered_records.push(decode_projected_record(&mut decoder)?);
            }
            OutboxRow::Produced(ProducedBatch::new(
                source_log_sequence,
                source_kind,
                ordered_records,
            ))
        }
        ROW_ACK_ADVANCED => OutboxRow::AckAdvanced {
            source_log_sequence: decoder.u64("source_log_sequence")?,
            participant_id: decoder.u64("participant_id")?,
            through_seq: decoder.u64("through_seq")?,
        },
        ROW_MARKER_ACK_COMMITTED => OutboxRow::MarkerAckCommitted(StoredMarkerAckCommitted {
            request: decode_marker_ack(&mut decoder)?,
            receiving_binding_epoch: decode_epoch(&mut decoder)?,
            offered_marker_delivery_seq: decoder.u64("offered_marker_delivery_seq")?,
            delivered_binding_epoch: decode_epoch(&mut decoder)?,
            from_cursor: decoder.u64("from_cursor")?,
            resulting_cursor: decoder.u64("resulting_cursor")?,
            base_log_head: decoder.u64("base_log_head")?,
            extension_sequence: decoder.u64("extension_sequence")?,
        }),
        value => {
            return Err(OutboxLogError::UnknownTag {
                domain: "row-kind",
                value,
            });
        }
    };
    decoder.finish()?;
    Ok(row)
}

fn encode_projected_record(
    encoder: &mut Encoder,
    record: &ProjectedRecord,
) -> Result<(), OutboxLogError> {
    encoder.u64(record.delivery_seq);
    encode_record_body(encoder, &record.body)?;
    encoder.length("recipients", record.recipients.len())?;
    for recipient in &record.recipients {
        encoder.u64(*recipient);
    }
    encode_optional_participant(encoder, record.sender);
    encoder.u64(record.encoded_push_bytes);
    Ok(())
}

fn decode_projected_record(decoder: &mut Decoder<'_>) -> Result<ProjectedRecord, OutboxLogError> {
    let delivery_seq = decoder.u64("delivery_seq")?;
    let body = decode_record_body(decoder)?;
    let recipient_count = decoder.length("recipients")?;
    let mut recipients = Vec::with_capacity(recipient_count);
    for _ in 0..recipient_count {
        recipients.push(decoder.u64("recipient")?);
    }
    let sender = decode_optional_participant(decoder)?;
    let encoded_push_bytes = decoder.u64("encoded_push_bytes")?;
    Ok(ProjectedRecord {
        delivery_seq,
        body,
        recipients,
        sender,
        encoded_push_bytes,
    })
}

fn encode_record_body(
    encoder: &mut Encoder,
    body: &ParticipantRecord,
) -> Result<(), OutboxLogError> {
    match body {
        ParticipantRecord::OrdinaryRecord {
            sender_participant_id,
            payload,
        } => {
            encoder.u8(0);
            encoder.u64(*sender_participant_id);
            encoder.length("ordinary payload", payload.len())?;
            encoder.bytes(payload);
        }
        ParticipantRecord::Attached {
            affected_participant_id,
            binding_epoch,
        } => {
            encoder.u8(1);
            encoder.u64(*affected_participant_id);
            encode_epoch(encoder, *binding_epoch);
        }
        ParticipantRecord::Detached {
            affected_participant_id,
            binding_epoch,
            cause,
        } => {
            encoder.u8(2);
            encoder.u64(*affected_participant_id);
            encode_epoch(encoder, *binding_epoch);
            encoder.u8(match cause {
                DetachedCause::CleanDeregister => 0,
                DetachedCause::Superseded => 1,
                DetachedCause::ServerShutdown => 2,
            });
        }
        ParticipantRecord::Died {
            affected_participant_id,
            binding_epoch,
            cause,
        } => {
            encoder.u8(3);
            encoder.u64(*affected_participant_id);
            encode_epoch(encoder, *binding_epoch);
            match cause {
                DiedCause::ConnectionLost => encoder.u8(0),
                DiedCause::ProcessKilled => encoder.u8(1),
                DiedCause::ProtocolError => encoder.u8(2),
                DiedCause::UncleanServerRestart {
                    prior_server_incarnation,
                } => {
                    encoder.u8(3);
                    encoder.u64(*prior_server_incarnation);
                }
            }
        }
        ParticipantRecord::Left {
            affected_participant_id,
            ended_binding_epoch,
        } => {
            encoder.u8(4);
            encoder.u64(*affected_participant_id);
            encode_optional_epoch(encoder, *ended_binding_epoch);
        }
        ParticipantRecord::HistoryCompacted {
            affected_participant_id,
            abandoned_after,
            abandoned_through,
            physical_floor_at_decision,
        } => {
            encoder.u8(5);
            encoder.u64(*affected_participant_id);
            encoder.u64(*abandoned_after);
            encoder.u64(*abandoned_through);
            encoder.u64(*physical_floor_at_decision);
        }
    }
    Ok(())
}

fn decode_record_body(decoder: &mut Decoder<'_>) -> Result<ParticipantRecord, OutboxLogError> {
    match decoder.u8("record_kind")? {
        0 => {
            let sender_participant_id = decoder.u64("sender_participant_id")?;
            let payload_length = decoder.length("ordinary payload")?;
            let payload = decoder.bytes("ordinary payload", payload_length)?.to_vec();
            Ok(ParticipantRecord::OrdinaryRecord {
                sender_participant_id,
                payload,
            })
        }
        1 => Ok(ParticipantRecord::Attached {
            affected_participant_id: decoder.u64("affected_participant_id")?,
            binding_epoch: decode_epoch(decoder)?,
        }),
        2 => decode_detached(decoder),
        3 => decode_died(decoder),
        4 => Ok(ParticipantRecord::Left {
            affected_participant_id: decoder.u64("affected_participant_id")?,
            ended_binding_epoch: decode_optional_epoch(decoder)?,
        }),
        5 => Ok(ParticipantRecord::HistoryCompacted {
            affected_participant_id: decoder.u64("affected_participant_id")?,
            abandoned_after: decoder.u64("abandoned_after")?,
            abandoned_through: decoder.u64("abandoned_through")?,
            physical_floor_at_decision: decoder.u64("physical_floor_at_decision")?,
        }),
        value => Err(OutboxLogError::UnknownTag {
            domain: "record-kind",
            value,
        }),
    }
}

fn decode_detached(decoder: &mut Decoder<'_>) -> Result<ParticipantRecord, OutboxLogError> {
    let affected_participant_id = decoder.u64("affected_participant_id")?;
    let binding_epoch = decode_epoch(decoder)?;
    let cause = match decoder.u8("detached_cause")? {
        0 => DetachedCause::CleanDeregister,
        1 => DetachedCause::Superseded,
        2 => DetachedCause::ServerShutdown,
        value => {
            return Err(OutboxLogError::UnknownTag {
                domain: "detached-cause",
                value,
            });
        }
    };
    Ok(ParticipantRecord::Detached {
        affected_participant_id,
        binding_epoch,
        cause,
    })
}

fn decode_died(decoder: &mut Decoder<'_>) -> Result<ParticipantRecord, OutboxLogError> {
    let affected_participant_id = decoder.u64("affected_participant_id")?;
    let binding_epoch = decode_epoch(decoder)?;
    let cause = match decoder.u8("died_cause")? {
        0 => DiedCause::ConnectionLost,
        1 => DiedCause::ProcessKilled,
        2 => DiedCause::ProtocolError,
        3 => DiedCause::UncleanServerRestart {
            prior_server_incarnation: decoder.u64("prior_server_incarnation")?,
        },
        value => {
            return Err(OutboxLogError::UnknownTag {
                domain: "died-cause",
                value,
            });
        }
    };
    Ok(ParticipantRecord::Died {
        affected_participant_id,
        binding_epoch,
        cause,
    })
}

pub(super) fn canonical_push_bytes(
    conversation_id: u64,
    delivery_seq: u64,
    body: &ParticipantRecord,
) -> Result<u64, OutboxLogError> {
    let frame =
        ParticipantFrame::ServerPush(ServerPush::ParticipantDelivery(ParticipantDelivery {
            conversation_id,
            delivery_seq,
            record: body.clone(),
        }));
    let bytes = encoded_len(&frame).map_err(OutboxLogError::PushCodec)?;
    u64::try_from(bytes).map_err(|_| OutboxLogError::LengthOverflow {
        field: "encoded participant push",
        length: bytes,
    })
}

const fn source_kind_tag(kind: ProducedSourceKind) -> u8 {
    match kind {
        ProducedSourceKind::Enrolled => 0,
        ProducedSourceKind::Attached => 1,
        ProducedSourceKind::Detached => 2,
        ProducedSourceKind::MarkerDrained => 3,
        ProducedSourceKind::RecordAdmission => 4,
        ProducedSourceKind::Left => 5,
    }
}

const fn decode_source_kind(value: u8) -> Result<ProducedSourceKind, OutboxLogError> {
    match value {
        0 => Ok(ProducedSourceKind::Enrolled),
        1 => Ok(ProducedSourceKind::Attached),
        2 => Ok(ProducedSourceKind::Detached),
        3 => Ok(ProducedSourceKind::MarkerDrained),
        4 => Ok(ProducedSourceKind::RecordAdmission),
        5 => Ok(ProducedSourceKind::Left),
        _ => Err(OutboxLogError::UnknownTag {
            domain: "source-kind",
            value,
        }),
    }
}

fn encode_marker_ack(encoder: &mut Encoder, request: &MarkerAck) {
    encoder.u64(request.conversation_id);
    encoder.u64(request.participant_id);
    encoder.u64(request.capability_generation.get());
    encoder.u64(request.marker_delivery_seq);
}

fn decode_marker_ack(decoder: &mut Decoder<'_>) -> Result<MarkerAck, OutboxLogError> {
    Ok(MarkerAck {
        conversation_id: decoder.u64("conversation_id")?,
        participant_id: decoder.u64("participant_id")?,
        capability_generation: decode_generation(decoder.u64("capability_generation")?)?,
        marker_delivery_seq: decoder.u64("marker_delivery_seq")?,
    })
}

fn encode_epoch(encoder: &mut Encoder, epoch: BindingEpoch) {
    encoder.u64(epoch.connection_incarnation.server_incarnation);
    encoder.u64(epoch.connection_incarnation.connection_ordinal);
    encoder.u64(epoch.capability_generation.get());
}

fn decode_epoch(decoder: &mut Decoder<'_>) -> Result<BindingEpoch, OutboxLogError> {
    let server_incarnation = decoder.u64("server_incarnation")?;
    let connection_ordinal = decoder.u64("connection_ordinal")?;
    let generation = decode_generation(decoder.u64("capability_generation")?)?;
    Ok(BindingEpoch::new(
        ConnectionIncarnation::new(server_incarnation, connection_ordinal),
        generation,
    ))
}

fn encode_optional_epoch(encoder: &mut Encoder, epoch: Option<BindingEpoch>) {
    match epoch {
        Some(value) => {
            encoder.u8(1);
            encode_epoch(encoder, value);
        }
        None => encoder.u8(0),
    }
}

fn decode_optional_epoch(
    decoder: &mut Decoder<'_>,
) -> Result<Option<BindingEpoch>, OutboxLogError> {
    match decoder.u8("optional binding epoch")? {
        0 => Ok(None),
        1 => decode_epoch(decoder).map(Some),
        value => Err(OutboxLogError::InvalidSelector {
            field: "optional binding epoch",
            value,
        }),
    }
}

fn encode_optional_participant(encoder: &mut Encoder, participant: Option<u64>) {
    match participant {
        Some(value) => {
            encoder.u8(1);
            encoder.u64(value);
        }
        None => encoder.u8(0),
    }
}

fn decode_optional_participant(decoder: &mut Decoder<'_>) -> Result<Option<u64>, OutboxLogError> {
    match decoder.u8("optional sender")? {
        0 => Ok(None),
        1 => decoder.u64("sender").map(Some),
        value => Err(OutboxLogError::InvalidSelector {
            field: "optional sender",
            value,
        }),
    }
}

fn decode_generation(value: u64) -> Result<Generation, OutboxLogError> {
    Generation::new(value).ok_or(OutboxLogError::ZeroGeneration)
}
