use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal::durability::{DurableStore, open_ephemeral};
use liminal_protocol::wire::{
    AttachAttemptToken, AttachBound, AttachSecret, ClientRequest, ConnectionIncarnation,
    CredentialAttachRequest, DetachAttemptToken, DetachRequest, DetachedCause, EnrollBound,
    EnrollmentRequest, EnrollmentToken, Generation, LeaveAttemptToken, LeaveRequest,
    ParticipantAck, ParticipantRecord, RecordAdmission, RecordAdmissionAttemptToken, ServerValue,
};

use super::ProductionParticipantHandler;
use super::log::{
    DecodedStoredOperation, OperationLog, OperationSchemaPhase, StoredAttachModeV3,
    StoredDetachedSource, StoredOperation, StoredTerminalDisposition,
};
use super::outbox_log::{OutboxLog, OutboxRow, ProducedBatch, ProducedSourceKind};
use super::tests::{dispatch, test_participant_config};
use super::tests_marker_ack::{commit_exact_marker_ack, record_exact_marker_offer};
use super::tests_marker_ack_fixture::prepare_marker_fixture;

#[path = "tests_unit2_log_rows.rs"]
mod log_rows;
use log_rows::{assert_primary_delivery_mapping, base_rows, extension_rows};
#[derive(Clone, Copy)]
struct Member {
    connection: ConnectionIncarnation,
    participant_id: u64,
    generation: Generation,
    secret: AttachSecret,
}

fn enroll(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    connection: ConnectionIncarnation,
    token: u8,
) -> Result<Member, Box<dyn Error>> {
    let value = dispatch(
        handler,
        connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([token; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(bound) = value else {
        return Err(format!("enrollment {token} did not bind: {value:?}").into());
    };
    Ok(member_from_enroll(connection, &bound))
}

fn member_from_enroll(connection: ConnectionIncarnation, bound: &EnrollBound) -> Member {
    Member {
        connection,
        participant_id: bound.participant_id(),
        generation: Generation::ONE,
        secret: bound.attach_secret(),
    }
}

fn detach(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    member: Member,
    token: u8,
) -> Result<(), Box<dyn Error>> {
    let value = dispatch(
        handler,
        member.connection,
        ClientRequest::Detach(DetachRequest {
            conversation_id,
            participant_id: member.participant_id,
            capability_generation: member.generation,
            detach_attempt_token: DetachAttemptToken::new([token; 16]),
        }),
    )?;
    if !matches!(value, ServerValue::DetachCommitted(_)) {
        return Err(format!("detach {token} did not commit: {value:?}").into());
    }
    Ok(())
}

fn attach(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    member: Member,
    connection: ConnectionIncarnation,
    token: u8,
) -> Result<Member, Box<dyn Error>> {
    let value = dispatch(
        handler,
        connection,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id,
            participant_id: member.participant_id,
            capability_generation: member.generation,
            attach_secret: member.secret,
            attach_attempt_token: AttachAttemptToken::new([token; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    let ServerValue::AttachBound(bound) = value else {
        return Err(format!("attach {token} did not bind: {value:?}").into());
    };
    Ok(member_from_attach(
        connection,
        member.participant_id,
        &bound,
    ))
}

fn member_from_attach(
    connection: ConnectionIncarnation,
    participant_id: u64,
    bound: &AttachBound,
) -> Member {
    Member {
        connection,
        participant_id,
        generation: bound.capability_generation(),
        secret: bound.attach_secret(),
    }
}

fn admit(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    member: Member,
    token: u8,
    payload: Vec<u8>,
) -> Result<u64, Box<dyn Error>> {
    let value = dispatch(
        handler,
        member.connection,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id,
            participant_id: member.participant_id,
            capability_generation: member.generation,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([token; 16]),
            payload,
        }),
    )?;
    let ServerValue::RecordCommitted(committed) = value else {
        return Err(format!("record {token} did not commit: {value:?}").into());
    };
    Ok(committed.delivery_seq())
}

fn ack(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    member: Member,
    through_seq: u64,
) -> Result<(), Box<dyn Error>> {
    let value = dispatch(
        handler,
        member.connection,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id,
            participant_id: member.participant_id,
            capability_generation: member.generation,
            through_seq,
        }),
    )?;
    if !matches!(value, ServerValue::AckCommitted(_)) {
        return Err(format!("ack through {through_seq} did not commit: {value:?}").into());
    }
    Ok(())
}

fn leave(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    member: Member,
    token: u8,
) -> Result<(), Box<dyn Error>> {
    let value = dispatch(
        handler,
        member.connection,
        ClientRequest::Leave(LeaveRequest {
            conversation_id,
            participant_id: member.participant_id,
            capability_generation: member.generation,
            attach_secret: member.secret,
            leave_attempt_token: LeaveAttemptToken::new([token; 16]),
        }),
    )?;
    if !matches!(value, ServerValue::LeaveCommitted(_)) {
        return Err(format!("leave {token} did not commit: {value:?}").into());
    }
    Ok(())
}

fn rows_by_source(rows: &[(u64, OutboxRow)]) -> BTreeMap<u64, Vec<&OutboxRow>> {
    let mut by_source = BTreeMap::new();
    for (_, row) in rows {
        let source = match row {
            OutboxRow::Produced(batch) => Some(batch.source_log_sequence()),
            OutboxRow::AckAdvanced {
                source_log_sequence,
                ..
            } => Some(*source_log_sequence),
            OutboxRow::MarkerAckCommitted(_) => None,
        };
        if let Some(source) = source {
            by_source.entry(source).or_insert_with(Vec::new).push(row);
        }
    }
    by_source
}

fn only_produced<'a>(
    by_source: &'a BTreeMap<u64, Vec<&OutboxRow>>,
    source: u64,
) -> Result<&'a ProducedBatch, Box<dyn Error>> {
    let rows = by_source
        .get(&source)
        .ok_or_else(|| format!("source {source} had no extension row"))?;
    let [OutboxRow::Produced(batch)] = rows.as_slice() else {
        return Err(format!("source {source} did not map to exactly one Produced row").into());
    };
    Ok(batch)
}

fn assert_primary_membership_mapping(
    base: &[(u64, StoredOperation)],
    extension: &[(u64, OutboxRow)],
) -> Result<(), Box<dyn Error>> {
    let by_source = rows_by_source(extension);
    let mut enrolled_count = 0;
    let mut ordinary_attach = 0;
    let mut superseding_attach = 0;
    let mut seen = BTreeSet::new();
    for (source, operation) in base {
        match operation {
            StoredOperation::Genesis { .. } => {
                assert!(!by_source.contains_key(source));
                seen.insert("genesis");
            }
            StoredOperation::Enrolled { allocation, .. } => {
                enrolled_count += 1;
                let batch = only_produced(&by_source, *source)?;
                assert_eq!(batch.source_kind(), ProducedSourceKind::Enrolled);
                let [record] = batch.ordered_records() else {
                    return Err("enrollment did not map to one record".into());
                };
                assert_eq!(record.delivery_seq(), allocation.attached_seq);
                assert_eq!(
                    record.body(),
                    &ParticipantRecord::Attached {
                        affected_participant_id: allocation.participant_id,
                        binding_epoch: allocation.origin_epoch.to_epoch()?,
                    }
                );
                seen.insert("enrolled");
            }
            StoredOperation::Attached {
                request,
                allocation,
                mode,
                ..
            } => {
                let batch = only_produced(&by_source, *source)?;
                assert_eq!(batch.source_kind(), ProducedSourceKind::Attached);
                match (mode.as_ref(), batch.ordered_records()) {
                    (StoredAttachModeV3::Ordinary, [attached]) => {
                        ordinary_attach += 1;
                        assert_eq!(attached.delivery_seq(), allocation.attached_seq);
                        assert_eq!(
                            attached.body(),
                            &ParticipantRecord::Attached {
                                affected_participant_id: request.participant_id,
                                binding_epoch: allocation.binding_epoch.to_epoch()?,
                            }
                        );
                    }
                    (
                        StoredAttachModeV3::Superseding {
                            terminal_delivery_seq,
                            ..
                        },
                        [terminal, attached],
                    ) => {
                        superseding_attach += 1;
                        assert_eq!(terminal.delivery_seq(), *terminal_delivery_seq);
                        assert!(matches!(
                            terminal.body(),
                            ParticipantRecord::Detached {
                                affected_participant_id,
                                cause: DetachedCause::Superseded,
                                ..
                            } if *affected_participant_id == request.participant_id
                        ));
                        assert_eq!(attached.delivery_seq(), allocation.attached_seq);
                        assert_eq!(
                            terminal.delivery_seq().checked_add(1),
                            Some(attached.delivery_seq())
                        );
                        assert_eq!(
                            attached.body(),
                            &ParticipantRecord::Attached {
                                affected_participant_id: request.participant_id,
                                binding_epoch: allocation.binding_epoch.to_epoch()?,
                            }
                        );
                    }
                    _ => {
                        return Err(
                            "attach source batch count/order disagreed with allocation".into()
                        );
                    }
                }
                seen.insert("attached");
            }
            _ => {}
        }
    }
    assert_eq!(
        enrolled_count, 2,
        "initial and subsequent enrollment both mapped"
    );
    assert_eq!(ordinary_attach, 1);
    assert_eq!(superseding_attach, 1);
    assert_eq!(seen, BTreeSet::from(["attached", "enrolled", "genesis"]));
    Ok(())
}

fn assert_primary_mapping(
    base: &[(u64, StoredOperation)],
    extension: &[(u64, OutboxRow)],
) -> Result<(), Box<dyn Error>> {
    assert_primary_membership_mapping(base, extension)?;
    assert_primary_delivery_mapping(base, extension)
}

#[test]
fn every_v2_source_maps_exhaustively_in_sequence_order() -> Result<(), Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let conversation_id = 0xF0_51;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config())?;
    let first = enroll(
        &handler,
        conversation_id,
        ConnectionIncarnation::new(0x51, 1),
        0x11,
    )?;
    let mut second = enroll(
        &handler,
        conversation_id,
        ConnectionIncarnation::new(0x51, 2),
        0x12,
    )?;
    detach(&handler, conversation_id, second, 0x13)?;
    second = attach(
        &handler,
        conversation_id,
        second,
        ConnectionIncarnation::new(0x51, 3),
        0x14,
    )?;
    second = attach(
        &handler,
        conversation_id,
        second,
        ConnectionIncarnation::new(0x51, 4),
        0x15,
    )?;
    let ordinary_seq = admit(
        &handler,
        conversation_id,
        first,
        0x16,
        vec![0, 0xFF, 0xA5, 0],
    )?;
    ack(&handler, conversation_id, first, ordinary_seq - 1)?;
    ack(&handler, conversation_id, second, ordinary_seq)?;
    leave(&handler, conversation_id, second, 0x17)?;

    let base = base_rows(Arc::clone(&store), conversation_id)?;
    let extension = extension_rows(Arc::clone(&store), conversation_id)?;
    assert_primary_mapping(&base, &extension)?;

    let marker = prepare_marker_fixture()?;
    let marker_conversation = marker.marker_delivery.conversation_id;
    let marker_base = base_rows(Arc::clone(&marker.store), marker_conversation)?;
    let marker_extension = extension_rows(Arc::clone(&marker.store), marker_conversation)?;
    let marker_by_source = rows_by_source(&marker_extension);
    let mut marker_sources = 0;
    for (source, operation) in marker_base {
        if matches!(operation, StoredOperation::MarkerDrained { .. }) {
            marker_sources += 1;
            let batch = only_produced(&marker_by_source, source)?;
            assert_eq!(batch.source_kind(), ProducedSourceKind::MarkerDrained);
            let [record] = batch.ordered_records() else {
                return Err("marker drain did not map to one record".into());
            };
            assert_eq!(record.delivery_seq(), marker.marker_delivery.delivery_seq);
            assert_eq!(record.body(), &marker.marker_delivery.record);
        }
    }
    assert_eq!(marker_sources, 1);

    let produced_before = marker_extension
        .iter()
        .filter(|(_, row)| matches!(row, OutboxRow::Produced(_)))
        .count();
    record_exact_marker_offer(&marker)?;
    let _ = commit_exact_marker_ack(&marker)?;
    let after_marker_ack = extension_rows(Arc::clone(&marker.store), marker_conversation)?;
    assert_eq!(
        after_marker_ack
            .iter()
            .filter(|(_, row)| matches!(row, OutboxRow::Produced(_)))
            .count(),
        produced_before,
        "MarkerAckCommitted must map to no push source batch"
    );
    assert!(matches!(
        after_marker_ack.last(),
        Some((_, OutboxRow::MarkerAckCommitted(_)))
    ));
    Ok(())
}

#[path = "tests_unit2_recipient_snapshot.rs"]
mod recipient_snapshot;
