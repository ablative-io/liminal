use super::super::log::StoredBindingEpoch;
use super::*;

pub(super) fn base_rows(
    store: Arc<dyn DurableStore>,
    conversation_id: u64,
) -> Result<Vec<(u64, StoredOperation)>, Box<dyn Error>> {
    let log = OperationLog::new(store, conversation_id);
    let mut rows = Vec::new();
    let mut next = 0;
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
            .ok_or("nonempty base page lost its tail")?;
        for decoded in page.rows {
            let DecodedStoredOperation::V3(operation) = decoded.operation else {
                return Err("new Unit 2 fixture unexpectedly decoded a v2 row".into());
            };
            rows.push((decoded.sequence, operation));
        }
    }
    Ok(rows)
}

pub(super) fn extension_rows(
    store: Arc<dyn DurableStore>,
    conversation_id: u64,
) -> Result<Vec<(u64, OutboxRow)>, Box<dyn Error>> {
    Ok(block_on(
        OutboxLog::new(store, conversation_id).read_all(),
    )??)
}

pub(super) fn assert_primary_delivery_mapping(
    base: &[(u64, StoredOperation)],
    extension: &[(u64, OutboxRow)],
) -> Result<(), Box<dyn Error>> {
    let by_source = rows_by_source(extension);
    let mut seen = BTreeSet::new();
    for (source, operation) in base {
        match operation {
            StoredOperation::Detached { row } => {
                let (
                    StoredTerminalDisposition::Committed { terminal_seq },
                    StoredDetachedSource::ExplicitRequestCommitted {
                        request,
                        receiving_epoch,
                        ..
                    },
                ) = (&row.disposition, &row.source)
                else {
                    return Err("fixture Detached row was not explicit committed".into());
                };
                let batch = only_produced(&by_source, *source)?;
                assert_eq!(batch.source_kind(), ProducedSourceKind::Detached);
                let [record] = batch.ordered_records() else {
                    return Err("detach did not map to one record".into());
                };
                assert_eq!(record.delivery_seq(), *terminal_seq);
                assert_eq!(
                    record.body(),
                    &ParticipantRecord::Detached {
                        affected_participant_id: request.participant_id,
                        binding_epoch: receiving_epoch.to_epoch()?,
                        cause: DetachedCause::CleanDeregister,
                    }
                );
                seen.insert("detached");
            }
            StoredOperation::ZeroDebtAck { request, .. } => {
                let rows = by_source
                    .get(source)
                    .ok_or("ack source had no extension row")?;
                assert!(matches!(
                    rows.as_slice(),
                    [OutboxRow::AckAdvanced {
                        participant_id,
                        through_seq,
                        ..
                    }] if *participant_id == request.participant_id && *through_seq == request.through_seq
                ));
                seen.insert("zero_debt_ack");
            }
            StoredOperation::RecordAdmission { row } => {
                let batch = only_produced(&by_source, *source)?;
                assert_eq!(batch.source_kind(), ProducedSourceKind::RecordAdmission);
                let [record] = batch.ordered_records() else {
                    return Err("record admission did not map to one record".into());
                };
                assert_eq!(record.delivery_seq(), row.delivery_seq);
                assert_eq!(
                    record.body(),
                    &ParticipantRecord::OrdinaryRecord {
                        sender_participant_id: row.request.participant_id,
                        payload: row.request.payload.clone(),
                    }
                );
                seen.insert("record_admission");
            }
            StoredOperation::Left { row } => {
                let batch = only_produced(&by_source, *source)?;
                assert_eq!(batch.source_kind(), ProducedSourceKind::Left);
                let [record] = batch.ordered_records() else {
                    return Err("leave did not map to one record".into());
                };
                assert_eq!(record.delivery_seq(), row.left_delivery_seq);
                assert_eq!(
                    record.body(),
                    &ParticipantRecord::Left {
                        affected_participant_id: row.request.participant_id,
                        ended_binding_epoch: row
                            .ended_binding_epoch
                            .map(StoredBindingEpoch::to_epoch)
                            .transpose()?,
                    }
                );
                seen.insert("left");
            }
            _ => {}
        }
    }
    assert_eq!(
        seen,
        BTreeSet::from(["detached", "left", "record_admission", "zero_debt_ack"])
    );
    Ok(())
}
