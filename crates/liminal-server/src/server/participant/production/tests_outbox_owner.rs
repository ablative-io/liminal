//! Move-only outbox restore, idempotency, retention, and Leave discharge.

use std::error::Error;

use liminal_protocol::wire::ParticipantRecord;

use super::outbox::{ConversationOutbox, ConversationOutboxError};
use super::outbox_log::{OutboxRow, ProducedBatch, ProducedSourceKind, ProjectedRecord};

const CONVERSATION: u64 = 0xF0_C3;
const SENDER: u64 = 3;
const RECIPIENT: u64 = 7;

fn ordinary(
    source_sequence: u64,
    delivery_sequence: u64,
    recipients: Vec<u64>,
) -> Result<OutboxRow, ConversationOutboxError> {
    let record = ProjectedRecord::try_new(
        CONVERSATION,
        delivery_sequence,
        ParticipantRecord::OrdinaryRecord {
            sender_participant_id: SENDER,
            payload: vec![1, 2, 3, 4],
        },
        recipients,
        Some(SENDER),
    )?;
    Ok(OutboxRow::Produced(ProducedBatch::new(
        source_sequence,
        ProducedSourceKind::RecordAdmission,
        vec![record],
    )))
}

fn left(
    source_sequence: u64,
    delivery_sequence: u64,
    participant_id: u64,
) -> Result<OutboxRow, ConversationOutboxError> {
    let record = ProjectedRecord::try_new(
        CONVERSATION,
        delivery_sequence,
        ParticipantRecord::Left {
            affected_participant_id: participant_id,
            ended_binding_epoch: None,
        },
        Vec::new(),
        Some(participant_id),
    )?;
    Ok(OutboxRow::Produced(ProducedBatch::new(
        source_sequence,
        ProducedSourceKind::Left,
        vec![record],
    )))
}

#[test]
fn uncertain_duplicate_source_batch_is_idempotent_only_by_exact_bytes() -> Result<(), Box<dyn Error>>
{
    let exact = ordinary(0, 1, vec![RECIPIENT])?;
    let owner =
        ConversationOutbox::restore(CONVERSATION, vec![(0, exact.clone()), (1, exact.clone())])?;
    assert_eq!(owner.next_extension_sequence(), 2);
    assert_eq!(owner.source_batch_count(), 1);
    assert_eq!(owner.live_record_count(), 1);

    let OutboxRow::Produced(base_batch) = exact else {
        return Err("ordinary fixture was not Produced".into());
    };
    let mut mutations = Vec::new();

    let mut changed_count = base_batch.clone();
    changed_count.ordered_records.clear();
    mutations.push(OutboxRow::Produced(changed_count));

    let mut changed_recipient = base_batch.clone();
    changed_recipient.ordered_records[0].recipients = vec![RECIPIENT + 1];
    mutations.push(OutboxRow::Produced(changed_recipient));

    let mut changed_body = base_batch.clone();
    changed_body.ordered_records[0].body = ParticipantRecord::OrdinaryRecord {
        sender_participant_id: SENDER,
        payload: vec![9],
    };
    mutations.push(OutboxRow::Produced(changed_body));

    let mut changed_sequence = base_batch.clone();
    changed_sequence.ordered_records[0].delivery_seq = 2;
    mutations.push(OutboxRow::Produced(changed_sequence));

    let mut changed_sender = base_batch.clone();
    changed_sender.ordered_records[0].sender = Some(SENDER + 1);
    mutations.push(OutboxRow::Produced(changed_sender));

    let mut changed_charge = base_batch.clone();
    changed_charge.ordered_records[0].encoded_push_bytes += 1;
    mutations.push(OutboxRow::Produced(changed_charge));

    for mutation in mutations {
        let result = ConversationOutbox::restore(
            CONVERSATION,
            vec![(0, OutboxRow::Produced(base_batch.clone())), (1, mutation)],
        );
        assert!(result.is_err(), "conflicting source mutation was accepted");
    }
    Ok(())
}

#[test]
fn cumulative_ack_reclaims_only_the_recipient_prefix() -> Result<(), Box<dyn Error>> {
    let produced = ordinary(0, 1, vec![RECIPIENT, RECIPIENT + 1])?;
    let ack = OutboxRow::AckAdvanced {
        source_log_sequence: 1,
        participant_id: RECIPIENT,
        through_seq: 1,
    };
    let owner = ConversationOutbox::restore(CONVERSATION, vec![(0, produced), (1, ack)])?;
    assert_eq!(owner.ack_through(RECIPIENT), 1);
    assert_eq!(owner.next_live(RECIPIENT), None);
    assert_eq!(owner.next_live(RECIPIENT + 1), Some(1));
    assert_eq!(owner.live_record_count(), 1);
    assert!(owner.charged_bytes() > 0);
    let testimony = owner.recipient_ack_obligations(RECIPIENT + 1)?;
    assert!(format!("{testimony:?}").contains("delivery_sequences: [1]"));
    Ok(())
}

#[test]
fn ack_gap_and_regression_refuse_loudly() -> Result<(), Box<dyn Error>> {
    let produced = ordinary(0, 1, vec![RECIPIENT])?;
    let gap = OutboxRow::AckAdvanced {
        source_log_sequence: 1,
        participant_id: RECIPIENT,
        through_seq: 2,
    };
    assert!(matches!(
        ConversationOutbox::restore(CONVERSATION, vec![(0, produced.clone()), (1, gap)]),
        Err(ConversationOutboxError::AckGap { through_seq: 2, .. })
    ));

    let ack = OutboxRow::AckAdvanced {
        source_log_sequence: 1,
        participant_id: RECIPIENT,
        through_seq: 1,
    };
    let repeated = OutboxRow::AckAdvanced {
        source_log_sequence: 2,
        participant_id: RECIPIENT,
        through_seq: 1,
    };
    assert!(matches!(
        ConversationOutbox::restore(CONVERSATION, vec![(0, produced), (1, ack), (2, repeated)]),
        Err(ConversationOutboxError::AckRegression { through_seq: 1, .. })
    ));
    Ok(())
}

#[test]
fn leave_atomically_discharges_retired_obligations_and_replays() -> Result<(), Box<dyn Error>> {
    let payload = ordinary(0, 1, vec![RECIPIENT])?;
    let leave = left(1, 2, RECIPIENT)?;
    let rows = vec![(0, payload), (1, leave)];
    let first = ConversationOutbox::restore(CONVERSATION, rows.clone())?;
    assert_eq!(first.source_batch_count(), 2);
    assert_eq!(first.live_record_count(), 0);
    assert_eq!(first.charged_bytes(), 0);
    assert_eq!(first.next_live(RECIPIENT), None);

    let second = ConversationOutbox::restore(CONVERSATION, rows)?;
    assert_eq!(second.source_batch_count(), first.source_batch_count());
    assert_eq!(
        second.next_extension_sequence(),
        first.next_extension_sequence()
    );
    assert_eq!(second.charged_bytes(), first.charged_bytes());
    Ok(())
}
