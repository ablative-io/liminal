//! Move-only outbox restore, idempotency, retention, and Leave discharge.

use std::error::Error;
use std::io::Write;

use liminal::protocol::{encode, encoded_len};
use liminal_protocol::lifecycle::{
    LiveFrontierOwner, RecipientAckObligations, RecordAdmissionCommit,
};
use liminal_protocol::wire::{
    BindingEpoch, ConnectionIncarnation, Generation, MarkerAck, ParticipantRecord, ServerPush,
};

use crate::server::participant::encode_server_push;

use super::outbox::{ConversationOutbox, ConversationOutboxError, ConversationOutboxLimits};
use super::outbox_log::{
    OutboxRow, ProducedBatch, ProducedSourceKind, ProjectedRecord, StoredMarkerAckCommitted,
};
use super::outbox_projection::ReplayedProjectionFacts;
use super::outbox_replay::ExtensionMerge;

const CONVERSATION: u64 = 0xF0_C3;
const SENDER: u64 = 3;
const RECIPIENT: u64 = 7;

fn test_limits() -> Result<ConversationOutboxLimits, ConversationOutboxError> {
    ConversationOutboxLimits::try_new(64, 64)
}

fn restore_owner(
    rows: Vec<(u64, OutboxRow)>,
) -> Result<ConversationOutbox, ConversationOutboxError> {
    ConversationOutbox::restore(CONVERSATION, rows, test_limits()?)
}

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
    let owner = restore_owner(vec![(0, exact.clone()), (1, exact.clone())])?;
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
        let result = restore_owner(vec![
            (0, OutboxRow::Produced(base_batch.clone())),
            (1, mutation),
        ]);
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
    let owner = restore_owner(vec![(0, produced), (1, ack)])?;
    assert_eq!(owner.ack_through(RECIPIENT), 1);
    assert_eq!(owner.next_live(RECIPIENT), None);
    assert_eq!(owner.next_live(RECIPIENT + 1), Some(1));
    assert_eq!(owner.live_record_count(), 1);
    assert!(owner.charged_bytes() > 0);
    Ok(())
}

#[test]
fn literal_v2_nonobligation_ack_restores_while_regression_refuses() -> Result<(), Box<dyn Error>> {
    let produced = ordinary(0, 1, vec![RECIPIENT])?;
    let historical = OutboxRow::AckAdvanced {
        source_log_sequence: 1,
        participant_id: RECIPIENT,
        through_seq: 2,
    };
    let historical_owner = restore_owner(vec![(0, produced.clone()), (1, historical)])?;
    assert_eq!(historical_owner.ack_through(RECIPIENT), 2);
    assert_eq!(historical_owner.live_record_count(), 0);

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
        restore_owner(vec![(0, produced), (1, ack), (2, repeated)]),
        Err(ConversationOutboxError::AckRegression { through_seq: 1, .. })
    ));
    Ok(())
}

#[test]
fn leave_atomically_discharges_retired_obligations_and_replays() -> Result<(), Box<dyn Error>> {
    let payload = ordinary(0, 1, vec![RECIPIENT])?;
    let leave = left(1, 2, RECIPIENT)?;
    let rows = vec![(0, payload), (1, leave)];
    let first = restore_owner(rows.clone())?;
    assert_eq!(first.source_batch_count(), 2);
    assert_eq!(first.live_record_count(), 0);
    assert_eq!(first.charged_bytes(), 0);
    assert_eq!(first.next_live(RECIPIENT), None);

    let second = restore_owner(rows)?;
    assert_eq!(second.source_batch_count(), first.source_batch_count());
    assert_eq!(
        second.next_extension_sequence(),
        first.next_extension_sequence()
    );
    assert_eq!(second.charged_bytes(), first.charged_bytes());
    Ok(())
}

#[test]
fn participant_ack_only_advances_receipt_frontier() -> Result<(), Box<dyn Error>> {
    let first = ordinary(0, 1, vec![RECIPIENT])?;
    let third = ordinary(1, 3, vec![RECIPIENT])?;
    let before_rows = vec![(0, first.clone()), (1, third.clone())];
    let before = restore_owner(before_rows.clone())?;
    let before_state = (
        before.ack_through(RECIPIENT),
        before.next_live(RECIPIENT),
        before.live_record_count(),
        before.charged_bytes(),
        before.source_batch_count(),
    );
    assert_eq!(before_state.0, 0);
    assert_eq!(before_state.1, Some(1));

    // Select, encode, enqueue, and write the real production frame. These are
    // transport-progress facts only: rebuilding from the same durable rows must
    // expose byte-for-byte the same head and every receipt-owned fact.
    let offered = before
        .delivery_after(RECIPIENT, 0)
        .ok_or("recipient obligation disappeared before offer")?;
    let frame = encode_server_push(ServerPush::ParticipantDelivery(offered.clone()))
        .map_err(|error| format!("push encoding failed: {error:?}"))?;
    let mut encoded = vec![0; encoded_len(&frame)?];
    let written = encode(&frame, &mut encoded)?;
    encoded.truncate(written);
    let mut socket_bytes = Vec::new();
    socket_bytes.write_all(&encoded)?;
    assert_eq!(socket_bytes, encoded);

    let after_write = restore_owner(before_rows.clone())?;
    assert_eq!(after_write.delivery_after(RECIPIENT, 0), Some(offered));
    assert_eq!(
        (
            after_write.ack_through(RECIPIENT),
            after_write.next_live(RECIPIENT),
            after_write.live_record_count(),
            after_write.charged_bytes(),
            after_write.source_batch_count(),
        ),
        before_state
    );

    // Sequence two is a real conversation gap for this recipient. The exact
    // cumulative endpoint three is nevertheless eligible and releases both
    // obligations through it in one committed AckAdvanced row.
    assert_eq!(
        after_write
            .delivery_after(RECIPIENT, 1)
            .map(|d| d.delivery_seq),
        Some(3)
    );
    let committed = restore_owner(vec![
        (0, first),
        (1, third),
        (
            2,
            OutboxRow::AckAdvanced {
                source_log_sequence: 2,
                participant_id: RECIPIENT,
                through_seq: 3,
            },
        ),
    ])?;
    assert_eq!(committed.ack_through(RECIPIENT), 3);
    assert_eq!(committed.next_live(RECIPIENT), None);
    assert_eq!(committed.live_record_count(), 0);
    assert_eq!(committed.charged_bytes(), 0);

    // Repeated/no-op and regression rows are rejected atomically. A gap endpoint
    // has no source row at all; restoring the unchanged durable prefix retains
    // the original frontier and both obligations.
    for through_seq in [3, 2] {
        let mut rows = before_rows.clone();
        rows.push((
            2,
            OutboxRow::AckAdvanced {
                source_log_sequence: 2,
                participant_id: RECIPIENT,
                through_seq: 3,
            },
        ));
        rows.push((
            3,
            OutboxRow::AckAdvanced {
                source_log_sequence: 3,
                participant_id: RECIPIENT,
                through_seq,
            },
        ));
        assert!(matches!(
            restore_owner(rows),
            Err(ConversationOutboxError::AckRegression { .. })
        ));
    }
    let gap_unchanged = restore_owner(before_rows)?;
    assert_eq!(gap_unchanged.ack_through(RECIPIENT), 0);
    assert_eq!(gap_unchanged.next_live(RECIPIENT), Some(1));
    Ok(())
}

pub(super) fn assert_live_recipient_obligation_bound_holds_without_mutation_and_owner_continues()
-> Result<(), Box<dyn Error>> {
    let limits = ConversationOutboxLimits::try_new(1, 2)?;
    let mut owner = ConversationOutbox::restore(CONVERSATION, Vec::new(), limits)?;
    owner.apply_row(0, ordinary(0, 1, vec![RECIPIENT, RECIPIENT + 1])?)?;
    assert_eq!(owner.live_recipient_obligation_count(), 2);

    let Err(error) = owner.apply_row(1, ordinary(1, 2, vec![RECIPIENT + 2])?) else {
        return Err("third live recipient obligation exceeded the signed bound".into());
    };
    assert!(matches!(
        error,
        ConversationOutboxError::LiveRecipientObligationsExceeded {
            limit: 2,
            attempted: 3
        }
    ));
    assert_eq!(owner.next_extension_sequence(), 1);
    assert_eq!(owner.source_batch_count(), 1);
    assert_eq!(owner.live_recipient_obligation_count(), 2);
    assert!(owner.delivery_after(RECIPIENT, 0).is_some());

    owner.apply_row(
        1,
        OutboxRow::AckAdvanced {
            source_log_sequence: 2,
            participant_id: RECIPIENT,
            through_seq: 1,
        },
    )?;
    assert_eq!(owner.live_recipient_obligation_count(), 1);
    owner.apply_row(2, ordinary(1, 2, vec![RECIPIENT + 2])?)?;
    assert_eq!(owner.live_recipient_obligation_count(), 2);
    assert_eq!(owner.next_extension_sequence(), 3);
    assert!(owner.delivery_after(RECIPIENT + 2, 0).is_some());
    Ok(())
}

#[test]
fn live_recipient_obligation_bound_holds_without_mutation_and_owner_continues()
-> Result<(), Box<dyn Error>> {
    assert_live_recipient_obligation_bound_holds_without_mutation_and_owner_continues()
}

#[test]
fn live_recipient_obligation_bound_rejects_checked_product_overflow() {
    assert!(matches!(
        ConversationOutboxLimits::try_new(u64::MAX, 2),
        Err(ConversationOutboxError::BoundOverflow {
            name: "UNIT2_MAX_LIVE_RECIPIENT_OBLIGATIONS"
        })
    ));
}

#[test]
fn marker_cursor_provenance_reconciles_without_consuming_outbox_state() -> Result<(), Box<dyn Error>>
{
    let marker_cursor = 10;
    let offered_cursor = marker_cursor + 2;
    let binding_epoch = BindingEpoch::new(ConnectionIncarnation::new(4, 2), Generation::ONE);
    let marker = OutboxRow::MarkerAckCommitted(StoredMarkerAckCommitted {
        request: MarkerAck {
            conversation_id: CONVERSATION,
            participant_id: RECIPIENT,
            capability_generation: Generation::ONE,
            marker_delivery_seq: marker_cursor,
        },
        receiving_binding_epoch: binding_epoch,
        offered_marker_delivery_seq: marker_cursor,
        delivered_binding_epoch: binding_epoch,
        from_cursor: 0,
        resulting_cursor: marker_cursor,
        base_log_head: 0,
        extension_sequence: 0,
    });
    let rows = vec![(0, marker)];
    let live = restore_owner(rows.clone())?;
    let cold = restore_owner(rows)?;
    let before = (
        live.next_extension_sequence(),
        live.ack_through(RECIPIENT),
        live.live_record_count(),
        live.live_recipient_obligation_count(),
        live.charged_bytes(),
    );

    assert_eq!(
        live.dispatch_after(RECIPIENT, marker_cursor, Some(offered_cursor))?,
        offered_cursor
    );
    assert_eq!(
        cold.dispatch_after(RECIPIENT, marker_cursor, None)?,
        marker_cursor
    );
    assert!(matches!(
        live.dispatch_after(RECIPIENT, marker_cursor + 1, None),
        Err(ConversationOutboxError::ProtocolCursorProvenance { .. })
    ));
    assert_eq!(
        (
            live.next_extension_sequence(),
            live.ack_through(RECIPIENT),
            live.live_record_count(),
            live.live_recipient_obligation_count(),
            live.charged_bytes(),
        ),
        before
    );
    Ok(())
}

macro_rules! assert_not_impl {
    ($type:ty: $trait:path) => {
        const _: fn() = || {
            struct Probe<T: ?Sized>(core::marker::PhantomData<T>);
            trait AmbiguousIfImplemented<A> {
                fn probe() {}
            }
            impl<T: ?Sized> AmbiguousIfImplemented<()> for Probe<T> {}
            impl<T: ?Sized + $trait> AmbiguousIfImplemented<u8> for Probe<T> {}
            let _ = <Probe<$type> as AmbiguousIfImplemented<_>>::probe;
        };
    };
}

#[test]
fn participant_outbox_owner_is_move_only() {
    crate::server::connection::assert_held_heads_are_move_only();
    assert_not_impl!(ConversationOutbox: Clone);
    assert_not_impl!(ConversationOutbox: Copy);
    assert_not_impl!(LiveFrontierOwner: Clone);
    assert_not_impl!(LiveFrontierOwner: Copy);
    assert_not_impl!(RecipientAckObligations: Clone);
    assert_not_impl!(RecipientAckObligations: Copy);
    assert_not_impl!(RecordAdmissionCommit: Clone);
    assert_not_impl!(RecordAdmissionCommit: Copy);
    assert_not_impl!(ReplayedProjectionFacts: Clone);
    assert_not_impl!(ReplayedProjectionFacts: Copy);
    assert_not_impl!(ExtensionMerge<'static>: Clone);
    assert_not_impl!(ExtensionMerge<'static>: Copy);
}
