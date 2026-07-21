use std::error::Error;
use std::sync::Arc;

use liminal::durability::{bridge::block_on, open_ephemeral};
use liminal_protocol::lifecycle::BindingState;
use liminal_protocol::wire::{
    AttachAttemptToken, BindingEpoch, ClientRequest, ConnectionIncarnation,
    CredentialAttachRequest, DeliverySeq, EnrollmentRequest, EnrollmentToken, Generation,
    ParticipantId, ServerValue,
};

use crate::server::participant::{
    ConnectionFateClass, ConnectionFateWorkItem, ParticipantConnectionConversations,
    ParticipantSemanticHandler,
};

use super::ProductionParticipantHandler;
use super::log::{
    DecodedStoredOperation, OperationLog, StoredOperation, StoredOrdinaryTerminalSource,
    StoredSpecificFateIntent,
};
use super::tests::{dispatch_tracked, test_participant_config};

#[derive(Clone, Copy)]
struct OrdinaryCompletionExpectation {
    conversation_id: u64,
    participant_id: ParticipantId,
    attached_epoch: BindingEpoch,
    attached_source_sequence: u64,
    died_source_sequence: u64,
    observer_before: DeliverySeq,
}

#[test]
fn ordinary_completion_uses_protocol_floor_and_exact_production_caller()
-> Result<(), Box<dyn Error>> {
    let store = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let connection_incarnation = ConnectionIncarnation::new(89, 7);
    let conversation_id = 59;
    let mut conversations = ParticipantConnectionConversations::default();
    let enrolled = dispatch_tracked(
        &handler,
        connection_incarnation,
        &mut conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([61; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt) = enrolled else {
        return Err(format!("enrollment did not bind: {enrolled:?}").into());
    };
    let attached = dispatch_tracked(
        &handler,
        connection_incarnation,
        &mut conversations,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id,
            participant_id: receipt.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: receipt.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([67; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    let ServerValue::AttachBound(attached) = attached else {
        return Err(format!("ordinary attach did not bind: {attached:?}").into());
    };

    let cell = handler.cell(conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "fate completion owner lock was poisoned")?;
    let authority = owner
        .as_ref()
        .ok_or("fate completion owner was unavailable")?;
    let attached_source_sequence = authority
        .next_log_sequence
        .checked_sub(1)
        .ok_or("attached source sequence underflow")?;
    let died_source_sequence = authority.next_log_sequence;
    let observer_before = authority.observer_progress;
    let slot = authority
        .slots
        .get(&receipt.participant_id())
        .ok_or("ordinary attached participant slot is absent")?;
    assert!(
        matches!(slot.binding, BindingState::Bound(active) if active.binding_epoch == attached.origin_binding_epoch())
    );
    assert!(slot.binding_fate.is_some());
    drop(owner);

    handler.handle_connection_fate(ConnectionFateWorkItem {
        open_sequence: 23,
        connection_incarnation,
        class: ConnectionFateClass::ConnectionLost,
        tracked_conversations: conversations.tracked_conversations(),
    })?;

    assert_ordinary_completion(
        &handler,
        OrdinaryCompletionExpectation {
            conversation_id,
            participant_id: receipt.participant_id(),
            attached_epoch: attached.origin_binding_epoch(),
            attached_source_sequence,
            died_source_sequence,
            observer_before,
        },
    )
}

fn assert_ordinary_completion(
    handler: &ProductionParticipantHandler,
    expected: OrdinaryCompletionExpectation,
) -> Result<(), Box<dyn Error>> {
    let log = OperationLog::new(Arc::clone(&handler.store), expected.conversation_id);
    let Some(died) = block_on(log.read_at(expected.died_source_sequence))?? else {
        return Err("Died source row is absent".into());
    };
    let DecodedStoredOperation::V3(StoredOperation::Died { row: died }) = died.operation else {
        return Err("expected Died source row".into());
    };
    assert!(matches!(
        died.specific_fate_intent,
        Some(StoredSpecificFateIntent::Ordinary {
            attached_source_sequence: source
        }) if source == expected.attached_source_sequence
    ));

    let ordinary_source_sequence = expected
        .died_source_sequence
        .checked_add(1)
        .ok_or("ordinary source sequence overflow")?;
    let Some(ordinary) = block_on(log.read_at(ordinary_source_sequence))?? else {
        return Err("Ordinary completion row is absent".into());
    };
    let DecodedStoredOperation::V3(StoredOperation::Ordinary { row, event }) = ordinary.operation
    else {
        return Err("expected Ordinary completion row".into());
    };
    assert_eq!(row.participant_id, expected.participant_id);
    assert_eq!(
        row.last_dead_binding_epoch.to_epoch()?,
        expected.attached_epoch
    );
    assert_eq!(
        row.ordinary_attached_source_sequence,
        expected.attached_source_sequence
    );
    assert!(matches!(
        row.terminal_source,
        StoredOrdinaryTerminalSource::DiedCommitted {
            died_source_sequence: source
        } if source == expected.died_source_sequence
    ));
    assert!(row.resulting_floor > expected.observer_before);
    let decoded_event = liminal_protocol::lifecycle::ConversationEvent::decode_canonical(&event)
        .map_err(|error| format!("Ordinary canonical event failed to decode: {error:?}"))?;
    assert_eq!(decoded_event.conversation_id(), expected.conversation_id);

    let cell = handler.cell(expected.conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "fate completion owner lock was poisoned after completion")?;
    let authority = owner
        .as_ref()
        .ok_or("fate completion owner was unavailable after completion")?;
    let slot = authority
        .slots
        .get(&expected.participant_id)
        .ok_or("ordinary participant slot disappeared after completion")?;
    assert!(slot.binding_fate.is_none());
    let frontier = authority
        .frontier
        .as_ref()
        .ok_or("measured frontier owner was not installed after completion")?;
    assert_eq!(
        frontier.frontiers().retained_floor(),
        u128::from(row.resulting_floor)
    );
    drop(owner);

    let cold =
        ProductionParticipantHandler::new(Arc::clone(&handler.store), test_participant_config())?;
    let replayed = cold.replay_and_repair(expected.conversation_id, &log)?;
    let replayed_frontier = replayed
        .frontier
        .as_ref()
        .ok_or("cold replay omitted the measured frontier owner")?;
    assert_eq!(
        replayed_frontier.frontiers().retained_floor(),
        u128::from(row.resulting_floor)
    );
    Ok(())
}
