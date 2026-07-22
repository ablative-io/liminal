use std::error::Error;
use std::sync::Arc;

use liminal::durability::{DurableStore, open_ephemeral};
use liminal_protocol::lifecycle::ObligationDebtDispatchState;
use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, EnrollmentRequest, EnrollmentToken, Generation,
    ParticipantAck, ServerValue,
};

use super::ProductionParticipantHandler;
use super::e2e_cold_all_shapes_fixture::decoded_history_from_store;
use super::log_v3::StoredOperationV3;
use super::tests::{dispatch, test_participant_config};
use crate::server::participant::ParticipantSemanticHandler;

const CONVERSATION: u64 = 0xD2_02;

fn handler_with_capacity(
    retained_capacity_entries: u64,
) -> Result<(ProductionParticipantHandler, Arc<dyn DurableStore>), Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let mut config = test_participant_config();
    config.retained_capacity_entries = retained_capacity_entries;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), config)?;
    Ok((handler, store))
}

fn enroll(
    handler: &ProductionParticipantHandler,
    connection_ordinal: u64,
    token: u8,
) -> Result<(ConnectionIncarnation, liminal_protocol::wire::EnrollBound), Box<dyn Error>> {
    let connection = ConnectionIncarnation::new(CONVERSATION, connection_ordinal);
    let result = dispatch(
        handler,
        connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([token; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(bound) = result else {
        return Err(format!("enrollment did not bind: {result:?}").into());
    };
    Ok((connection, bound))
}

fn is_owed(handler: &ProductionParticipantHandler) -> Result<bool, Box<dyn Error>> {
    let cell = handler.cell(CONVERSATION)?;
    let owner = cell
        .lock()
        .map_err(|_| "conversation owner lock was poisoned")?;
    let authority = owner.as_ref().ok_or("conversation owner was absent")?;
    let owed = matches!(
        authority.obligation_debt_dispatch(),
        Some(ObligationDebtDispatchState::Owed(_))
    );
    drop(owner);
    Ok(owed)
}

#[derive(Debug, PartialEq, Eq)]
struct OwnerSnapshot {
    cursor: u64,
    outbox_ack_through: u64,
    coupled_owner: String,
}

fn owner_snapshot(
    handler: &ProductionParticipantHandler,
    participant_id: u64,
) -> Result<OwnerSnapshot, Box<dyn Error>> {
    let cell = handler.cell(CONVERSATION)?;
    let owner = cell
        .lock()
        .map_err(|_| "conversation owner lock was poisoned")?;
    let authority = owner.as_ref().ok_or("conversation owner was absent")?;
    let cursor = authority
        .slots
        .get(&participant_id)
        .ok_or("participant slot was absent")?
        .member
        .cursor();
    let outbox_ack_through = authority
        .outbox
        .as_ref()
        .ok_or("outbox owner was absent")?
        .ack_through(participant_id);
    let coupled_owner = format!("{:?}", authority.obligation_debt_dispatch());
    drop(owner);
    Ok(OwnerSnapshot {
        cursor,
        outbox_ack_through,
        coupled_owner,
    })
}

#[test]
fn enrollment_clear_or_owed_and_no_obligation_are_total() -> Result<(), Box<dyn Error>> {
    let (clear, _clear_store) = handler_with_capacity(2_048)?;
    let (_clear_connection, _) = enroll(&clear, 1, 0xC1)?;
    assert!(!is_owed(&clear)?);

    let (owed, _owed_store) = handler_with_capacity(12)?;
    let (first_connection, _) = enroll(&owed, 1, 0xD1)?;
    assert!(is_owed(&owed)?);
    assert_eq!(
        owed.next_publication(first_connection, CONVERSATION, None)?,
        None,
        "first Owed enrollment must be total when sender filtering leaves no obligation"
    );

    let (_second_connection, _) = enroll(&owed, 2, 0xD2)?;
    assert!(is_owed(&owed)?);
    assert!(
        owed.next_publication(first_connection, CONVERSATION, None)?
            .is_some(),
        "Owed enrollment with another recipient must select its least obligation"
    );
    Ok(())
}

#[test]
fn nonzero_debt_ack_row_replays_obligation_aware_commit() -> Result<(), Box<dyn Error>> {
    let (live, store) = handler_with_capacity(12)?;
    let (first_connection, first) = enroll(&live, 1, 0xE1)?;
    let (_second_connection, _) = enroll(&live, 2, 0xE2)?;
    assert!(is_owed(&live)?);
    let publication = live
        .next_publication(first_connection, CONVERSATION, None)?
        .ok_or("second enrollment did not create a first-participant obligation")?;
    let through_seq = publication.delivery_seq();

    let outcome = dispatch(
        &live,
        first_connection,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: CONVERSATION,
            participant_id: first.participant_id(),
            capability_generation: Generation::ONE,
            through_seq,
        }),
    )?;
    assert!(matches!(outcome, ServerValue::AckCommitted(_)));
    let live_snapshot = owner_snapshot(&live, first.participant_id())?;
    assert_eq!(live_snapshot.cursor, through_seq);
    assert_eq!(live_snapshot.outbox_ack_through, through_seq);

    let (rows, _) = decoded_history_from_store(Arc::clone(&store), CONVERSATION)?;
    let nonzero = rows
        .iter()
        .filter_map(|(_, row)| match row {
            StoredOperationV3::NonzeroDebtAck {
                request,
                contiguously_available_through,
                event,
                ..
            } => Some((request, *contiguously_available_through, event)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(nonzero.len(), 1);
    let (request, scalar_audit, event) = nonzero[0];
    assert_eq!(request.conversation_id, CONVERSATION);
    assert_eq!(request.participant_id, first.participant_id());
    assert_eq!(request.through_seq, through_seq);
    assert_eq!(scalar_audit, through_seq);
    assert!(!event.is_empty());

    drop(live);
    let mut config = test_participant_config();
    config.retained_capacity_entries = 12;
    let cold = ProductionParticipantHandler::new(store, config)?;
    let replayed = owner_snapshot(&cold, first.participant_id())?;
    assert_eq!(replayed, live_snapshot);
    Ok(())
}
