//! F8 red-first units, server side (`docs/design/F8-MARKER-POISON.md` §3.2 and
//! §5.4's no-new-poison property; the leg's shape is
//! `docs/design/F8-BUILD-LEG-SHAPE.md`).
//!
//! These two units replay §1's incident on a live store rather than asserting
//! about it. A compaction marker for P1 is minted and drained by
//! `prepare_marker_fixture`, and P1 never MarkerAcks — no offer is recorded and
//! no ack is dispatched, so the retained marker record stays replayable and
//! P1's cursor stays below it. The peer then departs, which sets ITS cursor to
//! the high watermark, now past the marker. P1's connection is lost last.
//!
//! `complete_target` appends the durable Died row at `connection_fate.rs:256`
//! and only then calls `open_specific_fate` at `:296`, whose measurement lives
//! at `:368`. A refusal at `:368` is therefore permanent by construction: the
//! intent it refuses to discharge is already durable, and
//! `repair_unclean_server_restart` re-mints the same poisoned Died on every
//! subsequent boot.

use std::error::Error;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal::durability::DurableStore;
use liminal_protocol::wire::ConnectionIncarnation;

use crate::server::participant::{
    ConnectionFateClass, ConnectionFateWorkItem, ParticipantSemanticHandler,
};

use super::ProductionParticipantHandler;
use super::log::{DecodedStoredOperation, OperationLog, StoredOperation};
use super::tests_marker_ack_fixture::{MarkerFixture, marker_fixture_config, prepare_marker_fixture};

/// The durable `Open` the departing peer's fate completes under.
const PEER_OPEN_SEQUENCE: u64 = 0xF801;
/// The durable `Open` the marker owner's fate completes under.
const OWNER_OPEN_SEQUENCE: u64 = 0xF802;

/// Reads every decoded v3 row of one conversation's durable log.
///
/// Same walk as `tests_f8b_typed_refusal::operation_rows`: the residue question
/// is answered on ROWS, never on a count the server reports about itself.
fn operation_rows(
    store: &Arc<dyn DurableStore>,
    conversation_id: u64,
) -> Result<Vec<StoredOperation>, Box<dyn Error>> {
    let log = OperationLog::new(Arc::clone(store), conversation_id);
    let mut rows = Vec::new();
    let mut sequence = 0;
    while let Some(entry) = block_on(log.read_at(sequence))?? {
        let DecodedStoredOperation::V3(operation) = entry.operation else {
            return Err(format!("conversation {conversation_id} row {sequence} is not v3").into());
        };
        rows.push(operation);
        sequence = sequence
            .checked_add(1)
            .ok_or("durable log sequence overflowed")?;
    }
    Ok(rows)
}

#[derive(Clone, Copy)]
struct IncidentRoles {
    conversation_id: u64,
    marker_delivery_seq: u64,
    owner_connection: ConnectionIncarnation,
    owner_participant: u64,
    peer_connection: ConnectionIncarnation,
}

/// Names the two identities by the role §1 gives them: the marker's OWNER,
/// whose unacked marker pins the floor, and the PEER whose departure moves the
/// only remaining cursor past it.
fn incident_roles(fixture: &MarkerFixture) -> Result<IncidentRoles, Box<dyn Error>> {
    let owner_participant = fixture.target_participant;
    let (peer_connection, peer_participant) = if owner_participant == fixture.record_participant {
        (fixture.catchup_connection, fixture.catchup_participant)
    } else if owner_participant == fixture.catchup_participant {
        (fixture.record_connection, fixture.record_participant)
    } else {
        return Err(format!(
            "the marker fixture targeted {owner_participant}, which is neither of its two \
             members — this fixture no longer mints the two-identity incident"
        )
        .into());
    };
    if peer_participant == owner_participant {
        return Err("the incident needs two distinct identities".into());
    }
    Ok(IncidentRoles {
        conversation_id: fixture.marker_delivery.conversation_id,
        marker_delivery_seq: fixture.marker_delivery.delivery_seq,
        owner_connection: fixture.target_connection,
        owner_participant,
        peer_connection,
    })
}

/// Asserts the prestate this whole file depends on: the marker is RETAINED and
/// UNACKED, and the owner's cursor sits below it. A fixture that stops minting
/// that state would make both units pass for the wrong reason.
fn assert_unacked_marker_prestate(
    handler: &ProductionParticipantHandler,
    roles: &IncidentRoles,
) -> Result<(), Box<dyn Error>> {
    let cell = handler.cell(roles.conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "F8 incident conversation owner lock was poisoned")?;
    let authority = owner
        .as_ref()
        .ok_or("F8 incident conversation owner was absent")?;
    let retained = authority
        .frontier()
        .map(|frontier| frontier.frontiers().retained_marker_records().to_vec())
        .unwrap_or_default();
    let pinned = retained
        .iter()
        .any(|record| record.delivery_seq == roles.marker_delivery_seq);
    let cursor = authority
        .slots
        .get(&roles.owner_participant)
        .ok_or("F8 incident marker owner was absent from its conversation")?
        .member
        .cursor();
    drop(owner);
    if !pinned {
        return Err(format!(
            "the drained marker at {} is not retained: {retained:?} — this fixture no longer \
             mints an unacked marker",
            roles.marker_delivery_seq
        )
        .into());
    }
    if cursor >= roles.marker_delivery_seq {
        return Err(format!(
            "the marker owner's cursor {cursor} already covers the marker at {} — the fixture \
             acked a marker this incident requires to stay unacked",
            roles.marker_delivery_seq
        )
        .into());
    }
    Ok(())
}

fn connection_lost(
    open_sequence: u64,
    connection_incarnation: ConnectionIncarnation,
    conversation_id: u64,
) -> ConnectionFateWorkItem {
    ConnectionFateWorkItem {
        open_sequence,
        connection_incarnation,
        class: ConnectionFateClass::ConnectionLost,
        tracked_conversations: vec![conversation_id],
    }
}

struct DepartedPeer {
    fixture: MarkerFixture,
    roles: IncidentRoles,
    rows_before_owner_drop: Vec<StoredOperation>,
}

/// Replays §1 up to the instant before P1 drops: marker drained and unacked,
/// peer departed with its cursor at the high watermark.
fn peer_departed() -> Result<DepartedPeer, Box<dyn Error>> {
    let fixture = prepare_marker_fixture()?;
    let roles = incident_roles(&fixture)?;
    assert_unacked_marker_prestate(&fixture.handler, &roles)?;

    let consumer: &dyn ParticipantSemanticHandler = &fixture.handler;
    consumer
        .handle_connection_fate(connection_lost(
            PEER_OPEN_SEQUENCE,
            roles.peer_connection,
            roles.conversation_id,
        ))
        .map_err(|error| {
            format!(
                "the peer's departure refused, so this fixture never reaches the incident it \
                 exists to replay: {error}"
            )
        })?;

    let rows_before_owner_drop = operation_rows(&fixture.store, roles.conversation_id)?;
    Ok(DepartedPeer {
        fixture,
        roles,
        rows_before_owner_drop,
    })
}

/// §3.2 RED UNIT. A refused binding-fate measurement must leave ZERO appended
/// rows.
///
/// Stated as the invariant rather than as one outcome, because §3.1 changes
/// which branch this incident takes: a connection fate leaves EITHER nothing
/// at all OR a completely discharged fate — never a durable Died row carrying
/// an intent that cannot be discharged. Today it takes the third road, which
/// is the one that poisons the store.
#[test]
fn a_refused_connection_fate_leaves_no_durable_residue() -> Result<(), Box<dyn Error>> {
    let departed = peer_departed()?;
    let roles = departed.roles;
    let store = Arc::clone(&departed.fixture.store);

    let consumer: &dyn ParticipantSemanticHandler = &departed.fixture.handler;
    let outcome = consumer.handle_connection_fate(connection_lost(
        OWNER_OPEN_SEQUENCE,
        roles.owner_connection,
        roles.conversation_id,
    ));
    let rows_after = operation_rows(&store, roles.conversation_id)?;

    let Err(error) = outcome else {
        // The measurement was satisfiable. Then the fate must be DISCHARGED:
        // no participant may be left holding an open specific-fate intent.
        let cell = departed.fixture.handler.cell(roles.conversation_id)?;
        let owner = cell
            .lock()
            .map_err(|_| "F8 discharge check owner lock was poisoned")?;
        let authority = owner
            .as_ref()
            .ok_or("F8 discharge check conversation owner was absent")?;
        let stranded = authority
            .pending_specific_fates
            .contains_key(&roles.owner_participant);
        drop(owner);
        if stranded {
            return Err(format!(
                "the connection fate reported success while leaving participant {} holding an \
                 open specific-fate intent",
                roles.owner_participant
            )
            .into());
        }
        return Ok(());
    };

    if rows_after != departed.rows_before_owner_drop {
        return Err(format!(
            "F8 §3.2: the binding-fate measurement refused ({error}) but the connection fate \
             had already appended {} durable row(s) to conversation {} — {} rows before, {} \
             after. The last row is {:?}. That row's intent is now undischargeable, and \
             `repair_unclean_server_restart` re-mints it on every boot.",
            rows_after
                .len()
                .saturating_sub(departed.rows_before_owner_drop.len()),
            roles.conversation_id,
            departed.rows_before_owner_drop.len(),
            rows_after.len(),
            rows_after.last()
        )
        .into());
    }
    Ok(())
}

/// §5.4 NO-NEW-POISON PROPERTY. The incident sequence replayed from clean
/// produces a discharged fate and a LIVE BOOT.
///
/// This is the property the whole leg exists for, at the seat §1 names:
/// `ProductionParticipantHandler::new` runs `replay_and_repair`, which calls
/// `repair_pending_specific_fates` (`handler.rs:481-483`) and surfaces its
/// failure as the boot's own error — `ServerError::ParticipantStartupRestore`
/// at `connection/services.rs:391` in the real server. Today that boot dies,
/// because the previous run left a durable Died carrying an open Ordinary
/// intent.
///
/// It can only go red through §3.1's and §3.2's mechanism, so if it is green
/// before the fix, that is a vacuity finding to report, not a pass.
#[test]
fn the_incident_sequence_reboots_into_a_discharged_fate_and_a_live_server()
-> Result<(), Box<dyn Error>> {
    let departed = peer_departed()?;
    let roles = departed.roles;
    let store = Arc::clone(&departed.fixture.store);

    let consumer: &dyn ParticipantSemanticHandler = &departed.fixture.handler;
    let fate = consumer.handle_connection_fate(connection_lost(
        OWNER_OPEN_SEQUENCE,
        roles.owner_connection,
        roles.conversation_id,
    ));
    let live_refusal = fate.err();
    drop(departed);

    // The boot the incident kills. `new` IS `restore_all_conversations`.
    let booted = ProductionParticipantHandler::new(Arc::clone(&store), marker_fixture_config())
        .map_err(|error| {
            format!(
                "F8 no-new-poison: the server did not reach listening after the incident \
                 sequence — boot failed with `{error}`. The live fate had answered {live_refusal:?}."
            )
        })?;

    // A live boot that still holds the stranded intent is not a discharge, it
    // is a poison that has merely not been stepped on yet.
    let cell = booted.cell(roles.conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "F8 rebooted conversation owner lock was poisoned")?;
    let authority = owner
        .as_ref()
        .ok_or("F8 rebooted conversation owner was absent")?;
    let stranded = authority
        .pending_specific_fates
        .contains_key(&roles.owner_participant);
    let marker_still_retained = authority
        .frontier()
        .map(|frontier| {
            frontier
                .frontiers()
                .retained_marker_records()
                .iter()
                .any(|record| record.delivery_seq == roles.marker_delivery_seq)
        })
        .unwrap_or(false);
    drop(owner);

    if stranded {
        return Err(format!(
            "F8 no-new-poison: the rebooted server still holds an open specific-fate intent for \
             participant {} — the fate was never discharged",
            roles.owner_participant
        )
        .into());
    }
    // §5.4 item 2: the discharge must not have been bought by releasing a
    // marker its owner has still not acked.
    if !marker_still_retained {
        return Err(format!(
            "F8 no-new-poison: the marker at {} was released although its owner never acked it",
            roles.marker_delivery_seq
        )
        .into());
    }
    Ok(())
}
