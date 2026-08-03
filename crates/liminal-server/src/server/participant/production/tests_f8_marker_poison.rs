//! F8 red-first units, server side (`docs/design/F8-MARKER-POISON.md` §3.2 and
//! §5.4's no-new-poison property; the leg's shape is
//! `docs/design/F8-BUILD-LEG-SHAPE.md`).
//!
//! These two units replay §1's incident on a live store rather than asserting
//! about it. A compaction marker for P1 is minted and drained by
//! `attached_marker_fixture` (which drives the CredentialAttach that mints the
//! binding-fate token, so the Died row carries an open Ordinary intent as §1
//! requires), and P1 never MarkerAcks — no offer is recorded and
//! no ack is dispatched, so the retained marker record stays replayable and
//! P1's cursor stays below it. The peer acks past the marker, which lifts hard
//! observer progress over it, and then departs, which sets ITS cursor to the
//! high watermark. P1's connection is lost last.
//!
//! BOTH peer steps are load-bearing, and the first was MISSING when this file
//! was first run at tree 6d18f51: the measured floor is capped by hard observer
//! progress as well as by the minimum remaining cursor, so with observer
//! progress at its initial 0 (`state.rs:371`) the floor could never reach the
//! marker, and both units below printed `ok` while never once reaching the
//! branch they exist to judge. They were born green. The arming condition is
//! now ASSERTED before the incident is driven, so the fixture fails loudly
//! rather than passing silently if it ever stops reproducing the defect.
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
use liminal_protocol::lifecycle::BindingState;
use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, Generation, ParticipantAck, ServerValue,
};

use crate::server::participant::{
    ConnectionFateClass, ConnectionFateWorkItem, ParticipantConnectionConversations,
    ParticipantSemanticHandler,
};

use super::ProductionParticipantHandler;
use super::log::{DecodedStoredOperation, OperationLog, StoredOperation};
use super::tests::dispatch_tracked;
use super::tests_marker_ack_fixture::{
    MarkerFixture, attached_marker_fixture, marker_fixture_config,
};

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
    peer_participant: u64,
    /// The boundary the peer acks through. Must be at or past the marker: it is
    /// what lifts hard observer progress over `marker_delivery_seq`, and
    /// without that lift the floor can never reach the marker to cross it.
    peer_ack_through_seq: u64,
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
    let marker_delivery_seq = fixture.marker_delivery.delivery_seq;
    let peer_ack_through_seq = fixture.catchup_through_seq;
    if peer_ack_through_seq < marker_delivery_seq {
        return Err(format!(
            "the fixture's ack boundary {peer_ack_through_seq} is below the marker at \
             {marker_delivery_seq}, so no ack can lift hard observer progress over the marker \
             and the incident cannot be armed from this fixture"
        )
        .into());
    }
    Ok(IncidentRoles {
        conversation_id: fixture.marker_delivery.conversation_id,
        marker_delivery_seq,
        owner_connection: fixture.target_connection,
        owner_participant,
        peer_connection,
        peer_participant,
        peer_ack_through_seq,
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

/// The non-vacuity guard this file was missing the first time it ran.
///
/// Both units previously passed while never reaching the branch they exist to
/// judge, because hard observer progress was still 0 and the floor could not
/// reach the marker. A fixture that silently stops arming its own defect is
/// indistinguishable, from the outside, from a fixture whose defect is fixed —
/// so the arming condition is now asserted rather than assumed.
fn assert_observer_progress_cleared_the_marker(
    handler: &ProductionParticipantHandler,
    roles: &IncidentRoles,
) -> Result<(), Box<dyn Error>> {
    let cell = handler.cell(roles.conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "F8 observer-progress check owner lock was poisoned")?;
    let authority = owner
        .as_ref()
        .ok_or("F8 observer-progress check conversation owner was absent")?;
    let observer_progress = authority.observer_progress;
    drop(owner);
    if observer_progress < roles.marker_delivery_seq {
        return Err(format!(
            "hard observer progress is {observer_progress}, still below the marker at {} — the \
             floor is capped under the marker, the transition has nothing to cross, and both \
             units below would pass WITHOUT EVER WITNESSING THE DEFECT",
            roles.marker_delivery_seq
        )
        .into());
    }
    Ok(())
}

/// STEP 0 DISCRIMINATOR (ruling 413f8725). Diagnostic only — this is not the
/// rework, it is the measurement that decides whether a rework is authored at
/// all.
///
/// `prepare_connection_fate_transaction` (`connection_fate.rs:70-96`) filters
/// to slots that are BOUND and whose binding epoch names the exact
/// `ConnectionIncarnation` the work item carries. An empty match set is not an
/// error: the transaction completes `Ok` having done nothing. Both units pass
/// on `Ok`, so a no-op departure and a genuinely discharged one are
/// indistinguishable at the unit's own assertions — which is precisely how
/// they came back green.
///
/// Two outcomes, and they mean opposite things:
///   * this fires  → the hypothesis is CONFIRMED (the slot is not Bound for
///     that incarnation, so the departure was a no-op) and the rework is
///     authorized;
///   * this passes → the hypothesis is FALSE. The slot WAS Bound, the target
///     set was non-empty, and the unit went green anyway. That is a different
///     and deeper class and it goes back to the design gate before anything
///     else is built.
fn assert_peer_is_bound_for_its_departure(
    handler: &ProductionParticipantHandler,
    roles: &IncidentRoles,
) -> Result<(), Box<dyn Error>> {
    let cell = handler.cell(roles.conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "F8 step-0 discriminator owner lock was poisoned")?;
    let authority = owner
        .as_ref()
        .ok_or("F8 step-0 discriminator conversation owner was absent")?;
    let binding = authority
        .slots
        .get(&roles.peer_participant)
        .map(|slot| slot.binding);
    drop(owner);
    let Some(binding) = binding else {
        return Err(format!(
            "STEP 0 CONFIRMS THE HYPOTHESIS: participant {} has no slot at all in conversation \
             {}, so the departure names a target that cannot be matched and completes Ok having \
             done nothing",
            roles.peer_participant, roles.conversation_id
        )
        .into());
    };
    let BindingState::Bound(active) = binding else {
        return Err(format!(
            "STEP 0 CONFIRMS THE HYPOTHESIS: participant {}'s slot is {binding:?}, not Bound, so \
             prepare_connection_fate_transaction matches an EMPTY target set and the departure \
             completes Ok having done nothing. The peer never departs, its cursor never reaches \
             the high watermark, minimum_remaining_cursor stays below the marker at {}, and the \
             floor cannot cross it no matter how far hard observer progress is lifted.",
            roles.peer_participant, roles.marker_delivery_seq
        )
        .into());
    };
    let bound_incarnation = active.binding_epoch.connection_incarnation;
    if bound_incarnation != roles.peer_connection {
        return Err(format!(
            "STEP 0 CONFIRMS THE HYPOTHESIS: participant {} is Bound, but to incarnation \
             {bound_incarnation:?}, while the departure work item names {:?}. The filter matches \
             on the EXACT incarnation, so the target set is empty and the departure completes Ok \
             having done nothing.",
            roles.peer_participant, roles.peer_connection
        )
        .into());
    }
    Ok(())
}

/// STEP 0b, lead 1 (ruling b7dc92b2). Did the peer's departure actually MOVE
/// the peer, and is §1's premise unconditional?
///
/// §1 states flatly that "the departing participant's cursor is set to the
/// high watermark". `claim_frontier/binding_fate_transition.rs:45-49` in
/// liminal-protocol makes that conditional on one flag:
/// `resulting_cursor = if reserve_finalizer { cursor } else { high_watermark }`.
/// Read at the bytes, `reserve_finalizer` is true ONLY for a Recovered intent
/// carrying no terminal (`binding_fate_completion.rs:448-465` —
/// `RecoveredAndReserveFinalizer` is that arm and only that arm), so an
/// ORDINARY departure never reserves one and should land at the watermark.
///
/// But there is a second, earlier exit that moves nothing at all:
/// `binding_fate_completion.rs:66-77` re-inserts an Ordinary intent whose
/// terminal is not yet committed and returns `Ok` WITHOUT measuring. A
/// departure down that path completes successfully, leaves the cursor where it
/// was, and is indistinguishable at the call site from one that discharged.
///
/// So this measures the OUTCOME rather than trusting either reading: the fate
/// must be gone from `pending_specific_fates` (it completed rather than
/// deferring), and the cursor must have reached the watermark.
fn assert_peer_actually_departed(
    handler: &ProductionParticipantHandler,
    roles: &IncidentRoles,
) -> Result<(), Box<dyn Error>> {
    let cell = handler.cell(roles.conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "F8 step-0b peer-departure owner lock was poisoned")?;
    let authority = owner
        .as_ref()
        .ok_or("F8 step-0b peer-departure conversation owner was absent")?;
    let still_pending = authority
        .pending_specific_fates
        .contains_key(&roles.peer_participant);
    let cursor = authority
        .slots
        .get(&roles.peer_participant)
        .map(|slot| slot.member.cursor());
    let binding = authority
        .slots
        .get(&roles.peer_participant)
        .map(|slot| slot.binding);
    let high_watermark = authority
        .frontier()
        .map(|frontier| frontier.frontiers().sequence().ledger().high_watermark());
    drop(owner);

    if still_pending {
        return Err(format!(
            "STEP 0b LEAD 1 CONFIRMED — DEFERRED, NOT DISCHARGED: after its departure, peer {} \
             STILL holds an open specific-fate intent. That is the `binding_fate_completion.rs:\
             66-77` early exit: an Ordinary intent whose terminal is not yet committed is \
             re-inserted and returns Ok WITHOUT measuring. The peer's cursor never moves, \
             minimum_remaining_cursor stays below the marker at {}, and no floor can cross it. \
             cursor={cursor:?} high_watermark={high_watermark:?} binding={binding:?}",
            roles.peer_participant, roles.marker_delivery_seq
        )
        .into());
    }
    let (Some(cursor), Some(high_watermark)) = (cursor, high_watermark) else {
        return Err(format!(
            "STEP 0b LEAD 1: peer {} has no slot or the conversation has no frontier after \
             departure — cursor={cursor:?} high_watermark={high_watermark:?}",
            roles.peer_participant
        )
        .into());
    };
    // The cursor-versus-watermark assertion that used to stand here is GONE, not
    // relaxed. It claimed §1's premise was conditional; instrument #1 refuted
    // that with a PASSING positive control (post-departure cursor 9 equalled the
    // PRE-departure watermark 9 — the fate set the cursor to the watermark it
    // could see, and the apparent gap was the departure's own Died row landing
    // afterwards). I withdrew the reading, so the assertion had to go with it:
    // a red citing a refuted cause poisons every suite count that reads it.
    if cursor < roles.marker_delivery_seq {
        return Err(format!(
            "STEP 0b LEAD 1: peer {} reached the high watermark {high_watermark}, but that \
             watermark is itself BELOW the marker at {} — so minimum_remaining_cursor cannot \
             clear the marker and the incident is unreachable from this fixture's geometry",
            roles.peer_participant, roles.marker_delivery_seq
        )
        .into());
    }
    Ok(())
}

/// STEP 0b, lead 2 (ruling b7dc92b2). Closes the owner side that step 0 left
/// unmeasured: step 0 verified the PEER was Bound for its departure, and said
/// so; the owner was never checked, and a no-op there produces the same green.
fn assert_owner_is_bound_for_its_departure(
    handler: &ProductionParticipantHandler,
    roles: &IncidentRoles,
) -> Result<(), Box<dyn Error>> {
    let cell = handler.cell(roles.conversation_id)?;
    let owner_cell = cell
        .lock()
        .map_err(|_| "F8 step-0b owner-side owner lock was poisoned")?;
    let authority = owner_cell
        .as_ref()
        .ok_or("F8 step-0b owner-side conversation owner was absent")?;
    let binding = authority
        .slots
        .get(&roles.owner_participant)
        .map(|slot| slot.binding);
    drop(owner_cell);
    let Some(BindingState::Bound(active)) = binding else {
        return Err(format!(
            "STEP 0b LEAD 2 CONFIRMED — OWNER-SIDE NO-OP: marker owner {}'s slot is {binding:?}, \
             not Bound, so its departure matches an EMPTY target set and completes Ok having \
             done nothing. The measurement the units judge is never reached.",
            roles.owner_participant
        )
        .into());
    };
    let bound_incarnation = active.binding_epoch.connection_incarnation;
    if bound_incarnation != roles.owner_connection {
        return Err(format!(
            "STEP 0b LEAD 2 CONFIRMED — OWNER-SIDE NO-OP: marker owner {} is Bound to \
             {bound_incarnation:?} but its departure names {:?}; the filter matches on the exact \
             incarnation, so the target set is empty.",
            roles.owner_participant, roles.owner_connection
        )
        .into());
    }
    Ok(())
}

/// One sample of everything the instrument lane needs, taken at one instant
/// under one lock acquisition so the fields cannot drift against each other.
#[derive(Debug)]
struct AuthoritySample {
    high_watermark: Option<u64>,
    peer_cursor: Option<u64>,
    peer_binding: Option<BindingState>,
    peer_pending_fate: bool,
    owner_cursor: Option<u64>,
    owner_binding: Option<BindingState>,
    owner_pending_fate: bool,
    observer_progress: u64,
    retained_marker_seqs: Vec<u64>,
    /// The witness that a floor computation actually RAN and installed a
    /// result. `install_binding_fate_transition` sets `retained_floor` to the
    /// measured `resulting_floor`, so a move here is the observable footprint
    /// of the computation at `binding_fate.rs:428-450` having been reached.
    retained_floor: Option<u128>,
}

fn sample_authority(
    handler: &ProductionParticipantHandler,
    roles: &IncidentRoles,
) -> Result<AuthoritySample, Box<dyn Error>> {
    let cell = handler.cell(roles.conversation_id)?;
    let guard = cell
        .lock()
        .map_err(|_| "F8 instrument sample owner lock was poisoned")?;
    let authority = guard
        .as_ref()
        .ok_or("F8 instrument sample conversation owner was absent")?;
    let sample = AuthoritySample {
        high_watermark: authority
            .frontier()
            .map(|frontier| frontier.frontiers().sequence().ledger().high_watermark()),
        peer_cursor: authority
            .slots
            .get(&roles.peer_participant)
            .map(|slot| slot.member.cursor()),
        peer_binding: authority
            .slots
            .get(&roles.peer_participant)
            .map(|slot| slot.binding),
        peer_pending_fate: authority
            .pending_specific_fates
            .contains_key(&roles.peer_participant),
        owner_cursor: authority
            .slots
            .get(&roles.owner_participant)
            .map(|slot| slot.member.cursor()),
        owner_binding: authority
            .slots
            .get(&roles.owner_participant)
            .map(|slot| slot.binding),
        owner_pending_fate: authority
            .pending_specific_fates
            .contains_key(&roles.owner_participant),
        observer_progress: authority.observer_progress,
        retained_marker_seqs: authority
            .frontier()
            .map(|frontier| {
                frontier
                    .frontiers()
                    .retained_marker_records()
                    .iter()
                    .map(|record| record.delivery_seq)
                    .collect()
            })
            .unwrap_or_default(),
        retained_floor: authority
            .frontier()
            .map(|frontier| frontier.frontiers().retained_floor()),
    };
    drop(guard);
    Ok(sample)
}


/// Evidence that the §1-intent arming assertions ACTUALLY RAN (condition 3).
/// An unexecuted branch is not a passing branch, so these values are carried
/// out and PRINTED by whichever unit reports, rather than being swallowed by a
/// silent `?`.
#[derive(Clone, Copy, Debug)]
struct ArmingEvidence {
    target_holds_binding_fate_before: bool,
    target_pending_fate_after: bool,
    target_died_rows_after: usize,
}

/// §1-INTENT ARMING ASSERTION, BEFORE the departure (Cally b23e4cc1).
///
/// §1 requires "P1's Died row carrying an open Ordinary intent", and
/// `connection_fate.rs:234-242` opens that intent ONLY IF the target slot holds
/// `slot.binding_fate`. This is the amended assertion that superseded the
/// retained_floor==marker form: it is the server-side condition §1 actually
/// demands.
fn assert_intent_geometry_is_built(
    handler: &ProductionParticipantHandler,
    roles: &IncidentRoles,
) -> Result<bool, Box<dyn Error>> {
    let cell = handler.cell(roles.conversation_id)?;
    let guard = cell
        .lock()
        .map_err(|_| "F8 arming assertion owner lock was poisoned")?;
    let authority = guard
        .as_ref()
        .ok_or("F8 arming assertion conversation owner was absent")?;
    let holds = authority
        .slots
        .get(&roles.owner_participant)
        .is_some_and(|slot| slot.binding_fate.is_some());
    drop(guard);
    if !holds {
        return Err(format!(
            "F8 §1-INTENT ARMING FAILED: marker owner {} holds NO binding-fate token before its \
             departure, so `connection_fate.rs:234-242` will set specific_fate_intent = None, its \
             Died row will be INTENT-FREE, and the units below would witness nothing. The fixture \
             is not armed and no result from it may be claimed.",
            roles.owner_participant
        )
        .into());
    }
    Ok(holds)
}

/// §1-INTENT ARMING ASSERTION, AFTER the departure: `pending_specific_fates`
/// gains the participant. That is the durable open intent §1 says the Died row
/// carries -- the thing that gets stranded when the measurement refuses.
fn observe_intent_after_departure(
    handler: &ProductionParticipantHandler,
    roles: &IncidentRoles,
) -> Result<bool, Box<dyn Error>> {
    let cell = handler.cell(roles.conversation_id)?;
    let guard = cell
        .lock()
        .map_err(|_| "F8 post-departure arming observation owner lock was poisoned")?;
    let authority = guard
        .as_ref()
        .ok_or("F8 post-departure arming observation owner was absent")?;
    let pending = authority
        .pending_specific_fates
        .contains_key(&roles.owner_participant);
    drop(guard);
    Ok(pending)
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
    /// Condition 3: carried out so the units PRINT it. An assertion whose value
    /// is never shown is an assertion nobody watched run.
    target_holds_binding_fate_before: bool,
}

/// Replays §1 up to the instant before P1 drops: marker drained and unacked,
/// hard observer progress lifted past the marker, peer departed with its cursor
/// at the high watermark.
///
/// THE ARMING STEP, and why it is not optional. The measured floor is
/// `max(retained_floor, min(minimum_remaining_cursor, hard_observer_progress) + 1)`
/// (`binding_fate.rs:436-442` feeding `algebra/floor.rs:20-34`), and the server
/// passes `self.observer_progress` as that second input
/// (`binding_fate_completion.rs:89`). It starts at 0 (`state.rs:371`). So the
/// departed peer's cursor being past the marker is NECESSARY but not
/// SUFFICIENT: while observer progress sits below the marker it caps the floor
/// below the marker too, the transition never crosses anything, and the
/// incident cannot occur. Both inputs must clear the marker.
///
/// The lift uses production's own path rather than a poked field: a participant
/// ack projects hard observer progress from the acking participant's OWN
/// `through_seq` (`operations/participant_ack.rs:37-40`,
/// `ObserverProgressProjection::new(request.conversation_id, request.through_seq)`),
/// and `record_observer_progress_projection` folds it in with `max`
/// (`state.rs:393`). It is deliberately NOT gated by the slowest member, which
/// is precisely why one peer ack can lift it over a marker its own owner has
/// never acked.
fn peer_departed() -> Result<DepartedPeer, Box<dyn Error>> {
    // THE ARMED FIXTURE. `prepare_marker_fixture` mints no binding-fate token,
    // so its Died rows are intent-free and §1's incident cannot occur in it.
    // This one drives the CredentialAttach that mints the intent.
    let fixture = attached_marker_fixture()?;
    let roles = incident_roles(&fixture)?;
    assert_unacked_marker_prestate(&fixture.handler, &roles)?;

    let acked = dispatch_tracked(
        &fixture.handler,
        roles.peer_connection,
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: roles.conversation_id,
            participant_id: roles.peer_participant,
            capability_generation: Generation::ONE,
            through_seq: roles.peer_ack_through_seq,
        }),
    )?;
    if !matches!(acked, ServerValue::AckCommitted(_)) {
        return Err(format!(
            "the peer's ack through {} did not commit, so hard observer progress was never \
             lifted over the marker at {} and the incident is not armed: {acked:?}",
            roles.peer_ack_through_seq, roles.marker_delivery_seq
        )
        .into());
    }
    assert_observer_progress_cleared_the_marker(&fixture.handler, &roles)?;
    assert_peer_is_bound_for_its_departure(&fixture.handler, &roles)?;

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

    // STEP 0b (ruling b7dc92b2): both leads answered at this one instant. These
    // two remain TRUE and stay as guards.
    assert_peer_actually_departed(&fixture.handler, &roles)?;
    assert_owner_is_bound_for_its_departure(&fixture.handler, &roles)?;

    // --- PAIRED RETIREMENT, half 2 (Cally b23e4cc1 condition 2) ---
    // `refuse_until_the_geometry_exists` retires HERE, in the same commit that
    // adds the §1-intent arming assertion that replaces it. The refusal said
    // the fixture could not express the incident because no intent is minted.
    // The armed fixture mints one, so the refusal's own stated cause is gone
    // and this assertion now proves the geometry positively instead.
    let arming = assert_intent_geometry_is_built(&fixture.handler, &roles)?;

    let rows_before_owner_drop = operation_rows(&fixture.store, roles.conversation_id)?;
    Ok(DepartedPeer {
        fixture,
        roles,
        rows_before_owner_drop,
        target_holds_binding_fate_before: arming,
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
    // §1-INTENT ARMING ASSERTION, AFTER. Observed unconditionally so its value
    // is reportable on every exit (condition 3).
    let pending_after = observe_intent_after_departure(&departed.fixture.handler, &roles)?;
    let arming = format!(
        "ARMING EVIDENCE (both §1-intent assertions RAN):          slot.binding_fate.is_some() BEFORE departure = {}; pending_specific_fates gained          participant {} AFTER departure = {}",
        departed.target_holds_binding_fate_before, roles.owner_participant, pending_after
    );

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
                 open specific-fate intent. {arming}",
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
             `repair_unclean_server_restart` re-mints it on every boot. {arming}",
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
    // §1-INTENT ARMING ASSERTION, AFTER -- observed BEFORE `departed` is
    // dropped, so its value is reportable on every exit (condition 3).
    let pending_after = observe_intent_after_departure(&departed.fixture.handler, &roles)?;
    let arming = format!(
        "ARMING EVIDENCE (both §1-intent assertions RAN): \
         slot.binding_fate.is_some() BEFORE departure = {}; pending_specific_fates gained \
         participant {} AFTER departure = {}",
        departed.target_holds_binding_fate_before, roles.owner_participant, pending_after
    );
    let live_refusal = fate.err();
    drop(departed);

    // The boot the incident kills. `new` IS `restore_all_conversations`.
    let booted = ProductionParticipantHandler::new(Arc::clone(&store), marker_fixture_config())
        .map_err(|error| {
            format!(
                "F8 no-new-poison: the server did not reach listening after the incident \
                 sequence — boot failed with `{error}`. The live fate had answered \
                 {live_refusal:?}. {arming}"
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
             participant {} — the fate was never discharged. {arming}",
            roles.owner_participant
        )
        .into());
    }
    // §5.4 item 2: the discharge must not have been bought by releasing a
    // marker its owner has still not acked.
    if !marker_still_retained {
        return Err(format!(
            "F8 no-new-poison: the marker at {} was released although its owner never acked it. \
             {arming}",
            roles.marker_delivery_seq
        )
        .into());
    }
    Ok(())
}
