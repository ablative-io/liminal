//! F8B `R-BOOT-DRAIN` and `R-BOOT-VERDICT`
//! (`docs/design/F8B-INTENT-DEADLOCK.md` §6.2).
//!
//! A crash-restored conversation can rest with a pending binding terminal in
//! its immutable-candidate lane. Boot recovery then replays the retained
//! connection-fate `Open`, which must admit a terminal of its own — and the
//! occupied lane refuses it (`Precedence`). Nothing in the boot path clears
//! the occupant, so the server can never start again. R-BOOT-DRAIN empties
//! the lane before the replay; R-BOOT-VERDICT names the outcome of every
//! attempt.
//!
//! WHERE THESE UNITS CUT THE BOOT CHAIN. `ConnectionIncarnationAuthority`
//! lives under `crate::server::connection` and is not reachable from the
//! participant tree, so unit 1 drives the two seats that authority drives, in
//! its order: `ProductionParticipantHandler::new` — which IS
//! `restore_all_conversations`, the seat R-BOOT-DRAIN lands in — and then
//! `<dyn ParticipantSemanticHandler>::handle_connection_fate`, the exact call
//! `connection/incarnation.rs:87-88` makes with `intent.work_item()`.
//! `incarnation.rs:89-96` is a bare `map_err` onto
//! `ServerError::ParticipantIncarnation { phase: "connection-fate handler
//! recovery" }`, and between that call returning `Ok` and the listener
//! binding lie only the durable `Complete` append and `finish_startup`,
//! neither of which reads the candidate lane. So a refusal at this seat IS
//! that boot failure, and an `Ok` here is the boot reaching listening. Unit 2
//! needs no fate at all: its lane holds markers, and boot alone must empty
//! it.

use std::error::Error;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal::durability::{DurableStore, open_ephemeral};
use liminal_protocol::lifecycle::ImmutableSequenceCandidate;
use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, ConnectionIncarnation, CredentialAttachRequest,
    EnrollmentRequest, EnrollmentToken, Generation, RecordAdmission, RecordAdmissionAttemptToken,
    ServerValue,
};

use crate::server::participant::{
    ConnectionFateClass, ConnectionFateWorkItem, ParticipantConnectionConversations,
    ParticipantSemanticError, ParticipantSemanticHandler,
};

use super::ProductionParticipantHandler;
use super::log::{DecodedStoredOperation, OperationLog, StoredOperation};
use super::state::ConversationAuthority;
use super::tests::{dispatch, dispatch_tracked, test_participant_config};
use super::tests_marker_ack_fixture::marker_fixture_config;
use super::tests_w1b_pending_died_restart::{PendingRestartFixture, pending_restart_fixture};

/// The durable `Open` sequence the restored boot replays under.
const BOOT_OPEN_SEQUENCE: u64 = 307;

/// Cold-boots the pending-terminal fixture's durable bytes into a fresh
/// handler — the exact `restore_all_conversations` seat R-BOOT-DRAIN lands in.
///
/// The write-side fixture bound `max_retained_record_rows = 4`; the booted
/// configuration must present the same shape for replay audits to hold.
fn boot_over(
    fixture: &PendingRestartFixture,
) -> Result<ProductionParticipantHandler, ParticipantSemanticError> {
    let mut config = test_participant_config();
    config.max_retained_record_rows = 4;
    ProductionParticipantHandler::new(Arc::clone(&fixture.handler.store), config)
}

/// Reads the lane of the owner boot actually installed. A conversation with no
/// coupled frontier owner has no lane and reports empty.
fn installed_lane(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
) -> Result<Vec<ImmutableSequenceCandidate>, Box<dyn Error>> {
    let cell = handler.cell(conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "boot-drain conversation owner lock was poisoned")?;
    let lane = owner
        .as_ref()
        .and_then(ConversationAuthority::frontier)
        .map(|frontier| {
            frontier
                .frontiers()
                .sequence()
                .immutable_candidates()
                .to_vec()
        })
        .unwrap_or_default();
    drop(owner);
    Ok(lane)
}

/// Reads every durable operation row on one conversation stream.
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

/// The work item the retained `Open` hands the recovery consumer. It names the
/// still-live peer connection, so completing it needs a binding terminal of its
/// own — which is exactly what the occupied lane refuses.
fn recovery_work_item(fixture: &PendingRestartFixture) -> ConnectionFateWorkItem {
    ConnectionFateWorkItem {
        open_sequence: BOOT_OPEN_SEQUENCE,
        connection_incarnation: fixture.peer_connection,
        class: ConnectionFateClass::ConnectionLost,
        tracked_conversations: vec![fixture.conversation_id],
    }
}

/// §6.2 red-first unit 1. A restored conversation whose lane holds one pending
/// binding terminal, with a retained `Open` still to replay: boot reaches
/// listening.
///
/// Fails today — boot leaves the lane occupied, so the replayed `Open` is
/// refused and the authority raises `ParticipantIncarnation { phase:
/// "connection-fate handler recovery" }`.
#[test]
fn boot_drains_a_pending_terminal_lane_and_reaches_listening() -> Result<(), Box<dyn Error>> {
    let fixture = pending_restart_fixture()?;
    let booted = boot_over(&fixture)?;

    // R-BOOT-DRAIN: the owner boot installed carries an empty lane.
    let lane = installed_lane(&booted, fixture.conversation_id)?;
    if !lane.is_empty() {
        return Err(format!("boot left the restored candidate lane occupied: {lane:?}").into());
    }

    // The drain is DURABLE, not an in-memory tidy: replaying the enlarged log
    // reproduces the emptied lane.
    let replayed = booted.replay_aggregate_reference(fixture.conversation_id, &fixture.log)?;
    let durable_lane = replayed.frontier().map_or_else(Vec::new, |frontier| {
        frontier
            .frontiers()
            .sequence()
            .immutable_candidates()
            .to_vec()
    });
    if !durable_lane.is_empty() {
        return Err(format!("the boot drain did not survive replay: {durable_lane:?}").into());
    }

    // R-BOOT-DRAIN's point: the retained Open now completes at the exact seat
    // `ConnectionIncarnationAuthority::startup` drives.
    let consumer: &dyn ParticipantSemanticHandler = &booted;
    consumer
        .handle_connection_fate(recovery_work_item(&fixture))
        .map_err(|error| {
            format!("the retained Open failed before Complete at the recovery consumer: {error}")
        })?;
    Ok(())
}

/// The conversation the two-marker fixture writes.
const MARKER_CONVERSATION: u64 = 4242;

/// Mints a lane holding TWO marker candidates through real dispatch only:
/// two enrolled members, the first attached, one ordinary record. Both
/// cursors then lag the retained floor that record chooses and neither holds a
/// surviving marker credit, so the single ordinary projection mints one marker
/// per overtaken identity (`claim_frontier.rs:2902-2907`, over
/// `ordinary_record_projection.rs:1210-1225`). Nothing acks and nothing else
/// is admitted afterwards, because the next admission or leave would drain the
/// head itself.
fn two_marker_store() -> Result<Arc<dyn DurableStore>, Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), marker_fixture_config())?;
    let first_connection = ConnectionIncarnation::new(0xB7, 1);
    let second_connection = ConnectionIncarnation::new(0xB7, 2);
    let ServerValue::EnrollBound(first) = dispatch(
        &handler,
        first_connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: MARKER_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0xB1; 16]),
        }),
    )?
    else {
        return Err("the two-marker fixture's first member did not enroll".into());
    };
    let ServerValue::EnrollBound(_second) = dispatch(
        &handler,
        second_connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: MARKER_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0xB2; 16]),
        }),
    )?
    else {
        return Err("the two-marker fixture's second member did not enroll".into());
    };
    let mut conversations = ParticipantConnectionConversations::default();
    let attached = dispatch_tracked(
        &handler,
        first_connection,
        &mut conversations,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: MARKER_CONVERSATION,
            participant_id: first.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: first.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0xB5; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    let ServerValue::AttachBound(attached) = attached else {
        return Err(format!("the two-marker fixture did not attach: {attached:?}").into());
    };
    let committed = dispatch_tracked(
        &handler,
        first_connection,
        &mut conversations,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: MARKER_CONVERSATION,
            participant_id: first.participant_id(),
            capability_generation: attached.origin_binding_epoch().capability_generation,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xB9; 16]),
            payload: vec![0xBA],
        }),
    )?;
    if !matches!(committed, ServerValue::RecordCommitted(_)) {
        return Err(format!("the two-marker fixture's record did not commit: {committed:?}").into());
    }

    // The fixture only proves what it is asserted to hold.
    let lane = installed_lane(&handler, MARKER_CONVERSATION)?;
    let [ImmutableSequenceCandidate::Marker(_), ImmutableSequenceCandidate::Marker(_)] =
        lane.as_slice()
    else {
        return Err(format!(
            "the two-marker fixture no longer mints exactly two marker candidates: {lane:?}"
        )
        .into());
    };
    drop(handler);
    Ok(store)
}

/// §6.2 red-first unit 2. A restored conversation whose lane holds two marker
/// candidates: boot empties the lane in two drains and reaches listening.
///
/// Fails today — no boot caller of the drain exists, so the restored lane
/// keeps both markers and no `MarkerDrained` row is ever appended.
#[test]
fn boot_empties_a_two_marker_lane_in_two_drains() -> Result<(), Box<dyn Error>> {
    let store = two_marker_store()?;
    let before = operation_rows(&store, MARKER_CONVERSATION)?;
    let drains_before = marker_drain_rows(&before);

    let booted = ProductionParticipantHandler::new(Arc::clone(&store), marker_fixture_config())?;

    let lane = installed_lane(&booted, MARKER_CONVERSATION)?;
    if !lane.is_empty() {
        return Err(format!("boot left the restored marker lane occupied: {lane:?}").into());
    }

    // N markers need N drains: the head is removed one at a time, so exactly
    // two durable marker-drain rows appear.
    let after = operation_rows(&store, MARKER_CONVERSATION)?;
    let drains = marker_drain_rows(&after)
        .checked_sub(drains_before)
        .ok_or("the boot drain removed durable marker-drain rows")?;
    if drains != 2 {
        return Err(format!("boot emptied the two-marker lane in {drains} drains, not two").into());
    }
    Ok(())
}

/// Counts the durable marker-drain rows in one conversation's log.
fn marker_drain_rows(rows: &[StoredOperation]) -> usize {
    rows.iter()
        .filter(|operation| matches!(**operation, StoredOperation::MarkerDrained { .. }))
        .count()
}
