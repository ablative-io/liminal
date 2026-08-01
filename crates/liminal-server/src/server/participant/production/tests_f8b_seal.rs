//! F8B `R-SEAL` (`docs/design/F8B-INTENT-DEADLOCK.md` §6.6).
//!
//! `release_drained_binding_slot` is the workspace's ONLY production site that
//! removes an enrollment token. A LIVE drain can never erase the last one —
//! `persist_drain_first`'s live caller is the record-admission `DrainFirst`
//! path, which requires a still-bound publisher whose own token survives. The
//! BOOT drain has no such floor, so when the restored lane's terminal belongs
//! to the conversation's LAST live identity the drain empties `tokens` while a
//! frontier survives, and replay refuses that shape outright
//! (`ops_session_replay.rs`, twin in the aggregate reference: *"durably empty
//! conversation rebuilt an executable frontier"*). The conversation becomes
//! durably unreplayable — every later cold touch and every later boot refuses.
//!
//! R-SEAL rules the derived closure: the same drain apply that erases the
//! final token retires the frontier and marks the authority Closed, live and
//! replay alike, with no second appended row and therefore no crash window.
//! Late arrivals then meet a NAMED refusal instead of silently re-opening a
//! conversation whose log holds records, terminals and drain rows.
//!
//! HOW THESE UNITS REACH THE LAST-IDENTITY SHAPE, through real transitions
//! only. `pending_restart_fixture` leaves the victim's binding terminal
//! pending in the lane with the peer still Bound. Boot #1 drains it (the peer's
//! token survives — no seal). The peer's own connection then dies, and with the
//! lane now clear its terminal PENDS where `tests_restore_window`'s
//! `second_pending_terminal_cannot_join_the_candidate_lane` measured
//! `Precedence` against the occupied lane. Draining THAT terminal erases the
//! conversation's last token. This is the same shape the `e2e_terminal_drain`
//! acceptance test reaches over real sockets, at unit granularity.

use std::error::Error;
use std::sync::Arc;

use liminal::durability::DurableStore;
use liminal::durability::bridge::block_on;
use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, EnrollmentRequest, EnrollmentToken, ServerValue,
};

use crate::server::participant::{
    ConnectionFateClass, ConnectionFateWorkItem, ParticipantConnectionContext,
    ParticipantConnectionConversations, ParticipantSemanticError, ParticipantSemanticHandler,
};

use super::ProductionParticipantHandler;
use super::boot_drain::BootDrainVerdict;
use super::log::{
    DecodedStoredOperation, OperationLog, OperationLogError, StoredOperation,
    StoredTerminalDisposition,
};
use super::state::DurableAppend;
use super::tests::test_participant_config;
use super::tests_w1b_pending_died_restart::pending_restart_fixture;

/// The retained `Open` sequence the sealed boot replays under.
const SEAL_OPEN_SEQUENCE: u64 = 409;

/// The write-side fixture bound `max_retained_record_rows = 4`; every reopen
/// must present the same shape for replay audits to hold.
fn seal_config() -> crate::config::types::ParticipantConfig {
    let mut config = test_participant_config();
    config.max_retained_record_rows = 4;
    config
}

/// Appends straight to one conversation's durable stream, exactly as
/// `tests_restore_window`'s multi-candidate pin does.
struct StoreAppender<'a> {
    log: &'a OperationLog,
}

impl DurableAppend for StoreAppender<'_> {
    fn append(
        &self,
        operation: &StoredOperation,
        expected_sequence: u64,
    ) -> Result<(), OperationLogError> {
        block_on(self.log.append(operation, expected_sequence))?
    }
}

/// Durable bytes whose sole surviving identity's binding terminal rests
/// pending in the immutable-candidate lane — the last-live-identity shape.
struct LastIdentityStore {
    store: Arc<dyn DurableStore>,
    conversation_id: u64,
    /// The connection incarnation the survivor was bound on, which the
    /// retained `Open` names.
    survivor_connection: ConnectionIncarnation,
    survivor_participant_id: u64,
    /// The handler that minted the shape. It was constructed BEFORE the
    /// survivor's terminal became durable, so it is the only seat from which
    /// the still-occupied lane can be drained by hand — constructing any new
    /// handler over these bytes IS the boot that drains them.
    minter: ProductionParticipantHandler,
}

impl LastIdentityStore {
    fn log(&self) -> OperationLog {
        OperationLog::new(Arc::clone(&self.store), self.conversation_id)
    }
}

fn last_identity_store() -> Result<LastIdentityStore, Box<dyn Error>> {
    let fixture = pending_restart_fixture()?;
    let store = Arc::clone(&fixture.handler.store);
    let conversation_id = fixture.conversation_id;
    let survivor_connection = fixture.peer_connection;
    let survivor_participant_id = fixture.peer_participant_id;
    drop(fixture);

    // Boot #1: the victim's pending terminal drains and its token is erased.
    // The survivor's token remains, so this drain does NOT seal.
    let minter = ProductionParticipantHandler::new(Arc::clone(&store), seal_config())?;
    let log = OperationLog::new(Arc::clone(&store), conversation_id);
    let survivors = token_count(&minter, conversation_id)?;
    if survivors != 1 {
        return Err(format!(
            "the last-identity fixture wanted exactly one surviving token after boot #1, \
             found {survivors} — the shape it exists to mint is gone"
        )
        .into());
    }

    // The sole survivor's connection dies. The lane is clear now, so its
    // terminal enters it instead of being refused for lane occupancy.
    let pending_sequence = {
        let cell = minter.cell(conversation_id)?;
        let mut owner = cell
            .lock()
            .map_err(|_| "last-identity owner lock was poisoned")?;
        let authority = owner
            .as_mut()
            .ok_or("last-identity owner was unavailable")?;
        let sequence = authority.next_log_sequence;
        authority
            .prepare_connection_fate_transaction(&ConnectionFateWorkItem {
                open_sequence: SEAL_OPEN_SEQUENCE,
                connection_incarnation: survivor_connection,
                class: ConnectionFateClass::ConnectionLost,
                tracked_conversations: Vec::new(),
            })
            .complete(authority, &StoreAppender { log: &log })?;
        drop(owner);
        sequence
    };

    // The fixture asserts its own prestate: the survivor's terminal must be
    // PENDING in the lane, not committed, or these units measure nothing.
    let Some(entry) = block_on(log.read_at(pending_sequence))?? else {
        return Err("the survivor's connection fate appended no row".into());
    };
    let DecodedStoredOperation::V3(StoredOperation::Died { row }) = entry.operation else {
        return Err("the survivor's connection fate did not append a Died row".into());
    };
    if row.participant_id != survivor_participant_id {
        return Err("the survivor's Died row names another participant".into());
    }
    if row.disposition != StoredTerminalDisposition::Pending {
        return Err(format!(
            "the survivor's terminal committed instead of pending ({:?}) — the \
             last-identity shape is no longer mintable this way",
            row.disposition
        )
        .into());
    }

    Ok(LastIdentityStore {
        store,
        conversation_id,
        survivor_connection,
        survivor_participant_id,
        minter,
    })
}

/// Counts the enrollment tokens the installed owner holds.
fn token_count(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
) -> Result<usize, Box<dyn Error>> {
    let cell = handler.cell(conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "token census owner lock was poisoned")?;
    let count = owner
        .as_ref()
        .ok_or("token census owner was absent")?
        .tokens
        .len();
    drop(owner);
    Ok(count)
}

/// §6.6 red-first unit 1. Boot-draining the LAST live identity's terminal must
/// leave a conversation that still replays: Closed, frontier retired, tokens
/// empty, marker set — and boot reaches listening.
///
/// Fails today — the drain erases the final token and leaves the frontier
/// standing, so `ProductionParticipantHandler::new` refuses the whole store
/// with *"durably empty conversation rebuilt an executable frontier"*.
#[test]
fn boot_draining_the_last_identity_seals_the_conversation() -> Result<(), Box<dyn Error>> {
    let minted = last_identity_store()?;

    // Boot reaches listening at all — today this is the STOP.
    let booted = ProductionParticipantHandler::new(Arc::clone(&minted.store), seal_config())?;

    // The installed authority is Closed: marker set, tokens empty, frontier
    // retired. All three, because the invariant clause is the conjunction.
    let cell = booted.cell(minted.conversation_id)?;
    let owner = cell.lock().map_err(|_| "sealed owner lock was poisoned")?;
    let authority = owner.as_ref().ok_or("sealed owner was absent")?;
    if !authority.is_closed() {
        return Err("the last-identity drain left the conversation unsealed".into());
    }
    if !authority.tokens.is_empty() {
        return Err("the sealed conversation retained enrollment tokens".into());
    }
    if authority.frontier().is_some() {
        return Err("the sealed conversation retained an executable frontier".into());
    }
    if authority.slots.contains_key(&minted.survivor_participant_id) {
        return Err("the sealed conversation retained the drained identity's slot".into());
    }
    drop(owner);

    // Closure is DERIVED, not appended: replaying the same durable bytes
    // rebuilds it, with no seal row of its own to be lost to a crash.
    let replayed = booted.replay_aggregate_reference(minted.conversation_id, &minted.log())?;
    if !replayed.is_closed() || replayed.frontier().is_some() || !replayed.tokens.is_empty() {
        return Err("replaying the sealed bytes did not rebuild the closure".into());
    }

    // Boot's own point: the retained Open completes at the exact seat
    // `ConnectionIncarnationAuthority::startup` drives it from.
    let consumer: &dyn ParticipantSemanticHandler = &booted;
    consumer
        .handle_connection_fate(ConnectionFateWorkItem {
            open_sequence: SEAL_OPEN_SEQUENCE,
            connection_incarnation: minted.survivor_connection,
            class: ConnectionFateClass::ConnectionLost,
            tracked_conversations: vec![minted.conversation_id],
        })
        .map_err(|error| {
            format!("the retained Open failed before Complete at the recovery consumer: {error}")
        })?;
    Ok(())
}

/// §6.6 verdict surface: the seal is a PROPERTY of a successful drain, carried
/// on `Drained` and logged with it as ONE event — never a fifth verdict, which
/// would let a log reader believe a seal happened without a drain.
///
/// The drain is driven at the minting handler's seat, whose lane still holds
/// the survivor's terminal; constructing a new handler over these bytes is
/// itself the boot that drains them, so this is the only seat where the
/// verdict for THIS drain can be read.
///
/// Fails today — the drain erases the last token without closing anything, so
/// the verdict reports `sealed: false`.
#[test]
fn the_boot_drain_verdict_names_the_seal() -> Result<(), Box<dyn Error>> {
    let minted = last_identity_store()?;
    let log = minted.log();
    let mut replayed = minted
        .minter
        .replay_aggregate_reference(minted.conversation_id, &log)?;

    let verdict = minted
        .minter
        .drain_restored_candidate_lane(minted.conversation_id, &mut replayed, &log);
    if verdict != (BootDrainVerdict::Drained {
        drains: 1,
        sealed: true,
    }) {
        return Err(format!(
            "the last-identity drain's verdict did not name the seal: {verdict:?}"
        )
        .into());
    }
    if !replayed.is_closed() {
        return Err("the drained authority carries no closed marker".into());
    }
    Ok(())
}

/// §6.6 red-first unit 2. A late arrival enrolling into a Closed conversation
/// meets a NAMED, TYPED refusal — never a silent re-open onto a log that holds
/// records, terminals and drain rows.
///
/// Fails today — the conversation is unreachable (boot refuses the store
/// outright), and nothing produces the refusal.
#[test]
fn enrollment_into_a_sealed_conversation_is_refused_by_type() -> Result<(), Box<dyn Error>> {
    let minted = last_identity_store()?;
    let booted = ProductionParticipantHandler::new(Arc::clone(&minted.store), seal_config())?;

    let refused = booted.handle(
        ParticipantConnectionContext::new(ConnectionIncarnation::new(0xF5, 1)),
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: minted.conversation_id,
            enrollment_token: EnrollmentToken::new([0xF6; 16]),
        }),
    );
    let Err(ParticipantSemanticError::ConversationSealed { conversation_id }) = refused else {
        return Err(format!(
            "enrollment into the sealed conversation did not answer with the typed \
             ConversationSealed refusal: {refused:?}"
        )
        .into());
    };
    if conversation_id != minted.conversation_id {
        return Err(format!(
            "the sealed refusal named conversation {conversation_id}, not {}",
            minted.conversation_id
        )
        .into());
    }
    Ok(())
}

/// The refusal is reachable ONLY through closure: enrollment on every other
/// conversation is untouched, so a sealed conversation cannot quietly become a
/// server-wide refusal.
///
/// Fails today for the same reason unit 2 does — boot refuses the store, so
/// there is no booted handler to ask.
#[test]
fn a_sealed_conversation_does_not_refuse_its_neighbours() -> Result<(), Box<dyn Error>> {
    let minted = last_identity_store()?;
    let booted = ProductionParticipantHandler::new(Arc::clone(&minted.store), seal_config())?;
    let neighbour = minted
        .conversation_id
        .checked_add(1)
        .ok_or("neighbour conversation id overflowed")?;
    let bound = booted.handle(
        ParticipantConnectionContext::new(ConnectionIncarnation::new(0xF7, 1)),
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: neighbour,
            enrollment_token: EnrollmentToken::new([0xF8; 16]),
        }),
    )?;
    if !matches!(bound, ServerValue::EnrollBound(_)) {
        return Err(format!("a sealed neighbour refused an unrelated enrollment: {bound:?}").into());
    }
    Ok(())
}
