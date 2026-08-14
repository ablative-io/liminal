//! R18 amendment A7 (`PARTICIPANT-CONTRACT.md` §0.18) — the acceptance frame
//! of `OperatorCredentialReissue`, measured against the real production stack.
//!
//! Every pin here drives the live handler over a real on-disk haematite store
//! with real wire frames for the participant half and the real operator entry
//! point for the operator half. Nothing is stubbed and no lifecycle rule is
//! re-implemented.
//!
//! # The specimen's shape, and why the fossil is built the way it is
//!
//! Board #74, meridian `819dfdff`: `@compose` holds an identity whose attach
//! response was LOST and whose receipt then expired, so no hand ever received
//! the secret that attach minted. The binding died with the connection — not
//! by an explicit `Detach` frame — and that difference is load-bearing, not
//! cosmetic: an explicit detach leaves a `DetachCell::Committed` behind, and
//! the credential re-issue of an identity holding one would mint a credential
//! the ordinary re-entry attach REFUSES. That is its own pin below, and its
//! own flag back to the seat.
//!
//! # The clock is pinned, never waited out
//!
//! The fossil requires both receipt windows to have closed. Real windows are
//! signed TTLs, so these fixtures pin the participant clock
//! ([`ProductionParticipantHandler::pin_clock_ms`]) and step it, exactly as
//! `tests_receipts` does — the production path reads its admitted clock either
//! way, and a pinned reading makes the window states deterministic instead of
//! a race.

use std::error::Error;
use std::sync::Arc;

use liminal::durability::{DurableStore, bridge::block_on};
use liminal_protocol::lifecycle::{BindingState, DetachCell};
use liminal_protocol::wire::{
    AttachSecret, ClientRequest, ConnectionIncarnation, Generation, LeaveAttemptToken, LeaveRequest,
    ServerValue, StaleAuthority,
};

use crate::config::types::ParticipantConfig;
use crate::health::reissue::{
    OperatorCredentialReissueOutcome, OperatorCredentialReissueRefusal,
    OperatorCredentialReissueRequest, OperatorCredentialReissued,
};
use crate::server::participant::{
    ConnectionFateClass, ConnectionFateWorkItem, ParticipantSemanticHandler,
};

use super::ProductionParticipantHandler;
use super::state::ConversationAuthority;
use super::tests::{dispatch, open_disk_store_for_tests};
use super::tests_receipts::{
    GEN_ONE, attach, attach_request, detach, enroll, generation, short_ttl_config,
};

/// Pinned base reading for every fixture's admitted clock.
const BASE_MS: u64 = 1_770_000_000_000;
/// Signed attach-receipt window used by these fixtures.
const RECEIPT_TTL_MS: u64 = 60_000;
/// Signed provenance window used by these fixtures.
const PROVENANCE_TTL_MS: u64 = 600_000;
/// A reading past BOTH windows: the fossil's own moment.
const FOSSIL_MS: u64 = BASE_MS + PROVENANCE_TTL_MS + 1;

fn fossil_config() -> ParticipantConfig {
    short_ttl_config(RECEIPT_TTL_MS, PROVENANCE_TTL_MS)
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

/// The specimen, as this module reproduces it.
struct Fossil {
    participant_id: u64,
    /// Secret minted by the attach whose response was lost (generation 2).
    /// The client never received it; the fixture holds it only so the pins can
    /// present it and watch it be refused.
    lost_secret: AttachSecret,
}

/// Builds the fossil: an identity at generation 2, detached because its
/// connection died, with both receipt windows closed and no hand holding a
/// usable credential.
///
/// The steps are the specimen's own history: enroll (generation 1), detach,
/// attach presenting generation 1 (this is the attach whose response is lost —
/// it mints generation 2 and binds), then lose the connection. The clock is
/// left standing at [`FOSSIL_MS`], past both windows.
fn build_fossil(
    handler: &ProductionParticipantHandler,
    incarnation: ConnectionIncarnation,
    conversation_id: u64,
    tokens: [[u8; 16]; 3],
) -> Result<Fossil, Box<dyn Error>> {
    let [enrollment_token, detach_token, attach_token] = tokens;
    handler.pin_clock_ms(BASE_MS);
    let enrolled = enroll(handler, incarnation, conversation_id, enrollment_token)?;
    let participant_id = enrolled.participant_id();
    detach(
        handler,
        incarnation,
        conversation_id,
        participant_id,
        GEN_ONE,
        detach_token,
    )?;
    let lost = attach(
        handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            GEN_ONE,
            enrolled.attach_secret(),
            attach_token,
        ),
    )?;
    assert_eq!(lost.capability_generation(), generation(2)?);
    lose_the_connection(handler, incarnation, conversation_id)?;
    handler.pin_clock_ms(FOSSIL_MS);
    Ok(Fossil {
        participant_id,
        lost_secret: lost.attach_secret(),
    })
}

/// Kills the connection the identity is bound on, exactly as the specimen's
/// did.
///
/// This is deliberately NOT an explicit `Detach` frame: a connection-fate death
/// leaves the detach replay cell where the previous attach terminalized it,
/// while an explicit detach opens a fresh committed cell. The two produce the
/// same `BindingState::Detached` and different re-issue answers, which is the
/// whole point of `an_open_detach_replay_cell_refuses_rather_than_minting_a_trapped_credential`.
fn lose_the_connection(
    handler: &ProductionParticipantHandler,
    incarnation: ConnectionIncarnation,
    conversation_id: u64,
) -> Result<(), Box<dyn Error>> {
    handler.handle_connection_fate(ConnectionFateWorkItem {
        open_sequence: 1,
        connection_incarnation: incarnation,
        class: ConnectionFateClass::ConnectionLost,
        tracked_conversations: vec![conversation_id],
    })?;
    Ok(())
}

fn reissue_request(
    conversation_id: u64,
    participant_id: u64,
    expected_current_generation: u64,
) -> OperatorCredentialReissueRequest {
    OperatorCredentialReissueRequest {
        conversation_id,
        participant_id,
        expected_current_generation,
    }
}

/// Runs one re-issue and demands a COMMITTED answer.
fn expect_issued(
    handler: &ProductionParticipantHandler,
    request: OperatorCredentialReissueRequest,
) -> Result<OperatorCredentialReissued, Box<dyn Error>> {
    match handler.operator_credential_reissue(request)? {
        OperatorCredentialReissueOutcome::Issued(issued) => Ok(issued),
        OperatorCredentialReissueOutcome::Refused(refusal) => {
            Err(format!("re-issue was refused: {refusal:?}").into())
        }
    }
}

/// Runs one re-issue and demands a REFUSED answer.
fn expect_refused(
    handler: &ProductionParticipantHandler,
    request: OperatorCredentialReissueRequest,
) -> Result<OperatorCredentialReissueRefusal, Box<dyn Error>> {
    match handler.operator_credential_reissue(request)? {
        OperatorCredentialReissueOutcome::Refused(refusal) => Ok(refusal),
        OperatorCredentialReissueOutcome::Issued(issued) => {
            Err(format!("re-issue committed where it must refuse: {issued:?}").into())
        }
    }
}

/// The minted secret, back from its single wire rendering.
fn decode_hex(rendered: &str) -> Result<AttachSecret, Box<dyn Error>> {
    if rendered.len() != 64 {
        return Err(format!("issued secret is not sixty-four hex characters: {rendered}").into());
    }
    let mut bytes = [0_u8; 32];
    for (index, slot) in bytes.iter_mut().enumerate() {
        let start = index * 2;
        let pair = rendered
            .get(start..start + 2)
            .ok_or("issued secret hex is truncated")?;
        *slot = u8::from_str_radix(pair, 16)?;
    }
    Ok(AttachSecret::new(bytes))
}

// ---------------------------------------------------------------------------
// State census
// ---------------------------------------------------------------------------

/// Every stored entry in the node, as `(sequence, payload)` pairs.
type DurableCensus = Vec<(u64, Vec<u8>)>;

/// Every durable byte this node holds, as one comparable value.
///
/// Deliberately the WHOLE store and not the conversation's operation log: a
/// refusal that wrote a registry row, an outbox row, or an observer row would
/// be invisible to a census scoped to the log it did not touch. Entries are
/// sorted so the scan's stream-visit order cannot make two identical stores
/// compare unequal, while any change to the multiset of stored bytes still
/// shows.
fn durable_census(store: &Arc<dyn DurableStore>) -> Result<DurableCensus, Box<dyn Error>> {
    let mut entries: DurableCensus = block_on(store.scan(""))??
        .into_iter()
        .map(|entry| (entry.sequence, entry.payload))
        .collect();
    entries.sort_unstable();
    Ok(entries)
}

/// The volatile participant state a refusal must also leave alone.
///
/// A durable census alone would pass a build that mutated only the in-memory
/// owner — and the owner is RETAINED across a refusal (the refusal is an `Ok`,
/// so the handler does not discard and cold-replay it), which is exactly the
/// state a durable-only census is blind to.
#[derive(Clone, Debug, PartialEq, Eq)]
struct MemoryCensus {
    generation: u64,
    verifier: [u8; 32],
    binding: &'static str,
    detach_cell: &'static str,
    next_log_sequence: u64,
    next_order: u64,
    next_seq: u64,
    observer_progress: u64,
    enrollment_receipt_ended: bool,
    attach_receipt_result_generation: Option<u64>,
    attach_provenance_tokens: Vec<[u8; 16]>,
    slot_count: usize,
    retired_count: usize,
    token_count: usize,
}

fn census_of(authority: &ConversationAuthority, participant_id: u64) -> Option<MemoryCensus> {
    let slot = authority.slots.get(&participant_id)?;
    Some(MemoryCensus {
        generation: slot.member.generation().get(),
        verifier: slot.attach_secret.into_bytes(),
        binding: match slot.binding {
            BindingState::Detached => "detached",
            BindingState::Bound(_) => "bound",
            BindingState::PendingFinalization(_) => "pending_finalization",
        },
        detach_cell: match slot.cell {
            DetachCell::Empty(_) => "empty",
            DetachCell::Pending(_) => "pending",
            DetachCell::Committed(_) => "committed",
            DetachCell::Terminalized(_) => "terminalized",
        },
        next_log_sequence: authority.next_log_sequence,
        next_order: authority.next_order,
        next_seq: authority.next_seq,
        observer_progress: authority.observer_progress,
        enrollment_receipt_ended: slot.enrollment_receipt_ended.is_some(),
        attach_receipt_result_generation: slot
            .attach
            .as_ref()
            .map(|attach| attach.result_generation.get()),
        attach_provenance_tokens: slot.attach_provenance.keys().copied().collect(),
        slot_count: authority.slots.len(),
        retired_count: authority.retired.len(),
        token_count: authority.tokens.len(),
    })
}

/// Reads the live owner's census, or `None` when the identity is not present.
///
/// Reads THROUGH the handler's own cell so it observes the same owner the
/// operation just ran against; it installs nothing and replays nothing.
fn memory_census(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    participant_id: u64,
) -> Result<Option<MemoryCensus>, Box<dyn Error>> {
    let cell = handler.cell(conversation_id)?;
    let owner = cell.lock().map_err(|_| "census owner lock is poisoned")?;
    let census = owner
        .as_ref()
        .and_then(|authority| census_of(authority, participant_id));
    drop(owner);
    Ok(census)
}

/// One complete before/after census pair around a refusal.
struct Census {
    durable: DurableCensus,
    memory: Option<MemoryCensus>,
    registry_len: usize,
}

fn census(
    handler: &ProductionParticipantHandler,
    store: &Arc<dyn DurableStore>,
    conversation_id: u64,
    participant_id: u64,
) -> Result<Census, Box<dyn Error>> {
    Ok(Census {
        durable: durable_census(store)?,
        memory: memory_census(handler, conversation_id, participant_id)?,
        registry_len: handler.registry_len(),
    })
}

fn assert_unchanged(before: &Census, after: &Census, label: &str) {
    assert_eq!(
        before.durable, after.durable,
        "{label}: the refusal changed durable bytes"
    );
    assert_eq!(
        before.memory, after.memory,
        "{label}: the refusal changed live participant state"
    );
    assert_eq!(
        before.registry_len, after.registry_len,
        "{label}: the refusal changed the conversation registry"
    );
}

// ---------------------------------------------------------------------------
// Pin 1 — the end-to-end fossil shape (§0.18 acceptance 1)
// ---------------------------------------------------------------------------

/// THE LANE'S REASON, end to end.
///
/// The specimen's shape is built, proved to be sitting in the terminal entry
/// state §4 routes to `CredentialRecoveryLost` (`EnrollmentKnown` — the
/// enrollment-mapping replay resolves the live identity and hands back no
/// credential), reclaimed by one operator re-issue, and then re-entered by an
/// ORDINARY credential attach with the issued secret.
///
/// The three numbers §0.18 acceptance 1 names are asserted: G+1 issued, G+2
/// bound, and the dead secret answering `StaleAuthority` with the current
/// generation.
#[test]
fn the_fossil_shape_is_reclaimed_by_one_operator_reissue() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(741, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), fossil_config())?;
    let conversation_id = 7_401;
    let enrollment_token = [0xA1; 16];

    let fossil = build_fossil(
        &handler,
        incarnation,
        conversation_id,
        [enrollment_token, [0xA2; 16], [0xA3; 16]],
    )?;

    // ENTRY STATE. The enrollment-mapping replay resolves the live identity and
    // returns `EnrollmentKnown` — no receipt body, no provenance row, nothing an
    // SDK could attach with. This is precisely the state §4 routes into terminal
    // `CredentialRecoveryLost`, and it is where the specimen has been sitting.
    let entry = dispatch(
        &handler,
        incarnation,
        ClientRequest::Enrollment(liminal_protocol::wire::EnrollmentRequest {
            conversation_id,
            enrollment_token: liminal_protocol::wire::EnrollmentToken::new(enrollment_token),
        }),
    )?;
    assert!(
        matches!(entry, ServerValue::EnrollmentKnown(_)),
        "the fossil must sit in the EnrollmentKnown entry state that routes to \
         CredentialRecoveryLost, got: {entry:?}"
    );

    // THE REPAIR. One operator re-issue against the identity's current
    // generation.
    let issued = expect_issued(
        &handler,
        reissue_request(conversation_id, fossil.participant_id, 2),
    )?;
    assert_eq!(issued.presented_generation, 2);
    assert_eq!(
        issued.issued_generation, 3,
        "§0.18 acceptance 1: G+1 is issued"
    );
    let issued_secret = decode_hex(&issued.attach_secret)?;

    // RE-ENTRY. An ORDINARY R-C1 credential attach presenting G+1, which itself
    // checked-increments to G+2, rotates, and binds. A7 created no new attach
    // path: this is the same `ClientRequest::CredentialAttach` every member
    // uses.
    let bound = attach(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            fossil.participant_id,
            generation(3)?,
            issued_secret,
            [0xA4; 16],
        ),
    )?;
    assert_eq!(
        bound.capability_generation(),
        generation(4)?,
        "§0.18 acceptance 1: G+2 is bound"
    );

    // THE DEAD SECRET. The issued credential was consumed by the attach above,
    // so presenting it again is an old generation against a rotated identity and
    // answers the one `StaleAuthority` row carrying the current generation.
    let dead = dispatch(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            fossil.participant_id,
            generation(3)?,
            issued_secret,
            [0xA5; 16],
        ),
    )?;
    let ServerValue::StaleAuthority(StaleAuthority::Live {
        current_generation, ..
    }) = dead
    else {
        return Err(format!("the consumed secret must answer StaleAuthority, got: {dead:?}").into());
    };
    assert_eq!(current_generation, generation(4)?);

    // And the secret the lost response carried is dead too — it was already
    // invalidated by the attach that minted it, and the re-issue moved the
    // credential twice more past it.
    let lost = dispatch(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            fossil.participant_id,
            generation(2)?,
            fossil.lost_secret,
            [0xA6; 16],
        ),
    )?;
    assert!(
        !matches!(lost, ServerValue::AttachBound(_)),
        "the lost generation-2 secret must never bind again, got: {lost:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Pin 4 — the withheld control (§0.18 acceptance 4)
// ---------------------------------------------------------------------------

/// THE CONTROL that makes the amendment's necessity a measurement.
///
/// The same fossil, with the re-issue WITHHELD. Every credential the client or
/// the operator could still present is presented, including across a cold
/// restart, and the identity stays unreachable. Nothing in the pre-A7 build
/// repairs it — which is why A7 exists.
///
/// ⛔ This pin must stay green FOREVER. It goes red the moment some path other
/// than the operator operation starts handing out a usable credential.
#[test]
fn without_a_reissue_the_fossil_stays_refused_forever() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(742, 1);
    let conversation_id = 7_402;
    let enrollment_token = [0xB1; 16];
    let fossil;
    let enrollment_secret;

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, fossil_config())?;
        handler.pin_clock_ms(BASE_MS);
        let enrolled = enroll(&handler, incarnation, conversation_id, enrollment_token)?;
        enrollment_secret = enrolled.attach_secret();
        fossil = build_fossil_from(
            &handler,
            incarnation,
            conversation_id,
            &enrolled,
            [0xB2; 16],
            [0xB3; 16],
        )?;
        assert_refused_forever(
            &handler,
            incarnation,
            conversation_id,
            &fossil,
            enrollment_secret,
            [0xB4; 16],
            "live handler",
        )?;
    }

    // COLD RESTART: same bytes, a brand-new handler that has replayed them, and
    // the identity is exactly as unreachable. The refusal is a property of the
    // durable state, not of one process's memory.
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, fossil_config())?;
    handler.pin_clock_ms(FOSSIL_MS);
    assert_refused_forever(
        &handler,
        incarnation,
        conversation_id,
        &fossil,
        enrollment_secret,
        [0xB5; 16],
        "cold restart",
    )?;
    Ok(())
}

/// The fossil's remaining steps once enrollment has already happened.
fn build_fossil_from(
    handler: &ProductionParticipantHandler,
    incarnation: ConnectionIncarnation,
    conversation_id: u64,
    enrolled: &liminal_protocol::wire::EnrollBound,
    detach_token: [u8; 16],
    attach_token: [u8; 16],
) -> Result<Fossil, Box<dyn Error>> {
    let participant_id = enrolled.participant_id();
    detach(
        handler,
        incarnation,
        conversation_id,
        participant_id,
        GEN_ONE,
        detach_token,
    )?;
    let lost = attach(
        handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            GEN_ONE,
            enrolled.attach_secret(),
            attach_token,
        ),
    )?;
    lose_the_connection(handler, incarnation, conversation_id)?;
    handler.pin_clock_ms(FOSSIL_MS);
    Ok(Fossil {
        participant_id,
        lost_secret: lost.attach_secret(),
    })
}

/// Presents every credential THE CLIENT could still hold, and demands that none
/// of them binds.
///
/// ⛔ The possession set is the client's, not the server's. The fossil's
/// defining property is that no HAND holds a usable credential — the server
/// still holds the generation-2 secret in the slot, because that is what a
/// server does, and presenting it here would prove nothing except that a
/// credential nobody has still works. This pin was caught red doing exactly
/// that: the first draft presented `Fossil::lost_secret` and the attach BOUND,
/// which is correct behaviour and a worthless control.
fn assert_refused_forever(
    handler: &ProductionParticipantHandler,
    incarnation: ConnectionIncarnation,
    conversation_id: u64,
    fossil: &Fossil,
    enrollment_secret: AttachSecret,
    token_base: [u8; 16],
    label: &str,
) -> Result<(), Box<dyn Error>> {
    let attempts: [(Generation, AttachSecret); 3] = [
        // The generation-1 secret the enrollment receipt minted: the ONLY
        // credential this client ever received, and one the committed
        // generation-2 attach already invalidated.
        (GEN_ONE, enrollment_secret),
        // The same dead secret presented at the generation the client can
        // deduce it must be at by now.
        (generation(2)?, enrollment_secret),
        // A guess at the current generation with no secret at all.
        (generation(2)?, AttachSecret::new([0x00; 32])),
    ];
    for (index, (presented, secret)) in attempts.into_iter().enumerate() {
        let mut token = token_base;
        token[15] = u8::try_from(index)?;
        let answer = dispatch(
            handler,
            incarnation,
            attach_request(
                conversation_id,
                fossil.participant_id,
                presented,
                secret,
                token,
            ),
        )?;
        assert!(
            !matches!(answer, ServerValue::AttachBound(_)),
            "{label}: attempt {index} must not bind the withheld fossil, got: {answer:?}"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Pin 2 — every refusal, with a byte-identical census (§0.18 acceptance 2)
// ---------------------------------------------------------------------------

/// Pre-guard lookup miss 1: a conversation id that resolves to nothing.
///
/// The probe must also leave NO residue — no durable row, and no live registry
/// cell — which is the same property a refused participant-wire probe of an
/// unknown conversation already has.
#[test]
fn an_unknown_conversation_refuses_typed_and_leaves_no_residue() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(743, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), fossil_config())?;
    // A real conversation exists, so the refusal cannot be an artefact of an
    // empty node.
    let fossil = build_fossil(
        &handler,
        incarnation,
        7_403,
        [[0xC1; 16], [0xC2; 16], [0xC3; 16]],
    )?;
    let before = census(&handler, &store, 7_403, fossil.participant_id)?;

    let refusal = expect_refused(&handler, reissue_request(9_999, 0, 1))?;

    assert_eq!(
        refusal,
        OperatorCredentialReissueRefusal::ConversationUnknown {
            conversation_id: 9_999
        }
    );
    let after = census(&handler, &store, 7_403, fossil.participant_id)?;
    assert_unchanged(&before, &after, "unknown conversation");
    Ok(())
}

/// Pre-guard lookup miss 2: the conversation resolves, the identity does not.
#[test]
fn an_unknown_participant_refuses_typed_with_no_state_delta() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(744, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), fossil_config())?;
    let conversation_id = 7_404;
    let fossil = build_fossil(
        &handler,
        incarnation,
        conversation_id,
        [[0xC4; 16], [0xC5; 16], [0xC6; 16]],
    )?;
    let before = census(&handler, &store, conversation_id, fossil.participant_id)?;

    let refusal = expect_refused(&handler, reissue_request(conversation_id, 4_242, 2))?;

    assert_eq!(
        refusal,
        OperatorCredentialReissueRefusal::ParticipantUnknown {
            conversation_id,
            participant_id: 4_242
        }
    );
    let after = census(&handler, &store, conversation_id, fossil.participant_id)?;
    assert_unchanged(&before, &after, "unknown participant");
    Ok(())
}

/// Guard (a): re-issue never remints a retired identity.
///
/// The tombstone is a RESOLVED identity, not a lookup miss, so this pin also
/// proves the unknown-participant arm does not swallow it: a retired
/// participant is absent from `slots` and present in `retired`, and answering
/// `ParticipantUnknown` for it would be a lie the operator acts on.
#[test]
fn a_retired_identity_refuses_retired_with_no_state_delta() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(745, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), fossil_config())?;
    let conversation_id = 7_405;
    handler.pin_clock_ms(BASE_MS);
    let enrolled = enroll(&handler, incarnation, conversation_id, [0xC7; 16])?;
    let participant_id = enrolled.participant_id();
    let left = dispatch(
        &handler,
        incarnation,
        ClientRequest::Leave(LeaveRequest {
            conversation_id,
            participant_id,
            capability_generation: GEN_ONE,
            attach_secret: enrolled.attach_secret(),
            leave_attempt_token: LeaveAttemptToken::new([0xC8; 16]),
        }),
    )?;
    assert!(
        matches!(left, ServerValue::LeaveCommitted(_)),
        "the fixture's Leave must commit a tombstone, got: {left:?}"
    );
    let before = census(&handler, &store, conversation_id, participant_id)?;

    let refusal = expect_refused(&handler, reissue_request(conversation_id, participant_id, 1))?;

    assert_eq!(
        refusal,
        OperatorCredentialReissueRefusal::Retired {
            conversation_id,
            participant_id,
            retired_generation: 1
        }
    );
    let after = census(&handler, &store, conversation_id, participant_id)?;
    assert_unchanged(&before, &after, "retired identity");
    Ok(())
}

/// Guard (b): a bound member is demonstrably operating under working authority,
/// and re-issue against it would be seat revocation.
#[test]
fn a_live_binding_refuses_with_no_state_delta() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(746, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), fossil_config())?;
    let conversation_id = 7_406;
    handler.pin_clock_ms(BASE_MS);
    let enrolled = enroll(&handler, incarnation, conversation_id, [0xC9; 16])?;
    let participant_id = enrolled.participant_id();
    let before = census(&handler, &store, conversation_id, participant_id)?;
    assert_eq!(
        before.memory.as_ref().map(|census| census.binding),
        Some("bound"),
        "the fixture must actually be bound, or this pin measures nothing"
    );

    let refusal = expect_refused(&handler, reissue_request(conversation_id, participant_id, 1))?;

    assert_eq!(
        refusal,
        OperatorCredentialReissueRefusal::LiveBinding {
            conversation_id,
            participant_id,
            current_generation: 1,
            binding_state: "bound"
        }
    );
    let after = census(&handler, &store, conversation_id, participant_id)?;
    assert_unchanged(&before, &after, "live binding");
    Ok(())
}

/// Guard (c), enrollment arm: a live receipt means the R-C0 recovery window is
/// still open and the ordinary recovery path must be exhausted first.
#[test]
fn a_live_enrollment_receipt_refuses_with_no_state_delta() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(747, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), fossil_config())?;
    let conversation_id = 7_407;
    handler.pin_clock_ms(BASE_MS);
    let enrolled = enroll(&handler, incarnation, conversation_id, [0xCA; 16])?;
    let participant_id = enrolled.participant_id();
    // Lose the connection rather than detaching: guard (b) must be satisfied
    // (the slot is Detached) so this pin measures guard (c) and not guard (b).
    lose_the_connection(&handler, incarnation, conversation_id)?;
    let before = census(&handler, &store, conversation_id, participant_id)?;
    assert_eq!(
        before.memory.as_ref().map(|census| census.binding),
        Some("detached"),
        "guard (b) must already be satisfied, or this pin measures the wrong guard"
    );

    let refusal = expect_refused(&handler, reissue_request(conversation_id, participant_id, 1))?;

    assert_eq!(
        refusal,
        OperatorCredentialReissueRefusal::LiveReceipt {
            conversation_id,
            participant_id,
            current_generation: 1,
            receipt: "enrollment"
        }
    );
    let after = census(&handler, &store, conversation_id, participant_id)?;
    assert_unchanged(&before, &after, "live enrollment receipt");
    Ok(())
}

/// Guard (c), attach arm: the same law over the OTHER receipt.
///
/// The fossil is built and the re-issue is attempted BEFORE the clock steps
/// past the attach-receipt window, so the identity is exactly the specimen's
/// except that its recovery window is still open.
#[test]
fn a_live_attach_receipt_refuses_with_no_state_delta() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(748, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), fossil_config())?;
    let conversation_id = 7_408;
    handler.pin_clock_ms(BASE_MS);
    let enrolled = enroll(&handler, incarnation, conversation_id, [0xCB; 16])?;
    let participant_id = enrolled.participant_id();
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        GEN_ONE,
        [0xCC; 16],
    )?;
    attach(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            GEN_ONE,
            enrolled.attach_secret(),
            [0xCD; 16],
        ),
    )?;
    lose_the_connection(&handler, incarnation, conversation_id)?;
    // Inside the attach-receipt window, and past nothing.
    handler.pin_clock_ms(BASE_MS + RECEIPT_TTL_MS - 1);
    let before = census(&handler, &store, conversation_id, participant_id)?;

    let refusal = expect_refused(&handler, reissue_request(conversation_id, participant_id, 2))?;

    assert_eq!(
        refusal,
        OperatorCredentialReissueRefusal::LiveReceipt {
            conversation_id,
            participant_id,
            current_generation: 2,
            receipt: "attach"
        }
    );
    let after = census(&handler, &store, conversation_id, participant_id)?;
    assert_unchanged(&before, &after, "live attach receipt");
    Ok(())
}

/// Guard (d): the compare-and-set, and its NORMATIVE payload.
///
/// §0.18 item 4 makes the presented/current pair the lost-response repair
/// loop's only read path, so this pin asserts BOTH numbers rather than merely
/// that a refusal happened. It then closes the loop the contract describes: the
/// operator reads the current generation out of the refusal and repeats the
/// operation, which succeeds.
#[test]
fn a_generation_mismatch_refuses_carrying_the_repair_loops_read_path()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(749, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), fossil_config())?;
    let conversation_id = 7_409;
    let fossil = build_fossil(
        &handler,
        incarnation,
        conversation_id,
        [[0xCE; 16], [0xCF; 16], [0xD0; 16]],
    )?;
    let before = census(&handler, &store, conversation_id, fossil.participant_id)?;

    // The operator believes the identity is still at generation 1.
    let refusal = expect_refused(
        &handler,
        reissue_request(conversation_id, fossil.participant_id, 1),
    )?;

    let OperatorCredentialReissueRefusal::GenerationMismatch {
        presented_generation,
        current_generation,
        ..
    } = refusal
    else {
        return Err(format!("the compare-and-set must refuse by name, got: {refusal:?}").into());
    };
    assert_eq!(presented_generation, 1);
    assert_eq!(
        current_generation, 2,
        "§0.18 item 4: the refusal MUST carry the post-rotation generation, or a lost response \
         can never be repaired"
    );
    let after = census(&handler, &store, conversation_id, fossil.participant_id)?;
    assert_unchanged(&before, &after, "generation mismatch");

    // THE REPAIR LOOP the payload exists for: the operator now knows the real
    // generation and repeats the operation.
    let issued = expect_issued(
        &handler,
        reissue_request(conversation_id, fossil.participant_id, current_generation),
    )?;
    assert_eq!(issued.issued_generation, 3);
    Ok(())
}

/// A repeated operator call rotates AGAIN rather than replaying (§0.18 item 4),
/// and the second call's compare-and-set is what serializes them.
///
/// There is deliberately no receipt replay for operator issue, so this is the
/// documented behaviour and not a defect: the first call's presented generation
/// is stale by the time the second arrives.
#[test]
fn a_repeated_reissue_serializes_on_the_compare_and_set() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(750, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, fossil_config())?;
    let conversation_id = 7_410;
    let fossil = build_fossil(
        &handler,
        incarnation,
        conversation_id,
        [[0xD1; 16], [0xD2; 16], [0xD3; 16]],
    )?;

    let first = expect_issued(
        &handler,
        reissue_request(conversation_id, fossil.participant_id, 2),
    )?;
    assert_eq!(first.issued_generation, 3);

    // The identical call again: the compare-and-set refuses it instead of
    // double-rotating, and hands back the generation that makes the retry
    // correct.
    let repeated = expect_refused(
        &handler,
        reissue_request(conversation_id, fossil.participant_id, 2),
    )?;
    assert_eq!(
        repeated,
        OperatorCredentialReissueRefusal::GenerationMismatch {
            conversation_id,
            participant_id: fossil.participant_id,
            presented_generation: 2,
            current_generation: 3
        }
    );

    // A deliberate second re-issue rotates again, as the contract says it does.
    let second = expect_issued(
        &handler,
        reissue_request(conversation_id, fossil.participant_id, 3),
    )?;
    assert_eq!(second.issued_generation, 4);
    assert_ne!(
        first.attach_secret, second.attach_secret,
        "each re-issue must mint a FRESH secret"
    );
    Ok(())
}

/// ⚠ THE FLAG, pinned rather than described.
///
/// An identity whose last committed detach still holds its exact-replay cell
/// open is refused, because re-issuing it would mint a credential the ordinary
/// re-entry attach cannot use — `transition_detach_cell` requires the cell's
/// request generation to equal the member's, and a re-issue moves the member's.
///
/// This refusal is NOT one of §0.18's four guards. It is a defect this build
/// measured, and the red proof for it deletes the arm and watches the re-entry
/// attach die at `AttachCommitError::DetachCellAuthority`. The pin also proves
/// the shape is genuinely reachable — the census records the cell as
/// `committed` — so the guard is not answering a state that cannot happen.
#[test]
fn an_open_detach_replay_cell_refuses_rather_than_minting_a_trapped_credential()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(751, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), fossil_config())?;
    let conversation_id = 7_411;
    handler.pin_clock_ms(BASE_MS);
    let enrolled = enroll(&handler, incarnation, conversation_id, [0xD4; 16])?;
    let participant_id = enrolled.participant_id();
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        GEN_ONE,
        [0xD5; 16],
    )?;
    // Past both windows, so guard (c) is satisfied and this pin measures the
    // detach-cell arm rather than a live receipt.
    handler.pin_clock_ms(FOSSIL_MS);
    let before = census(&handler, &store, conversation_id, participant_id)?;
    assert_eq!(
        before.memory.as_ref().map(|census| census.detach_cell),
        Some("committed"),
        "the trapped shape must be REACHABLE, or this guard answers nothing"
    );

    let refusal = expect_refused(&handler, reissue_request(conversation_id, participant_id, 1))?;

    assert_eq!(
        refusal,
        OperatorCredentialReissueRefusal::DetachReplayOpen {
            conversation_id,
            participant_id,
            current_generation: 1
        }
    );
    let after = census(&handler, &store, conversation_id, participant_id)?;
    assert_unchanged(&before, &after, "open detach replay cell");

    // The ordinary re-entry path is still open at the UNMOVED generation, which
    // is what makes the refusal a repair rather than a dead end: the identity is
    // reachable by exactly the credential it always was.
    let bound = attach(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            GEN_ONE,
            enrolled.attach_secret(),
            [0xD6; 16],
        ),
    )?;
    assert_eq!(bound.capability_generation(), generation(2)?);
    Ok(())
}

// ---------------------------------------------------------------------------
// Pin 3 — replay equivalence across both crash boundaries (§0.18 acceptance 3)
// ---------------------------------------------------------------------------

/// The credential a cold replay reconstructs, read at the bytes.
fn replayed_credential(
    data_dir: &std::path::Path,
    conversation_id: u64,
    participant_id: u64,
) -> Result<(u64, [u8; 32]), Box<dyn Error>> {
    let store = open_disk_store_for_tests(data_dir)?;
    let handler = ProductionParticipantHandler::new(store, fossil_config())?;
    handler.pin_clock_ms(FOSSIL_MS);
    let census = memory_census(&handler, conversation_id, participant_id)?
        .ok_or("the replayed conversation lost its participant slot")?;
    Ok((census.generation, census.verifier))
}

/// §0.18 acceptance 3, BOTH boundaries of the re-issue row.
///
/// Crash immediately BEFORE the row: the replayed store reaches the pre-issue
/// state exactly. Crash immediately AFTER it: the replayed store reaches the
/// identical generation AND the identical verifier — the second half being the
/// one a generation-only assertion would miss, and the reason the verifier is
/// compared byte for byte against the secret the operator response carried.
#[test]
fn the_reissue_row_replays_identically_across_both_crash_boundaries()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(752, 1);
    let conversation_id = 7_412;
    let participant_id;
    let pre_issue;
    let issued;

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(Arc::clone(&store), fossil_config())?;
        let fossil = build_fossil(
            &handler,
            incarnation,
            conversation_id,
            [[0xE1; 16], [0xE2; 16], [0xE3; 16]],
        )?;
        participant_id = fossil.participant_id;
        pre_issue = memory_census(&handler, conversation_id, participant_id)?
            .ok_or("the fossil lost its participant slot")?;
    }

    // BOUNDARY 1 — crash immediately before the row. The durable log stops one
    // append short of the re-issue, and replay reaches the pre-issue state.
    let (generation_before, verifier_before) =
        replayed_credential(&data_dir, conversation_id, participant_id)?;
    assert_eq!(generation_before, pre_issue.generation);
    assert_eq!(verifier_before, pre_issue.verifier);
    assert_eq!(generation_before, 2);

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, fossil_config())?;
        handler.pin_clock_ms(FOSSIL_MS);
        issued = expect_issued(
            &handler,
            reissue_request(conversation_id, participant_id, 2),
        )?;
    }

    // BOUNDARY 2 — crash immediately after the row.
    let (generation_after, verifier_after) =
        replayed_credential(&data_dir, conversation_id, participant_id)?;
    assert_eq!(
        generation_after, 3,
        "the replayed store must reach the identical generation"
    );
    assert_eq!(
        AttachSecret::new(verifier_after),
        decode_hex(&issued.attach_secret)?,
        "the replayed store must reach the identical verifier -- the operator's one delivery of \
         the secret and the durable verifier are the same bytes"
    );
    assert_ne!(
        verifier_after, verifier_before,
        "the re-issue must have INVALIDATED the previous verifier"
    );

    // And the replayed credential is usable: a cold-started server admits the
    // ordinary re-entry attach, which is the whole point of replaying to the
    // identical verifier.
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, fossil_config())?;
    handler.pin_clock_ms(FOSSIL_MS);
    let bound = attach(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            generation(3)?,
            decode_hex(&issued.attach_secret)?,
            [0xE4; 16],
        ),
    )?;
    assert_eq!(bound.capability_generation(), generation(4)?);
    Ok(())
}

/// The re-issue row appends EXACTLY ONE durable row and no lifecycle record
/// (§0.18 item 3), measured over the log rather than asserted.
#[test]
fn the_reissue_appends_exactly_one_row_and_no_lifecycle_record() -> Result<(), Box<dyn Error>> {
    use super::log::{DecodedStoredOperation, OperationLog, StoredOperation};

    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(753, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), fossil_config())?;
    let conversation_id = 7_413;
    let fossil = build_fossil(
        &handler,
        incarnation,
        conversation_id,
        [[0xF1; 16], [0xF2; 16], [0xF3; 16]],
    )?;
    let log = OperationLog::new(Arc::clone(&store), conversation_id);
    let mut head = 0_u64;
    while block_on(log.read_at(head))??.is_some() {
        head += 1;
    }
    let outbox_before = block_on(
        super::outbox_log::OutboxLog::new(Arc::clone(&store), conversation_id).read_all(),
    )??
    .len();

    let issued = expect_issued(
        &handler,
        reissue_request(conversation_id, fossil.participant_id, 2),
    )?;

    let Some(decoded) = block_on(log.read_at(head))?? else {
        return Err("the re-issue appended no durable row".into());
    };
    let DecodedStoredOperation::V3(StoredOperation::CredentialReissued { row }) = decoded.operation
    else {
        return Err("the re-issue row is not a CredentialReissued v3 row".into());
    };
    assert_eq!(row.participant_id, fossil.participant_id);
    assert_eq!(row.presented_generation, 2);
    assert_eq!(row.issued_generation, 3);
    assert_eq!(
        AttachSecret::new(row.attach_secret_verifier),
        decode_hex(&issued.attach_secret)?
    );
    assert!(
        block_on(log.read_at(head + 1))??.is_none(),
        "§0.18 item 3: EXACTLY one row -- no Attached/Detached lifecycle record and no receipt row"
    );
    let outbox_after = block_on(
        super::outbox_log::OutboxLog::new(Arc::clone(&store), conversation_id).read_all(),
    )??
    .len();
    assert_eq!(
        outbox_before, outbox_after,
        "the re-issue binds nothing, so it produces no participant record and owes no extension row"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Pin 5 — the no-polling audit (§0.18 acceptance 5)
// ---------------------------------------------------------------------------

/// Executing wait/schedule shapes LAW-1 forbids, as source tokens.
const WAIT_SHAPES: [&str; 12] = [
    "sleep",
    "Instant::now",
    "SystemTime::now",
    "thread::spawn",
    "set_read_timeout",
    "recv_timeout",
    "try_recv",
    "park_timeout",
    "wait_timeout",
    "sweep_once",
    "sweep_interval",
    "yield_now",
];

/// Source with every `//` comment line removed, so the audit reads what RUNS.
///
/// The modules under audit talk ABOUT timers and sweeps at length — they have
/// to, since not having one is the property — and a raw grep would find those
/// sentences and report a violation that is a paragraph. Stripping comments is
/// what makes the predicate measure executable text.
fn executable_source(source: &str) -> String {
    source
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn wait_shapes_in(source: &str) -> Vec<&'static str> {
    let executable = executable_source(source);
    WAIT_SHAPES
        .into_iter()
        .filter(|shape| executable.contains(shape))
        .collect()
}

/// §0.18 acceptance 5: the operation introduces no timer, sweep, scan, or retry
/// loop. Repetition is operator-driven only.
///
/// ⛔ The POSITIVE CONTROL runs FIRST and through the SAME predicate. An
/// absence is a measurement of the instrument until something known-present has
/// been detected by it, and this audit's instrument is a comment-stripping
/// substring search that a careless edit could silently neuter.
#[test]
fn the_reissue_operation_introduces_no_timer_sweep_or_retry_loop() {
    // POSITIVE CONTROL: the main listener's accept loop is the estate's
    // canonical polling shape and is named as such in the contract's own
    // nonconforming inventory. If the predicate cannot see it, the predicate is
    // broken and the absences below mean nothing.
    let control = wait_shapes_in(include_str!("../../listener.rs"));
    assert!(
        !control.is_empty(),
        "the no-polling instrument detected nothing in a file that provably polls; every absence \
         it reports is meaningless until it can see a known-present case"
    );

    let operation = wait_shapes_in(include_str!("ops_reissue.rs"));
    assert_eq!(
        operation,
        Vec::<&str>::new(),
        "the A7 operation introduced an executing wait/schedule shape"
    );
    let surface = wait_shapes_in(include_str!("../../../health/reissue.rs"));
    assert_eq!(
        surface,
        Vec::<&str>::new(),
        "the A7 operator surface introduced an executing wait/schedule shape"
    );
}

/// The operation's own doc has to keep saying what it does not do.
///
/// A structural companion to the audit above: the audit proves the code has no
/// wait shape, and this proves the module still CLAIMS the property, so a
/// future edit cannot quietly drop the claim and leave the audit passing over a
/// file nobody reads.
#[test]
fn the_reissue_operation_still_declares_its_no_polling_property() {
    let source = include_str!("ops_reissue.rs");
    assert!(
        source.contains("No polling"),
        "the A7 operation dropped its LAW-1 declaration"
    );
}

