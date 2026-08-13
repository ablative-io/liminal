//! A2 (§0.13) defensive-idempotence pins for ordinary record admission.
//!
//! Provenance: field 2026-08-08, conversation 6 — one staged message
//! committed twice (kernel op-log rows [1510]/[1513]) after the commit answer
//! died with the connection and the client re-presented the same bytes under
//! a rotated generation. Pin 1 is deliberately built the strong way: the
//! re-present runs against state COLD-REPLAYED from the durable log and
//! across a real generation rotation, because an in-memory re-present cannot
//! contain payload-fingerprint drift — the failure the guard exists to catch.
//! The window-honesty pin (a re-present arriving after its witness row is
//! compacted commits a second copy) is DECLARED, NOT ARMABLE: no server-side
//! op-log compaction exists today, so there is no mechanism to build it
//! against; the boundary is named in the amendment instead.

use std::error::Error;
use std::path::Path;

use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, ConnectionIncarnation, CredentialAttachRequest, EnrollBound,
    EnrollmentRequest, EnrollmentToken, Generation, RecordAdmission, RecordAdmissionAttemptToken,
    RecordCommitted, ServerValue,
};

use super::ProductionParticipantHandler;
use super::tests::{dispatch, open_disk_store_for_tests, test_participant_config};

fn open_handler(data_dir: &Path) -> Result<ProductionParticipantHandler, Box<dyn Error>> {
    let store = open_disk_store_for_tests(data_dir)?;
    Ok(ProductionParticipantHandler::new(
        store,
        test_participant_config(),
    )?)
}

fn require_enrolled(value: ServerValue) -> Result<EnrollBound, Box<dyn Error>> {
    let ServerValue::EnrollBound(receipt) = value else {
        return Err(format!("dedup fixture enrollment did not bind: {value:?}").into());
    };
    Ok(receipt)
}

fn require_committed(value: ServerValue) -> Result<RecordCommitted, Box<dyn Error>> {
    let ServerValue::RecordCommitted(committed) = value else {
        return Err(format!("dedup fixture admission did not commit: {value:?}").into());
    };
    Ok(committed)
}

fn admission(
    conversation_id: u64,
    member: &EnrollBound,
    generation: Generation,
    token: RecordAdmissionAttemptToken,
    payload: Vec<u8>,
) -> ClientRequest {
    ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id,
        participant_id: member.participant_id(),
        capability_generation: generation,
        record_admission_attempt_token: token,
        payload,
    })
}

/// PIN 1 — the field case, closed: same token + same bytes re-presented
/// across a cold replay of the durable log AND a generation rotation answers
/// the FIRST commit's `delivery_seq`/`sender_participant_id` and commits
/// nothing. The commits-nothing proof is the sequence equality itself: a real
/// commit always allocates a strictly larger `delivery_seq`, so an answer
/// carrying the first sequence cannot have allocated. The cold-replay
/// construction simultaneously proves the two halves the dedup rests on:
/// the payload fingerprint survives the log encode/decode/replay round-trip
/// (drift premise, server half), and the replay-side map rebuild observes
/// committed admissions from the durable rows, not process memory.
#[test]
fn same_token_same_bytes_across_cold_replay_and_rotation_answers_first_commit()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let conversation_id = 947;
    let live_connection = ConnectionIncarnation::new(94, 1);
    let handler = open_handler(&data_dir)?;
    let member = require_enrolled(dispatch(
        &handler,
        live_connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x51; 16]),
        }),
    )?)?;
    let token = RecordAdmissionAttemptToken::new([0x52; 16]);
    let payload = vec![0xB7, 0x03, 0x5C];
    let first = require_committed(dispatch(
        &handler,
        live_connection,
        admission(
            conversation_id,
            &member,
            Generation::ONE,
            token,
            payload.clone(),
        ),
    )?)?;

    // Live re-present first: the commit-side map insert must already answer
    // before any replay is involved.
    let live_replay = require_committed(dispatch(
        &handler,
        live_connection,
        admission(
            conversation_id,
            &member,
            Generation::ONE,
            token,
            payload.clone(),
        ),
    )?)?;
    assert_eq!(live_replay.delivery_seq(), first.delivery_seq());

    // Cold replay + rotation: the field trigger reproduced end to end.
    drop(handler);
    let handler = open_handler(&data_dir)?;
    let cold_connection = ConnectionIncarnation::new(94, 2);
    let attached = dispatch(
        &handler,
        cold_connection,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id,
            participant_id: member.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: member.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0x53; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    let ServerValue::AttachBound(attached) = attached else {
        return Err(format!("cold reattach did not bind: {attached:?}").into());
    };
    assert!(
        attached.capability_generation().get() > 1,
        "the pin requires a real rotation between the presentations"
    );
    let cold_replay = require_committed(dispatch(
        &handler,
        cold_connection,
        admission(
            conversation_id,
            &member,
            attached.capability_generation(),
            token,
            payload.clone(),
        ),
    )?)?;
    assert_eq!(cold_replay.delivery_seq(), first.delivery_seq());
    assert_eq!(
        cold_replay.sender_participant_id(),
        first.sender_participant_id()
    );

    assert_token_survives_an_intervening_commit(
        &handler,
        cold_connection,
        &member,
        attached.capability_generation(),
        token,
        payload,
        &first,
    )
}

/// A fresh admission still allocates, and the original token still answers the
/// first commit after an intervening commit.
fn assert_token_survives_an_intervening_commit(
    handler: &ProductionParticipantHandler,
    connection: ConnectionIncarnation,
    member: &EnrollBound,
    generation: Generation,
    token: RecordAdmissionAttemptToken,
    payload: Vec<u8>,
    first: &RecordCommitted,
) -> Result<(), Box<dyn Error>> {
    let conversation_id = first.request().conversation_id;
    let fresh = require_committed(dispatch(
        handler,
        connection,
        admission(
            conversation_id,
            member,
            generation,
            RecordAdmissionAttemptToken::new([0x54; 16]),
            vec![0x99],
        ),
    )?)?;
    assert!(fresh.delivery_seq() > first.delivery_seq());
    let again = require_committed(dispatch(
        handler,
        connection,
        admission(conversation_id, member, generation, token, payload),
    )?)?;
    assert_eq!(again.delivery_seq(), first.delivery_seq());
    Ok(())
}

/// PIN 2 — the positive control (field rows [1517]/[1520]): two
/// intent-distinct sends of the same body carry distinct attempt tokens and
/// MUST remain two commits. Any dedup keyed on payload bytes alone would
/// collapse them; this pin keeps that wrong answer dead.
#[test]
fn distinct_tokens_identical_bodies_commit_twice() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let conversation_id = 948;
    let connection = ConnectionIncarnation::new(94, 3);
    let handler = open_handler(&data_dir)?;
    let member = require_enrolled(dispatch(
        &handler,
        connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x55; 16]),
        }),
    )?)?;
    let body = vec![0x68, 0x69];
    let first = require_committed(dispatch(
        &handler,
        connection,
        admission(
            conversation_id,
            &member,
            Generation::ONE,
            RecordAdmissionAttemptToken::new([0x56; 16]),
            body.clone(),
        ),
    )?)?;
    let second = require_committed(dispatch(
        &handler,
        connection,
        admission(
            conversation_id,
            &member,
            Generation::ONE,
            RecordAdmissionAttemptToken::new([0x57; 16]),
            body,
        ),
    )?)?;
    assert!(second.delivery_seq() > first.delivery_seq());
    Ok(())
}

/// PIN 4 — the guard, re-derived against the PAIR-KEYED map (review
/// finding, Cally Ray 2026-08-08): same token + DIFFERENT payload bytes is a
/// DISTINCT identity and is specifically never answered with any prior
/// commit, which would silently discard the changed body.
///
/// ⚠ **SUPERSEDED IN PART by §0.15 amendment A4.** Under A2 this arm bypassed
/// dedup and committed a NEW record; A4 converts the SAME-participant half of
/// it into a typed `AttemptTokenBodyConflict::RecordAdmission` refusal that
/// commits nothing. What this pin still holds — and what A4 does not touch —
/// is the half that mattered to the review finding: the conflicting
/// presentation is NEVER answered with the first commit, and after it, BOTH
/// the original identity and any later distinct identity remain independently
/// re-presentable. Under the rejected token-only key an answer-lost
/// re-present of the ORIGINAL bytes duplicated — the field bug re-opened for
/// the original record — and that half is pinned here unchanged.
///
/// The commits-a-new-record half now lives in
/// `tests_a4_body_conflict::same_participant_same_token_different_body_refuses_and_commits_nothing`
/// as its refusal twin, and the CROSS-participant half (which A4 leaves
/// committing, forever) in
/// `tests_a4_body_conflict::a_cross_participant_token_hit_still_commits_with_no_refusal`
/// and in PIN 5 below.
#[test]
fn same_token_different_body_is_refused_and_never_answers_first() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let conversation_id = 949;
    let connection = ConnectionIncarnation::new(94, 4);
    let handler = open_handler(&data_dir)?;
    let member = require_enrolled(dispatch(
        &handler,
        connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x58; 16]),
        }),
    )?)?;
    let token = RecordAdmissionAttemptToken::new([0x59; 16]);
    let original_body = vec![0x01, 0x02, 0x03];
    let edited_body = vec![0x01, 0x02, 0x04];
    let first = require_committed(dispatch(
        &handler,
        connection,
        admission(
            conversation_id,
            &member,
            Generation::ONE,
            token,
            original_body.clone(),
        ),
    )?)?;
    let edited = dispatch(
        &handler,
        connection,
        admission(
            conversation_id,
            &member,
            Generation::ONE,
            token,
            edited_body.clone(),
        ),
    )?;
    assert!(
        matches!(edited, ServerValue::AttemptTokenBodyConflict(_)),
        "under A4 a mismatched body is refused, and under neither amendment \
         may it receive the first answer: {edited:?}"
    );

    // A repeat of the conflicting presentation is answered identically: the
    // refusal is stateless and wrote nothing to the map.
    let edited_again = dispatch(
        &handler,
        connection,
        admission(
            conversation_id,
            &member,
            Generation::ONE,
            token,
            edited_body,
        ),
    )?;
    assert_eq!(edited_again, edited);
    // An answer-lost re-present of the ORIGINAL bytes still dedups against
    // the original commit — the half that duplicated under a token-only key,
    // held here across the intervening refusal.
    let original_replay = require_committed(dispatch(
        &handler,
        connection,
        admission(
            conversation_id,
            &member,
            Generation::ONE,
            token,
            original_body,
        ),
    )?)?;
    assert_eq!(original_replay.delivery_seq(), first.delivery_seq());
    Ok(())
}

/// PIN 5 — the participant leg of the identity triple (review finding,
/// Cally Ray 2026-08-08, second pass): a DIFFERENT verified participant
/// presenting an already-committed (token, bytes) pair commits its OWN new
/// record — it can never be answered with the first participant's commit,
/// structurally, because it cannot hit a map entry not keyed to it — and
/// its commit EVICTS NOTHING: both participants' identities remain
/// independently re-presentable afterwards. Under the rejected pair key
/// (participant demoted to a checked field) the foreign commit overwrote
/// the original participant's answer and the original's honest answer-lost
/// re-present committed a second copy — the field bug re-opened across
/// participants.
#[test]
fn different_participant_same_token_and_bytes_commits_anew_and_evicts_nothing()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let conversation_id = 950;
    let first_connection = ConnectionIncarnation::new(94, 5);
    let second_connection = ConnectionIncarnation::new(94, 6);
    let handler = open_handler(&data_dir)?;
    let first_member = require_enrolled(dispatch(
        &handler,
        first_connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x5A; 16]),
        }),
    )?)?;
    let second_member = require_enrolled(dispatch(
        &handler,
        second_connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x5B; 16]),
        }),
    )?)?;
    let token = RecordAdmissionAttemptToken::new([0x5C; 16]);
    let body = vec![0x42, 0x43, 0x44];
    let first = require_committed(dispatch(
        &handler,
        first_connection,
        admission(
            conversation_id,
            &first_member,
            Generation::ONE,
            token,
            body.clone(),
        ),
    )?)?;

    // The second participant's presentation of the identical (token, bytes)
    // is a DISTINCT identity: a new commit with its own sender, never the
    // first participant's answer.
    let foreign = require_committed(dispatch(
        &handler,
        second_connection,
        admission(
            conversation_id,
            &second_member,
            Generation::ONE,
            token,
            body.clone(),
        ),
    )?)?;
    assert!(foreign.delivery_seq() > first.delivery_seq());
    assert_eq!(
        foreign.sender_participant_id(),
        second_member.participant_id()
    );

    // Nothing was evicted: BOTH identities still answer their own commits.
    let first_replay = require_committed(dispatch(
        &handler,
        first_connection,
        admission(
            conversation_id,
            &first_member,
            Generation::ONE,
            token,
            body.clone(),
        ),
    )?)?;
    assert_eq!(first_replay.delivery_seq(), first.delivery_seq());
    assert_eq!(
        first_replay.sender_participant_id(),
        first_member.participant_id()
    );
    let second_replay = require_committed(dispatch(
        &handler,
        second_connection,
        admission(
            conversation_id,
            &second_member,
            Generation::ONE,
            token,
            body,
        ),
    )?)?;
    assert_eq!(second_replay.delivery_seq(), foreign.delivery_seq());
    Ok(())
}
