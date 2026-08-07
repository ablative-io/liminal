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
    AttachAttemptToken, ClientRequest, ConnectionIncarnation, CredentialAttachRequest,
    EnrollBound, EnrollmentRequest, EnrollmentToken, Generation, RecordAdmission,
    RecordAdmissionAttemptToken, RecordCommitted, ServerValue,
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

    // A fresh admission still allocates, and the original token still answers
    // the first commit after an intervening commit.
    let fresh = require_committed(dispatch(
        &handler,
        cold_connection,
        admission(
            conversation_id,
            &member,
            attached.capability_generation(),
            RecordAdmissionAttemptToken::new([0x54; 16]),
            vec![0x99],
        ),
    )?)?;
    assert!(fresh.delivery_seq() > first.delivery_seq());
    let again = require_committed(dispatch(
        &handler,
        cold_connection,
        admission(
            conversation_id,
            &member,
            attached.capability_generation(),
            token,
            payload,
        ),
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

/// PIN 4 — the guard: same token + DIFFERENT payload bytes bypasses dedup
/// and commits a NEW record; it is specifically never answered with the
/// first commit, which would silently discard the changed body. The
/// server-side warning is a diagnostic, not the mechanism, and is not
/// asserted here (no capture harness in this suite); the two-commits outcome
/// is the mechanism and is.
///
/// The pin also fixes the post-bypass map semantics: the token map mirrors
/// the MOST RECENT commit under a token (last-writer-wins). That choice is
/// load-bearing for the answer-lost class: after an edit slipped through
/// under a reused token, a lost answer and re-present of the EDITED bytes
/// must dedup against the edited commit — first-writer-wins would re-open
/// the field bug for exactly that record.
#[test]
fn same_token_different_body_commits_new_record_and_never_answers_first()
-> Result<(), Box<dyn Error>> {
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
    let edited = require_committed(dispatch(
        &handler,
        connection,
        admission(
            conversation_id,
            &member,
            Generation::ONE,
            token,
            edited_body.clone(),
        ),
    )?)?;
    assert!(
        edited.delivery_seq() > first.delivery_seq(),
        "a mismatched body must commit anew, never receive the first answer"
    );

    // Last-writer-wins: the edited commit now owns the token's dedup answer,
    // so an answer-lost re-present of the edited bytes dedups against it...
    let edited_replay = require_committed(dispatch(
        &handler,
        connection,
        admission(
            conversation_id,
            &member,
            Generation::ONE,
            token,
            edited_body,
        ),
    )?)?;
    assert_eq!(edited_replay.delivery_seq(), edited.delivery_seq());
    // ...while the ORIGINAL bytes under the same token are again a mismatch
    // and again commit anew — never a silent answer carrying either seq.
    let original_again = require_committed(dispatch(
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
    assert!(original_again.delivery_seq() > edited.delivery_seq());
    Ok(())
}
