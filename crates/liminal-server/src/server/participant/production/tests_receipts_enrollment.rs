//! Enrollment-token receipt/provenance production-path tests.
//!
//! Split from [`super::tests_receipts`] under the 500-code-line lens; the
//! shared drivers live there. Each test drives the live dispatch seam with
//! real wire frames over a real on-disk store and pins the contract's
//! enrollment-token rows (register row 5652): the receipt's own deadline
//! pair, the `Deadline` and `Superseded` terminal reasons, and the permanent
//! lifetime mapping. Deadline passage is real wall clock against short
//! configured TTLs — the production path reads its admitted clock, so these
//! tests wait out the signed windows rather than faking time.

use std::error::Error;
use std::thread::sleep;
use std::time::Duration;

use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, EnrollmentKnown, EnrollmentRequest, EnrollmentToken,
    ReceiptExpired, ReceiptExpiryReason, ServerValue,
};

use super::ProductionParticipantHandler;
use super::tests::{dispatch, open_disk_store_for_tests, test_participant_config};
use super::tests_receipts::{
    GEN_ONE, attach, attach_request, detach, enroll, generation, short_ttl_config,
};

/// Enrollment provenance window (register row 5652), in-window half: an
/// exact enrollment-token replay AFTER the receipt's own deadline but INSIDE
/// its provenance window answers `ReceiptExpired` with reason `Deadline`,
/// the minted result generation, and the CURRENT generation — never a
/// resurrected generation-1 secret-bearing receipt, even while a later
/// attach's own receipt is live. The provenance TTL is deliberately huge so
/// scheduler jitter cannot carry the replay past the window.
#[test]
fn enrollment_token_replay_inside_provenance_window_is_receipt_expired()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(61, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    // Enrollment receipt dies after 300ms; provenance stays open long after.
    let handler = ProductionParticipantHandler::new(store, short_ttl_config(300, 600_000))?;
    let conversation_id = 601;
    let enrollment_token = [61; 16];

    let receipt = enroll(&handler, incarnation, conversation_id, enrollment_token)?;
    let participant_id = receipt.participant_id();
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        GEN_ONE,
        [62; 16],
    )?;
    // Wait out the enrollment receipt's own signed window, staying inside
    // its provenance window.
    sleep(Duration::from_millis(500));
    // A later attach opens a FRESH live window for its own receipt and moves
    // the current generation to 2.
    let attached = attach(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            GEN_ONE,
            receipt.attach_secret(),
            [63; 16],
        ),
    )?;
    assert_eq!(attached.capability_generation(), generation(2)?);

    // INSIDE the enrollment provenance window: the exact ReceiptExpired row
    // with reason Deadline, the minted result generation 1, and the current
    // generation 2 — the attach's live window must NOT re-open the expired
    // enrollment receipt.
    let in_window = dispatch(
        &handler,
        incarnation,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new(enrollment_token),
        }),
    )?;
    let ServerValue::ReceiptExpired(ReceiptExpired::Enrollment {
        conversation_id: expired_conversation,
        token,
        participant_id: expired_participant,
        result_generation,
        current_generation,
        reason,
    }) = in_window
    else {
        return Err(format!(
            "enrollment token inside its provenance window must answer ReceiptExpired, got: \
             {in_window:?}"
        )
        .into());
    };
    assert_eq!(expired_conversation, conversation_id);
    assert_eq!(token, EnrollmentToken::new(enrollment_token));
    assert_eq!(expired_participant, participant_id);
    assert_eq!(
        result_generation, GEN_ONE,
        "enrollment provenance retains the minted result generation"
    );
    assert_eq!(current_generation, generation(2)?);
    assert_eq!(reason, ReceiptExpiryReason::Deadline);
    Ok(())
}

/// Enrollment provenance window (register row 5652), after-window half:
/// once the provenance deadline has also passed, the permanent lifetime
/// mapping answers `EnrollmentKnown` with the current generation. Sleeping
/// past the window is jitter-safe in this direction (later is still after).
#[test]
fn enrollment_token_replay_after_provenance_window_is_enrollment_known()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(65, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    // Receipt window 300ms, provenance window 700ms.
    let handler = ProductionParticipantHandler::new(store, short_ttl_config(300, 700))?;
    let conversation_id = 605;
    let enrollment_token = [75; 16];

    let receipt = enroll(&handler, incarnation, conversation_id, enrollment_token)?;
    let participant_id = receipt.participant_id();
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        GEN_ONE,
        [76; 16],
    )?;
    // Wait out BOTH enrollment windows, then rotate to generation 2 (the
    // attach secret is credential authority, not receipt-window state).
    sleep(Duration::from_millis(900));
    let attached = attach(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            GEN_ONE,
            receipt.attach_secret(),
            [77; 16],
        ),
    )?;
    assert_eq!(attached.capability_generation(), generation(2)?);

    let after_window = dispatch(
        &handler,
        incarnation,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new(enrollment_token),
        }),
    )?;
    let ServerValue::EnrollmentKnown(EnrollmentKnown {
        conversation_id: known_conversation,
        participant_id: known_participant,
        current_generation,
        ..
    }) = after_window
    else {
        return Err(format!(
            "enrollment token after its provenance window must map to EnrollmentKnown, got: \
             {after_window:?}"
        )
        .into());
    };
    assert_eq!(known_conversation, conversation_id);
    assert_eq!(known_participant, participant_id);
    assert_eq!(current_generation, generation(2)?);
    Ok(())
}

/// Enrollment receipt supersession (contract R-C0, register row 5652), the
/// fast-reconnect ordering: a credential attach INSIDE the enrollment
/// receipt's live window ends the generation-1 receipt body, so an exact
/// enrollment-token replay answers the exact `ReceiptExpired` payload with
/// `result_generation: 1`, `current_generation: 2`, and reason `Superseded`
/// — never the `UnboundReceipt` replay carrying the invalidated
/// generation-1 secret. Both windows are deliberately long, so no sleep and
/// no jitter exist in this test.
#[test]
fn attach_inside_enrollment_receipt_window_supersedes_the_receipt() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(66, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    // Receipt 60s, provenance 600s: the attach lands well inside BOTH.
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let conversation_id = 606;
    let enrollment_token = [78; 16];

    let receipt = enroll(&handler, incarnation, conversation_id, enrollment_token)?;
    let participant_id = receipt.participant_id();
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        GEN_ONE,
        [79; 16],
    )?;
    // Rotate to generation 2 while the enrollment receipt is still live.
    let attached = attach(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            GEN_ONE,
            receipt.attach_secret(),
            [80; 16],
        ),
    )?;
    assert_eq!(attached.capability_generation(), generation(2)?);

    // In-window replay of the exact enrollment token: the supersession must
    // have ended the receipt body, so the invalidated generation-1 secret
    // payload (Bound / UnboundReceipt) is never re-served.
    let replayed = dispatch(
        &handler,
        incarnation,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new(enrollment_token),
        }),
    )?;
    let ServerValue::ReceiptExpired(ReceiptExpired::Enrollment {
        conversation_id: expired_conversation,
        token,
        participant_id: expired_participant,
        result_generation,
        current_generation,
        reason,
    }) = replayed
    else {
        return Err(format!(
            "an enrollment token superseded by a newer generation must answer ReceiptExpired \
             (never the invalidated secret payload), got: {replayed:?}"
        )
        .into());
    };
    assert_eq!(expired_conversation, conversation_id);
    assert_eq!(token, EnrollmentToken::new(enrollment_token));
    assert_eq!(expired_participant, participant_id);
    assert_eq!(
        result_generation, GEN_ONE,
        "the ended enrollment receipt minted generation 1"
    );
    assert_eq!(current_generation, generation(2)?);
    assert_eq!(
        reason,
        ReceiptExpiryReason::Superseded,
        "an attach inside the live receipt window records the exact Superseded reason"
    );
    Ok(())
}

/// Enrollment receipt supersession, after-window half: once the enrollment
/// provenance deadline has also passed, the permanent lifetime mapping
/// answers `EnrollmentKnown` — regardless of whether the receipt ended by
/// supersession or by its own deadline, so scheduler jitter around the
/// attach cannot change this test's assertion.
#[test]
fn superseded_enrollment_token_maps_to_enrollment_known_after_provenance()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(67, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    // Receipt 1500ms, provenance 1600ms: the attach normally lands inside
    // the live window (Superseded); the post-window assertion holds either
    // way.
    let handler = ProductionParticipantHandler::new(store, short_ttl_config(1_500, 1_600))?;
    let conversation_id = 607;
    let enrollment_token = [81; 16];

    let receipt = enroll(&handler, incarnation, conversation_id, enrollment_token)?;
    let participant_id = receipt.participant_id();
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        GEN_ONE,
        [82; 16],
    )?;
    let attached = attach(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            GEN_ONE,
            receipt.attach_secret(),
            [83; 16],
        ),
    )?;
    assert_eq!(attached.capability_generation(), generation(2)?);
    // Wait out the enrollment provenance window (sleeping late is safe: the
    // assertion is about AFTER the window).
    sleep(Duration::from_millis(2_000));

    let after_window = dispatch(
        &handler,
        incarnation,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new(enrollment_token),
        }),
    )?;
    let ServerValue::EnrollmentKnown(EnrollmentKnown {
        conversation_id: known_conversation,
        participant_id: known_participant,
        current_generation,
        ..
    }) = after_window
    else {
        return Err(format!(
            "a superseded enrollment token after its provenance window must map to \
             EnrollmentKnown, got: {after_window:?}"
        )
        .into());
    };
    assert_eq!(known_conversation, conversation_id);
    assert_eq!(known_participant, participant_id);
    assert_eq!(current_generation, generation(2)?);
    Ok(())
}
