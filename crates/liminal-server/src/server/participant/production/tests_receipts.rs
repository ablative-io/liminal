//! Receipt/provenance production-path tests (gap-closure fix round).
//!
//! Each test drives the live dispatch seam with real wire frames over a real
//! on-disk store and pins the contract's bounded-provenance register rows:
//! independent enrollment/attach deadline pairs, the `result_generation`
//! carried by a deadline-provenance replay, the `Superseded` terminal reason
//! of a replaced receipt, and exact live-receipt replay with the invalidated
//! old secret (contract row 4). Deadline passage is real wall clock against
//! short configured TTLs — the production path reads its admitted clock, so
//! these tests wait out the signed windows rather than faking time.

use std::error::Error;
use std::thread::sleep;
use std::time::Duration;

use liminal_protocol::wire::{
    AttachAttemptToken, AttachBound, AttachSecret, ClientRequest, ConnectionIncarnation,
    CredentialAttachRequest, DetachAttemptToken, DetachRequest, EnrollmentRequest, EnrollmentToken,
    Generation, ReceiptExpired, ReceiptExpiryReason, ReceiptReplay, ServerValue,
    StaleOrUnknownReceipt,
};

use crate::config::types::ParticipantConfig;

use super::ProductionParticipantHandler;
use super::tests::{dispatch, open_disk_store_for_tests, test_participant_config};

/// Config whose receipt/provenance TTLs are short enough to wait out.
pub(super) const fn short_ttl_config(
    attach_receipt_ttl_ms: u64,
    receipt_provenance_ttl_ms: u64,
) -> ParticipantConfig {
    let mut config = test_participant_config();
    config.attach_receipt_ttl_ms = attach_receipt_ttl_ms;
    config.receipt_provenance_ttl_ms = receipt_provenance_ttl_ms;
    config
}

pub(super) fn enroll(
    handler: &ProductionParticipantHandler,
    incarnation: ConnectionIncarnation,
    conversation_id: u64,
    token: [u8; 16],
) -> Result<liminal_protocol::wire::EnrollBound, Box<dyn Error>> {
    let enrolled = dispatch(
        handler,
        incarnation,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new(token),
        }),
    )?;
    let ServerValue::EnrollBound(receipt) = enrolled else {
        return Err(format!("enrollment did not bind: {enrolled:?}").into());
    };
    Ok(receipt)
}

pub(super) fn detach(
    handler: &ProductionParticipantHandler,
    incarnation: ConnectionIncarnation,
    conversation_id: u64,
    participant_id: u64,
    generation: Generation,
    token: [u8; 16],
) -> Result<(), Box<dyn Error>> {
    let detached = dispatch(
        handler,
        incarnation,
        ClientRequest::Detach(DetachRequest {
            conversation_id,
            participant_id,
            capability_generation: generation,
            detach_attempt_token: DetachAttemptToken::new(token),
        }),
    )?;
    if !matches!(detached, ServerValue::DetachCommitted(_)) {
        return Err(format!("detach did not commit: {detached:?}").into());
    }
    Ok(())
}

pub(super) fn attach_request(
    conversation_id: u64,
    participant_id: u64,
    generation: Generation,
    secret: AttachSecret,
    token: [u8; 16],
) -> ClientRequest {
    ClientRequest::CredentialAttach(CredentialAttachRequest {
        conversation_id,
        participant_id,
        capability_generation: generation,
        attach_secret: secret,
        attach_attempt_token: AttachAttemptToken::new(token),
        accept_marker_delivery_seq: None,
    })
}

pub(super) fn attach(
    handler: &ProductionParticipantHandler,
    incarnation: ConnectionIncarnation,
    request: ClientRequest,
) -> Result<AttachBound, Box<dyn Error>> {
    let attached = dispatch(handler, incarnation, request)?;
    let ServerValue::AttachBound(receipt) = attached else {
        return Err(format!("attach did not bind: {attached:?}").into());
    };
    Ok(receipt)
}

pub(super) const GEN_ONE: Generation = Generation::ONE;

pub(super) fn generation(value: u64) -> Result<Generation, Box<dyn Error>> {
    Generation::new(value).ok_or_else(|| "zero generation in test fixture".into())
}

/// A deadline-provenance replay of an attach token carries the RESULT
/// generation (presented + 1), not the presented one.
#[test]
fn attach_deadline_provenance_replay_carries_result_generation() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(62, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    // Receipt dies after 300ms; provenance stays open long after.
    let handler = ProductionParticipantHandler::new(store, short_ttl_config(300, 600_000))?;
    let conversation_id = 602;

    let receipt = enroll(&handler, incarnation, conversation_id, [64; 16])?;
    let participant_id = receipt.participant_id();
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        GEN_ONE,
        [65; 16],
    )?;
    let attach_token = [66; 16];
    let request = attach_request(
        conversation_id,
        participant_id,
        GEN_ONE,
        receipt.attach_secret(),
        attach_token,
    );
    let attached = attach(&handler, incarnation, request.clone())?;
    assert_eq!(attached.capability_generation(), generation(2)?);
    // Wait out the receipt window, staying inside the provenance window.
    sleep(Duration::from_millis(500));

    let replayed = dispatch(&handler, incarnation, request)?;
    let ServerValue::ReceiptExpired(ReceiptExpired::CredentialAttach {
        token,
        presented_generation,
        result_generation,
        current_generation,
        reason,
        ..
    }) = replayed
    else {
        return Err(
            format!("expected the ReceiptExpired provenance row, got: {replayed:?}").into(),
        );
    };
    assert_eq!(token, AttachAttemptToken::new(attach_token));
    assert_eq!(presented_generation, GEN_ONE);
    assert_eq!(
        result_generation,
        generation(2)?,
        "the provenance row must carry the minted RESULT generation"
    );
    assert_eq!(current_generation, generation(2)?);
    assert_eq!(reason, ReceiptExpiryReason::Deadline);
    Ok(())
}

/// A receipt replaced by a newer rotation keeps a bounded provenance record:
/// inside its window the exact old token answers `ReceiptExpired` with reason
/// `Superseded`; after the window it answers `StaleOrUnknownReceipt` — never
/// the false no-commit proof `StaleAuthority`.
#[test]
fn superseded_receipt_keeps_provenance_then_degrades_to_stale_or_unknown()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(63, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    // Receipt window 2s, provenance window 2.5s: wide enough that the second
    // rotation lands inside the first receipt's live window even under full
    // parallel test-suite load (the Superseded reason depends on it), while
    // the after-window half stays a bounded sleep.
    let handler = ProductionParticipantHandler::new(store, short_ttl_config(2_000, 2_500))?;
    let conversation_id = 603;

    let receipt = enroll(&handler, incarnation, conversation_id, [67; 16])?;
    let participant_id = receipt.participant_id();
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        GEN_ONE,
        [68; 16],
    )?;
    let first_token = [69; 16];
    let first_request = attach_request(
        conversation_id,
        participant_id,
        GEN_ONE,
        receipt.attach_secret(),
        first_token,
    );
    let first = attach(&handler, incarnation, first_request.clone())?;
    assert_eq!(first.capability_generation(), generation(2)?);
    // Rotate again while the first receipt is still LIVE: detach the second
    // epoch, then attach with the second-generation secret.
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        generation(2)?,
        [70; 16],
    )?;
    let second = attach(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            generation(2)?,
            first.attach_secret(),
            [71; 16],
        ),
    )?;
    assert_eq!(second.capability_generation(), generation(3)?);

    // Inside the replaced receipt's provenance window: the exact committed
    // old token returns the exact ReceiptExpired payload with Superseded.
    let in_window = dispatch(&handler, incarnation, first_request.clone())?;
    let ServerValue::ReceiptExpired(ReceiptExpired::CredentialAttach {
        token,
        presented_generation,
        result_generation,
        current_generation,
        reason,
        ..
    }) = in_window
    else {
        return Err(format!(
            "superseded token inside its window must answer ReceiptExpired, got: {in_window:?}"
        )
        .into());
    };
    assert_eq!(token, AttachAttemptToken::new(first_token));
    assert_eq!(presented_generation, GEN_ONE);
    assert_eq!(result_generation, generation(2)?);
    assert_eq!(current_generation, generation(3)?);
    assert_eq!(reason, ReceiptExpiryReason::Superseded);

    // After the provenance window: exact-old degrades to the intentionally
    // ambiguous StaleOrUnknownReceipt (no no-commit claim). Sleeping late is
    // safe in this direction.
    sleep(Duration::from_millis(3_000));
    let after_window = dispatch(&handler, incarnation, first_request)?;
    let ServerValue::StaleOrUnknownReceipt(StaleOrUnknownReceipt {
        token,
        presented_generation,
        current_generation,
        ..
    }) = after_window
    else {
        return Err(format!(
            "superseded token after its window must answer StaleOrUnknownReceipt, got: \
             {after_window:?}"
        )
        .into());
    };
    assert_eq!(token, AttachAttemptToken::new(first_token));
    assert_eq!(presented_generation, GEN_ONE);
    assert_eq!(current_generation, generation(3)?);
    Ok(())
}

/// Lost-rotation recovery (contract row 4): the exact committed attach token
/// replays with the INVALIDATED old secret while its receipt is live and
/// returns the byte-identical committed result; a wrong secret on the same
/// token stays `StaleAuthority`.
#[test]
fn live_receipt_replays_with_invalidated_old_secret() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(64, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let conversation_id = 604;

    let receipt = enroll(&handler, incarnation, conversation_id, [72; 16])?;
    let participant_id = receipt.participant_id();
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        GEN_ONE,
        [73; 16],
    )?;
    let old_secret = receipt.attach_secret();
    let request = attach_request(
        conversation_id,
        participant_id,
        GEN_ONE,
        old_secret,
        [74; 16],
    );
    let attached = attach(&handler, incarnation, request.clone())?;
    assert_ne!(
        attached.attach_secret(),
        old_secret,
        "rotation must invalidate the presented secret"
    );

    // Exact replay with the invalidated OLD secret, same connection: the
    // origin slot still holds this binding epoch, so the replay is Bound and
    // byte-identical to the committed result.
    let replayed = dispatch(&handler, incarnation, request)?;
    let ServerValue::Bound(ReceiptReplay::CredentialAttach(replay)) = replayed else {
        return Err(format!("live receipt replay must answer Bound, got: {replayed:?}").into());
    };
    assert_eq!(
        replay, attached,
        "replay must be the exact committed result"
    );

    // Same token with a WRONG secret is StaleAuthority, not a replay.
    let forged = dispatch(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            GEN_ONE,
            AttachSecret::new([0xEE; 32]),
            [74; 16],
        ),
    )?;
    assert!(
        matches!(forged, ServerValue::StaleAuthority(_)),
        "wrong-secret replay of a live token must be StaleAuthority: {forged:?}"
    );
    Ok(())
}
