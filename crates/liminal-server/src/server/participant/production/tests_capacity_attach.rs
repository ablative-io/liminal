//! R-D1 stage-8 credential-attach capacity and provenance-pruning tests.
//!
//! Each test drives the live dispatch seam with real wire frames over a real
//! on-disk store and pins one register-row refusal of credential attach's
//! exact five-scope receipt/provenance order (row 5662), plus the
//! request-time expiry rules: pruned fingerprints free capacity, an exact
//! old token past its window degrades to `StaleOrUnknownReceipt`, and an
//! unknown old token inside the window keeps the `StaleAuthority` no-commit
//! proof.

use std::error::Error;
use std::thread::sleep;
use std::time::Duration;

use liminal_protocol::wire::{
    AttachSecret, ConnectionIncarnation, ReceiptCapacityExceeded, ReceiptCapacityScope,
    ServerValue, StaleAuthority, StaleOrUnknownReceipt,
};

use super::ProductionParticipantHandler;
use super::tests::{dispatch, open_disk_store_for_tests, test_participant_config};
use super::tests_capacity::capacity_config;
use super::tests_receipts::{GEN_ONE, attach, attach_request, detach, enroll, generation};

/// Asserts one exact credential-attach `ReceiptCapacityExceeded` row.
fn assert_attach_receipt_refusal(
    value: &ServerValue,
    conversation_id: u64,
    scope: ReceiptCapacityScope,
    limit: u64,
    occupied: u64,
) -> Result<(), Box<dyn Error>> {
    let ServerValue::ReceiptCapacityExceeded(ReceiptCapacityExceeded::CredentialAttach {
        request,
        scope: got_scope,
        limit: got_limit,
        occupied: got_occupied,
    }) = value
    else {
        return Err(format!(
            "expected the credential-attach ReceiptCapacityExceeded row ({scope:?}), got: \
             {value:?}"
        )
        .into());
    };
    assert_eq!(request.conversation_id, conversation_id);
    assert_eq!(*got_scope, scope);
    assert_eq!(*got_limit, limit);
    assert_eq!(*got_occupied, occupied);
    Ok(())
}

/// `LiveReceiptServer`: the live enrollment receipt fills the whole server
/// cap, so the first rotation refuses at the first scope in the fixed order.
#[test]
fn attach_live_receipt_server_scope_refusal() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(75, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = capacity_config(|c| c.max_live_attach_receipts_server = 1);
    let handler = ProductionParticipantHandler::new(store, config)?;
    let conversation_id = 741;

    let receipt = enroll(&handler, incarnation, conversation_id, [41; 16])?;
    let participant_id = receipt.participant_id();
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        GEN_ONE,
        [42; 16],
    )?;
    let refused = dispatch(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            GEN_ONE,
            receipt.attach_secret(),
            [43; 16],
        ),
    )?;
    assert_attach_receipt_refusal(
        &refused,
        conversation_id,
        ReceiptCapacityScope::LiveReceiptServer,
        1,
        1,
    )
}

/// `LiveReceiptParticipant`: the participant's own live enrollment receipt
/// fills its per-participant cap, so rotation refuses at the second scope
/// (the server scope has headroom).
#[test]
fn attach_live_receipt_participant_scope_refusal() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(76, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = capacity_config(|c| c.max_live_attach_receipts_per_participant = 1);
    let handler = ProductionParticipantHandler::new(store, config)?;
    let conversation_id = 742;

    let receipt = enroll(&handler, incarnation, conversation_id, [44; 16])?;
    let participant_id = receipt.participant_id();
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        GEN_ONE,
        [45; 16],
    )?;
    let refused = dispatch(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            GEN_ONE,
            receipt.attach_secret(),
            [46; 16],
        ),
    )?;
    assert_attach_receipt_refusal(
        &refused,
        conversation_id,
        ReceiptCapacityScope::LiveReceiptParticipant,
        1,
        1,
    )
}

/// `ProvenanceServer` / `ProvenanceConversation` / `ProvenanceParticipant`:
/// the retained enrollment fingerprint fills each scope's cap in turn; the
/// refusal names exactly the first full scope in the fixed order.
#[test]
fn attach_provenance_scope_refusals_follow_the_fixed_order() -> Result<(), Box<dyn Error>> {
    type Mutator = fn(&mut crate::config::types::ParticipantConfig);
    let cases: [(u64, Mutator, ReceiptCapacityScope); 3] = [
        (
            743,
            |c| c.max_receipt_provenance_server = 1,
            ReceiptCapacityScope::ProvenanceServer,
        ),
        (
            744,
            |c| c.max_receipt_provenance_per_conversation = 1,
            ReceiptCapacityScope::ProvenanceConversation,
        ),
        (
            745,
            |c| c.max_receipt_provenance_per_participant = 1,
            ReceiptCapacityScope::ProvenanceParticipant,
        ),
    ];
    for (conversation_id, mutate, scope) in cases {
        let home = tempfile::tempdir()?;
        let data_dir = home.path().join("durability");
        let incarnation = ConnectionIncarnation::new(77, 1);
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, capacity_config(mutate))?;

        let receipt = enroll(&handler, incarnation, conversation_id, [47; 16])?;
        let participant_id = receipt.participant_id();
        detach(
            &handler,
            incarnation,
            conversation_id,
            participant_id,
            GEN_ONE,
            [48; 16],
        )?;
        let refused = dispatch(
            &handler,
            incarnation,
            attach_request(
                conversation_id,
                participant_id,
                GEN_ONE,
                receipt.attach_secret(),
                [49; 16],
            ),
        )?;
        assert_attach_receipt_refusal(&refused, conversation_id, scope, 1, 1)?;
    }
    Ok(())
}

/// Full-cap refusal and the in-window no-commit proof, with LONG windows so
/// no scheduler jitter exists: the participant's two in-window fingerprints
/// (enrollment + first rotation) fill the per-participant cap and the second
/// rotation refuses at `ProvenanceParticipant`; an unknown token at the old
/// generation is provably absent from the complete in-window fingerprint set
/// and keeps the `StaleAuthority` no-commit proof.
#[test]
fn full_provenance_participant_scope_refuses_and_in_window_unknown_is_stale_authority()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(78, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    // Default long windows (60s receipt / 600s provenance); cap 2 holds the
    // enrollment fingerprint plus one rotation fingerprint.
    let config = capacity_config(|c| c.max_receipt_provenance_per_participant = 2);
    let handler = ProductionParticipantHandler::new(store, config)?;
    let conversation_id = 746;

    let receipt = enroll(&handler, incarnation, conversation_id, [50; 16])?;
    let participant_id = receipt.participant_id();
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        GEN_ONE,
        [51; 16],
    )?;
    let first = attach(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            GEN_ONE,
            receipt.attach_secret(),
            [52; 16],
        ),
    )?;
    assert_eq!(first.capability_generation(), generation(2)?);

    // The participant's in-window fingerprints (enrollment + rotation) fill
    // the cap: the second rotation refuses at ProvenanceParticipant.
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        generation(2)?,
        [53; 16],
    )?;
    let refused = dispatch(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            generation(2)?,
            first.attach_secret(),
            [54; 16],
        ),
    )?;
    assert_attach_receipt_refusal(
        &refused,
        conversation_id,
        ReceiptCapacityScope::ProvenanceParticipant,
        2,
        2,
    )?;

    // IN window: an unknown token at the old generation is provably absent
    // from the complete in-window fingerprint set — StaleAuthority.
    let unknown_in_window = dispatch(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            GEN_ONE,
            AttachSecret::new([0xAB; 32]),
            [55; 16],
        ),
    )?;
    let ServerValue::StaleAuthority(StaleAuthority::Live {
        current_generation, ..
    }) = unknown_in_window
    else {
        return Err(format!(
            "an unknown old token inside the fingerprint window must keep the StaleAuthority \
             no-commit proof, got: {unknown_in_window:?}"
        )
        .into());
    };
    assert_eq!(current_generation, generation(2)?);
    Ok(())
}

/// Out-of-model over-limit refusal with true numbers on the attach arm: two
/// retained enrollment fingerprints, then a restart whose server provenance
/// cap was lowered to 1 BENEATH that durable occupancy. Every earlier scope
/// in the frozen five-scope order has headroom, so the reconnecting attach
/// refuses at `ProvenanceServer` with the lowered limit and the true
/// occupancy — never admitting past the signed cap.
#[test]
fn attach_over_limit_scope_refuses_with_true_numbers() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let conversation_id = 748;
    let participant_id;
    let secret;

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
        let incarnation = ConnectionIncarnation::new(82, 1);
        let receipt = enroll(&handler, incarnation, conversation_id, [61; 16])?;
        participant_id = receipt.participant_id();
        secret = receipt.attach_secret();
        enroll(&handler, incarnation, 749, [62; 16])?;
    }

    // RESTART with the server provenance cap lowered beneath the two
    // retained in-window fingerprints; the client reconnects and attaches.
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = capacity_config(|c| c.max_receipt_provenance_server = 1);
    let handler = ProductionParticipantHandler::new(store, config)?;
    let refused = dispatch(
        &handler,
        ConnectionIncarnation::new(82, 2),
        attach_request(conversation_id, participant_id, GEN_ONE, secret, [63; 16]),
    )?;
    assert_attach_receipt_refusal(
        &refused,
        conversation_id,
        ReceiptCapacityScope::ProvenanceServer,
        1,
        2,
    )
}

/// Frozen first-full precedence across the model boundary on the attach
/// arm: `LiveReceiptServer` is exactly full IN model (two live enrollment
/// receipts against a cap of 2) while the LATER `ProvenanceServer` scope is
/// over-limit (cap lowered to 1 beneath 2 retained fingerprints). The
/// contract's suborder says the first full scope answers and no later
/// occupancy is disclosed, so the refusal must name `LiveReceiptServer`
/// with the live-receipt numbers — never the later provenance scope's.
#[test]
fn attach_mixed_full_and_over_limit_refuses_the_earlier_full_scope() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let conversation_id = 750;
    let participant_id;
    let secret;

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
        let incarnation = ConnectionIncarnation::new(83, 1);
        let receipt = enroll(&handler, incarnation, conversation_id, [64; 16])?;
        participant_id = receipt.participant_id();
        secret = receipt.attach_secret();
        enroll(&handler, incarnation, 751, [65; 16])?;
    }

    // RESTART: LiveReceiptServer exactly full in model (both enrollment
    // receipts are still inside their 60s window), ProvenanceServer
    // over-limit out of model.
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = capacity_config(|c| {
        c.max_live_attach_receipts_server = 2;
        c.max_receipt_provenance_server = 1;
    });
    let handler = ProductionParticipantHandler::new(store, config)?;
    let refused = dispatch(
        &handler,
        ConnectionIncarnation::new(83, 2),
        attach_request(conversation_id, participant_id, GEN_ONE, secret, [66; 16]),
    )?;
    assert_attach_receipt_refusal(
        &refused,
        conversation_id,
        ReceiptCapacityScope::LiveReceiptServer,
        2,
        2,
    )
}

/// Request-time expiry, late-safe half (sleeping longer only strengthens the
/// preconditions): once every provenance window has passed, the request-time
/// checks prune the retained fingerprints — a rotation that a full cap would
/// otherwise refuse ADMITS, and the exact old token degrades to the
/// intentionally ambiguous `StaleOrUnknownReceipt`, never the false
/// no-commit proof `StaleAuthority`.
#[test]
fn expired_provenance_prunes_freeing_capacity_and_degrading_exact_old_tokens()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(79, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    // Short windows (1s receipt / 1.2s provenance) waited out below; the cap
    // of 2 would refuse the second rotation if the expired fingerprints were
    // still counted.
    let config = capacity_config(|c| {
        c.attach_receipt_ttl_ms = 1_000;
        c.receipt_provenance_ttl_ms = 1_200;
        c.max_receipt_provenance_per_participant = 2;
    });
    let handler = ProductionParticipantHandler::new(store, config)?;
    let conversation_id = 747;

    let receipt = enroll(&handler, incarnation, conversation_id, [56; 16])?;
    let participant_id = receipt.participant_id();
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        GEN_ONE,
        [57; 16],
    )?;
    let first_request = attach_request(
        conversation_id,
        participant_id,
        GEN_ONE,
        receipt.attach_secret(),
        [58; 16],
    );
    let first = attach(&handler, incarnation, first_request.clone())?;
    assert_eq!(first.capability_generation(), generation(2)?);
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        generation(2)?,
        [59; 16],
    )?;

    // Wait out every provenance window; the request-time checks prune the
    // retained fingerprints, freeing the participant's capacity.
    sleep(Duration::from_millis(2_000));
    let second = attach(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            generation(2)?,
            first.attach_secret(),
            [60; 16],
        ),
    )?;
    assert_eq!(second.capability_generation(), generation(3)?);

    // The pruned exact old token is now intentionally indistinguishable from
    // an unknown one: StaleOrUnknownReceipt, never StaleAuthority.
    let exact_old = dispatch(&handler, incarnation, first_request)?;
    let ServerValue::StaleOrUnknownReceipt(StaleOrUnknownReceipt {
        presented_generation,
        current_generation,
        ..
    }) = exact_old
    else {
        return Err(format!(
            "a pruned exact old token must answer StaleOrUnknownReceipt, got: {exact_old:?}"
        )
        .into());
    };
    assert_eq!(presented_generation, GEN_ONE);
    assert_eq!(current_generation, generation(3)?);
    Ok(())
}
