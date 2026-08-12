//! R-D1 stage-8 credential-attach capacity and provenance-pruning tests.
//!
//! # Lane p0-39: every refusal this file pinned has been retired
//!
//! Credential attach used to walk five refusable receipt scopes (row 5662).
//! None of them refuse now: the three shared pools are TTL-bounded with a
//! reporting tripwire, and the two per-participant scopes are retention
//! windows that displace their own oldest member. Every pin below whose
//! assertion read a `ReceiptCapacityExceeded` row is rewritten to assert the
//! law that replaced it, and names the pin it replaces — deleting them would
//! have left these paths with no evidence at all.
//!
//! The request-time expiry rules are UNCHANGED and their pins stand: pruned
//! fingerprints free retention, an exact old token past its window degrades to
//! `StaleOrUnknownReceipt`, and an unknown old token inside the window keeps
//! the `StaleAuthority` no-commit proof.

use std::error::Error;
use std::thread::sleep;
use std::time::Duration;

use liminal_protocol::wire::{
    AttachSecret, ConnectionIncarnation, ServerValue, StaleAuthority, StaleOrUnknownReceipt,
};

use super::ProductionParticipantHandler;
use super::tests::{dispatch, open_disk_store_for_tests, test_participant_config};
use super::tests_capacity::capacity_config;
use super::tests_receipts::{
    GEN_ONE, attach, attach_request, detach, enroll, enroll_proving_provenance, generation,
};

/// Lane p0-39 REWRITE of `attach_live_receipt_server_scope_refusal`.
///
/// The shared server live-receipt pool filled by an unrelated enrollment used
/// to refuse the first rotation at the first scope in the fixed order. It
/// gates nothing now, and the rotation lands.
#[test]
fn shared_live_receipt_pool_never_refuses_a_rotation() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(75, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = capacity_config(|c| c.live_receipt_server_report_threshold = 1);
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
    let bound = attach(
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
    assert_eq!(bound.capability_generation(), generation(2)?);
    Ok(())
}

/// Lane p0-39 REWRITE of `attach_live_receipt_participant_scope_refusal`.
///
/// The old pin set the per-participant live-receipt cap to 1 and asserted the
/// rotation was refused — by the participant's OWN live enrollment receipt,
/// which that very rotation was about to end. A pure wedge. The window now
/// displaces, so the rotation lands; and because the commit's retire set
/// covers every live receipt the window counted, post-commit occupancy is
/// exactly one, which the SECOND rotation landing here demonstrates.
#[test]
fn full_participant_live_receipt_window_displaces_and_rotations_keep_landing()
-> Result<(), Box<dyn Error>> {
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
    let first = attach(
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
    assert_eq!(first.capability_generation(), generation(2)?);

    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        generation(2)?,
        [0x46; 16],
    )?;
    let second = attach(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            generation(2)?,
            first.attach_secret(),
            [0x47; 16],
        ),
    )?;
    assert_eq!(second.capability_generation(), generation(3)?);
    Ok(())
}

/// Lane p0-39 REWRITE of `attach_provenance_scope_refusals_follow_the_fixed_order`.
///
/// Three cases, each driving one provenance scope to its old cap of 1 through
/// an EARNED fingerprint, and each formerly asserting the named refusal. None
/// of the three refuses now — the two shared pools gate nothing, and the
/// per-participant window displaces — so all three rotations land. The earned
/// premise is kept: without the proving rotation the pools would be empty and
/// this would be a green bought by an untriggered fixture.
#[test]
fn no_provenance_scope_refuses_a_rotation_at_its_old_cap() -> Result<(), Box<dyn Error>> {
    type Mutator = fn(&mut crate::config::types::ParticipantConfig);
    let cases: [(u64, Mutator); 3] = [
        (743, |c| c.receipt_provenance_server_report_threshold = 1),
        (744, |c| {
            c.receipt_provenance_per_conversation_report_threshold = 1;
        }),
        (745, |c| c.max_receipt_provenance_per_participant = 1),
    ];
    for (conversation_id, mutate) in cases {
        let home = tempfile::tempdir()?;
        let data_dir = home.path().join("durability");
        let incarnation = ConnectionIncarnation::new(77, 1);
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, capacity_config(mutate))?;

        // Board #37: the rotation that PROVES possession of the enrollment
        // secret is what retains the fingerprint, so the cap of 1 is filled by
        // that first rotation and the SECOND one meets the full scope.
        let proven = enroll_proving_provenance(
            &handler,
            incarnation,
            conversation_id,
            [[47; 16], [147; 16], [247; 16]],
        )?;
        detach(
            &handler,
            incarnation,
            conversation_id,
            proven.participant_id,
            generation(2)?,
            [48; 16],
        )?;
        let bound = attach(
            &handler,
            incarnation,
            attach_request(
                conversation_id,
                proven.participant_id,
                generation(2)?,
                proven.attach_secret,
                [49; 16],
            ),
        )?;
        assert_eq!(bound.capability_generation(), generation(3)?);
    }
    Ok(())
}

/// Lane p0-39 REWRITE of
/// `full_provenance_participant_scope_refuses_and_in_window_unknown_is_stale_authority`.
///
/// Its first half asserted a `ProvenanceParticipant` refusal at a full window;
/// the window displaces now and the rotation lands. Its SECOND half is
/// unchanged law and is kept verbatim in effect: an unknown token at the old
/// generation, inside the window, is provably absent from the complete
/// in-window fingerprint set and still keeps the `StaleAuthority` no-commit
/// proof. Long windows, so no scheduler jitter exists.
#[test]
fn full_participant_window_displaces_and_in_window_unknown_is_still_stale_authority()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(78, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    // Default long windows (60s receipt / 600s provenance); a window of 2
    // holds the enrollment fingerprint plus one rotation fingerprint.
    let config = capacity_config(|c| c.max_receipt_provenance_per_participant = 2);
    let handler = ProductionParticipantHandler::new(store, config)?;
    let conversation_id = 746;

    let proven = enroll_proving_provenance(
        &handler,
        incarnation,
        conversation_id,
        [[50; 16], [51; 16], [52; 16]],
    )?;
    let participant_id = proven.participant_id;

    // Board #37: a second rotation proves possession of the FIRST rotation's
    // secret too, so the participant now holds two retained fingerprints
    // (enrollment + rotation one) and fills the window of 2.
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        generation(2)?,
        [53; 16],
    )?;
    let second = attach(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            generation(2)?,
            proven.attach_secret,
            [54; 16],
        ),
    )?;
    assert_eq!(second.capability_generation(), generation(3)?);

    // The third rotation meets the full window and DISPLACES rather than
    // refusing.
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        generation(3)?,
        [0x53; 16],
    )?;
    let third = attach(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            generation(3)?,
            second.attach_secret(),
            [0x54; 16],
        ),
    )?;
    assert_eq!(third.capability_generation(), generation(4)?);

    // IN window: an unknown token at the CURRENT-minus-one generation is
    // provably absent from the complete in-window fingerprint set for that
    // generation — the rotation from it is the one just committed, whose
    // fingerprint is retained — so the no-commit proof still holds.
    let unknown_in_window = dispatch(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            generation(3)?,
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
    assert_eq!(current_generation, generation(4)?);
    Ok(())
}

/// Lane p0-39 REWRITE of `attach_over_limit_scope_refuses_with_true_numbers`.
///
/// The old pin restarted with the SHARED server provenance cap lowered beneath
/// durable occupancy and asserted the reconnecting attach was refused with the
/// true numbers. That is exactly the boot wedge this lane exists to remove, and
/// the shape is now pinned in its repaired form: a shared pool lowered
/// underneath durable state refuses nothing, and a per-participant WINDOW
/// lowered underneath durable state simply displaces down to its new size on
/// the next insert.
#[test]
fn numbers_lowered_beneath_durable_state_displace_instead_of_refusing() -> Result<(), Box<dyn Error>>
{
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let conversation_id = 748;
    let participant_id;
    let secret;

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
        let incarnation = ConnectionIncarnation::new(82, 1);
        // Board #37: both fingerprints are earned, so the restart below really
        // does restore an occupancy of 2 from durable bytes — which is what
        // makes the lowered numbers out-of-model rather than merely tight.
        let proven = enroll_proving_provenance(
            &handler,
            incarnation,
            conversation_id,
            [[61; 16], [161; 16], [0xC1; 16]],
        )?;
        participant_id = proven.participant_id;
        secret = proven.attach_secret;
        enroll_proving_provenance(
            &handler,
            incarnation,
            749,
            [[62; 16], [162; 16], [0xC2; 16]],
        )?;
    }

    // RESTART with BOTH the shared pool and the participant window lowered
    // beneath retained durable occupancy.
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = capacity_config(|c| {
        c.receipt_provenance_server_report_threshold = 1;
        c.max_receipt_provenance_per_participant = 1;
    });
    let handler = ProductionParticipantHandler::new(store, config)?;
    let reconnect = ConnectionIncarnation::new(82, 2);
    let bound = attach(
        &handler,
        reconnect,
        attach_request(
            conversation_id,
            participant_id,
            generation(2)?,
            secret,
            [63; 16],
        ),
    )?;
    assert_eq!(bound.capability_generation(), generation(3)?);
    Ok(())
}

/// Lane p0-39 REWRITE of `attach_mixed_full_and_over_limit_refuses_the_earlier_full_scope`.
///
/// Both scopes that pin played off against each other — `LiveReceiptServer`
/// exactly full in model, `ProvenanceServer` over-limit out of model — are
/// SHARED pools, and neither refuses anything now, so the precedence question
/// it asked no longer has a subject on this arm. (The identity twin of that
/// question is still live and pinned in
/// [`super::tests_capacity::enrollment_mixed_full_and_over_limit_refuses_the_earlier_full_scope`].)
/// What is worth keeping is the composed state itself: with both shared pools
/// driven past their thresholds at once, the reconnecting attach still lands.
#[test]
fn several_shared_pools_past_their_thresholds_at_once_still_refuse_nothing()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let conversation_id = 750;
    let participant_id;
    let secret;

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
        let incarnation = ConnectionIncarnation::new(83, 1);
        let proven = enroll_proving_provenance(
            &handler,
            incarnation,
            conversation_id,
            [[64; 16], [164; 16], [0xC4; 16]],
        )?;
        participant_id = proven.participant_id;
        secret = proven.attach_secret;
        enroll_proving_provenance(
            &handler,
            incarnation,
            751,
            [[65; 16], [165; 16], [0xC5; 16]],
        )?;
    }

    let store = open_disk_store_for_tests(&data_dir)?;
    let config = capacity_config(|c| {
        c.live_receipt_server_report_threshold = 1;
        c.receipt_provenance_server_report_threshold = 1;
        c.receipt_provenance_per_conversation_report_threshold = 1;
    });
    let handler = ProductionParticipantHandler::new(store, config)?;
    let reconnect = ConnectionIncarnation::new(83, 2);
    let bound = attach(
        &handler,
        reconnect,
        attach_request(
            conversation_id,
            participant_id,
            generation(2)?,
            secret,
            [66; 16],
        ),
    )?;
    assert_eq!(bound.capability_generation(), generation(3)?);
    Ok(())
}

/// Request-time expiry, late-safe half (sleeping longer only strengthens the
/// preconditions): once every provenance window has passed, the request-time
/// checks prune the retained fingerprints and the exact old token degrades to
/// the intentionally ambiguous `StaleOrUnknownReceipt`, never the false
/// no-commit proof `StaleAuthority`.
///
/// Lane p0-39 keeps this pin: TTL expiry is now the SOLE bound on the shared
/// pools, so its evidence matters more than it did, not less. Its old
/// "freeing capacity" framing is gone with the cap it referred to — the
/// rotation below lands either way now — and what it still proves exactly is
/// the classification consequence of physical pruning.
#[test]
fn expired_provenance_prunes_and_degrades_exact_old_tokens() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(79, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    // Short windows (1s receipt / 1.2s provenance) waited out below.
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
    // retained fingerprints.
    sleep(Duration::from_secs(2));
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
