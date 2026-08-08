//! Board #12's remainder: the credential-attach marker-proof site stops
//! asserting a durable fact it never measured.
//!
//! `ops_attach_lookup::attach_marker_proof_state` used to hand the frozen
//! marker-proof selector a hardcoded `accepted_marker_at_cursor = false`. The
//! live marker-ack site had the same hardcoded `false` and it was NOT inert
//! there — it killed a kernel on 2026-08-07 — so it now computes the flag from
//! the retained marker-record census. This file is the attach site's half of
//! the same fix, and it carries the three obligations that make the change
//! safe rather than merely tidy:
//!
//! 1. **Truthful.** With a retained compaction marker of this participant
//!    sitting exactly at its cursor, the flag is `true`. This is the RED: it
//!    is `false` on the tree that hardcodes it.
//! 2. **Outcome-identical, proven BOTH ways.** On the credential-attach path
//!    the flag cannot change the selector's answer, because the only branch
//!    reading it also requires `input.is_marker_ack()`. That is asserted by
//!    measuring the selector with the flag both ways over the SAME state — and
//!    the same comparison is run with a marker-ack input, where the two states
//!    DO diverge, so the harness is proven able to see a difference before its
//!    "no difference" reading is believed.
//! 3. **Replay-identical.** Board #19 established that cold replay
//!    re-executes attaches, so an attach-path site is replay-reachable in
//!    general. This one is not: `replay_attached` goes straight to
//!    `attach_commit` and never enters `marker_bearing_attach_refusal`, so the
//!    replay path is correct BY SKIP. The pin below closes the loop the only
//!    way a test can — it drives a cold restart over the same durable store
//!    and asserts the marker-bearing attach is answered byte-identically
//!    before and after, with the marker census live on both sides.

use std::error::Error;

use liminal_protocol::lifecycle::{
    MarkerProofDecision, MarkerProofInput, MarkerProofState, select_marker_proof,
};
use liminal_protocol::wire::{
    AttachAttemptToken, AttachSecret, ClientRequest, CredentialAttachRequest, MarkerAck,
    ParticipantAck, ServerValue,
};

use super::ProductionParticipantHandler;
use super::ops_attach_lookup::attach_marker_proof_state;
use super::tests::dispatch;
use super::tests_marker_ack_fixture::{
    MarkerFixture, attached_marker_fixture, marker_fixture_config, marker_fixture_facts,
};
use super::tests_marker_fate_repro::{assert_armed_post_attach, offer_marker_and_read_live_epoch};

/// Everything a marker-bearing attach against the armed fixture needs to get
/// PAST the lookup stage and actually reach the marker-proof site.
struct ArmedCensus {
    fixture: MarkerFixture,
    marker_seq: u64,
    generation_value: u64,
    /// The target's live attach secret, read from its own slot. A test that
    /// invents one is refused at the lookup stage with `StaleAuthority` and
    /// never reaches the site under test at all.
    attach_secret: AttachSecret,
}

/// Drives the armed marker fixture to the exact state this file is about: the
/// target participant's cursor sitting ON its own retained compaction marker.
fn fixture_with_cursor_on_its_marker() -> Result<ArmedCensus, Box<dyn Error>> {
    let fixture = attached_marker_fixture()?;
    let epoch = offer_marker_and_read_live_epoch(&fixture)?;
    assert_armed_post_attach(epoch)?;
    let conversation_id = fixture.marker_delivery.conversation_id;
    let marker_seq = fixture.marker_delivery.delivery_seq;

    // The cumulative ordinary ack EXACTLY through the marker. Crossing
    // includes equality, so the cursor lands AT the marker — the only state in
    // which `accepted_marker_at_cursor` has anything to be true about.
    let crossing = dispatch(
        &fixture.handler,
        fixture.target_connection,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id,
            participant_id: fixture.target_participant,
            capability_generation: epoch.capability_generation,
            through_seq: marker_seq,
        }),
    )?;
    if !matches!(crossing, ServerValue::AckCommitted(_)) {
        return Err(format!(
            "NOT ARMED: the ordinary ack through the marker did not commit; it answered \
             {crossing:?}, so the cursor never reached the marker and every assertion in this \
             file would be measuring an empty census."
        )
        .into());
    }
    let attach_secret = {
        let cell = fixture.handler.cell(conversation_id)?;
        let owner = cell
            .lock()
            .map_err(|_| "#12 census owner lock was poisoned")?;
        let authority = owner.as_ref().ok_or("#12 census owner was unavailable")?;
        let secret = authority
            .slots
            .get(&fixture.target_participant)
            .ok_or("#12 census target slot was absent")?
            .attach_secret;
        drop(owner);
        secret
    };
    Ok(ArmedCensus {
        fixture,
        marker_seq,
        generation_value: epoch.capability_generation.get(),
        attach_secret,
    })
}

/// Builds the marker-bearing credential attach these tests classify.
fn marker_bearing_attach(
    armed: &ArmedCensus,
    token_byte: u8,
) -> Result<CredentialAttachRequest, Box<dyn Error>> {
    Ok(CredentialAttachRequest {
        conversation_id: armed.fixture.marker_delivery.conversation_id,
        participant_id: armed.fixture.target_participant,
        capability_generation: liminal_protocol::wire::Generation::new(armed.generation_value)
            .ok_or("zero generation in the #12 census fixture")?,
        attach_secret: armed.attach_secret,
        attach_attempt_token: AttachAttemptToken::new([token_byte; 16]),
        accept_marker_delivery_seq: Some(armed.marker_seq),
    })
}

/// THE RED. The attach site's marker-proof state must report the durable
/// truth: a retained compaction marker of this participant sits at its cursor.
///
/// Fails on the tree that hardcodes `false`.
#[test]
fn the_attach_marker_proof_state_reports_the_retained_marker_at_the_cursor()
-> Result<(), Box<dyn Error>> {
    let armed = fixture_with_cursor_on_its_marker()?;
    let marker_seq = armed.marker_seq;
    let request = marker_bearing_attach(&armed, 0xD1)?;
    let config = marker_fixture_config();
    let facts = marker_fixture_facts(armed.fixture.target_connection, &config)?;

    let cell = armed
        .fixture
        .handler
        .cell(armed.fixture.marker_delivery.conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "#12 census owner lock was poisoned")?;
    let authority = owner.as_ref().ok_or("#12 census owner was unavailable")?;
    let slot = authority
        .slots
        .get(&armed.fixture.target_participant)
        .ok_or("#12 census target slot was absent")?;
    let state = attach_marker_proof_state(authority, &request, slot, &facts);

    assert_eq!(
        state.current_cursor(),
        marker_seq,
        "fixture precondition: the target's cursor is ON the marker"
    );
    assert!(
        state.accepted_marker_at_cursor(),
        "#12: the credential-attach marker-proof site must read the retained marker-record \
         census rather than asserting a hardcoded `false`. The target's cursor is at delivery \
         sequence {marker_seq}, which is its own retained compaction marker, and the site \
         reported that no marker had been accepted."
    );
    drop(owner);
    Ok(())
}

/// The truthful flag changes NO outcome on the credential-attach path — and
/// the harness that says so is proven able to detect a change.
///
/// `select_marker_proof` reads `accepted_marker_at_cursor` at exactly one
/// branch, and that branch also requires `input.is_marker_ack()`. So over the
/// same state, flipping the flag must leave a credential-attach input's
/// decision identical. The NEGATIVE half of that claim is worthless without
/// the positive control in the same test: the same two states, the same flip,
/// against a MARKER-ACK input, must produce DIFFERENT decisions. If the
/// control ever stops diverging, the "identical" reading above is measuring
/// nothing and the assertion says so.
#[test]
fn the_truthful_flag_cannot_change_the_credential_attach_outcome() -> Result<(), Box<dyn Error>> {
    let armed = fixture_with_cursor_on_its_marker()?;
    let request = marker_bearing_attach(&armed, 0xD2)?;
    let config = marker_fixture_config();
    let facts = marker_fixture_facts(armed.fixture.target_connection, &config)?;

    let cell = armed
        .fixture
        .handler
        .cell(armed.fixture.marker_delivery.conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "#12 census owner lock was poisoned")?;
    let authority = owner.as_ref().ok_or("#12 census owner was unavailable")?;
    let slot = authority
        .slots
        .get(&armed.fixture.target_participant)
        .ok_or("#12 census target slot was absent")?;
    let truthful = attach_marker_proof_state(authority, &request, slot, &facts);
    drop(owner);

    assert!(
        truthful.accepted_marker_at_cursor(),
        "this test only means something while the flag is genuinely TRUE"
    );
    let hardcoded = MarkerProofState::new(
        truthful.current_cursor(),
        false,
        truthful.expected_marker_delivery_seq(),
        truthful.proof_binding_epoch(),
        truthful.delivered_to_proof_epoch(),
    );

    let attach_input =
        MarkerProofInput::credential_attach(&request).ok_or("the attach presents no marker")?;
    assert_eq!(
        select_marker_proof(&truthful, attach_input.clone()),
        select_marker_proof(&hardcoded, attach_input),
        "#12: truing up `accepted_marker_at_cursor` changed a credential-attach outcome. It \
         must not: the only branch reading the flag also requires `input.is_marker_ack()`, and \
         a credential attach is not a marker ack."
    );

    // POSITIVE CONTROL, through the SAME predicate. A marker-ack input over
    // the identical pair of states must diverge, or the equality above is
    // vacuous.
    let ack_input = MarkerProofInput::marker_ack(&MarkerAck {
        conversation_id: armed.fixture.marker_delivery.conversation_id,
        participant_id: armed.fixture.target_participant,
        capability_generation: request.capability_generation,
        marker_delivery_seq: armed.marker_seq,
    });
    let ack_truthful = select_marker_proof(&truthful, ack_input.clone());
    let ack_hardcoded = select_marker_proof(&hardcoded, ack_input);
    assert!(
        matches!(ack_truthful, MarkerProofDecision::AckNoOp(_)),
        "control: the truthful flag must make a marker-ack at the cursor a no-op, got \
         {ack_truthful:?}"
    );
    assert_ne!(
        ack_truthful, ack_hardcoded,
        "CONTROL FAILED: flipping the flag changed nothing even for a marker-ack input, so the \
         equality asserted above for credential attach proves nothing at all."
    );
    Ok(())
}

/// Replay is untouched, end to end.
///
/// Board #19's hazard is that cold replay re-executes attaches, so a change to
/// an attach-path site can silently change what a boot rebuilds. This site is
/// safe by SKIP — `replay_attached` calls `attach_commit` directly and never
/// enters the refusal mapper this file changes — and the way to close that
/// loop rather than assert it is to make a boot happen: a second handler over
/// the SAME durable store replays the whole conversation from rows, and the
/// marker-bearing attach must be answered exactly as the live handler answered
/// it, with the census live on both sides.
#[test]
fn a_cold_replay_answers_the_marker_bearing_attach_identically() -> Result<(), Box<dyn Error>> {
    let armed = fixture_with_cursor_on_its_marker()?;
    let live = dispatch(
        &armed.fixture.handler,
        armed.fixture.target_connection,
        ClientRequest::CredentialAttach(marker_bearing_attach(&armed, 0xD3)?),
    )?;

    // A fresh handler over the same store owns no in-memory authority, so the
    // first touch cold-replays the conversation from its durable rows.
    let rebooted =
        ProductionParticipantHandler::new(armed.fixture.store.clone(), marker_fixture_config())?;
    let replayed = dispatch(
        &rebooted,
        armed.fixture.target_connection,
        ClientRequest::CredentialAttach(marker_bearing_attach(&armed, 0xD4)?),
    )?;

    let (
        ServerValue::MarkerMismatch(live_mismatch),
        ServerValue::MarkerMismatch(replayed_mismatch),
    ) = (&live, &replayed)
    else {
        return Err(format!(
            "the marker-bearing attach must be refused with a marker mismatch on both sides \
             (no delivery pump exists, so no marker can be expected): live={live:?} \
             replayed={replayed:?}"
        )
        .into());
    };
    assert_eq!(
        live_mismatch.mismatch, replayed_mismatch.mismatch,
        "#12/#19: a cold replay answered the marker-bearing attach differently from the live \
         handler. The attempt tokens differ by construction; the classification must not."
    );

    // And the census the change added is genuinely live on the replayed side —
    // otherwise this test would agree because BOTH sides measured nothing.
    let config = marker_fixture_config();
    let facts = marker_fixture_facts(armed.fixture.target_connection, &config)?;
    let request = marker_bearing_attach(&armed, 0xD5)?;
    let cell = rebooted.cell(armed.fixture.marker_delivery.conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "#12 replayed owner lock was poisoned")?;
    let authority = owner.as_ref().ok_or("#12 replayed owner was unavailable")?;
    let slot = authority
        .slots
        .get(&armed.fixture.target_participant)
        .ok_or("#12 replayed target slot was absent")?;
    let state = attach_marker_proof_state(authority, &request, slot, &facts);
    assert!(
        state.accepted_marker_at_cursor(),
        "the retained marker-record census must survive the boot, or this test's agreement is \
         two empty measurements agreeing with each other"
    );
    drop(owner);
    Ok(())
}
