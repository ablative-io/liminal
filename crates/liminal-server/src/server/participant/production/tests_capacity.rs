//! R-D1 stage-8 enrollment capacity production-path tests.
//!
//! Each test drives the live dispatch seam with real wire frames over a real
//! on-disk store and pins the enrollment identity capacity family (register
//! row 5655): the exact scope in the frozen order, the signed limit, and the
//! true occupancy — plus the cold-restart exactness of the server-scope
//! ledger (a restart must not forget reserved identity slots).
//!
//! # Lane p0-39: what these tests stopped being able to say
//!
//! Three RECEIPT scopes used to refuse enrollment here (`LiveReceiptServer`,
//! `ProvenanceServer`, `ProvenanceConversation`, register row 5654). They no
//! longer refuse anything, so the pins that walked them are rewritten below to
//! assert the law that replaced them — an honest arrival lands — rather than
//! deleted, which would have removed the only evidence anyone checks that
//! behaviour at all. Each rewrite names the pin it replaces.
//!
//! The IDENTITY scopes are untouched by that lane and their pins stand exactly
//! as they were. The credential attach scopes live in
//! [`super::tests_capacity_attach`].

use std::error::Error;

use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, EnrollmentRequest, EnrollmentToken,
    IdentityCapacityExceeded, IdentityCapacityScope, ServerValue,
};

use crate::config::types::ParticipantConfig;

use super::ProductionParticipantHandler;
use super::tests::{dispatch, open_disk_store_for_tests, test_participant_config};
use super::tests_receipts::{enroll, enroll_proving_provenance};

/// Deployment-shaped config with one capacity knob turned down.
pub(super) fn capacity_config(mutate: impl FnOnce(&mut ParticipantConfig)) -> ParticipantConfig {
    let mut config = test_participant_config();
    mutate(&mut config);
    config
}

fn enrollment_request(conversation_id: u64, token: [u8; 16]) -> ClientRequest {
    ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id,
        enrollment_token: EnrollmentToken::new(token),
    })
}

/// Server-scope identity capacity (register row 5655): the third identity
/// across the whole server refuses with scope `Server` (tested BEFORE the
/// conversation scope, whose per-conversation occupancy is far below its
/// limit) — and the refusal SURVIVES a cold restart, proving the startup
/// restore rebuilds the identity ledger from durable truth.
///
/// Lane p0-39 leaves this pin untouched: identity capacity is a GATE and
/// stays one.
#[test]
fn enrollment_identity_server_scope_refuses_and_survives_restart() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(71, 1);
    let config = capacity_config(|c| c.max_retired_identity_slots_server = 2);

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, config)?;
        enroll(&handler, incarnation, 701, [1; 16])?;
        enroll(&handler, incarnation, 702, [2; 16])?;
        let refused = dispatch(&handler, incarnation, enrollment_request(703, [3; 16]))?;
        let ServerValue::IdentityCapacityExceeded(IdentityCapacityExceeded {
            request,
            scope,
            limit,
            occupied,
        }) = refused
        else {
            return Err(format!(
                "third server-wide identity must refuse with IdentityCapacityExceeded, got: \
                 {refused:?}"
            )
            .into());
        };
        assert_eq!(request.conversation_id, 703);
        assert_eq!(scope, IdentityCapacityScope::Server);
        assert_eq!(limit, 2);
        assert_eq!(occupied, 2);
    }

    // COLD RESTART: the ledger is rebuilt from the durable conversation
    // streams alone; the server scope must still refuse.
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, config)?;
    let refused = dispatch(&handler, incarnation, enrollment_request(703, [3; 16]))?;
    let ServerValue::IdentityCapacityExceeded(IdentityCapacityExceeded {
        scope,
        limit,
        occupied,
        ..
    }) = refused
    else {
        return Err(format!(
            "the server identity scope must survive a cold restart, got: {refused:?}"
        )
        .into());
    };
    assert_eq!(scope, IdentityCapacityScope::Server);
    assert_eq!(limit, 2);
    assert_eq!(occupied, 2);
    Ok(())
}

/// Lane p0-39 REWRITE of `enrollment_live_receipt_server_scope_refuses_and_survives_restart`.
///
/// That pin asserted a `LiveReceiptServer` refusal and its survival across a
/// cold restart. The refusal is gone, so both of its assertions read a wire row
/// that can no longer be produced. What it was really guarding — that the
/// server-scope live-receipt ledger is rebuilt exactly from durable truth — is
/// preserved here in its non-refusing form: the pool is deliberately driven far
/// past its old cap, before and after a restart, and every honest arrival still
/// lands.
#[test]
fn shared_live_receipt_pool_never_refuses_before_or_after_a_restart() -> Result<(), Box<dyn Error>>
{
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(72, 1);
    let config = capacity_config(|c| c.live_receipt_server_report_threshold = 1);

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, config)?;
        enroll(&handler, incarnation, 711, [11; 16])?;
        // Third party, far past the old cap of one.
        enroll(&handler, incarnation, 712, [12; 16])?;
        enroll(&handler, incarnation, 713, [13; 16])?;
    }

    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, config)?;
    enroll(&handler, incarnation, 714, [14; 16])?;
    Ok(())
}

/// Lane p0-39 REWRITE of `enrollment_provenance_server_scope_refusal`.
///
/// The shared server provenance pool no longer gates, so the refusal that pin
/// asserted cannot occur. Its earned-fingerprint premise is kept — the fixture
/// still pays for a real retained fingerprint through a rotation, so this is
/// not a green bought by an empty pool.
#[test]
fn shared_server_provenance_pool_never_refuses_an_enrollment() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(73, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = capacity_config(|c| c.receipt_provenance_server_report_threshold = 1);
    let handler = ProductionParticipantHandler::new(store, config)?;

    enroll_proving_provenance(&handler, incarnation, 721, [[21; 16], [121; 16], [221; 16]])?;
    enroll(&handler, incarnation, 722, [22; 16])?;
    Ok(())
}

/// Lane p0-39 REWRITE of `enrollment_over_limit_scope_refuses_with_true_numbers`.
///
/// The out-of-model over-limit arm SURVIVES — a configured number lowered
/// beneath restored durable occupancy still refuses with its true numbers
/// rather than admitting past a signed cap — but only for the scopes that are
/// still gates. The original drove it through `ProvenanceServer`, which no
/// longer refuses; this drives the identical mechanism through the identity
/// server scope, whose cap is lowered to 1 beneath two durable identities.
#[test]
fn enrollment_over_limit_identity_scope_refuses_with_true_numbers() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(80, 1);

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
        enroll(&handler, incarnation, 751, [61; 16])?;
        enroll(&handler, incarnation, 752, [62; 16])?;
    }

    // RESTART with the server identity cap lowered beneath the two durable
    // identities.
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = capacity_config(|c| c.max_retired_identity_slots_server = 1);
    let handler = ProductionParticipantHandler::new(store, config)?;
    let refused = dispatch(&handler, incarnation, enrollment_request(753, [63; 16]))?;
    let ServerValue::IdentityCapacityExceeded(IdentityCapacityExceeded {
        scope,
        limit,
        occupied,
        ..
    }) = refused
    else {
        return Err(format!(
            "an identity cap lowered beneath durable occupancy must refuse with its true \
             numbers, got: {refused:?}"
        )
        .into());
    };
    assert_eq!(scope, IdentityCapacityScope::Server);
    assert_eq!(limit, 1);
    assert_eq!(occupied, 2);
    Ok(())
}

/// Lane p0-39 REWRITE of `enrollment_mixed_full_and_over_limit_refuses_the_earlier_full_scope`.
///
/// # The model-boundary variant is now UNCONSTRUCTIBLE, and that is a finding
///
/// The original played an in-model exactly-full scope (identity Server) off
/// against a later OVER-LIMIT scope (`ProvenanceServer`, its cap lowered
/// beneath durable occupancy) and asserted the earlier one answered. Both
/// halves of that setup are gone: the receipt scopes no longer refuse, and the
/// only surviving later scope — identity Conversation — cannot be driven
/// over-limit at all. Lowering `identity_slots` beneath already-minted ordinals
/// makes the conversation REFUSE TO REPLAY (the protocol's initial-enrollment
/// slot allocator rejects an ordinal outside `0..I` during restore, long before
/// stage-8 capacity is consulted), so the state the old pin needed cannot be
/// reached through any sequence of operations. Measured, not assumed: the
/// attempt answers `ConversationUnloadable … "durable initial enrollment was
/// refused during protocol replay"`.
///
/// What IS still constructible, and is pinned here, is the in-model half of
/// the same law: with BOTH identity scopes exactly full, the refusal names the
/// EARLIER one — Server — and discloses no later occupancy.
#[test]
fn enrollment_first_full_identity_scope_answers_before_the_later_one() -> Result<(), Box<dyn Error>>
{
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(81, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    // Two identities, one conversation, and BOTH scopes sized to exactly two:
    // server and conversation are full together.
    let config = capacity_config(|c| {
        c.max_retired_identity_slots_server = 2;
        c.identity_slots = 2;
    });
    let handler = ProductionParticipantHandler::new(store, config)?;

    enroll(&handler, incarnation, 761, [64; 16])?;
    enroll(&handler, ConnectionIncarnation::new(81, 2), 761, [65; 16])?;
    let refused = dispatch(
        &handler,
        ConnectionIncarnation::new(81, 3),
        enrollment_request(761, [66; 16]),
    )?;
    let ServerValue::IdentityCapacityExceeded(IdentityCapacityExceeded {
        request,
        scope,
        limit,
        occupied,
    }) = refused
    else {
        return Err(format!(
            "two full identity scopes must refuse with IdentityCapacityExceeded, got: {refused:?}"
        )
        .into());
    };
    assert_eq!(request.conversation_id, 761);
    assert_eq!(
        scope,
        IdentityCapacityScope::Server,
        "the EARLIER scope in the frozen order must answer"
    );
    assert_eq!(limit, 2);
    assert_eq!(occupied, 2);
    Ok(())
}

/// Lane p0-39 REWRITE of `enrollment_provenance_conversation_scope_refusal_is_scoped`.
///
/// The conversation provenance pool no longer refuses, so the original's
/// refusal assertion is gone; its second half — that the same enrollment
/// succeeds against a fresh conversation — is kept and generalised. Both
/// participants now land, which is the whole point: the second participant of a
/// conversation is a third party to the first participant's churn.
#[test]
fn shared_conversation_provenance_pool_never_refuses_either_participant()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(74, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = capacity_config(|c| c.receipt_provenance_per_conversation_report_threshold = 1);
    let handler = ProductionParticipantHandler::new(store, config)?;

    enroll_proving_provenance(&handler, incarnation, 731, [[31; 16], [131; 16], [231; 16]])?;
    // Same conversation, past the old cap.
    enroll(&handler, ConnectionIncarnation::new(74, 2), 731, [32; 16])?;
    // And a fresh conversation, exactly as before.
    enroll(&handler, ConnectionIncarnation::new(74, 3), 732, [33; 16])?;
    Ok(())
}
