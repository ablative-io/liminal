//! R-D1 stage-8 enrollment capacity production-path tests.
//!
//! Each test drives the live dispatch seam with real wire frames over a real
//! on-disk store and pins one register-row refusal of the enrollment
//! identity/receipt capacity family (rows 5654/5655): the exact scope in the
//! frozen seven-scope order, the signed limit, and the true occupancy — plus
//! the cold-restart exactness of the server-scope ledger (a restart must not
//! forget reserved identity slots or in-window receipts). The credential
//! attach scopes live in [`super::tests_capacity_attach`].

use std::error::Error;

use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, EnrollmentReceiptCapacityScope, EnrollmentRequest,
    EnrollmentToken, IdentityCapacityExceeded, IdentityCapacityScope, ReceiptCapacityExceeded,
    ServerValue,
};

use crate::config::types::ParticipantConfig;

use super::ProductionParticipantHandler;
use super::tests::{dispatch, open_disk_store_for_tests, test_participant_config};
use super::tests_receipts::enroll;

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

/// Asserts one exact enrollment `ReceiptCapacityExceeded` row.
fn assert_enrollment_receipt_refusal(
    value: &ServerValue,
    conversation_id: u64,
    scope: EnrollmentReceiptCapacityScope,
    limit: u64,
    occupied: u64,
) -> Result<(), Box<dyn Error>> {
    let ServerValue::ReceiptCapacityExceeded(ReceiptCapacityExceeded::Enrollment {
        request,
        scope: got_scope,
        limit: got_limit,
        occupied: got_occupied,
    }) = value
    else {
        return Err(format!(
            "expected the enrollment ReceiptCapacityExceeded row ({scope:?}), got: {value:?}"
        )
        .into());
    };
    assert_eq!(request.conversation_id, conversation_id);
    assert_eq!(*got_scope, scope);
    assert_eq!(*got_limit, limit);
    assert_eq!(*got_occupied, occupied);
    Ok(())
}

/// Server-scope identity capacity (register row 5655): the third identity
/// across the whole server refuses with scope `Server` (tested BEFORE the
/// conversation scope, whose per-conversation occupancy is far below its
/// limit) — and the refusal SURVIVES a cold restart, proving the startup
/// restore rebuilds the identity ledger from durable truth.
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

/// Server-scope live-receipt capacity (register row 5654): with one live
/// enrollment receipt occupying the whole server cap, a second enrollment on
/// a DIFFERENT conversation refuses with `LiveReceiptServer` — before and
/// after a cold restart (the receipt is still inside its 60s window).
#[test]
fn enrollment_live_receipt_server_scope_refuses_and_survives_restart() -> Result<(), Box<dyn Error>>
{
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(72, 1);
    let config = capacity_config(|c| c.max_live_attach_receipts_server = 1);

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, config)?;
        enroll(&handler, incarnation, 711, [11; 16])?;
        let refused = dispatch(&handler, incarnation, enrollment_request(712, [12; 16]))?;
        assert_enrollment_receipt_refusal(
            &refused,
            712,
            EnrollmentReceiptCapacityScope::LiveReceiptServer,
            1,
            1,
        )?;
    }

    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, config)?;
    let refused = dispatch(&handler, incarnation, enrollment_request(712, [12; 16]))?;
    assert_enrollment_receipt_refusal(
        &refused,
        712,
        EnrollmentReceiptCapacityScope::LiveReceiptServer,
        1,
        1,
    )
}

/// Server-scope provenance capacity: one retained enrollment fingerprint
/// fills the server cap, so a second enrollment on another conversation
/// refuses with `ProvenanceServer` (live-receipt scopes pass first).
#[test]
fn enrollment_provenance_server_scope_refusal() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(73, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = capacity_config(|c| c.max_receipt_provenance_server = 1);
    let handler = ProductionParticipantHandler::new(store, config)?;

    enroll(&handler, incarnation, 721, [21; 16])?;
    let refused = dispatch(&handler, incarnation, enrollment_request(722, [22; 16]))?;
    assert_enrollment_receipt_refusal(
        &refused,
        722,
        EnrollmentReceiptCapacityScope::ProvenanceServer,
        1,
        1,
    )
}

/// Out-of-model over-limit refusal with true numbers: two retained
/// enrollment fingerprints, then a restart whose server provenance cap was
/// lowered to 1 BENEATH that durable occupancy. Every earlier scope in the
/// frozen order has headroom, so the next enrollment refuses at
/// `ProvenanceServer` with the lowered limit and the true occupancy — never
/// admitting past the signed cap and never inventing in-model numbers.
#[test]
fn enrollment_over_limit_scope_refuses_with_true_numbers() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(80, 1);

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
        enroll(&handler, incarnation, 751, [61; 16])?;
        enroll(&handler, incarnation, 752, [62; 16])?;
    }

    // RESTART with the server provenance cap lowered beneath the two
    // retained in-window fingerprints.
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = capacity_config(|c| c.max_receipt_provenance_server = 1);
    let handler = ProductionParticipantHandler::new(store, config)?;
    let refused = dispatch(&handler, incarnation, enrollment_request(753, [63; 16]))?;
    assert_enrollment_receipt_refusal(
        &refused,
        753,
        EnrollmentReceiptCapacityScope::ProvenanceServer,
        1,
        2,
    )
}

/// Frozen first-full precedence across the model boundary: the identity
/// Server scope is exactly full IN model (2 identities against a cap of 2)
/// while a LATER scope is over-limit (server provenance cap lowered to 1
/// beneath 2 retained fingerprints). The contract's seven-scope suborder
/// says the first full scope answers and no later occupancy is disclosed,
/// so the refusal must be `IdentityCapacityExceeded` scope `Server` with the
/// identity numbers — never the later provenance scope's.
#[test]
fn enrollment_mixed_full_and_over_limit_refuses_the_earlier_full_scope()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(81, 1);

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
        enroll(&handler, incarnation, 761, [64; 16])?;
        enroll(&handler, incarnation, 762, [65; 16])?;
    }

    // RESTART: identity Server exactly full in model, ProvenanceServer
    // over-limit out of model.
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = capacity_config(|c| {
        c.max_retired_identity_slots_server = 2;
        c.max_receipt_provenance_server = 1;
    });
    let handler = ProductionParticipantHandler::new(store, config)?;
    let refused = dispatch(&handler, incarnation, enrollment_request(763, [66; 16]))?;
    let ServerValue::IdentityCapacityExceeded(IdentityCapacityExceeded {
        request,
        scope,
        limit,
        occupied,
    }) = refused
    else {
        return Err(format!(
            "the earlier full identity Server scope must answer before the later over-limit \
             provenance scope, got: {refused:?}"
        )
        .into());
    };
    assert_eq!(request.conversation_id, 763);
    assert_eq!(scope, IdentityCapacityScope::Server);
    assert_eq!(limit, 2);
    assert_eq!(occupied, 2);
    Ok(())
}

/// Conversation-scope provenance capacity: a second identity in the SAME
/// conversation refuses with `ProvenanceConversation`, while the same
/// enrollment on a fresh conversation still admits — the scope is really
/// per conversation.
#[test]
fn enrollment_provenance_conversation_scope_refusal_is_scoped() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(74, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = capacity_config(|c| c.max_receipt_provenance_per_conversation = 1);
    let handler = ProductionParticipantHandler::new(store, config)?;

    enroll(&handler, incarnation, 731, [31; 16])?;
    let refused = dispatch(
        &handler,
        ConnectionIncarnation::new(74, 2),
        enrollment_request(731, [32; 16]),
    )?;
    assert_enrollment_receipt_refusal(
        &refused,
        731,
        EnrollmentReceiptCapacityScope::ProvenanceConversation,
        1,
        1,
    )?;
    // The identical enrollment against a FRESH conversation admits.
    enroll(&handler, ConnectionIncarnation::new(74, 2), 732, [32; 16])?;
    Ok(())
}
