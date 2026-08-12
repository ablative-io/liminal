//! Lane p0-39: the stage-8 receipt-capacity hybrid — TTL-only shared pools,
//! displacement windows per participant.
//!
//! Tom's governing sentence is *"no configured number refuses an honest
//! arrival."* These pins assert that law directly, on both of its halves:
//!
//! * **Shared pools** (`LiveReceiptServer`, `ProvenanceServer`,
//!   `ProvenanceConversation`) stop being admission gates entirely. A third
//!   party's honest enrollment can never meet a number someone else's churn
//!   consumed. Retention there is bounded by the TTL windows alone.
//! * **Per-participant pools** (`LiveReceiptParticipant`,
//!   `ProvenanceParticipant`) keep their configured numbers as WINDOW SIZES.
//!   At a full window the OLDEST in-window entry is displaced and the new
//!   entry always lands, so the (N+1)th honest fingerprint never refuses.
//!
//! Each pin below was RED at `77e4845` (the pre-lane base), where every one of
//! these arrivals met a `ReceiptCapacityExceeded` refusal instead.

use std::error::Error;

use liminal_protocol::wire::{
    AttachSecret, ClientRequest, ConnectionIncarnation, EnrollmentRequest, EnrollmentToken,
    ServerValue,
};

use super::ProductionParticipantHandler;
use super::tests::{dispatch, open_disk_store_for_tests, test_participant_config};
use super::tests_capacity::capacity_config;
use super::tests_receipts::{
    GEN_ONE, attach, attach_request, detach, enroll, enroll_proving_provenance, generation,
};

/// One committed rotation at `from_generation`, returning the secret the
/// rotation minted. Each rotation retires its predecessor's receipt into the
/// participant's own bounded provenance record — the growth these windows
/// bound, and the residue that used to wedge the (N+1)th attach.
fn rotate(
    handler: &ProductionParticipantHandler,
    incarnation: ConnectionIncarnation,
    conversation_id: u64,
    participant_id: u64,
    from_generation: u64,
    secret: AttachSecret,
    tokens: ([u8; 16], [u8; 16]),
) -> Result<AttachSecret, Box<dyn Error>> {
    let (detach_token, attach_token) = tokens;
    detach(
        handler,
        incarnation,
        conversation_id,
        participant_id,
        generation(from_generation)?,
        detach_token,
    )?;
    let bound = attach(
        handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            generation(from_generation)?,
            secret,
            attach_token,
        ),
    )?;
    Ok(bound.attach_secret())
}

fn enrollment_request(conversation_id: u64, token: [u8; 16]) -> ClientRequest {
    ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id,
        enrollment_token: EnrollmentToken::new(token),
    })
}

/// Asserts one honest arrival LANDED rather than meeting a configured number.
fn assert_landed(value: &ServerValue, what: &str) -> Result<(), Box<dyn Error>> {
    match value {
        ServerValue::EnrollBound(_) | ServerValue::AttachBound(_) => Ok(()),
        ServerValue::ReceiptCapacityExceeded(refusal) => Err(format!(
            "no configured number may refuse an honest arrival: {what} met \
             {refusal:?}"
        )
        .into()),
        other => Err(format!("{what} must bind, got: {other:?}").into()),
    }
}

/// THE FIELD SPECIMEN, per-participant provenance window (red at `77e4845`,
/// where it answered `ReceiptCapacityExceeded ProvenanceParticipant 2/2`).
///
/// A participant's own committed rotations fill its own window; the next
/// rotation of the SAME participant must displace its own oldest fingerprint
/// and land. Per-participant pressure is self-inflicted, so the number bounds
/// memory without ever refusing.
#[test]
fn full_participant_provenance_window_displaces_and_the_next_rotation_lands()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(139, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = capacity_config(|c| c.max_receipt_provenance_per_participant = 2);
    let handler = ProductionParticipantHandler::new(store, config)?;
    let conversation_id = 3901;

    // Retained fingerprint #1: the enrollment receipt, proven by the first
    // rotation. The participant is now bound at generation 2.
    let proven = enroll_proving_provenance(
        &handler,
        incarnation,
        conversation_id,
        [[0x39; 16], [0x3A; 16], [0x3B; 16]],
    )?;
    // Retained fingerprint #2: the generation-2 attach receipt, retired by
    // this rotation. The window of 2 is now exactly full.
    let secret = rotate(
        &handler,
        incarnation,
        conversation_id,
        proven.participant_id,
        2,
        proven.attach_secret,
        ([0x3C; 16], [0x3D; 16]),
    )?;

    // The (N+1)th honest rotation of the participant's OWN identity.
    detach(
        &handler,
        incarnation,
        conversation_id,
        proven.participant_id,
        generation(3)?,
        [0x3E; 16],
    )?;
    let arrival = dispatch(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            proven.participant_id,
            generation(3)?,
            secret,
            [0x3F; 16],
        ),
    )?;
    assert_landed(
        &arrival,
        "the (N+1)th rotation at a full participant window",
    )
}

/// The same specimen FROM DURABLE STATE (red at `77e4845`): a cold restart
/// replays the committed rotations, rebuilds the full window, and the
/// reconnecting client's attach must still land. This is the boot-wedge shape
/// the field reported — committed-rotation residue refusing the attach of the
/// very participant that produced it.
#[test]
fn full_participant_provenance_window_lands_on_cold_replay() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let conversation_id = 3902;
    let config = capacity_config(|c| c.max_receipt_provenance_per_participant = 2);
    let participant_id;
    let secret;

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, config)?;
        let incarnation = ConnectionIncarnation::new(140, 1);
        let proven = enroll_proving_provenance(
            &handler,
            incarnation,
            conversation_id,
            [[0x40; 16], [0x41; 16], [0x42; 16]],
        )?;
        participant_id = proven.participant_id;
        secret = rotate(
            &handler,
            incarnation,
            conversation_id,
            participant_id,
            2,
            proven.attach_secret,
            ([0x43; 16], [0x44; 16]),
        )?;
    }

    // COLD RESTART: the window is rebuilt from durable bytes alone. The
    // restart already left the binding detached, so the client reconnects
    // straight into a rotation at its current generation.
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, config)?;
    let reconnect = ConnectionIncarnation::new(140, 2);
    let arrival = dispatch(
        &handler,
        reconnect,
        attach_request(
            conversation_id,
            participant_id,
            generation(3)?,
            secret,
            [0x45; 16],
        ),
    )?;
    assert_landed(
        &arrival,
        "the reconnecting attach at a replayed full participant window",
    )
}

/// Per-participant LIVE-RECEIPT window (red at `77e4845`, where it answered
/// `ReceiptCapacityExceeded LiveReceiptParticipant 1/1`).
///
/// The participant's own live enrollment receipt filled its own window, and
/// the attach that was refused is exactly the one that ENDS that receipt — a
/// pure wedge. The window must displace and the rotation must land.
#[test]
fn full_participant_live_receipt_window_never_wedges_its_own_rotation() -> Result<(), Box<dyn Error>>
{
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(141, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = capacity_config(|c| c.max_live_attach_receipts_per_participant = 1);
    let handler = ProductionParticipantHandler::new(store, config)?;
    let conversation_id = 3903;

    let receipt = enroll(&handler, incarnation, conversation_id, [0x46; 16])?;
    let participant_id = receipt.participant_id();
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        GEN_ONE,
        [0x47; 16],
    )?;
    let arrival = dispatch(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            GEN_ONE,
            receipt.attach_secret(),
            [0x48; 16],
        ),
    )?;
    assert_landed(
        &arrival,
        "the first rotation at a full participant live-receipt window",
    )
}

/// SHARED POOL, server provenance (red at `77e4845`, where it answered
/// `ReceiptCapacityExceeded ProvenanceServer 1/1`).
///
/// The fingerprint filling the server pool belongs to conversation 3904; the
/// refused enrollment is an honest THIRD PARTY on conversation 3905 that has
/// consumed nothing. No configured number may refuse it.
#[test]
fn shared_server_provenance_churn_never_refuses_a_third_party_enrollment()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(142, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = capacity_config(|c| c.receipt_provenance_server_report_threshold = 1);
    let handler = ProductionParticipantHandler::new(store, config)?;

    // Someone else's churn fills the shared pool.
    enroll_proving_provenance(
        &handler,
        incarnation,
        3904,
        [[0x49; 16], [0x4A; 16], [0x4B; 16]],
    )?;

    let arrival = dispatch(&handler, incarnation, enrollment_request(3905, [0x4C; 16]))?;
    assert_landed(
        &arrival,
        "an honest third-party enrollment against a full shared provenance pool",
    )
}

/// SHARED POOL, server live receipts (red at `77e4845`, where it answered
/// `ReceiptCapacityExceeded LiveReceiptServer 1/1`).
#[test]
fn shared_server_live_receipt_churn_never_refuses_a_third_party_enrollment()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(143, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = capacity_config(|c| c.live_receipt_server_report_threshold = 1);
    let handler = ProductionParticipantHandler::new(store, config)?;

    enroll(&handler, incarnation, 3906, [0x4D; 16])?;
    let arrival = dispatch(&handler, incarnation, enrollment_request(3907, [0x4E; 16]))?;
    assert_landed(
        &arrival,
        "an honest third-party enrollment against a full shared live-receipt pool",
    )
}

/// SHARED POOL, per-conversation provenance (red at `77e4845`, where it
/// answered `ReceiptCapacityExceeded ProvenanceConversation 1/1`).
///
/// The second participant of a conversation is a third party to the first
/// participant's churn: it consumed nothing and must not be refused.
#[test]
fn shared_conversation_provenance_churn_never_refuses_a_second_participant()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(144, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = capacity_config(|c| c.receipt_provenance_per_conversation_report_threshold = 1);
    let handler = ProductionParticipantHandler::new(store, config)?;
    let conversation_id = 3908;

    enroll_proving_provenance(
        &handler,
        incarnation,
        conversation_id,
        [[0x4F; 16], [0x50; 16], [0x51; 16]],
    )?;

    let arrival = dispatch(
        &handler,
        ConnectionIncarnation::new(144, 2),
        enrollment_request(conversation_id, [0x52; 16]),
    )?;
    assert_landed(
        &arrival,
        "a second participant against a full shared conversation provenance pool",
    )
}

/// SHARED POOL on the ATTACH arm (red at `77e4845`, where it answered
/// `ReceiptCapacityExceeded ProvenanceServer 1/2`).
///
/// A rotating participant meets a shared pool that ANOTHER conversation's
/// churn filled — the third-party shape on the arm a reconnecting client
/// actually takes after a restart.
#[test]
fn shared_server_provenance_churn_never_refuses_a_third_party_attach() -> Result<(), Box<dyn Error>>
{
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let conversation_id = 3909;
    let participant_id;
    let secret;

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
        let incarnation = ConnectionIncarnation::new(145, 1);
        let proven = enroll_proving_provenance(
            &handler,
            incarnation,
            conversation_id,
            [[0x53; 16], [0x54; 16], [0x55; 16]],
        )?;
        participant_id = proven.participant_id;
        secret = proven.attach_secret;
        // A DIFFERENT conversation's churn.
        enroll_proving_provenance(
            &handler,
            incarnation,
            3910,
            [[0x56; 16], [0x57; 16], [0x58; 16]],
        )?;
    }

    // Restart with the shared server pool lowered beneath the retained
    // fingerprints; the client reconnects and rotates.
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = capacity_config(|c| c.receipt_provenance_server_report_threshold = 1);
    let handler = ProductionParticipantHandler::new(store, config)?;
    let reconnect = ConnectionIncarnation::new(145, 2);
    let arrival = dispatch(
        &handler,
        reconnect,
        attach_request(
            conversation_id,
            participant_id,
            generation(2)?,
            secret,
            [0x59; 16],
        ),
    )?;
    assert_landed(
        &arrival,
        "a reconnecting attach against a shared provenance pool another conversation filled",
    )
}
