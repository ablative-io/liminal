//! Superseding-attach and marker-bearing-attach production-path tests.
//!
//! Contract R-C1.3: an attach authorized by the same participant capability
//! while a binding epoch is active — even on this same connection
//! incarnation — records exactly one ordered `Detached(Superseded)`/
//! `Attached` handoff and fences every old-epoch operation. These tests
//! drive that arm through the live dispatch seam over a real on-disk store,
//! including the crash-reconnect shape (bound on a dead connection, fresh
//! attach from a new incarnation) that previously fail-closed the connection
//! and permanently locked the participant out.

use std::error::Error;

use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, ConnectionIncarnation, CredentialAttachRequest,
    EnrollmentRequest, EnrollmentToken, Generation, MarkerMismatchBody, MarkerProofRequest,
    ParticipantAck, ServerValue,
};

use super::ProductionParticipantHandler;
use super::tests::{dispatch, open_disk_store_for_tests, test_participant_config};

fn generation(value: u64) -> Result<Generation, Box<dyn Error>> {
    Generation::new(value).ok_or_else(|| "zero generation in test fixture".into())
}

/// Attach while bound on the SAME connection incarnation: the rotation
/// supersedes the active epoch atomically, fences the old generation, and
/// the handoff survives a cold restart through replay.
#[test]
fn attach_while_bound_same_connection_supersedes_and_survives_cold_reopen()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(71, 1);
    let conversation_id = 701;
    let participant_id;

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, test_participant_config());
        let enrolled = dispatch(
            &handler,
            incarnation,
            ClientRequest::Enrollment(EnrollmentRequest {
                conversation_id,
                enrollment_token: EnrollmentToken::new([81; 16]),
            }),
        )?;
        let ServerValue::EnrollBound(receipt) = enrolled else {
            return Err(format!("enrollment did not bind: {enrolled:?}").into());
        };
        participant_id = receipt.participant_id();

        // NO detach: the binding is still active when the attach arrives.
        let attached = dispatch(
            &handler,
            incarnation,
            ClientRequest::CredentialAttach(CredentialAttachRequest {
                conversation_id,
                participant_id,
                capability_generation: Generation::ONE,
                attach_secret: receipt.attach_secret(),
                attach_attempt_token: AttachAttemptToken::new([82; 16]),
                accept_marker_delivery_seq: None,
            }),
        )?;
        let ServerValue::AttachBound(bound) = attached else {
            return Err(format!("attach while bound did not supersede: {attached:?}").into());
        };
        assert_eq!(bound.capability_generation(), generation(2)?);
        assert_eq!(
            bound.origin_binding_epoch().connection_incarnation,
            incarnation,
            "the superseding epoch names this same connection incarnation"
        );
        assert_ne!(
            bound.attach_secret(),
            receipt.attach_secret(),
            "supersession must rotate the secret"
        );

        // The OLD epoch is fenced: a generation-1 operation is stale.
        let stale = dispatch(
            &handler,
            incarnation,
            ClientRequest::ParticipantAck(ParticipantAck {
                conversation_id,
                participant_id,
                capability_generation: Generation::ONE,
                through_seq: 1,
            }),
        )?;
        assert!(
            matches!(stale, ServerValue::StaleAuthority(_)),
            "old-epoch operation after supersession must be stale: {stale:?}"
        );
    }

    // COLD RESTART: the superseding handoff replays from the durable log.
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, test_participant_config());
    // Enrollment record seq 1, superseded terminal seq 2, attached record
    // seq 3: the new epoch acknowledges the full contiguous suffix.
    let acked = dispatch(
        &handler,
        incarnation,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id,
            participant_id,
            capability_generation: generation(2)?,
            through_seq: 3,
        }),
    )?;
    assert!(
        matches!(acked, ServerValue::AckCommitted(_)),
        "post-restart ack under the superseding epoch must commit: {acked:?}"
    );
    Ok(())
}

/// The crash-reconnect shape: the binding survived a connection crash (no
/// detach was ever sent); the client re-presents its current-generation
/// secret with a fresh token from a NEW incarnation. The server supersedes
/// instead of tearing the connection down, and the participant keeps
/// rotating across cold restarts.
#[test]
fn attach_while_bound_from_new_incarnation_recovers_crashed_binding() -> Result<(), Box<dyn Error>>
{
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let conversation_id = 702;
    let participant_id;
    let second_secret;

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, test_participant_config());
        let enrolled = dispatch(
            &handler,
            ConnectionIncarnation::new(72, 1),
            ClientRequest::Enrollment(EnrollmentRequest {
                conversation_id,
                enrollment_token: EnrollmentToken::new([83; 16]),
            }),
        )?;
        let ServerValue::EnrollBound(receipt) = enrolled else {
            return Err(format!("enrollment did not bind: {enrolled:?}").into());
        };
        participant_id = receipt.participant_id();

        // The enrolling connection "crashes" (nothing is sent for it again);
        // the client reconnects with a new incarnation and its persisted
        // current-generation secret, per R-C1.2.
        let reconnect = ConnectionIncarnation::new(72, 2);
        let attached = dispatch(
            &handler,
            reconnect,
            ClientRequest::CredentialAttach(CredentialAttachRequest {
                conversation_id,
                participant_id,
                capability_generation: Generation::ONE,
                attach_secret: receipt.attach_secret(),
                attach_attempt_token: AttachAttemptToken::new([84; 16]),
                accept_marker_delivery_seq: None,
            }),
        )?;
        let ServerValue::AttachBound(bound) = attached else {
            return Err(format!(
                "reconnect attach over a crashed binding did not bind: {attached:?}"
            )
            .into());
        };
        assert_eq!(bound.capability_generation(), generation(2)?);
        assert_eq!(
            bound.origin_binding_epoch().connection_incarnation,
            reconnect
        );
        second_secret = bound.attach_secret();
    }

    // COLD RESTART, then another crash-shaped rotation: the replayed
    // superseding entry restores a bound slot that supersedes again.
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, test_participant_config());
    let third = dispatch(
        &handler,
        ConnectionIncarnation::new(73, 1),
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id,
            participant_id,
            capability_generation: generation(2)?,
            attach_secret: second_secret,
            attach_attempt_token: AttachAttemptToken::new([85; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    let ServerValue::AttachBound(bound) = third else {
        return Err(format!("post-restart superseding attach did not bind: {third:?}").into());
    };
    assert_eq!(bound.capability_generation(), generation(3)?);
    Ok(())
}

/// A valid-credential attach carrying `accept_marker_delivery_seq: Some(_)`
/// classifies through the crate's total marker-proof selector against the
/// factual (empty) delivery state — a typed wire refusal, never a
/// connection-fatal invariant.
#[test]
fn marker_bearing_attach_refuses_no_marker_expected() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(74, 1);
    let conversation_id = 703;
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, test_participant_config());

    let enrolled = dispatch(
        &handler,
        incarnation,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([86; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt) = enrolled else {
        return Err(format!("enrollment did not bind: {enrolled:?}").into());
    };
    let token = AttachAttemptToken::new([87; 16]);
    let refused = dispatch(
        &handler,
        incarnation,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id,
            participant_id: receipt.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: receipt.attach_secret(),
            attach_attempt_token: token,
            accept_marker_delivery_seq: Some(9),
        }),
    )?;
    let ServerValue::MarkerMismatch(mismatch) = refused else {
        return Err(format!(
            "marker-bearing attach must refuse through the marker-proof selector: {refused:?}"
        )
        .into());
    };
    assert!(
        matches!(mismatch.mismatch, MarkerMismatchBody::NoMarkerExpected),
        "no marker was ever expected: {mismatch:?}"
    );
    let MarkerProofRequest::CredentialAttach(proof) = &mismatch.request else {
        return Err(format!("refusal must echo the attach envelope: {mismatch:?}").into());
    };
    assert_eq!(proof.conversation_id, conversation_id);
    assert_eq!(proof.participant_id, receipt.participant_id());
    assert_eq!(proof.capability_generation, Generation::ONE);
    assert_eq!(proof.token, token);
    assert_eq!(proof.requested_marker_delivery_seq, 9);
    Ok(())
}
