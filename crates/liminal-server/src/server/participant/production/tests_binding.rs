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
        let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
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
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
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
        let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
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
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
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
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;

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

/// Register row 5655, conversation scope (contract: the monotone
/// `next_participant_index` in `0..=I`; when it equals `I` the
/// conversation-scope `IdentityCapacityExceeded` wins): with
/// `identity_slots = 4`, four enrollments mint ordinals 0..=3 and the fifth
/// returns the typed refusal with exact `scope`/`limit`/`occupied` — the
/// connection stays open and nothing is minted for the refused token.
#[test]
fn enrollment_at_identity_limit_returns_typed_capacity_refusal_without_minting()
-> Result<(), Box<dyn Error>> {
    use liminal_protocol::wire::{IdentityCapacityExceeded, IdentityCapacityScope};

    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let conversation_id = 801;
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = test_participant_config();
    assert_eq!(config.identity_slots, 4, "fixture assumes four slots");
    let handler = ProductionParticipantHandler::new(store, config)?;

    // Four distinct enrollments from four connection incarnations mint the
    // exact permanent ordinals 0..=3.
    for slot in 0..4_u64 {
        let enrolled = dispatch(
            &handler,
            ConnectionIncarnation::new(81, slot + 1),
            ClientRequest::Enrollment(EnrollmentRequest {
                conversation_id,
                enrollment_token: EnrollmentToken::new([90 + u8::try_from(slot)?; 16]),
            }),
        )?;
        let ServerValue::EnrollBound(receipt) = enrolled else {
            return Err(format!("enrollment {slot} did not bind: {enrolled:?}").into());
        };
        assert_eq!(receipt.participant_id(), slot);
    }

    // The fifth enrollment is the exact conversation-scope typed refusal.
    let refused_token = [99; 16];
    let refused = dispatch(
        &handler,
        ConnectionIncarnation::new(81, 5),
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new(refused_token),
        }),
    )?;
    let ServerValue::IdentityCapacityExceeded(IdentityCapacityExceeded {
        request,
        scope,
        limit,
        occupied,
    }) = refused
    else {
        return Err(
            format!("fifth enrollment must be IdentityCapacityExceeded, got: {refused:?}").into(),
        );
    };
    assert_eq!(request.conversation_id, conversation_id);
    assert_eq!(
        request.enrollment_token,
        EnrollmentToken::new(refused_token)
    );
    assert_eq!(scope, IdentityCapacityScope::Conversation);
    assert_eq!(limit, 4);
    assert_eq!(occupied, 4);

    // Nothing was minted: the refused token is NOT a lifetime mapping — its
    // replay refuses again instead of answering a replay row.
    let replayed = dispatch(
        &handler,
        ConnectionIncarnation::new(81, 6),
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new(refused_token),
        }),
    )?;
    assert!(
        matches!(replayed, ServerValue::IdentityCapacityExceeded(_)),
        "the refused token must not have minted a mapping: {replayed:?}"
    );

    // A different conversation is an independent identity domain: the same
    // deployment still enrolls ordinal 0 there.
    let sibling = dispatch(
        &handler,
        ConnectionIncarnation::new(81, 7),
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: conversation_id + 1,
            enrollment_token: EnrollmentToken::new([98; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt) = sibling else {
        return Err(format!("sibling conversation enrollment did not bind: {sibling:?}").into());
    };
    assert_eq!(receipt.participant_id(), 0);
    Ok(())
}

/// Register row 5641: the first decoded semantic operation for an untracked
/// conversation on a connection whose semantic-conversation map is full
/// answers `ConnectionConversationCapacityExceeded` with the triggering
/// operation's exact common request envelope and the signed limit — while
/// already-tracked conversations keep operating and a different connection's
/// fresh map is unaffected.
#[test]
fn semantic_conversations_beyond_connection_limit_refuse_with_exact_envelope()
-> Result<(), Box<dyn Error>> {
    use liminal_protocol::wire::{
        ConnectionConversationCapacityExceeded, EnrollmentEnvelope, ResponseEnvelope,
    };

    use crate::server::participant::ParticipantConnectionConversations;

    use super::tests::dispatch_tracked;

    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(85, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let mut config = test_participant_config();
    config.max_semantic_conversations_per_connection = 2;
    let handler = ProductionParticipantHandler::new(store, config)?;
    let mut conversations = ParticipantConnectionConversations::default();

    // Two enrollments fill the connection's two conversation slots.
    let mut secrets = Vec::new();
    for (index, conversation_id) in [901_u64, 902].into_iter().enumerate() {
        let enrolled = dispatch_tracked(
            &handler,
            incarnation,
            &mut conversations,
            ClientRequest::Enrollment(EnrollmentRequest {
                conversation_id,
                enrollment_token: EnrollmentToken::new([100 + u8::try_from(index)?; 16]),
            }),
        )?;
        let ServerValue::EnrollBound(receipt) = enrolled else {
            return Err(format!("enrollment {conversation_id} did not bind: {enrolled:?}").into());
        };
        secrets.push(receipt);
    }

    // The third conversation's first semantic operation is the exact typed
    // refusal: the enrollment common request envelope plus the signed limit.
    let refused_token = [111; 16];
    let refused = dispatch_tracked(
        &handler,
        incarnation,
        &mut conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: 903,
            enrollment_token: EnrollmentToken::new(refused_token),
        }),
    )?;
    let ServerValue::ConnectionConversationCapacityExceeded(
        ConnectionConversationCapacityExceeded::SemanticRequest {
            request:
                ResponseEnvelope::Enrollment(EnrollmentEnvelope {
                    conversation_id,
                    enrollment_token,
                }),
            limit,
        },
    ) = refused
    else {
        return Err(
            format!("third conversation must refuse on connection capacity: {refused:?}").into(),
        );
    };
    assert_eq!(conversation_id, 903);
    assert_eq!(enrollment_token, EnrollmentToken::new(refused_token));
    assert_eq!(limit, 2);

    // An ALREADY TRACKED conversation keeps operating at full capacity.
    let acked = dispatch_tracked(
        &handler,
        incarnation,
        &mut conversations,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: 901,
            participant_id: secrets
                .first()
                .ok_or("first enrollment receipt is present")?
                .participant_id(),
            capability_generation: Generation::ONE,
            through_seq: 1,
        }),
    )?;
    assert!(
        matches!(acked, ServerValue::AckCommitted(_)),
        "a tracked conversation must keep operating at full capacity: {acked:?}"
    );

    // A DIFFERENT connection has its own fresh map: conversation 903 enrolls.
    let mut sibling_map = ParticipantConnectionConversations::default();
    let sibling = dispatch_tracked(
        &handler,
        ConnectionIncarnation::new(85, 2),
        &mut sibling_map,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: 903,
            enrollment_token: EnrollmentToken::new(refused_token),
        }),
    )?;
    assert!(
        matches!(sibling, ServerValue::EnrollBound(_)),
        "a fresh connection map must admit the refused conversation: {sibling:?}"
    );
    Ok(())
}

/// The record-admission arm classifies every frozen lookup row (stages 2-5)
/// through the crate's frontier-free binding classifier: unknown participant,
/// stale generation, and no-binding each answer their exact typed rows over
/// the production dispatch seam — the connection stays open throughout.
#[test]
fn record_admission_lookup_rows_classify_typed_over_production_dispatch()
-> Result<(), Box<dyn Error>> {
    use liminal_protocol::wire::{DetachAttemptToken, DetachRequest, RecordAdmission};

    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(86, 1);
    let conversation_id = 904;
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;

    let enrolled = dispatch(
        &handler,
        incarnation,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([120; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt) = enrolled else {
        return Err(format!("enrollment did not bind: {enrolled:?}").into());
    };
    let participant_id = receipt.participant_id();
    let record = |participant: u64, generation: Generation| {
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id,
            participant_id: participant,
            capability_generation: generation,
            record_admission_attempt_token:
                liminal_protocol::wire::RecordAdmissionAttemptToken::new([0xA7; 16]),
            payload: vec![7, 7, 7],
        })
    };

    // Stage 4: unknown participant.
    let unknown = dispatch(&handler, incarnation, record(55, Generation::ONE))?;
    assert!(
        matches!(unknown, ServerValue::ParticipantUnknown(_)),
        "unknown participant must answer ParticipantUnknown: {unknown:?}"
    );

    // Stage 4: live identity, stale presented generation.
    let stale = dispatch(
        &handler,
        incarnation,
        record(participant_id, generation(2)?),
    )?;
    assert!(
        matches!(stale, ServerValue::StaleAuthority(_)),
        "stale generation must answer StaleAuthority: {stale:?}"
    );

    // Stage 5: authorized identity without a current binding.
    let detached = dispatch(
        &handler,
        incarnation,
        ClientRequest::Detach(DetachRequest {
            conversation_id,
            participant_id,
            capability_generation: Generation::ONE,
            detach_attempt_token: DetachAttemptToken::new([121; 16]),
        }),
    )?;
    assert!(matches!(detached, ServerValue::DetachCommitted(_)));
    let unbound = dispatch(
        &handler,
        incarnation,
        record(participant_id, Generation::ONE),
    )?;
    assert!(
        matches!(unbound, ServerValue::NoBinding(_)),
        "detached participant must answer NoBinding: {unbound:?}"
    );
    Ok(())
}
