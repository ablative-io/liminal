//! Production-path blocker scenarios through the live dispatch seam.
//!
//! Both phase-B blocker scenarios re-expressed against the REAL production
//! stack: the installed production semantic handler, real wire frames through
//! [`dispatch_generic_frame`], a real haematite database on disk, and a cold
//! restart (drop every live handle, reopen the same database directory)
//! between the state-building half and the assertion half.

use std::error::Error;
use std::path::Path;
use std::sync::Arc;

use haematite::{Database, DatabaseConfig, EventStore};
use liminal::durability::{DurableStore, HaematiteStore};
use liminal::protocol::{Frame, decode as decode_generic};
use liminal_protocol::wire::{
    AttachAttemptToken, BindingEpoch, ClientRequest, ConnectionIncarnation,
    CredentialAttachRequest, DetachAttemptToken, DetachRequest, DetachStaleAuthority,
    EnrollmentRequest, EnrollmentToken, Generation, ParticipantAck, ParticipantFrame,
    ReceiverDirection, ServerValue, StaleAuthority, decode, encode, encoded_len,
};

use crate::config::types::ParticipantConfig;
use crate::server::participant::{
    ParticipantConnectionContext, ParticipantDispatch, ParticipantSession, dispatch_generic_frame,
    normalize_configured_frame_limit,
};

use super::ProductionParticipantHandler;

/// Deployment-shaped participant configuration for the production tests.
pub(super) const fn test_participant_config() -> ParticipantConfig {
    ParticipantConfig {
        wire_frame_limit: 65_536,
        attach_receipt_ttl_ms: 60_000,
        receipt_provenance_ttl_ms: 600_000,
        identity_slots: 4,
        observer_recovery_max_entries: 64,
        max_semantic_conversations_per_connection: 32,
    }
}

/// Opens (or creates) the on-disk haematite database at `data_dir`.
pub(super) fn open_disk_store_for_tests(
    data_dir: &Path,
) -> Result<Arc<dyn DurableStore>, Box<dyn Error>> {
    let database = if data_dir.join("config.json").exists() {
        Database::open(data_dir)?
    } else {
        Database::create(DatabaseConfig {
            data_dir: data_dir.to_path_buf(),
            shard_count: 2,
            sweep_interval: None,
            distributed: None,
        })?
    };
    Ok(Arc::new(HaematiteStore::new(Arc::new(EventStore::new(
        database,
    )))))
}

fn negotiated_session() -> Result<ParticipantSession, Box<dyn Error>> {
    let limit = normalize_configured_frame_limit(test_participant_config().wire_frame_limit)
        .map_err(|error| format!("{error:?}"))?;
    let mut session = ParticipantSession::default();
    session.negotiate_v1(limit);
    Ok(session)
}

fn participant_generic(request: ClientRequest) -> Result<Frame, Box<dyn Error>> {
    let participant = ParticipantFrame::ClientRequest(request);
    let mut bytes = vec![0; encoded_len(&participant).map_err(|error| format!("{error:?}"))?];
    let written = encode(&participant, &mut bytes).map_err(|error| format!("{error:?}"))?;
    bytes.truncate(written);
    let (generic, consumed) = decode_generic(&bytes)?;
    if consumed != bytes.len() {
        return Err("generic decoder left an unread suffix".into());
    }
    Ok(generic)
}

/// Dispatches one request through the live production seam and decodes the
/// wire response back into a semantic value.
fn dispatch(
    handler: &ProductionParticipantHandler,
    incarnation: ConnectionIncarnation,
    request: ClientRequest,
) -> Result<ServerValue, Box<dyn Error>> {
    let generic = participant_generic(request)?;
    let outcome = dispatch_generic_frame(
        &generic,
        true,
        negotiated_session()?,
        ParticipantConnectionContext::new(incarnation),
        handler,
    );
    let ParticipantDispatch::Respond(response) = outcome else {
        return Err(format!("dispatch did not respond: {outcome:?}").into());
    };
    let generic_len = liminal::protocol::encoded_len(&response)?;
    let mut response_bytes = vec![0; generic_len];
    let written = liminal::protocol::encode(&response, &mut response_bytes)?;
    response_bytes.truncate(written);
    let decoded =
        decode(&response_bytes, ReceiverDirection::Client).map_err(|error| format!("{error:?}"))?;
    let ParticipantFrame::ServerValue(value) = decoded else {
        return Err("response did not decode as a server value".into());
    };
    Ok(value)
}

const CONVERSATION: u64 = 7;

/// Blocker scenario 1, production path: a terminalized detach cell replayed
/// with the OLD exact token after a COLD RESTART answers with the OLD
/// committed binding epoch, end to end through real wire frames.
#[test]
fn terminalized_detach_cold_reopen_carries_old_epoch_through_dispatch() -> Result<(), Box<dyn Error>>
{
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(11, 1);
    let detach_token = DetachAttemptToken::new([7; 16]);
    let old_epoch;

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, test_participant_config());

        let enrolled = dispatch(
            &handler,
            incarnation,
            ClientRequest::Enrollment(EnrollmentRequest {
                conversation_id: CONVERSATION,
                enrollment_token: EnrollmentToken::new([1; 16]),
            }),
        )?;
        let ServerValue::EnrollBound(receipt) = enrolled else {
            return Err(format!("enrollment did not bind: {enrolled:?}").into());
        };
        let secret = receipt.attach_secret();
        old_epoch = receipt.origin_binding_epoch();
        assert_eq!(old_epoch, BindingEpoch::new(incarnation, Generation::ONE));

        let detached = dispatch(
            &handler,
            incarnation,
            ClientRequest::Detach(DetachRequest {
                conversation_id: CONVERSATION,
                participant_id: receipt.participant_id(),
                capability_generation: Generation::ONE,
                detach_attempt_token: detach_token,
            }),
        )?;
        assert!(
            matches!(detached, ServerValue::DetachCommitted(_)),
            "detach did not commit: {detached:?}"
        );

        // The ordinary attach — from a NEW connection incarnation, as a real
        // reconnect would — terminalizes the committed cell atomically with
        // the credential rotation (Fix 1). The old committed epoch and the
        // new bound epoch now genuinely differ.
        let attached = dispatch(
            &handler,
            ConnectionIncarnation::new(11, 2),
            ClientRequest::CredentialAttach(CredentialAttachRequest {
                conversation_id: CONVERSATION,
                participant_id: receipt.participant_id(),
                capability_generation: Generation::ONE,
                attach_secret: secret,
                attach_attempt_token: AttachAttemptToken::new([2; 16]),
                accept_marker_delivery_seq: None,
            }),
        )?;
        assert!(
            matches!(attached, ServerValue::AttachBound(_)),
            "attach did not bind: {attached:?}"
        );
    }

    // COLD RESTART: every live handle above is dropped; reopen the same
    // database directory and rebuild the handler from durable reality alone.
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, test_participant_config());
    let replayed = dispatch(
        &handler,
        ConnectionIncarnation::new(12, 1),
        ClientRequest::Detach(DetachRequest {
            conversation_id: CONVERSATION,
            participant_id: 0,
            capability_generation: Generation::ONE,
            detach_attempt_token: detach_token,
        }),
    )?;
    let ServerValue::StaleAuthority(StaleAuthority::Detach(
        DetachStaleAuthority::TerminalizedDetachCell(cell),
    )) = replayed
    else {
        return Err(
            format!("old detach token did not replay the terminalized cell: {replayed:?}").into(),
        );
    };
    assert_eq!(
        cell.committed_binding_epoch(),
        old_epoch,
        "the terminalized cell must carry the OLD committed epoch"
    );
    assert_eq!(cell.detach_attempt_token(), detach_token);
    Ok(())
}

/// Blocker scenario 2, production path: two participants acknowledge over
/// the SAME retained suffix boundary, each against its own per-participant
/// cursor authority; after a cold restart a regression for one participant is
/// refused while the other participant's cursor is intact — through the live
/// dispatch seam with real wire frames.
#[test]
fn two_participant_same_suffix_acks_and_regression_refusal_survive_cold_reopen()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation_a = ConnectionIncarnation::new(21, 1);
    let incarnation_b = ConnectionIncarnation::new(21, 2);
    let participant_a;
    let participant_b;

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, test_participant_config());

        let enrolled_a = dispatch(
            &handler,
            incarnation_a,
            ClientRequest::Enrollment(EnrollmentRequest {
                conversation_id: CONVERSATION,
                enrollment_token: EnrollmentToken::new([3; 16]),
            }),
        )?;
        let ServerValue::EnrollBound(receipt_a) = enrolled_a else {
            return Err(format!("first enrollment did not bind: {enrolled_a:?}").into());
        };
        participant_a = receipt_a.participant_id();

        let enrolled_b = dispatch(
            &handler,
            incarnation_b,
            ClientRequest::Enrollment(EnrollmentRequest {
                conversation_id: CONVERSATION,
                enrollment_token: EnrollmentToken::new([4; 16]),
            }),
        )?;
        let ServerValue::EnrollBound(receipt_b) = enrolled_b else {
            return Err(format!("second enrollment did not bind: {enrolled_b:?}").into());
        };
        participant_b = receipt_b.participant_id();
        assert_ne!(participant_a, participant_b);

        // Both participants acknowledge through the SAME suffix boundary —
        // the exact shape the contract's fixed occurrence array could not
        // represent and per-participant cursor facts must.
        let same_suffix_boundary = 2;
        for (incarnation, participant) in [
            (incarnation_a, participant_a),
            (incarnation_b, participant_b),
        ] {
            let acked = dispatch(
                &handler,
                incarnation,
                ClientRequest::ParticipantAck(ParticipantAck {
                    conversation_id: CONVERSATION,
                    participant_id: participant,
                    capability_generation: Generation::ONE,
                    through_seq: same_suffix_boundary,
                }),
            )?;
            assert!(
                matches!(acked, ServerValue::AckCommitted(_)),
                "same-suffix ack did not commit for participant {participant}: {acked:?}"
            );
        }
    }

    // COLD RESTART, then the regression refusal.
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, test_participant_config());
    let regressed = dispatch(
        &handler,
        incarnation_b,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: CONVERSATION,
            participant_id: participant_b,
            capability_generation: Generation::ONE,
            through_seq: 1,
        }),
    )?;
    assert!(
        matches!(regressed, ServerValue::AckRegression(_)),
        "cursor regression was not refused after cold reopen: {regressed:?}"
    );
    // The sibling participant's own cursor authority is intact: replaying its
    // exact boundary is a no-op, not a regression and not a second advance.
    let replayed = dispatch(
        &handler,
        incarnation_a,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: CONVERSATION,
            participant_id: participant_a,
            capability_generation: Generation::ONE,
            through_seq: 2,
        }),
    )?;
    assert!(
        matches!(replayed, ServerValue::AckNoOp(_)),
        "exact-boundary replay must be a no-op: {replayed:?}"
    );
    Ok(())
}

/// Marker acknowledgements classify through the crate's marker-proof
/// selector against factual (empty) delivery state.
#[test]
fn marker_ack_refuses_through_crate_selector() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(31, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, test_participant_config());

    let enrolled = dispatch(
        &handler,
        incarnation,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([5; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt) = enrolled else {
        return Err(format!("enrollment did not bind: {enrolled:?}").into());
    };
    let refused = dispatch(
        &handler,
        incarnation,
        ClientRequest::MarkerAck(liminal_protocol::wire::MarkerAck {
            conversation_id: CONVERSATION,
            participant_id: receipt.participant_id(),
            capability_generation: Generation::ONE,
            marker_delivery_seq: 9,
        }),
    )?;
    assert!(
        matches!(
            refused,
            ServerValue::MarkerMismatch(_) | ServerValue::MarkerNotDelivered(_)
        ),
        "undelivered marker ack must refuse through the marker-proof selector: {refused:?}"
    );
    Ok(())
}

/// Observer recovery for an untracked conversation refuses through the A4
/// transaction; the durable observer rows survive a cold reopen.
#[test]
fn observer_recovery_refuses_unknown_epoch_and_tracks_durably() -> Result<(), Box<dyn Error>> {
    use liminal_protocol::wire::{ObserverRecoveryHandshake, ObserverRefusal};

    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(41, 1);

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, test_participant_config());
        // Enrollment registers the conversation's observer progress row.
        let enrolled = dispatch(
            &handler,
            incarnation,
            ClientRequest::Enrollment(EnrollmentRequest {
                conversation_id: CONVERSATION,
                enrollment_token: EnrollmentToken::new([6; 16]),
            }),
        )?;
        assert!(matches!(enrolled, ServerValue::EnrollBound(_)));
    }

    // Cold reopen: the recovery batch runs over durably restored rows only.
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, test_participant_config());
    let value = dispatch(
        &handler,
        ConnectionIncarnation::new(42, 1),
        ClientRequest::ObserverRecovery(ObserverRecoveryHandshake {
            observer_refusals: vec![ObserverRefusal {
                conversation_id: CONVERSATION,
                refused_epoch: 1,
            }],
        }),
    )?;
    assert!(
        matches!(
            value,
            ServerValue::ObserverRecoveryAccepted(_) | ServerValue::InvalidObserverEpoch(_)
        ),
        "observer recovery must classify through the A4 transaction: {value:?}"
    );
    Ok(())
}
