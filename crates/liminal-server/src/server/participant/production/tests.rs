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
pub(super) fn dispatch(
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

/// Refused requests against never-seen conversation ids leave the durable
/// store byte-identical and the registry empty: no genesis is minted before
/// classification, and probe cells are evicted (R-C1 refusal law — "every
/// refusal commits no receipt, order, cursor, binding, lifecycle record,
/// candidate, or retention mutation").
#[test]
fn refused_probes_of_fresh_conversations_leave_no_durable_or_registry_residue()
-> Result<(), Box<dyn Error>> {
    use liminal::durability::bridge::block_on;
    use liminal_protocol::wire::{LeaveAttemptToken, LeaveRequest, MarkerAck};

    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(51, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config());

    // One refused probe per pure-refusal arm, each on its own fresh id.
    let acked = dispatch(
        &handler,
        incarnation,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: 9001,
            participant_id: 0,
            capability_generation: Generation::ONE,
            through_seq: 1,
        }),
    )?;
    assert!(
        matches!(acked, ServerValue::ParticipantUnknown(_)),
        "fresh-conversation ack must refuse ParticipantUnknown: {acked:?}"
    );
    let detached = dispatch(
        &handler,
        incarnation,
        ClientRequest::Detach(DetachRequest {
            conversation_id: 9002,
            participant_id: 0,
            capability_generation: Generation::ONE,
            detach_attempt_token: DetachAttemptToken::new([9; 16]),
        }),
    )?;
    assert!(
        matches!(detached, ServerValue::ParticipantUnknown(_)),
        "fresh-conversation detach must refuse ParticipantUnknown: {detached:?}"
    );
    let left = dispatch(
        &handler,
        incarnation,
        ClientRequest::Leave(LeaveRequest {
            conversation_id: 9003,
            participant_id: 0,
            capability_generation: Generation::ONE,
            attach_secret: liminal_protocol::wire::AttachSecret::new([3; 32]),
            leave_attempt_token: LeaveAttemptToken::new([4; 16]),
        }),
    )?;
    assert!(
        matches!(left, ServerValue::ParticipantUnknown(_)),
        "fresh-conversation leave must refuse ParticipantUnknown: {left:?}"
    );
    let marker = dispatch(
        &handler,
        incarnation,
        ClientRequest::MarkerAck(MarkerAck {
            conversation_id: 9004,
            participant_id: 0,
            capability_generation: Generation::ONE,
            marker_delivery_seq: 1,
        }),
    )?;
    assert!(
        matches!(marker, ServerValue::ParticipantUnknown(_)),
        "fresh-conversation marker ack must refuse ParticipantUnknown: {marker:?}"
    );
    let attach = dispatch(
        &handler,
        incarnation,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: 9005,
            participant_id: 0,
            capability_generation: Generation::ONE,
            attach_secret: liminal_protocol::wire::AttachSecret::new([5; 32]),
            attach_attempt_token: AttachAttemptToken::new([6; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    assert!(
        matches!(attach, ServerValue::ParticipantUnknown(_)),
        "fresh-conversation attach must refuse ParticipantUnknown: {attach:?}"
    );

    // Durable store: every probed conversation stream is byte-absent, and no
    // observer row exists.
    for conversation_id in [9001_u64, 9002, 9003, 9004, 9005] {
        let stream_key = format!("liminal:participant-production:{conversation_id}");
        let entries = block_on(store.read_from(&stream_key, 0, 8))??;
        assert!(
            entries.is_empty(),
            "refused probe minted durable entries for conversation {conversation_id}"
        );
    }
    let observer_rows = block_on(store.read_from("liminal:participant-observer-recovery", 0, 8))??;
    assert!(
        observer_rows.is_empty(),
        "refused probes minted observer rows: {observer_rows:?}"
    );
    // Registry: every probe cell was evicted.
    assert_eq!(
        handler.registry_len(),
        0,
        "refused probes left in-memory registry cells behind"
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
    // The frozen selector precedence yields exactly ONE row for these facts
    // (cursor 0, requested 9, no expected marker): NoMarkerExpected.
    let ServerValue::MarkerMismatch(mismatch) = refused else {
        return Err(format!(
            "undelivered marker ack must refuse through the marker-proof selector: {refused:?}"
        )
        .into());
    };
    assert!(
        matches!(
            mismatch.mismatch,
            liminal_protocol::wire::MarkerMismatchBody::NoMarkerExpected
        ),
        "no marker was ever expected: {mismatch:?}"
    );
    let liminal_protocol::wire::MarkerProofRequest::MarkerAck(proof) = &mismatch.request else {
        return Err(format!("refusal must echo the marker-ack envelope: {mismatch:?}").into());
    };
    assert_eq!(proof.conversation_id, CONVERSATION);
    assert_eq!(proof.participant_id, receipt.participant_id());
    assert_eq!(proof.capability_generation, Generation::ONE);
    assert_eq!(proof.requested_marker_delivery_seq, 9);
    Ok(())
}

/// Observer recovery classifies each contract-derived row exactly, over
/// durably restored rows only: a tracked conversation at progress 0 with
/// refused epoch 1 is `EpochAhead` (pinning the Track row's survival across
/// the cold reopen), an armable refusal at the exact progress is accepted
/// and armed, and an untracked conversation id is `ConversationUnknown`.
#[test]
fn observer_recovery_classifies_exact_rows_over_durable_tracking() -> Result<(), Box<dyn Error>> {
    use liminal_protocol::wire::{
        InvalidObserverEpoch, ObserverRecoveryHandshake, ObserverRefusal,
    };

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

    // Cold reopen: the recovery batches run over durably restored rows only.
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, test_participant_config());
    // Tracked conversation, durable progress 0, refused epoch 1: the ONE
    // contract row for these facts is EpochAhead with exact fields.
    let ahead = dispatch(
        &handler,
        ConnectionIncarnation::new(42, 1),
        ClientRequest::ObserverRecovery(ObserverRecoveryHandshake {
            observer_refusals: vec![ObserverRefusal {
                conversation_id: CONVERSATION,
                refused_epoch: 1,
            }],
        }),
    )?;
    let ServerValue::InvalidObserverEpoch(InvalidObserverEpoch::EpochAhead {
        conversation_id,
        presented_epoch,
        current_observer_progress,
    }) = ahead
    else {
        return Err(
            format!("refused epoch 1 over progress 0 must be EpochAhead: {ahead:?}").into(),
        );
    };
    assert_eq!(conversation_id, CONVERSATION);
    assert_eq!(presented_epoch, 1);
    assert_eq!(current_observer_progress, 0);

    // The exact durable progress arms: accepted with one armed status row.
    let accepted = dispatch(
        &handler,
        ConnectionIncarnation::new(42, 2),
        ClientRequest::ObserverRecovery(ObserverRecoveryHandshake {
            observer_refusals: vec![ObserverRefusal {
                conversation_id: CONVERSATION,
                refused_epoch: 0,
            }],
        }),
    )?;
    let ServerValue::ObserverRecoveryAccepted(outcome) = accepted else {
        return Err(
            format!("refused epoch at the exact durable progress must arm: {accepted:?}").into(),
        );
    };
    assert_eq!(outcome.statuses.len(), 1);
    let status = outcome
        .statuses
        .first()
        .ok_or("accepted outcome carries one status row")?;
    assert_eq!(status.conversation_id, CONVERSATION);
    assert_eq!(status.refused_epoch, 0);
    assert_eq!(status.current_observer_progress, 0);
    assert!(status.armed, "the equal-epoch refusal must arm");
    assert!(!status.progressed);

    // An untracked conversation id classifies as ConversationUnknown.
    let unknown_id = 9099_u64;
    let unknown = dispatch(
        &handler,
        ConnectionIncarnation::new(42, 3),
        ClientRequest::ObserverRecovery(ObserverRecoveryHandshake {
            observer_refusals: vec![ObserverRefusal {
                conversation_id: unknown_id,
                refused_epoch: 1,
            }],
        }),
    )?;
    let ServerValue::InvalidObserverEpoch(InvalidObserverEpoch::ConversationUnknown {
        conversation_id,
        presented_epoch,
    }) = unknown
    else {
        return Err(
            format!("an untracked conversation must be ConversationUnknown: {unknown:?}").into(),
        );
    };
    assert_eq!(conversation_id, unknown_id);
    assert_eq!(presented_epoch, 1);
    Ok(())
}
