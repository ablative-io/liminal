//! Production-path blocker scenarios through the live dispatch seam.
//!
//! Both phase-B blocker scenarios re-expressed against the REAL production
//! stack: the installed production semantic handler, real wire frames through
//! [`dispatch_generic_frame`], a real haematite database on disk, and a cold
//! restart (drop every live handle, reopen the same database directory)
//! between the state-building half and the assertion half.

use crate::config::types::ParticipantConfig;
use crate::server::participant::{
    ParticipantConnectionContext, ParticipantConnectionConversations, ParticipantDispatch,
    ParticipantSemanticError, ParticipantSemanticHandler, ParticipantServiceFatal,
    ParticipantSession, dispatch_generic_frame, normalize_configured_frame_limit,
};

use haematite::{Database, DatabaseConfig, EventStore};

use liminal::durability::{DurableStore, HaematiteStore, open_ephemeral};
use liminal::protocol::{Frame, decode as decode_generic};

use liminal_protocol::wire::{
    AttachAttemptToken, BindingEpoch, ClientRequest, ConnectionIncarnation,
    CredentialAttachRequest, DetachAttemptToken, DetachRequest, DetachStaleAuthority,
    EnrollmentRequest, EnrollmentToken, Generation, ObserverRecoveryHandshake, ParticipantAck,
    ParticipantFrame, ReceiverDirection, ServerValue, StaleAuthority, decode, encode, encoded_len,
};

use std::error::Error;
use std::path::Path;
use std::sync::Arc;

use super::ProductionParticipantHandler;

/// Deployment-shaped participant configuration for the production tests.
pub const fn test_participant_config() -> ParticipantConfig {
    ParticipantConfig {
        wire_frame_limit: 65_536,
        attach_receipt_ttl_ms: 60_000,
        receipt_provenance_ttl_ms: 600_000,
        max_live_attach_receipts_server: 1_024,
        max_live_attach_receipts_per_participant: 8,
        max_receipt_provenance_server: 4_096,
        max_receipt_provenance_per_conversation: 256,
        max_receipt_provenance_per_participant: 64,
        max_retired_identity_slots_server: 1_024,
        identity_slots: 4,
        observer_recovery_max_entries: 64,
        max_semantic_conversations_per_connection: 32,
        max_ordinary_record_entries: 1,
        max_ordinary_record_bytes: 131_072,
        max_generated_marker_entries: 1,
        max_generated_marker_bytes: 4_096,
        mandatory_transaction_bound_entries: 4,
        mandatory_transaction_bound_bytes: 16_384,
        full_recovery_claim_entries: 4,
        full_recovery_claim_bytes: 16_384,
        retained_capacity_entries: 2_048,
        retained_capacity_bytes: 16_777_216,
        max_retained_record_rows: 1_024,
        closure_episode_churn_limit: 1_024,
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
            distributed: None,
            executor_threads: None,
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
/// wire response back into a semantic value, over a throwaway per-request
/// connection map (each call behaves as a fresh connection).
pub(super) fn dispatch(
    handler: &ProductionParticipantHandler,
    incarnation: ConnectionIncarnation,
    request: ClientRequest,
) -> Result<ServerValue, Box<dyn Error>> {
    dispatch_tracked(
        handler,
        incarnation,
        &mut ParticipantConnectionConversations::default(),
        request,
    )
}

/// Dispatches one request through the live production seam over a CALLER-HELD
/// connection map, so a test can drive one connection's semantic-conversation
/// occupancy across requests.
pub(super) fn dispatch_tracked(
    handler: &ProductionParticipantHandler,
    incarnation: ConnectionIncarnation,
    conversations: &mut ParticipantConnectionConversations,
    request: ClientRequest,
) -> Result<ServerValue, Box<dyn Error>> {
    let generic = participant_generic(request)?;
    let outcome = dispatch_generic_frame(
        &generic,
        true,
        negotiated_session()?,
        ParticipantConnectionContext::new(incarnation),
        conversations,
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
        let handler = ProductionParticipantHandler::new(store, test_participant_config())?;

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
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
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
    let incarnation_c = ConnectionIncarnation::new(21, 3);
    let participant_a;
    let participant_b;

    {
        let store = open_disk_store_for_tests(&data_dir)?;
        let handler = ProductionParticipantHandler::new(store, test_participant_config())?;

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

        // A third participant's committed attach is a real shared recipient
        // obligation for A and B. Their own enrollment endpoints are excluded
        // from their recipient snapshots and therefore cannot be ack targets.
        let enrolled_c = dispatch(
            &handler,
            incarnation_c,
            ClientRequest::Enrollment(EnrollmentRequest {
                conversation_id: CONVERSATION,
                enrollment_token: EnrollmentToken::new([0x43; 16]),
            }),
        )?;
        assert!(
            matches!(enrolled_c, ServerValue::EnrollBound(_)),
            "third enrollment did not create the shared obligation: {enrolled_c:?}"
        );

        // Both participants acknowledge through the SAME suffix boundary —
        // the exact shape the contract's fixed occurrence array could not
        // represent and per-participant cursor facts must.
        let same_suffix_boundary = 3;
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
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let regressed = dispatch(
        &handler,
        incarnation_b,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: CONVERSATION,
            participant_id: participant_b,
            capability_generation: Generation::ONE,
            through_seq: 2,
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
            through_seq: 3,
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
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;

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
        let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
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
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
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

#[test]
fn connection_fate_intent_incomplete_latches_first_fatal_before_semantic_or_publication_entry()
-> Result<(), Box<dyn Error>> {
    const OPEN_SEQUENCE: u64 = 17;
    const CONVERSATION: u64 = 23;
    const LATER_OPEN_SEQUENCE: u64 = 29;
    const LATER_CONVERSATION: u64 = 31;

    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    assert_eq!(handler.service_fatal()?, None);

    let selected = handler.latch_connection_fate_intent_incomplete(OPEN_SEQUENCE, CONVERSATION)?;
    assert_eq!(
        selected,
        ParticipantServiceFatal::ConnectionFateIntentIncomplete {
            open_sequence: OPEN_SEQUENCE,
            conversation_id: CONVERSATION,
        }
    );
    let repeated =
        handler.latch_connection_fate_intent_incomplete(LATER_OPEN_SEQUENCE, LATER_CONVERSATION)?;
    assert_eq!(repeated, selected, "the first post-Open fatal must win");
    assert_eq!(handler.service_fatal()?, Some(selected.clone()));

    let ready = handler.ready_connection_incarnations(CONVERSATION);
    assert!(matches!(
        ready,
        Err(ParticipantSemanticError::ServiceFatal(fatal)) if fatal == selected
    ));
    let publication =
        handler.next_publication(ConnectionIncarnation::new(1, 0), CONVERSATION, None);
    assert!(matches!(
        publication,
        Err(ParticipantSemanticError::ServiceFatal(fatal)) if fatal == selected
    ));

    let mut conversations = ParticipantConnectionConversations::default();
    let request = ClientRequest::ObserverRecovery(ObserverRecoveryHandshake {
        observer_refusals: Vec::new(),
    });
    let semantic_result = handler.handle(
        ParticipantConnectionContext::new(ConnectionIncarnation::new(1, 0)),
        &mut conversations,
        request,
    );
    assert!(matches!(
        semantic_result,
        Err(ParticipantSemanticError::ServiceFatal(fatal)) if fatal == selected
    ));
    assert_eq!(conversations.occupied(), 0);
    Ok(())
}
