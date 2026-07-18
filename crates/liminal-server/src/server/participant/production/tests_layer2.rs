use std::error::Error;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal::durability::{DurableStore, open_ephemeral};
use liminal::protocol::{Frame, decode as decode_generic};
use liminal_protocol::algebra::{ResourceDimension, ResourceVector, WideResourceVector};
use liminal_protocol::wire::{
    ClientRequest, ClosureCheckedEnvelope, ClosureRefusalReason, ClosureSnapshot,
    ConnectionConversationCapacityExceeded, ConnectionIncarnation, ConversationOrderExhausted,
    ConversationSequenceExhausted, EnrollmentRequest, EnrollmentToken, Generation,
    MarkerClosureCapacityExceeded, ObserverBackpressure, ObserverBackpressureState,
    OrderAllocatingEnvelope, ParticipantFrame, ProtocolVersion, ReceiverDirection, RecordAdmission,
    RecordAdmissionAttemptToken, RecordAdmissionEnvelope, RecordTooLarge, RepaymentEdge,
    ResponseEnvelope, SequenceAllocatingEnvelope, SequenceBudget, ServerValue, decode, encode,
    encoded_len,
};

use crate::server::participant::{
    ParticipantConnectionContext, ParticipantConnectionConversations, ParticipantDispatch,
    ParticipantSemanticError, ParticipantSemanticHandler, ParticipantSession,
    dispatch_generic_frame, normalize_configured_frame_limit,
};

use super::ProductionParticipantHandler;
use super::log::{
    OperationLog, OperationLogError, SCHEMA_VERSION, STREAM_PREFIX, StoredMarkerDrain,
    StoredOperation, StoredResourceVector, StoredRetainedCharge,
};
use super::tests::{dispatch, open_disk_store_for_tests, test_participant_config};

const CONVERSATION: u64 = 0xF0C6;
const PAYLOAD: [u8; 5] = [0xF0, 0x0C, 1, 2, 3];
const TOKEN: RecordAdmissionAttemptToken = RecordAdmissionAttemptToken::new([0xC1; 16]);

const LEGACY_V1_FIVE_KIND_ROWS: [&[u8]; 5] = [
    br#"{"schema_version":1,"operation":{"operation":"genesis","event":[]}}"#,
    br#"{"schema_version":1,"operation":{"operation":"enrolled","request":{"conversation_id":1,"token":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1]},"allocation":{"participant_id":0,"identity_limit":4,"attach_secret":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1],"origin_epoch":{"server_incarnation":1,"connection_ordinal":1,"capability_generation":1},"attached_order":0,"attached_seq":1,"receipt_expires_at":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1],"provenance_expires_at":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2],"enrollment_fingerprint":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1]},"event":[]}}"#,
    br#"{"schema_version":1,"operation":{"operation":"attached","request":{"conversation_id":1,"participant_id":0,"capability_generation":1,"attach_secret":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1],"token":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2],"accept_marker_delivery_seq":null},"secret_verified":true,"allocation":{"binding_epoch":{"server_incarnation":1,"connection_ordinal":2,"capability_generation":2},"attach_secret":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2],"attached_order":1,"attached_seq":2,"receipt_expires_at":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3],"provenance_expires_at":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,4]},"event":[]}}"#,
    br#"{"schema_version":1,"operation":{"operation":"detached","request":{"conversation_id":1,"participant_id":0,"capability_generation":2,"token":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3]},"verifier":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3],"receiving_epoch":{"server_incarnation":1,"connection_ordinal":2,"capability_generation":2},"terminal_order":2,"terminal_seq":3,"event":[]}}"#,
    br#"{"schema_version":1,"operation":{"operation":"zero_debt_ack","request":{"conversation_id":1,"participant_id":0,"capability_generation":2,"through_seq":3},"receiving_epoch":{"server_incarnation":1,"connection_ordinal":2,"capability_generation":2},"contiguously_available_through":3}}"#,
];

fn append_literal(
    store: &Arc<dyn DurableStore>,
    sequence: u64,
    bytes: &[u8],
) -> Result<(), Box<dyn Error>> {
    let key = format!("{STREAM_PREFIX}{CONVERSATION}");
    assert_eq!(
        block_on(store.append(&key, bytes.to_vec(), sequence))??,
        sequence
    );
    block_on(store.flush())??;
    Ok(())
}

#[test]
fn encode_dispatch_decode_assigns_sequence_and_one_exact_payload_row() -> Result<(), Box<dyn Error>>
{
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(90, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let observed = Arc::clone(&store);
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let enrolled = dispatch(
        &handler,
        incarnation,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([0xC0; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt) = enrolled else {
        return Err(format!("Layer 2 enrollment failed: {enrolled:?}").into());
    };
    let key = format!("{STREAM_PREFIX}{CONVERSATION}");
    let before = block_on(observed.read_from(&key, 0, 16))??;

    let committed = dispatch(
        &handler,
        incarnation,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: CONVERSATION,
            participant_id: receipt.participant_id(),
            capability_generation: Generation::ONE,
            record_admission_attempt_token: TOKEN,
            payload: PAYLOAD.to_vec(),
        }),
    )?;
    let ServerValue::RecordCommitted(committed) = committed else {
        return Err(format!("Layer 2 record did not commit: {committed:?}").into());
    };
    assert_eq!(committed.delivery_seq(), 2);
    assert_eq!(committed.request().record_admission_attempt_token, TOKEN);

    let after = block_on(observed.read_from(&key, 0, 16))??;
    assert_eq!(after.len(), before.len() + 1);
    let sole = after.last().ok_or("record row must exist")?;
    let json: serde_json::Value = serde_json::from_slice(&sole.payload)?;
    assert_eq!(json["schema_version"], SCHEMA_VERSION);
    assert_eq!(json["operation"]["operation"], "record_admission");
    assert_eq!(
        json["operation"]["row"]["request"]["payload"],
        serde_json::json!(PAYLOAD)
    );
    assert_eq!(json["operation"]["row"]["delivery_seq"], 2);
    Ok(())
}

#[test]
fn drain_first_row_is_one_complete_poststate_and_contains_no_push() -> Result<(), Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let log = OperationLog::new(Arc::clone(&store), CONVERSATION);
    let row = StoredMarkerDrain {
        marker: vec![0xAA, 0xBB],
        retained_charge: StoredRetainedCharge {
            delivery_seq: 8,
            transaction_order: 7,
            candidate_phase: 1,
            participant_id: 0,
            charge: StoredResourceVector {
                entries: 1,
                bytes: 2,
            },
        },
        resulting_retained_charges: vec![StoredRetainedCharge {
            delivery_seq: 8,
            transaction_order: 7,
            candidate_phase: 1,
            participant_id: 0,
            charge: StoredResourceVector {
                entries: 1,
                bytes: 2,
            },
        }],
        successor: b"complete-marker-successor".to_vec(),
    };
    block_on(log.append(&StoredOperation::MarkerDrained { row: row.clone() }, 0))??;
    let decoded = block_on(log.read_page(0))??;
    assert_eq!(decoded.len(), 1);
    let StoredOperation::MarkerDrained { row: restored } = &decoded[0].1 else {
        return Err("durable row was not MarkerDrained".into());
    };
    assert_eq!(restored, &row);
    let raw = block_on(store.read_from(&format!("{STREAM_PREFIX}{CONVERSATION}"), 0, 8))??;
    assert_eq!(raw.len(), 1);
    assert!(!String::from_utf8_lossy(&raw[0].payload).contains("participant_push"));
    Ok(())
}

#[test]
fn literal_same_stream_v1_five_kind_rows_are_schema_version_one() -> Result<(), Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    for (sequence, row) in LEGACY_V1_FIVE_KIND_ROWS.into_iter().enumerate() {
        append_literal(&store, u64::try_from(sequence)?, row)?;
    }
    let log = OperationLog::new(store, CONVERSATION);
    for sequence in 0..5_u64 {
        let result = block_on(log.read_page(sequence))?;
        assert!(matches!(result, Err(OperationLogError::SchemaVersion(1))));
    }
    Ok(())
}

#[derive(Debug)]
struct MatrixResponseHandler {
    response: ServerValue,
}

impl ParticipantSemanticHandler for MatrixResponseHandler {
    fn handle(
        &self,
        context: ParticipantConnectionContext,
        conversations: &mut ParticipantConnectionConversations,
        request: ClientRequest,
    ) -> Result<ServerValue, ParticipantSemanticError> {
        if context.connection_incarnation() != ConnectionIncarnation::new(91, 1)
            || request.discriminant() != matrix_request().discriminant()
        {
            return Err(ParticipantSemanticError::Internal {
                message: "matrix handler received the wrong dispatch facts".to_owned(),
            });
        }
        conversations.track(CONVERSATION);
        Ok(self.response.clone())
    }
}

fn matrix_envelope() -> RecordAdmissionEnvelope {
    RecordAdmissionEnvelope {
        conversation_id: CONVERSATION,
        participant_id: 0,
        capability_generation: Generation::ONE,
        record_admission_attempt_token: TOKEN,
    }
}

fn matrix_request() -> ClientRequest {
    ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: CONVERSATION,
        participant_id: 0,
        capability_generation: Generation::ONE,
        record_admission_attempt_token: TOKEN,
        payload: PAYLOAD.to_vec(),
    })
}

fn matrix_closure_snapshot() -> ClosureSnapshot {
    ClosureSnapshot {
        marker_capacity_credits: 0,
        marker_anchors: 0,
        entry_debt: 0,
        byte_debt: 0,
        repayment_edge: RepaymentEdge::None,
        edge_sequence_claims: 0,
        edge_order_position_claims: 0,
        edge_k_remaining: ResourceVector::new(0, 0),
        k_headroom: WideResourceVector::new(0, 0),
        episode_churn_used: 0,
        delta_cycles: 0,
        episode_churn_limit: 1,
    }
}

fn matrix_sequence_budget() -> SequenceBudget {
    SequenceBudget {
        high_watermark: u64::MAX,
        remaining: 0,
        e: 1,
        t: 1,
        m: 0,
        rs: 0,
        rt: 0,
        l_times_t: 1,
        l_times_rt: 0,
        l_other_times_e: 0,
    }
}

fn matrix_refusals() -> Vec<ServerValue> {
    let envelope = matrix_envelope();
    vec![
        ServerValue::ConnectionConversationCapacityExceeded(
            ConnectionConversationCapacityExceeded::SemanticRequest {
                request: ResponseEnvelope::RecordAdmission(envelope.clone()),
                limit: 1,
            },
        ),
        ServerValue::RecordTooLarge(RecordTooLarge {
            request: envelope.clone(),
            dimension: ResourceDimension::Bytes,
            encoded_record_charge: ResourceVector::new(1, 101),
            max_ordinary_record_charge: ResourceVector::new(1, 100),
        }),
        ServerValue::ObserverBackpressure(ObserverBackpressure::RecordAdmission {
            request: envelope.clone(),
            state: ObserverBackpressureState::initial(7),
        }),
        ServerValue::MarkerClosureCapacityExceeded(Box::new(MarkerClosureCapacityExceeded {
            request: ClosureCheckedEnvelope::RecordAdmission(envelope.clone()),
            snapshot: matrix_closure_snapshot(),
            reason: ClosureRefusalReason::RecoveryFence,
        })),
        ServerValue::ConversationOrderExhausted(Box::new(ConversationOrderExhausted::new(
            OrderAllocatingEnvelope::RecordAdmission(envelope.clone()),
            u64::MAX,
            0,
            0,
            0,
            0,
        ))),
        ServerValue::ConversationSequenceExhausted(Box::new(ConversationSequenceExhausted {
            request: SequenceAllocatingEnvelope::RecordAdmission(envelope),
            sequence_budget: matrix_sequence_budget(),
        })),
    ]
}

fn matrix_generic_request() -> Result<Frame, Box<dyn Error>> {
    let participant = ParticipantFrame::ClientRequest(matrix_request());
    let mut bytes = vec![0; encoded_len(&participant).map_err(|error| format!("{error:?}"))?];
    let written = encode(&participant, &mut bytes).map_err(|error| format!("{error:?}"))?;
    bytes.truncate(written);
    let (generic, consumed) = decode_generic(&bytes)?;
    if consumed != bytes.len() {
        return Err("generic decoder left an unread matrix suffix".into());
    }
    Ok(generic)
}

fn dispatch_matrix_response(value: ServerValue) -> Result<ServerValue, Box<dyn Error>> {
    let mut session = ParticipantSession::default();
    let limit = normalize_configured_frame_limit(test_participant_config().wire_frame_limit)
        .map_err(|error| format!("{error:?}"))?;
    session.negotiate_v1(limit);
    let handler = MatrixResponseHandler { response: value };
    let outcome = dispatch_generic_frame(
        &matrix_generic_request()?,
        true,
        session,
        ParticipantConnectionContext::new(ConnectionIncarnation::new(91, 1)),
        &mut ParticipantConnectionConversations::default(),
        &handler,
    );
    let ParticipantDispatch::Respond(response) = outcome else {
        return Err(format!("typed matrix answer was not Respond: {outcome:?}").into());
    };
    let mut bytes = vec![0; liminal::protocol::encoded_len(&response)?];
    let written = liminal::protocol::encode(&response, &mut bytes)?;
    bytes.truncate(written);
    let decoded =
        decode(&bytes, ReceiverDirection::Client).map_err(|error| format!("{error:?}"))?;
    let ParticipantFrame::ServerValue(decoded) = decoded else {
        return Err("matrix response did not decode as ServerValue".into());
    };
    Ok(decoded)
}

#[test]
fn production_dispatch_refusal_matrix_is_typed_tokened_and_respond_only()
-> Result<(), Box<dyn Error>> {
    let values = matrix_refusals();
    assert_eq!(values.len(), 6);
    for expected in values {
        let decoded = dispatch_matrix_response(expected.clone())?;
        assert_eq!(decoded, expected);
        assert_eq!(
            decoded.originating_request(),
            Some(matrix_request().discriminant())
        );
    }
    assert_eq!(ProtocolVersion::V1, ProtocolVersion::new(1, 0));
    Ok(())
}
