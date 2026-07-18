use alloc::{boxed::Box, vec, vec::Vec};

use super::*;
use crate::algebra::{ResourceDimension, ResourceVector, WideResourceVector};
use crate::wire::{
    BindingEpoch, BindingRequiredEnvelope, ClientDiscriminant, ClientRequest,
    ClosureCheckedEnvelope, ClosureRefusalReason, ClosureSnapshot, CommonStaleAuthorityEnvelope,
    ConnectionConversationCapacityExceeded, ConnectionIncarnation, ConversationOrderExhausted,
    ConversationSequenceExhausted, Generation, MarkerClosureCapacityExceeded, NoBinding,
    ObserverBackpressure, ObserverBackpressureState, ObserverRecoveryHandshake,
    OrderAllocatingEnvelope, ParticipantReferenceEnvelope, ParticipantUnknown, ProtocolVersion,
    ReceiverDirection, RecordAdmission, RecordAdmissionAttemptToken, RecordAdmissionEnvelope,
    RecordCommitted, RecordTooLarge, RepaymentEdge, ResponseEnvelope, Retired,
    SequenceAllocatingEnvelope, SequenceBudget, ServerDiscriminant, ServerValue, StaleAuthority,
    decode, decode_server_value_body, encode_server_value_body,
};

// D1 item 6 remains covered by the pre-existing, unchanged
// `resume_tests::resume_round_trips_every_expected_operation_and_continuous_ack`.
// The tests below are the named home for items 1-5 and 7.

type TestResult<T = ()> = Result<T, &'static str>;

const TOKEN_A: RecordAdmissionAttemptToken = RecordAdmissionAttemptToken::new([0xA7; 16]);
const TOKEN_B: RecordAdmissionAttemptToken = RecordAdmissionAttemptToken::new([0xB8; 16]);

// Canonical r1 request body at 8d2bfd3: conversation, participant, generation,
// u32 payload length, payload. It deliberately carries no D1 token.
const LEGACY_R1_RECORD_ADMISSION_REQUEST_BODY: [u8; 31] = [
    0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 3, 0xAA, 0xBB,
    0xCC,
];

// Canonical r1 RecordCommitted body at 8d2bfd3: the three-u64 admission
// envelope followed by the assigned delivery sequence, with no D1 token.
const LEGACY_R1_RECORD_COMMITTED_RESPONSE_BODY: [u8; 32] = [
    0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4,
];

fn generation(value: u64) -> TestResult<Generation> {
    Generation::new(value).ok_or("generation must be nonzero")
}

fn bound() -> TestResult<ClientParticipantAggregate> {
    let generation = generation(3)?;
    let mut aggregate = ClientParticipantAggregate::new();
    aggregate.binding = ClientBindingState::Bound {
        conversation_id: 1,
        participant_id: 2,
        generation,
        attach_secret: crate::wire::AttachSecret::new([0x33; 32]),
        binding_epoch: BindingEpoch::new(ConnectionIncarnation::new(4, 5), generation),
    };
    Ok(aggregate)
}

fn record_request(token: RecordAdmissionAttemptToken) -> TestResult<ClientRequest> {
    Ok(ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: 1,
        participant_id: 2,
        capability_generation: generation(3)?,
        record_admission_attempt_token: token,
        payload: vec![0xAA, 0xBB, 0xCC],
    }))
}

fn record_envelope(token: RecordAdmissionAttemptToken) -> TestResult<RecordAdmissionEnvelope> {
    Ok(RecordAdmissionEnvelope {
        conversation_id: 1,
        participant_id: 2,
        capability_generation: generation(3)?,
        record_admission_attempt_token: token,
    })
}

fn issued_record(
    token: RecordAdmissionAttemptToken,
) -> TestResult<(
    ClientParticipantAggregate,
    ClientResponseCorrelation,
    ClientRequest,
)> {
    let request = record_request(token)?;
    let ClientOperationRecordDecision::Pending(pending) =
        record_operation(bound()?, request.clone())
    else {
        return Err("record admission must enter the write-ahead barrier");
    };
    let (aggregate, operation) = pending.commit().into_parts();
    let (released, correlation) = operation.into_request();
    if released != request {
        return Err("barrier must release the exact request");
    }
    Ok((aggregate, correlation, request))
}

fn committed(token: RecordAdmissionAttemptToken, sequence: u64) -> TestResult<ServerValue> {
    Ok(ServerValue::RecordCommitted(RecordCommitted::new(
        record_envelope(token)?,
        sequence,
    )))
}

fn closure_snapshot() -> ClosureSnapshot {
    ClosureSnapshot {
        marker_capacity_credits: 0,
        marker_anchors: 0,
        entry_debt: 0,
        byte_debt: 0,
        repayment_edge: RepaymentEdge::None,
        edge_sequence_claims: 0,
        edge_order_position_claims: 0,
        edge_k_remaining: ResourceVector::new(0, 0),
        k_headroom: WideResourceVector::new(10, 20),
        episode_churn_used: 0,
        delta_cycles: 0,
        episode_churn_limit: 2,
    }
}

fn sequence_budget() -> SequenceBudget {
    SequenceBudget {
        high_watermark: 10,
        remaining: 9,
        e: 1,
        t: 1,
        m: 1,
        rs: 1,
        rt: 1,
        l_times_t: 1,
        l_times_rt: 1,
        l_other_times_e: 1,
    }
}

fn first_record_outcomes(envelope: &RecordAdmissionEnvelope) -> TestResult<Vec<ServerValue>> {
    let generation = generation(4)?;
    Ok(vec![
        ServerValue::ConnectionConversationCapacityExceeded(
            ConnectionConversationCapacityExceeded::SemanticRequest {
                request: ResponseEnvelope::RecordAdmission(envelope.clone()),
                limit: 2,
            },
        ),
        ServerValue::ConversationOrderExhausted(Box::new(ConversationOrderExhausted::new(
            OrderAllocatingEnvelope::RecordAdmission(envelope.clone()),
            10,
            20,
            30,
            19,
            31,
        ))),
        ServerValue::ParticipantUnknown(ParticipantUnknown {
            request: ParticipantReferenceEnvelope::RecordAdmission(envelope.clone()),
        }),
        ServerValue::NoBinding(NoBinding {
            request: BindingRequiredEnvelope::RecordAdmission(envelope.clone()),
        }),
        ServerValue::StaleAuthority(StaleAuthority::Live {
            request: CommonStaleAuthorityEnvelope::RecordAdmission(envelope.clone()),
            current_generation: generation,
        }),
        ServerValue::Retired(Retired::Participant {
            request: ParticipantReferenceEnvelope::RecordAdmission(envelope.clone()),
            retired_generation: generation,
        }),
    ])
}

fn remaining_record_outcomes(envelope: &RecordAdmissionEnvelope) -> Vec<ServerValue> {
    vec![
        ServerValue::MarkerClosureCapacityExceeded(Box::new(MarkerClosureCapacityExceeded {
            request: ClosureCheckedEnvelope::RecordAdmission(envelope.clone()),
            snapshot: closure_snapshot(),
            reason: ClosureRefusalReason::RecoveryFence,
        })),
        ServerValue::RecordCommitted(RecordCommitted::new(envelope.clone(), 11)),
        ServerValue::RecordTooLarge(RecordTooLarge {
            request: envelope.clone(),
            dimension: ResourceDimension::Bytes,
            encoded_record_charge: ResourceVector::new(1, 4),
            max_ordinary_record_charge: ResourceVector::new(1, 3),
        }),
        ServerValue::ConversationSequenceExhausted(Box::new(ConversationSequenceExhausted {
            request: SequenceAllocatingEnvelope::RecordAdmission(envelope.clone()),
            sequence_budget: sequence_budget(),
        })),
        ServerValue::ObserverBackpressure(ObserverBackpressure::RecordAdmission {
            request: envelope.clone(),
            state: ObserverBackpressureState::initial(12),
        }),
    ]
}

fn canonical_resume_round_trip(
    aggregate: &ClientParticipantAggregate,
) -> TestResult<ClientParticipantAggregate> {
    let record = aggregate
        .resume_record()
        .map_err(|_| "aggregate must encode for resume")?;
    let bytes = record.encode_canonical();
    ClientResumeRecord::decode_canonical(&bytes)
        .map_err(|_| "canonical resume bytes must decode")?
        .restore()
        .map_err(|_| "canonical resume record must restore")
}

fn restored_observer_abandonment() -> TestResult<ClientParticipantAggregate> {
    let request = ClientRequest::ObserverRecovery(ObserverRecoveryHandshake {
        observer_refusals: Vec::new(),
    });
    let ClientOperationRecordDecision::Pending(pending) = record_operation(bound()?, request)
    else {
        return Err("observer recovery must enter the write-ahead barrier");
    };
    let (aggregate, operation) = pending.commit().into_parts();
    let _ = operation.into_request();
    let restored = canonical_resume_round_trip(&aggregate)?;
    if restored.restored_operation_abandonment().is_none() {
        return Err("observer restore must retain an abandonment");
    }
    Ok(restored)
}

#[test]
fn item_1_exact_record_token_applies_and_clears_expected() -> TestResult {
    let (aggregate, correlation, _) = issued_record(TOKEN_A)?;
    let ClientCorrelatedInboundDecision::Applied(applied) =
        decide_correlated_inbound(aggregate, committed(TOKEN_A, 11)?, correlation)
    else {
        return Err("exact-token terminal answer must apply");
    };
    let (aggregate, value) = applied.into_parts();
    assert_eq!(value, committed(TOKEN_A, 11)?);
    assert!(!aggregate.has_expected_operation());
    assert!(aggregate.expected.is_none());
    Ok(())
}

#[test]
fn item_2_different_record_token_is_ambiguous_and_retains_authority() -> TestResult {
    let (aggregate, correlation, request) = issued_record(TOKEN_A)?;
    let ClientCorrelatedInboundDecision::Refused(refusal) =
        decide_correlated_inbound(aggregate, committed(TOKEN_B, 11)?, correlation)
    else {
        return Err("different-token terminal answer must be refused");
    };
    assert_eq!(
        refusal.reason(),
        ClientInboundRefusalReason::AmbiguousResponse
    );
    let (aggregate, refused_value, correlation) = refusal.into_parts();
    assert_eq!(refused_value, committed(TOKEN_B, 11)?);
    assert_eq!(
        aggregate
            .expected
            .as_ref()
            .map(|expected| &expected.request),
        Some(&request)
    );

    assert!(matches!(
        decide_correlated_inbound(aggregate, committed(TOKEN_A, 11)?, correlation),
        ClientCorrelatedInboundDecision::Applied(_)
    ));
    Ok(())
}

#[test]
fn item_3_unsealed_record_answer_requires_response_authority() -> TestResult {
    let (aggregate, correlation, request) = issued_record(TOKEN_A)?;
    let _ = correlation;
    let ClientInboundDecision::Refused(refusal) =
        decide_inbound(aggregate, committed(TOKEN_A, 11)?)
    else {
        return Err("unsealed RecordAdmission answer must be refused");
    };
    assert_eq!(
        refusal.reason(),
        ClientInboundRefusalReason::MissingResponseAuthority
    );
    let (aggregate, _) = refusal.into_parts();
    assert_eq!(
        aggregate
            .expected
            .as_ref()
            .map(|expected| &expected.request),
        Some(&request)
    );
    Ok(())
}

#[test]
fn item_4_all_eleven_record_outcomes_codec_round_trip_exact_token() -> TestResult {
    let envelope = record_envelope(TOKEN_A)?;
    let request = record_request(TOKEN_A)?;
    let mut outcomes = first_record_outcomes(&envelope)?;
    outcomes.extend(remaining_record_outcomes(&envelope));
    assert_eq!(outcomes.len(), 11);

    for outcome in outcomes {
        let (discriminant, body) = encode_server_value_body(&outcome, ProtocolVersion::V1)
            .map_err(|_| "RecordAdmission outcome must encode")?;
        let (decoded, version) = decode_server_value_body(discriminant, ProtocolVersion::V1, &body)
            .map_err(|_| "RecordAdmission outcome must decode")?;
        assert_eq!(version, ProtocolVersion::V1);
        assert_eq!(decoded, outcome);
        assert!(super::correlation::matches_request(&decoded, &request));
    }
    Ok(())
}

fn legacy_request_frame() -> TestResult<Vec<u8>> {
    let participant_payload_len = 6_usize
        .checked_add(LEGACY_R1_RECORD_ADMISSION_REQUEST_BODY.len())
        .ok_or("legacy fixture length must fit")?;
    let total_len = 10_usize
        .checked_add(participant_payload_len)
        .ok_or("legacy fixture length must fit")?;
    let encoded_payload_len: u32 = participant_payload_len
        .try_into()
        .map_err(|_| "legacy fixture length must fit u32")?;
    let mut bytes = vec![0; total_len];
    bytes[0] = 0x1A;
    bytes[6..10].copy_from_slice(&encoded_payload_len.to_be_bytes());
    bytes[10..12].copy_from_slice(&ProtocolVersion::V1.major.to_be_bytes());
    bytes[14..16].copy_from_slice(
        &ClientDiscriminant::RecordAdmission
            .wire_value()
            .to_be_bytes(),
    );
    bytes[16..].copy_from_slice(&LEGACY_R1_RECORD_ADMISSION_REQUEST_BODY);
    Ok(bytes)
}

#[test]
fn item_5_named_r1_record_fixtures_fail_staged_canonical_decode() -> TestResult {
    assert!(decode(&legacy_request_frame()?, ReceiverDirection::Server).is_err());
    assert!(
        decode_server_value_body(
            ServerDiscriminant::RecordCommitted,
            ProtocolVersion::V1,
            &LEGACY_R1_RECORD_COMMITTED_RESPONSE_BODY,
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn item_7_two_cycle_record_resume_keeps_expected_and_lost_testimony() -> TestResult {
    let (issued, correlation, request) = issued_record(TOKEN_A)?;
    let _ = correlation;
    let first_restore = canonical_resume_round_trip(&issued)?;
    assert_eq!(
        first_restore
            .lost_operation_testimony()
            .map(LostAuthorityTestimony::kind),
        Some(LostAuthorityKind::IssuedOperationCorrelation)
    );

    let second_restore = canonical_resume_round_trip(&first_restore)?;
    assert_eq!(
        second_restore
            .expected
            .as_ref()
            .map(|expected| &expected.request),
        Some(&request)
    );
    assert_eq!(
        second_restore
            .lost_operation_testimony()
            .map(LostAuthorityTestimony::kind),
        Some(LostAuthorityKind::IssuedOperationCorrelation)
    );
    Ok(())
}

#[test]
fn item_7_restored_observer_abandonment_only_blocks_fresh_observer() -> TestResult {
    let restored = restored_observer_abandonment()?;
    let ClientOperationRecordDecision::Pending(record_pending) =
        record_operation(restored, record_request(TOKEN_A)?)
    else {
        return Err("stored ObserverRecovery abandonment must admit RecordAdmission");
    };
    assert!(
        record_pending
            .successor
            .restored_operation_abandonment()
            .is_some()
    );

    let restored = restored_observer_abandonment()?;
    let ClientOperationRecordDecision::Refused(refusal) = record_operation(
        restored,
        ClientRequest::ObserverRecovery(ObserverRecoveryHandshake {
            observer_refusals: Vec::new(),
        }),
    ) else {
        return Err("stored ObserverRecovery abandonment must refuse another ObserverRecovery");
    };
    assert_eq!(
        refusal.reason(),
        ClientOperationRecordRefusalReason::AbandonmentPending
    );
    let (aggregate, _) = refusal.into_parts();
    assert!(aggregate.restored_operation_abandonment().is_some());
    Ok(())
}
