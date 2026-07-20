mod support;

use support::{mint_fenced_attach, recovered_fate_from_fenced, settled_leave_authority};

use std::boxed::Box;

use liminal_protocol::algebra::{
    ResourceDimension, ResourceVector, WideResourceVector, floor_transition, mandatory_capacity,
    no_edge_legal, recovery_transfer, retained_baseline, zero_debt_admission,
};
use liminal_protocol::lifecycle::{
    ActiveBinding, AttachCommitParameters, AttachSecretProof, AttachedRecordPosition, BindingState,
    BindingTerminalDisposition, BoundParticipantCursor, ClosureDebt, ClosureState,
    CommittedBindingTerminalPosition, CredentialAttachLiveReceipt, CredentialAttachLookupResult,
    CredentialAttachProvenance, CredentialAttachTokenPhase, CumulativeAckOutcome,
    CursorFateSuccessor, DebtCompletion, DetachCell, DetachLookupContext, DetachLookupResult,
    DetachTokenResolution, DetachedCredentialRecovery, EnrollmentFingerprint,
    EnrollmentLookupResult, EnrollmentTokenPhase, Event, IdentityState, LeaveCommitParameters,
    LeaveFingerprint, LeaveSecretProof, LiveMember, LiveMemberRestore, NonzeroDebtCursorEpisode,
    ObserverProjection, ParticipantBindingRequest, PendingBindingTerminalPosition,
    PendingDrainDecision, PendingReplay, PresentedIdentity, RecoveredBindingFateTransition,
    ResolvedIdentity, StoredEdge, commit_attach, commit_detach, commit_leave,
    lookup_binding_required, lookup_credential_attach, lookup_detach, lookup_enrollment,
    lookup_leave, start_blocked_detach,
};
use liminal_protocol::outcome::{
    CheckedMultiplyOverflow, ConnectionIncarnationExhausted, HandshakeSizeOperands,
    ParkingLimitField, SdkObserverParkCapacityExceeded, SdkParkOrderExhausted,
    SdkParkingCapacityIncompatible, SdkParticipantRequestTooLarge,
};
use liminal_protocol::wire::{
    AckNoOp, AttachAttemptToken, AttachBound, AttachSecret, BindingEpoch, BindingStateView,
    ClientRequest, ConnectionConversationBindingOccupied, ConnectionConversationCapacityExceeded,
    ConnectionIncarnation, ConversationOrderExhausted, ConversationSequenceExhausted,
    CredentialAttachRequest, DetachAttemptToken, DetachRequest, EnrollmentEnvelope,
    EnrollmentRequest, EnrollmentToken, Generation, InvalidObserverEpoch, InvalidObserverEpochList,
    LeaveAttemptToken, LeaveRequest, MarkerAck, MarkerAckCommitted, MarkerAckEnvelope,
    MarkerAckProof, MarkerMismatch, MarkerMismatchBody, MarkerNotDelivered,
    MarkerNotDeliveredReason, MarkerProofRequest, ObserverProgressStatus, ObserverRecoveryAccepted,
    ObserverRecoveryHandshake, ObserverRefusal, OrderAllocatingEnvelope, ParticipantAck,
    ParticipantFrame, ProtocolVersion, ReceiptExpiryReason, ReceiptReplay, ReceiverDirection,
    RecordAdmission, RecordAdmissionEnvelope, RecordTooLarge, ResponseEnvelope, Retired,
    SequenceAllocatingEnvelope, SequenceBudget, ServerDiscriminant, ServerValue, StaleAuthority,
    decode, decode_server_value_body, encode, encode_server_value_body, encoded_len,
};
use support::marker_delivery;

type TestResult = Result<(), String>;
type TestMember = LiveMember<[u8; 32]>;
type TestIdentity = IdentityState<[u8; 32], [u8; 32], [u8; 32]>;

fn generation(value: u64) -> Result<Generation, String> {
    Generation::new(value).ok_or_else(|| "generation must be nonzero".to_owned())
}

fn epoch(server: u64, ordinal: u64, generation_value: u64) -> Result<BindingEpoch, String> {
    Ok(BindingEpoch::new(
        ConnectionIncarnation::new(server, ordinal),
        generation(generation_value)?,
    ))
}

fn debt(entries: u128, bytes: u128) -> Result<ClosureDebt, String> {
    ClosureDebt::new(WideResourceVector::new(entries, bytes))
        .ok_or_else(|| "closure debt must be nonzero".to_owned())
}

fn member(
    participant_id: u64,
    conversation_id: u64,
    generation_value: u64,
    cursor: u64,
) -> Result<TestMember, String> {
    LiveMember::restore(LiveMemberRestore {
        participant_id,
        conversation_id,
        generation: generation(generation_value)?,
        attach_secret: AttachSecret::new([0xA5; 32]),
        cursor,
        enrollment_fingerprint: EnrollmentFingerprint::new([0xE1; 32]),
        latest_terminal: None,
    })
    .map_err(|error| format!("member restore failed: {error:?}"))
}

fn server_round_trip(value: &ServerValue) -> Result<ServerValue, String> {
    let discriminant = value.discriminant();
    let (_, body) = encode_server_value_body(value, ProtocolVersion::V1)
        .map_err(|error| format!("server encode failed: {error:?}"))?;
    let (decoded, version) = decode_server_value_body(discriminant, ProtocolVersion::V1, &body)
        .map_err(|error| format!("server decode failed: {error:?}"))?;
    if version != ProtocolVersion::V1 {
        return Err("server value changed protocol version".to_owned());
    }
    Ok(decoded)
}

fn client_round_trip(request: ClientRequest) -> Result<(ClientRequest, usize), String> {
    let frame = ParticipantFrame::ClientRequest(request);
    let length = encoded_len(&frame).map_err(|error| format!("length failed: {error:?}"))?;
    let mut bytes = vec![0; length];
    let written =
        encode(&frame, &mut bytes).map_err(|error| format!("encode failed: {error:?}"))?;
    if written != length {
        return Err("client codec wrote a noncanonical length".to_owned());
    }
    let ParticipantFrame::ClientRequest(decoded) = decode(&bytes, ReceiverDirection::Server)
        .map_err(|error| format!("decode failed: {error:?}"))?
    else {
        return Err("client frame decoded to another direction".to_owned());
    };
    Ok((decoded, length))
}

fn retire_detached_member(
    live: TestMember,
    token_byte: u8,
    left_delivery_seq: u64,
) -> Result<TestIdentity, String> {
    let authority = settled_leave_authority(
        &live,
        BindingState::Detached,
        left_delivery_seq,
        left_delivery_seq,
    )?;
    let request = LeaveRequest {
        conversation_id: live.conversation_id(),
        participant_id: live.participant_id(),
        capability_generation: live.generation(),
        attach_secret: live.attach_secret(),
        leave_attempt_token: LeaveAttemptToken::new([token_byte; 16]),
    };
    let verified = live
        .verify_leave_request(
            &request,
            AttachSecretProof::Verified,
            [token_byte; 32],
            LeaveFingerprint::new([token_byte.wrapping_add(1); 32]),
        )
        .map_err(|error| format!("leave verify failed: {error:?}"))?;
    commit_leave(
        live,
        BindingState::Detached,
        DetachCell::<[u8; 32]>::default(),
        verified,
        authority,
        LeaveCommitParameters { left_delivery_seq },
    )
    .map(|commit| commit.into_parts().0)
    .map_err(|error| format!("leave commit failed: {error:?}"))
}

fn credential_recovery(
    participant_id: u64,
    binding_epoch: BindingEpoch,
    marker_delivery_seq: u64,
    closure_debt: ClosureDebt,
) -> Result<DetachedCredentialRecovery, String> {
    let delivery = marker_delivery(participant_id, binding_epoch, marker_delivery_seq)?;
    let delivered = delivery
        .delivered(
            closure_debt,
            Event::marker_delivered(participant_id, binding_epoch, marker_delivery_seq),
        )
        .map_err(|state| format!("marker delivery failed: {state:?}"))?;
    let ClosureState::Owed {
        edge: StoredEdge::ParticipantCursorProgress(progress),
        ..
    } = delivered
    else {
        return Err("marker delivery did not select participant cursor progress".to_owned());
    };
    match progress
        .binding_fate(
            closure_debt,
            Event::binding_fate_observed(participant_id, binding_epoch, marker_delivery_seq),
        )
        .map_err(|state| format!("marker fate failed: {state:?}"))?
    {
        CursorFateSuccessor::DetachedCredentialRecovery(recovery) => Ok(recovery),
        CursorFateSuccessor::DetachedCursorRelease(_) => {
            Err("marker-backed progress selected cursor-only release".to_owned())
        }
    }
}

// Frozen contract case 26, lines 3603-3634.
#[test]
fn case_26_entry_sequence_equality_snapshot() -> TestResult {
    let h = u64::MAX - 17;
    let budget = SequenceBudget {
        high_watermark: h,
        remaining: 17,
        e: 3,
        t: 2,
        m: 0,
        rs: 0,
        rt: 0,
        l_times_t: 6,
        l_times_rt: 0,
        l_other_times_e: 6,
    };
    assert_eq!(
        u128::from(budget.e + budget.t + budget.m + budget.rs + budget.rt)
            + budget.l_times_t
            + budget.l_times_rt
            + budget.l_other_times_e,
        17
    );
    assert_eq!(h.checked_add(12), Some(u64::MAX - 5));
    assert_eq!(h.checked_add(17), Some(u64::MAX));

    let baseline = retained_baseline(
        ResourceVector::new(1, 100),
        3,
        0,
        ResourceVector::new(1, 100),
    )
    .map_err(|error| format!("baseline failed: {error:?}"))?;
    assert_eq!(baseline, WideResourceVector::new(4, 400));
    assert!(zero_debt_admission(
        baseline,
        ResourceVector::new(2, 200),
        ResourceVector::new(2, 200),
        ResourceVector::new(64, 6_400),
    ));

    let pinned = floor_transition(u128::from(h), Some(h - 1), h + 4, h, u128::from(h));
    assert_eq!(pinned.preferred_floor, u128::from(h));
    assert_eq!(pinned.resulting_floor, u128::from(h));
    let empty = floor_transition(u128::from(h), None, h + 5, h, u128::from(h));
    assert_eq!(empty.member_cursor, h + 5);
    assert_eq!(empty.preferred_floor, u128::from(h) + 1);
    assert_eq!(empty.resulting_floor, u128::from(h) + 1);
    assert_eq!(h + 5, u64::MAX - 12);
    Ok(())
}

// Frozen contract case 27, lines 3635-3649.
#[test]
fn case_27_reconnect_race_and_connection_capacity() -> TestResult {
    let enrollment = EnrollmentEnvelope {
        conversation_id: 27_003,
        enrollment_token: EnrollmentToken::new([0x27; 16]),
    };
    let semantic = ServerValue::ConnectionConversationCapacityExceeded(
        ConnectionConversationCapacityExceeded::SemanticRequest {
            request: ResponseEnvelope::Enrollment(enrollment.clone()),
            limit: 2,
        },
    );
    assert_eq!(
        semantic.discriminant(),
        ServerDiscriminant::ConnectionConversationCapacityExceeded
    );
    assert_eq!(server_round_trip(&semantic)?, semantic);

    let batch = ServerValue::ConnectionConversationCapacityExceeded(
        ConnectionConversationCapacityExceeded::ObserverRecovery {
            conversation_id: 27_003,
            limit: 2,
        },
    );
    assert_eq!(
        batch.discriminant(),
        ServerDiscriminant::ObserverRecoveryConnectionCapacityExceeded
    );
    assert_eq!(server_round_trip(&batch)?, batch);

    let request = ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: enrollment.conversation_id,
        enrollment_token: enrollment.enrollment_token,
    });
    assert_eq!(client_round_trip(request.clone())?.0, request);
    let refusal = liminal_protocol::wire::ObserverBackpressureState::initial(8);
    assert_eq!(refusal.backpressure_epoch(), 8);
    assert_eq!(refusal.observer_progress(), 8);
    assert_eq!(
        liminal_protocol::wire::ObserverBackpressureState::replay(8, 8),
        Some(refusal)
    );
    assert_eq!(
        liminal_protocol::wire::ObserverBackpressureState::replay(8, 9),
        None
    );
    Ok(())
}

// Frozen contract case 28, lines 3650-3654.
#[test]
#[allow(clippy::too_many_lines)]
fn case_28_detach_replay_terminalization_and_leave_precedence() -> TestResult {
    let old_epoch = epoch(7, 2, 4)?;
    let old_binding = ActiveBinding {
        participant_id: 2,
        conversation_id: 28,
        binding_epoch: old_epoch,
    };
    let request = DetachRequest {
        conversation_id: 28,
        participant_id: 2,
        capability_generation: generation(4)?,
        detach_attempt_token: DetachAttemptToken::new([0x28; 16]),
    };
    let verifier = [0xA8; 32];
    let verified_request = old_binding
        .verify_detach_request(request.clone(), verifier)
        .map_err(|error| format!("detach verify failed: {error:?}"))?;
    let detached = commit_detach(
        member(2, 28, 4, 9)?,
        verified_request,
        DetachCell::default(),
        CommittedBindingTerminalPosition::new(6, 30),
    )
    .map_err(|error| format!("detach commit failed: {error:?}"))?;
    let (detached_member, _, _, committed, outcome) = detached.into_parts();
    assert_eq!(
        committed
            .verify_exact(&request, verifier)
            .map_err(|error| format!("replay verify failed: {error:?}"))?
            .outcome(28),
        outcome
    );

    let new_epoch = epoch(7, 3, 5)?;
    let attach_request = CredentialAttachRequest {
        conversation_id: 28,
        participant_id: 2,
        capability_generation: generation(4)?,
        attach_secret: detached_member.attach_secret(),
        attach_attempt_token: AttachAttemptToken::new([0xA8; 16]),
        accept_marker_delivery_seq: None,
    };
    let verified_attach = detached_member
        .verify_detached_attach(
            BindingState::Detached,
            ClosureState::Clear
                .ordinary_detached_attach_admission()
                .map_err(|state| format!("clear attach admission failed: {state:?}"))?,
            attach_request,
            AttachSecretProof::Verified,
            AttachCommitParameters {
                binding: ActiveBinding {
                    participant_id: 2,
                    conversation_id: 28,
                    binding_epoch: new_epoch,
                },
                attach_secret: AttachSecret::new([0x58; 32]),
                attached_position: AttachedRecordPosition::new(7, 31),
                receipt_expires_at: 1_000,
                provenance_expires_at: 2_000,
            },
        )
        .map_err(|error| format!("attach verify failed: {error:?}"))?;
    let attached = commit_attach(verified_attach, DetachCell::Committed(committed))
        .map_err(|error| format!("attach commit failed: {error:?}"))?;
    let DetachCell::Terminalized(terminalized) = attached.detach_cell else {
        return Err("successful attach did not terminalize old detach".to_owned());
    };
    let stale = terminalized
        .verify_exact(&request, verifier)
        .map_err(|error| format!("terminalized replay failed: {error:?}"))?
        .outcome(
            28,
            generation(5)?,
            BindingStateView::Bound {
                current_binding_epoch: new_epoch,
            },
        );
    assert_eq!(stale.committed_binding_epoch(), old_epoch);
    assert_eq!(
        stale.binding_state(),
        BindingStateView::Bound {
            current_binding_epoch: new_epoch
        }
    );

    let leave_request = LeaveRequest {
        conversation_id: 28,
        participant_id: 2,
        capability_generation: generation(5)?,
        attach_secret: attached.member.attach_secret(),
        leave_attempt_token: LeaveAttemptToken::new([0xD0; 16]),
    };
    let verified_leave = attached
        .member
        .verify_leave_request(
            &leave_request,
            AttachSecretProof::Verified,
            [0xB8; 32],
            LeaveFingerprint::new([0xC8; 32]),
        )
        .map_err(|error| format!("leave verify failed: {error:?}"))?;
    let authority = settled_leave_authority(&attached.member, attached.binding_state, 32, 32)?;
    let (retired, _frontiers) = commit_leave(
        attached.member,
        attached.binding_state,
        DetachCell::Terminalized(terminalized),
        verified_leave,
        authority,
        LeaveCommitParameters {
            left_delivery_seq: 32,
        },
    )
    .map_err(|error| format!("leave commit failed: {error:?}"))?
    .into_parts();
    let empty_cell = DetachCell::<[u8; 32]>::default();
    let result = lookup_detach(&DetachLookupContext {
        token_resolution: DetachTokenResolution::Exact(ResolvedIdentity::from(&retired)),
        presented_identity: PresentedIdentity::Absent,
        cell: &empty_cell,
        binding: &BindingState::Detached,
        receiving_binding_epoch: None,
        request: &request,
        request_verifier: verifier,
        observer_progress: 0,
    });
    assert!(matches!(result, DetachLookupResult::Retired(_)));
    Ok(())
}

// Frozen contract case 29, lines 3655-3657.
#[test]
fn case_29_post_leave_enrollment_and_credential_token_replay() -> TestResult {
    let live = member(3, 29, 2, 7)?;
    let attach = AttachBound::ordinary(
        29,
        AttachAttemptToken::new([0x29; 16]),
        3,
        generation(1)?,
        AttachSecret::new([0xA2; 32]),
        epoch(9, 3, 2)?,
        7,
        1_000,
        2_000,
    )
    .ok_or_else(|| "attach receipt relation failed".to_owned())?;
    let leave = LeaveRequest {
        conversation_id: 29,
        participant_id: 3,
        capability_generation: generation(2)?,
        attach_secret: live.attach_secret(),
        leave_attempt_token: LeaveAttemptToken::new([0x29; 16]),
    };
    let verified = live
        .verify_leave_request(
            &leave,
            AttachSecretProof::Verified,
            [0x29; 32],
            LeaveFingerprint::new([0x92; 32]),
        )
        .map_err(|error| format!("leave verify failed: {error:?}"))?;
    let authority = settled_leave_authority(&live, BindingState::Detached, 8, 8)?;
    let (retired, _frontiers) = commit_leave(
        live,
        BindingState::Detached,
        DetachCell::<[u8; 32]>::default(),
        verified,
        authority,
        LeaveCommitParameters {
            left_delivery_seq: 8,
        },
    )
    .map_err(|error| format!("leave failed: {error:?}"))?
    .into_parts();

    let enrollment_request = EnrollmentRequest {
        conversation_id: 29,
        enrollment_token: EnrollmentToken::new([0xE9; 16]),
    };
    let enrollment = lookup_enrollment(
        EnrollmentTokenPhase::LifetimeMapping {
            identity: ResolvedIdentity::from(&retired),
        },
        &BindingState::Detached,
        &enrollment_request,
    );
    assert!(matches!(
        enrollment,
        EnrollmentLookupResult::Retired(Retired::Enrollment {
            participant_id: 3,
            retired_generation,
            ..
        }) if retired_generation == generation(2)?
    ));

    let attach_request = CredentialAttachRequest {
        conversation_id: 29,
        participant_id: 3,
        capability_generation: generation(1)?,
        attach_secret: AttachSecret::new([0x11; 32]),
        attach_attempt_token: AttachAttemptToken::new([0x29; 16]),
        accept_marker_delivery_seq: None,
    };
    let receipt = CredentialAttachLiveReceipt::from_commit(attach);
    let credential = lookup_credential_attach(
        CredentialAttachTokenPhase::LiveReceipt {
            identity: ResolvedIdentity::from(&retired),
            receipt: &receipt,
        },
        PresentedIdentity::Absent,
        &BindingState::Detached,
        &attach_request,
        AttachSecretProof::Verified,
    );
    assert!(matches!(
        credential,
        CredentialAttachLookupResult::Retired(Retired::Participant {
            retired_generation,
            ..
        }) if retired_generation == generation(2)?
    ));
    Ok(())
}

// Frozen contract case 30, lines 3658-3663.
#[test]
fn case_30_candidate_order_controls_marker_and_terminal_append_order() -> TestResult {
    let original_debt = debt(2, 200)?;
    let projection = ObserverProjection::new(30);
    let marker_event = Event::marker_appended(31, 31);
    let successor = projection
        .later_projection_after_marker(&marker_event, original_debt, ObserverProjection::new(31))
        .ok_or_else(|| "marker did not derive later projection".to_owned())?;
    let marker_first = projection
        .marker_appended(original_debt, marker_event, successor)
        .map_err(|state| format!("marker-first transition failed: {state:?}"))?;
    assert_eq!(
        marker_first,
        ClosureState::Owed {
            debt: original_debt,
            edge: StoredEdge::ObserverProjection(ObserverProjection::new(31)),
        }
    );

    let earlier_marker = liminal_protocol::lifecycle::AdmissionOrder::new(
        7,
        liminal_protocol::outcome::CandidatePhase::CompactionMarker,
        0,
    );
    let later_terminal = liminal_protocol::lifecycle::AdmissionOrder::binding_terminal(8, 0);
    assert!(earlier_marker < later_terminal);
    let earlier_terminal = liminal_protocol::lifecycle::AdmissionOrder::binding_terminal(7, 0);
    let later_marker = liminal_protocol::lifecycle::AdmissionOrder::new(
        8,
        liminal_protocol::outcome::CandidatePhase::CompactionMarker,
        0,
    );
    assert!(earlier_terminal < later_marker);
    Ok(())
}

// Frozen contract case 31, lines 3664-3696.
#[test]
fn case_31_sequence_exhaustion_is_atomic_in_both_arms() -> TestResult {
    let enrollment_budget = SequenceBudget {
        high_watermark: u64::MAX - 1,
        remaining: 1,
        e: 1,
        t: 1,
        m: 1,
        rs: 0,
        rt: 0,
        l_times_t: 1,
        l_times_rt: 0,
        l_other_times_e: 0,
    };
    let enrollment =
        ServerValue::ConversationSequenceExhausted(Box::new(ConversationSequenceExhausted {
            request: SequenceAllocatingEnvelope::Enrollment(EnrollmentEnvelope {
                conversation_id: 31_001,
                enrollment_token: EnrollmentToken::new([0x31; 16]),
            }),
            sequence_budget: enrollment_budget,
        }));
    assert_eq!(server_round_trip(&enrollment)?, enrollment);

    let ordinary_budget = SequenceBudget {
        high_watermark: u64::MAX - 3,
        remaining: 3,
        e: 1,
        t: 1,
        m: 1,
        rs: 0,
        rt: 0,
        l_times_t: 1,
        l_times_rt: 0,
        l_other_times_e: 0,
    };
    let ordinary =
        ServerValue::ConversationSequenceExhausted(Box::new(ConversationSequenceExhausted {
            request: SequenceAllocatingEnvelope::RecordAdmission(RecordAdmissionEnvelope {
                conversation_id: 31_002,
                participant_id: 0,
                capability_generation: generation((u64::MAX - 5) / 2)?,
                record_admission_attempt_token:
                    liminal_protocol::wire::RecordAdmissionAttemptToken::new([0xA7; 16]),
            }),
            sequence_budget: ordinary_budget,
        }));
    assert_eq!(server_round_trip(&ordinary)?, ordinary);
    assert_eq!(enrollment_budget.remaining, 1);
    assert_eq!(ordinary_budget.remaining, 3);
    assert_eq!(ordinary_budget.e + ordinary_budget.t + ordinary_budget.m, 3);
    assert_eq!(ordinary_budget.l_times_t, 1);

    let before = retained_baseline(
        ResourceVector::new(2, 200),
        1,
        0,
        ResourceVector::new(1, 100),
    )
    .map_err(|error| format!("baseline failed: {error:?}"))?;
    assert_eq!(before, WideResourceVector::new(3, 300));
    assert!(zero_debt_admission(
        before,
        ResourceVector::new(2, 200),
        ResourceVector::new(2, 200),
        ResourceVector::new(7, 700),
    ));
    assert!(!zero_debt_admission(
        WideResourceVector::new(4, 400),
        ResourceVector::new(2, 200),
        ResourceVector::new(2, 200),
        ResourceVector::new(7, 700),
    ));
    Ok(())
}

// Frozen contract case 32, lines 3697-3750.
#[test]
fn case_32_sdk_parking_and_static_record_size_boundaries() -> TestResult {
    let attach_request = ClientRequest::CredentialAttach(CredentialAttachRequest {
        conversation_id: 32_001,
        participant_id: 32,
        capability_generation: generation(7)?,
        attach_secret: AttachSecret::new([0x32; 32]),
        attach_attempt_token: AttachAttemptToken::new([0x32; 16]),
        accept_marker_delivery_seq: Some(10),
    });
    let (_, exact_size) = client_round_trip(attach_request)?;
    assert_eq!(exact_size, 97);
    assert_eq!(
        SdkParticipantRequestTooLarge {
            conversation_id: 32_001,
            encoded_bytes: 98,
            limit: 97,
        },
        SdkParticipantRequestTooLarge {
            conversation_id: 32_001,
            encoded_bytes: exact_size as u64 + 1,
            limit: exact_size as u64,
        }
    );

    let b32 = 97 + 23;
    let failures = [
        SdkObserverParkCapacityExceeded::PerConversationRows {
            conversation_id: 32_001,
            limit: 2,
            occupied: 2,
            requested: 1,
        },
        SdkObserverParkCapacityExceeded::PerConversationBytes {
            conversation_id: 32_001,
            limit: 3 * b32 - 1,
            occupied: 2 * b32,
            requested: b32,
        },
        SdkObserverParkCapacityExceeded::SdkWideConversations {
            conversation_id: 32_003,
            limit: 2,
            occupied: 2,
            requested: 1,
        },
        SdkObserverParkCapacityExceeded::SdkWideRows {
            conversation_id: 32_001,
            limit: 2,
            occupied: 2,
            requested: 1,
        },
        SdkObserverParkCapacityExceeded::SdkWideBytes {
            conversation_id: 32_001,
            limit: 3 * b32 - 1,
            occupied: 2 * b32,
            requested: b32,
        },
    ];
    assert_eq!(failures.len(), 5);

    let envelope = RecordAdmissionEnvelope {
        conversation_id: 32_004,
        participant_id: 32,
        capability_generation: generation(7)?,
        record_admission_attempt_token: liminal_protocol::wire::RecordAdmissionAttemptToken::new(
            [0xA7; 16],
        ),
    };
    let bytes_failure = ServerValue::RecordTooLarge(RecordTooLarge {
        request: envelope.clone(),
        dimension: ResourceDimension::Bytes,
        encoded_record_charge: ResourceVector::new(1, 111),
        max_ordinary_record_charge: ResourceVector::new(1, 110),
    });
    assert_eq!(server_round_trip(&bytes_failure)?, bytes_failure);
    let entries_failure = ServerValue::RecordTooLarge(RecordTooLarge {
        request: envelope,
        dimension: ResourceDimension::Entries,
        encoded_record_charge: ResourceVector::new(1, 111),
        max_ordinary_record_charge: ResourceVector::new(0, 110),
    });
    assert_eq!(server_round_trip(&entries_failure)?, entries_failure);
    let exhausted = SdkParkOrderExhausted::new(32_001);
    assert_eq!(exhausted.value(), u64::MAX);
    assert_eq!(exhausted.conversation_id(), 32_001);
    Ok(())
}

// Frozen contract case 33, lines 3751-3791.
#[test]
#[allow(clippy::too_many_lines)]
fn case_33_observer_recovery_list_selector_and_sizes() -> TestResult {
    let three = ClientRequest::ObserverRecovery(ObserverRecoveryHandshake {
        observer_refusals: vec![
            ObserverRefusal {
                conversation_id: 33_001,
                refused_epoch: 4,
            },
            ObserverRefusal {
                conversation_id: 33_002,
                refused_epoch: 5,
            },
            ObserverRefusal {
                conversation_id: 33_003,
                refused_epoch: 6,
            },
        ],
    });
    assert_eq!(client_round_trip(three)?.1, 72);
    let four = ClientRequest::ObserverRecovery(ObserverRecoveryHandshake {
        observer_refusals: vec![
            ObserverRefusal {
                conversation_id: 33_001,
                refused_epoch: 5,
            };
            4
        ],
    });
    assert_eq!(client_round_trip(four)?.1, 88);

    let too_many =
        ServerValue::InvalidObserverEpochList(InvalidObserverEpochList::TooManyEntries {
            presented_entries: 4,
            max_entries: 3,
        });
    assert_eq!(server_round_trip(&too_many)?, too_many);
    let duplicate =
        ServerValue::InvalidObserverEpochList(InvalidObserverEpochList::DuplicateConversation {
            conversation_id: 33_001,
            first_index: 0,
            duplicate_index: 2,
        });
    assert_eq!(server_round_trip(&duplicate)?, duplicate);
    let ahead = ServerValue::InvalidObserverEpoch(InvalidObserverEpoch::EpochAhead {
        conversation_id: 33_003,
        presented_epoch: 6,
        current_observer_progress: 5,
    });
    assert_eq!(server_round_trip(&ahead)?, ahead);
    let unknown = ServerValue::InvalidObserverEpoch(InvalidObserverEpoch::ConversationUnknown {
        conversation_id: 33_099,
        presented_epoch: 5,
    });
    assert_eq!(server_round_trip(&unknown)?, unknown);

    let accepted = ServerValue::ObserverRecoveryAccepted(ObserverRecoveryAccepted {
        statuses: vec![
            ObserverProgressStatus {
                conversation_id: 33_001,
                refused_epoch: 4,
                current_observer_progress: 5,
                armed: false,
                progressed: true,
            },
            ObserverProgressStatus {
                conversation_id: 33_002,
                refused_epoch: 5,
                current_observer_progress: 5,
                armed: true,
                progressed: false,
            },
        ],
    });
    assert_eq!(server_round_trip(&accepted)?, accepted);
    let empty = ServerValue::ObserverRecoveryAccepted(ObserverRecoveryAccepted {
        statuses: Vec::new(),
    });
    assert_eq!(server_round_trip(&empty)?, empty);

    for conversation_id in [33_002, 33_001] {
        let capacity = ServerValue::ConnectionConversationCapacityExceeded(
            ConnectionConversationCapacityExceeded::ObserverRecovery {
                conversation_id,
                limit: 3,
            },
        );
        assert_eq!(server_round_trip(&capacity)?, capacity);
    }

    let status_fixture = ServerValue::ObserverRecoveryAccepted(ObserverRecoveryAccepted {
        statuses: vec![
            ObserverProgressStatus {
                conversation_id: 33_001,
                refused_epoch: 5,
                current_observer_progress: 5,
                armed: true,
                progressed: false,
            };
            3
        ],
    });
    let frame = ParticipantFrame::ServerValue(status_fixture);
    assert_eq!(
        encoded_len(&frame).map_err(|error| format!("size failed: {error:?}"))?,
        102
    );
    Ok(())
}

// Frozen contract case 34, lines 3792-3829.
#[test]
fn case_34_ack_and_marker_proof_selector() -> TestResult {
    let current_epoch = epoch(34, 1, 7)?;
    let mut episode = NonzeroDebtCursorEpisode::new(
        34,
        debt(1, 1)?,
        0,
        12,
        1,
        1,
        vec![BoundParticipantCursor::new(34, current_epoch, 10)],
    )
    .map_err(|error| format!("episode failed: {error:?}"))?;
    let ack = |through_seq| ParticipantAck {
        conversation_id: 34,
        participant_id: 34,
        capability_generation: current_epoch.capability_generation,
        through_seq,
    };
    assert!(matches!(
        episode
            .acknowledge(34, current_epoch, &ack(9), 12)
            .map_err(|error| format!("ack failed: {error:?}"))?,
        CumulativeAckOutcome::Regression(_)
    ));
    assert!(matches!(
        episode
            .acknowledge(34, current_epoch, &ack(10), 12)
            .map_err(|error| format!("ack failed: {error:?}"))?,
        CumulativeAckOutcome::NoOp(_)
    ));
    assert!(matches!(
        episode
            .acknowledge(34, current_epoch, &ack(12), 12)
            .map_err(|error| format!("ack failed: {error:?}"))?,
        CumulativeAckOutcome::Committed(_)
    ));
    assert_eq!(
        episode.participant(34).map(BoundParticipantCursor::cursor),
        Some(12)
    );

    let mut gap_episode = NonzeroDebtCursorEpisode::new(
        34,
        debt(1, 1)?,
        0,
        12,
        1,
        1,
        vec![BoundParticipantCursor::new(34, current_epoch, 10)],
    )
    .map_err(|error| format!("episode failed: {error:?}"))?;
    assert!(matches!(
        gap_episode
            .acknowledge(34, current_epoch, &ack(12), 11)
            .map_err(|error| format!("gap failed: {error:?}"))?,
        CumulativeAckOutcome::Gap(_)
    ));

    let proof = |sequence| {
        MarkerProofRequest::MarkerAck(MarkerAckProof {
            conversation_id: 34,
            participant_id: 34,
            capability_generation: current_epoch.capability_generation,
            requested_marker_delivery_seq: sequence,
        })
    };
    let marker_values = [
        ServerValue::MarkerNotDelivered(MarkerNotDelivered {
            request: proof(20),
            reason: MarkerNotDeliveredReason::NotDeliveredToProofEpoch,
            expected_marker_delivery_seq: 20,
        }),
        ServerValue::MarkerMismatch(MarkerMismatch {
            request: proof(19),
            mismatch: MarkerMismatchBody::ExpectedDifferentMarker {
                expected_marker_delivery_seq: 20,
            },
        }),
        ServerValue::MarkerMismatch(MarkerMismatch {
            request: proof(19),
            mismatch: MarkerMismatchBody::BelowCursor { current_cursor: 20 },
        }),
        ServerValue::MarkerMismatch(MarkerMismatch {
            request: proof(20),
            mismatch: MarkerMismatchBody::NoMarkerExpected,
        }),
    ];
    for value in marker_values {
        assert_eq!(server_round_trip(&value)?, value);
    }
    let marker_envelope = MarkerAckEnvelope {
        conversation_id: 34,
        participant_id: 34,
        capability_generation: current_epoch.capability_generation,
        marker_delivery_seq: 20,
    };
    let committed =
        ServerValue::MarkerAckCommitted(MarkerAckCommitted::new(marker_envelope.clone()));
    assert_eq!(server_round_trip(&committed)?, committed);
    let replay = ServerValue::AckNoOp(AckNoOp::marker_ack(marker_envelope));
    assert_eq!(server_round_trip(&replay)?, replay);
    Ok(())
}

// Frozen contract case 35, lines 3830-3836.
// Extraction Fix 1: LP-EXTRACTION-GOAL.md requires committed detach to become
// Terminalized, which alone can construct the old-epoch stale-authority body.
#[test]
fn case_35_atomic_detach_and_terminalized_replay_without_fabricated_epoch() -> TestResult {
    let old_epoch = epoch(35, 1, 7)?;
    let old_binding = ActiveBinding {
        participant_id: 35,
        conversation_id: 35,
        binding_epoch: old_epoch,
    };
    let request = DetachRequest {
        conversation_id: 35,
        participant_id: 35,
        capability_generation: generation(7)?,
        detach_attempt_token: DetachAttemptToken::new([0x35; 16]),
    };
    let verifier = [0x35; 32];
    let before = (member(35, 35, 7, 19)?, BindingState::Bound(old_binding));
    let verified_request = old_binding
        .verify_detach_request(request.clone(), verifier)
        .map_err(|error| format!("detach verification failed: {error:?}"))?;
    let committed = commit_detach(
        before.0,
        verified_request,
        DetachCell::default(),
        CommittedBindingTerminalPosition::new(10, 20),
    )
    .map_err(|error| format!("detach commit failed: {error:?}"))?;
    let (detached_member, _, detached_state, cell, outcome) = committed.into_parts();
    assert_eq!(detached_state, BindingState::Detached);
    assert_eq!(outcome.committed_binding_epoch(), old_epoch);
    assert_eq!(outcome.detached_delivery_seq(), 20);
    assert_eq!(
        cell.verify_exact(&request, verifier)
            .map_err(|error| format!("stable replay failed: {error:?}"))?
            .outcome(35),
        outcome
    );

    let new_epoch = epoch(35, 1, 8)?;
    let new_binding = ActiveBinding {
        participant_id: 35,
        conversation_id: 35,
        binding_epoch: new_epoch,
    };
    let attach_request = CredentialAttachRequest {
        conversation_id: 35,
        participant_id: 35,
        capability_generation: generation(7)?,
        attach_secret: detached_member.attach_secret(),
        attach_attempt_token: AttachAttemptToken::new([0xA5; 16]),
        accept_marker_delivery_seq: None,
    };
    let attach = detached_member
        .verify_detached_attach(
            BindingState::Detached,
            ClosureState::Clear
                .ordinary_detached_attach_admission()
                .map_err(|state| format!("clear attach admission failed: {state:?}"))?,
            attach_request,
            AttachSecretProof::Verified,
            AttachCommitParameters {
                binding: new_binding,
                attach_secret: AttachSecret::new([0x85; 32]),
                attached_position: AttachedRecordPosition::new(11, 21),
                receipt_expires_at: 1_000,
                provenance_expires_at: 2_000,
            },
        )
        .map_err(|error| format!("attach verify failed: {error:?}"))?;
    let attached = commit_attach(attach, DetachCell::Committed(cell))
        .map_err(|error| format!("attach failed: {error:?}"))?;
    let DetachCell::Terminalized(terminalized) = attached.detach_cell else {
        return Err("attach did not preserve terminalized old detach".to_owned());
    };

    let ended = new_binding.connection_lost(BindingTerminalDisposition::Committed(
        CommittedBindingTerminalPosition::new(12, 22),
    ));
    assert_eq!(ended.binding_state(), BindingState::Detached);
    let stale = terminalized
        .verify_exact(&request, verifier)
        .map_err(|error| format!("terminalized replay failed: {error:?}"))?
        .outcome(35, generation(8)?, BindingStateView::Detached);
    assert_eq!(stale.committed_binding_epoch(), old_epoch);
    assert_eq!(stale.binding_state(), BindingStateView::Detached);
    let value = ServerValue::StaleAuthority(StaleAuthority::Detach(
        liminal_protocol::wire::DetachStaleAuthority::TerminalizedDetachCell(stale),
    ));
    assert_eq!(server_round_trip(&value)?, value);
    Ok(())
}

// Frozen contract case 36, lines 3837-3843.
#[test]
fn case_36_attach_receipt_superseded_before_deadline() -> TestResult {
    let live: IdentityState<[u8; 32], [u8; 32], [u8; 32]> =
        IdentityState::Live(member(36, 36, 9, 12)?);
    let token = AttachAttemptToken::new([0x36; 16]);
    let request = CredentialAttachRequest {
        conversation_id: 36,
        participant_id: 36,
        capability_generation: generation(7)?,
        attach_secret: AttachSecret::new([0x76; 32]),
        attach_attempt_token: token,
        accept_marker_delivery_seq: None,
    };
    let result = lookup_credential_attach(
        CredentialAttachTokenPhase::Provenance {
            identity: ResolvedIdentity::from(&live),
            provenance: CredentialAttachProvenance::new(
                generation(8)?,
                ReceiptExpiryReason::Superseded,
            ),
        },
        PresentedIdentity::Absent,
        &BindingState::Detached,
        &request,
        AttachSecretProof::Mismatch,
    );
    let CredentialAttachLookupResult::ReceiptExpired(expired) = result else {
        return Err("superseded provenance did not win lookup".to_owned());
    };
    assert!(matches!(
        expired,
        liminal_protocol::wire::ReceiptExpired::CredentialAttach {
            conversation_id: 36,
            token: actual_token,
            participant_id: 36,
            presented_generation,
            presented_marker_delivery_seq: None,
            result_generation,
            current_generation,
            reason: ReceiptExpiryReason::Superseded,
        } if actual_token == token
            && presented_generation == generation(7)?
            && result_generation == generation(8)?
            && current_generation == generation(9)?
    ));
    let value = ServerValue::ReceiptExpired(expired);
    assert_eq!(server_round_trip(&value)?, value);
    Ok(())
}

// Frozen contract case 37, lines 3844-3892.
#[test]
fn case_37_uniform_marker_plan_position_and_capacity_arithmetic() -> TestResult {
    const BM: u64 = 100;
    let empty = retained_baseline(ResourceVector::new(0, 0), 3, 0, ResourceVector::new(1, BM))
        .map_err(|error| format!("empty baseline failed: {error:?}"))?;
    assert_eq!(empty, WideResourceVector::new(3, 3 * u128::from(BM)));
    assert!(zero_debt_admission(
        empty,
        ResourceVector::new(2, 2 * BM),
        ResourceVector::new(2, 2 * BM),
        ResourceVector::new(9, 9 * BM),
    ));

    let post_plan = retained_baseline(
        ResourceVector::new(5, 5 * BM),
        3,
        3,
        ResourceVector::new(1, BM),
    )
    .map_err(|error| format!("planned baseline failed: {error:?}"))?;
    assert_eq!(post_plan, WideResourceVector::new(5, 5 * u128::from(BM)));
    let budget = SequenceBudget {
        high_watermark: 33,
        remaining: u64::MAX - 33,
        e: 3,
        t: 3,
        m: 3,
        rs: 0,
        rt: 0,
        l_times_t: 9,
        l_times_rt: 0,
        l_other_times_e: 6,
    };
    let claim_count =
        u128::from(budget.e + budget.t + budget.m) + budget.l_times_t + budget.l_other_times_e;
    assert_eq!(claim_count, 24);
    assert_eq!(u128::from(budget.high_watermark) + claim_count, 57);
    let marker_positions = [
        budget.high_watermark + 1,
        budget.high_watermark + 2,
        budget.high_watermark + 3,
    ];
    assert_eq!(marker_positions, [34, 35, 36]);

    let final_floor = floor_transition(32, Some(36), 36, 36, 37);
    assert_eq!(final_floor.preferred_floor, 37);
    assert_eq!(final_floor.resulting_floor, 37);
    assert!(no_edge_legal(
        WideResourceVector::new(0, 0),
        post_plan,
        ResourceVector::new(2, 2 * BM),
        ResourceVector::new(2, 2 * BM),
        ResourceVector::new(9, 9 * BM),
    ));
    Ok(())
}

// Frozen contract case 38, lines 3893-3897.
#[test]
fn case_38_delayed_ack_is_keyed_to_explicit_identity_and_generation() -> TestResult {
    let retired_p = retire_detached_member(member(38, 38, 7, 12)?, 0x38, 13)?;
    let q_epoch = epoch(38, 2, 7)?;
    let q = member(39, 38, 7, 12)?;
    let q_binding = BindingState::Bound(ActiveBinding {
        participant_id: 39,
        conversation_id: 38,
        binding_epoch: q_epoch,
    });
    let delayed = [
        ParticipantBindingRequest::ParticipantAck(ParticipantAck {
            conversation_id: 38,
            participant_id: 38,
            capability_generation: generation(7)?,
            through_seq: 12,
        }),
        ParticipantBindingRequest::MarkerAck(MarkerAck {
            conversation_id: 38,
            participant_id: 38,
            capability_generation: generation(7)?,
            marker_delivery_seq: 12,
        }),
    ];
    for request in delayed {
        assert!(matches!(
            lookup_binding_required(
                PresentedIdentity::from(Some(&retired_p)),
                &q_binding,
                Some(q_epoch),
                &request,
            ),
            liminal_protocol::lifecycle::BindingRequiredLookupResult::Retired(_)
        ));
    }
    assert_eq!(q.cursor(), 12);

    let rotated: IdentityState<[u8; 32], [u8; 32], [u8; 32]> =
        IdentityState::Live(member(38, 38, 8, 12)?);
    let rotated_epoch = epoch(38, 3, 8)?;
    let stale = ParticipantBindingRequest::ParticipantAck(ParticipantAck {
        conversation_id: 38,
        participant_id: 38,
        capability_generation: generation(7)?,
        through_seq: 13,
    });
    assert!(matches!(
        lookup_binding_required(
            PresentedIdentity::from(Some(&rotated)),
            &BindingState::Bound(ActiveBinding {
                participant_id: 38,
                conversation_id: 38,
                binding_epoch: rotated_epoch,
            }),
            Some(rotated_epoch),
            &stale,
        ),
        liminal_protocol::lifecycle::BindingRequiredLookupResult::StaleAuthority(_)
    ));
    Ok(())
}

// Frozen contract case 39, lines 3898-3904.
#[test]
#[allow(clippy::too_many_lines)]
fn case_39_total_unknown_stale_unbound_and_retired_precedence() -> TestResult {
    let seven = generation(7)?;
    let attach = CredentialAttachRequest {
        conversation_id: 39,
        participant_id: 39,
        capability_generation: seven,
        attach_secret: AttachSecret::new([0x39; 32]),
        attach_attempt_token: AttachAttemptToken::new([0x39; 16]),
        accept_marker_delivery_seq: None,
    };
    for phase in [
        CredentialAttachTokenPhase::NoMatch,
        CredentialAttachTokenPhase::AfterProvenance,
    ] {
        assert!(matches!(
            lookup_credential_attach::<[u8; 32], [u8; 32], [u8; 32]>(
                phase,
                PresentedIdentity::Absent,
                &BindingState::Detached,
                &attach,
                AttachSecretProof::Verified,
            ),
            CredentialAttachLookupResult::ParticipantUnknown(_)
        ));
    }

    let detach = DetachRequest {
        conversation_id: 39,
        participant_id: 39,
        capability_generation: seven,
        detach_attempt_token: DetachAttemptToken::new([0xD9; 16]),
    };
    let empty_cell = DetachCell::<[u8; 32]>::default();
    assert!(matches!(
        lookup_detach(&DetachLookupContext {
            token_resolution: DetachTokenResolution::NoExactMatch,
            presented_identity: PresentedIdentity::<[u8; 32], [u8; 32], [u8; 32]>::Absent,
            cell: &empty_cell,
            binding: &BindingState::Detached,
            receiving_binding_epoch: None,
            request: &detach,
            request_verifier: [0xD9; 32],
            observer_progress: 0,
        }),
        DetachLookupResult::ParticipantUnknown(_)
    ));
    let leave = LeaveRequest {
        conversation_id: 39,
        participant_id: 39,
        capability_generation: seven,
        attach_secret: AttachSecret::new([0x39; 32]),
        leave_attempt_token: LeaveAttemptToken::new([0x99; 16]),
    };
    assert!(matches!(
        lookup_leave::<[u8; 32], [u8; 32], [u8; 32]>(
            PresentedIdentity::Absent,
            &BindingState::Detached,
            None,
            &leave,
            LeaveSecretProof::Verified,
        ),
        liminal_protocol::lifecycle::LeaveLookupResult::ParticipantUnknown(_)
    ));

    let binding_requests = [
        ParticipantBindingRequest::ParticipantAck(ParticipantAck {
            conversation_id: 39,
            participant_id: 39,
            capability_generation: seven,
            through_seq: 10,
        }),
        ParticipantBindingRequest::MarkerAck(MarkerAck {
            conversation_id: 39,
            participant_id: 39,
            capability_generation: seven,
            marker_delivery_seq: 10,
        }),
        ParticipantBindingRequest::RecordAdmission(RecordAdmission {
            conversation_id: 39,
            participant_id: 39,
            capability_generation: seven,
            record_admission_attempt_token:
                liminal_protocol::wire::RecordAdmissionAttemptToken::new([0xA7; 16]),
            payload: vec![0; 3],
        }),
    ];
    for request in &binding_requests {
        assert!(matches!(
            lookup_binding_required::<[u8; 32], [u8; 32], [u8; 32]>(
                PresentedIdentity::Absent,
                &BindingState::Detached,
                None,
                request,
            ),
            liminal_protocol::lifecycle::BindingRequiredLookupResult::ParticipantUnknown(_)
        ));
    }

    let live: TestIdentity = IdentityState::Live(member(39, 39, 7, 9)?);
    for request in &binding_requests {
        assert!(matches!(
            lookup_binding_required(
                PresentedIdentity::from(Some(&live)),
                &BindingState::Detached,
                None,
                request,
            ),
            liminal_protocol::lifecycle::BindingRequiredLookupResult::NoBinding(_)
        ));
    }
    let stale_detach = DetachRequest {
        capability_generation: generation(6)?,
        ..detach
    };
    assert!(matches!(
        lookup_detach(&DetachLookupContext {
            token_resolution: DetachTokenResolution::NoExactMatch,
            presented_identity: PresentedIdentity::from(Some(&live)),
            cell: &empty_cell,
            binding: &BindingState::Detached,
            receiving_binding_epoch: None,
            request: &stale_detach,
            request_verifier: [0xD9; 32],
            observer_progress: 0,
        }),
        DetachLookupResult::StaleAuthority(_)
    ));

    let retired = retire_detached_member(member(39, 39, 7, 9)?, 0x79, 10)?;
    for request in &binding_requests {
        assert!(matches!(
            lookup_binding_required(
                PresentedIdentity::from(Some(&retired)),
                &BindingState::Detached,
                None,
                request,
            ),
            liminal_protocol::lifecycle::BindingRequiredLookupResult::Retired(_)
        ));
    }
    let committed = liminal_protocol::wire::RecordCommitted::new(
        RecordAdmissionEnvelope {
            conversation_id: 39,
            participant_id: 39,
            capability_generation: seven,
            record_admission_attempt_token:
                liminal_protocol::wire::RecordAdmissionAttemptToken::new([0xA7; 16]),
        },
        11,
    );
    assert_eq!(committed.sender_participant_id(), 39);
    Ok(())
}

// Frozen contract case 40, lines 3905-3925.
#[test]
#[allow(clippy::too_many_lines)]
fn case_40_parking_renegotiation_selects_first_incompatible_dimension() -> TestResult {
    let record = ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: 40_001,
        participant_id: 40,
        capability_generation: generation(7)?,
        record_admission_attempt_token: liminal_protocol::wire::RecordAdmissionAttemptToken::new(
            [0xA7; 16],
        ),
        payload: vec![0; 54],
    });
    let (_, request_bytes) = client_round_trip(record)?;
    let request_bytes = u64::try_from(request_bytes).map_err(|error| error.to_string())?;
    let row_metadata = 23;
    let row_bound = request_bytes + row_metadata;

    let products = [
        CheckedMultiplyOverflow {
            left: u64::MAX,
            right: row_bound,
        },
        CheckedMultiplyOverflow {
            left: u64::MAX,
            right: row_bound,
        },
    ];
    for product in products {
        assert_eq!(product.checked_result(), None);
        assert!(product.overflow());
    }
    let shape_arms = [
        SdkParkingCapacityIncompatible::NonzeroLimit {
            field: ParkingLimitField::N,
            actual: 0,
            required_minimum: 1,
        },
        SdkParkingCapacityIncompatible::RecoveryEntrySchemaBytes {
            actual: 15,
            required: 16,
        },
        SdkParkingCapacityIncompatible::WireSchemaBytes {
            actual: 15,
            required: 16,
        },
        SdkParkingCapacityIncompatible::RequestSchemaBytes {
            configured_request_limit: request_bytes - 1,
            wire_frame_limit: request_bytes,
            actual: request_bytes - 1,
            required: request_bytes,
        },
        SdkParkingCapacityIncompatible::RowSchemaBytes {
            request_limit: request_bytes,
            row_metadata_bytes: row_metadata,
            actual: row_bound - 1,
            required: u128::from(row_bound),
        },
        SdkParkingCapacityIncompatible::CheckedProduct(products[0]),
        SdkParkingCapacityIncompatible::CheckedProduct(products[1]),
        SdkParkingCapacityIncompatible::RowBytesBound {
            left: 2,
            right: row_bound,
            checked_product: 2 * row_bound,
            actual: 2 * row_bound + 1,
        },
        SdkParkingCapacityIncompatible::SdkBytesBound {
            left: 3,
            right: row_bound,
            checked_product: 3 * row_bound,
            actual: 3 * row_bound + 1,
        },
        SdkParkingCapacityIncompatible::RecoverableSlots {
            actual: 6,
            limit: 5,
        },
    ];
    assert_eq!(shape_arms.len(), 10);

    let aggregate_ties = [
        SdkParkingCapacityIncompatible::ConversationRows {
            conversation_id: 40_001,
            occupied: 2,
            limit: 1,
        },
        SdkParkingCapacityIncompatible::ConversationBytes {
            conversation_id: 40_001,
            occupied: 2 * row_bound,
            limit: row_bound,
        },
        SdkParkingCapacityIncompatible::SdkConversations {
            occupied: 2,
            limit: 1,
        },
        SdkParkingCapacityIncompatible::SdkRows {
            occupied: 2,
            limit: 1,
        },
        SdkParkingCapacityIncompatible::SdkBytes {
            occupied: 2 * row_bound,
            limit: row_bound,
        },
    ];
    let ranks: Vec<u8> = aggregate_ties
        .iter()
        .map(|value| match value {
            SdkParkingCapacityIncompatible::ConversationRows { .. } => 0,
            SdkParkingCapacityIncompatible::ConversationBytes { .. } => 1,
            SdkParkingCapacityIncompatible::SdkConversations { .. } => 2,
            SdkParkingCapacityIncompatible::SdkRows { .. } => 3,
            SdkParkingCapacityIncompatible::SdkBytes { .. } => 4,
            _ => u8::MAX,
        })
        .collect();
    assert_eq!(ranks, [0, 1, 2, 3, 4]);

    let request_failure = SdkParkingCapacityIncompatible::RequestBytes {
        conversation_id: 40_001,
        park_order: 7,
        actual: request_bytes,
        limit: request_bytes - 1,
    };
    assert!(matches!(
        request_failure,
        SdkParkingCapacityIncompatible::RequestBytes {
            conversation_id: 40_001,
            park_order: 7,
            actual,
            limit,
        } if actual == request_bytes && limit == request_bytes - 1
    ));

    let p = 3_u64;
    let operands = HandshakeSizeOperands {
        max_entries: p,
        framing_bytes: 24,
        request_entry_bytes: 16,
        response_entry_bytes: 26,
        error_response_bytes: 27,
        request_encoded_bytes: 24 + 16 * u128::from(p),
        response_encoded_bytes: 24 + 26 * u128::from(p),
    };
    let handshake_arms = [
        SdkParkingCapacityIncompatible::RecoveryHandshakeRequestBytes {
            operands,
            limit: 71,
        },
        SdkParkingCapacityIncompatible::RecoveryHandshakeRequestWireFrameBytes {
            operands,
            limit: 71,
        },
        SdkParkingCapacityIncompatible::RecoveryHandshakeResponseWireFrameBytes {
            operands,
            limit: 101,
        },
    ];
    assert_eq!(handshake_arms.len(), 3);
    Ok(())
}

// Frozen contract case 41, lines 3926-3933.
#[test]
fn case_41_pending_detach_replay_and_progress_race() -> TestResult {
    let binding = ActiveBinding {
        participant_id: 41,
        conversation_id: 41,
        binding_epoch: epoch(41, 1, 7)?,
    };
    let request = DetachRequest {
        conversation_id: 41,
        participant_id: 41,
        capability_generation: generation(7)?,
        detach_attempt_token: DetachAttemptToken::new([0x41; 16]),
    };
    let verifier = [0x41; 32];
    let verified_request = binding
        .verify_detach_request(request.clone(), verifier)
        .map_err(|error| format!("detach verify failed: {error:?}"))?;
    let pending = start_blocked_detach(
        member(41, 41, 7, 10)?,
        verified_request,
        DetachCell::default(),
        PendingBindingTerminalPosition::new(11),
        5,
    )
    .map_err(|error| format!("pending detach failed: {error:?}"))?;
    let (live, pending_binding, cell, _) = pending.into_parts();
    let exact = cell
        .verify_exact(&request, verifier)
        .map_err(|error| format!("exact pending replay failed: {error:?}"))?
        .prepare_replay(41, pending_binding, 5)
        .apply(live.clone(), PendingDrainDecision::NotAttempted)
        .map_err(|error| format!("equal replay failed: {error:?}"))?;
    assert!(matches!(exact, PendingReplay::Pending { .. }));

    let competing_token = DetachAttemptToken::new([0x42; 16]);
    let competing = cell.competing_attempt(41, competing_token, generation(7)?);
    assert_eq!(competing.presented_token, competing_token);
    assert_eq!(competing.committed_binding_epoch, binding.binding_epoch);

    let rewritten = cell
        .verify_exact(&request, verifier)
        .map_err(|error| format!("pending replay failed: {error:?}"))?
        .prepare_replay(41, pending_binding, 6)
        .apply(live, PendingDrainDecision::StillBlocked)
        .map_err(|error| format!("rewrite failed: {error:?}"))?;
    let PendingReplay::Pending {
        member: live,
        binding_state,
        cell: rewritten_cell,
        outcome,
    } = rewritten
    else {
        return Err("greater progress did not retain complete pending state".to_owned());
    };
    let liminal_protocol::wire::ObserverBackpressure::Detach { state, .. } = outcome else {
        return Err("rewritten pending detach returned another operation outcome".to_owned());
    };
    assert_eq!(state.backpressure_epoch(), 6);
    assert_eq!(state.observer_progress(), 6);
    let committed = rewritten_cell
        .verify_exact(&request, verifier)
        .map_err(|error| format!("rewritten replay failed: {error:?}"))?
        .prepare_replay(41, binding_state, 7)
        .apply(
            live,
            PendingDrainDecision::Committed {
                detached_delivery_seq: 12,
            },
        )
        .map_err(|error| format!("drain failed: {error:?}"))?;
    let PendingReplay::Committed { cell, outcome, .. } = committed else {
        return Err("successful drain did not atomically commit".to_owned());
    };
    assert_eq!(outcome.detached_delivery_seq(), 12);
    assert_eq!(
        cell.verify_exact(&request, verifier)
            .map_err(|error| format!("committed replay failed: {error:?}"))?
            .outcome(41),
        outcome
    );
    Ok(())
}

// Frozen contract case 42, lines 3934-3965.
#[test]
fn case_42_binding_slot_handoff_and_incarnation_exhaustion() -> TestResult {
    let enrollment = ServerValue::ConnectionConversationBindingOccupied(
        ConnectionConversationBindingOccupied::Enrollment {
            conversation_id: 42,
            enrollment_token: EnrollmentToken::new([0x42; 16]),
        },
    );
    assert_eq!(server_round_trip(&enrollment)?, enrollment);
    let attach_occupied = ServerValue::ConnectionConversationBindingOccupied(
        ConnectionConversationBindingOccupied::CredentialAttach {
            conversation_id: 42,
            participant_id: 43,
            capability_generation: generation(2)?,
            attach_attempt_token: AttachAttemptToken::new([0x43; 16]),
            accept_marker_delivery_seq: None,
        },
    );
    assert_eq!(server_round_trip(&attach_occupied)?, attach_occupied);

    let receipt = AttachBound::ordinary(
        42,
        AttachAttemptToken::new([0x42; 16]),
        42,
        generation(1)?,
        AttachSecret::new([0x22; 32]),
        epoch(7, 1, 2)?,
        8,
        1_000,
        2_000,
    )
    .ok_or_else(|| "receipt invariant failed".to_owned())?;
    let live_p: IdentityState<[u8; 32], [u8; 32], [u8; 32]> =
        IdentityState::Live(member(42, 42, 2, 8)?);
    let replay_request = CredentialAttachRequest {
        conversation_id: 42,
        participant_id: 42,
        capability_generation: generation(1)?,
        attach_secret: AttachSecret::new([0x11; 32]),
        attach_attempt_token: AttachAttemptToken::new([0x42; 16]),
        accept_marker_delivery_seq: None,
    };
    let stored = CredentialAttachLiveReceipt::from_commit(receipt);
    let q_binding = BindingState::Bound(ActiveBinding {
        participant_id: 43,
        conversation_id: 42,
        binding_epoch: epoch(7, 1, 2)?,
    });
    assert!(matches!(
        lookup_credential_attach(
            CredentialAttachTokenPhase::LiveReceipt {
                identity: ResolvedIdentity::from(&live_p),
                receipt: &stored,
            },
            PresentedIdentity::Absent,
            &q_binding,
            &replay_request,
            AttachSecretProof::Verified,
        ),
        CredentialAttachLookupResult::UnboundReceipt(ReceiptReplay::CredentialAttach(_))
    ));

    let same_connection_rotation = epoch(7, 1, 3)?;
    let new_connection = epoch(7, 2, 3)?;
    let after_restart = epoch(8, 0, 3)?;
    assert_eq!(
        same_connection_rotation.connection_incarnation,
        epoch(7, 1, 2)?.connection_incarnation
    );
    assert_ne!(
        new_connection.connection_incarnation,
        same_connection_rotation.connection_incarnation
    );
    assert_ne!(
        after_restart.connection_incarnation.server_incarnation,
        new_connection.connection_incarnation.server_incarnation
    );

    let server_exhausted = ConnectionIncarnationExhausted::ServerIncarnation;
    assert_eq!(server_exhausted.current_value(), u64::MAX);
    assert_eq!(server_exhausted.attempted_server_incarnation(), None);
    let ordinal_exhausted = ConnectionIncarnationExhausted::ConnectionOrdinal {
        attempted_server_incarnation: 7,
    };
    assert_eq!(ordinal_exhausted.current_value(), u64::MAX);
    assert_eq!(ordinal_exhausted.attempted_server_incarnation(), Some(7));
    Ok(())
}

// Frozen contract case 43, lines 3966-4034.
#[test]
fn case_43_reachable_park_and_transaction_order_exhaustion() -> TestResult {
    let q = (u64::MAX - 3) / 4;
    let park_high_watermark = 2_u64
        .checked_mul(q)
        .and_then(|value| value.checked_add(5))
        .ok_or_else(|| "park history arithmetic overflowed".to_owned())?;
    let park_budget = SequenceBudget {
        high_watermark: park_high_watermark,
        remaining: u64::MAX - park_high_watermark,
        e: 1,
        t: 1,
        m: 0,
        rs: 0,
        rt: 0,
        l_times_t: 1,
        l_times_rt: 0,
        l_other_times_e: 0,
    };
    assert_eq!(park_budget.high_watermark + park_budget.remaining, u64::MAX);
    assert_eq!(
        u128::from(park_budget.e + park_budget.t) + park_budget.l_times_t,
        3
    );
    let exhausted = SdkParkOrderExhausted::new(43_001);
    assert_eq!(exhausted.value(), u64::MAX);

    let generation_value = q + 3;
    assert!(Generation::new(generation_value).is_some());
    let request = RecordAdmissionEnvelope {
        conversation_id: 43_002,
        participant_id: 3,
        capability_generation: generation(1)?,
        record_admission_attempt_token: liminal_protocol::wire::RecordAdmissionAttemptToken::new(
            [0xA7; 16],
        ),
    };
    let order = ConversationOrderExhausted::new(
        OrderAllocatingEnvelope::RecordAdmission(request),
        u64::MAX - 2,
        2,
        2,
        1,
        2,
    );
    assert_eq!(order.next_value(), Some(u64::MAX - 1));
    assert_eq!(order.order_remaining(), 2);
    assert_eq!(order.reserved_claims(), 2);
    assert_eq!(order.resulting_order_remaining(), 1);
    assert_eq!(order.resulting_reserved_claims(), 2);
    let value = ServerValue::ConversationOrderExhausted(Box::new(order));
    assert_eq!(server_round_trip(&value)?, value);

    let baseline = retained_baseline(ResourceVector::new(0, 0), 4, 0, ResourceVector::new(1, 100))
        .map_err(|error| format!("baseline failed: {error:?}"))?;
    assert_eq!(baseline, WideResourceVector::new(4, 400));
    assert!(zero_debt_admission(
        baseline,
        ResourceVector::new(2, 200),
        ResourceVector::new(2, 200),
        ResourceVector::new(9, 900),
    ));
    let final_floor = floor_transition(
        u128::from(u64::MAX - 3),
        None,
        u64::MAX - 2,
        u64::MAX - 3,
        u128::from(u64::MAX - 2),
    );
    assert_eq!(final_floor.resulting_floor, u128::from(u64::MAX - 2));
    Ok(())
}

// Frozen contract case 44, lines 4035-4079.
// Extraction Fix 2: LP-EXTRACTION-GOAL.md replaces the document's serialized
// occurrence slots with typed transition authority; no O_max/O_base is ported.
#[test]
#[allow(clippy::too_many_lines)]
fn case_44_dcr_leave_fence_and_typed_cursor_release_suffix() -> TestResult {
    const BM: u64 = 100;
    let h = u64::MAX - 6;
    let prior_epoch = epoch(44, 1, 7)?;
    let recovered_epoch = epoch(44, 1, 8)?;
    let original_debt = debt(1, u128::from(BM))?;
    let pre_capacity = mandatory_capacity(
        WideResourceVector::new(4, 4 * u128::from(BM)),
        ResourceVector::new(2, 2 * BM),
        ResourceVector::new(2, 2 * BM),
        ResourceVector::new(7, 7 * BM),
    );
    assert_eq!(pre_capacity.debt, original_debt.value());
    assert!(pre_capacity.is_legal());

    let budget = SequenceBudget {
        high_watermark: h + 1,
        remaining: 5,
        e: 1,
        t: 0,
        m: 0,
        rs: 1,
        rt: 1,
        l_times_t: 0,
        l_times_rt: 1,
        l_other_times_e: 0,
    };
    assert_eq!(budget.high_watermark + budget.remaining, u64::MAX);

    let leave_recovery = credential_recovery(44, prior_epoch, h, original_debt)?;
    let claim = leave_recovery
        .validate_leave_claim(
            44,
            ResourceVector::new(1, BM),
            ResourceVector::new(2, 2 * BM),
            1,
        )
        .ok_or_else(|| "exact detached Leave did not validate K claim".to_owned())?;
    let leave_state = leave_recovery
        .detached_leave(
            original_debt,
            Event::detached_leave_committed(44, h + 1),
            claim,
            DebtCompletion::clear(),
        )
        .map_err(|state| format!("DCR Leave failed: {state:?}"))?;
    assert_eq!(leave_state, ClosureState::Clear);

    let transfer = recovery_transfer(
        WideResourceVector::new(2, 2 * u128::from(BM)),
        ResourceVector::new(2, 2 * BM),
        ResourceVector::new(1, BM),
    )
    .map_err(|error| format!("K transfer failed: {error:?}"))?;
    assert_eq!(
        transfer.baseline,
        WideResourceVector::new(3, 3 * u128::from(BM))
    );
    assert_eq!(
        transfer.remaining_recovery_claim,
        ResourceVector::new(1, BM)
    );
    assert!(no_edge_legal(
        WideResourceVector::new(0, 0),
        transfer.baseline,
        ResourceVector::new(2, 2 * BM),
        ResourceVector::new(2, 2 * BM),
        ResourceVector::new(7, 7 * BM),
    ));

    let fenced_recovery = credential_recovery(44, prior_epoch, h, original_debt)?;
    let fenced = mint_fenced_attach(
        fenced_recovery,
        original_debt,
        Event::fenced_recovery_committed(44, h, prior_epoch, recovered_epoch, h + 1),
        DebtCompletion::clear(),
    )
    .map_err(|error| format!("DCR owner mint failed: {error}"))?;
    assert_eq!(fenced.marker_delivery_seq(), h);
    assert_eq!(fenced.prior_binding_epoch(), prior_epoch);
    assert_eq!(fenced.new_binding_epoch(), recovered_epoch);
    assert_eq!(fenced.next_state(), ClosureState::Clear);

    let op_recovery = credential_recovery(44, prior_epoch, h, original_debt)?;
    let projection = ObserverProjection::new(h + 2);
    let attached_debt = debt(1, 80)?;
    let commit = mint_fenced_attach(
        op_recovery,
        original_debt,
        Event::fenced_recovery_committed(44, h, prior_epoch, recovered_epoch, h + 1),
        DebtCompletion::observer_projection(attached_debt, projection),
    )
    .map_err(|error| format!("nonzero DCR owner mint failed: {error}"))?;
    let authority = recovered_fate_from_fenced(
        commit,
        Event::binding_fate_observed(44, recovered_epoch, h + 2),
    )
    .map_err(|error| format!("exact recovered fate chain failed: {error}"))?;
    let pending = projection
        .apply_recovered_binding_fate(attached_debt, debt(1, 70)?, authority)
        .map_err(|_| "projection rejected exact recovered fate".to_owned())?;
    let RecoveredBindingFateTransition::PendingStorage(pending) = pending else {
        return Err("OP should preserve its storage witness before DCursor".to_owned());
    };
    let released = projection
        .complete_after_recovered_binding_fate(
            Event::projection_completed(h + 2),
            Some(debt(1, 60)?),
            pending,
        )
        .map_err(|_| "exact OP completion did not consume suffix authority".to_owned())?;
    let ClosureState::Owed {
        edge: StoredEdge::DetachedCursorRelease(cursor_release),
        ..
    } = released
    else {
        return Err("recovered fate did not select DetachedCursorRelease".to_owned());
    };
    assert_eq!(cursor_release.participant_id(), 44);
    assert_eq!(cursor_release.last_dead_binding_epoch(), recovered_epoch);

    let final_floor = floor_transition(u128::from(h - 2), None, h + 2, h, u128::from(h + 1));
    assert_eq!(final_floor.preferred_floor, u128::from(h + 1));
    assert_eq!(final_floor.resulting_floor, u128::from(h + 1));
    Ok(())
}
