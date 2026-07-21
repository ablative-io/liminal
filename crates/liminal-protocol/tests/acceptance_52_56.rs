#![allow(
    clippy::expect_used,
    clippy::items_after_statements,
    clippy::many_single_char_names,
    clippy::missing_const_for_fn,
    clippy::panic,
    clippy::similar_names,
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::type_complexity
)]

mod support;

use std::{boxed::Box, collections::BTreeMap, vec, vec::Vec};

use liminal_protocol::{
    algebra::{
        ResourceVector, WideResourceVector, floor_transition, mandatory_capacity, no_edge_legal,
        recovery_transfer, retained_baseline, zero_debt_admission,
    },
    lifecycle::{
        ActiveBinding, AttachCommitParameters, AttachSecretProof, AttachedRecordPosition,
        BindingState, BindingTerminalDisposition, BoundParticipantCursor, ClosureDebt,
        ClosureState, CommittedBindingTerminalPosition, CredentialAttachLiveReceipt,
        CredentialAttachLookupResult, CredentialAttachProvenance, CredentialAttachTokenPhase,
        CursorFateSuccessor, CursorProgressFact, CursorProgressKey, DebtCompletion, DetachCell,
        DetachLookupContext, DetachLookupResult, DetachTokenResolution, DetachedAttachRefusal,
        Event, IdentityState, LeaveCommitParameters, LeaveFingerprint, LeaveLookupResult,
        LeaveOnlyEdge, LeaveSecretProof, LiveMember, LiveMemberRestore, MarkerDelivery,
        NonzeroDebtCursorEpisode, ObserverProjection, PendingBindingTerminalPosition,
        PendingFinalization, PendingLeaveCommitParameters, PhysicalCompaction,
        PrepareLeaveAuthorityError, PresentedIdentity, ResolvedIdentity, StoredEdge, commit_attach,
        commit_detach, commit_leave, commit_pending_leave, lookup_credential_attach, lookup_detach,
        lookup_leave,
    },
    outcome::{
        HandshakeSizeOperands, ParticipantRecoveryHandshakeTooLarge, RecoveryHandshakeDimension,
        SdkParkingCapacityIncompatible,
    },
    wire::{
        AttachAttemptToken, AttachBound, AttachEnvelope, AttachSecret, AttemptConflict,
        AttemptTokenBodyConflict, BindingEpoch, BindingStateView, ClientRequest,
        ClosureCheckedEnvelope, ClosureRefusalReason, ClosureSnapshot,
        CommonStaleAuthorityEnvelope, ConnectionIncarnation, ConversationSequenceExhausted,
        CredentialAttachRequest, DetachAttemptToken, DetachRequest, DetachStaleAuthority,
        FRAME_MAX, Generation, LeaveAttemptToken, LeaveCommitted, LeaveEnvelope, LeaveRequest,
        LeaveStaleAuthority, MarkerClosureCapacityExceeded, ParticipantAck, ParticipantFrame,
        ReceiptExpired, ReceiptExpiryReason, ReceiptReplay, ReceiverDirection, RepaymentEdge,
        Retired, SequenceAllocatingEnvelope, SequenceBudget, ServerValue, StaleAuthority,
        StaleOrUnknownReceipt, decode, encode, encoded_len,
    },
};
use support::{
    intervening_pending_leave_refusal, marker_delivery, mint_fenced_attach_with_owner,
    pending_leave_authority, recovered_fate_from_fenced, settled_leave_authority,
};

const BM: u64 = 16;
const P0: u64 = 0;
const P1: u64 = 1;

type Fingerprint = [u8; 32];

fn generation(value: u64) -> Generation {
    Generation::new(value).expect("acceptance generations are nonzero")
}

fn epoch(server: u64, ordinal: u64, generation_value: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(server, ordinal),
        generation(generation_value),
    )
}

fn secret(byte: u8) -> AttachSecret {
    AttachSecret::new([byte; 32])
}

fn attach_token(byte: u8) -> AttachAttemptToken {
    AttachAttemptToken::new([byte; 16])
}

fn detach_token(byte: u8) -> DetachAttemptToken {
    DetachAttemptToken::new([byte; 16])
}

fn leave_token(byte: u8) -> LeaveAttemptToken {
    LeaveAttemptToken::new([byte; 16])
}

fn uniform(units: u64) -> ResourceVector {
    ResourceVector::new(
        units,
        units
            .checked_mul(BM)
            .expect("uniform fixture bytes fit u64"),
    )
}

fn wide_uniform(units: u128) -> WideResourceVector {
    WideResourceVector::new(units, units * u128::from(BM))
}

fn closure_debt(units: u128) -> ClosureDebt {
    ClosureDebt::new(wide_uniform(units)).expect("fixture debt is nonzero")
}

fn member(
    conversation_id: u64,
    participant_id: u64,
    generation_value: u64,
    cursor: u64,
) -> LiveMember<Fingerprint> {
    LiveMember::restore(LiveMemberRestore {
        participant_id,
        conversation_id,
        generation: generation(generation_value),
        attach_secret: secret(
            u8::try_from(generation_value).expect("fixture generation fits a secret byte"),
        ),
        cursor,
        enrollment_fingerprint: liminal_protocol::lifecycle::EnrollmentFingerprint::new(
            [u8::try_from(participant_id).expect("fixture participant fits a fingerprint byte");
                32],
        ),
        latest_terminal: None,
    })
    .expect("fixture member history is valid")
}

fn attach_envelope(request: &CredentialAttachRequest) -> AttachEnvelope {
    AttachEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        attach_attempt_token: request.attach_attempt_token,
        accept_marker_delivery_seq: request.accept_marker_delivery_seq,
    }
}

fn leave_envelope(request: &LeaveRequest) -> LeaveEnvelope {
    LeaveEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        leave_attempt_token: request.leave_attempt_token,
    }
}

fn sequence_budget(
    high_watermark: u64,
    e: u64,
    t: u64,
    m: u64,
    rs: u64,
    rt: u64,
    l_times_t: u128,
    l_times_rt: u128,
    l_other_times_e: u128,
) -> SequenceBudget {
    SequenceBudget {
        high_watermark,
        remaining: u64::MAX - high_watermark,
        e,
        t,
        m,
        rs,
        rt,
        l_times_t,
        l_times_rt,
        l_other_times_e,
    }
}

fn encoded(frame: &ParticipantFrame) -> Vec<u8> {
    let mut bytes = vec![0; encoded_len(frame).expect("typed frame has a wire size")];
    let written = encode(frame, &mut bytes).expect("typed frame encodes");
    assert_eq!(written, bytes.len());
    bytes
}

fn assert_client_round_trip(request: ClientRequest) -> Vec<u8> {
    let frame = ParticipantFrame::ClientRequest(request);
    let bytes = encoded(&frame);
    assert_eq!(
        decode(&bytes, ReceiverDirection::Server).expect("canonical request decodes"),
        frame
    );
    bytes
}

fn assert_server_round_trip(value: ServerValue) -> Vec<u8> {
    let frame = ParticipantFrame::ServerValue(value);
    let bytes = encoded(&frame);
    assert_eq!(
        decode(&bytes, ReceiverDirection::Client).expect("canonical server value decodes"),
        frame
    );
    bytes
}

fn handshake_outcome(
    operands: HandshakeSizeOperands,
    request_limit: u64,
    wire_frame_limit: u64,
) -> ParticipantRecoveryHandshakeTooLarge {
    let dimension = if operands.request_encoded_bytes > u128::from(request_limit) {
        RecoveryHandshakeDimension::RequestBytes
    } else if operands.request_encoded_bytes > u128::from(wire_frame_limit) {
        RecoveryHandshakeDimension::RequestWireFrameBytes
    } else {
        assert!(operands.response_encoded_bytes > u128::from(wire_frame_limit));
        RecoveryHandshakeDimension::ResponseWireFrameBytes
    };
    ParticipantRecoveryHandshakeTooLarge {
        max_entries: operands.max_entries,
        framing_bytes: operands.framing_bytes,
        request_entry_bytes: operands.request_entry_bytes,
        response_entry_bytes: operands.response_entry_bytes,
        error_response_bytes: operands.error_response_bytes,
        request_encoded_bytes: operands.request_encoded_bytes,
        response_encoded_bytes: operands.response_encoded_bytes,
        request_limit,
        wire_frame_limit,
        dimension,
    }
}

#[test]
fn acceptance_case_52_one_shot_recovery_phase_latch_and_handshake_limits() {
    // Frozen PARTICIPANT-CONTRACT.md lines 4617-4698.
    // PF is deliberately exercised at its generated v1 ceiling: the frozen
    // case is symbolic in PF and promises these exact upper-bound arithmetic
    // values, while every selector below is independent of PF's concrete value.
    const PF: u64 = 1_048_576;
    const PR: u64 = 97;
    const MR: u64 = 23;
    let p52 = PF.max(PR) + 1;
    let a52 = 24_u64 + 16 * p52;
    let z52 = 24_u64 + 26 * p52;
    assert_eq!(a52, 16_777_256);
    assert_eq!(z52, 27_263_026);
    assert!(z52 < FRAME_MAX);
    assert!(4_u64.checked_mul(a52 + MR).is_some());

    let operands = HandshakeSizeOperands {
        max_entries: p52,
        framing_bytes: 24,
        request_entry_bytes: 16,
        response_entry_bytes: 26,
        error_response_bytes: 27,
        request_encoded_bytes: u128::from(a52),
        response_encoded_bytes: u128::from(z52),
    };
    let proposals = [
        (a52 - 1, z52),
        (a52, a52 - 1),
        (a52, z52 - 1),
        (a52 - 1, a52 - 1),
    ];
    let expected_dimensions = [
        RecoveryHandshakeDimension::RequestBytes,
        RecoveryHandshakeDimension::RequestWireFrameBytes,
        RecoveryHandshakeDimension::ResponseWireFrameBytes,
        RecoveryHandshakeDimension::RequestBytes,
    ];
    let initial: Vec<_> = proposals
        .into_iter()
        .map(|(r, wf)| handshake_outcome(operands, r, wf))
        .collect();
    assert_eq!(
        initial
            .iter()
            .map(|outcome| outcome.dimension)
            .collect::<Vec<_>>(),
        expected_dimensions
    );
    for (outcome, (request_limit, wire_frame_limit)) in initial.iter().zip(proposals) {
        assert_eq!(outcome.max_entries, p52);
        assert_eq!(outcome.framing_bytes, 24);
        assert_eq!(outcome.request_entry_bytes, 16);
        assert_eq!(outcome.response_entry_bytes, 26);
        assert_eq!(outcome.error_response_bytes, 27);
        assert_eq!(outcome.request_encoded_bytes, u128::from(a52));
        assert_eq!(outcome.response_encoded_bytes, u128::from(z52));
        assert_eq!(outcome.request_limit, request_limit);
        assert_eq!(outcome.wire_frame_limit, wire_frame_limit);
    }

    let parked = [
        SdkParkingCapacityIncompatible::RecoveryHandshakeRequestBytes {
            operands,
            limit: a52 - 1,
        },
        SdkParkingCapacityIncompatible::RecoveryHandshakeRequestWireFrameBytes {
            operands,
            limit: a52 - 1,
        },
        SdkParkingCapacityIncompatible::RecoveryHandshakeResponseWireFrameBytes {
            operands,
            limit: z52 - 1,
        },
        SdkParkingCapacityIncompatible::RecoveryHandshakeRequestBytes {
            operands,
            limit: a52 - 1,
        },
    ];
    assert_eq!(parked.len(), expected_dimensions.len());

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct ParkedRow {
        conversation_id: u64,
        park_order: u64,
        request_bytes: u64,
        charged_bytes: u64,
        interested: bool,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct RecoveryDomain {
        row: Option<ParkedRow>,
        request_bytes_written: u64,
        response_bytes_written: u64,
        configuration_revision: u64,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum LatchedPhase {
        Initial,
        Parked,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum RacingAction {
        Admission,
        PromoteAndAuthorizeWrite,
        ResponseDeletion,
        ExpiryDeletion,
        AuthorityInvalidation,
    }

    impl RecoveryDomain {
        fn latch(&self) -> (LatchedPhase, Option<ParkedRow>) {
            let snapshot = self.row.clone();
            let phase = if snapshot.is_some() {
                LatchedPhase::Parked
            } else {
                LatchedPhase::Initial
            };
            (phase, snapshot)
        }

        fn reject(&mut self, phase: LatchedPhase, snapshot: Option<&ParkedRow>) {
            assert_eq!(phase == LatchedPhase::Parked, snapshot.is_some());
            self.request_bytes_written = 0;
            self.response_bytes_written = 0;
        }

        fn before_action(action: RacingAction, row: &ParkedRow) -> Self {
            let row = match action {
                RacingAction::Admission => None,
                RacingAction::PromoteAndAuthorizeWrite => {
                    let mut reserved = row.clone();
                    reserved.interested = false;
                    Some(reserved)
                }
                RacingAction::ResponseDeletion
                | RacingAction::ExpiryDeletion
                | RacingAction::AuthorityInvalidation => Some(row.clone()),
            };
            Self {
                row,
                request_bytes_written: 0,
                response_bytes_written: 0,
                configuration_revision: 1,
            }
        }

        fn apply(&mut self, action: RacingAction, row: &ParkedRow) {
            match action {
                RacingAction::Admission => self.row = Some(row.clone()),
                RacingAction::PromoteAndAuthorizeWrite => {
                    self.row
                        .as_mut()
                        .expect("reserved row exists before promotion")
                        .interested = true;
                }
                RacingAction::ResponseDeletion
                | RacingAction::ExpiryDeletion
                | RacingAction::AuthorityInvalidation => self.row = None,
            }
        }
    }

    let row = ParkedRow {
        conversation_id: 52,
        park_order: 7,
        request_bytes: PR,
        charged_bytes: PR + MR,
        interested: true,
    };
    let first_use = CredentialAttachRequest {
        conversation_id: 52,
        participant_id: 52,
        capability_generation: generation(7),
        attach_secret: secret(7),
        attach_attempt_token: attach_token(0x52),
        accept_marker_delivery_seq: Some(40),
    };
    assert_eq!(
        assert_client_round_trip(ClientRequest::CredentialAttach(first_use)).len(),
        usize::try_from(PR).expect("PR fits the platform size domain"),
    );
    assert!(row.request_bytes < a52);
    assert!(row.charged_bytes <= a52 + MR);

    // Exercise every SDK-wide state change from lines 4687-4698 in both
    // linearization orders against the phase latch.
    for action in [
        RacingAction::Admission,
        RacingAction::PromoteAndAuthorizeWrite,
        RacingAction::ResponseDeletion,
        RacingAction::ExpiryDeletion,
        RacingAction::AuthorityInvalidation,
    ] {
        let mut action_first = RecoveryDomain::before_action(action, &row);
        action_first.apply(action, &row);
        let expected_after_action = action_first.clone();
        let (phase, snapshot) = action_first.latch();
        action_first.reject(phase, snapshot.as_ref());
        assert_eq!(
            phase == LatchedPhase::Parked,
            expected_after_action.row.is_some()
        );
        assert_eq!(action_first, expected_after_action);

        let mut validation_first = RecoveryDomain::before_action(action, &row);
        let expected_latched_phase = if validation_first.row.is_some() {
            LatchedPhase::Parked
        } else {
            LatchedPhase::Initial
        };
        let (phase, snapshot) = validation_first.latch();
        validation_first.apply(action, &row);
        let expected_after_late_action = validation_first.clone();
        validation_first.reject(phase, snapshot.as_ref());
        assert_eq!(phase, expected_latched_phase);
        assert_eq!(validation_first, expected_after_late_action);
        assert_eq!(validation_first.configuration_revision, 1);
        assert_eq!(validation_first.request_bytes_written, 0);
        assert_eq!(validation_first.response_bytes_written, 0);
    }

    // P=1 is error-list dominant and therefore cannot be the later response
    // size trigger; p52 reaches the success-list product arm.
    let error_dominant_response =
        operands.framing_bytes + u128::from(operands.error_response_bytes);
    assert_eq!(error_dominant_response, 51);
    const { assert!(PF >= 51) };
    assert!(26_u128 * u128::from(p52) > 27);
}

#[test]
fn acceptance_case_53_verifier_precedence_receipt_provenance_and_terminalized_detach() {
    // Frozen PARTICIPANT-CONTRACT.md lines 4699-4765. The terminalized replay
    // follows docs/design/LP-EXTRACTION-GOAL.md Fix 1: only the real fourth
    // detach-cell variant can construct TerminalizedDetachCell with old e53.
    const C53: u64 = 53;
    const P53: u64 = 53;
    let e53 = epoch(53, 7, 7);
    let e53next = epoch(53, 8, 8);
    let u53 = attach_token(0x53);
    let canonical = CredentialAttachRequest {
        conversation_id: C53,
        participant_id: P53,
        capability_generation: generation(7),
        attach_secret: secret(7),
        attach_attempt_token: u53,
        accept_marker_delivery_seq: Some(40),
    };
    assert_client_round_trip(ClientRequest::CredentialAttach(canonical.clone()));

    let receipt_member = member(C53, P53, 8, 40);
    let committed = AttachBound::fenced(
        C53,
        u53,
        P53,
        generation(7),
        secret(8),
        e53next,
        40,
        1_000,
        2_000,
    )
    .expect("canonical receipt generation advances exactly once");
    let receipt = CredentialAttachLiveReceipt::from_commit(committed.clone());
    let bound = BindingState::Bound(ActiveBinding {
        participant_id: P53,
        conversation_id: C53,
        binding_epoch: e53next,
    });
    type Phase<'a> = CredentialAttachTokenPhase<'a, Fingerprint, Fingerprint, Fingerprint>;
    type Presented<'a> = PresentedIdentity<'a, Fingerprint, Fingerprint, Fingerprint>;
    let phase = Phase::LiveReceipt {
        identity: ResolvedIdentity::Live(&receipt_member),
        receipt: &receipt,
    };
    let presented = Presented::Live(&receipt_member);
    assert_eq!(
        lookup_credential_attach(
            phase,
            presented,
            &bound,
            &canonical,
            AttachSecretProof::Verified,
        ),
        CredentialAttachLookupResult::Bound(ReceiptReplay::CredentialAttach(committed.clone()))
    );
    assert_eq!(
        lookup_credential_attach(
            phase,
            presented,
            &BindingState::Detached,
            &canonical,
            AttachSecretProof::Verified,
        ),
        CredentialAttachLookupResult::UnboundReceipt(ReceiptReplay::CredentialAttach(committed))
    );

    let expected_stale = StaleAuthority::Live {
        request: CommonStaleAuthorityEnvelope::CredentialAttach(attach_envelope(&canonical)),
        current_generation: generation(8),
    };
    assert_eq!(
        lookup_credential_attach(
            phase,
            presented,
            &bound,
            &canonical,
            AttachSecretProof::Mismatch,
        ),
        CredentialAttachLookupResult::StaleAuthority(expected_stale)
    );

    let generation_changed = CredentialAttachRequest {
        capability_generation: generation(8),
        ..canonical
    };
    let marker_changed = CredentialAttachRequest {
        accept_marker_delivery_seq: Some(41),
        ..canonical
    };
    let both_changed = CredentialAttachRequest {
        capability_generation: generation(8),
        accept_marker_delivery_seq: Some(41),
        ..canonical
    };
    for (request, conflict) in [
        (&generation_changed, AttemptConflict::Generation),
        (&marker_changed, AttemptConflict::MarkerDeliverySequence),
        (&both_changed, AttemptConflict::Generation),
    ] {
        let expected = AttemptTokenBodyConflict::CredentialAttach {
            token: u53,
            conversation_id: C53,
            presented_participant_id: P53,
            presented_generation: request.capability_generation,
            presented_marker_delivery_seq: request.accept_marker_delivery_seq,
            conflict,
        };
        let result = lookup_credential_attach(
            phase,
            presented,
            &bound,
            request,
            AttachSecretProof::Verified,
        );
        assert_eq!(
            result,
            CredentialAttachLookupResult::AttemptTokenBodyConflict(expected.clone())
        );
        assert_server_round_trip(ServerValue::AttemptTokenBodyConflict(expected));
        assert_eq!(
            lookup_credential_attach(
                phase,
                presented,
                &bound,
                request,
                AttachSecretProof::Mismatch,
            ),
            CredentialAttachLookupResult::StaleAuthority(StaleAuthority::Live {
                request: CommonStaleAuthorityEnvelope::CredentialAttach(attach_envelope(request)),
                current_generation: generation(8),
            })
        );
    }

    let altered = CredentialAttachRequest {
        capability_generation: generation(9),
        attach_secret: secret(9),
        accept_marker_delivery_seq: None,
        ..canonical
    };
    let provenance = CredentialAttachProvenance::new(generation(8), ReceiptExpiryReason::Deadline);
    let expired = ReceiptExpired::CredentialAttach {
        conversation_id: C53,
        token: u53,
        participant_id: P53,
        presented_generation: generation(9),
        presented_marker_delivery_seq: None,
        result_generation: generation(8),
        current_generation: generation(8),
        reason: ReceiptExpiryReason::Deadline,
    };
    assert_eq!(
        lookup_credential_attach(
            Phase::Provenance {
                identity: ResolvedIdentity::Live(&receipt_member),
                provenance,
            },
            presented,
            &bound,
            &altered,
            AttachSecretProof::Mismatch,
        ),
        CredentialAttachLookupResult::ReceiptExpired(expired.clone())
    );
    assert_server_round_trip(ServerValue::ReceiptExpired(expired));
    let after = StaleOrUnknownReceipt {
        conversation_id: C53,
        token: u53,
        participant_id: P53,
        presented_generation: generation(9),
        presented_marker_delivery_seq: None,
        current_generation: generation(8),
    };
    assert_eq!(
        lookup_credential_attach(
            Phase::AfterProvenance,
            presented,
            &bound,
            &altered,
            AttachSecretProof::Mismatch,
        ),
        CredentialAttachLookupResult::StaleOrUnknownReceipt(after.clone())
    );
    assert_server_round_trip(ServerValue::StaleOrUnknownReceipt(after));
    for missed_key in [
        CredentialAttachRequest {
            conversation_id: C53 + 1,
            ..altered
        },
        CredentialAttachRequest {
            participant_id: P53 + 1,
            ..altered
        },
    ] {
        assert!(matches!(
            lookup_credential_attach(
                Phase::NoMatch,
                presented,
                &bound,
                &missed_key,
                AttachSecretProof::Mismatch,
            ),
            CredentialAttachLookupResult::ParticipantUnknown(_)
        ));
    }
    let token_miss = CredentialAttachRequest {
        attach_attempt_token: attach_token(0x59),
        ..canonical
    };
    assert_eq!(
        lookup_credential_attach(
            Phase::NoMatch,
            presented,
            &bound,
            &token_miss,
            AttachSecretProof::Verified,
        ),
        CredentialAttachLookupResult::StaleAuthority(StaleAuthority::Live {
            request: CommonStaleAuthorityEnvelope::CredentialAttach(attach_envelope(&token_miss)),
            current_generation: generation(8),
        })
    );

    let live7 = member(C53, P53, 7, 0);
    let live_binding = BindingState::Bound(ActiveBinding {
        participant_id: P53,
        conversation_id: C53,
        binding_epoch: e53,
    });
    let ll53 = LeaveRequest {
        conversation_id: C53,
        participant_id: P53,
        capability_generation: generation(7),
        attach_secret: secret(0xEE),
        leave_attempt_token: leave_token(0x52),
    };
    let ll53_generation8 = LeaveRequest {
        capability_generation: generation(8),
        attach_secret: secret(7),
        ..ll53
    };
    for (request, proof) in [
        (&ll53, LeaveSecretProof::Mismatch),
        (&ll53_generation8, LeaveSecretProof::Verified),
    ] {
        let stale = LeaveStaleAuthority::Live {
            conversation_id: C53,
            participant_id: P53,
            presented_generation: request.capability_generation,
            leave_attempt_token: request.leave_attempt_token,
            current_generation: generation(7),
        };
        assert_eq!(
            lookup_leave(
                Presented::Live(&live7),
                &live_binding,
                Some(e53),
                request,
                proof,
            ),
            LeaveLookupResult::StaleAuthority(stale.clone())
        );
        let bytes =
            assert_server_round_trip(ServerValue::StaleAuthority(StaleAuthority::Leave(stale)));
        assert_eq!(&bytes[16..18], &0x0005_u16.to_be_bytes());
        assert_eq!(&bytes[18..20], &0x0001_u16.to_be_bytes());
        assert!(
            !bytes
                .windows(32)
                .any(|window| window == request.attach_secret.as_bytes())
        );
    }

    let d53 = DetachRequest {
        conversation_id: C53,
        participant_id: P53,
        capability_generation: generation(7),
        detach_attempt_token: detach_token(0x53),
    };
    let verifier: [u8; 32] = [0xD5; 32];
    let verified = ActiveBinding {
        participant_id: P53,
        conversation_id: C53,
        binding_epoch: e53,
    }
    .verify_detach_request(d53.clone(), verifier)
    .expect("D53 exactly names e53");
    let detached = commit_detach(
        live7,
        verified,
        DetachCell::default(),
        CommittedBindingTerminalPosition::new(10, 40),
    )
    .expect("D53 commits its terminal");
    let (detached_member, _, detached_binding, committed_cell, committed_outcome) =
        detached.into_parts();
    let committed_enum = DetachCell::Committed(committed_cell);
    let committed_lookup = lookup_detach(&DetachLookupContext {
        token_resolution: DetachTokenResolution::Exact(ResolvedIdentity::<
            Fingerprint,
            Fingerprint,
            Fingerprint,
        >::Live(&detached_member)),
        presented_identity: Presented::Live(&detached_member),
        cell: &committed_enum,
        binding: &detached_binding,
        receiving_binding_epoch: None,
        request: &d53,
        request_verifier: verifier,
        observer_progress: 40,
    });
    assert_eq!(
        committed_lookup,
        DetachLookupResult::DetachCommitted(committed_outcome.clone())
    );
    assert_server_round_trip(ServerValue::DetachCommitted(committed_outcome));

    let stale_d53 = DetachRequest {
        capability_generation: generation(8),
        ..d53.clone()
    };
    assert_eq!(
        lookup_detach(&DetachLookupContext {
            token_resolution: DetachTokenResolution::Exact(ResolvedIdentity::<
                Fingerprint,
                Fingerprint,
                Fingerprint,
            >::Live(&detached_member)),
            presented_identity: Presented::Live(&detached_member),
            cell: &committed_enum,
            binding: &detached_binding,
            receiving_binding_epoch: None,
            request: &stale_d53,
            request_verifier: verifier,
            observer_progress: 40,
        }),
        DetachLookupResult::StaleAuthority(DetachStaleAuthority::Live {
            conversation_id: C53,
            participant_id: P53,
            capability_generation: generation(8),
            detach_attempt_token: d53.detach_attempt_token,
            current_generation: generation(7),
        })
    );

    let attach = CredentialAttachRequest {
        conversation_id: C53,
        participant_id: P53,
        capability_generation: generation(7),
        attach_secret: secret(7),
        attach_attempt_token: attach_token(0x54),
        accept_marker_delivery_seq: None,
    };
    let verified_attach = detached_member
        .verify_detached_attach(
            detached_binding,
            ClosureState::Clear
                .ordinary_detached_attach_admission()
                .expect("committed detach leaves clear ordinary-attach closure"),
            attach,
            AttachSecretProof::Verified,
            AttachCommitParameters {
                binding: ActiveBinding {
                    participant_id: P53,
                    conversation_id: C53,
                    binding_epoch: e53next,
                },
                attach_secret: secret(8),
                attached_position: AttachedRecordPosition::new(11, 41),
                receipt_expires_at: 1_000,
                provenance_expires_at: 2_000,
            },
        )
        .expect("ordinary attach follows the committed detach");
    let attached = commit_attach(verified_attach, committed_enum)
        .expect("successful attach terminalizes D53 rather than clearing it");
    let terminalized = attached.detach_cell;
    let terminalized_member = attached.member;
    let bound_terminalized = lookup_detach(&DetachLookupContext {
        token_resolution: DetachTokenResolution::Exact(ResolvedIdentity::<
            Fingerprint,
            Fingerprint,
            Fingerprint,
        >::Live(&terminalized_member)),
        presented_identity: Presented::Live(&terminalized_member),
        cell: &terminalized,
        binding: &attached.binding_state,
        receiving_binding_epoch: None,
        request: &d53,
        request_verifier: verifier,
        observer_progress: 40,
    });
    let DetachLookupResult::StaleAuthority(DetachStaleAuthority::TerminalizedDetachCell(
        terminalized_body,
    )) = bound_terminalized
    else {
        panic!("exact old D53 must resolve through the Terminalized variant")
    };
    assert_eq!(terminalized_body.committed_binding_epoch(), e53);
    assert_eq!(terminalized_body.current_generation(), generation(8));
    assert_eq!(
        terminalized_body.binding_state(),
        BindingStateView::Bound {
            current_binding_epoch: e53next,
        }
    );
    let bytes = assert_server_round_trip(ServerValue::StaleAuthority(StaleAuthority::Detach(
        DetachStaleAuthority::TerminalizedDetachCell(terminalized_body),
    )));
    assert_eq!(&bytes[16..18], &0x0003_u16.to_be_bytes());
    assert_eq!(&bytes[18..20], &0x0002_u16.to_be_bytes());
    let detached_binding = BindingState::Detached;
    let detached_terminalized = lookup_detach(&DetachLookupContext {
        token_resolution: DetachTokenResolution::Exact(ResolvedIdentity::<
            Fingerprint,
            Fingerprint,
            Fingerprint,
        >::Live(&terminalized_member)),
        presented_identity: Presented::Live(&terminalized_member),
        cell: &terminalized,
        binding: &detached_binding,
        receiving_binding_epoch: None,
        request: &d53,
        request_verifier: verifier,
        observer_progress: 40,
    });
    let DetachLookupResult::StaleAuthority(DetachStaleAuthority::TerminalizedDetachCell(
        detached_body,
    )) = detached_terminalized
    else {
        panic!("independent empty binding slot must retain D53 terminalized authority")
    };
    assert_eq!(detached_body.binding_state(), BindingStateView::Detached);

    let live8 = member(C53, P53, 8, 40);
    let l53 = LeaveRequest {
        conversation_id: C53,
        participant_id: P53,
        capability_generation: generation(8),
        attach_secret: secret(8),
        leave_attempt_token: leave_token(0x53),
    };
    let verified_leave = live8
        .verify_leave_request(
            &l53,
            AttachSecretProof::Verified,
            [0x53; 32],
            LeaveFingerprint::new([0x54; 32]),
        )
        .expect("L53 canonical authority is valid");
    let prepared_leave = settled_leave_authority(&live8, BindingState::Detached, 42, 42)
        .expect("L53 consumes its exact X frontier handle");
    let (identity, _frontiers) = commit_leave(
        live8,
        BindingState::Detached,
        DetachCell::<[u8; 32]>::default(),
        verified_leave,
        prepared_leave,
        LeaveCommitParameters {
            left_delivery_seq: 42,
        },
    )
    .expect("L53 commits its permanent tombstone")
    .into_parts();
    let IdentityState::Retired(tombstone) = identity else {
        panic!("Leave must retire P53")
    };
    let tombstone_before = tombstone.clone();
    let retired_presented = PresentedIdentity::Retired(&tombstone);
    assert_eq!(
        lookup_leave(
            retired_presented,
            &BindingState::Detached,
            None,
            &l53,
            LeaveSecretProof::Verified,
        ),
        LeaveLookupResult::LeaveCommitted(tombstone.committed_result().clone())
    );
    assert_server_round_trip(ServerValue::LeaveCommitted(
        tombstone.committed_result().clone(),
    ));
    let l53_generation9 = LeaveRequest {
        capability_generation: generation(9),
        ..l53
    };
    let conflict = AttemptTokenBodyConflict::Leave {
        token: l53.leave_attempt_token,
        conversation_id: C53,
        presented_participant_id: P53,
        presented_generation: generation(9),
    };
    assert_eq!(
        lookup_leave(
            retired_presented,
            &BindingState::Detached,
            None,
            &l53_generation9,
            LeaveSecretProof::Verified,
        ),
        LeaveLookupResult::AttemptTokenBodyConflict(conflict.clone())
    );
    assert_server_round_trip(ServerValue::AttemptTokenBodyConflict(conflict));
    let committed_stale = LeaveStaleAuthority::CommittedLeaveTombstone {
        conversation_id: C53,
        participant_id: P53,
        presented_generation: generation(9),
        leave_attempt_token: l53.leave_attempt_token,
        retired_generation: generation(8),
    };
    assert_eq!(
        lookup_leave(
            retired_presented,
            &BindingState::Detached,
            None,
            &l53_generation9,
            LeaveSecretProof::Mismatch,
        ),
        LeaveLookupResult::StaleAuthority(committed_stale.clone())
    );
    let bytes = assert_server_round_trip(ServerValue::StaleAuthority(StaleAuthority::Leave(
        committed_stale,
    )));
    assert_eq!(&bytes[16..18], &0x0005_u16.to_be_bytes());
    assert_eq!(&bytes[18..20], &0x0002_u16.to_be_bytes());
    let other_token = LeaveRequest {
        leave_attempt_token: leave_token(0x55),
        ..l53
    };
    assert!(matches!(
        lookup_leave(
            retired_presented,
            &BindingState::Detached,
            None,
            &other_token,
            LeaveSecretProof::Mismatch,
        ),
        LeaveLookupResult::Retired(Retired::Participant { .. })
    ));
    assert_eq!(tombstone, tombstone_before);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct OrderedRecord {
    sequence: u64,
    kind: RecordKind54,
    participant: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecordKind54 {
    Attached,
    Terminal,
    Marker,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SequenceClaimKind54 {
    Exit(u64),
    Terminal(u64),
    Marker(u64),
    LiveTimesTerminal {
        live_participant: u64,
        terminal_participant: u64,
    },
    OtherTimesExit {
        other_participant: u64,
        exiting_participant: u64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SequenceClaim54 {
    sequence: u64,
    kind: SequenceClaimKind54,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ShutdownSnapshot54 {
    high: u64,
    floor: u64,
    observer: u64,
    records: Vec<OrderedRecord>,
    marker_candidates: Vec<OrderedRecord>,
    sequence_claims: Vec<SequenceClaim54>,
    edge: StoredEdge,
}

impl ShutdownSnapshot54 {
    fn atomic_shutdown(&self) -> Self {
        assert_eq!(self.high, 2);
        let mut post = self.clone();
        post.high = 4;
        post.floor = 2;
        post.records.extend([
            OrderedRecord {
                sequence: 3,
                kind: RecordKind54::Terminal,
                participant: P0,
            },
            OrderedRecord {
                sequence: 4,
                kind: RecordKind54::Terminal,
                participant: P1,
            },
        ]);
        post.marker_candidates = vec![
            OrderedRecord {
                sequence: 5,
                kind: RecordKind54::Marker,
                participant: P0,
            },
            OrderedRecord {
                sequence: 6,
                kind: RecordKind54::Marker,
                participant: P1,
            },
        ];
        post.sequence_claims = vec![
            SequenceClaim54 {
                sequence: 5,
                kind: SequenceClaimKind54::Marker(P0),
            },
            SequenceClaim54 {
                sequence: 6,
                kind: SequenceClaimKind54::Marker(P1),
            },
            SequenceClaim54 {
                sequence: 7,
                kind: SequenceClaimKind54::Exit(P0),
            },
            SequenceClaim54 {
                sequence: 8,
                kind: SequenceClaimKind54::Exit(P1),
            },
            SequenceClaim54 {
                sequence: 9,
                kind: SequenceClaimKind54::OtherTimesExit {
                    other_participant: P1,
                    exiting_participant: P0,
                },
            },
            SequenceClaim54 {
                sequence: 10,
                kind: SequenceClaimKind54::OtherTimesExit {
                    other_participant: P0,
                    exiting_participant: P1,
                },
            },
        ];
        post.edge = StoredEdge::ObserverProjection(ObserverProjection::new(4));
        post
    }

    fn sequence_budget(&self) -> SequenceBudget {
        let mut e = 0;
        let mut t = 0;
        let mut m = 0;
        let mut l_times_t = 0;
        let mut l_other_times_e = 0;
        for claim in &self.sequence_claims {
            match claim.kind {
                SequenceClaimKind54::Exit(_) => e += 1,
                SequenceClaimKind54::Terminal(_) => t += 1,
                SequenceClaimKind54::Marker(_) => m += 1,
                SequenceClaimKind54::LiveTimesTerminal { .. } => l_times_t += 1,
                SequenceClaimKind54::OtherTimesExit { .. } => l_other_times_e += 1,
            }
        }
        sequence_budget(self.high, e, t, m, 0, 0, l_times_t, 0, l_other_times_e)
    }

    fn append_marker(&mut self, sequence: u64) {
        let candidate = self
            .marker_candidates
            .first()
            .copied()
            .expect("one marker candidate remains");
        assert_eq!(candidate.sequence, sequence);
        self.marker_candidates.remove(0);
        self.records.push(candidate);
        self.high = sequence;
        self.edge = StoredEdge::ObserverProjection(ObserverProjection::new(sequence));
    }

    fn complete_projection(&mut self, through: u64) {
        self.observer = self.observer.max(through);
        if let StoredEdge::ObserverProjection(current) = self.edge
            && current.through_seq() <= through
            && let Some(next) = self.marker_candidates.first()
        {
            self.edge = StoredEdge::ObserverProjection(ObserverProjection::new(next.sequence));
        }
    }
}

#[test]
fn acceptance_case_54_multi_binding_shutdown_families_and_participant_scoped_progress() {
    // Frozen PARTICIPANT-CONTRACT.md lines 4766-5270. Per
    // docs/design/LP-EXTRACTION-GOAL.md Fix 2 this test deliberately has no
    // successor_milestones array, O_max/O_base, ordinal partition, or
    // occurrence-array corruption arm. Completion coverage comes from typed
    // edge transitions and variable (participant_index,boundary) facts.
    const C54: u64 = 54;
    const H: u64 = 100;
    let cap = uniform(7);
    let q = uniform(2);
    let k = uniform(2);
    let marker_max = uniform(1);

    let startup =
        retained_baseline(uniform(0), 2, 0, marker_max).expect("startup has two identity reserves");
    assert_eq!(startup, wide_uniform(2));
    let p0_enrolled = retained_baseline(uniform(1), 2, 0, marker_max)
        .expect("P0 enrollment baseline is representable");
    assert_eq!(p0_enrolled, wide_uniform(3));
    assert!(zero_debt_admission(p0_enrolled, q, uniform(0), cap));
    let p1_enrolled = retained_baseline(uniform(2), 2, 0, marker_max)
        .expect("P1 enrollment baseline is representable");
    assert_eq!(p1_enrolled, wide_uniform(4));
    let episode_capacity = mandatory_capacity(p1_enrolled, q, k, cap);
    assert_eq!(episode_capacity.debt, wide_uniform(1));
    assert!(episode_capacity.is_legal());

    let e0 = epoch(54, 0, 1);
    let e1 = epoch(54, 1, 1);
    let debt_one = closure_debt(1);
    let mut progress = NonzeroDebtCursorEpisode::new(
        C54,
        debt_one,
        2,
        6,
        1,
        1,
        vec![
            BoundParticipantCursor::new(P0, e0, 0),
            BoundParticipantCursor::new(P1, e1, 0),
        ],
    )
    .expect("two participant-scoped cursor domains fit the same episode");
    for participant in [P0, P1] {
        let epoch = if participant == P0 { e0 } else { e1 };
        let request = ParticipantAck {
            conversation_id: C54,
            participant_id: participant,
            capability_generation: Generation::ONE,
            through_seq: 2,
        };
        assert_client_round_trip(ClientRequest::ParticipantAck(request.clone()));
        assert!(matches!(
            progress
                .acknowledge(participant, epoch, &request, 6)
                .expect("same retained suffix is independently available"),
            liminal_protocol::lifecycle::CumulativeAckOutcome::Committed(_)
        ));
    }
    assert_eq!(progress.facts().len(), 2);
    for participant_index in [P0, P1] {
        assert_eq!(
            progress.facts().get(CursorProgressKey {
                participant_index,
                boundary: 2,
            }),
            Some(CursorProgressFact::Consumed)
        );
    }
    let encoded_progress = progress.encode().expect("variable facts serialize");
    let fact_count_offset = 92 + 2 * 40;
    assert_eq!(
        u32::from_be_bytes(
            encoded_progress[fact_count_offset..fact_count_offset + 4]
                .try_into()
                .expect("fact count after two cursor rows"),
        ),
        2
    );

    let pre_shutdown = ShutdownSnapshot54 {
        high: 2,
        floor: 1,
        observer: 2,
        records: vec![
            OrderedRecord {
                sequence: 1,
                kind: RecordKind54::Attached,
                participant: P0,
            },
            OrderedRecord {
                sequence: 2,
                kind: RecordKind54::Attached,
                participant: P1,
            },
        ],
        marker_candidates: Vec::new(),
        sequence_claims: vec![
            SequenceClaim54 {
                sequence: 3,
                kind: SequenceClaimKind54::Terminal(P0),
            },
            SequenceClaim54 {
                sequence: 4,
                kind: SequenceClaimKind54::Terminal(P1),
            },
            SequenceClaim54 {
                sequence: 5,
                kind: SequenceClaimKind54::LiveTimesTerminal {
                    live_participant: P0,
                    terminal_participant: P0,
                },
            },
            SequenceClaim54 {
                sequence: 6,
                kind: SequenceClaimKind54::LiveTimesTerminal {
                    live_participant: P1,
                    terminal_participant: P0,
                },
            },
            SequenceClaim54 {
                sequence: 7,
                kind: SequenceClaimKind54::LiveTimesTerminal {
                    live_participant: P0,
                    terminal_participant: P1,
                },
            },
            SequenceClaim54 {
                sequence: 8,
                kind: SequenceClaimKind54::LiveTimesTerminal {
                    live_participant: P1,
                    terminal_participant: P1,
                },
            },
            SequenceClaim54 {
                sequence: 9,
                kind: SequenceClaimKind54::Exit(P0),
            },
            SequenceClaim54 {
                sequence: 10,
                kind: SequenceClaimKind54::Exit(P1),
            },
            SequenceClaim54 {
                sequence: 11,
                kind: SequenceClaimKind54::OtherTimesExit {
                    other_participant: P1,
                    exiting_participant: P0,
                },
            },
            SequenceClaim54 {
                sequence: 12,
                kind: SequenceClaimKind54::OtherTimesExit {
                    other_participant: P0,
                    exiting_participant: P1,
                },
            },
        ],
        edge: StoredEdge::ObserverProjection(ObserverProjection::new(2)),
    };
    let crash_before = pre_shutdown.clone();
    let post_batch = pre_shutdown.atomic_shutdown();
    let crash_after = post_batch.clone();
    assert_eq!(crash_before.high, 2);
    assert_eq!(crash_after.high, 4);
    assert_eq!(
        &crash_after.records[2..],
        &[
            OrderedRecord {
                sequence: 3,
                kind: RecordKind54::Terminal,
                participant: P0,
            },
            OrderedRecord {
                sequence: 4,
                kind: RecordKind54::Terminal,
                participant: P1,
            },
        ]
    );
    assert_eq!(post_batch, pre_shutdown.atomic_shutdown());

    let post_batch_baseline = retained_baseline(uniform(5), 2, 2, marker_max)
        .expect("two planned markers own both capacity credits");
    assert_eq!(post_batch_baseline, wide_uniform(5));
    let post_batch_capacity = mandatory_capacity(post_batch_baseline, q, k, cap);
    assert_eq!(post_batch_capacity.debt, wide_uniform(2));
    assert!(post_batch_capacity.is_legal());
    let actual_post_shutdown_budget = post_batch.sequence_budget();
    assert_eq!(
        actual_post_shutdown_budget,
        SequenceBudget {
            high_watermark: 4,
            remaining: u64::MAX - 4,
            e: 2,
            t: 0,
            m: 2,
            rs: 0,
            rt: 0,
            l_times_t: 0,
            l_times_rt: 0,
            l_other_times_e: 2,
        }
    );
    let reserve_total = u128::try_from(post_batch.sequence_claims.len())
        .expect("fixture reserve count fits the widened domain");
    assert_eq!(reserve_total, 6);

    // One representative linear extension: M5<P5<P4<M6<P6. Delayed P4
    // advances o but preserves the already selected OP6.
    let mut schedule = post_batch;
    schedule.append_marker(5);
    assert_eq!(
        schedule.edge,
        StoredEdge::ObserverProjection(ObserverProjection::new(5))
    );
    schedule.complete_projection(5);
    assert_eq!(
        schedule.edge,
        StoredEdge::ObserverProjection(ObserverProjection::new(6))
    );
    schedule.complete_projection(4);
    assert_eq!(schedule.observer, 5);
    assert_eq!(
        schedule.edge,
        StoredEdge::ObserverProjection(ObserverProjection::new(6))
    );
    schedule.append_marker(6);
    schedule.complete_projection(6);
    assert_eq!(schedule.high, 6);
    assert_eq!(schedule.observer, 6);
    assert!(schedule.marker_candidates.is_empty());
    assert_eq!(
        schedule
            .records
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        [1, 2, 3, 4, 5, 6]
    );

    let debt_two = closure_debt(2);
    let op4 = ObserverProjection::new(4);
    let marker5_event = Event::marker_appended(5, 5);
    let op5_authority = op4
        .later_projection_after_marker(&marker5_event, debt_two, ObserverProjection::new(5))
        .expect("M5 owns exact OP4 to OP5 successor");
    assert_eq!(
        op4.marker_appended(debt_two, marker5_event, op5_authority)
            .expect("typed M5 transition commits"),
        ClosureState::Owed {
            debt: debt_two,
            edge: StoredEdge::ObserverProjection(ObserverProjection::new(5)),
        }
    );
    let op6 = ObserverProjection::new(6);
    let delayed = op6
        .independent_event(
            debt_two,
            Event::binding_fate_observed(P0, e0, 2),
            Some(debt_two),
        )
        .expect("delayed lower completion/fate cannot regress OP6");
    assert_eq!(
        delayed,
        ClosureState::Owed {
            debt: debt_two,
            edge: StoredEdge::ObserverProjection(op6),
        }
    );

    let l54_p0 = LeaveRequest {
        conversation_id: C54,
        participant_id: P0,
        capability_generation: Generation::ONE,
        attach_secret: secret(1),
        leave_attempt_token: leave_token(0x50),
    };
    let l54_p1 = LeaveRequest {
        participant_id: P1,
        leave_attempt_token: leave_token(0x51),
        ..l54_p0
    };
    for request in [&l54_p0, &l54_p1] {
        assert_client_round_trip(ClientRequest::Leave(request.clone()));
        let refusal = liminal_protocol::wire::ObserverBackpressure::Leave {
            request: leave_envelope(request),
            state: liminal_protocol::wire::ObserverBackpressureState::initial(6),
            prior_terminal_cell_exists: false,
        };
        assert_server_round_trip(ServerValue::ObserverBackpressure(refusal));
    }

    // Family A: both markers then Left7/Left8. The second Leave is refused for
    // o<=6 and succeeds after OP7; equality clears full K in the same commit.
    let family_a_first = mandatory_capacity(wide_uniform(6), q, uniform(1), cap);
    assert_eq!(family_a_first.debt, wide_uniform(2));
    assert!(family_a_first.is_legal());
    assert!(no_edge_legal(
        WideResourceVector::default(),
        wide_uniform(3),
        q,
        k,
        cap,
    ));
    let family_a_floor = floor_transition(2, None, 8, 7, 8);
    assert_eq!(family_a_floor.preferred_floor, 8);
    assert_eq!(family_a_floor.resulting_floor, 8);

    // Family B: M5 then P1 Left6 cancels M6; P0 Left7 is safe only after OP6.
    let family_b_floor = floor_transition(2, None, 7, 6, 7);
    assert_eq!(family_b_floor.preferred_floor, 7);
    assert_eq!(family_b_floor.resulting_floor, 7);
    assert!(no_edge_legal(
        WideResourceVector::default(),
        wide_uniform(3),
        q,
        k,
        cap,
    ));

    // The generated byte-capacity walk near frozen lines 5123-5194.
    const U: u64 = 16;
    let marker_four = ResourceVector::new(1, 4 * U);
    let q_bytes = ResourceVector::new(2, 8 * U);
    let cap_bytes = ResourceVector::new(12, 48 * U);
    let pre = retained_baseline(ResourceVector::new(6, 28 * U), 1, 0, marker_four)
        .expect("exact six-row byte walk is valid");
    assert_eq!(pre, WideResourceVector::new(7, 32 * u128::from(U)));
    assert!(zero_debt_admission(pre, q_bytes, q_bytes, cap_bytes));
    let tentative = retained_baseline(ResourceVector::new(7, 32 * U), 1, 0, marker_four)
        .expect("uniform ordinary append updates exact baseline");
    assert_eq!(tentative, WideResourceVector::new(8, 36 * u128::from(U)));
    let after_first_removal = WideResourceVector::new(7, 35 * u128::from(U));
    let after_first_envelope = WideResourceVector::new(
        after_first_removal.entries + u128::from(q_bytes.entries) + u128::from(q_bytes.entries),
        after_first_removal.bytes + u128::from(q_bytes.bytes) + u128::from(q_bytes.bytes),
    );
    assert_eq!(
        after_first_envelope,
        WideResourceVector::new(11, 51 * u128::from(U))
    );
    assert!(after_first_envelope.bytes > u128::from(cap_bytes.bytes));
    let after_two = WideResourceVector::new(6, 24 * u128::from(U));
    assert_eq!(
        WideResourceVector::new(
            after_two.entries + u128::from(q_bytes.entries) + u128::from(q_bytes.entries),
            after_two.bytes + u128::from(q_bytes.bytes) + u128::from(q_bytes.bytes),
        ),
        WideResourceVector::new(10, 40 * u128::from(U))
    );

    let m54 = H - 4;
    let e54_0 = epoch(54, 10, 10);
    let e54_1 = epoch(54, 11, 11);
    let e54_2 = epoch(54, 12, 12);
    let e54_3 = epoch(54, 13, 13);
    let marker = marker_delivery(P0, e54_0, m54).expect("validated case-54 marker record restores");
    let marker = marker
        .retarget(e54_1, 0, 1, 2)
        .expect("first retarget activates block0");
    let marker = marker
        .retarget(e54_2, 1, 1, 2)
        .expect("second retarget activates block1");
    assert_eq!(marker.binding_epoch(), e54_2);
    assert_eq!(
        marker.retarget(e54_3, 2, 1, 2),
        Err((marker, DetachedAttachRefusal::EpisodeChurnLimit))
    );
    let closure_refusal = MarkerClosureCapacityExceeded {
        request: ClosureCheckedEnvelope::CredentialAttach(attach_envelope(
            &CredentialAttachRequest {
                conversation_id: C54,
                participant_id: P0,
                capability_generation: generation(12),
                attach_secret: secret(12),
                attach_attempt_token: attach_token(0xA3),
                accept_marker_delivery_seq: None,
            },
        )),
        snapshot: ClosureSnapshot {
            marker_capacity_credits: 1,
            marker_anchors: 1,
            entry_debt: 2,
            byte_debt: 8 * U,
            repayment_edge: RepaymentEdge::MarkerDelivery {
                participant_id: P0,
                binding_epoch: e54_2,
                marker_delivery_seq: m54,
            },
            edge_sequence_claims: 6,
            edge_order_position_claims: 4,
            edge_k_remaining: q_bytes,
            k_headroom: WideResourceVector::new(2, 8 * u128::from(U)),
            episode_churn_used: 2,
            delta_cycles: 1,
            episode_churn_limit: 2,
        },
        reason: ClosureRefusalReason::EpisodeChurnLimit,
    };
    assert_server_round_trip(ServerValue::MarkerClosureCapacityExceeded(Box::new(
        closure_refusal,
    )));

    let delivered = marker
        .delivered(debt_two, Event::marker_delivered(P0, e54_2, m54))
        .expect("m54 delivery is tied to final retarget e2");
    let ClosureState::Owed {
        edge: StoredEdge::ParticipantCursorProgress(marker_progress),
        ..
    } = delivered
    else {
        panic!("marker delivery must select marker-backed PCP")
    };
    let CursorFateSuccessor::DetachedCredentialRecovery(dcr) = marker_progress
        .binding_fate(debt_two, Event::binding_fate_observed(P0, e54_2, H - 4))
        .expect("delivered-marker fate derives DCR")
    else {
        panic!("delivered marker cannot derive DMR")
    };
    let leave_claim = dcr
        .validate_leave_claim(P0, marker_four, q_bytes, 1)
        .expect("detached Leave owns one exact K-backed charge");
    assert_eq!(leave_claim.actual_charge(), marker_four);
    assert_eq!(
        dcr.detached_leave(
            debt_two,
            Event::detached_leave_committed(P0, H + 6),
            leave_claim,
            DebtCompletion::clear(),
        )
        .expect("ticket-free DCR Leave clears the episode"),
        ClosureState::Clear
    );
    let (fate_owner, recovery_claim) = mint_fenced_attach_with_owner(
        dcr,
        debt_two,
        Event::fenced_recovery_committed(P0, m54, e54_2, e54_3, H + 1),
        DebtCompletion::clear(),
    )
    .expect("owner consumes the exact delivered marker for fenced recovery");
    assert_eq!(recovery_claim.new_binding_epoch(), e54_3);
    assert!(
        recovered_fate_from_fenced(fate_owner, recovery_claim, H + 1).is_err(),
        "a fenced attach that cleared debt has no recovered cursor suffix"
    );
    let transfer = recovery_transfer(
        WideResourceVector::new(10, 40 * u128::from(U)),
        q_bytes,
        marker_four,
    )
    .expect("recovery transfers one exact record charge");
    assert_eq!(
        transfer.remaining_recovery_claim,
        ResourceVector::new(1, 4 * U)
    );
}

fn pending_fate(
    conversation_id: u64,
    participant_id: u64,
    binding_epoch: BindingEpoch,
    major: u64,
) -> PendingFinalization {
    let active = ActiveBinding {
        participant_id,
        conversation_id,
        binding_epoch,
    };
    match active.connection_lost(BindingTerminalDisposition::Pending(
        PendingBindingTerminalPosition::new(major),
    )) {
        liminal_protocol::lifecycle::DiedBindingTransition::Pending(pending) => pending.into(),
        liminal_protocol::lifecycle::DiedBindingTransition::Committed(_) => {
            panic!("case 55 fate is intentionally pending behind V's marker")
        }
    }
}

#[test]
fn acceptance_case_55_adjacent_and_intervening_positional_leave_crash_replay() {
    // Frozen PARTICIPANT-CONTRACT.md lines 5271-5358.
    const C55A: u64 = 5_501;
    const C55I: u64 = 5_502;
    let e_p_a = epoch(55, 1, 1);
    let p_adjacent = member(C55A, P0, 1, 9);
    let pending_a = pending_fate(C55A, P0, e_p_a, 10);
    let request_a = LeaveRequest {
        conversation_id: C55A,
        participant_id: P0,
        capability_generation: Generation::ONE,
        attach_secret: secret(1),
        leave_attempt_token: leave_token(0x5A),
    };
    assert_client_round_trip(ClientRequest::Leave(request_a.clone()));
    let verify_a = |member: &LiveMember<Fingerprint>| {
        member
            .verify_leave_request(
                &request_a,
                AttachSecretProof::Verified,
                [0x5A; 32],
                LeaveFingerprint::new([0xA5; 32]),
            )
            .expect("L55A has exact permanent verifier authority")
    };
    let commit_adjacent = |member: LiveMember<Fingerprint>| {
        let authority = pending_leave_authority(&member, pending_a, 12, 11)
            .expect("marker11 leaves terminal-major10 adjacent to exact X-major11");
        commit_pending_leave(
            member.clone(),
            pending_a,
            DetachCell::<Fingerprint>::default(),
            verify_a(&member),
            authority,
            PendingLeaveCommitParameters {
                terminal_delivery_seq: 12,
                left_delivery_seq: 13,
            },
        )
        .expect("adjacent terminal plus Left commit atomically")
    };
    let crash_before_a = (p_adjacent, pending_a);
    let after_a = commit_adjacent(crash_before_a.0.clone());
    let replay_after_a = commit_adjacent(crash_before_a.0);
    assert_eq!(after_a, replay_after_a);
    let (after_a, _after_a_frontiers) = after_a.into_parts();
    let IdentityState::Retired(tombstone_a) = after_a else {
        panic!("adjacent Leave must retire P")
    };
    assert_eq!(
        tombstone_a.committed_result(),
        &LeaveCommitted::new(
            C55A,
            request_a.leave_attempt_token,
            P0,
            Generation::ONE,
            None,
            Some(12),
            13,
        )
        .expect("terminal precedes Left")
    );
    assert_server_round_trip(ServerValue::LeaveCommitted(
        tombstone_a.committed_result().clone(),
    ));

    let e_p_i = epoch(55, 2, 1);
    let e_u_i = epoch(55, 3, 1);
    let p_intervening = member(C55I, P0, 1, 9);
    let pending_p = pending_fate(C55I, P0, e_p_i, 10);
    let pending_u = pending_fate(C55I, P1, e_u_i, 11);
    assert_eq!(pending_p.admission_order().transaction_order(), 10);
    assert_eq!(pending_u.admission_order().transaction_order(), 11);
    assert_eq!(
        intervening_pending_leave_refusal(&p_intervening, pending_p, pending_u, 12, 12)
            .expect("case-55 intervening snapshot restores before authorization"),
        PrepareLeaveAuthorityError::PendingCandidate,
        "U's immutable terminal makes P's positional authority unconstructible"
    );
    let request_i = LeaveRequest {
        conversation_id: C55I,
        participant_id: P0,
        capability_generation: Generation::ONE,
        attach_secret: secret(1),
        leave_attempt_token: leave_token(0x5B),
    };
    assert_client_round_trip(ClientRequest::Leave(request_i.clone()));

    // Marker11, P terminal12, U terminal13, then P Left14 are four durable
    // commits. The intervening U major forbids the adjacent composition path.
    let p_terminal = pending_p.commit(12);
    let _u_terminal = pending_u.commit(13);
    let p_with_terminal = p_intervening
        .with_committed_terminal(p_terminal)
        .expect("separately drained P terminal belongs to P");
    let commit_intervening = |member: LiveMember<Fingerprint>| {
        let authority = settled_leave_authority(&member, BindingState::Detached, 14, 14)
            .expect("separate drains leave an exact settled X-major14 authority");
        let verified = member
            .verify_leave_request(
                &request_i,
                AttachSecretProof::Verified,
                [0x5B; 32],
                LeaveFingerprint::new([0xB5; 32]),
            )
            .expect("replay begins from identical drained state");
        commit_leave(
            member,
            BindingState::Detached,
            DetachCell::<Fingerprint>::default(),
            verified,
            authority,
            LeaveCommitParameters {
                left_delivery_seq: 14,
            },
        )
        .expect("intervening arm appends only Left after separate drains")
    };
    let after_i = commit_intervening(p_with_terminal.clone());
    let replay_after_i = commit_intervening(p_with_terminal);
    assert_eq!(after_i, replay_after_i);
    let (after_i, _after_i_frontiers) = after_i.into_parts();
    let IdentityState::Retired(tombstone_i) = after_i else {
        panic!("intervening Leave must retire P")
    };
    assert_eq!(
        tombstone_i.committed_result(),
        &LeaveCommitted::new(
            C55I,
            request_i.leave_attempt_token,
            P0,
            Generation::ONE,
            None,
            Some(12),
            14,
        )
        .expect("separately drained terminal precedes Left14")
    );
    assert_eq!(
        sequence_budget(14, 2, 1, 0, 0, 0, 2, 0, 2),
        SequenceBudget {
            high_watermark: 14,
            remaining: u64::MAX - 14,
            e: 2,
            t: 1,
            m: 0,
            rs: 0,
            rt: 0,
            l_times_t: 2,
            l_times_rt: 0,
            l_other_times_e: 2,
        }
    );

    let adjacent_baseline =
        retained_baseline(uniform(5), 3, 2, uniform(1)).expect("adjacent final retained baseline");
    let intervening_baseline = retained_baseline(uniform(6), 3, 2, uniform(1))
        .expect("intervening final retained baseline");
    assert_eq!(adjacent_baseline, wide_uniform(6));
    assert_eq!(intervening_baseline, wide_uniform(7));
    assert!(zero_debt_admission(
        adjacent_baseline,
        uniform(2),
        uniform(2),
        uniform(16)
    ));
    assert!(zero_debt_admission(
        intervening_baseline,
        uniform(2),
        uniform(2),
        uniform(16)
    ));
    assert_server_round_trip(ServerValue::LeaveCommitted(
        tombstone_i.committed_result().clone(),
    ));
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum OrderHandle56 {
    AttachP0,
    AttachP1,
    ExitP0,
    ExitP1,
}

fn relocate_after_p1_leave(r: u64) -> (u64, BTreeMap<OrderHandle56, u64>) {
    let mut positions = BTreeMap::from([
        (OrderHandle56::AttachP1, r + 1),
        (OrderHandle56::AttachP0, r + 2),
        (OrderHandle56::ExitP1, r + 3),
        (OrderHandle56::ExitP0, r + 4),
    ]);
    assert_eq!(positions.remove(&OrderHandle56::ExitP1), Some(r + 3));
    assert_eq!(positions.remove(&OrderHandle56::AttachP1), Some(r + 1));
    positions.insert(OrderHandle56::AttachP0, r + 4);
    positions.insert(OrderHandle56::ExitP0, r + 5);
    (r + 3, positions)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecordKind56 {
    Left,
    Ordinary,
    BindingTerminal,
    Attached,
    Marker,
    DiedTerminal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DurableRecord56 {
    sequence: u64,
    kind: RecordKind56,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PcpOrderingSnapshot56 {
    conversation_id: u64,
    participant_id: u64,
    binding_epoch: BindingEpoch,
    seed_high: u64,
    high: u64,
    floor: u64,
    observer: u64,
    log: Vec<DurableRecord56>,
    retained: Vec<u64>,
    edge: StoredEdge,
    planned_marker: MarkerDelivery,
    marker_pending_append: bool,
    marker_credits: u64,
    pending_finalization: Option<PendingFinalization>,
    fate_observed: bool,
    debt: ClosureDebt,
    k_remaining: ResourceVector,
}

impl PcpOrderingSnapshot56 {
    fn seed(
        conversation_id: u64,
        participant_id: u64,
        binding_epoch: BindingEpoch,
        high: u64,
        debt: ClosureDebt,
        compaction: PhysicalCompaction,
        marker: MarkerDelivery,
    ) -> Self {
        Self {
            conversation_id,
            participant_id,
            binding_epoch,
            seed_high: high,
            high,
            floor: high - 3,
            observer: high,
            log: vec![
                DurableRecord56 {
                    sequence: high - 3,
                    kind: RecordKind56::Left,
                },
                DurableRecord56 {
                    sequence: high - 2,
                    kind: RecordKind56::Ordinary,
                },
                DurableRecord56 {
                    sequence: high - 1,
                    kind: RecordKind56::BindingTerminal,
                },
                DurableRecord56 {
                    sequence: high,
                    kind: RecordKind56::Attached,
                },
            ],
            retained: vec![high - 3, high - 2, high - 1, high],
            edge: StoredEdge::PhysicalCompaction(compaction),
            planned_marker: marker,
            marker_pending_append: true,
            marker_credits: 1,
            pending_finalization: None,
            fate_observed: false,
            debt,
            k_remaining: uniform(2),
        }
    }

    fn baseline(&self) -> WideResourceVector {
        let planned_rows = self.retained.len() + usize::from(self.marker_pending_append);
        retained_baseline(
            uniform(u64::try_from(planned_rows).expect("case 56 retained count fits u64")),
            2,
            self.marker_credits,
            uniform(1),
        )
        .expect("case 56 has no excess marker credit")
    }

    fn persist_fate_before_storage(mut self) -> Self {
        assert!(!self.fate_observed);
        assert!(self.pending_finalization.is_none());
        let StoredEdge::PhysicalCompaction(compaction) = self.edge else {
            panic!("fate-first begins while the exact PC is current")
        };
        let pending = pending_fate(
            self.conversation_id,
            self.participant_id,
            self.binding_epoch,
            self.seed_high - 5,
        );
        assert_eq!(pending.participant_id(), self.participant_id);
        assert_eq!(pending.binding_epoch(), self.binding_epoch);
        assert_eq!(
            pending.admission_order().transaction_order(),
            self.seed_high - 5
        );

        let terminal_tentative = retained_baseline(
            uniform(
                u64::try_from(self.retained.len() + 2)
                    .expect("retained rows plus marker and terminal fit u64"),
            ),
            2,
            self.marker_credits,
            uniform(1),
        )
        .expect("the tentative terminal preserves the one marker credit");
        assert_eq!(terminal_tentative, wide_uniform(7));
        let terminal_and_k = WideResourceVector::new(
            terminal_tentative.entries + u128::from(self.k_remaining.entries),
            terminal_tentative.bytes + u128::from(self.k_remaining.bytes),
        );
        assert_eq!(terminal_and_k, wide_uniform(9));
        assert!(terminal_and_k.entries > 8);
        assert!(terminal_and_k.bytes > u128::from(uniform(8).bytes));

        let event = Event::binding_fate_observed(
            self.participant_id,
            self.binding_epoch,
            self.seed_high - 3,
        );
        assert_eq!(
            compaction
                .preserve_progress(self.debt, event, self.debt)
                .expect("pending fate preserves the still-required PC"),
            ClosureState::Owed {
                debt: self.debt,
                edge: StoredEdge::PhysicalCompaction(compaction),
            }
        );
        self.pending_finalization = Some(pending);
        self.fate_observed = true;
        self
    }

    fn complete_compaction(mut self) -> Self {
        let StoredEdge::PhysicalCompaction(compaction) = self.edge else {
            panic!("PC completion consumes the exact stored range")
        };
        let event =
            Event::compaction_completed(self.seed_high - 3, self.seed_high - 3, self.seed_high - 2)
                .expect("case 56 completion advances the floor");
        let successor = compaction
            .strict_after_completion(
                &event,
                self.debt,
                StoredEdge::MarkerDelivery(self.planned_marker),
                self.seed_high + 1,
            )
            .expect("PC owns the exact marker successor at h+1");
        let state = compaction
            .complete(self.debt, event, successor)
            .expect("PC completion installs the marker suffix");
        assert_eq!(
            state,
            ClosureState::Owed {
                debt: self.debt,
                edge: StoredEdge::MarkerDelivery(self.planned_marker),
            }
        );
        assert_eq!(self.retained.remove(0), self.seed_high - 3);
        self.floor = self.seed_high - 2;
        self.edge = StoredEdge::MarkerDelivery(self.planned_marker);
        self
    }

    fn append_marker(mut self) -> Self {
        assert!(self.marker_pending_append);
        assert_eq!(self.high, self.seed_high);
        assert_eq!(self.edge, StoredEdge::MarkerDelivery(self.planned_marker));
        assert_eq!(self.planned_marker.participant_id(), self.participant_id);
        assert_eq!(self.planned_marker.binding_epoch(), self.binding_epoch);
        assert_eq!(
            self.planned_marker.marker_delivery_seq(),
            self.seed_high + 1
        );
        self.high = self.seed_high + 1;
        self.log.push(DurableRecord56 {
            sequence: self.high,
            kind: RecordKind56::Marker,
        });
        self.retained.push(self.high);
        self.marker_pending_append = false;
        self
    }

    fn drain_pending_terminal(mut self) -> Self {
        assert!(self.fate_observed);
        let pending = self
            .pending_finalization
            .take()
            .expect("fate-first persists the exact terminal authority");
        let committed = pending.commit(self.seed_high + 2);
        assert_eq!(committed.participant_id(), self.participant_id);
        assert_eq!(committed.binding_epoch(), self.binding_epoch);
        assert_eq!(
            committed.admission_order().transaction_order(),
            self.seed_high - 5
        );
        assert_eq!(committed.delivery_seq(), self.seed_high + 2);
        self.install_terminal_and_dmr()
    }

    fn commit_fate_after_marker(mut self) -> Self {
        assert!(!self.fate_observed);
        assert!(self.pending_finalization.is_none());
        let active = ActiveBinding {
            participant_id: self.participant_id,
            conversation_id: self.conversation_id,
            binding_epoch: self.binding_epoch,
        };
        let transition = active.connection_lost(BindingTerminalDisposition::Committed(
            CommittedBindingTerminalPosition::new(self.seed_high - 5, self.seed_high + 2),
        ));
        let liminal_protocol::lifecycle::DiedBindingTransition::Committed(committed) = transition
        else {
            panic!("PC-first fate commits its terminal immediately")
        };
        assert_eq!(committed.participant_id(), self.participant_id);
        assert_eq!(committed.binding_epoch(), self.binding_epoch);
        assert_eq!(
            committed.admission_order().transaction_order(),
            self.seed_high - 5
        );
        assert_eq!(committed.delivery_seq(), self.seed_high + 2);
        self.fate_observed = true;
        self.install_terminal_and_dmr()
    }

    fn install_terminal_and_dmr(mut self) -> Self {
        assert_eq!(self.high, self.seed_high + 1);
        let StoredEdge::MarkerDelivery(marker) = self.edge else {
            panic!("undelivered marker owns the fate-to-DMR transition")
        };
        let fate = Event::binding_fate_observed(
            self.participant_id,
            self.binding_epoch,
            self.seed_high - 3,
        );
        let dmr_state = marker
            .binding_fate(self.debt, fate)
            .expect("exact undelivered-marker fate derives DMR");
        let ClosureState::Owed {
            debt,
            edge: StoredEdge::DetachedMarkerRelease(dmr),
        } = dmr_state
        else {
            panic!("case 56 fate must retain debt under DMR")
        };
        assert_eq!(debt, self.debt);
        assert_eq!(dmr.participant_id(), self.participant_id);
        assert_eq!(dmr.marker_delivery_seq(), self.seed_high + 1);
        assert_eq!(dmr.last_dead_binding_epoch(), self.binding_epoch);
        self.high = self.seed_high + 2;
        self.log.push(DurableRecord56 {
            sequence: self.high,
            kind: RecordKind56::DiedTerminal,
        });
        self.retained.push(self.high);
        self.edge = StoredEdge::DetachedMarkerRelease(dmr);
        self
    }
}

fn replay_transition56<F>(prestate: &PcpOrderingSnapshot56, transition: F) -> PcpOrderingSnapshot56
where
    F: Fn(PcpOrderingSnapshot56) -> PcpOrderingSnapshot56,
{
    let committed = transition(prestate.clone());
    let replayed = transition(prestate.clone());
    assert_eq!(committed, replayed);
    committed
}

#[test]
fn acceptance_case_56_sequence_equality_and_pcp_ordering_without_occurrence_array() {
    // Frozen PARTICIPANT-CONTRACT.md lines 5359-5535. Per
    // docs/design/LP-EXTRACTION-GOAL.md Fix 2, no O_max/base ranges or fixed
    // occurrence slots are transcribed; the same ordering is covered by typed
    // PC/marker/DMR/DCR transitions and participant-scoped cursor facts.
    const C56E: u64 = 5_601;
    const C56S: u64 = 5_602;
    const MAX: u64 = u64::MAX;
    let g = MAX - 6;
    let r_e = g - 10;
    let (order_high, relocated) = relocate_after_p1_leave(r_e);
    assert_eq!(order_high, r_e + 3);
    assert_eq!(relocated[&OrderHandle56::AttachP0], r_e + 4);
    assert_eq!(relocated[&OrderHandle56::ExitP0], r_e + 5);
    assert_eq!(relocated.len(), 2);

    let request_e = CredentialAttachRequest {
        conversation_id: C56E,
        participant_id: P1,
        capability_generation: generation(4),
        attach_secret: secret(4),
        attach_attempt_token: attach_token(0x56),
        accept_marker_delivery_seq: None,
    };
    assert_client_round_trip(ClientRequest::CredentialAttach(request_e.clone()));
    let pre_budget = sequence_budget(g - 2, 1, 1, 0, 0, 0, 1, 0, 0);
    assert_eq!(pre_budget.remaining, 8);
    let resulting_budget = sequence_budget(g, 1, 1, 1, 1, 1, 1, 1, 0);
    assert_eq!(resulting_budget.remaining, 6);
    let required_positions = u128::from(resulting_budget.e)
        + u128::from(resulting_budget.t)
        + u128::from(resulting_budget.m)
        + u128::from(resulting_budget.rs)
        + u128::from(resulting_budget.rt)
        + resulting_budget.l_times_t
        + resulting_budget.l_times_rt
        + resulting_budget.l_other_times_e;
    assert_eq!(required_positions, 7);
    assert!(required_positions > u128::from(resulting_budget.remaining));

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct EqualitySnapshot {
        high: u64,
        floor: u64,
        observer: u64,
        cursor: u64,
        retained: [u64; 2],
        marker_credits: u64,
        debt: WideResourceVector,
        generation: Generation,
        order_high: u64,
    }
    let before = EqualitySnapshot {
        high: g - 2,
        floor: g - 3,
        observer: g - 2,
        cursor: g - 4,
        retained: [g - 3, g - 2],
        marker_credits: 1,
        debt: WideResourceVector::default(),
        generation: generation(4),
        order_high,
    };
    let exhausted = ConversationSequenceExhausted {
        request: SequenceAllocatingEnvelope::CredentialAttach(attach_envelope(&request_e)),
        sequence_budget: resulting_budget,
    };
    assert_server_round_trip(ServerValue::ConversationSequenceExhausted(Box::new(
        exhausted,
    )));
    let after = before.clone();
    assert_eq!(after, before);
    let prospective = retained_baseline(uniform(5), 2, 2, uniform(1))
        .expect("absolute-fit snapshot is algebraically legal");
    assert_eq!(prospective, wide_uniform(5));
    let capacity = mandatory_capacity(prospective, uniform(2), uniform(2), uniform(7));
    assert_eq!(capacity.debt, wide_uniform(2));
    assert!(capacity.is_legal());

    let h = MAX - 7;
    let r_s = h - 11;
    let (success_order_high, success_relocated) = relocate_after_p1_leave(r_s);
    assert_eq!(success_order_high, r_s + 3);
    assert_eq!(success_relocated[&OrderHandle56::AttachP0], r_s + 4);
    assert_eq!(success_relocated[&OrderHandle56::ExitP0], r_s + 5);
    let request_s = CredentialAttachRequest {
        conversation_id: C56S,
        participant_id: P1,
        capability_generation: generation(4),
        attach_secret: secret(4),
        attach_attempt_token: attach_token(0x57),
        accept_marker_delivery_seq: None,
    };
    assert_client_round_trip(ClientRequest::CredentialAttach(request_s));
    let success_budget = sequence_budget(h, 1, 1, 1, 1, 1, 1, 1, 0);
    assert_eq!(success_budget.remaining, 7);
    let success_positions = [h + 1, h + 2, h + 3, h + 4, h + 5, h + 6, MAX];
    assert_eq!(success_positions.len(), 7);
    assert_eq!(success_positions[6], MAX);

    let success_baseline =
        retained_baseline(uniform(5), 2, 1, uniform(1)).expect("planned P0 marker owns one credit");
    assert_eq!(success_baseline, wide_uniform(6));
    let success_capacity = mandatory_capacity(success_baseline, uniform(2), uniform(2), uniform(8));
    assert_eq!(success_capacity.debt, wide_uniform(2));
    assert!(success_capacity.is_legal());

    let e5 = epoch(56, 5, 5);
    let e6 = epoch(56, 6, 6);
    let debt_two = closure_debt(2);
    let pc = PhysicalCompaction::new(h - 3, h - 3).expect("single-row P1 Left range is nonempty");
    let marker = marker_delivery(P1, e5, h + 1).expect("validated case-56 marker record restores");
    let fate_first_seed = PcpOrderingSnapshot56::seed(C56S, P1, e5, h, debt_two, pc, marker);
    let fate_pending = replay_transition56(
        &fate_first_seed,
        PcpOrderingSnapshot56::persist_fate_before_storage,
    );
    assert_eq!(fate_pending.high, h);
    assert_eq!(fate_pending.floor, h - 3);
    assert_eq!(fate_pending.log, fate_first_seed.log);
    assert_eq!(fate_pending.baseline(), wide_uniform(6));
    let persisted = fate_pending
        .pending_finalization
        .expect("fate-first has a durable pending terminal");
    assert_eq!(persisted.participant_id(), P1);
    assert_eq!(persisted.binding_epoch(), e5);
    assert_eq!(persisted.admission_order().transaction_order(), h - 5);
    assert_eq!(fate_pending.edge, StoredEdge::PhysicalCompaction(pc));

    let fate_pc_completed =
        replay_transition56(&fate_pending, PcpOrderingSnapshot56::complete_compaction);
    assert_eq!(fate_pc_completed.high, h);
    assert_eq!(fate_pc_completed.floor, h - 2);
    assert_eq!(fate_pc_completed.retained, [h - 2, h - 1, h]);
    assert_eq!(fate_pc_completed.pending_finalization, Some(persisted));
    assert_eq!(fate_pc_completed.edge, StoredEdge::MarkerDelivery(marker));

    let fate_marker_appended =
        replay_transition56(&fate_pc_completed, PcpOrderingSnapshot56::append_marker);
    assert_eq!(fate_marker_appended.high, h + 1);
    assert_eq!(fate_marker_appended.retained, [h - 2, h - 1, h, h + 1]);
    assert_eq!(fate_marker_appended.pending_finalization, Some(persisted));
    let fate_first_final = replay_transition56(
        &fate_marker_appended,
        PcpOrderingSnapshot56::drain_pending_terminal,
    );

    let pc_first_seed = PcpOrderingSnapshot56::seed(C56S, P1, e5, h, debt_two, pc, marker);
    assert!(pc_first_seed.pending_finalization.is_none());
    let pc_completed =
        replay_transition56(&pc_first_seed, PcpOrderingSnapshot56::complete_compaction);
    assert_eq!(pc_completed.high, h);
    assert_eq!(pc_completed.floor, h - 2);
    assert_eq!(pc_completed.retained, [h - 2, h - 1, h]);
    assert!(!pc_completed.fate_observed);
    let pc_marker_appended =
        replay_transition56(&pc_completed, PcpOrderingSnapshot56::append_marker);
    assert_eq!(pc_marker_appended.high, h + 1);
    assert!(pc_marker_appended.pending_finalization.is_none());
    assert!(!pc_marker_appended.fate_observed);
    let pc_first_final = replay_transition56(
        &pc_marker_appended,
        PcpOrderingSnapshot56::commit_fate_after_marker,
    );

    let expected_complete_log = vec![
        DurableRecord56 {
            sequence: h - 3,
            kind: RecordKind56::Left,
        },
        DurableRecord56 {
            sequence: h - 2,
            kind: RecordKind56::Ordinary,
        },
        DurableRecord56 {
            sequence: h - 1,
            kind: RecordKind56::BindingTerminal,
        },
        DurableRecord56 {
            sequence: h,
            kind: RecordKind56::Attached,
        },
        DurableRecord56 {
            sequence: h + 1,
            kind: RecordKind56::Marker,
        },
        DurableRecord56 {
            sequence: h + 2,
            kind: RecordKind56::DiedTerminal,
        },
    ];
    for completed in [&fate_first_final, &pc_first_final] {
        assert_eq!(completed.log, expected_complete_log);
        assert_eq!(completed.high, h + 2);
        assert_eq!(completed.floor, h - 2);
        assert_eq!(completed.observer, h);
        assert_eq!(completed.retained, [h - 2, h - 1, h, h + 1, h + 2]);
        assert!(!completed.marker_pending_append);
        assert!(completed.pending_finalization.is_none());
        assert!(completed.fate_observed);
        assert_eq!(completed.baseline(), wide_uniform(6));
        assert_eq!(completed.k_remaining, uniform(2));
        let completed_capacity = mandatory_capacity(
            completed.baseline(),
            uniform(2),
            completed.k_remaining,
            uniform(8),
        );
        assert_eq!(completed_capacity.debt, wide_uniform(2));
        assert!(completed_capacity.is_legal());
        let StoredEdge::DetachedMarkerRelease(completed_dmr) = completed.edge else {
            panic!("both arrival orders finish at exact DMR")
        };
        assert_eq!(completed_dmr.participant_id(), P1);
        assert_eq!(completed_dmr.marker_delivery_seq(), h + 1);
        assert_eq!(completed_dmr.last_dead_binding_epoch(), e5);
    }
    assert_eq!(fate_first_final, pc_first_final);
    let StoredEdge::DetachedMarkerRelease(dmr) = pc_first_final.edge else {
        panic!("undelivered e5 marker must select DMR")
    };
    let leave_claim = dmr
        .validate_leave_claim(P1, uniform(1), uniform(2), 1)
        .expect("detached Leave transfers one K-backed record");
    assert_eq!(
        dmr.leave(
            debt_two,
            Event::detached_leave_committed(P1, h + 3),
            leave_claim,
            DebtCompletion::clear(),
        )
        .expect("detached Leave reaches full-K equality in one commit"),
        ClosureState::Clear
    );

    let delivered = marker
        .delivered(debt_two, Event::marker_delivered(P1, e5, h + 1))
        .expect("separate delivery arm produces marker-backed PCP");
    let ClosureState::Owed {
        edge: StoredEdge::ParticipantCursorProgress(marker_progress),
        ..
    } = delivered
    else {
        panic!("marker delivery must produce PCP")
    };
    let CursorFateSuccessor::DetachedCredentialRecovery(dcr) = marker_progress
        .binding_fate(debt_two, Event::binding_fate_observed(P1, e5, h - 3))
        .expect("delivered-marker fate must produce DCR")
    else {
        panic!("delivered marker cannot produce DMR")
    };
    let (unused_owner, fenced) = mint_fenced_attach_with_owner(
        dcr,
        debt_two,
        Event::fenced_recovery_committed(P1, h + 1, e5, e6, h + 1),
        DebtCompletion::clear(),
    )
    .expect("V56-C owner mint consumes the delivery-win marker");
    drop(unused_owner);
    assert_eq!(fenced.new_binding_epoch(), e6);
    let dcr_leave_claim = dcr
        .validate_leave_claim(P1, uniform(1), uniform(2), 1)
        .expect("DCR also reserves detached Leave as its other successor");
    assert_eq!(
        dcr.detached_leave(
            debt_two,
            Event::detached_leave_committed(P1, h + 3),
            dcr_leave_claim,
            DebtCompletion::clear(),
        )
        .expect("DCR Leave also clears at full-K equality"),
        ClosureState::Clear
    );
    assert_eq!(
        marker
            .leave(
                debt_two,
                Event::live_leave_committed(P1, e5, h + 1),
                DebtCompletion::clear(),
            )
            .expect("ordinary-Q live Leave cancels or consumes the marker path"),
        ClosureState::Clear
    );

    let floor = floor_transition(u128::from(h - 3), None, h + 3, h, u128::from(h + 1));
    assert_eq!(floor.preferred_floor, u128::from(h + 1));
    assert_eq!(floor.resulting_floor, u128::from(h + 1));
    assert!(no_edge_legal(
        WideResourceVector::default(),
        wide_uniform(4),
        uniform(2),
        uniform(2),
        uniform(8),
    ));

    // Participant-scoped replacement for the frozen base occurrence groups.
    let mut progress = NonzeroDebtCursorEpisode::new(
        C56S,
        debt_two,
        h,
        h + 2,
        u128::from(h - 3),
        u128::from(h - 3),
        vec![BoundParticipantCursor::new(P1, e5, h - 5)],
    )
    .expect("successful arm has one live participant-scoped cursor domain");
    let ack = ParticipantAck {
        conversation_id: C56S,
        participant_id: P1,
        capability_generation: generation(5),
        through_seq: h + 1,
    };
    assert!(matches!(
        progress
            .acknowledge(P1, e5, &ack, h + 2)
            .expect("continuous exact suffix is available"),
        liminal_protocol::lifecycle::CumulativeAckOutcome::Committed(_)
    ));
    assert_eq!(
        progress.facts().get(CursorProgressKey {
            participant_index: P1,
            boundary: h + 1,
        }),
        Some(CursorProgressFact::Consumed)
    );
    assert!(
        !progress
            .encode()
            .expect("participant facts serialize")
            .is_empty()
    );
}
