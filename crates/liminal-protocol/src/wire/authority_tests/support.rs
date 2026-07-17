//! Shared fixtures for the request-bound response-authority matrix tests.
//!
//! Every fixture is envelope-correct for the conversation-7/participant-3
//! request family asserted by the per-request tests in the parent module.
//! Payloads whose production mint is a shared selector (order/sequence
//! allocators, closure selector, binding lookup) are built here with the
//! exact envelope arm of the request under test; the terminalized detach
//! cell is minted through the lifecycle's sole state-derived constructor
//! path, never hand-assembled.

use crate::algebra::{ResourceVector, WideResourceVector};
use crate::lifecycle::{
    ActiveBinding, AttachCommitParameters, AttachSecretProof, AttachedRecordPosition, BindingState,
    ClosureState, CommittedBindingTerminalPosition, DetachCell, EnrollmentFingerprint, LiveMember,
    LiveMemberRestore, commit_attach, commit_detach,
};
use crate::wire::{
    AttachAttemptToken, AttachBound, AttachEnvelope, AttachMarkerProof, AttachSecret, BindingEpoch,
    BindingStateView, ClientDiscriminant, ClosureCheckedEnvelope, ClosureRefusalReason,
    ClosureSnapshot, ConnectionIncarnation, ConversationOrderExhausted,
    ConversationSequenceExhausted, CredentialAttachRequest, DetachAttemptToken, DetachEnvelope,
    DetachRequest, EnrollBound, EnrollmentEnvelope, EnrollmentToken, Generation, LeaveAttemptToken,
    LeaveEnvelope, MarkerAckEnvelope, MarkerAckProof, MarkerClosureCapacityExceeded,
    OrderAllocatingEnvelope, ParticipantAckEnvelope, RecordAdmissionEnvelope, RepaymentEdge,
    SequenceAllocatingEnvelope, SequenceBudget, ServerDiscriminant, ServerValue,
    TerminalizedDetachCell,
};

pub(super) fn generation(value: u64) -> Generation {
    Generation::new(value).expect("test generation is nonzero")
}

pub(super) fn epoch(generation_value: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(1, 1),
        generation(generation_value),
    )
}

pub(super) fn enrollment_envelope() -> EnrollmentEnvelope {
    EnrollmentEnvelope {
        conversation_id: 7,
        enrollment_token: EnrollmentToken::new([1; 16]),
    }
}

pub(super) fn attach_envelope() -> AttachEnvelope {
    AttachEnvelope {
        conversation_id: 7,
        participant_id: 3,
        capability_generation: generation(2),
        attach_attempt_token: AttachAttemptToken::new([2; 16]),
        accept_marker_delivery_seq: None,
    }
}

pub(super) fn detach_envelope() -> DetachEnvelope {
    DetachEnvelope {
        conversation_id: 7,
        participant_id: 3,
        capability_generation: generation(2),
        detach_attempt_token: DetachAttemptToken::new([3; 16]),
    }
}

pub(super) fn participant_ack_envelope() -> ParticipantAckEnvelope {
    ParticipantAckEnvelope {
        conversation_id: 7,
        participant_id: 3,
        capability_generation: generation(2),
        through_seq: 9,
    }
}

pub(super) fn leave_envelope() -> LeaveEnvelope {
    LeaveEnvelope {
        conversation_id: 7,
        participant_id: 3,
        capability_generation: generation(2),
        leave_attempt_token: LeaveAttemptToken::new([4; 16]),
    }
}

pub(super) fn marker_ack_envelope() -> MarkerAckEnvelope {
    MarkerAckEnvelope {
        conversation_id: 7,
        participant_id: 3,
        capability_generation: generation(2),
        marker_delivery_seq: 11,
    }
}

pub(super) fn record_envelope() -> RecordAdmissionEnvelope {
    RecordAdmissionEnvelope {
        conversation_id: 7,
        participant_id: 3,
        capability_generation: generation(2),
    }
}

pub(super) fn enroll_bound() -> EnrollBound {
    EnrollBound::new(
        7,
        EnrollmentToken::new([1; 16]),
        3,
        AttachSecret::new([5; 32]),
        epoch(1),
        1_000,
        2_000,
    )
    .expect("generation-1 epoch builds an enrollment receipt")
}

pub(super) fn attach_bound() -> AttachBound {
    AttachBound::ordinary(
        7,
        AttachAttemptToken::new([2; 16]),
        3,
        generation(1),
        AttachSecret::new([6; 32]),
        epoch(2),
        0,
        1_000,
        2_000,
    )
    .expect("successor generation builds an attach receipt")
}

pub(super) fn closure_snapshot() -> ClosureSnapshot {
    ClosureSnapshot {
        marker_capacity_credits: 0,
        marker_anchors: 0,
        entry_debt: 0,
        byte_debt: 0,
        repayment_edge: RepaymentEdge::None,
        edge_sequence_claims: 0,
        edge_order_position_claims: 0,
        edge_k_remaining: ResourceVector::new(0, 0),
        k_headroom: WideResourceVector::new(1, 1),
        episode_churn_used: 0,
        delta_cycles: 0,
        episode_churn_limit: 2,
    }
}

pub(super) fn attach_marker_proof() -> AttachMarkerProof {
    AttachMarkerProof {
        conversation_id: 7,
        token: AttachAttemptToken::new([2; 16]),
        participant_id: 3,
        capability_generation: generation(2),
        requested_marker_delivery_seq: 11,
    }
}

pub(super) fn marker_ack_proof() -> MarkerAckProof {
    MarkerAckProof {
        conversation_id: 7,
        participant_id: 3,
        capability_generation: generation(2),
        requested_marker_delivery_seq: 11,
    }
}

pub(super) fn sequence_budget() -> SequenceBudget {
    SequenceBudget {
        high_watermark: 5,
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

/// Canonical order-exhaustion payload as the shared order allocator mints it
/// for the given envelope arm (register row 5644).
pub(super) fn order_exhausted(request: OrderAllocatingEnvelope) -> ConversationOrderExhausted {
    ConversationOrderExhausted::new(request, 9, 1, 4, 0, 4)
}

/// Canonical sequence-exhaustion payload as the shared sequence allocator
/// mints it for the given envelope arm (register rows 5657, 5686).
pub(super) fn sequence_exhausted(
    request: SequenceAllocatingEnvelope,
) -> ConversationSequenceExhausted {
    ConversationSequenceExhausted {
        request,
        sequence_budget: sequence_budget(),
    }
}

/// Closure-capacity refusal as the shared remaining-closure selector mints it
/// for the given envelope arm (register rows 5649, 5686).
pub(super) fn closure_capacity_exceeded(
    request: ClosureCheckedEnvelope,
) -> MarkerClosureCapacityExceeded {
    MarkerClosureCapacityExceeded {
        request,
        snapshot: closure_snapshot(),
        reason: ClosureRefusalReason::DeliveredMarkerAwaitingAck,
    }
}

/// Mints the terminalized detach cell for `detach_envelope`'s request through
/// the lifecycle's sole state-derived constructor path (register row 5671):
/// detach commit, successor-attach terminalization (`Committed` to
/// `Terminalized`, extraction-brief Fix 1), then exact-token replay
/// verification.
pub(super) fn terminalized_detach_cell() -> TerminalizedDetachCell {
    let request = DetachRequest {
        conversation_id: 7,
        participant_id: 3,
        capability_generation: generation(2),
        detach_attempt_token: DetachAttemptToken::new([3; 16]),
    };
    let verifier = [0xA5; 32];
    let member: LiveMember<[u8; 32]> = LiveMember::restore(LiveMemberRestore {
        participant_id: 3,
        conversation_id: 7,
        generation: generation(2),
        attach_secret: AttachSecret::new([5; 32]),
        cursor: 0,
        enrollment_fingerprint: EnrollmentFingerprint::new([1; 32]),
        latest_terminal: None,
    })
    .expect("fixture membership has no inconsistent retained terminal");
    let binding = ActiveBinding {
        participant_id: 3,
        conversation_id: 7,
        binding_epoch: epoch(2),
    };
    let verified_detach = binding
        .verify_detach_request(request.clone(), verifier)
        .expect("detach request matches the active binding");
    let (member, _, _, committed, _) = commit_detach(
        member,
        verified_detach,
        DetachCell::default(),
        CommittedBindingTerminalPosition::new(1, 21),
    )
    .expect("empty cell accepts detach")
    .into_parts();
    let successor_epoch = BindingEpoch::new(ConnectionIncarnation::new(1, 2), generation(3));
    let attach_request = CredentialAttachRequest {
        conversation_id: 7,
        participant_id: 3,
        capability_generation: generation(2),
        attach_secret: member.attach_secret(),
        attach_attempt_token: AttachAttemptToken::new([2; 16]),
        accept_marker_delivery_seq: None,
    };
    let verified_attach = member
        .verify_detached_attach(
            BindingState::Detached,
            ClosureState::Clear
                .ordinary_detached_attach_admission()
                .expect("clear state admits ordinary attach"),
            attach_request,
            AttachSecretProof::Verified,
            AttachCommitParameters {
                binding: ActiveBinding {
                    participant_id: 3,
                    conversation_id: 7,
                    binding_epoch: successor_epoch,
                },
                attach_secret: AttachSecret::new([6; 32]),
                attached_position: AttachedRecordPosition::new(2, 22),
                receipt_expires_at: 1_000,
                provenance_expires_at: 2_000,
            },
        )
        .expect("detached attach authority is current");
    let attach = commit_attach(verified_attach, DetachCell::Committed(committed))
        .expect("verified attach terminalizes the old detach cell");
    let terminalized = attach
        .detach_cell
        .into_terminalized()
        .expect("attach must produce the terminalized variant");
    terminalized
        .verify_exact(&request, verifier)
        .expect("stored detach request replays exactly")
        .outcome(
            7,
            generation(3),
            BindingStateView::Bound {
                current_binding_epoch: successor_epoch,
            },
        )
}

pub(super) fn assert_bound(
    value: &ServerValue,
    expected_request: ClientDiscriminant,
    expected_value: ServerDiscriminant,
) {
    assert_eq!(
        value.originating_request(),
        Some(expected_request),
        "constructed {:?} must echo its bound originating request",
        value.discriminant(),
    );
    assert_eq!(
        value.discriminant(),
        expected_value,
        "constructor must select its register-mandated wire variant",
    );
}
