//! Producer-binding regressions for the durable operation payloads.
//!
//! Every payload's public producer must consume the crate's own sealed commit
//! value for that exact operation kind, so each test here drives a real
//! lifecycle commit and proves the recorded fact repeats that commit — and
//! that an incongruent pairing is refused rather than recorded.

#![allow(clippy::expect_used, clippy::panic)]

use alloc::string::String;
use alloc::{vec, vec::Vec};

use crate::wire::{
    AttachAttemptToken, AttachSecret, BindingEpoch, ConnectionIncarnation, CredentialAttachRequest,
    DetachAttemptToken, DetachRequest, EnrollmentRequest, EnrollmentToken, Generation,
    LeaveAttemptToken, LeaveRequest,
};

use super::aggregate_commit_tests::{nonzero_ack_commit, ordinary_fate, recovered_fate};
use super::operation_event::{
    AttachedOperation, BindingFateOperation, DetachedOperation, EnrolledOperation, LeftOperation,
    NonzeroDebtAckOperation,
};
use super::test_support::settled_leave_authority;
use super::{
    ActiveBinding, AllocatedParticipantSlot, AttachCommit, AttachCommitParameters,
    AttachSecretProof, AttachedRecordPosition, BindingState, CommittedBindingTerminalPosition,
    CommittedDetachTransition, DetachCell, EnrollmentCommit, EnrollmentCommitParameters,
    EnrollmentFingerprint, IdentityState, LeaveCommit, LeaveCommitParameters, LeaveFingerprint,
    LiveMember, LiveMemberRestore, ParticipantSlotAllocatorProof, RetiredIdentity, commit_attach,
    commit_detach, commit_enrollment, commit_leave,
};

fn generation(value: u64) -> Generation {
    Generation::new(value).expect("test generations are nonzero")
}

fn epoch(generation_value: u64, connection_ordinal: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(8, connection_ordinal),
        generation(generation_value),
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AllocationProof {
    conversation_id: u64,
    participant_index: u64,
    identity_limit: u64,
}

impl ParticipantSlotAllocatorProof for AllocationProof {
    fn conversation_id(&self) -> u64 {
        self.conversation_id
    }

    fn participant_index(&self) -> u64 {
        self.participant_index
    }

    fn identity_limit(&self) -> u64 {
        self.identity_limit
    }
}

pub(super) fn enrollment_commit() -> EnrollmentCommit<Vec<u8>> {
    commit_enrollment(
        &EnrollmentRequest {
            conversation_id: 17,
            enrollment_token: EnrollmentToken::new([0x17; 16]),
        },
        EnrollmentCommitParameters {
            allocated_slot: AllocatedParticipantSlot::from_allocator(AllocationProof {
                conversation_id: 17,
                participant_index: 3,
                identity_limit: 4,
            })
            .expect("fixture slot is in range"),
            attach_secret: AttachSecret::new([0xA1; 32]),
            origin_binding_epoch: BindingEpoch::new(
                ConnectionIncarnation::new(9, 4),
                Generation::ONE,
            ),
            attached_position: AttachedRecordPosition::new(12, 33),
            receipt_expires_at: 100,
            provenance_expires_at: 200,
            enrollment_fingerprint: EnrollmentFingerprint::new(vec![1, 7, 1, 7]),
        },
    )
    .expect("generation-one allocated enrollment commits")
}

fn attach_member() -> LiveMember<Vec<u8>> {
    LiveMember::restore(LiveMemberRestore {
        participant_id: 3,
        conversation_id: 29,
        generation: generation(4),
        attach_secret: AttachSecret::new([0x44; 32]),
        cursor: 5,
        enrollment_fingerprint: EnrollmentFingerprint::new(vec![3, 29]),
        latest_terminal: None,
    })
    .expect("fixture membership is internally consistent")
}

pub(super) fn superseding_attach_commit() -> AttachCommit<Vec<u8>, [u8; 32]> {
    let old_binding = ActiveBinding {
        participant_id: 3,
        conversation_id: 29,
        binding_epoch: epoch(4, 11),
    };
    let verified = attach_member()
        .verify_superseding_attach(
            old_binding,
            CredentialAttachRequest {
                conversation_id: 29,
                participant_id: 3,
                capability_generation: generation(4),
                attach_secret: AttachSecret::new([0x44; 32]),
                attach_attempt_token: AttachAttemptToken::new([0xA4; 16]),
                accept_marker_delivery_seq: None,
            },
            AttachSecretProof::Verified,
            CommittedBindingTerminalPosition::new(9, 14),
            AttachCommitParameters {
                binding: ActiveBinding {
                    participant_id: 3,
                    conversation_id: 29,
                    binding_epoch: epoch(5, 12),
                },
                attach_secret: AttachSecret::new([0x55; 32]),
                attached_position: AttachedRecordPosition::new(9, 15),
                receipt_expires_at: 100,
                provenance_expires_at: 200,
            },
        )
        .expect("same-participant current binding may be superseded");
    commit_attach(verified, DetachCell::<[u8; 32]>::default())
        .expect("verified supersession commits")
}

pub(super) fn detach_transition(
    participant_id: u64,
    token_byte: u8,
    delivery_seq: u64,
) -> CommittedDetachTransition<Vec<u8>, [u8; 32]> {
    let binding_epoch = epoch(4, 11);
    let binding = ActiveBinding {
        participant_id,
        conversation_id: 29,
        binding_epoch,
    };
    let member = LiveMember::restore(LiveMemberRestore {
        participant_id,
        conversation_id: 29,
        generation: generation(4),
        attach_secret: AttachSecret::new([0x44; 32]),
        cursor: 5,
        enrollment_fingerprint: EnrollmentFingerprint::new(vec![3, 29]),
        latest_terminal: None,
    })
    .expect("fixture membership is internally consistent");
    let verified = binding
        .verify_detach_request(
            DetachRequest {
                conversation_id: 29,
                participant_id,
                capability_generation: generation(4),
                detach_attempt_token: DetachAttemptToken::new([token_byte; 16]),
            },
            [0xA6; 32],
        )
        .expect("request matches the active binding");
    commit_detach(
        member,
        verified,
        DetachCell::default(),
        CommittedBindingTerminalPosition::new(9, delivery_seq),
    )
    .expect("empty cell accepts detach")
}

fn leave_member() -> LiveMember<Vec<u8>> {
    LiveMember::restore(LiveMemberRestore {
        participant_id: 7,
        conversation_id: 11,
        generation: generation(3),
        attach_secret: AttachSecret::new([0xA3; 32]),
        cursor: 5,
        enrollment_fingerprint: EnrollmentFingerprint::new(vec![0xE1, 0xE2, 0xE3]),
        latest_terminal: None,
    })
    .expect("fixture history belongs to the member")
}

pub(super) fn bound_leave_commit() -> LeaveCommit<Vec<u8>, Vec<u8>, String> {
    let member = leave_member();
    let binding = ActiveBinding {
        participant_id: 7,
        conversation_id: 11,
        binding_epoch: BindingEpoch::new(ConnectionIncarnation::new(70, 1), generation(3)),
    };
    let binding_state = BindingState::Bound(binding);
    let authority = settled_leave_authority(&member, binding_state, 10, 6);
    let verified = member
        .verify_leave_request(
            &LeaveRequest {
                conversation_id: 11,
                participant_id: 7,
                capability_generation: generation(3),
                attach_secret: AttachSecret::new([0xA3; 32]),
                leave_attempt_token: LeaveAttemptToken::new([0x51; 16]),
            },
            AttachSecretProof::Verified,
            vec![0xC1, 0xC2],
            LeaveFingerprint::new(String::from("canonical-leave")),
        )
        .expect("fixture request has current authority");
    commit_leave(
        member,
        binding_state,
        DetachCell::<[u8; 32]>::default(),
        verified,
        authority,
        LeaveCommitParameters {
            left_delivery_seq: 6,
        },
    )
    .expect("bound Leave commits")
}

fn retired_identity() -> RetiredIdentity<Vec<u8>, Vec<u8>, String> {
    let (state, _) = bound_leave_commit().into_parts();
    match state {
        IdentityState::Retired(retired) => retired,
        IdentityState::Live(_) => panic!("Leave must return only a tombstone"),
    }
}

#[test]
fn enrolled_operation_repeats_the_enrollment_commits_attached_record() {
    let commit = enrollment_commit();
    let operation = EnrolledOperation::new(&commit);
    assert_eq!(operation.conversation_id(), 17);
    assert_eq!(operation.participant_id(), 3);
    assert_eq!(operation.binding_epoch(), commit.attached.binding_epoch());
    assert_eq!(
        operation.binding_epoch().capability_generation,
        Generation::ONE
    );
    assert_eq!(
        operation.attached_transaction_order(),
        commit.attached.admission_order().transaction_order()
    );
    assert_eq!(operation.attached_delivery_seq(), 33);
}

#[test]
fn attached_operation_repeats_the_attach_commits_attached_record() {
    let commit = superseding_attach_commit();
    let operation = AttachedOperation::new(&commit);
    assert_eq!(operation.conversation_id(), 29);
    assert_eq!(operation.participant_id(), 3);
    assert_eq!(operation.binding_epoch(), commit.attached.binding_epoch());
    assert_eq!(operation.binding_epoch(), epoch(5, 12));
    assert_eq!(
        operation.attached_transaction_order(),
        commit.attached.admission_order().transaction_order()
    );
    assert_eq!(operation.attached_delivery_seq(), 15);
}

#[test]
fn detached_operation_records_the_committed_cells_own_attempt_token() {
    let transition = detach_transition(3, 0xD3, 44);
    let operation = DetachedOperation::new(&transition)
        .expect("a commit's own terminal/cell pair is congruent");
    assert_eq!(
        operation.detach_attempt_token(),
        DetachAttemptToken::new([0xD3; 16])
    );
    assert_eq!(operation.conversation_id(), 29);
    assert_eq!(operation.participant_id(), 3);
    assert_eq!(operation.committed_binding_epoch(), epoch(4, 11));
    assert_eq!(operation.detached_delivery_seq(), 44);
    assert_eq!(
        operation.detached_transaction_order(),
        transition.terminal().admission_order().transaction_order()
    );
}

// The mispairing refusals that lived here (a cell from another detach, a
// supersession terminal from an attach commit, a colliding terminal/cell
// pair from two different conversations) are now unrepresentable: the only
// producer consumes the whole sealed `CommittedDetachTransition`, so no
// caller can present a terminal and a cell that were not born in one detach
// commit. The `compile_fail` doctests on `DetachedOperation::new` prove both
// the split-pair and the standalone-terminal presentations no longer
// compile.

#[test]
fn binding_fate_operation_repeats_the_ordinary_fates_committed_facts() {
    let fate = ordinary_fate();
    let operation = BindingFateOperation::from_ordinary(&fate);
    assert_eq!(operation.conversation_id(), 29);
    assert_eq!(operation.participant_id(), 3);
    assert_eq!(operation.last_dead_binding_epoch(), epoch(5, 12));
    assert_eq!(
        operation.last_dead_binding_epoch(),
        fate.last_dead_binding_epoch()
    );
    assert_eq!(operation.resulting_floor(), 9);
}

#[test]
fn binding_fate_operation_repeats_the_recovered_fates_committed_facts() {
    let fate = recovered_fate();
    let operation = BindingFateOperation::from_recovered(&fate);
    assert_eq!(operation.conversation_id(), 1);
    assert_eq!(operation.participant_id(), 4);
    assert_eq!(
        operation.last_dead_binding_epoch(),
        BindingEpoch::new(ConnectionIncarnation::new(1, 3), generation(3))
    );
    assert_eq!(
        operation.last_dead_binding_epoch(),
        fate.last_dead_binding_epoch()
    );
    assert_eq!(operation.resulting_floor(), 15);
}

#[test]
fn nonzero_debt_ack_operation_repeats_the_ack_commits_request() {
    let commit = nonzero_ack_commit();
    let request = commit.outcome().request();
    let operation = NonzeroDebtAckOperation::new(&commit);
    assert_eq!(operation.conversation_id(), 54);
    assert_eq!(operation.participant_id(), 0);
    assert_eq!(operation.capability_generation(), Generation::ONE);
    assert_eq!(operation.through_seq(), 1);
    assert_eq!(operation.conversation_id(), request.conversation_id);
    assert_eq!(operation.participant_id(), request.participant_id);
    assert_eq!(
        operation.capability_generation(),
        request.capability_generation
    );
    assert_eq!(operation.through_seq(), request.through_seq);
}

#[test]
fn left_operation_repeats_the_retired_identitys_committed_result() {
    let retired = retired_identity();
    let operation = LeftOperation::new(&retired);
    assert_eq!(operation.committed(), retired.committed_result());
    assert_eq!(
        operation.left_transaction_order(),
        retired.left_admission_order().transaction_order()
    );
    assert_eq!(operation.left_transaction_order(), 10);
    assert_eq!(operation.committed().conversation_id(), 11);
    assert_eq!(operation.committed().participant_id(), 7);
    assert_eq!(operation.committed().left_delivery_seq(), 6);
}
