#![allow(clippy::expect_used)]

use alloc::{vec, vec::Vec};

use crate::outcome::CandidatePhase;
use crate::wire::{
    AttachSecret, BindingEpoch, ConnectionIncarnation, EnrollmentRequest, EnrollmentToken,
    Generation,
};

use super::{
    AllocatedParticipantSlot, AttachedRecordPosition, BindingState, EnrollmentCommitError,
    EnrollmentCommitParameters, EnrollmentFingerprint, ParticipantSlotAllocationError,
    ParticipantSlotAllocatorProof, commit_enrollment,
};

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

fn request() -> EnrollmentRequest {
    EnrollmentRequest {
        conversation_id: 17,
        enrollment_token: EnrollmentToken::new([0x17; 16]),
    }
}

fn parameters(proof: AllocationProof) -> EnrollmentCommitParameters<Vec<u8>, AllocationProof> {
    EnrollmentCommitParameters {
        allocated_slot: AllocatedParticipantSlot::from_allocator(proof)
            .expect("fixture slot is in range"),
        attach_secret: AttachSecret::new([0xA1; 32]),
        origin_binding_epoch: BindingEpoch::new(ConnectionIncarnation::new(9, 4), Generation::ONE),
        attached_position: AttachedRecordPosition::new(12, 33),
        receipt_expires_at: 100,
        provenance_expires_at: 200,
        enrollment_fingerprint: EnrollmentFingerprint::new(vec![1, 7, 1, 7]),
    }
}

#[test]
fn enrollment_consumes_allocated_slot_and_derives_every_fixed_value() {
    let committed = commit_enrollment(
        &request(),
        parameters(AllocationProof {
            conversation_id: 17,
            participant_index: 3,
            identity_limit: 4,
        }),
    )
    .expect("generation-one allocated enrollment commits");

    assert_eq!(committed.member.participant_id(), 3);
    assert_eq!(committed.member.conversation_id(), 17);
    assert_eq!(committed.member.generation(), Generation::ONE);
    assert_eq!(committed.member.cursor(), 0);
    assert_eq!(committed.member.latest_terminal(), None);
    assert_eq!(
        committed.member.enrollment_fingerprint().value(),
        &vec![1, 7, 1, 7]
    );
    assert!(matches!(committed.binding_state, BindingState::Bound(_)));
    assert_eq!(
        committed.attached.admission_order().candidate_phase(),
        CandidatePhase::AttachLifecycle
    );
    assert_eq!(committed.attached.delivery_seq(), 33);
    assert_eq!(committed.outcome.request_generation(), None);
    assert_eq!(committed.outcome.capability_generation(), Generation::ONE);
    assert_eq!(committed.outcome.persisted_cursor(), 0);
    assert_eq!(committed.outcome.accepted_marker_delivery_seq(), None);
    assert!(committed.binding_origin().is_unfenced());
    assert_eq!(committed.binding_origin().attached(), committed.attached);
    assert_eq!(committed.binding_origin().recovered_marker(), None);
}

#[test]
fn exhausted_sentinel_cannot_become_a_participant() {
    assert_eq!(
        AllocatedParticipantSlot::from_allocator(AllocationProof {
            conversation_id: 17,
            participant_index: 4,
            identity_limit: 4,
        }),
        Err(ParticipantSlotAllocationError::IdentityLimit)
    );
}

#[test]
fn allocation_proof_is_bound_to_the_request_conversation() {
    assert_eq!(
        commit_enrollment(
            &request(),
            parameters(AllocationProof {
                conversation_id: 18,
                participant_index: 3,
                identity_limit: 4,
            }),
        ),
        Err(EnrollmentCommitError::Conversation)
    );
}
