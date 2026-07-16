use crate::outcome::CandidatePhase;
use crate::wire::{
    AttachSecret, BindingEpoch, ConversationId, DeliverySeq, EnrollBound, EnrollmentRequest,
    ParticipantId, ParticipantIndex, TransactionOrder,
};

use super::{ActiveBinding, AdmissionOrder, BindingState, EnrollmentFingerprint, LiveMember};

/// Consuming allocator proof for one permanent participant reservation slot.
///
/// Implementations are supplied by the serialized identity allocator; the
/// proof's existence attests that the returned slot was reserved exactly once.
pub trait ParticipantSlotAllocatorProof {
    /// Conversation whose identity allocator produced the slot.
    fn conversation_id(&self) -> ConversationId;

    /// Permanent participant index, also the participant id in protocol v1.
    fn participant_index(&self) -> ParticipantIndex;

    /// Configured half-open identity limit `I` used by the allocation.
    fn identity_limit(&self) -> u64;
}

/// Opaque, checked participant-slot allocation consumed by enrollment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AllocatedParticipantSlot<P> {
    allocator_proof: P,
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    identity_limit: u64,
}

/// Invalid participant-slot allocation proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantSlotAllocationError {
    /// Allocated index is outside the half-open `0..<I` domain.
    IdentityLimit,
}

impl<P: ParticipantSlotAllocatorProof> AllocatedParticipantSlot<P> {
    /// Validates and binds one consuming-layer allocator proof.
    ///
    /// # Errors
    ///
    /// Returns [`ParticipantSlotAllocationError::IdentityLimit`] when the
    /// allocator reports the exhausted sentinel or an out-of-range index.
    pub fn from_allocator(allocator_proof: P) -> Result<Self, ParticipantSlotAllocationError> {
        let participant_id = allocator_proof.participant_index();
        let identity_limit = allocator_proof.identity_limit();
        if participant_id >= identity_limit {
            return Err(ParticipantSlotAllocationError::IdentityLimit);
        }
        Ok(Self {
            conversation_id: allocator_proof.conversation_id(),
            participant_id,
            identity_limit,
            allocator_proof,
        })
    }

    /// Returns the allocator-bound conversation.
    #[must_use]
    pub const fn conversation_id(&self) -> ConversationId {
        self.conversation_id
    }

    /// Returns the permanent participant id/index.
    #[must_use]
    pub const fn participant_id(&self) -> ParticipantId {
        self.participant_id
    }

    /// Returns the checked half-open identity domain used by the allocator.
    #[must_use]
    pub const fn identity_limit(&self) -> u64 {
        self.identity_limit
    }
}

/// Assigned ordering and delivery position of one `Attached` lifecycle record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AttachedRecordPosition {
    transaction_order: TransactionOrder,
    delivery_seq: DeliverySeq,
}

impl AttachedRecordPosition {
    /// Creates the committed position allocated by the conversation lane.
    #[must_use]
    pub const fn new(transaction_order: TransactionOrder, delivery_seq: DeliverySeq) -> Self {
        Self {
            transaction_order,
            delivery_seq,
        }
    }

    /// Returns the assigned transaction-order major.
    #[must_use]
    pub const fn transaction_order(self) -> TransactionOrder {
        self.transaction_order
    }

    /// Returns the assigned delivery sequence.
    #[must_use]
    pub const fn delivery_seq(self) -> DeliverySeq {
        self.delivery_seq
    }
}

/// Exact committed `Attached` lifecycle fact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AttachedLifecycleRecord {
    participant_id: ParticipantId,
    conversation_id: ConversationId,
    binding_epoch: BindingEpoch,
    admission_order: AdmissionOrder,
    delivery_seq: DeliverySeq,
}

impl AttachedLifecycleRecord {
    pub(crate) const fn from_binding(
        binding: ActiveBinding,
        position: AttachedRecordPosition,
    ) -> Self {
        Self {
            participant_id: binding.participant_id,
            conversation_id: binding.conversation_id,
            binding_epoch: binding.binding_epoch,
            admission_order: AdmissionOrder::new(
                position.transaction_order,
                CandidatePhase::AttachLifecycle,
                binding.participant_id,
            ),
            delivery_seq: position.delivery_seq,
        }
    }

    /// Returns the permanent participant id/index.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        self.participant_id
    }

    /// Returns the owning conversation.
    #[must_use]
    pub const fn conversation_id(self) -> ConversationId {
        self.conversation_id
    }

    /// Returns the newly attached binding epoch.
    #[must_use]
    pub const fn binding_epoch(self) -> BindingEpoch {
        self.binding_epoch
    }

    /// Returns the canonical phase-2 admission order.
    #[must_use]
    pub const fn admission_order(self) -> AdmissionOrder {
        self.admission_order
    }

    /// Returns the committed record sequence.
    #[must_use]
    pub const fn delivery_seq(self) -> DeliverySeq {
        self.delivery_seq
    }
}

/// Values allocated by one successful enrollment transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnrollmentCommitParameters<F, P> {
    /// Consumed permanent identity reservation.
    pub allocated_slot: AllocatedParticipantSlot<P>,
    /// Newly minted generation-one attach secret.
    pub attach_secret: AttachSecret,
    /// New generation-one binding epoch.
    pub origin_binding_epoch: BindingEpoch,
    /// Assigned `Attached` record position.
    pub attached_position: AttachedRecordPosition,
    /// Live receipt deadline.
    pub receipt_expires_at: u128,
    /// Provenance deadline.
    pub provenance_expires_at: u128,
    /// Permanent enrollment-token mapping fingerprint.
    pub enrollment_fingerprint: EnrollmentFingerprint<F>,
}

/// Complete atomic enrollment result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnrollmentCommit<F> {
    /// New durable generation-one membership.
    pub member: LiveMember<F>,
    /// New authoritative binding slot.
    pub binding_state: BindingState,
    /// Exact committed `Attached` lifecycle fact.
    pub attached: AttachedLifecycleRecord,
    /// Exact canonical enrollment receipt.
    pub outcome: EnrollBound,
}

/// Failure while committing a previously allocated enrollment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnrollmentCommitError {
    /// Request and allocated slot name different conversations.
    Conversation,
    /// Allocated binding epoch is not generation one.
    BindingGeneration,
    /// Canonical wire receipt rejected the supplied epoch.
    ReceiptInvariant,
}

/// Atomically creates membership, binding, `Attached`, and canonical receipt.
///
/// # Errors
///
/// Returns [`EnrollmentCommitError`] when the consumed allocator proof and
/// request disagree or the result binding is not generation one.
pub fn commit_enrollment<F, P>(
    request: &EnrollmentRequest,
    parameters: EnrollmentCommitParameters<F, P>,
) -> Result<EnrollmentCommit<F>, EnrollmentCommitError> {
    if request.conversation_id != parameters.allocated_slot.conversation_id {
        return Err(EnrollmentCommitError::Conversation);
    }
    if parameters.origin_binding_epoch.capability_generation != crate::wire::Generation::ONE {
        return Err(EnrollmentCommitError::BindingGeneration);
    }
    let participant_id = parameters.allocated_slot.participant_id;
    let binding = ActiveBinding {
        participant_id,
        conversation_id: request.conversation_id,
        binding_epoch: parameters.origin_binding_epoch,
    };
    let attached = AttachedLifecycleRecord::from_binding(binding, parameters.attached_position);
    let Some(outcome) = EnrollBound::new(
        request.conversation_id,
        request.enrollment_token,
        participant_id,
        parameters.attach_secret,
        parameters.origin_binding_epoch,
        parameters.receipt_expires_at,
        parameters.provenance_expires_at,
    ) else {
        return Err(EnrollmentCommitError::ReceiptInvariant);
    };
    let member = LiveMember::from_enrollment(
        participant_id,
        request.conversation_id,
        parameters.attach_secret,
        parameters.enrollment_fingerprint,
    );
    let _consumed_allocator_proof = parameters.allocated_slot.allocator_proof;
    Ok(EnrollmentCommit {
        member,
        binding_state: BindingState::Bound(binding),
        attached,
        outcome,
    })
}
