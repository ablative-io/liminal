//! Append-only participant binding-fate repository.
//!
//! Durable entries contain the inputs to protocol transitions rather than a
//! parallel server-owned lifecycle model. Recovery replays enrollment and one
//! binding fate exclusively through `liminal-protocol`; a committed fate and
//! its member terminal-history update are installed by one durable append.

use std::sync::Arc;

use liminal::durability::{DurabilityError, DurableStore};
use liminal_protocol::lifecycle::{
    AllocatedParticipantSlot, AttachedRecordPosition, BindingState, BindingTerminalDisposition,
    CommittedBindingTerminalPosition, DiedBindingTransition, EnrollmentCommitError,
    EnrollmentCommitParameters, EnrollmentFingerprint, LiveMember, MembershipInvariantError,
    ParticipantSlotAllocationError, ParticipantSlotAllocatorProof, PendingBindingTerminalPosition,
    commit_enrollment,
};
use liminal_protocol::wire::{
    AttachSecret, BindingEpoch, ConnectionIncarnation, DeliverySeq, EnrollBound, EnrollmentRequest,
    EnrollmentToken, Generation, ParticipantId, TransactionOrder,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const EVENT_SCHEMA_VERSION: u8 = 1;
const READ_BATCH_SIZE: usize = 64;
const STREAM_PREFIX: &str = "liminal:participant-crash:";

/// Fixed server-side enrollment-token mapping digest retained by live membership.
pub type CrashEnrollmentDigest = [u8; 32];

/// Server allocations committed with initial enrollment in the crash stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CrashEnrollmentAllocation {
    /// Newly minted generation-one attach secret.
    pub attach_secret: AttachSecret,
    /// New generation-one binding epoch.
    pub origin_binding_epoch: BindingEpoch,
    /// Serialized conversation order of the initial `Attached` record.
    pub attached_transaction_order: TransactionOrder,
    /// Delivery sequence of the initial `Attached` record.
    pub attached_delivery_seq: DeliverySeq,
    /// Secret-bearing enrollment receipt deadline.
    pub receipt_expires_at: u128,
    /// Non-secret enrollment provenance deadline.
    pub provenance_expires_at: u128,
    /// Permanent enrollment-token mapping digest.
    pub enrollment_fingerprint: CrashEnrollmentDigest,
}

/// Unexpected binding-fate class selected by the server's observed event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantCrashCause {
    /// TCP, keepalive, read, or write connection loss.
    ConnectionLost,
    /// Trapped linked exit or known forced participant termination.
    ProcessKilled,
    /// Terminating decode or participant protocol-state refusal.
    ProtocolError,
    /// A prior server incarnation left this binding durably active.
    UncleanServerRestart,
}

/// Durable placement selected for the binding-terminal record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrashTerminalDisposition {
    /// The `Died` terminal committed in the fate transaction.
    Committed {
        /// Serialized conversation order of the terminal record.
        transaction_order: TransactionOrder,
        /// Assigned delivery sequence of the terminal record.
        delivery_seq: DeliverySeq,
    },
    /// The exact `Died` terminal remains in the bounded pending slot.
    Pending {
        /// Serialized conversation order reserved for the terminal record.
        transaction_order: TransactionOrder,
    },
}

/// Durable participant state reconstructed from the crash stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveredCrashState {
    member: LiveMember<CrashEnrollmentDigest>,
    binding_state: BindingState,
    last_transition: Option<DiedBindingTransition>,
}

impl RecoveredCrashState {
    /// Borrows the replayed live membership and terminal history.
    #[must_use]
    pub const fn member(&self) -> &LiveMember<CrashEnrollmentDigest> {
        &self.member
    }

    /// Returns the replayed binding slot.
    #[must_use]
    pub const fn binding_state(&self) -> BindingState {
        self.binding_state
    }

    /// Returns the replayed binding fate, if one has committed or become pending.
    #[must_use]
    pub const fn last_transition(&self) -> Option<DiedBindingTransition> {
        self.last_transition
    }
}

/// Failure to persist, restore, or apply a participant crash transition.
#[derive(Debug, Error)]
pub enum ParticipantCrashRepositoryError {
    /// The underlying durable store rejected an operation.
    #[error("participant crash durable-store operation failed: {0}")]
    DurableStore(#[from] DurabilityError),
    /// A durable event could not be encoded or decoded.
    #[error("participant crash event serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    /// A durable event uses an unsupported schema version.
    #[error("unsupported participant crash event schema version {0}")]
    EventSchemaVersion(u8),
    /// A durable event carried the forbidden zero generation.
    #[error("participant crash event contains zero generation")]
    ZeroGeneration,
    /// A durable stream contained a gap, duplicate, or reordered event.
    #[error("participant crash stream expected sequence {expected}, found {actual}")]
    EventSequence {
        /// Next sequence required by replay.
        expected: u64,
        /// Sequence returned from durable storage.
        actual: u64,
    },
    /// The store returned a different assigned sequence after append.
    #[error("participant crash append expected assigned sequence {expected}, got {actual}")]
    AssignedSequence {
        /// Optimistic-concurrency sequence supplied to the store.
        expected: u64,
        /// Assigned sequence returned by the store.
        actual: u64,
    },
    /// A durable event belongs to a different conversation stream.
    #[error(
        "participant crash stream belongs to conversation {stream_conversation}, event names {event_conversation}"
    )]
    ConversationMismatch {
        /// Conversation selected by this repository.
        stream_conversation: u64,
        /// Conversation encoded in the durable event.
        event_conversation: u64,
    },
    /// Enrollment was applied to a non-empty crash stream.
    #[error("participant crash enrollment requires an empty stream")]
    EnrollmentState,
    /// A crash was applied without one current bound participant.
    #[error("participant crash transition requires one current bound participant")]
    CrashState,
    /// The consuming allocator supplied an invalid permanent participant slot.
    #[error("participant crash allocator proof failed: {0:?}")]
    ParticipantSlotAllocation(ParticipantSlotAllocationError),
    /// The protocol crate rejected enrollment inputs.
    #[error("protocol enrollment transition failed: {0:?}")]
    EnrollmentTransition(EnrollmentCommitError),
    /// The protocol crate rejected the committed terminal-history update.
    #[error("protocol member terminal-history update failed: {0:?}")]
    TerminalHistory(MembershipInvariantError),
}

/// Append-only repository for one conversation's independently durable crash fate.
///
/// A handle caches no protocol state. Every operation first replays the stream,
/// computes the candidate transition through `liminal-protocol`, and appends
/// exactly one event only after the candidate succeeds.
#[derive(Debug)]
pub struct ParticipantCrashRepository {
    store: Arc<dyn DurableStore>,
    conversation_id: u64,
    stream_key: String,
}

impl ParticipantCrashRepository {
    /// Creates a stateless repository handle over one conversation crash stream.
    #[must_use]
    pub fn new(store: Arc<dyn DurableStore>, conversation_id: u64) -> Self {
        Self {
            store,
            conversation_id,
            stream_key: format!("{STREAM_PREFIX}{conversation_id}"),
        }
    }

    /// Returns the namespaced append-only stream key.
    #[must_use]
    pub fn stream_key(&self) -> &str {
        &self.stream_key
    }

    /// Commits initial enrollment and persists its complete transition inputs.
    ///
    /// # Errors
    ///
    /// Returns [`ParticipantCrashRepositoryError`] for existing state, invalid
    /// allocation or protocol inputs, serialization failure, or store failure.
    pub async fn commit_enrollment<P>(
        &self,
        request: EnrollmentRequest,
        allocator_proof: P,
        allocation: CrashEnrollmentAllocation,
    ) -> Result<EnrollBound, ParticipantCrashRepositoryError>
    where
        P: ParticipantSlotAllocatorProof,
    {
        let event = StoredEvent::enrollment(&request, &allocator_proof, allocation);
        self.validate_event_conversation(&event.transition)?;
        let replayed = self.replay().await?;
        let (_, outcome) =
            apply_enrollment_with_proof(&replayed.state, &event.transition, allocator_proof)?;
        self.append_event(&event, replayed.next_sequence).await?;
        Ok(outcome)
    }

    /// Commits one unexpected binding fate and its exact terminal placement.
    ///
    /// A committed terminal is installed into `LiveMember` history through
    /// [`LiveMember::with_committed_terminal`] before the single event append.
    /// A pending terminal remains represented by the transition-derived private
    /// pending state and is reproduced on every recovery replay.
    ///
    /// # Errors
    ///
    /// Returns [`ParticipantCrashRepositoryError`] when no current bound state
    /// exists, protocol terminal-history validation fails, or persistence fails.
    pub async fn commit_crash(
        &self,
        cause: ParticipantCrashCause,
        disposition: CrashTerminalDisposition,
    ) -> Result<DiedBindingTransition, ParticipantCrashRepositoryError> {
        let event = StoredEvent::crash(self.conversation_id, cause, disposition);
        let replayed = self.replay().await?;
        let (_, transition) = apply_crash(replayed.state, &event.transition)?;
        self.append_event(&event, replayed.next_sequence).await?;
        Ok(transition)
    }

    /// Reconstructs the current typed state by replaying all durable transitions.
    ///
    /// # Errors
    ///
    /// Returns [`ParticipantCrashRepositoryError`] for corrupt ordering or
    /// payloads, protocol transition rejection, or durable-store failure.
    pub async fn recover(
        &self,
    ) -> Result<Option<RecoveredCrashState>, ParticipantCrashRepositoryError> {
        let replayed = self.replay().await?;
        Ok(match replayed.state {
            ReplayState::Empty => None,
            ReplayState::Live(state) => Some(*state),
        })
    }

    /// Flushes the backing store after completed crash transitions.
    ///
    /// # Errors
    ///
    /// Returns [`ParticipantCrashRepositoryError`] when the store cannot flush.
    pub async fn flush(&self) -> Result<(), ParticipantCrashRepositoryError> {
        self.store.flush().await?;
        Ok(())
    }

    async fn append_event(
        &self,
        event: &StoredEvent,
        expected_sequence: u64,
    ) -> Result<(), ParticipantCrashRepositoryError> {
        let payload = serde_json::to_vec(event)?;
        let assigned = self
            .store
            .append(&self.stream_key, payload, expected_sequence)
            .await?;
        if assigned != expected_sequence {
            return Err(ParticipantCrashRepositoryError::AssignedSequence {
                expected: expected_sequence,
                actual: assigned,
            });
        }
        Ok(())
    }

    const fn validate_event_conversation(
        &self,
        transition: &DurableTransition,
    ) -> Result<(), ParticipantCrashRepositoryError> {
        let event_conversation = transition.conversation_id();
        if event_conversation != self.conversation_id {
            return Err(ParticipantCrashRepositoryError::ConversationMismatch {
                stream_conversation: self.conversation_id,
                event_conversation,
            });
        }
        Ok(())
    }

    async fn replay(&self) -> Result<ReplayedRepository, ParticipantCrashRepositoryError> {
        let mut state = ReplayState::Empty;
        let mut next_sequence = 0_u64;
        loop {
            let entries = self
                .store
                .read_from(&self.stream_key, next_sequence, READ_BATCH_SIZE)
                .await?;
            if entries.is_empty() {
                break;
            }
            let entry_count = entries.len();
            for entry in entries {
                if entry.sequence != next_sequence {
                    return Err(ParticipantCrashRepositoryError::EventSequence {
                        expected: next_sequence,
                        actual: entry.sequence,
                    });
                }
                let event: StoredEvent = serde_json::from_slice(&entry.payload)?;
                if event.schema_version != EVENT_SCHEMA_VERSION {
                    return Err(ParticipantCrashRepositoryError::EventSchemaVersion(
                        event.schema_version,
                    ));
                }
                self.validate_event_conversation(&event.transition)?;
                state = apply_transition(state, &event.transition)?;
                next_sequence = next_sequence.checked_add(1).ok_or(
                    ParticipantCrashRepositoryError::EventSequence {
                        expected: u64::MAX,
                        actual: entry.sequence,
                    },
                )?;
            }
            if entry_count < READ_BATCH_SIZE {
                break;
            }
        }
        Ok(ReplayedRepository {
            state,
            next_sequence,
        })
    }
}

#[derive(Debug)]
struct ReplayedRepository {
    state: ReplayState,
    next_sequence: u64,
}

#[derive(Debug)]
enum ReplayState {
    Empty,
    Live(Box<RecoveredCrashState>),
}

fn apply_transition(
    state: ReplayState,
    transition: &DurableTransition,
) -> Result<ReplayState, ParticipantCrashRepositoryError> {
    match transition {
        DurableTransition::EnrollmentCommitted { allocator, .. } => {
            let proof = ReplayedParticipantSlot {
                conversation_id: allocator.conversation_id,
                participant_id: allocator.participant_id,
                identity_limit: allocator.identity_limit,
            };
            let (state, _) = apply_enrollment_with_proof(&state, transition, proof)?;
            Ok(state)
        }
        DurableTransition::CrashCommitted { .. } => {
            let (state, _) = apply_crash(state, transition)?;
            Ok(state)
        }
    }
}

fn apply_enrollment_with_proof<P>(
    state: &ReplayState,
    transition: &DurableTransition,
    allocator_proof: P,
) -> Result<(ReplayState, EnrollBound), ParticipantCrashRepositoryError>
where
    P: ParticipantSlotAllocatorProof,
{
    let ReplayState::Empty = state else {
        return Err(ParticipantCrashRepositoryError::EnrollmentState);
    };
    let DurableTransition::EnrollmentCommitted {
        request,
        allocation,
        ..
    } = transition
    else {
        return Err(ParticipantCrashRepositoryError::EnrollmentState);
    };
    let allocated_slot = AllocatedParticipantSlot::from_allocator(allocator_proof)
        .map_err(ParticipantCrashRepositoryError::ParticipantSlotAllocation)?;
    let request = request.to_enrollment_request();
    let committed = commit_enrollment(
        &request,
        EnrollmentCommitParameters {
            allocated_slot,
            attach_secret: AttachSecret::new(allocation.attach_secret),
            origin_binding_epoch: allocation.origin_binding_epoch.to_binding_epoch()?,
            attached_position: AttachedRecordPosition::new(
                allocation.attached_transaction_order,
                allocation.attached_delivery_seq,
            ),
            receipt_expires_at: allocation.receipt_expires_at.get(),
            provenance_expires_at: allocation.provenance_expires_at.get(),
            enrollment_fingerprint: EnrollmentFingerprint::new(allocation.enrollment_fingerprint),
        },
    )
    .map_err(ParticipantCrashRepositoryError::EnrollmentTransition)?;
    let outcome = committed.outcome;
    Ok((
        ReplayState::Live(Box::new(RecoveredCrashState {
            member: committed.member,
            binding_state: committed.binding_state,
            last_transition: None,
        })),
        outcome,
    ))
}

fn apply_crash(
    state: ReplayState,
    transition: &DurableTransition,
) -> Result<(ReplayState, DiedBindingTransition), ParticipantCrashRepositoryError> {
    let ReplayState::Live(state) = state else {
        return Err(ParticipantCrashRepositoryError::CrashState);
    };
    let RecoveredCrashState {
        member,
        binding_state,
        ..
    } = *state;
    let DurableTransition::CrashCommitted {
        cause, disposition, ..
    } = transition
    else {
        return Err(ParticipantCrashRepositoryError::CrashState);
    };
    let BindingState::Bound(binding) = binding_state else {
        return Err(ParticipantCrashRepositoryError::CrashState);
    };
    let transition = match cause {
        StoredCrashCause::ConnectionLost => binding.connection_lost(disposition.into_protocol()),
        StoredCrashCause::ProcessKilled => binding.process_killed(disposition.into_protocol()),
        StoredCrashCause::ProtocolError => binding.protocol_error(disposition.into_protocol()),
        StoredCrashCause::UncleanServerRestart => {
            binding.unclean_server_restart(disposition.into_protocol())
        }
    };
    let member = match transition {
        DiedBindingTransition::Committed(terminal) => member
            .with_committed_terminal(terminal.into())
            .map_err(ParticipantCrashRepositoryError::TerminalHistory)?,
        DiedBindingTransition::Pending(_) => member,
    };
    Ok((
        ReplayState::Live(Box::new(RecoveredCrashState {
            member,
            binding_state: transition.binding_state(),
            last_transition: Some(transition),
        })),
        transition,
    ))
}

#[derive(Clone, Copy, Debug)]
struct ReplayedParticipantSlot {
    conversation_id: u64,
    participant_id: ParticipantId,
    identity_limit: u64,
}

impl ParticipantSlotAllocatorProof for ReplayedParticipantSlot {
    fn conversation_id(&self) -> u64 {
        self.conversation_id
    }

    fn participant_index(&self) -> u64 {
        self.participant_id
    }

    fn identity_limit(&self) -> u64 {
        self.identity_limit
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct StoredEvent {
    schema_version: u8,
    transition: DurableTransition,
}

impl StoredEvent {
    fn enrollment<P: ParticipantSlotAllocatorProof>(
        request: &EnrollmentRequest,
        allocator: &P,
        allocation: CrashEnrollmentAllocation,
    ) -> Self {
        Self {
            schema_version: EVENT_SCHEMA_VERSION,
            transition: DurableTransition::EnrollmentCommitted {
                request: StoredEnrollmentRequest::from(request),
                allocator: StoredAllocator {
                    conversation_id: allocator.conversation_id(),
                    participant_id: allocator.participant_index(),
                    identity_limit: allocator.identity_limit(),
                },
                allocation: StoredEnrollmentAllocation::from(allocation),
            },
        }
    }

    const fn crash(
        conversation_id: u64,
        cause: ParticipantCrashCause,
        disposition: CrashTerminalDisposition,
    ) -> Self {
        Self {
            schema_version: EVENT_SCHEMA_VERSION,
            transition: DurableTransition::CrashCommitted {
                conversation_id,
                cause: StoredCrashCause::from_public(cause),
                disposition: StoredCrashDisposition::from_public(disposition),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", tag = "transition")]
enum DurableTransition {
    EnrollmentCommitted {
        request: StoredEnrollmentRequest,
        allocator: StoredAllocator,
        allocation: StoredEnrollmentAllocation,
    },
    CrashCommitted {
        conversation_id: u64,
        cause: StoredCrashCause,
        disposition: StoredCrashDisposition,
    },
}

impl DurableTransition {
    const fn conversation_id(&self) -> u64 {
        match self {
            Self::EnrollmentCommitted { request, .. } => request.conversation_id,
            Self::CrashCommitted {
                conversation_id, ..
            } => *conversation_id,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct StoredEnrollmentRequest {
    conversation_id: u64,
    token: [u8; 16],
}

impl From<&EnrollmentRequest> for StoredEnrollmentRequest {
    fn from(request: &EnrollmentRequest) -> Self {
        Self {
            conversation_id: request.conversation_id,
            token: request.enrollment_token.into_bytes(),
        }
    }
}

impl StoredEnrollmentRequest {
    const fn to_enrollment_request(self) -> EnrollmentRequest {
        EnrollmentRequest {
            conversation_id: self.conversation_id,
            enrollment_token: EnrollmentToken::new(self.token),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct StoredAllocator {
    conversation_id: u64,
    participant_id: ParticipantId,
    identity_limit: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct StoredBindingEpoch {
    server_incarnation: u64,
    connection_ordinal: u64,
    capability_generation: u64,
}

impl From<BindingEpoch> for StoredBindingEpoch {
    fn from(epoch: BindingEpoch) -> Self {
        Self {
            server_incarnation: epoch.connection_incarnation.server_incarnation,
            connection_ordinal: epoch.connection_incarnation.connection_ordinal,
            capability_generation: epoch.capability_generation.get(),
        }
    }
}

impl StoredBindingEpoch {
    fn to_binding_epoch(self) -> Result<BindingEpoch, ParticipantCrashRepositoryError> {
        Ok(BindingEpoch::new(
            ConnectionIncarnation::new(self.server_incarnation, self.connection_ordinal),
            generation(self.capability_generation)?,
        ))
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct StoredEnrollmentAllocation {
    attach_secret: [u8; 32],
    origin_binding_epoch: StoredBindingEpoch,
    attached_transaction_order: TransactionOrder,
    attached_delivery_seq: DeliverySeq,
    receipt_expires_at: StoredU128,
    provenance_expires_at: StoredU128,
    enrollment_fingerprint: CrashEnrollmentDigest,
}

impl From<CrashEnrollmentAllocation> for StoredEnrollmentAllocation {
    fn from(allocation: CrashEnrollmentAllocation) -> Self {
        Self {
            attach_secret: allocation.attach_secret.into_bytes(),
            origin_binding_epoch: allocation.origin_binding_epoch.into(),
            attached_transaction_order: allocation.attached_transaction_order,
            attached_delivery_seq: allocation.attached_delivery_seq,
            receipt_expires_at: allocation.receipt_expires_at.into(),
            provenance_expires_at: allocation.provenance_expires_at.into(),
            enrollment_fingerprint: allocation.enrollment_fingerprint,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum StoredCrashCause {
    ConnectionLost,
    ProcessKilled,
    ProtocolError,
    UncleanServerRestart,
}

impl StoredCrashCause {
    const fn from_public(cause: ParticipantCrashCause) -> Self {
        match cause {
            ParticipantCrashCause::ConnectionLost => Self::ConnectionLost,
            ParticipantCrashCause::ProcessKilled => Self::ProcessKilled,
            ParticipantCrashCause::ProtocolError => Self::ProtocolError,
            ParticipantCrashCause::UncleanServerRestart => Self::UncleanServerRestart,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", tag = "placement")]
enum StoredCrashDisposition {
    Committed {
        transaction_order: TransactionOrder,
        delivery_seq: DeliverySeq,
    },
    Pending {
        transaction_order: TransactionOrder,
    },
}

impl StoredCrashDisposition {
    const fn from_public(disposition: CrashTerminalDisposition) -> Self {
        match disposition {
            CrashTerminalDisposition::Committed {
                transaction_order,
                delivery_seq,
            } => Self::Committed {
                transaction_order,
                delivery_seq,
            },
            CrashTerminalDisposition::Pending { transaction_order } => {
                Self::Pending { transaction_order }
            }
        }
    }

    const fn into_protocol(self) -> BindingTerminalDisposition {
        match self {
            Self::Committed {
                transaction_order,
                delivery_seq,
            } => BindingTerminalDisposition::Committed(CommittedBindingTerminalPosition::new(
                transaction_order,
                delivery_seq,
            )),
            Self::Pending { transaction_order } => BindingTerminalDisposition::Pending(
                PendingBindingTerminalPosition::new(transaction_order),
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct StoredU128([u8; 16]);

impl StoredU128 {
    const fn get(self) -> u128 {
        u128::from_be_bytes(self.0)
    }
}

impl From<u128> for StoredU128 {
    fn from(value: u128) -> Self {
        Self(value.to_be_bytes())
    }
}

fn generation(value: u64) -> Result<Generation, ParticipantCrashRepositoryError> {
    Generation::new(value).ok_or(ParticipantCrashRepositoryError::ZeroGeneration)
}
