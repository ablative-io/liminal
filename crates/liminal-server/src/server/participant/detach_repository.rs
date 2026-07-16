//! Append-only participant-state repository for the detach/attach replay path.
//!
//! Durable entries contain transition inputs rather than snapshots of protocol
//! state. Opening a repository replays every entry through `liminal-protocol`,
//! so the server never grows a second implementation of lifecycle rules.

use std::sync::Arc;

use liminal::durability::{DurabilityError, DurableStore};
use liminal_protocol::lifecycle::{
    ActiveBinding, AllocatedParticipantSlot, AttachCommitError, AttachCommitParameters,
    AttachSecretProof, AttachVerificationError, AttachedRecordPosition, BindingState,
    CommittedBindingTerminalPosition, DetachCell, DetachCommitError, DetachLookupContext,
    DetachLookupResult, DetachTokenResolution, DetachVerificationError, EnrollmentCommitError,
    EnrollmentCommitParameters, EnrollmentFingerprint, LiveMember, ParticipantSlotAllocationError,
    ParticipantSlotAllocatorProof, PresentedIdentity, ResolvedIdentity, commit_attach,
    commit_detach, commit_enrollment, lookup_detach,
};
use liminal_protocol::wire::{
    AttachAttemptToken, AttachBound, AttachSecret, BindingEpoch, ConnectionIncarnation,
    CredentialAttachRequest, DeliverySeq, DetachAttemptToken, DetachCommitted, DetachRequest,
    DetachStaleAuthority, EnrollBound, EnrollmentRequest, EnrollmentToken, Generation,
    ParticipantId, TerminalizedDetachCell, TransactionOrder,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const EVENT_SCHEMA_VERSION: u8 = 1;
const READ_BATCH_SIZE: usize = 64;
const STREAM_PREFIX: &str = "liminal:participant-lifecycle:";

/// Fixed server-side digest used for enrollment-token mappings and detach requests.
pub type ParticipantRequestDigest = [u8; 32];

/// Server allocations committed together with one enrollment transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnrollmentAllocation {
    /// Newly minted generation-one attach secret.
    pub attach_secret: AttachSecret,
    /// New generation-one binding epoch.
    pub origin_binding_epoch: BindingEpoch,
    /// Serialized conversation order of the `Attached` record.
    pub attached_transaction_order: TransactionOrder,
    /// Delivery sequence of the `Attached` record.
    pub attached_delivery_seq: DeliverySeq,
    /// Secret-bearing receipt deadline.
    pub receipt_expires_at: u128,
    /// Non-secret provenance deadline.
    pub provenance_expires_at: u128,
    /// Permanent enrollment-token mapping digest.
    pub enrollment_fingerprint: ParticipantRequestDigest,
}

/// Server allocations committed together with one immediate detach transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DetachAllocation {
    /// Canonical non-secret digest of the detach request.
    pub request_verifier: ParticipantRequestDigest,
    /// Exact binding epoch carried by the receiving serialized connection context.
    pub receiving_binding_epoch: BindingEpoch,
    /// Serialized conversation order of the `Detached` terminal.
    pub terminal_transaction_order: TransactionOrder,
    /// Delivery sequence of the `Detached` terminal.
    pub terminal_delivery_seq: DeliverySeq,
}

/// Server allocations committed together with one ordinary detached attach.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrdinaryAttachAllocation {
    /// Newly authoritative binding epoch.
    pub binding_epoch: BindingEpoch,
    /// Newly minted attach secret.
    pub attach_secret: AttachSecret,
    /// Serialized conversation order of the new `Attached` record.
    pub attached_transaction_order: TransactionOrder,
    /// Delivery sequence of the new `Attached` record.
    pub attached_delivery_seq: DeliverySeq,
    /// Secret-bearing receipt deadline.
    pub receipt_expires_at: u128,
    /// Non-secret provenance deadline.
    pub provenance_expires_at: u128,
}

/// Failure to persist, restore, or apply the supported participant transition path.
#[derive(Debug, Error)]
pub enum ParticipantDetachRepositoryError {
    /// The underlying durable store rejected an operation.
    #[error("participant lifecycle durable-store operation failed: {0}")]
    DurableStore(#[from] DurabilityError),
    /// A durable event could not be encoded or decoded.
    #[error("participant lifecycle event serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    /// A durable event uses an unsupported schema version.
    #[error("unsupported participant lifecycle event schema version {0}")]
    EventSchemaVersion(u8),
    /// A durable event carried the forbidden zero generation.
    #[error("participant lifecycle event contains zero generation")]
    ZeroGeneration,
    /// A durable stream contained a gap, duplicate, or reordered event.
    #[error("participant lifecycle stream expected sequence {expected}, found {actual}")]
    EventSequence {
        /// Next sequence required by replay.
        expected: u64,
        /// Sequence read from durable storage.
        actual: u64,
    },
    /// The store returned a different assigned sequence after a successful append.
    #[error("participant lifecycle append expected assigned sequence {expected}, got {actual}")]
    AssignedSequence {
        /// Optimistic-concurrency sequence supplied to the store.
        expected: u64,
        /// Assigned sequence returned by the store.
        actual: u64,
    },
    /// A durable event belongs to a different conversation than its stream.
    #[error(
        "participant lifecycle stream belongs to conversation {stream_conversation}, event names {event_conversation}"
    )]
    ConversationMismatch {
        /// Conversation selected by the repository stream key.
        stream_conversation: u64,
        /// Conversation encoded in the transition event.
        event_conversation: u64,
    },
    /// An enrollment was applied to a non-empty lifecycle stream.
    #[error("enrollment transition requires an empty participant lifecycle stream")]
    EnrollmentState,
    /// A detach was applied before enrollment or without a bound participant.
    #[error("detach transition requires one enrolled, bound participant")]
    DetachState,
    /// An ordinary attach was applied before a committed detach.
    #[error("ordinary attach transition requires one committed detached participant")]
    AttachState,
    /// A lookup was attempted before the participant existed.
    #[error("detach lookup requires one replayed participant")]
    LookupState,
    /// The caller described an invalid permanent participant-slot allocation.
    #[error("participant-slot allocation failed: {0:?}")]
    ParticipantSlotAllocation(ParticipantSlotAllocationError),
    /// The protocol crate rejected enrollment transition inputs.
    #[error("protocol enrollment transition failed: {0:?}")]
    EnrollmentTransition(EnrollmentCommitError),
    /// The protocol crate rejected detach authority.
    #[error("protocol detach verification failed: {0:?}")]
    DetachVerification(DetachVerificationError),
    /// The protocol crate's ordered detach lookup did not authorize the request.
    #[error("protocol detach lookup did not authorize a fresh bound request")]
    DetachLookupRejected,
    /// The protocol crate rejected a committed detach transition.
    #[error("protocol detach transition failed: {0:?}")]
    DetachTransition(DetachCommitError),
    /// The protocol crate rejected ordinary-attach authority.
    #[error("protocol attach verification failed: {0:?}")]
    AttachVerification(AttachVerificationError),
    /// The protocol crate rejected a committed ordinary attach transition.
    #[error("protocol attach transition failed: {0:?}")]
    AttachTransition(AttachCommitError),
    /// A clear closure unexpectedly failed to authorize ordinary detached attach.
    #[error("clear closure did not authorize ordinary detached attach")]
    ClearClosureAdmission,
    /// Exact lookup did not select the terminalized detach-cell outcome.
    #[error("exact detach lookup did not resolve a terminalized detach cell")]
    NotTerminalizedDetach,
}

/// Append-only repository for one conversation's participant detach replay state.
///
/// This first binding intentionally covers the mandatory enrollment, immediate
/// committed detach, ordinary attach, and exact old-token lookup path. Each
/// mutating method appends and flushes exactly one event with an optimistic
/// stream-head precondition before publishing its outcome. No protocol state
/// is cached or serialized.
#[derive(Debug)]
pub struct ParticipantDetachRepository {
    store: Arc<dyn DurableStore>,
    conversation_id: u64,
    stream_key: String,
}

impl ParticipantDetachRepository {
    /// Creates a stateless repository handle over one durable conversation stream.
    #[must_use]
    pub fn new(store: Arc<dyn DurableStore>, conversation_id: u64) -> Self {
        Self {
            store,
            conversation_id,
            stream_key: format!("{STREAM_PREFIX}{conversation_id}"),
        }
    }

    /// Returns the namespaced append-only stream key used by this repository.
    #[must_use]
    pub fn stream_key(&self) -> &str {
        &self.stream_key
    }

    /// Commits initial enrollment and appends its complete transition inputs.
    ///
    /// The live allocator proof is consumed by the initial transition. Durable
    /// replay uses the exact allocation facts stored in that successful event.
    ///
    /// # Errors
    ///
    /// Returns [`ParticipantDetachRepositoryError`] for existing state, invalid
    /// allocation or protocol inputs, serialization failure, or store failure.
    pub async fn commit_enrollment<P>(
        &self,
        request: EnrollmentRequest,
        allocator_proof: P,
        allocation: EnrollmentAllocation,
    ) -> Result<EnrollBound, ParticipantDetachRepositoryError>
    where
        P: ParticipantSlotAllocatorProof,
    {
        let event = StoredTransition::enrollment(&request, &allocator_proof, allocation);
        self.validate_event_conversation(&event.transition)?;
        let replayed = self.replay().await?;
        let (_, outcome) =
            apply_enrollment_with_proof(&replayed.state, &event.transition, allocator_proof)?;
        self.append_transition(&event, replayed.next_sequence)
            .await?;
        Ok(outcome)
    }

    /// Commits an immediate clean detach and appends its complete transition inputs.
    ///
    /// # Errors
    ///
    /// Returns [`ParticipantDetachRepositoryError`] when replay does not produce
    /// a bound participant, protocol verification fails, or persistence fails.
    pub async fn commit_detach(
        &self,
        request: DetachRequest,
        allocation: DetachAllocation,
    ) -> Result<DetachCommitted, ParticipantDetachRepositoryError> {
        let event = StoredTransition::detach(&request, allocation);
        self.validate_event_conversation(&event.transition)?;
        let replayed = self.replay().await?;
        let (_, outcome) = apply_transition(replayed.state, &event.transition)?;
        let TransitionOutcome::Detach(outcome) = outcome else {
            return Err(ParticipantDetachRepositoryError::DetachState);
        };
        self.append_transition(&event, replayed.next_sequence)
            .await?;
        Ok(outcome)
    }

    /// Commits a clear-closure ordinary attach after a committed detach.
    ///
    /// The resulting protocol transition changes `Committed` to `Terminalized`
    /// atomically with the new binding and `Attached` record.
    ///
    /// # Errors
    ///
    /// Returns [`ParticipantDetachRepositoryError`] when replay does not produce
    /// a committed detached member, protocol verification fails, or persistence fails.
    pub async fn commit_ordinary_attach(
        &self,
        request: CredentialAttachRequest,
        secret_proof: AttachSecretProof,
        allocation: OrdinaryAttachAllocation,
    ) -> Result<AttachBound, ParticipantDetachRepositoryError> {
        let event = StoredTransition::ordinary_attach(&request, secret_proof, allocation);
        self.validate_event_conversation(&event.transition)?;
        let replayed = self.replay().await?;
        let (_, outcome) = apply_transition(replayed.state, &event.transition)?;
        let TransitionOutcome::Attach(outcome) = outcome else {
            return Err(ParticipantDetachRepositoryError::AttachState);
        };
        self.append_transition(&event, replayed.next_sequence)
            .await?;
        Ok(outcome)
    }

    /// Replays durable state and performs the crate's exact detach lookup order.
    ///
    /// The response can be returned only when replay produced the mandated
    /// terminalized cell and the crate selected its exact-token stale-authority
    /// arm. The server never constructs [`TerminalizedDetachCell`] itself.
    ///
    /// # Errors
    ///
    /// Returns [`ParticipantDetachRepositoryError::NotTerminalizedDetach`] for
    /// every other lookup classification, including an inexact token or verifier.
    pub async fn exact_terminalized_detach_lookup(
        &self,
        request: &DetachRequest,
        request_verifier: ParticipantRequestDigest,
        receiving_binding_epoch: Option<BindingEpoch>,
        observer_progress: DeliverySeq,
    ) -> Result<TerminalizedDetachCell, ParticipantDetachRepositoryError> {
        let replayed = self.replay().await?;
        let ReplayState::Live(state) = replayed.state else {
            return Err(ParticipantDetachRepositoryError::LookupState);
        };
        let LiveReplayState {
            member,
            binding_state,
            detach_cell,
        } = *state;
        let token_resolution = if replayed.exact_detach_token == Some(request.detach_attempt_token)
        {
            DetachTokenResolution::Exact(ResolvedIdentity::<
                ParticipantRequestDigest,
                ParticipantRequestDigest,
                ParticipantRequestDigest,
            >::Live(&member))
        } else {
            DetachTokenResolution::NoExactMatch
        };
        let lookup = lookup_detach(&DetachLookupContext {
            token_resolution,
            presented_identity: PresentedIdentity::<
                ParticipantRequestDigest,
                ParticipantRequestDigest,
                ParticipantRequestDigest,
            >::Live(&member),
            cell: &detach_cell,
            binding: &binding_state,
            receiving_binding_epoch,
            request,
            request_verifier,
            observer_progress,
        });
        match lookup {
            DetachLookupResult::StaleAuthority(DetachStaleAuthority::TerminalizedDetachCell(
                outcome,
            )) => Ok(outcome),
            DetachLookupResult::Retired(_)
            | DetachLookupResult::ParticipantUnknown(_)
            | DetachLookupResult::StaleAuthority(DetachStaleAuthority::Live { .. })
            | DetachLookupResult::NoBinding(_)
            | DetachLookupResult::PendingReplayRequired(_)
            | DetachLookupResult::DetachInProgress(_)
            | DetachLookupResult::DetachCommitted(_)
            | DetachLookupResult::Authorized { .. } => {
                Err(ParticipantDetachRepositoryError::NotTerminalizedDetach)
            }
        }
    }

    async fn append_transition(
        &self,
        event: &StoredTransition,
        expected_sequence: u64,
    ) -> Result<(), ParticipantDetachRepositoryError> {
        let payload = serde_json::to_vec(event)?;
        let assigned = self
            .store
            .append(&self.stream_key, payload, expected_sequence)
            .await?;
        if assigned != expected_sequence {
            return Err(ParticipantDetachRepositoryError::AssignedSequence {
                expected: expected_sequence,
                actual: assigned,
            });
        }
        self.store.flush().await?;
        Ok(())
    }

    const fn validate_event_conversation(
        &self,
        transition: &DurableTransition,
    ) -> Result<(), ParticipantDetachRepositoryError> {
        let event_conversation = transition.conversation_id();
        if event_conversation != self.conversation_id {
            return Err(ParticipantDetachRepositoryError::ConversationMismatch {
                stream_conversation: self.conversation_id,
                event_conversation,
            });
        }
        Ok(())
    }

    async fn replay(&self) -> Result<ReplayedRepository, ParticipantDetachRepositoryError> {
        let mut state = ReplayState::Empty;
        let mut next_sequence = 0_u64;
        let mut exact_detach_token = None;
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
                    return Err(ParticipantDetachRepositoryError::EventSequence {
                        expected: next_sequence,
                        actual: entry.sequence,
                    });
                }
                let event: StoredTransition = serde_json::from_slice(&entry.payload)?;
                if event.schema_version != EVENT_SCHEMA_VERSION {
                    return Err(ParticipantDetachRepositoryError::EventSchemaVersion(
                        event.schema_version,
                    ));
                }
                self.validate_event_conversation(&event.transition)?;
                if let DurableTransition::Detach { request, .. } = &event.transition {
                    exact_detach_token = Some(DetachAttemptToken::new(request.token));
                }
                (state, _) = apply_transition(state, &event.transition)?;
                next_sequence = next_sequence.checked_add(1).ok_or(
                    ParticipantDetachRepositoryError::EventSequence {
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
            exact_detach_token,
        })
    }
}

#[derive(Debug)]
struct ReplayedRepository {
    state: ReplayState,
    next_sequence: u64,
    exact_detach_token: Option<DetachAttemptToken>,
}

#[derive(Debug)]
enum ReplayState {
    Empty,
    Live(Box<LiveReplayState>),
}

#[derive(Debug)]
struct LiveReplayState {
    member: LiveMember<ParticipantRequestDigest>,
    binding_state: BindingState,
    detach_cell: DetachCell<ParticipantRequestDigest>,
}

#[derive(Debug)]
enum TransitionOutcome {
    Enrollment,
    Detach(DetachCommitted),
    Attach(AttachBound),
}

fn apply_transition(
    state: ReplayState,
    transition: &DurableTransition,
) -> Result<(ReplayState, TransitionOutcome), ParticipantDetachRepositoryError> {
    match transition {
        DurableTransition::Enrollment { allocator, .. } => {
            let proof = ReplayedParticipantSlot {
                conversation_id: allocator.conversation_id,
                participant_id: allocator.participant_id,
                identity_limit: allocator.identity_limit,
            };
            let (next_state, _outcome) = apply_enrollment_with_proof(&state, transition, proof)?;
            Ok((next_state, TransitionOutcome::Enrollment))
        }
        DurableTransition::Detach { .. } => apply_detach_transition(state, transition),
        DurableTransition::OrdinaryAttach { .. } => apply_attach_transition(state, transition),
    }
}

fn apply_detach_transition(
    state: ReplayState,
    transition: &DurableTransition,
) -> Result<(ReplayState, TransitionOutcome), ParticipantDetachRepositoryError> {
    let DurableTransition::Detach {
        request,
        request_verifier,
        receiving_binding_epoch,
        terminal_transaction_order,
        terminal_delivery_seq,
    } = transition
    else {
        return Err(ParticipantDetachRepositoryError::DetachState);
    };
    let ReplayState::Live(state) = state else {
        return Err(ParticipantDetachRepositoryError::DetachState);
    };
    let LiveReplayState {
        member,
        binding_state,
        detach_cell,
    } = *state;
    let request = request.to_detach_request()?;
    let receiving_binding_epoch = receiving_binding_epoch.to_binding_epoch()?;
    let binding = {
        let lookup = lookup_detach(&DetachLookupContext {
            token_resolution: DetachTokenResolution::<
                ParticipantRequestDigest,
                ParticipantRequestDigest,
                ParticipantRequestDigest,
            >::NoExactMatch,
            presented_identity: PresentedIdentity::<
                ParticipantRequestDigest,
                ParticipantRequestDigest,
                ParticipantRequestDigest,
            >::Live(&member),
            cell: &detach_cell,
            binding: &binding_state,
            receiving_binding_epoch: Some(receiving_binding_epoch),
            request: &request,
            request_verifier: *request_verifier,
            observer_progress: 0,
        });
        let DetachLookupResult::Authorized { binding, .. } = lookup else {
            return Err(ParticipantDetachRepositoryError::DetachLookupRejected);
        };
        binding
    };
    let verified = binding
        .verify_detach_request(request, *request_verifier)
        .map_err(ParticipantDetachRepositoryError::DetachVerification)?;
    let committed = commit_detach(
        member,
        verified,
        detach_cell,
        CommittedBindingTerminalPosition::new(*terminal_transaction_order, *terminal_delivery_seq),
    )
    .map_err(ParticipantDetachRepositoryError::DetachTransition)?;
    let (member, _terminal, binding_state, cell, outcome) = committed.into_parts();
    Ok((
        ReplayState::Live(Box::new(LiveReplayState {
            member,
            binding_state,
            detach_cell: DetachCell::Committed(cell),
        })),
        TransitionOutcome::Detach(outcome),
    ))
}

fn apply_attach_transition(
    state: ReplayState,
    transition: &DurableTransition,
) -> Result<(ReplayState, TransitionOutcome), ParticipantDetachRepositoryError> {
    let DurableTransition::OrdinaryAttach {
        request,
        secret_proof,
        allocation,
    } = transition
    else {
        return Err(ParticipantDetachRepositoryError::AttachState);
    };
    let ReplayState::Live(state) = state else {
        return Err(ParticipantDetachRepositoryError::AttachState);
    };
    let LiveReplayState {
        member,
        binding_state,
        detach_cell,
    } = *state;
    if !matches!(detach_cell, DetachCell::Committed(_)) {
        return Err(ParticipantDetachRepositoryError::AttachState);
    }
    let closure_admission = liminal_protocol::lifecycle::ClosureState::Clear
        .ordinary_detached_attach_admission()
        .map_err(|_| ParticipantDetachRepositoryError::ClearClosureAdmission)?;
    let request = request.to_attach_request()?;
    let binding = ActiveBinding {
        participant_id: request.participant_id,
        conversation_id: request.conversation_id,
        binding_epoch: allocation.binding_epoch.to_binding_epoch()?,
    };
    let verified = member
        .verify_detached_attach(
            binding_state,
            closure_admission,
            request,
            (*secret_proof).into(),
            AttachCommitParameters {
                binding,
                attach_secret: AttachSecret::new(allocation.attach_secret),
                attached_position: AttachedRecordPosition::new(
                    allocation.attached_transaction_order,
                    allocation.attached_delivery_seq,
                ),
                receipt_expires_at: allocation.receipt_expires_at.get(),
                provenance_expires_at: allocation.provenance_expires_at.get(),
            },
        )
        .map_err(ParticipantDetachRepositoryError::AttachVerification)?;
    let committed = commit_attach(verified, detach_cell)
        .map_err(ParticipantDetachRepositoryError::AttachTransition)?;
    let outcome = committed.outcome;
    Ok((
        ReplayState::Live(Box::new(LiveReplayState {
            member: committed.member,
            binding_state: committed.binding_state,
            detach_cell: committed.detach_cell,
        })),
        TransitionOutcome::Attach(outcome),
    ))
}

fn apply_enrollment_with_proof<P>(
    state: &ReplayState,
    transition: &DurableTransition,
    allocator_proof: P,
) -> Result<(ReplayState, EnrollBound), ParticipantDetachRepositoryError>
where
    P: ParticipantSlotAllocatorProof,
{
    let ReplayState::Empty = state else {
        return Err(ParticipantDetachRepositoryError::EnrollmentState);
    };
    let DurableTransition::Enrollment {
        request,
        allocation,
        ..
    } = transition
    else {
        return Err(ParticipantDetachRepositoryError::EnrollmentState);
    };
    let allocated_slot = AllocatedParticipantSlot::from_allocator(allocator_proof)
        .map_err(ParticipantDetachRepositoryError::ParticipantSlotAllocation)?;
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
    .map_err(ParticipantDetachRepositoryError::EnrollmentTransition)?;
    Ok((
        ReplayState::Live(Box::new(LiveReplayState {
            member: committed.member,
            binding_state: committed.binding_state,
            detach_cell: DetachCell::default(),
        })),
        committed.outcome,
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
struct StoredTransition {
    schema_version: u8,
    transition: DurableTransition,
}

impl StoredTransition {
    fn enrollment<P: ParticipantSlotAllocatorProof>(
        request: &EnrollmentRequest,
        allocator: &P,
        allocation: EnrollmentAllocation,
    ) -> Self {
        Self {
            schema_version: EVENT_SCHEMA_VERSION,
            transition: DurableTransition::Enrollment {
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

    fn detach(request: &DetachRequest, allocation: DetachAllocation) -> Self {
        Self {
            schema_version: EVENT_SCHEMA_VERSION,
            transition: DurableTransition::Detach {
                request: StoredDetachRequest::from(request),
                request_verifier: allocation.request_verifier,
                receiving_binding_epoch: allocation.receiving_binding_epoch.into(),
                terminal_transaction_order: allocation.terminal_transaction_order,
                terminal_delivery_seq: allocation.terminal_delivery_seq,
            },
        }
    }

    fn ordinary_attach(
        request: &CredentialAttachRequest,
        secret_proof: AttachSecretProof,
        allocation: OrdinaryAttachAllocation,
    ) -> Self {
        Self {
            schema_version: EVENT_SCHEMA_VERSION,
            transition: DurableTransition::OrdinaryAttach {
                request: StoredAttachRequest::from(request),
                secret_proof: secret_proof.into(),
                allocation: StoredAttachAllocation::from(allocation),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", tag = "transition")]
enum DurableTransition {
    Enrollment {
        request: StoredEnrollmentRequest,
        allocator: StoredAllocator,
        allocation: StoredEnrollmentAllocation,
    },
    Detach {
        request: StoredDetachRequest,
        request_verifier: ParticipantRequestDigest,
        receiving_binding_epoch: StoredBindingEpoch,
        terminal_transaction_order: TransactionOrder,
        terminal_delivery_seq: DeliverySeq,
    },
    OrdinaryAttach {
        request: StoredAttachRequest,
        secret_proof: StoredAttachSecretProof,
        allocation: StoredAttachAllocation,
    },
}

impl DurableTransition {
    const fn conversation_id(&self) -> u64 {
        match self {
            Self::Enrollment { request, .. } => request.conversation_id,
            Self::Detach { request, .. } => request.conversation_id,
            Self::OrdinaryAttach { request, .. } => request.conversation_id,
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
struct StoredDetachRequest {
    conversation_id: u64,
    participant_id: ParticipantId,
    capability_generation: u64,
    token: [u8; 16],
}

impl From<&DetachRequest> for StoredDetachRequest {
    fn from(request: &DetachRequest) -> Self {
        Self {
            conversation_id: request.conversation_id,
            participant_id: request.participant_id,
            capability_generation: request.capability_generation.get(),
            token: request.detach_attempt_token.into_bytes(),
        }
    }
}

impl StoredDetachRequest {
    fn to_detach_request(self) -> Result<DetachRequest, ParticipantDetachRepositoryError> {
        Ok(DetachRequest {
            conversation_id: self.conversation_id,
            participant_id: self.participant_id,
            capability_generation: generation(self.capability_generation)?,
            detach_attempt_token: DetachAttemptToken::new(self.token),
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct StoredAttachRequest {
    conversation_id: u64,
    participant_id: ParticipantId,
    capability_generation: u64,
    attach_secret: [u8; 32],
    token: [u8; 16],
    accept_marker_delivery_seq: Option<DeliverySeq>,
}

impl From<&CredentialAttachRequest> for StoredAttachRequest {
    fn from(request: &CredentialAttachRequest) -> Self {
        Self {
            conversation_id: request.conversation_id,
            participant_id: request.participant_id,
            capability_generation: request.capability_generation.get(),
            attach_secret: request.attach_secret.into_bytes(),
            token: request.attach_attempt_token.into_bytes(),
            accept_marker_delivery_seq: request.accept_marker_delivery_seq,
        }
    }
}

impl StoredAttachRequest {
    fn to_attach_request(
        self,
    ) -> Result<CredentialAttachRequest, ParticipantDetachRepositoryError> {
        Ok(CredentialAttachRequest {
            conversation_id: self.conversation_id,
            participant_id: self.participant_id,
            capability_generation: generation(self.capability_generation)?,
            attach_secret: AttachSecret::new(self.attach_secret),
            attach_attempt_token: AttachAttemptToken::new(self.token),
            accept_marker_delivery_seq: self.accept_marker_delivery_seq,
        })
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
    fn to_binding_epoch(self) -> Result<BindingEpoch, ParticipantDetachRepositoryError> {
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
    enrollment_fingerprint: ParticipantRequestDigest,
}

impl From<EnrollmentAllocation> for StoredEnrollmentAllocation {
    fn from(allocation: EnrollmentAllocation) -> Self {
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
struct StoredAttachAllocation {
    binding_epoch: StoredBindingEpoch,
    attach_secret: [u8; 32],
    attached_transaction_order: TransactionOrder,
    attached_delivery_seq: DeliverySeq,
    receipt_expires_at: StoredU128,
    provenance_expires_at: StoredU128,
}

impl From<OrdinaryAttachAllocation> for StoredAttachAllocation {
    fn from(allocation: OrdinaryAttachAllocation) -> Self {
        Self {
            binding_epoch: allocation.binding_epoch.into(),
            attach_secret: allocation.attach_secret.into_bytes(),
            attached_transaction_order: allocation.attached_transaction_order,
            attached_delivery_seq: allocation.attached_delivery_seq,
            receipt_expires_at: allocation.receipt_expires_at.into(),
            provenance_expires_at: allocation.provenance_expires_at.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum StoredAttachSecretProof {
    Mismatch,
    Verified,
}

impl From<AttachSecretProof> for StoredAttachSecretProof {
    fn from(proof: AttachSecretProof) -> Self {
        match proof {
            AttachSecretProof::Mismatch => Self::Mismatch,
            AttachSecretProof::Verified => Self::Verified,
        }
    }
}

impl From<StoredAttachSecretProof> for AttachSecretProof {
    fn from(proof: StoredAttachSecretProof) -> Self {
        match proof {
            StoredAttachSecretProof::Mismatch => Self::Mismatch,
            StoredAttachSecretProof::Verified => Self::Verified,
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

fn generation(value: u64) -> Result<Generation, ParticipantDetachRepositoryError> {
    Generation::new(value).ok_or(ParticipantDetachRepositoryError::ZeroGeneration)
}
