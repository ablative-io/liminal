use core::num::NonZeroU64;

use crate::wire::{
    AttachEnvelope, CredentialAttachRequest, CredentialAttachResponse, EnrollmentEnvelope,
    EnrollmentReceiptCapacityScope, EnrollmentRequest, EnrollmentResponse,
    IdentityCapacityExceeded, IdentityCapacityScope, ParticipantId, ReceiptCapacityScope,
};

/// Invalid persisted occupancy for one signed nonzero capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapacityCounterInvariantError {
    /// Protocol capacity limits are nonzero.
    ZeroLimit,
    /// Persisted occupancy is greater than its signed limit.
    OccupiedExceedsLimit {
        /// Persisted occupancy.
        occupied: u64,
        /// Signed capacity limit.
        limit: u64,
    },
}

/// Validated occupancy bounded by one nonzero signed limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CapacityCounter {
    limit: NonZeroU64,
    occupied: u64,
}

impl CapacityCounter {
    /// Restores one counter only when its limit is nonzero and occupancy fits.
    ///
    /// # Errors
    ///
    /// Returns [`CapacityCounterInvariantError::ZeroLimit`] for a zero limit or
    /// [`CapacityCounterInvariantError::OccupiedExceedsLimit`] when persisted
    /// occupancy is outside the inclusive `0..=limit` domain.
    pub const fn try_new(limit: u64, occupied: u64) -> Result<Self, CapacityCounterInvariantError> {
        let Some(limit) = NonZeroU64::new(limit) else {
            return Err(CapacityCounterInvariantError::ZeroLimit);
        };
        if occupied > limit.get() {
            return Err(CapacityCounterInvariantError::OccupiedExceedsLimit {
                occupied,
                limit: limit.get(),
            });
        }
        Ok(Self { limit, occupied })
    }

    /// Returns the signed nonzero limit.
    #[must_use]
    pub const fn limit(self) -> u64 {
        self.limit.get()
    }

    /// Returns current validated occupancy.
    #[must_use]
    pub const fn occupied(self) -> u64 {
        self.occupied
    }

    /// Returns whether another row would exceed the signed limit.
    #[must_use]
    pub const fn is_full(self) -> bool {
        self.occupied == self.limit.get()
    }

    const fn incremented(self) -> Option<Self> {
        if self.is_full() {
            return None;
        }
        Some(Self {
            limit: self.limit,
            occupied: self.occupied + 1,
        })
    }
}

/// Invalid restored occupancy for a participant that has not yet been minted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FreshParticipantCapacityCounterInvariantError {
    /// The underlying nonzero bounded counter is invalid.
    Capacity(CapacityCounterInvariantError),
    /// A not-yet-minted participant cannot already own receipt state.
    Nonempty {
        /// Invalid restored per-participant occupancy.
        occupied: u64,
    },
}

/// Provably empty, nonzero per-participant capacity for fresh enrollment.
///
/// This type removes the unreachable enrollment refusal arms while still
/// forcing the successful transaction to reserve both new participant rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FreshParticipantCapacityCounter {
    counter: CapacityCounter,
}

impl FreshParticipantCapacityCounter {
    /// Restores a fresh-participant counter only at occupancy zero.
    ///
    /// # Errors
    ///
    /// Returns [`FreshParticipantCapacityCounterInvariantError::Capacity`] for
    /// an invalid base counter or
    /// [`FreshParticipantCapacityCounterInvariantError::Nonempty`] when a
    /// not-yet-minted participant already has a row.
    pub const fn try_new(
        limit: u64,
        occupied: u64,
    ) -> Result<Self, FreshParticipantCapacityCounterInvariantError> {
        let counter = match CapacityCounter::try_new(limit, occupied) {
            Ok(counter) => counter,
            Err(error) => {
                return Err(FreshParticipantCapacityCounterInvariantError::Capacity(
                    error,
                ));
            }
        };
        if occupied != 0 {
            return Err(FreshParticipantCapacityCounterInvariantError::Nonempty { occupied });
        }
        Ok(Self { counter })
    }

    /// Returns the signed nonzero per-participant limit.
    #[must_use]
    pub const fn limit(self) -> u64 {
        self.counter.limit()
    }

    /// Returns the type-proven zero occupancy.
    #[must_use]
    pub const fn occupied(self) -> u64 {
        self.counter.occupied()
    }

    const fn reserved(self) -> CapacityCounter {
        CapacityCounter {
            limit: self.counter.limit,
            occupied: 1,
        }
    }
}

/// Whether a semantic request's conversation already owns a connection slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionConversationTracking {
    /// The conversation is already counted and consumes no additional slot.
    AlreadyTracked,
    /// The conversation needs its first connection-local slot.
    Untracked,
}

/// Atomic successful result of semantic connection-capacity admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectionConversationCapacityCommit {
    resulting: CapacityCounter,
    newly_tracked: bool,
}

impl ConnectionConversationCapacityCommit {
    /// Returns the complete post-operation connection occupancy.
    #[must_use]
    pub const fn resulting(self) -> CapacityCounter {
        self.resulting
    }

    /// Returns whether the operation must install a new conversation slot.
    #[must_use]
    pub const fn newly_tracked(self) -> bool {
        self.newly_tracked
    }
}

/// Stage-6 semantic connection-capacity result.
///
/// The refusal arm carries only the request-independent capacity fact; the
/// invoking operation mints its request-bound `0x0102` wire outcome from its
/// own exact envelope plus this signed limit, so the triggering envelope is
/// never duplicated through this shared selector.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SemanticConnectionCapacityDecision {
    /// Existing or newly reserved conversation capacity may commit.
    Commit(ConnectionConversationCapacityCommit),
    /// The untracked conversation would exceed the signed limit.
    Respond {
        /// Signed connection-conversation limit that is full.
        limit: u64,
    },
}

/// Applies semantic connection-conversation capacity before participant mutation.
///
/// An already tracked conversation succeeds without incrementing the counter,
/// even when capacity is full. An untracked conversation either returns the
/// complete incremented counter or the signed limit for the caller's exact
/// request-bound `0x0102` wire outcome.
#[must_use]
pub const fn select_semantic_connection_capacity(
    tracking: ConnectionConversationTracking,
    current: CapacityCounter,
) -> SemanticConnectionCapacityDecision {
    match tracking {
        ConnectionConversationTracking::AlreadyTracked => {
            SemanticConnectionCapacityDecision::Commit(ConnectionConversationCapacityCommit {
                resulting: current,
                newly_tracked: false,
            })
        }
        ConnectionConversationTracking::Untracked => {
            let Some(resulting) = current.incremented() else {
                return SemanticConnectionCapacityDecision::Respond {
                    limit: current.limit(),
                };
            };
            SemanticConnectionCapacityDecision::Commit(ConnectionConversationCapacityCommit {
                resulting,
                newly_tracked: true,
            })
        }
    }
}

/// Current participant occupancy of one connection/conversation binding slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindingSlotOccupancy {
    /// No participant currently occupies the slot.
    Empty,
    /// One participant currently occupies the slot.
    Occupied {
        /// Occupying participant, used only for same-participant rotation.
        participant_id: ParticipantId,
    },
}

/// Stage-6 participant binding-slot result, bound to the requesting
/// operation's response authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BindingSlotDecision<R> {
    /// The binding operation may continue.
    Available,
    /// Exact request-bound binding-slot refusal.
    Respond(R),
}

/// Selects enrollment binding-slot occupancy without revealing its occupant.
#[must_use]
pub const fn select_enrollment_binding_slot(
    request: &EnrollmentRequest,
    occupancy: BindingSlotOccupancy,
) -> BindingSlotDecision<EnrollmentResponse> {
    match occupancy {
        BindingSlotOccupancy::Empty => BindingSlotDecision::Available,
        BindingSlotOccupancy::Occupied { .. } => BindingSlotDecision::Respond(
            EnrollmentResponse::connection_conversation_binding_occupied(&enrollment_envelope(
                request,
            )),
        ),
    }
}

/// Selects credential-attach binding occupancy, permitting only an empty slot
/// or rotation of the same presented participant.
#[must_use]
pub const fn select_credential_attach_binding_slot(
    request: &CredentialAttachRequest,
    occupancy: BindingSlotOccupancy,
) -> BindingSlotDecision<CredentialAttachResponse> {
    match occupancy {
        BindingSlotOccupancy::Empty => BindingSlotDecision::Available,
        BindingSlotOccupancy::Occupied { participant_id }
            if participant_id == request.participant_id =>
        {
            BindingSlotDecision::Available
        }
        BindingSlotOccupancy::Occupied { .. } => BindingSlotDecision::Respond(
            CredentialAttachResponse::connection_conversation_binding_occupied(&attach_envelope(
                request,
            )),
        ),
    }
}

/// All seven stage-8 counters for a fresh enrollment.
///
/// Only five can refuse. The two per-participant counters use
/// [`FreshParticipantCapacityCounter`], proving their occupancy is zero and
/// their limits nonzero before identity mint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnrollmentCapacityCounters {
    identity_server: CapacityCounter,
    identity_conversation: CapacityCounter,
    live_receipt_server: CapacityCounter,
    live_receipt_participant: FreshParticipantCapacityCounter,
    provenance_server: CapacityCounter,
    provenance_conversation: CapacityCounter,
    provenance_participant: FreshParticipantCapacityCounter,
}

impl EnrollmentCapacityCounters {
    /// Creates the complete reachable enrollment counter snapshot.
    #[must_use]
    pub const fn new(
        identity_server: CapacityCounter,
        identity_conversation: CapacityCounter,
        live_receipt_server: CapacityCounter,
        live_receipt_participant: FreshParticipantCapacityCounter,
        provenance_server: CapacityCounter,
        provenance_conversation: CapacityCounter,
        provenance_participant: FreshParticipantCapacityCounter,
    ) -> Self {
        Self {
            identity_server,
            identity_conversation,
            live_receipt_server,
            live_receipt_participant,
            provenance_server,
            provenance_conversation,
            provenance_participant,
        }
    }

    /// Returns server-wide identity occupancy.
    #[must_use]
    pub const fn identity_server(self) -> CapacityCounter {
        self.identity_server
    }

    /// Returns conversation identity occupancy.
    #[must_use]
    pub const fn identity_conversation(self) -> CapacityCounter {
        self.identity_conversation
    }

    /// Returns server-wide live-receipt occupancy.
    #[must_use]
    pub const fn live_receipt_server(self) -> CapacityCounter {
        self.live_receipt_server
    }

    /// Returns the provably empty participant live-receipt capacity.
    #[must_use]
    pub const fn live_receipt_participant(self) -> FreshParticipantCapacityCounter {
        self.live_receipt_participant
    }

    /// Returns server-wide provenance occupancy.
    #[must_use]
    pub const fn provenance_server(self) -> CapacityCounter {
        self.provenance_server
    }

    /// Returns conversation provenance occupancy.
    #[must_use]
    pub const fn provenance_conversation(self) -> CapacityCounter {
        self.provenance_conversation
    }

    /// Returns the provably empty participant provenance capacity.
    #[must_use]
    pub const fn provenance_participant(self) -> FreshParticipantCapacityCounter {
        self.provenance_participant
    }
}

/// All seven post-enrollment identity and receipt/provenance counters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResultingEnrollmentCapacityCounters {
    identity_server: CapacityCounter,
    identity_conversation: CapacityCounter,
    live_receipt_server: CapacityCounter,
    live_receipt_participant: CapacityCounter,
    provenance_server: CapacityCounter,
    provenance_conversation: CapacityCounter,
    provenance_participant: CapacityCounter,
}

impl ResultingEnrollmentCapacityCounters {
    /// Returns server-wide identity occupancy.
    #[must_use]
    pub const fn identity_server(self) -> CapacityCounter {
        self.identity_server
    }

    /// Returns conversation identity occupancy.
    #[must_use]
    pub const fn identity_conversation(self) -> CapacityCounter {
        self.identity_conversation
    }

    /// Returns server-wide live-receipt occupancy.
    #[must_use]
    pub const fn live_receipt_server(self) -> CapacityCounter {
        self.live_receipt_server
    }

    /// Returns the newly minted participant's live-receipt occupancy.
    #[must_use]
    pub const fn live_receipt_participant(self) -> CapacityCounter {
        self.live_receipt_participant
    }

    /// Returns server-wide provenance occupancy.
    #[must_use]
    pub const fn provenance_server(self) -> CapacityCounter {
        self.provenance_server
    }

    /// Returns conversation provenance occupancy.
    #[must_use]
    pub const fn provenance_conversation(self) -> CapacityCounter {
        self.provenance_conversation
    }

    /// Returns the newly minted participant's provenance occupancy.
    #[must_use]
    pub const fn provenance_participant(self) -> CapacityCounter {
        self.provenance_participant
    }
}

/// Atomic successful enrollment capacity reservation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnrollmentCapacityCommit {
    resulting: ResultingEnrollmentCapacityCounters,
}

impl EnrollmentCapacityCommit {
    /// Returns every incremented enrollment counter as one commit value.
    #[must_use]
    pub const fn resulting(self) -> ResultingEnrollmentCapacityCounters {
        self.resulting
    }
}

/// Exhaustive stage-8 enrollment runtime-capacity result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnrollmentCapacityDecision {
    /// All seven reservations may commit together.
    Commit(EnrollmentCapacityCommit),
    /// Exact first-full identity or receipt scope, bound to enrollment.
    Respond(EnrollmentResponse),
}

/// Applies the fixed enrollment runtime-capacity order atomically.
///
/// The order is identity Server, identity Conversation, `LiveReceiptServer`,
/// `ProvenanceServer`, then `ProvenanceConversation`. A refusal exposes only
/// the first full scope; success carries every post-increment counter together.
#[must_use]
pub const fn select_enrollment_capacity(
    request: &EnrollmentRequest,
    current: EnrollmentCapacityCounters,
) -> EnrollmentCapacityDecision {
    let Some(identity_server) = current.identity_server.incremented() else {
        return enrollment_identity_refusal(
            request,
            IdentityCapacityScope::Server,
            current.identity_server,
        );
    };
    let Some(identity_conversation) = current.identity_conversation.incremented() else {
        return enrollment_identity_refusal(
            request,
            IdentityCapacityScope::Conversation,
            current.identity_conversation,
        );
    };
    let Some(live_receipt_server) = current.live_receipt_server.incremented() else {
        return enrollment_receipt_refusal(
            request,
            EnrollmentReceiptCapacityScope::LiveReceiptServer,
            current.live_receipt_server,
        );
    };
    let Some(provenance_server) = current.provenance_server.incremented() else {
        return enrollment_receipt_refusal(
            request,
            EnrollmentReceiptCapacityScope::ProvenanceServer,
            current.provenance_server,
        );
    };
    let Some(provenance_conversation) = current.provenance_conversation.incremented() else {
        return enrollment_receipt_refusal(
            request,
            EnrollmentReceiptCapacityScope::ProvenanceConversation,
            current.provenance_conversation,
        );
    };

    EnrollmentCapacityDecision::Commit(EnrollmentCapacityCommit {
        resulting: ResultingEnrollmentCapacityCounters {
            identity_server,
            identity_conversation,
            live_receipt_server,
            live_receipt_participant: current.live_receipt_participant.reserved(),
            provenance_server,
            provenance_conversation,
            provenance_participant: current.provenance_participant.reserved(),
        },
    })
}

/// The five ordered receipt/provenance counters for credential attach.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CredentialAttachCapacityCounters {
    live_receipt_server: CapacityCounter,
    live_receipt_participant: CapacityCounter,
    provenance_server: CapacityCounter,
    provenance_conversation: CapacityCounter,
    provenance_participant: CapacityCounter,
}

impl CredentialAttachCapacityCounters {
    /// Creates the complete credential-attach counter snapshot.
    #[must_use]
    pub const fn new(
        live_receipt_server: CapacityCounter,
        live_receipt_participant: CapacityCounter,
        provenance_server: CapacityCounter,
        provenance_conversation: CapacityCounter,
        provenance_participant: CapacityCounter,
    ) -> Self {
        Self {
            live_receipt_server,
            live_receipt_participant,
            provenance_server,
            provenance_conversation,
            provenance_participant,
        }
    }

    /// Returns server-wide live-receipt occupancy.
    #[must_use]
    pub const fn live_receipt_server(self) -> CapacityCounter {
        self.live_receipt_server
    }

    /// Returns participant live-receipt occupancy.
    #[must_use]
    pub const fn live_receipt_participant(self) -> CapacityCounter {
        self.live_receipt_participant
    }

    /// Returns server-wide provenance occupancy.
    #[must_use]
    pub const fn provenance_server(self) -> CapacityCounter {
        self.provenance_server
    }

    /// Returns conversation provenance occupancy.
    #[must_use]
    pub const fn provenance_conversation(self) -> CapacityCounter {
        self.provenance_conversation
    }

    /// Returns participant provenance occupancy.
    #[must_use]
    pub const fn provenance_participant(self) -> CapacityCounter {
        self.provenance_participant
    }
}

/// Atomic successful credential-attach capacity reservation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CredentialAttachCapacityCommit {
    resulting: CredentialAttachCapacityCounters,
}

impl CredentialAttachCapacityCommit {
    /// Returns all five incremented receipt/provenance counters together.
    #[must_use]
    pub const fn resulting(self) -> CredentialAttachCapacityCounters {
        self.resulting
    }
}

/// Exhaustive stage-8 credential-attach runtime-capacity result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CredentialAttachCapacityDecision {
    /// All five receipt/provenance reservations may commit together.
    Commit(CredentialAttachCapacityCommit),
    /// Exact first-full receipt/provenance scope, bound to credential attach.
    Respond(CredentialAttachResponse),
}

/// Applies credential attach's exact five-scope runtime-capacity order.
#[must_use]
pub const fn select_credential_attach_capacity(
    request: &CredentialAttachRequest,
    current: CredentialAttachCapacityCounters,
) -> CredentialAttachCapacityDecision {
    let Some(live_receipt_server) = current.live_receipt_server.incremented() else {
        return credential_attach_receipt_refusal(
            request,
            ReceiptCapacityScope::LiveReceiptServer,
            current.live_receipt_server,
        );
    };
    let Some(live_receipt_participant) = current.live_receipt_participant.incremented() else {
        return credential_attach_receipt_refusal(
            request,
            ReceiptCapacityScope::LiveReceiptParticipant,
            current.live_receipt_participant,
        );
    };
    let Some(provenance_server) = current.provenance_server.incremented() else {
        return credential_attach_receipt_refusal(
            request,
            ReceiptCapacityScope::ProvenanceServer,
            current.provenance_server,
        );
    };
    let Some(provenance_conversation) = current.provenance_conversation.incremented() else {
        return credential_attach_receipt_refusal(
            request,
            ReceiptCapacityScope::ProvenanceConversation,
            current.provenance_conversation,
        );
    };
    let Some(provenance_participant) = current.provenance_participant.incremented() else {
        return credential_attach_receipt_refusal(
            request,
            ReceiptCapacityScope::ProvenanceParticipant,
            current.provenance_participant,
        );
    };

    CredentialAttachCapacityDecision::Commit(CredentialAttachCapacityCommit {
        resulting: CredentialAttachCapacityCounters {
            live_receipt_server,
            live_receipt_participant,
            provenance_server,
            provenance_conversation,
            provenance_participant,
        },
    })
}

const fn enrollment_identity_refusal(
    request: &EnrollmentRequest,
    scope: IdentityCapacityScope,
    counter: CapacityCounter,
) -> EnrollmentCapacityDecision {
    EnrollmentCapacityDecision::Respond(EnrollmentResponse::identity_capacity_exceeded(
        IdentityCapacityExceeded {
            request: enrollment_envelope(request),
            scope,
            limit: counter.limit(),
            occupied: counter.occupied(),
        },
    ))
}

const fn enrollment_receipt_refusal(
    request: &EnrollmentRequest,
    scope: EnrollmentReceiptCapacityScope,
    counter: CapacityCounter,
) -> EnrollmentCapacityDecision {
    EnrollmentCapacityDecision::Respond(EnrollmentResponse::receipt_capacity_exceeded(
        enrollment_envelope(request),
        scope,
        counter.limit(),
        counter.occupied(),
    ))
}

const fn credential_attach_receipt_refusal(
    request: &CredentialAttachRequest,
    scope: ReceiptCapacityScope,
    counter: CapacityCounter,
) -> CredentialAttachCapacityDecision {
    CredentialAttachCapacityDecision::Respond(CredentialAttachResponse::receipt_capacity_exceeded(
        attach_envelope(request),
        scope,
        counter.limit(),
        counter.occupied(),
    ))
}

const fn enrollment_envelope(request: &EnrollmentRequest) -> EnrollmentEnvelope {
    EnrollmentEnvelope {
        conversation_id: request.conversation_id,
        enrollment_token: request.enrollment_token,
    }
}

const fn attach_envelope(request: &CredentialAttachRequest) -> AttachEnvelope {
    AttachEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        attach_attempt_token: request.attach_attempt_token,
        accept_marker_delivery_seq: request.accept_marker_delivery_seq,
    }
}
