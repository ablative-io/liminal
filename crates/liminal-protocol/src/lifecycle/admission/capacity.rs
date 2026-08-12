use core::num::NonZeroU64;

use crate::wire::{
    AttachEnvelope, CredentialAttachRequest, CredentialAttachResponse, EnrollmentEnvelope,
    EnrollmentRequest, EnrollmentResponse, IdentityCapacityExceeded, IdentityCapacityScope,
    ParticipantId,
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

    /// The window as enrollment leaves it when the operation fills NOTHING:
    /// nonzero size, zero occupancy.
    ///
    /// Board #37: enrollment mints a receipt body but retains no provenance
    /// fingerprint — nothing has yet proven possession of the secret that
    /// receipt minted — so the provenance window is reserved and still empty.
    const fn unfilled(self) -> CapacityCounter {
        self.counter
    }
}

/// Whether a per-participant window entry landed into headroom or had to
/// displace the window's oldest member to make room.
///
/// Both arms LAND the new entry. The window size is a bound on retention, not
/// a refusal threshold: per-participant pressure is self-inflicted (your own
/// churn displaces your own oldest fingerprint), so the number bounds memory
/// without ever refusing an honest arrival.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantWindowAdmission {
    /// The window had headroom; occupancy rose by one and nothing was lost.
    Landed,
    /// The window was exactly full; its OLDEST in-window member is displaced
    /// so the new entry can land, and occupancy stays exactly at the bound.
    Displaced,
}

/// One per-participant window's admission paired with the occupancy it leaves.
///
/// There is no refusal arm by construction. `resulting` never exceeds the
/// signed window size, and under [`ParticipantWindowAdmission::Displaced`] it
/// is exactly that size — the bound holds exactly, in both arms.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParticipantWindowCommit {
    admission: ParticipantWindowAdmission,
    resulting: CapacityCounter,
}

impl ParticipantWindowCommit {
    /// Returns whether landing displaced the window's oldest member.
    #[must_use]
    pub const fn admission(self) -> ParticipantWindowAdmission {
        self.admission
    }

    /// Returns whether this admission displaced an older member — the fact
    /// the server's visibility surface counts.
    #[must_use]
    pub const fn displaced(self) -> bool {
        matches!(self.admission, ParticipantWindowAdmission::Displaced)
    }

    /// Returns the post-admission occupancy, always within the window.
    #[must_use]
    pub const fn resulting(self) -> CapacityCounter {
        self.resulting
    }
}

/// Admits one entry into a per-participant retention window.
///
/// A window with headroom takes the entry and grows by one. A full window
/// displaces its oldest member and stays exactly full. The entry ALWAYS
/// lands: this selector has no refusal arm, which is the whole of Tom's
/// governing sentence — *no configured number refuses an honest arrival* —
/// expressed in the type.
#[must_use]
pub const fn select_participant_window(current: CapacityCounter) -> ParticipantWindowCommit {
    match current.incremented() {
        Some(resulting) => ParticipantWindowCommit {
            admission: ParticipantWindowAdmission::Landed,
            resulting,
        },
        None => ParticipantWindowCommit {
            admission: ParticipantWindowAdmission::Displaced,
            resulting: current,
        },
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

/// The stage-8 counters one fresh enrollment decides against.
///
/// Only the two IDENTITY counters can refuse. The two per-participant window
/// counters use [`FreshParticipantCapacityCounter`], proving their occupancy
/// is zero and their sizes nonzero before identity mint — which is exactly
/// what makes a fresh participant's first receipt land without a decision.
///
/// # Lane p0-39: the shared pools are gone from this decision
///
/// `LiveReceiptServer`, `ProvenanceServer`, and `ProvenanceConversation` are
/// no longer admission gates in any scope. They are where an honest THIRD
/// PARTY would meet a number someone else's churn consumed, and no configured
/// refusal is tolerable there; their retention is bounded by the TTL windows
/// alone, with a reporting tripwire in place of a wall. The wire scopes remain
/// assigned and defined — they are simply never emitted from these paths.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnrollmentCapacityCounters {
    identity_server: CapacityCounter,
    identity_conversation: CapacityCounter,
    live_receipt_participant: FreshParticipantCapacityCounter,
    provenance_participant: FreshParticipantCapacityCounter,
}

impl EnrollmentCapacityCounters {
    /// Creates the complete reachable enrollment counter snapshot.
    #[must_use]
    pub const fn new(
        identity_server: CapacityCounter,
        identity_conversation: CapacityCounter,
        live_receipt_participant: FreshParticipantCapacityCounter,
        provenance_participant: FreshParticipantCapacityCounter,
    ) -> Self {
        Self {
            identity_server,
            identity_conversation,
            live_receipt_participant,
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

    /// Returns the provably empty participant live-receipt window.
    #[must_use]
    pub const fn live_receipt_participant(self) -> FreshParticipantCapacityCounter {
        self.live_receipt_participant
    }

    /// Returns the provably empty participant provenance window.
    #[must_use]
    pub const fn provenance_participant(self) -> FreshParticipantCapacityCounter {
        self.provenance_participant
    }
}

/// The post-enrollment identity counters and per-participant window occupancy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResultingEnrollmentCapacityCounters {
    identity_server: CapacityCounter,
    identity_conversation: CapacityCounter,
    live_receipt_participant: CapacityCounter,
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

    /// Returns the newly minted participant's live-receipt window occupancy.
    #[must_use]
    pub const fn live_receipt_participant(self) -> CapacityCounter {
        self.live_receipt_participant
    }

    /// Returns the newly minted participant's provenance window occupancy.
    ///
    /// Board #37: this is zero at mint — nothing has proven possession of the
    /// secret the enrollment receipt just minted, so no fingerprint is
    /// retained yet. The window is reserved, not filled.
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
    /// Both identity reservations and both window reservations may commit.
    Commit(EnrollmentCapacityCommit),
    /// Exact first-full IDENTITY scope, bound to enrollment. The receipt
    /// scopes have no refusal arm on this path at all.
    Respond(EnrollmentResponse),
}

/// Applies the enrollment runtime-capacity order atomically.
///
/// The order is identity Server then identity Conversation — the complete
/// refusable set. A refusal exposes only the first full scope; success
/// carries the post-increment identity counters and the fresh participant's
/// two reserved windows.
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

    EnrollmentCapacityDecision::Commit(EnrollmentCapacityCommit {
        resulting: ResultingEnrollmentCapacityCounters {
            identity_server,
            identity_conversation,
            live_receipt_participant: current.live_receipt_participant.reserved(),
            provenance_participant: current.provenance_participant.unfilled(),
        },
    })
}

/// The two per-participant retention windows credential attach admits into.
///
/// The three shared scopes this snapshot used to carry are gone: they no
/// longer gate anything, so passing them here would be an unread number
/// pretending to be a decision input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CredentialAttachCapacityCounters {
    live_receipt_participant: CapacityCounter,
    provenance_participant: CapacityCounter,
}

impl CredentialAttachCapacityCounters {
    /// Creates the complete credential-attach window snapshot.
    #[must_use]
    pub const fn new(
        live_receipt_participant: CapacityCounter,
        provenance_participant: CapacityCounter,
    ) -> Self {
        Self {
            live_receipt_participant,
            provenance_participant,
        }
    }

    /// Returns participant live-receipt window occupancy.
    #[must_use]
    pub const fn live_receipt_participant(self) -> CapacityCounter {
        self.live_receipt_participant
    }

    /// Returns participant provenance window occupancy.
    #[must_use]
    pub const fn provenance_participant(self) -> CapacityCounter {
        self.provenance_participant
    }
}

/// Atomic credential-attach window admission.
///
/// There is no refusal counterpart. Every credential attach that reaches
/// stage 8 admits; the only outcome carried here is whether each window had
/// to displace its oldest member to make room.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CredentialAttachCapacityCommit {
    live_receipt_participant: ParticipantWindowCommit,
    provenance_participant: ParticipantWindowCommit,
}

impl CredentialAttachCapacityCommit {
    /// Returns the participant live-receipt window's admission.
    #[must_use]
    pub const fn live_receipt_participant(self) -> ParticipantWindowCommit {
        self.live_receipt_participant
    }

    /// Returns the participant provenance window's admission.
    #[must_use]
    pub const fn provenance_participant(self) -> ParticipantWindowCommit {
        self.provenance_participant
    }
}

/// Admits one credential attach into both per-participant windows.
///
/// This selector is TOTAL: it returns a commit for every input, because the
/// (N+1)th honest fingerprint of a participant always lands. Whichever window
/// was full displaces its own oldest member, and the caller applies exactly
/// that displacement to the ledger and the slot from one shared plan.
#[must_use]
pub const fn select_credential_attach_capacity(
    current: CredentialAttachCapacityCounters,
) -> CredentialAttachCapacityCommit {
    CredentialAttachCapacityCommit {
        live_receipt_participant: select_participant_window(current.live_receipt_participant),
        provenance_participant: select_participant_window(current.provenance_participant),
    }
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
