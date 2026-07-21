use alloc::{collections::BTreeMap, vec::Vec};

use crate::algebra::{FloorComputation, floor_transition};
use crate::wire::{
    AckCommitted, AckGap, AckNoOp, AckRegression, BindingEpoch, ConversationId, DeliverySeq,
    ParticipantAck, ParticipantAckEnvelope, ParticipantId, ParticipantIndex,
};

use super::{ClaimFrontiers, FrontierBinding, edge::ClosureDebt};

/// Participant-scoped cursor progress key mandated by extraction Fix 2.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CursorProgressKey {
    /// Permanent participant index.
    pub participant_index: ParticipantIndex,
    /// Requested cumulative boundary.
    pub boundary: DeliverySeq,
}

/// Durable cursor-progress fact state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorProgressFact {
    /// Boundary remains independently fireable for this participant.
    Pending,
    /// Boundary was covered by this participant's committed cumulative ack.
    Consumed,
}

/// Variable participant-scoped cursor facts; no fixed occurrence array exists.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CursorProgressFacts {
    facts: BTreeMap<CursorProgressKey, CursorProgressFact>,
}

/// Validated durable delivery obligations for one participant ack frontier.
///
/// The outbox supplies its sorted live-obligation index, while the protocol
/// owns participant/frontier binding and endpoint membership. Internal
/// conversation-sequence gaps are legal; a forward ack endpoint must occur in
/// this exact recipient index.
///
/// This testimony is move-only and cannot be assembled from independent public
/// fields:
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::RecipientAckObligations;
///
/// fn clone_testimony(value: &RecipientAckObligations) -> RecipientAckObligations {
///     value.clone()
/// }
/// ```
#[derive(Debug, PartialEq, Eq)]
pub struct RecipientAckObligations {
    participant_id: ParticipantId,
    acknowledged_through: DeliverySeq,
    delivery_sequences: Vec<DeliverySeq>,
}

/// Malformed durable recipient-obligation testimony.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecipientAckObligationsError {
    /// An indexed obligation is already at or below the durable ack frontier.
    NotLive {
        /// Durable participant acknowledgement frontier.
        acknowledged_through: DeliverySeq,
        /// Invalid obligation endpoint.
        delivery_seq: DeliverySeq,
    },
    /// Obligation endpoints are not strictly increasing and duplicate-free.
    NotStrictlyIncreasing {
        /// Previous obligation in the supplied index.
        previous: DeliverySeq,
        /// Current non-increasing obligation.
        current: DeliverySeq,
    },
}

/// The testimony belongs to another participant or durable ack prestate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecipientAckObligationsContextError {
    /// The testimony names another permanent participant.
    Participant {
        /// Participant required by the authorized request.
        expected: ParticipantId,
        /// Participant carried by the testimony.
        actual: ParticipantId,
    },
    /// The testimony was sealed against another durable ack frontier.
    AcknowledgedThrough {
        /// Cursor required by the authorized request prestate.
        expected: DeliverySeq,
        /// Cursor carried by the testimony.
        actual: DeliverySeq,
    },
}

impl RecipientAckObligations {
    /// Validates one recipient's complete current live-obligation index.
    ///
    /// # Errors
    ///
    /// Returns [`RecipientAckObligationsError`] when an obligation is already
    /// discharged or the index is not strictly increasing and duplicate-free.
    pub fn try_new(
        participant_id: ParticipantId,
        acknowledged_through: DeliverySeq,
        delivery_sequences: Vec<DeliverySeq>,
    ) -> Result<Self, RecipientAckObligationsError> {
        let mut previous = None;
        for delivery_seq in &delivery_sequences {
            if *delivery_seq <= acknowledged_through {
                return Err(RecipientAckObligationsError::NotLive {
                    acknowledged_through,
                    delivery_seq: *delivery_seq,
                });
            }
            if let Some(previous) = previous
                && *delivery_seq <= previous
            {
                return Err(RecipientAckObligationsError::NotStrictlyIncreasing {
                    previous,
                    current: *delivery_seq,
                });
            }
            previous = Some(*delivery_seq);
        }
        Ok(Self {
            participant_id,
            acknowledged_through,
            delivery_sequences,
        })
    }

    pub(in crate::lifecycle) fn contains_endpoint(
        &self,
        participant_id: ParticipantId,
        acknowledged_through: DeliverySeq,
        endpoint: DeliverySeq,
    ) -> Result<bool, RecipientAckObligationsContextError> {
        if self.participant_id != participant_id {
            return Err(RecipientAckObligationsContextError::Participant {
                expected: participant_id,
                actual: self.participant_id,
            });
        }
        if self.acknowledged_through != acknowledged_through {
            return Err(RecipientAckObligationsContextError::AcknowledgedThrough {
                expected: acknowledged_through,
                actual: self.acknowledged_through,
            });
        }
        Ok(self.delivery_sequences.binary_search(&endpoint).is_ok())
    }
}

/// One currently bound participant's durable cumulative-cursor state.
///
/// All fields are private so cursor advancement can occur only through the
/// monotonic cumulative-ack transition below.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BoundParticipantCursor {
    participant_id: ParticipantId,
    active_binding_epoch: BindingEpoch,
    cursor: DeliverySeq,
}

impl BoundParticipantCursor {
    /// Creates one bound participant cursor at its already-durable position.
    #[must_use]
    pub const fn new(
        participant_id: ParticipantId,
        active_binding_epoch: BindingEpoch,
        cursor: DeliverySeq,
    ) -> Self {
        Self {
            participant_id,
            active_binding_epoch,
            cursor,
        }
    }

    /// Returns the participant's permanent index.
    #[must_use]
    pub const fn participant_index(self) -> ParticipantIndex {
        self.participant_id
    }

    /// Returns the participant's permanent identifier.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        self.participant_id
    }

    /// Returns the binding epoch authorized to advance this cursor.
    #[must_use]
    pub const fn active_binding_epoch(self) -> BindingEpoch {
        self.active_binding_epoch
    }

    /// Returns the durable cumulative cursor.
    #[must_use]
    pub const fn cursor(self) -> DeliverySeq {
        self.cursor
    }
}

/// Private binding-tagged participant state for one coupled debt episode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DebtParticipantCursor {
    participant_id: ParticipantId,
    binding: FrontierBinding,
    cursor: DeliverySeq,
}

impl From<BoundParticipantCursor> for DebtParticipantCursor {
    fn from(participant: BoundParticipantCursor) -> Self {
        Self {
            participant_id: participant.participant_id,
            binding: FrontierBinding::Bound(participant.active_binding_epoch),
            cursor: participant.cursor,
        }
    }
}

impl DebtParticipantCursor {
    const fn advance_to(&mut self, boundary: DeliverySeq) {
        if boundary > self.cursor {
            self.cursor = boundary;
        }
    }

    const fn binding_epoch(self) -> BindingEpoch {
        match self.binding {
            FrontierBinding::Bound(epoch) | FrontierBinding::Detached(epoch) => epoch,
        }
    }
}

/// Construction failure for a participant-scoped nonzero-debt episode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorEpisodeBuildError {
    /// Two cursor states name the same permanent participant identifier/index.
    DuplicateParticipant {
        /// Repeated permanent participant identifier and index.
        participant_id: ParticipantId,
    },
    /// Hard observer progress cannot be beyond the candidate watermark.
    ObserverBeyondHighWatermark {
        /// Durable observer progress.
        observer_progress: DeliverySeq,
        /// Candidate high watermark `H'`.
        candidate_high_watermark: DeliverySeq,
    },
    /// A bound cursor cannot acknowledge beyond the candidate watermark.
    CursorBeyondHighWatermark {
        /// Permanent participant identifier/index.
        participant_id: ParticipantId,
        /// Durable participant cursor.
        cursor: DeliverySeq,
        /// Candidate high watermark `H'`.
        candidate_high_watermark: DeliverySeq,
    },
    /// The supplied first-retained floor is beyond checked `H' + 1`.
    FloorBeyondRetainedEnd {
        /// Supplied current floor `F`.
        current_floor: u128,
        /// Checked one-past retained end `H' + 1`.
        retained_end: u128,
    },
    /// The append-free ack envelope selected a floor below the actual base.
    CapacityFloorBelowBase {
        /// Supplied committing-class `cap_floor`.
        cap_floor: u128,
        /// `max(F, preferred_floor)` for the initial episode state.
        base_floor: u128,
    },
    /// The supplied capacity floor would overtake hard observer retention.
    CapacityFloorBeyondObserver {
        /// Supplied committing-class `cap_floor`.
        cap_floor: u128,
        /// Greatest floor allowed by hard observer retention, `o + 1`.
        observer_limit: u128,
    },
}

/// Authority failure before a cumulative-ack transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CumulativeAckAuthorizationError {
    /// The episode does not contain the selected permanent participant index.
    ParticipantIndexUnknown,
    /// The request names another conversation.
    ConversationMismatch,
    /// The request identifier does not match the selected participant index.
    ParticipantMismatch,
    /// The request generation does not match the active binding epoch.
    GenerationMismatch,
    /// The receiving connection does not own the participant's active epoch.
    BindingEpochMismatch,
    /// Durable recipient-obligation testimony belongs to another prestate.
    ObligationContext {
        /// Exact testimony mismatch.
        error: RecipientAckObligationsContextError,
    },
    /// A fixed wire-outcome constructor rejected an already-proven cursor relation.
    CursorRelationInvariant,
}

/// Exhaustive outcome of an authority-checked normal cumulative ack.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CumulativeAckOutcome {
    /// The cursor advanced and its participant-scoped fact was consumed.
    Committed(AckCommitted),
    /// The request exactly repeated the durable cursor.
    NoOp(AckNoOp),
    /// The forward endpoint lacks the availability testimony required by the selector.
    Gap(AckGap),
    /// The request boundary was below the durable cursor.
    Regression(AckRegression),
}

/// Participant-scoped cursor accounting for one provably nonzero-debt episode.
///
/// The wrapper requires [`ClosureDebt`], whose constructor rejects componentwise
/// zero. Unlike the frozen document's defective fixed occurrence array, its
/// progress facts are variable and keyed by `(participant_index, boundary)` as
/// mandated by `docs/design/LP-EXTRACTION-GOAL.md` Fix 2. It also owns the hard
/// observer position, candidate watermark, retained-suffix range, and the
/// append-free ack class's capacity floor. Every committed ack recomputes
/// `F' = max(F, min(m, o) + 1, cap_floor)` from those durable values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NonzeroDebtCursorEpisode {
    conversation_id: ConversationId,
    debt: ClosureDebt,
    observer_progress: DeliverySeq,
    candidate_high_watermark: DeliverySeq,
    cap_floor: u128,
    floor: FloorComputation,
    participants: BTreeMap<ParticipantIndex, DebtParticipantCursor>,
    facts: CursorProgressFacts,
}

/// Deterministic cursor-fact serialization failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorFactEncodeError {
    /// Participant count cannot fit the variable format's `u32` count.
    TooManyParticipants,
    /// Fact count cannot fit the variable format's `u32` count.
    TooManyFacts,
    /// Encoded byte length overflowed the platform allocation domain.
    LengthOverflow,
}

impl CursorProgressFacts {
    /// Creates an empty participant-scoped fact map.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            facts: BTreeMap::new(),
        }
    }

    /// Inserts an independently fireable `(participant_index, boundary)` fact.
    ///
    /// Returns `true` only when the exact pair was not already present.
    pub fn record(&mut self, key: CursorProgressKey) -> bool {
        if self.facts.contains_key(&key) {
            return false;
        }
        self.facts.insert(key, CursorProgressFact::Pending);
        true
    }

    /// Marks every pending boundary at or below `through` consumed for one
    /// participant, without touching another participant's identical boundary.
    pub fn consume_through(
        &mut self,
        participant_index: ParticipantIndex,
        through: DeliverySeq,
    ) -> Vec<CursorProgressKey> {
        let mut consumed = Vec::new();
        for (key, fact) in &mut self.facts {
            if key.participant_index == participant_index
                && key.boundary <= through
                && *fact == CursorProgressFact::Pending
            {
                *fact = CursorProgressFact::Consumed;
                consumed.push(*key);
            }
        }
        consumed
    }

    /// Returns one exact fact state.
    #[must_use]
    pub fn get(&self, key: CursorProgressKey) -> Option<CursorProgressFact> {
        self.facts.get(&key).copied()
    }

    /// Number of distinct participant/boundary pairs.
    #[must_use]
    pub fn len(&self) -> usize {
        self.facts.len()
    }

    /// Returns whether no cursor fact exists.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.facts.is_empty()
    }

    /// Deterministically serializes the variable map in key order.
    ///
    /// Format: `count:u32`, followed by `participant_index:u64`,
    /// `boundary:u64`, and `state:u8` (`0` Pending, `1` Consumed) per fact.
    /// This is lifecycle storage, not the participant network frame format.
    ///
    /// # Errors
    ///
    /// Returns [`CursorFactEncodeError`] if count or allocation length cannot be
    /// represented.
    pub fn encode(&self) -> Result<Vec<u8>, CursorFactEncodeError> {
        let count =
            u32::try_from(self.facts.len()).map_err(|_| CursorFactEncodeError::TooManyFacts)?;
        let body_len = self
            .facts
            .len()
            .checked_mul(17)
            .ok_or(CursorFactEncodeError::LengthOverflow)?;
        let capacity = 4_usize
            .checked_add(body_len)
            .ok_or(CursorFactEncodeError::LengthOverflow)?;
        let mut bytes = Vec::with_capacity(capacity);
        bytes.extend_from_slice(&count.to_be_bytes());
        for (key, fact) in &self.facts {
            bytes.extend_from_slice(&key.participant_index.to_be_bytes());
            bytes.extend_from_slice(&key.boundary.to_be_bytes());
            bytes.push(match fact {
                CursorProgressFact::Pending => 0,
                CursorProgressFact::Consumed => 1,
            });
        }
        Ok(bytes)
    }
}

impl NonzeroDebtCursorEpisode {
    /// Creates one nonzero-debt episode with retained-suffix and floor state.
    ///
    /// `candidate_high_watermark` is `H'`; the retained suffix is the inclusive
    /// sequence range from `current_floor` through `H'`, or empty when the floor
    /// is checked `H' + 1`. `cap_floor` is the committing append-free ack
    /// class's actual capacity floor at this initial state.
    ///
    /// # Errors
    ///
    /// Returns [`CursorEpisodeBuildError`] for a duplicate participant, a
    /// cursor/observer beyond `H'`, an invalid retained range, or a capacity
    /// floor outside the initial base and observer bounds.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        conversation_id: ConversationId,
        debt: ClosureDebt,
        observer_progress: DeliverySeq,
        candidate_high_watermark: DeliverySeq,
        current_floor: u128,
        cap_floor: u128,
        participants: Vec<BoundParticipantCursor>,
    ) -> Result<Self, CursorEpisodeBuildError> {
        if observer_progress > candidate_high_watermark {
            return Err(CursorEpisodeBuildError::ObserverBeyondHighWatermark {
                observer_progress,
                candidate_high_watermark,
            });
        }
        let retained_end = u128::from(candidate_high_watermark) + 1;
        if current_floor > retained_end {
            return Err(CursorEpisodeBuildError::FloorBeyondRetainedEnd {
                current_floor,
                retained_end,
            });
        }

        let mut indexed = BTreeMap::new();
        for participant in participants {
            if participant.cursor > candidate_high_watermark {
                return Err(CursorEpisodeBuildError::CursorBeyondHighWatermark {
                    participant_id: participant.participant_id,
                    cursor: participant.cursor,
                    candidate_high_watermark,
                });
            }
            if indexed.contains_key(&participant.participant_id) {
                return Err(CursorEpisodeBuildError::DuplicateParticipant {
                    participant_id: participant.participant_id,
                });
            }
            indexed.insert(
                participant.participant_id,
                DebtParticipantCursor::from(participant),
            );
        }

        let minimum_member_cursor = indexed.values().map(|participant| participant.cursor).min();
        let floor = floor_transition(
            current_floor,
            minimum_member_cursor,
            candidate_high_watermark,
            observer_progress,
            cap_floor,
        );
        let base_floor = current_floor.max(floor.preferred_floor);
        if cap_floor < base_floor {
            return Err(CursorEpisodeBuildError::CapacityFloorBelowBase {
                cap_floor,
                base_floor,
            });
        }
        let observer_limit = u128::from(observer_progress) + 1;
        if cap_floor > observer_limit {
            return Err(CursorEpisodeBuildError::CapacityFloorBeyondObserver {
                cap_floor,
                observer_limit,
            });
        }

        Ok(Self {
            conversation_id,
            debt,
            observer_progress,
            candidate_high_watermark,
            cap_floor,
            floor,
            participants: indexed,
            facts: CursorProgressFacts::new(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn restore(
        conversation_id: ConversationId,
        debt: ClosureDebt,
        observer_progress: DeliverySeq,
        candidate_high_watermark: DeliverySeq,
        current_floor: u128,
        cap_floor: u128,
        participants: Vec<BoundParticipantCursor>,
        facts: Vec<(CursorProgressKey, CursorProgressFact)>,
    ) -> Option<Self> {
        let mut episode = Self::new(
            conversation_id,
            debt,
            observer_progress,
            candidate_high_watermark,
            current_floor,
            cap_floor,
            participants,
        )
        .ok()?;
        if episode.floor.resulting_floor != current_floor {
            return None;
        }
        for (key, fact) in facts {
            let participant = episode.participants.get(&key.participant_index)?;
            if key.boundary > candidate_high_watermark
                || (fact == CursorProgressFact::Consumed && key.boundary > participant.cursor)
                || (fact == CursorProgressFact::Pending && key.boundary <= participant.cursor)
                || episode.facts.facts.insert(key, fact).is_some()
            {
                return None;
            }
        }
        Some(episode)
    }

    pub(super) fn from_claim_frontiers(
        frontiers: &ClaimFrontiers,
        debt: ClosureDebt,
        observer_progress: DeliverySeq,
    ) -> Result<Self, CursorEpisodeBuildError> {
        let candidate_high_watermark = frontiers.sequence().ledger().high_watermark();
        if observer_progress > candidate_high_watermark {
            return Err(CursorEpisodeBuildError::ObserverBeyondHighWatermark {
                observer_progress,
                candidate_high_watermark,
            });
        }
        let current_floor = frontiers.retained_floor();
        let retained_end = u128::from(candidate_high_watermark) + 1;
        if current_floor > retained_end {
            return Err(CursorEpisodeBuildError::FloorBeyondRetainedEnd {
                current_floor,
                retained_end,
            });
        }
        let participants = frontiers
            .active_identities()
            .participants()
            .iter()
            .map(|participant| {
                (
                    participant.participant_index(),
                    DebtParticipantCursor {
                        participant_id: participant.participant_index(),
                        binding: participant.binding(),
                        cursor: participant.cursor(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let minimum_member_cursor = participants
            .values()
            .map(|participant| participant.cursor)
            .min();
        let preferred = floor_transition(
            current_floor,
            minimum_member_cursor,
            candidate_high_watermark,
            observer_progress,
            current_floor,
        );
        let cap_floor = current_floor.max(preferred.preferred_floor);
        let floor = floor_transition(
            current_floor,
            minimum_member_cursor,
            candidate_high_watermark,
            observer_progress,
            cap_floor,
        );
        Ok(Self {
            conversation_id: frontiers.conversation_id(),
            debt,
            observer_progress,
            candidate_high_watermark,
            cap_floor,
            floor,
            participants,
            facts: CursorProgressFacts::new(),
        })
    }

    pub(super) fn reconcile_claim_frontiers(
        mut self,
        frontiers: &ClaimFrontiers,
        debt: ClosureDebt,
        observer_progress: DeliverySeq,
    ) -> Result<Self, CursorEpisodeBuildError> {
        let mut next = Self::from_claim_frontiers(frontiers, debt, observer_progress)?;
        for (key, fact) in self.facts.facts {
            let Some(participant) = next.participants.get(&key.participant_index) else {
                continue;
            };
            if key.boundary <= next.candidate_high_watermark {
                let reconciled = if key.boundary <= participant.cursor {
                    CursorProgressFact::Consumed
                } else {
                    fact
                };
                next.facts.facts.insert(key, reconciled);
            }
        }
        for participant in next.participants.values() {
            let _ = next
                .facts
                .consume_through(participant.participant_id, participant.cursor);
        }
        self.participants.clear();
        Ok(next)
    }

    pub(super) fn participant_binding(
        &self,
        participant_index: ParticipantIndex,
    ) -> Option<(FrontierBinding, DeliverySeq)> {
        self.participants
            .get(&participant_index)
            .map(|participant| (participant.binding, participant.cursor))
    }

    /// Returns the conversation owning this aggregate for internal operation
    /// prestate validation.
    pub(super) const fn conversation_id(&self) -> ConversationId {
        self.conversation_id
    }

    /// Returns the exact nonzero debt proving this episode is active.
    #[must_use]
    pub const fn debt(&self) -> ClosureDebt {
        self.debt
    }

    /// Returns hard observer progress `o` used by every ack floor transition.
    #[must_use]
    pub const fn observer_progress(&self) -> DeliverySeq {
        self.observer_progress
    }

    /// Returns the candidate high watermark `H'` and retained-suffix end.
    #[must_use]
    pub const fn candidate_high_watermark(&self) -> DeliverySeq {
        self.candidate_high_watermark
    }

    /// Returns the append-free ack transaction class's current `cap_floor`.
    #[must_use]
    pub const fn cap_floor(&self) -> u128 {
        self.cap_floor
    }

    /// Returns the latest reproducible document floor computation.
    #[must_use]
    pub const fn floor_computation(&self) -> FloorComputation {
        self.floor
    }

    /// Returns the first retained sequence, or `None` for an empty suffix.
    #[must_use]
    pub fn retained_suffix_start(&self) -> Option<DeliverySeq> {
        if self.floor.resulting_floor > u128::from(self.candidate_high_watermark) {
            return None;
        }
        DeliverySeq::try_from(self.floor.resulting_floor).ok()
    }

    /// Returns whether one durable sequence remains in the retained suffix.
    #[must_use]
    pub fn retains(&self, delivery_seq: DeliverySeq) -> bool {
        u128::from(delivery_seq) >= self.floor.resulting_floor
            && delivery_seq <= self.candidate_high_watermark
    }

    /// Returns one indexed bound participant cursor.
    #[must_use]
    pub fn participant(
        &self,
        participant_index: ParticipantIndex,
    ) -> Option<BoundParticipantCursor> {
        let participant = self.participants.get(&participant_index).copied()?;
        let FrontierBinding::Bound(epoch) = participant.binding else {
            return None;
        };
        Some(BoundParticipantCursor::new(
            participant.participant_id,
            epoch,
            participant.cursor,
        ))
    }

    /// Returns the participant-scoped cursor fact map.
    #[must_use]
    pub const fn facts(&self) -> &CursorProgressFacts {
        &self.facts
    }

    /// Applies one authority-checked cumulative normal acknowledgement.
    ///
    /// `receiving_binding_epoch` identifies the connection epoch on which the
    /// request arrived. This scalar entry point preserves the Unit 1 contiguous
    /// suffix selector; Unit 2 production uses
    /// [`Self::acknowledge_with_obligations`] so restart-safe durable recipient
    /// testimony, rather than volatile offered progress, decides endpoint
    /// availability. A successful advance records and consumes the fact keyed by
    /// the selected participant index and requested boundary; any lower pending
    /// facts for that participant are consumed in the same transition.
    /// It then recomputes the physical floor from the post-ack minimum cursor,
    /// hard observer progress, current floor, and append-free ack `cap_floor`.
    /// Another participant's equal boundary is never touched.
    ///
    /// # Errors
    ///
    /// Returns [`CumulativeAckAuthorizationError`] before any mutation when the
    /// conversation, participant, generation, or active binding epoch differs.
    pub fn acknowledge(
        &mut self,
        participant_index: ParticipantIndex,
        receiving_binding_epoch: BindingEpoch,
        request: &ParticipantAck,
        contiguously_available_through: DeliverySeq,
    ) -> Result<CumulativeAckOutcome, CumulativeAckAuthorizationError> {
        let available_through = contiguously_available_through.min(self.candidate_high_watermark);
        self.acknowledge_by_endpoint(
            participant_index,
            receiving_binding_epoch,
            request,
            move |_, _, endpoint| Ok(endpoint <= available_through),
        )
    }

    /// Applies the durable per-recipient obligation endpoint rule.
    ///
    /// Conversation-global sequence gaps are skipped as non-obligations. A
    /// forward request commits only when its endpoint exists in `obligations`;
    /// ending on a sender-excluded or otherwise absent sequence returns
    /// `AckGap`. The testimony must name the selected participant and current
    /// durable cursor exactly.
    ///
    /// # Errors
    ///
    /// Returns [`CumulativeAckAuthorizationError`] before mutation when request
    /// authority or testimony context differs from this episode.
    pub fn acknowledge_with_obligations(
        &mut self,
        participant_index: ParticipantIndex,
        receiving_binding_epoch: BindingEpoch,
        request: &ParticipantAck,
        obligations: &RecipientAckObligations,
    ) -> Result<CumulativeAckOutcome, CumulativeAckAuthorizationError> {
        self.acknowledge_by_endpoint(
            participant_index,
            receiving_binding_epoch,
            request,
            |participant_id, acknowledged_through, endpoint| {
                obligations
                    .contains_endpoint(participant_id, acknowledged_through, endpoint)
                    .map_err(|error| CumulativeAckAuthorizationError::ObligationContext { error })
            },
        )
    }

    fn acknowledge_by_endpoint<F>(
        &mut self,
        participant_index: ParticipantIndex,
        receiving_binding_epoch: BindingEpoch,
        request: &ParticipantAck,
        endpoint_is_available: F,
    ) -> Result<CumulativeAckOutcome, CumulativeAckAuthorizationError>
    where
        F: FnOnce(
            ParticipantId,
            DeliverySeq,
            DeliverySeq,
        ) -> Result<bool, CumulativeAckAuthorizationError>,
    {
        let Some(participant) = self.participants.get(&participant_index).copied() else {
            return Err(CumulativeAckAuthorizationError::ParticipantIndexUnknown);
        };
        if request.conversation_id != self.conversation_id {
            return Err(CumulativeAckAuthorizationError::ConversationMismatch);
        }
        if request.participant_id != participant.participant_id {
            return Err(CumulativeAckAuthorizationError::ParticipantMismatch);
        }
        let FrontierBinding::Bound(active_binding_epoch) = participant.binding else {
            return Err(CumulativeAckAuthorizationError::BindingEpochMismatch);
        };
        if request.capability_generation != active_binding_epoch.capability_generation {
            return Err(CumulativeAckAuthorizationError::GenerationMismatch);
        }
        if receiving_binding_epoch != active_binding_epoch {
            return Err(CumulativeAckAuthorizationError::BindingEpochMismatch);
        }

        let current_cursor = participant.cursor;
        let through_seq = request.through_seq;
        let envelope = ParticipantAckEnvelope {
            conversation_id: request.conversation_id,
            participant_id: request.participant_id,
            capability_generation: request.capability_generation,
            through_seq,
        };
        let endpoint_available =
            endpoint_is_available(participant.participant_id, current_cursor, through_seq)?;

        if through_seq < current_cursor {
            return AckRegression::new(envelope, current_cursor)
                .map(CumulativeAckOutcome::Regression)
                .ok_or(CumulativeAckAuthorizationError::CursorRelationInvariant);
        }
        if through_seq == current_cursor {
            return Ok(CumulativeAckOutcome::NoOp(AckNoOp::participant_ack(
                envelope,
            )));
        }
        if through_seq > self.candidate_high_watermark || !endpoint_available {
            return AckGap::new(envelope, current_cursor)
                .map(CumulativeAckOutcome::Gap)
                .ok_or(CumulativeAckAuthorizationError::CursorRelationInvariant);
        }

        let Some(stored) = self.participants.get_mut(&participant_index) else {
            return Err(CumulativeAckAuthorizationError::ParticipantIndexUnknown);
        };
        stored.advance_to(through_seq);
        let key = CursorProgressKey {
            participant_index,
            boundary: through_seq,
        };
        self.facts.record(key);
        let _ = self.facts.consume_through(participant_index, through_seq);

        let minimum_member_cursor = self
            .participants
            .values()
            .map(|participant| participant.cursor)
            .min();
        self.floor = floor_transition(
            self.floor.resulting_floor,
            minimum_member_cursor,
            self.candidate_high_watermark,
            self.observer_progress,
            self.cap_floor,
        );
        // `cap_floor` is defined from `f >= base_floor`, so for the next
        // append-free ack its durable lower bound is the just-committed floor.
        self.cap_floor = self.floor.resulting_floor;

        Ok(CumulativeAckOutcome::Committed(AckCommitted::new(envelope)))
    }

    /// Deterministically serializes the active debt, binding-tagged cursors, and facts.
    ///
    /// Format: conversation id, debt entries/bytes as two u128 values, observer
    /// progress and `H'` as two u64 values, current `F'` and `cap_floor` as two
    /// u128 values, participant count as u32, participants in index order, then
    /// fact count as u32 and facts in `(participant_index, boundary)` order. The
    /// retained suffix is reproduced exactly by `[F', H']`. A participant is
    /// one binding tag plus five u64 values: its canonical identifier/index,
    /// server incarnation, connection ordinal, generation, and cursor. A fact retains the 17-byte
    /// format documented by [`CursorProgressFacts::encode`]. This is lifecycle
    /// storage, not the participant network frame format.
    ///
    /// # Errors
    ///
    /// Returns [`CursorFactEncodeError`] if either count or the exact allocation
    /// length cannot be represented.
    pub fn encode(&self) -> Result<Vec<u8>, CursorFactEncodeError> {
        let participant_count = u32::try_from(self.participants.len())
            .map_err(|_| CursorFactEncodeError::TooManyParticipants)?;
        let fact_count =
            u32::try_from(self.facts.len()).map_err(|_| CursorFactEncodeError::TooManyFacts)?;
        let participant_bytes = self
            .participants
            .len()
            .checked_mul(41)
            .ok_or(CursorFactEncodeError::LengthOverflow)?;
        let fact_bytes = self
            .facts
            .len()
            .checked_mul(17)
            .ok_or(CursorFactEncodeError::LengthOverflow)?;
        let capacity = 96_usize
            .checked_add(participant_bytes)
            .and_then(|length| length.checked_add(fact_bytes))
            .ok_or(CursorFactEncodeError::LengthOverflow)?;

        let debt = self.debt.value();
        let mut bytes = Vec::with_capacity(capacity);
        bytes.extend_from_slice(&self.conversation_id.to_be_bytes());
        bytes.extend_from_slice(&debt.entries.to_be_bytes());
        bytes.extend_from_slice(&debt.bytes.to_be_bytes());
        bytes.extend_from_slice(&self.observer_progress.to_be_bytes());
        bytes.extend_from_slice(&self.candidate_high_watermark.to_be_bytes());
        bytes.extend_from_slice(&self.floor.resulting_floor.to_be_bytes());
        bytes.extend_from_slice(&self.cap_floor.to_be_bytes());
        bytes.extend_from_slice(&participant_count.to_be_bytes());
        for participant in self.participants.values() {
            bytes.push(match participant.binding {
                FrontierBinding::Bound(_) => 0,
                FrontierBinding::Detached(_) => 1,
            });
            bytes.extend_from_slice(&participant.participant_id.to_be_bytes());
            let binding_epoch = participant.binding_epoch();
            bytes.extend_from_slice(
                &binding_epoch
                    .connection_incarnation
                    .server_incarnation
                    .to_be_bytes(),
            );
            bytes.extend_from_slice(
                &binding_epoch
                    .connection_incarnation
                    .connection_ordinal
                    .to_be_bytes(),
            );
            bytes.extend_from_slice(&binding_epoch.capability_generation.get().to_be_bytes());
            bytes.extend_from_slice(&participant.cursor.to_be_bytes());
        }
        bytes.extend_from_slice(&fact_count.to_be_bytes());
        for (key, fact) in &self.facts.facts {
            bytes.extend_from_slice(&key.participant_index.to_be_bytes());
            bytes.extend_from_slice(&key.boundary.to_be_bytes());
            bytes.push(match fact {
                CursorProgressFact::Pending => 0,
                CursorProgressFact::Consumed => 1,
            });
        }
        Ok(bytes)
    }
}
