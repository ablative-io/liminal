//! In-memory authority for one production participant conversation.
//!
//! The authority is rebuilt from the durable transition-input log by
//! re-running the exact protocol transitions that produced it (see
//! [`super::log`]), so no lifecycle rule is duplicated here. The shell
//! ([`ParticipantConversation`]) advances only under the A3 aggregate
//! durability barrier: an event is committed exactly when its log entry —
//! carrying both the operation inputs and the canonical event bytes — has
//! been appended and flushed.

use std::collections::BTreeMap;

use liminal_protocol::lifecycle::{
    BindingState, CommittedDiedTerminal, ConversationDecision, ConversationGenesis,
    ConversationRefusalReason, CredentialAttachLiveReceipt, DetachCell, EnrollmentLiveReceipt,
    LiveFrontierOwner, LiveMember, NonzeroParticipantAckCommit, ObligationDebtDispatchState,
    ObligationDebtDispatchTransition, ObligationDebtOwnerError, ObserverProgressProjection,
    OrdinaryBindingFate, ParticipantConversation, PendingDiedOrdinaryFinalizer, RetiredIdentity,
    SealedBindingFateToken,
};
#[cfg(test)]
use liminal_protocol::wire::ParticipantDelivery;
use liminal_protocol::wire::{
    AckCommitted, AttachAttemptToken, AttachBound, AttachSecret, BindingEpoch, DeliverySeq,
    DetachAttemptToken, EnrollBound, Generation, ParticipantId, ReceiptExpiryReason,
    TransactionOrder,
};

use super::facts::{Digest, FactsError};
use super::fate_occurrence::{FateOccurrenceConflict, FateOccurrenceRouter};
use super::log::{
    OperationLogError, StoredOperation, StoredOrdinaryTerminalSource, StoredSpecificFateIntent,
};
use super::observer_progress::{
    ObserverProgressConformanceError, ObserverProgressSourceMetadata,
    ObserverProgressSourceWitness, ObserverProgressWitnessState,
};
use super::outbox::{ConversationOutbox, ConversationOutboxError};
use super::outbox_log::OutboxLogError;

/// Exact committed credential-attach receipt with its own deadline pair.
///
/// The deadlines belong to THIS receipt alone: a later attach mints a fresh
/// state and retires this one into an [`AttachProvenanceRecord`] — it never
/// rewrites or re-opens these windows (bounded-provenance law: secret-bearing
/// receipts never outlive their signed TTL).
#[derive(Debug)]
pub(super) struct AttachReceiptState {
    /// Attempt token keying the exact receipt.
    pub(super) token: AttachAttemptToken,
    /// Live receipt for lookup-phase resolution.
    pub(super) receipt: CredentialAttachLiveReceipt,
    /// Exact committed payload held for byte-identical replay.
    pub(super) outcome: AttachBound,
    /// Secret presented by the committed request — the live receipt's
    /// constant-time verifier (contract row 4: lost-rotation recovery
    /// presents the invalidated OLD secret), never the minted result secret.
    pub(super) verifier: [u8; 32],
    /// Result generation minted by this receipt (presented + 1), as stored in
    /// the contract's provenance record.
    pub(super) result_generation: Generation,
    /// Secret-bearing receipt deadline (epoch milliseconds).
    pub(super) receipt_expires_at: u128,
    /// Non-secret provenance deadline (epoch milliseconds).
    pub(super) provenance_expires_at: u128,
}

/// Non-secret provenance fingerprint retained for an ended attach receipt.
///
/// Minted when a newer attach replaces the receipt (terminal reason
/// `Superseded` if the receipt was still live, `Deadline` if its own deadline
/// had already ended it). Inside its window an exact old token answers the
/// contract's `ReceiptExpired` row; after the window the retained fingerprint
/// keeps exact-old classification on the `StaleOrUnknownReceipt` arm.
#[derive(Clone, Copy, Debug)]
pub(super) struct AttachProvenanceRecord {
    /// Result generation the ended receipt had minted.
    pub(super) result_generation: Generation,
    /// Exact terminal reason set when the receipt ended.
    pub(super) reason: ReceiptExpiryReason,
    /// Non-secret provenance deadline (epoch milliseconds).
    pub(super) provenance_expires_at: u128,
}

/// Durable Attached source identity coupled to its sole move-only fate token.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct PendingBindingFate {
    /// Physical operation-log sequence of the Attached row that minted the token.
    pub(super) attached_source_sequence: u64,
    /// Sole protocol authority consumed by measured Ordinary/Recovered completion.
    pub(super) token: SealedBindingFateToken,
}

/// Exact terminal and durable enclosing source that unlock an Ordinary intent.
#[derive(Clone, Copy, Debug)]
pub(super) struct PendingSpecificFateTerminal {
    pub(super) terminal: CommittedDiedTerminal,
    pub(super) source: StoredOrdinaryTerminalSource,
}

/// Durable Died intent coupled to the sole move-only authority that consumes it.
#[derive(Debug)]
pub(super) struct PendingSpecificFate {
    pub(super) died_source_sequence: u64,
    pub(super) intent: StoredSpecificFateIntent,
    pub(super) terminal: Option<PendingSpecificFateTerminal>,
    pub(super) binding_fate: PendingBindingFate,
}

/// Ordinary fate measured before its enclosing finalizer removes the participant frontier.
#[derive(Debug)]
pub(super) struct PreparedOrdinaryFinalizer {
    pub(super) attached_source_sequence: u64,
    pub(super) terminal: CommittedDiedTerminal,
    pub(super) terminal_source: StoredOrdinaryTerminalSource,
    pub(super) fate: OrdinaryBindingFate,
    pub(super) finalizer: PendingDiedOrdinaryFinalizer,
}

/// One enrolled participant's live authority and replay facts.
#[derive(Debug)]
pub(super) struct Slot {
    /// Live membership authority produced by protocol transitions.
    pub(super) member: LiveMember<Digest>,
    /// Current binding authority.
    pub(super) binding: BindingState,
    /// Sole move-only fate authority emitted by the most recent attach split.
    /// Operational slot state never contains or duplicates this authority.
    pub(super) binding_fate: Option<PendingBindingFate>,
    /// Four-variant detach replay cell.
    pub(super) cell: DetachCell<Digest>,
    /// Exact committed enrollment receipt held for lookup-phase resolution.
    /// Served only while the receipt body is live (its own deadline unpassed
    /// AND [`Self::enrollment_receipt_ended`] unset).
    pub(super) enrollment_receipt: EnrollmentLiveReceipt,
    /// Exact committed enrollment payload held for byte-identical replay
    /// while the receipt is live, and for the provenance row's result
    /// generation afterwards.
    pub(super) enrollment_outcome: EnrollBound,
    /// Enrollment receipt deadline, fixed at enroll commit and never
    /// rewritten by later attaches (epoch milliseconds).
    pub(super) enrollment_receipt_expires_at: u128,
    /// Enrollment provenance deadline, fixed at enroll commit (epoch
    /// milliseconds). Between the receipt's end and this deadline an exact
    /// enrollment-token replay answers the contract's `ReceiptExpired` row
    /// with the exact terminal reason; after it, the permanent lifetime
    /// mapping answers `EnrollmentKnown`.
    pub(super) enrollment_provenance_expires_at: u128,
    /// Exact terminal reason recorded when a committed credential attach
    /// ended the enrollment receipt's body (contract R-C0: the reason is
    /// `Superseded` when the newer generation ended a still-live receipt,
    /// `Deadline` when the receipt's own deadline had already ended it).
    /// `None` while no attach has committed; a receipt that dies by its own
    /// deadline with no attach keeps `None` and classifies as `Deadline` at
    /// lookup time. Set once by the FIRST rotation and never rewritten —
    /// derived from the committing attach's admitted clock, so cold replay
    /// reproduces the identical record.
    pub(super) enrollment_receipt_ended: Option<ReceiptExpiryReason>,
    /// Current attach receipt with its own independent deadline pair.
    pub(super) attach: Option<AttachReceiptState>,
    /// Bounded provenance fingerprints of ended attach receipts, keyed by
    /// their exact attempt tokens. One record exists per committed rotation
    /// (each rotation requires the then-current secret), so growth is bound
    /// to the participant's own committed history.
    pub(super) attach_provenance: BTreeMap<[u8; 16], AttachProvenanceRecord>,
    /// Currently valid attach secret for this slot.
    pub(super) attach_secret: AttachSecret,
    /// Exact token of the slot's committed/terminalized detach, if any.
    pub(super) exact_detach_token: Option<DetachAttemptToken>,
}

/// Sole live owner of one conversation's protocol state.
#[derive(Debug)]
pub(super) struct ConversationAuthority {
    pub(super) conversation_id: u64,
    /// Shell aggregate; `Some` from genesis onward. Temporarily taken while a
    /// pending aggregate barrier owns it.
    pub(super) shell: Option<ParticipantConversation>,
    /// Move-coupled frontier and obligation-debt episode authority. Absent only
    /// before first enrollment commits or while one protocol transition owns
    /// it together with its completion token.
    pub(super) obligation_debt_dispatch: Option<ObligationDebtDispatchState>,
    /// Protocol-owned completion authority present only while
    /// `obligation_debt_dispatch` is absent inside one synchronous transition.
    pending_debt_dispatch_transition: Option<ObligationDebtDispatchTransition>,
    /// Move-only durable delivery and recipient-obligation owner. It is absent
    /// only while cold restore is still validating the extension stream.
    pub(super) outbox: Option<ConversationOutbox>,
    /// Volatile exact marker enqueue testimony, keyed by recipient and marker
    /// sequence. Cold replay never restores this map; reattach starts without
    /// old-binding offer testimony.
    pub(super) offered_markers: BTreeMap<(ParticipantId, DeliverySeq), BindingEpoch>,
    /// Last protocol-owned marker projection, exposed only to acceptance
    /// fixtures that must stage the next leg's observer fact explicitly.
    #[cfg(test)]
    pub(super) last_marker_projection: Option<ParticipantDelivery>,
    /// Live participant slots keyed by permanent participant id.
    pub(super) slots: BTreeMap<ParticipantId, Slot>,
    /// Durable Died intents awaiting their exact Ordinary/Recovered consumer.
    pub(super) pending_specific_fates: BTreeMap<ParticipantId, PendingSpecificFate>,
    /// Ordinary fates measured immediately before an enclosing finalizer commit.
    pub(super) prepared_ordinary_finalizers: BTreeMap<ParticipantId, PreparedOrdinaryFinalizer>,
    /// Four-class occurrence ownership rebuilt from durable rows before any
    /// observer mutation. This retains active keys, never a copy of history.
    pub(super) fate_occurrences: FateOccurrenceRouter,
    /// Permanent retired identity tombstones keyed by participant id.
    pub(super) retired: BTreeMap<ParticipantId, RetiredIdentity<Digest, Digest, Digest>>,
    /// Permanent enrollment-token index.
    pub(super) tokens: BTreeMap<[u8; 16], ParticipantId>,
    /// Next unallocated transaction order.
    pub(super) next_order: TransactionOrder,
    /// Next unallocated delivery sequence.
    pub(super) next_seq: DeliverySeq,
    /// Next unallocated permanent participant index.
    pub(super) next_participant: ParticipantId,
    /// Optimistic durable log head.
    pub(super) next_log_sequence: u64,
    /// The pre-existing replay vector enriched with source, occurrence,
    /// producer, lineage, and checked merged-order provenance. The handler
    /// drains it only after the complete source replay succeeds.
    observer_progress_witnesses: ObserverProgressWitnessState,
    /// Durable hard-observer progress for this conversation.
    pub(super) observer_progress: DeliverySeq,
}

/// Failure while applying or replaying one production operation.
#[derive(Debug, thiserror::Error)]
pub(super) enum StateError {
    /// The durable log rejected a read or append.
    #[error(transparent)]
    Log(#[from] OperationLogError),
    /// The Unit 2 extension stream or outbox owner refused durable bytes.
    #[error(transparent)]
    Outbox(#[from] ConversationOutboxError),
    /// The canonical Unit 2 extension stream rejected a live append.
    #[error(transparent)]
    OutboxLog(#[from] OutboxLogError),
    /// The synchronous durability bridge failed while awaiting a barrier.
    #[error(transparent)]
    Bridge(#[from] liminal::durability::bridge::BridgeError),
    /// A server-owned fact could not be minted.
    #[error(transparent)]
    Facts(#[from] FactsError),
    /// Observer-progress source or durable-prefix conformance failed.
    #[error(transparent)]
    ObserverProgressConformance(#[from] ObserverProgressConformanceError),
    /// Four-class binding-fate occurrence routing refused conflicting history.
    #[error(transparent)]
    FateOccurrenceConflict(#[from] FateOccurrenceConflict),
    /// The protocol shell refused an operation the log claims committed.
    #[error("conversation shell refused a committed operation: {reason:?}")]
    ShellRefused {
        /// Exact protocol refusal reason.
        reason: ConversationRefusalReason,
    },
    /// The shell was unavailable (a pending barrier owns it).
    #[error("conversation shell authority is unavailable")]
    ShellUnavailable,
    /// The executable frontier was unavailable or absent after enrollment.
    #[error("conversation executable frontier authority is unavailable")]
    FrontierUnavailable,
    /// The protocol could not couple the exact resulting frontier and episode.
    #[error("conversation obligation-debt owner invariant failed: {error:?}")]
    ObligationDebtOwner { error: ObligationDebtOwnerError },
    /// A replayed decision minted different canonical bytes than stored.
    #[error("replayed event bytes diverge from durable event bytes at log sequence {sequence}")]
    ReplayedEventDrift {
        /// Durable log sequence of the diverging entry.
        sequence: u64,
    },
    /// A protocol transition rejected inputs the log claims committed, or a
    /// live invariant the crate makes unreachable was observed.
    #[error("participant production invariant violated: {message}")]
    Invariant {
        /// Diagnostic description for server logs.
        message: String,
    },
    /// The numeric allocation domain (orders, sequences, slots) is exhausted.
    #[error("participant production allocation domain exhausted: {domain}")]
    AllocationExhausted {
        /// Which allocator ran out.
        domain: &'static str,
    },
}

impl StateError {
    pub(super) fn invariant(message: impl Into<String>) -> Self {
        Self::Invariant {
            message: message.into(),
        }
    }
}

/// Synchronous durable append seam between state transitions and the store.
///
/// Live operation appends flow through this trait so the aggregate barrier
/// commit can wait on the exact durable append; cold replay never appends.
pub(super) trait DurableAppend {
    /// Appends one operation at the optimistic head and flushes.
    ///
    /// # Errors
    ///
    /// Returns [`OperationLogError`] when the append or flush fails; the
    /// caller aborts its pending barrier and publishes nothing.
    fn append(
        &self,
        operation: &StoredOperation,
        expected_sequence: u64,
    ) -> Result<(), OperationLogError>;
}

impl ConversationAuthority {
    /// Creates the pre-genesis empty authority for one conversation.
    pub(super) const fn empty(conversation_id: u64) -> Self {
        Self {
            conversation_id,
            shell: None,
            obligation_debt_dispatch: None,
            pending_debt_dispatch_transition: None,
            outbox: None,
            offered_markers: BTreeMap::new(),
            #[cfg(test)]
            last_marker_projection: None,
            slots: BTreeMap::new(),
            pending_specific_fates: BTreeMap::new(),
            prepared_ordinary_finalizers: BTreeMap::new(),
            fate_occurrences: FateOccurrenceRouter::new(),
            retired: BTreeMap::new(),
            tokens: BTreeMap::new(),
            next_order: 0,
            next_seq: 1,
            next_participant: 0,
            next_log_sequence: 0,
            observer_progress_witnesses: ObserverProgressWitnessState::new(),
            observer_progress: 0,
        }
    }

    /// Records one sealed protocol source projection for the observer barrier.
    pub(super) fn record_observer_progress_projection(
        &mut self,
        projection: ObserverProgressProjection,
        metadata: ObserverProgressSourceMetadata,
    ) -> Result<(), StateError> {
        #[cfg(test)]
        let inject_duplicate = super::observer_progress::duplicate_leave_injection_armed()
            && metadata.producer()
                == super::observer_progress::ObserverProgressProducer::LiveLeaveCommit;
        let progress = projection.new_observer_progress();
        self.observer_progress_witnesses
            .record(self.conversation_id, projection, metadata)?;
        #[cfg(test)]
        if inject_duplicate {
            self.observer_progress_witnesses
                .inject_duplicate_producer(metadata)?;
        }
        self.observer_progress = self.observer_progress.max(progress);
        Ok(())
    }

    /// Begins one checked source visit in the actual base/extension merge.
    pub(super) fn begin_observer_progress_source(&mut self) -> Result<(), StateError> {
        self.observer_progress_witnesses.begin_source()?;
        Ok(())
    }

    /// Completes the current checked merged source visit.
    pub(super) fn end_observer_progress_source(&mut self) -> Result<(), StateError> {
        self.observer_progress_witnesses.end_source()?;
        Ok(())
    }

    /// Surrenders the complete validated source pass in merged replay order.
    pub(super) fn take_observer_progress_witnesses(
        &mut self,
    ) -> Vec<ObserverProgressSourceWitness> {
        self.observer_progress_witnesses.take()
    }

    /// Takes the shell for a consuming protocol decision.
    pub(super) fn take_shell(&mut self) -> Result<ParticipantConversation, StateError> {
        self.shell.take().ok_or(StateError::ShellUnavailable)
    }

    /// Borrows the executable frontier through the sole coupled owner.
    pub(super) fn frontier(&self) -> Option<&LiveFrontierOwner> {
        self.obligation_debt_dispatch
            .as_ref()
            .map(ObligationDebtDispatchState::frontier)
    }

    /// Borrows the complete coupled state at the delivery-decision seam.
    pub(super) const fn obligation_debt_dispatch(&self) -> Option<&ObligationDebtDispatchState> {
        self.obligation_debt_dispatch.as_ref()
    }

    /// Takes the complete coupled owner and begins one consuming transition.
    pub(super) fn take_frontier(&mut self) -> Result<LiveFrontierOwner, StateError> {
        if self.pending_debt_dispatch_transition.is_some() {
            return Err(StateError::invariant(
                "obligation-debt transition authority is already active",
            ));
        }
        let (frontier, transition) = self
            .obligation_debt_dispatch
            .take()
            .map(ObligationDebtDispatchState::begin_transition)
            .ok_or(StateError::FrontierUnavailable)?;
        self.pending_debt_dispatch_transition = Some(transition);
        Ok(frontier)
    }

    /// Completes and installs one protocol-produced coupled poststate.
    pub(super) fn install_frontier(
        &mut self,
        frontier: LiveFrontierOwner,
    ) -> Result<(), StateError> {
        let state = match self.pending_debt_dispatch_transition.take() {
            Some(transition) => transition
                .complete(frontier, self.observer_progress)
                .map_err(|error| StateError::ObligationDebtOwner { error })?,
            None if self.obligation_debt_dispatch.is_none() => {
                ObligationDebtDispatchState::from_frontier(frontier, self.observer_progress)
                    .map_err(|error| StateError::ObligationDebtOwner { error })?
            }
            None => {
                return Err(StateError::invariant(
                    "obligation-debt transition authority is unavailable",
                ));
            }
        };
        self.obligation_debt_dispatch = Some(state);
        Ok(())
    }

    /// Installs one nonzero acknowledgement's exact coupled frontier, episode,
    /// and membership cursor result through the active transition token.
    pub(super) fn install_nonzero_ack(
        &mut self,
        frontier: LiveFrontierOwner,
        commit: NonzeroParticipantAckCommit,
        participant_id: ParticipantId,
    ) -> Result<AckCommitted, StateError> {
        let transition = self
            .pending_debt_dispatch_transition
            .take()
            .ok_or_else(|| StateError::invariant("nonzero ack transition authority is absent"))?;
        if self.obligation_debt_dispatch.is_some() {
            return Err(StateError::invariant(
                "nonzero ack retained a second obligation-debt owner",
            ));
        }
        let slot = self
            .slots
            .get_mut(&participant_id)
            .ok_or_else(|| StateError::invariant("nonzero ack participant slot is absent"))?;
        let (state, outcome) = transition
            .complete_nonzero_ack(frontier, commit, &mut slot.member, self.observer_progress)
            .map_err(|error| StateError::ObligationDebtOwner { error })?;
        self.obligation_debt_dispatch = Some(state);
        Ok(outcome)
    }

    /// Replaces a fixture's synthetic frontier with a newly coupled owner.
    #[cfg(test)]
    pub(super) fn replace_frontier_for_test(
        &mut self,
        frontier: LiveFrontierOwner,
    ) -> Result<(), StateError> {
        self.pending_debt_dispatch_transition = None;
        self.obligation_debt_dispatch = None;
        self.install_frontier(frontier)
    }

    /// Allocates the next transaction order and delivery sequence pair.
    pub(super) fn allocate_position(
        &mut self,
    ) -> Result<(TransactionOrder, DeliverySeq), StateError> {
        let order = self.next_order;
        let seq = self.next_seq;
        self.next_order =
            self.next_order
                .checked_add(1)
                .ok_or(StateError::AllocationExhausted {
                    domain: "transaction order",
                })?;
        self.next_seq = self
            .next_seq
            .checked_add(1)
            .ok_or(StateError::AllocationExhausted {
                domain: "delivery sequence",
            })?;
        Ok((order, seq))
    }

    /// Allocates one supersession handoff position: a single transaction
    /// major shared by the `Detached(Superseded)` terminal and the `Attached`
    /// record, with the terminal's delivery sequence immediately before the
    /// record's (the crate's lifecycle-order law for the ordered handoff).
    pub(super) fn allocate_supersession_position(
        &mut self,
    ) -> Result<(TransactionOrder, DeliverySeq, DeliverySeq), StateError> {
        let (order, terminal_seq) = self.allocate_position()?;
        let attached_seq = self.next_seq;
        self.next_seq = self
            .next_seq
            .checked_add(1)
            .ok_or(StateError::AllocationExhausted {
                domain: "delivery sequence",
            })?;
        Ok((order, terminal_seq, attached_seq))
    }

    /// Reports whether the receiving connection already owns a bound slot in
    /// this conversation, exposing only what the crate's stage-6 selector
    /// needs. Derived from binding-epoch authority; no side table exists.
    pub(super) fn binding_slot_occupancy(
        &self,
        receiving_incarnation: liminal_protocol::wire::ConnectionIncarnation,
    ) -> liminal_protocol::lifecycle::BindingSlotOccupancy {
        for (participant_id, slot) in &self.slots {
            if let BindingState::Bound(active) = slot.binding {
                if active.binding_epoch.connection_incarnation == receiving_incarnation {
                    return liminal_protocol::lifecycle::BindingSlotOccupancy::Occupied {
                        participant_id: *participant_id,
                    };
                }
            }
        }
        liminal_protocol::lifecycle::BindingSlotOccupancy::Empty
    }

    /// Advances the position allocators past a replayed entry's positions.
    pub(super) fn observe_replayed_position(
        &mut self,
        order: u64,
        seq: u64,
    ) -> Result<(), StateError> {
        let next_order = order
            .checked_add(1)
            .ok_or(StateError::AllocationExhausted {
                domain: "transaction order",
            })?;
        let next_seq = seq.checked_add(1).ok_or(StateError::AllocationExhausted {
            domain: "delivery sequence",
        })?;
        self.next_order = self.next_order.max(next_order);
        self.next_seq = self.next_seq.max(next_seq);
        Ok(())
    }

    /// Ensures durable shell genesis, appending event zero on first touch.
    ///
    /// Idempotent: an already genesis-validated shell returns immediately.
    pub(super) fn ensure_genesis(
        &mut self,
        appender: &dyn DurableAppend,
    ) -> Result<(), StateError> {
        if let Some(shell) = self.shell.as_ref() {
            if shell.genesis_validated() {
                return Ok(());
            }
        } else {
            self.shell = Some(ParticipantConversation::from_genesis(
                ConversationGenesis::new(self.conversation_id),
            ));
        }
        let shell = self.take_shell()?;
        match shell.decide_genesis_validation() {
            ConversationDecision::Refused(refusal) => {
                let reason = refusal.reason();
                self.shell = Some(refusal.into_conversation());
                if reason == ConversationRefusalReason::GenesisAlreadyValidated {
                    Ok(())
                } else {
                    Err(StateError::ShellRefused { reason })
                }
            }
            ConversationDecision::Commit(commit) => {
                let event = commit.event().encode_canonical();
                let operation = StoredOperation::Genesis { event };
                match appender.append(&operation, self.next_log_sequence) {
                    Ok(()) => {
                        self.shell = Some(commit.commit());
                        self.next_log_sequence = self.next_log_sequence.checked_add(1).ok_or(
                            StateError::AllocationExhausted {
                                domain: "log sequence",
                            },
                        )?;
                        Ok(())
                    }
                    Err(error) => {
                        self.shell = Some(commit.abort());
                        Err(StateError::Log(error))
                    }
                }
            }
        }
    }

    /// Replays shell genesis from its stored canonical event bytes.
    pub(super) fn replay_genesis(&mut self, stored_event: &[u8]) -> Result<(), StateError> {
        if self.shell.is_some() {
            return Err(StateError::invariant(
                "duplicate genesis entry in production log",
            ));
        }
        let shell =
            ParticipantConversation::from_genesis(ConversationGenesis::new(self.conversation_id));
        match shell.decide_genesis_validation() {
            ConversationDecision::Refused(refusal) => Err(StateError::ShellRefused {
                reason: refusal.reason(),
            }),
            ConversationDecision::Commit(commit) => {
                if commit.event().encode_canonical() != stored_event {
                    return Err(StateError::ReplayedEventDrift { sequence: 0 });
                }
                self.shell = Some(commit.commit());
                self.next_log_sequence = 1;
                Ok(())
            }
        }
    }

    /// Advances the durable log head after one committed replayed entry.
    pub(super) fn advance_log_head(&mut self) -> Result<(), StateError> {
        self.next_log_sequence =
            self.next_log_sequence
                .checked_add(1)
                .ok_or(StateError::AllocationExhausted {
                    domain: "log sequence",
                })?;
        Ok(())
    }
}
