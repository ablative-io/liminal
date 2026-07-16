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
    BindingState, ConversationDecision, ConversationGenesis, ConversationRefusalReason,
    CredentialAttachLiveReceipt, DetachCell, EnrollmentLiveReceipt, LiveMember,
    ParticipantConversation,
};
use liminal_protocol::wire::{
    AttachAttemptToken, AttachBound, AttachSecret, DeliverySeq, DetachAttemptToken, EnrollBound,
    Generation, ParticipantId, ReceiptExpiryReason, TransactionOrder,
};

use super::facts::{Digest, FactsError};
use super::log::{OperationLogError, StoredOperation};

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

/// One enrolled participant's live authority and replay facts.
#[derive(Debug)]
pub(super) struct Slot {
    /// Live membership authority produced by protocol transitions.
    pub(super) member: LiveMember<Digest>,
    /// Current binding authority.
    pub(super) binding: BindingState,
    /// Four-variant detach replay cell.
    pub(super) cell: DetachCell<Digest>,
    /// Exact committed enrollment receipt held for lookup-phase resolution.
    pub(super) enrollment_receipt: EnrollmentLiveReceipt,
    /// Exact committed enrollment payload held for byte-identical replay.
    pub(super) enrollment_outcome: EnrollBound,
    /// Enrollment receipt deadline, fixed at enroll commit and never
    /// rewritten by later attaches (epoch milliseconds).
    pub(super) enrollment_receipt_expires_at: u128,
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
    /// Live participant slots keyed by permanent participant id.
    pub(super) slots: BTreeMap<ParticipantId, Slot>,
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
    /// Durable hard-observer progress for this conversation.
    pub(super) observer_progress: DeliverySeq,
}

/// Failure while applying or replaying one production operation.
#[derive(Debug, thiserror::Error)]
pub(super) enum StateError {
    /// The durable log rejected a read or append.
    #[error(transparent)]
    Log(#[from] OperationLogError),
    /// A server-owned fact could not be minted.
    #[error(transparent)]
    Facts(#[from] FactsError),
    /// The protocol shell refused an operation the log claims committed.
    #[error("conversation shell refused a committed operation: {reason:?}")]
    ShellRefused {
        /// Exact protocol refusal reason.
        reason: ConversationRefusalReason,
    },
    /// The shell was unavailable (a pending barrier owns it).
    #[error("conversation shell authority is unavailable")]
    ShellUnavailable,
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
            slots: BTreeMap::new(),
            tokens: BTreeMap::new(),
            next_order: 0,
            next_seq: 1,
            next_participant: 0,
            next_log_sequence: 0,
            observer_progress: 0,
        }
    }

    /// Greatest delivery sequence contiguously admitted by this conversation.
    pub(super) const fn contiguously_available_through(&self) -> DeliverySeq {
        self.next_seq.saturating_sub(1)
    }

    /// Takes the shell for a consuming protocol decision.
    pub(super) fn take_shell(&mut self) -> Result<ParticipantConversation, StateError> {
        self.shell.take().ok_or(StateError::ShellUnavailable)
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
