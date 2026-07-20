use alloc::boxed::Box;

use crate::{
    algebra::ResourceVector,
    lifecycle::{ActiveBinding, AdmissionOrder},
    wire::{BindingEpoch, ConversationId, DeliverySeq, ParticipantId, TransactionOrder},
};

use super::{LiveFrontierError, LiveFrontierOwner, RetainedRecordCharge};
use crate::lifecycle::{CommittedBindingTerminalPosition, PendingBindingTerminalPosition};

/// Canonical durability encoding required for one binding-terminal candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindingTerminalEncoding {
    /// Canonical JSON encoding of one schema-v3 participant lifecycle row.
    ParticipantLifecycleV3CanonicalJson,
}

/// Closed terminal row class selected before canonical encoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindingTerminalCauseClass {
    /// A clean or orderly close produces `Detached`.
    Detached,
    /// An unexpected loss or refusal produces `Died`.
    Died,
}

/// Sealed exact identity exposed to the canonical server encoder.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CandidateTerminalKey {
    active_binding: ActiveBinding,
    cause_class: BindingTerminalCauseClass,
    admission_order: AdmissionOrder,
    delivery_seq: DeliverySeq,
}

impl CandidateTerminalKey {
    /// Returns the owning conversation.
    #[must_use]
    pub const fn conversation_id(self) -> ConversationId {
        self.active_binding.conversation_id
    }

    /// Returns the permanent participant identifier.
    #[must_use]
    pub const fn participant_id(self) -> ParticipantId {
        self.active_binding.participant_id
    }

    /// Returns the exact ended binding epoch.
    #[must_use]
    pub const fn binding_epoch(self) -> BindingEpoch {
        self.active_binding.binding_epoch
    }

    /// Returns the closed terminal row class.
    #[must_use]
    pub const fn cause_class(self) -> BindingTerminalCauseClass {
        self.cause_class
    }

    /// Returns the canonical binding-terminal admission key.
    #[must_use]
    pub const fn admission_order(self) -> AdmissionOrder {
        self.admission_order
    }

    /// Returns the candidate delivery sequence.
    #[must_use]
    pub const fn delivery_seq(self) -> DeliverySeq {
        self.delivery_seq
    }

    /// Keys one canonical schema-v3 encoded charge to this sealed candidate.
    #[must_use]
    pub const fn bind_v3_charge(
        self,
        encoded_charge: ResourceVector,
    ) -> BindingTerminalCandidateCharge {
        BindingTerminalCandidateCharge {
            conversation_id: self.conversation_id(),
            participant_id: self.participant_id(),
            binding_epoch: self.binding_epoch(),
            admission_order: self.admission_order,
            delivery_seq: self.delivery_seq,
            encoding: BindingTerminalEncoding::ParticipantLifecycleV3CanonicalJson,
            charge: RetainedRecordCharge::new(
                self.delivery_seq,
                self.admission_order,
                encoded_charge,
            ),
        }
    }
}

/// Canonical schema-v3 charge keyed to one sealed binding-terminal candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BindingTerminalCandidateCharge {
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    binding_epoch: BindingEpoch,
    admission_order: AdmissionOrder,
    delivery_seq: DeliverySeq,
    encoding: BindingTerminalEncoding,
    charge: RetainedRecordCharge,
}

impl BindingTerminalCandidateCharge {
    /// Returns the exact retained-row charge after successful admission.
    #[must_use]
    pub const fn retained_charge(self) -> RetainedRecordCharge {
        self.charge
    }
}

/// Typed reason the non-mutating prepare stage refused authority or positions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindingTerminalPrepareError {
    /// The active binding does not belong to this coupled owner.
    Authority,
    /// The supplied transaction order is not the next checked order.
    TransactionOrder,
    /// The supplied delivery sequence is not the next checked sequence.
    DeliverySequence,
    /// Hard observer progress exceeds the current sequence high watermark.
    ObserverProgress,
}

/// Prepare refusal preserving the unchanged complete owner.
#[derive(Debug, PartialEq, Eq)]
pub struct BindingTerminalPrepareRefused {
    owner: LiveFrontierOwner,
    error: BindingTerminalPrepareError,
}

impl BindingTerminalPrepareRefused {
    /// Returns the typed refusal reason.
    #[must_use]
    pub const fn error(&self) -> BindingTerminalPrepareError {
        self.error
    }

    /// Recovers the unchanged coupled owner.
    #[must_use]
    pub fn into_owner(self) -> LiveFrontierOwner {
        self.owner
    }
}

/// Non-mutating prepared terminal admission with its sealed canonical key.
#[derive(Debug, PartialEq, Eq)]
pub struct PreparedBindingTerminal {
    owner: LiveFrontierOwner,
    key: CandidateTerminalKey,
    hard_observer_progress: DeliverySeq,
}

impl PreparedBindingTerminal {
    /// Returns the sealed key required by the canonical v3 encoder.
    #[must_use]
    pub const fn candidate_key(&self) -> CandidateTerminalKey {
        self.key
    }

    /// Returns the hard-observer baseline captured by non-mutating preparation.
    #[must_use]
    pub const fn hard_observer_progress(&self) -> DeliverySeq {
        self.hard_observer_progress
    }

    /// Admits only the exact keyed v3 charge produced for this candidate.
    #[must_use]
    pub fn admit(self, candidate: BindingTerminalCandidateCharge) -> BindingTerminalAdmission {
        if candidate.conversation_id != self.key.conversation_id()
            || candidate.participant_id != self.key.participant_id()
            || candidate.binding_epoch != self.key.binding_epoch()
            || candidate.admission_order != self.key.admission_order()
            || candidate.delivery_seq != self.key.delivery_seq()
            || candidate.encoding != BindingTerminalEncoding::ParticipantLifecycleV3CanonicalJson
            || candidate.charge.delivery_seq() != self.key.delivery_seq()
            || candidate.charge.admission_order() != self.key.admission_order()
            || candidate.charge.encoded_charge().entries != 1
        {
            return admit_refusal(self.owner, BindingTerminalAdmitError::CandidateCharge);
        }

        let retained_len = u64::try_from(self.owner.retained_charges().len());
        let has_capacity = retained_len
            .ok()
            .and_then(|len| len.checked_add(1))
            .is_some_and(|len| len <= self.owner.retained_record_limit());
        if has_capacity {
            return match self.owner.commit_binding_terminal_candidate(
                self.key.active_binding,
                self.key.admission_order,
                self.key.delivery_seq,
                candidate.charge,
            ) {
                Ok(owner) => BindingTerminalAdmission::Commit(BindingTerminalCommit {
                    owner,
                    position: CommittedBindingTerminalPosition::new(
                        self.key.admission_order.transaction_order(),
                        self.key.delivery_seq,
                    ),
                }),
                Err(failure) => {
                    let (owner, error) = *failure;
                    admit_refusal(owner, map_live_frontier_error(error))
                }
            };
        }
        if self.hard_observer_progress < self.key.delivery_seq {
            return match self.owner.pend_binding_terminal_candidate(
                self.key.active_binding,
                self.key.admission_order,
                self.key.delivery_seq,
            ) {
                Ok(owner) => BindingTerminalAdmission::Pending(BindingTerminalPending {
                    owner,
                    position: PendingBindingTerminalPosition::new(
                        self.key.admission_order.transaction_order(),
                    ),
                    blocked_at_observer: self.hard_observer_progress,
                }),
                Err(failure) => {
                    let (owner, error) = *failure;
                    admit_refusal(owner, map_live_frontier_error(error))
                }
            };
        }
        admit_refusal(self.owner, BindingTerminalAdmitError::RetainedRecordLimit)
    }

    /// Recovers the unchanged owner when canonical encoding cannot produce a charge.
    #[must_use]
    pub fn into_owner(self) -> LiveFrontierOwner {
        self.owner
    }
}

/// Successful committed terminal admission.
#[derive(Debug, PartialEq, Eq)]
pub struct BindingTerminalCommit {
    owner: LiveFrontierOwner,
    position: CommittedBindingTerminalPosition,
}

impl BindingTerminalCommit {
    /// Consumes the admission into its transitioned owner and committed position.
    #[must_use]
    pub fn into_parts(self) -> (LiveFrontierOwner, CommittedBindingTerminalPosition) {
        (self.owner, self.position)
    }
}

/// Successful observer-blocked pending terminal admission.
#[derive(Debug, PartialEq, Eq)]
pub struct BindingTerminalPending {
    owner: LiveFrontierOwner,
    position: PendingBindingTerminalPosition,
    blocked_at_observer: DeliverySeq,
}

impl BindingTerminalPending {
    /// Returns the exact hard-observer baseline persisted with the pending source.
    #[must_use]
    pub const fn blocked_at_observer(&self) -> DeliverySeq {
        self.blocked_at_observer
    }

    /// Consumes the admission into its transitioned owner and pending position.
    #[must_use]
    pub fn into_parts(self) -> (LiveFrontierOwner, PendingBindingTerminalPosition) {
        (self.owner, self.position)
    }
}

/// Typed reason the keyed candidate could not be admitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindingTerminalAdmitError {
    /// Candidate identity, encoding, charge key, or entry count disagrees.
    CandidateCharge,
    /// The retained causal-row cap could not admit this candidate.
    RetainedRecordLimit,
    /// The selected binding no longer belongs to the coupled owner.
    Authority,
    /// An earlier immutable or recovery transition has precedence.
    Precedence,
    /// Claim arithmetic or exact owner reconstruction failed.
    Frontier,
    /// Resulting closure accounting exceeded its signed capacity.
    ClosureAccounting,
}

/// Admit refusal preserving the unchanged coupled owner.
#[derive(Debug, PartialEq, Eq)]
pub struct BindingTerminalAdmitRefused {
    owner: LiveFrontierOwner,
    error: BindingTerminalAdmitError,
}

impl BindingTerminalAdmitRefused {
    /// Returns the typed refusal reason.
    #[must_use]
    pub const fn error(&self) -> BindingTerminalAdmitError {
        self.error
    }

    /// Recovers the unchanged owner.
    #[must_use]
    pub fn into_owner(self) -> LiveFrontierOwner {
        self.owner
    }
}

/// Exhaustive terminal-admission result.
#[derive(Debug, PartialEq, Eq)]
pub enum BindingTerminalAdmission {
    /// The exact candidate committed and transformed the complete owner.
    Commit(BindingTerminalCommit),
    /// Observer progress blocked the candidate after consuming only its order.
    Pending(BindingTerminalPending),
    /// Candidate validation or a protocol invariant refused unchanged.
    Refused(Box<BindingTerminalAdmitRefused>),
}

fn admit_refusal(
    owner: LiveFrontierOwner,
    error: BindingTerminalAdmitError,
) -> BindingTerminalAdmission {
    BindingTerminalAdmission::Refused(Box::new(BindingTerminalAdmitRefused { owner, error }))
}

const fn map_live_frontier_error(error: LiveFrontierError) -> BindingTerminalAdmitError {
    match error {
        LiveFrontierError::Authority => BindingTerminalAdmitError::Authority,
        LiveFrontierError::Precedence => BindingTerminalAdmitError::Precedence,
        LiveFrontierError::RetainedCharge => BindingTerminalAdmitError::CandidateCharge,
        LiveFrontierError::RetainedRecordLimit => BindingTerminalAdmitError::RetainedRecordLimit,
        LiveFrontierError::Frontier => BindingTerminalAdmitError::Frontier,
        LiveFrontierError::ClosureAccounting => BindingTerminalAdmitError::ClosureAccounting,
    }
}

impl LiveFrontierOwner {
    /// Validates one active binding and checked candidate positions without mutation.
    ///
    /// # Errors
    ///
    /// Returns the unchanged owner when binding authority, checked order or sequence,
    /// or hard-observer progress disagrees with the coupled protocol state.
    pub fn prepare_binding_terminal(
        self,
        active_binding: ActiveBinding,
        cause_class: BindingTerminalCauseClass,
        next_transaction_order: TransactionOrder,
        next_delivery_sequence: DeliverySeq,
        hard_observer_progress: DeliverySeq,
    ) -> Result<PreparedBindingTerminal, Box<BindingTerminalPrepareRefused>> {
        let authority_matches = active_binding.conversation_id
            == self.frontiers().conversation_id()
            && self
                .frontiers()
                .active_identities()
                .participants()
                .iter()
                .any(|participant| {
                    participant.participant_index() == active_binding.participant_id
                        && participant.binding()
                            == crate::lifecycle::FrontierBinding::Bound(
                                active_binding.binding_epoch,
                            )
                });
        if !authority_matches {
            return prepare_refusal(self, BindingTerminalPrepareError::Authority);
        }
        let expected_order = match self.frontiers().order().ledger().high() {
            crate::lifecycle::OrderHigh::Empty => Some(0),
            crate::lifecycle::OrderHigh::Allocated(high) => high.checked_add(1),
        };
        if expected_order != Some(next_transaction_order) {
            return prepare_refusal(self, BindingTerminalPrepareError::TransactionOrder);
        }
        if self
            .frontiers()
            .sequence()
            .ledger()
            .high_watermark()
            .checked_add(1)
            != Some(next_delivery_sequence)
        {
            return prepare_refusal(self, BindingTerminalPrepareError::DeliverySequence);
        }
        if hard_observer_progress > self.frontiers().sequence().ledger().high_watermark() {
            return prepare_refusal(self, BindingTerminalPrepareError::ObserverProgress);
        }
        Ok(PreparedBindingTerminal {
            owner: self,
            key: CandidateTerminalKey {
                active_binding,
                cause_class,
                admission_order: AdmissionOrder::binding_terminal(
                    next_transaction_order,
                    active_binding.participant_id,
                ),
                delivery_seq: next_delivery_sequence,
            },
            hard_observer_progress,
        })
    }
}

fn prepare_refusal(
    owner: LiveFrontierOwner,
    error: BindingTerminalPrepareError,
) -> Result<PreparedBindingTerminal, Box<BindingTerminalPrepareRefused>> {
    Err(Box::new(BindingTerminalPrepareRefused { owner, error }))
}
