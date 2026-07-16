use alloc::boxed::Box;

use crate::wire::{ConversationSequenceExhausted, SequenceAllocatingEnvelope, SequenceBudget};

/// Edge-owned recovery sequence claims.
///
/// The frozen contract permits only the empty state or the DCR quartet's
/// coupled `RS=1, RT=1` state. Fenced recovery consumes `RS` and atomically
/// transfers `RT` into a normal terminal claim, so no durable half-pair exists.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RecoverySequenceReserve {
    /// No recovery attach or replacement-terminal sequence claim.
    #[default]
    None,
    /// One recovery attach claim and one replacement-terminal claim.
    DetachedCredentialRecovery,
}

impl RecoverySequenceReserve {
    const fn claims(self) -> (u64, u64) {
        match self {
            Self::None => (0, 0),
            Self::DetachedCredentialRecovery => (1, 1),
        }
    }
}

/// Primitive participant-lifecycle claims from which the sequence reserve is derived.
///
/// `E`, `L_other`, and all three products are deliberately absent from this
/// input. They are derived by the protocol so a caller cannot provide a
/// self-inconsistent canonical budget.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SequenceClaims {
    live_members: u64,
    binding_terminals: u64,
    markers: u64,
    recovery: RecoverySequenceReserve,
}

impl SequenceClaims {
    /// Creates the primitive `L`, `T`, `M`, and coupled recovery claim state.
    #[must_use]
    pub const fn new(
        live_members: u64,
        binding_terminals: u64,
        markers: u64,
        recovery: RecoverySequenceReserve,
    ) -> Self {
        Self {
            live_members,
            binding_terminals,
            markers,
            recovery,
        }
    }

    /// Returns live members `L`; flat exit claims `E` are exactly this value.
    #[must_use]
    pub const fn live_members(self) -> u64 {
        self.live_members
    }

    /// Returns binding-terminal claims `T`.
    #[must_use]
    pub const fn binding_terminals(self) -> u64 {
        self.binding_terminals
    }

    /// Returns required-but-unwritten marker claims `M`.
    #[must_use]
    pub const fn markers(self) -> u64 {
        self.markers
    }

    /// Returns the coupled recovery sequence reserve.
    #[must_use]
    pub const fn recovery(self) -> RecoverySequenceReserve {
        self.recovery
    }

    /// Derives the canonical ten-field budget at `high_watermark`.
    #[must_use]
    pub fn budget(self, high_watermark: u64) -> SequenceBudget {
        let live_members = u128::from(self.live_members);
        let exits = self.live_members;
        let exits_wide = u128::from(exits);
        let terminals = u128::from(self.binding_terminals);
        let (rs, rt) = self.recovery.claims();
        let replacement_terminals = u128::from(rt);
        let other_live_members = if live_members == 0 {
            0
        } else {
            live_members - 1
        };

        SequenceBudget {
            high_watermark,
            remaining: u64::MAX - high_watermark,
            e: exits,
            t: self.binding_terminals,
            m: self.markers,
            rs,
            rt,
            l_times_t: live_members * terminals,
            l_times_rt: live_members * replacement_terminals,
            l_other_times_e: other_live_members * exits_wide,
        }
    }

    /// Computes the exact reserve in the frozen canonical term order.
    ///
    /// The order is `E`, `T`, `M`, `RS`, `RT`, `L*T`, `L*RT`, then
    /// `L_other*E`, with every scalar widened before addition. `None` means the
    /// sum itself exceeds `u128`; each canonical product remains representable.
    #[must_use]
    pub fn checked_required_reserve(self) -> Option<u128> {
        checked_required_reserve(&self.budget(0))
    }
}

/// Invalid persisted sequence ledger.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SequenceLedgerInvariantError {
    /// Claims exceed the exact sequence suffix that remains after the high watermark.
    ClaimsExceedRemaining {
        /// Canonical ten-field budget derived from the invalid state.
        budget: Box<SequenceBudget>,
        /// Exact required reserve, or `None` if its canonical u128 sum overflowed.
        required_reserve: Option<u128>,
    },
}

/// Validated delivery-sequence watermark and all unmaterialized claims.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SequenceLedger {
    high_watermark: u64,
    claims: SequenceClaims,
    required_reserve: u128,
}

impl SequenceLedger {
    /// Restores a ledger only when its derived reserve fits the remaining suffix.
    ///
    /// # Errors
    ///
    /// Returns [`SequenceLedgerInvariantError`] if the canonical reserve sum
    /// overflows u128 or exceeds `u64::MAX - high_watermark`.
    pub fn try_new(
        high_watermark: u64,
        claims: SequenceClaims,
    ) -> Result<Self, SequenceLedgerInvariantError> {
        let budget = claims.budget(high_watermark);
        let required_reserve = checked_required_reserve(&budget);
        let Some(required_reserve) = required_reserve else {
            return Err(SequenceLedgerInvariantError::ClaimsExceedRemaining {
                budget: Box::new(budget),
                required_reserve,
            });
        };
        if required_reserve > u128::from(budget.remaining) {
            return Err(SequenceLedgerInvariantError::ClaimsExceedRemaining {
                budget: Box::new(budget),
                required_reserve: Some(required_reserve),
            });
        }
        Ok(Self {
            high_watermark,
            claims,
            required_reserve,
        })
    }

    /// Returns the greatest allocated delivery sequence.
    #[must_use]
    pub const fn high_watermark(self) -> u64 {
        self.high_watermark
    }

    /// Returns the primitive claims from which the reserve is derived.
    #[must_use]
    pub const fn claims(self) -> SequenceClaims {
        self.claims
    }

    /// Returns the canonical ten-field budget.
    #[must_use]
    pub fn budget(self) -> SequenceBudget {
        self.claims.budget(self.high_watermark)
    }

    /// Returns the exact checked-wide reserve owned by the claims.
    #[must_use]
    pub const fn required_reserve(self) -> u128 {
        self.required_reserve
    }
}

/// Protocol-produced proposed sequence state.
///
/// Construction is crate-private so a consuming server cannot invent the
/// post-operation `L`, `T`, `M`, `RS`, or `RT` state used by admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResultingSequenceState {
    high_watermark: u64,
    claims: SequenceClaims,
}

impl ResultingSequenceState {
    // This sealed producer is consumed by the protocol operation layer. Keep it
    // crate-private so a storage binding cannot forge simulated claims.
    #[allow(dead_code)]
    pub(crate) const fn from_parts(high_watermark: u64, claims: SequenceClaims) -> Self {
        Self {
            high_watermark,
            claims,
        }
    }
}

/// Successful sequence-reserve admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SequenceAdmission {
    resulting: SequenceLedger,
}

impl SequenceAdmission {
    /// Returns the complete validated proposed ledger.
    #[must_use]
    pub const fn resulting(self) -> SequenceLedger {
        self.resulting
    }
}

/// Sequence admission refusal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SequenceAdmissionError {
    /// The proposed canonical reserve exceeds the resulting sequence suffix.
    Exhausted(Box<ConversationSequenceExhausted>),
}

/// Applies the sealed resulting claim state to the sequence-reserve gate.
///
/// Exhaustion is decided from the complete resulting state, including every
/// fixed-point marker and recovery claim. The refusal carries exactly the
/// canonical ten-field [`SequenceBudget`] derived here.
///
/// # Errors
///
/// Returns [`SequenceAdmissionError::Exhausted`] if the canonical reserve sum
/// overflows u128 or is greater than `u64::MAX - resulting_high_watermark`.
pub fn admit_sequence(
    request: SequenceAllocatingEnvelope,
    resulting: ResultingSequenceState,
) -> Result<SequenceAdmission, SequenceAdmissionError> {
    let budget = resulting.claims.budget(resulting.high_watermark);
    let Some(required_reserve) = checked_required_reserve(&budget) else {
        return Err(SequenceAdmissionError::Exhausted(Box::new(
            ConversationSequenceExhausted {
                request,
                sequence_budget: budget,
            },
        )));
    };
    if required_reserve > u128::from(budget.remaining) {
        return Err(SequenceAdmissionError::Exhausted(Box::new(
            ConversationSequenceExhausted {
                request,
                sequence_budget: budget,
            },
        )));
    }

    Ok(SequenceAdmission {
        resulting: SequenceLedger {
            high_watermark: resulting.high_watermark,
            claims: resulting.claims,
            required_reserve,
        },
    })
}

fn checked_required_reserve(budget: &SequenceBudget) -> Option<u128> {
    let mut reserve = u128::from(budget.e);
    reserve = reserve.checked_add(u128::from(budget.t))?;
    reserve = reserve.checked_add(u128::from(budget.m))?;
    reserve = reserve.checked_add(u128::from(budget.rs))?;
    reserve = reserve.checked_add(u128::from(budget.rt))?;
    reserve = reserve.checked_add(budget.l_times_t)?;
    reserve = reserve.checked_add(budget.l_times_rt)?;
    reserve.checked_add(budget.l_other_times_e)
}
