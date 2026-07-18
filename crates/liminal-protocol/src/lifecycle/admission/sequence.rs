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
///
/// Storage bindings may restore and inspect this factual snapshot, but cannot
/// invoke the lower-level planner directly. Executable admission is owned by
/// the protocol's total lifecycle operations.
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::SequenceLedger;
///
/// fn bypass_total_operation(ledger: SequenceLedger) {
///     let _ = ledger.plan_ordinary_record(0);
/// }
/// ```
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

    /// Plans optional enrollment: one record, one member, one terminal claim,
    /// and the protocol-computed new marker claims.
    ///
    /// Existing recovery claims are preserved. `E`, `L_other`, and all product
    /// terms remain derived from the resulting primitive state.
    ///
    /// # Errors
    ///
    /// Returns the first arithmetic variant of [`SequenceAdmissionError`] whose
    /// checked addition cannot be represented.
    #[cfg(test)]
    pub(crate) fn plan_enrollment(
        self,
        new_markers: u64,
    ) -> Result<ResultingSequenceState, SequenceAdmissionError> {
        self.plan_enrollment_with_recovery_quartet(new_markers, false)
    }

    /// Plans enrollment while allowing the protocol-owned closure projection to
    /// endow the episode's sole coupled `RS=1,RT=1` reserve.
    ///
    /// The endowment selector is crate-private so a storage binding cannot mint
    /// recovery claims from raw values.
    pub(crate) fn plan_enrollment_with_recovery_quartet(
        self,
        new_markers: u64,
        endow_recovery_quartet: bool,
    ) -> Result<ResultingSequenceState, SequenceAdmissionError> {
        self.ensure_quartet_can_be_endowed(endow_recovery_quartet)?;
        let high_watermark = self.checked_high_watermark(1)?;
        let live_members = self.claims.live_members.checked_add(1).ok_or(
            SequenceAdmissionError::LiveMemberClaimOverflow {
                live_members: self.claims.live_members,
            },
        )?;
        let binding_terminals = self.claims.binding_terminals.checked_add(1).ok_or(
            SequenceAdmissionError::BindingTerminalClaimOverflow {
                binding_terminals: self.claims.binding_terminals,
            },
        )?;
        let markers = self.checked_markers(new_markers)?;
        Ok(ResultingSequenceState {
            high_watermark,
            claims: SequenceClaims {
                live_members,
                binding_terminals,
                markers,
                recovery: if endow_recovery_quartet {
                    RecoverySequenceReserve::DetachedCredentialRecovery
                } else {
                    self.claims.recovery
                },
            },
        })
    }

    /// Plans optional detached attach: one record, one terminal claim, and new markers.
    ///
    /// Membership and existing recovery claims are preserved.
    ///
    /// # Errors
    ///
    /// Returns the first arithmetic variant of [`SequenceAdmissionError`] whose
    /// checked addition cannot be represented.
    #[cfg(test)]
    pub(crate) fn plan_detached_attach(
        self,
        new_markers: u64,
    ) -> Result<ResultingSequenceState, SequenceAdmissionError> {
        let high_watermark = self.checked_high_watermark(1)?;
        let binding_terminals = self.claims.binding_terminals.checked_add(1).ok_or(
            SequenceAdmissionError::BindingTerminalClaimOverflow {
                binding_terminals: self.claims.binding_terminals,
            },
        )?;
        let markers = self.checked_markers(new_markers)?;
        Ok(ResultingSequenceState {
            high_watermark,
            claims: SequenceClaims {
                binding_terminals,
                markers,
                ..self.claims
            },
        })
    }

    /// Plans optional supersession: two records and the new marker claims.
    ///
    /// The old terminal claim is consumed while the replacement binding creates
    /// one, so `T` is unchanged. Membership and recovery claims are preserved.
    ///
    /// # Errors
    ///
    /// Returns the first arithmetic variant of [`SequenceAdmissionError`] whose
    /// checked addition cannot be represented.
    #[cfg(test)]
    pub(crate) fn plan_supersession(
        self,
        new_markers: u64,
    ) -> Result<ResultingSequenceState, SequenceAdmissionError> {
        let high_watermark = self.checked_high_watermark(2)?;
        let markers = self.checked_markers(new_markers)?;
        Ok(ResultingSequenceState {
            high_watermark,
            claims: SequenceClaims {
                markers,
                ..self.claims
            },
        })
    }

    /// Plans ordinary admission: one record and the new marker claims.
    ///
    /// Membership, terminal, and recovery claims are unchanged.
    ///
    /// # Errors
    ///
    /// Returns the first arithmetic variant of [`SequenceAdmissionError`] whose
    /// checked addition cannot be represented.
    pub(crate) fn plan_ordinary_record(
        self,
        new_markers: u64,
    ) -> Result<ResultingSequenceState, SequenceAdmissionError> {
        let high_watermark = self.checked_high_watermark(1)?;
        let markers = self.checked_markers(new_markers)?;
        Ok(ResultingSequenceState {
            high_watermark,
            claims: SequenceClaims {
                markers,
                ..self.claims
            },
        })
    }

    const fn ensure_quartet_can_be_endowed(
        self,
        endow_recovery_quartet: bool,
    ) -> Result<(), SequenceAdmissionError> {
        if endow_recovery_quartet
            && matches!(
                self.claims.recovery,
                RecoverySequenceReserve::DetachedCredentialRecovery
            )
        {
            return Err(SequenceAdmissionError::RecoverySequenceReserveAlreadyPresent);
        }
        Ok(())
    }

    /// Applies fenced recovery from its coupled `RS=1, RT=1` reserve.
    ///
    /// The recovery Attached record consumes `RS`, while `RT` transfers into a
    /// normal `T` claim for the recovered binding. The high watermark advances
    /// once, recovery claims become empty, and no optional sequence allocation
    /// is performed.
    ///
    /// # Errors
    ///
    /// Returns [`SequenceAdmissionError::RecoverySequenceReserveMissing`] if no
    /// DCR pair exists, an arithmetic error if a checked addition fails, or
    /// [`SequenceAdmissionError::RecoverySequenceInvariantViolation`] if the
    /// exact reserved transfer did not preserve the ledger invariant.
    pub(in crate::lifecycle) fn apply_fenced_recovery(
        self,
    ) -> Result<Self, SequenceAdmissionError> {
        if self.claims.recovery != RecoverySequenceReserve::DetachedCredentialRecovery {
            return Err(SequenceAdmissionError::RecoverySequenceReserveMissing);
        }
        let high_watermark = self.checked_high_watermark(1)?;
        let binding_terminals = self.claims.binding_terminals.checked_add(1).ok_or(
            SequenceAdmissionError::BindingTerminalClaimOverflow {
                binding_terminals: self.claims.binding_terminals,
            },
        )?;
        let claims = SequenceClaims {
            binding_terminals,
            recovery: RecoverySequenceReserve::None,
            ..self.claims
        };
        let budget = claims.budget(high_watermark);
        let required_reserve = checked_required_reserve(&budget)
            .ok_or(SequenceAdmissionError::RecoverySequenceInvariantViolation)?;
        if required_reserve > u128::from(budget.remaining) {
            return Err(SequenceAdmissionError::RecoverySequenceInvariantViolation);
        }
        Ok(Self {
            high_watermark,
            claims,
            required_reserve,
        })
    }

    fn checked_high_watermark(self, required_values: u64) -> Result<u64, SequenceAdmissionError> {
        self.high_watermark.checked_add(required_values).ok_or(
            SequenceAdmissionError::HighWatermarkOverflow {
                high_watermark: self.high_watermark,
                required_values,
            },
        )
    }

    fn checked_markers(self, new_markers: u64) -> Result<u64, SequenceAdmissionError> {
        self.claims.markers.checked_add(new_markers).ok_or(
            SequenceAdmissionError::MarkerClaimOverflow {
                markers: self.claims.markers,
                new_markers,
            },
        )
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
    /// Appending the operation's record count would exceed the sequence domain.
    HighWatermarkOverflow {
        /// Current high watermark.
        high_watermark: u64,
        /// Number of consecutive values the transition requires.
        required_values: u64,
    },
    /// Enrollment cannot add another live member claim.
    LiveMemberClaimOverflow {
        /// Current live member count.
        live_members: u64,
    },
    /// Enrollment, detached attach, or fenced recovery cannot add a terminal claim.
    BindingTerminalClaimOverflow {
        /// Current binding-terminal claim count.
        binding_terminals: u64,
    },
    /// The protocol-computed marker increment cannot be represented.
    MarkerClaimOverflow {
        /// Current marker claim count.
        markers: u64,
        /// New marker claims proposed by the fixed-point simulation.
        new_markers: u64,
    },
    /// Fenced recovery was attempted without the coupled `RS=1, RT=1` reserve.
    RecoverySequenceReserveMissing,
    /// A fixed-point projection attempted to mint a second `RS`/`RT` pair.
    RecoverySequenceReserveAlreadyPresent,
    /// A reserved fenced-recovery transfer failed to preserve the sequence invariant.
    RecoverySequenceInvariantViolation,
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
