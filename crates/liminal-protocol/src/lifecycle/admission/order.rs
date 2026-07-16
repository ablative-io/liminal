use alloc::boxed::Box;

use crate::wire::{ConversationOrderExhausted, OrderAllocatingEnvelope, TransactionOrder};

/// Movable unmaterialized transaction-order claims.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OrderClaims {
    active_binding_terminals: u64,
    membership_exits: u64,
    recovery_operation: bool,
    recovery_replacement_terminal: bool,
}

impl OrderClaims {
    /// Creates the four exact `A`, `X`, `RO`, and `RA` claim classes only when
    /// the recovery pair is coupled.
    ///
    /// # Errors
    ///
    /// Returns [`OrderClaimsInvariantError::RecoveryPairMismatch`] when exactly
    /// one of `RO` and `RA` is present.
    pub const fn new(
        active_binding_terminals: u64,
        membership_exits: u64,
        recovery_operation: bool,
        recovery_replacement_terminal: bool,
    ) -> Result<Self, OrderClaimsInvariantError> {
        if recovery_operation != recovery_replacement_terminal {
            return Err(OrderClaimsInvariantError::RecoveryPairMismatch {
                recovery_operation,
                recovery_replacement_terminal,
            });
        }
        Ok(Self {
            active_binding_terminals,
            membership_exits,
            recovery_operation,
            recovery_replacement_terminal,
        })
    }

    /// Returns active binding-terminal claims `A`.
    #[must_use]
    pub const fn active_binding_terminals(self) -> u64 {
        self.active_binding_terminals
    }

    /// Returns live membership-exit claims `X`.
    #[must_use]
    pub const fn membership_exits(self) -> u64 {
        self.membership_exits
    }

    /// Returns whether the stored edge owns recovery-operation claim `RO`.
    #[must_use]
    pub const fn recovery_operation(self) -> bool {
        self.recovery_operation
    }

    /// Returns whether the stored edge owns replacement-terminal claim `RA`.
    #[must_use]
    pub const fn recovery_replacement_terminal(self) -> bool {
        self.recovery_replacement_terminal
    }

    /// Returns checked-wide `A + X + RO + RA`.
    #[must_use]
    pub fn total(self) -> u128 {
        u128::from(self.active_binding_terminals)
            + u128::from(self.membership_exits)
            + u128::from(self.recovery_operation)
            + u128::from(self.recovery_replacement_terminal)
    }
}

/// Invalid primitive order-claim state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderClaimsInvariantError {
    /// The sole recovery-operation and replacement-terminal claims are coupled.
    RecoveryPairMismatch {
        /// Whether `RO` was present.
        recovery_operation: bool,
        /// Whether `RA` was present.
        recovery_replacement_terminal: bool,
    },
}

/// Persisted transaction-order high-watermark state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OrderHigh {
    /// No major has been allocated; all `2^64` values remain.
    #[default]
    Empty,
    /// Highest major already allocated exactly once.
    Allocated(TransactionOrder),
}

/// Invalid persisted order ledger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderLedgerInvariantError {
    /// Unmaterialized claims exceed the exact remaining counter suffix.
    ClaimsExceedRemaining {
        /// Exact remaining values.
        remaining: u128,
        /// Exact four-class claim count.
        claims: u128,
    },
}

/// Validated transaction-order high watermark and reserved claims.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderLedger {
    high: OrderHigh,
    claims: OrderClaims,
}

impl OrderLedger {
    /// Restores a ledger only when every unmaterialized claim owns a remaining value.
    ///
    /// # Errors
    ///
    /// Returns [`OrderLedgerInvariantError`] if claims exceed the exact suffix.
    pub fn try_new(
        high: OrderHigh,
        claims: OrderClaims,
    ) -> Result<Self, OrderLedgerInvariantError> {
        let remaining = remaining_after(high);
        let claim_count = claims.total();
        if claim_count > remaining {
            return Err(OrderLedgerInvariantError::ClaimsExceedRemaining {
                remaining,
                claims: claim_count,
            });
        }
        Ok(Self { high, claims })
    }

    /// Returns the persisted high-watermark state.
    #[must_use]
    pub const fn high(self) -> OrderHigh {
        self.high
    }

    /// Returns the exact four-class claim state.
    #[must_use]
    pub const fn claims(self) -> OrderClaims {
        self.claims
    }

    /// Returns exact unallocated majors remaining.
    #[must_use]
    pub fn remaining(self) -> u128 {
        remaining_after(self.high)
    }

    /// Plans optional enrollment's `A+1, X+1` claim transition.
    ///
    /// Existing `RO` and `RA` claims are preserved. The returned claim state is
    /// sealed and can only be consumed by [`allocate_order`].
    ///
    /// # Errors
    ///
    /// Returns [`OrderAdmissionError::ActiveBindingClaimOverflow`] or
    /// [`OrderAdmissionError::MembershipExitClaimOverflow`] for the first
    /// checked addition that cannot be represented.
    pub fn plan_enrollment(self) -> Result<ResultingOrderClaims, OrderAdmissionError> {
        self.plan_enrollment_with_recovery_quartet(false)
    }

    /// Plans enrollment while allowing the protocol-owned closure projection to
    /// endow the episode's sole `RO`/`RA` pair.
    ///
    /// The boolean is crate-private deliberately: only the fixed-point planner
    /// may derive it from durable marker/binding facts. A storage binding cannot
    /// request a quartet directly.
    pub(crate) fn plan_enrollment_with_recovery_quartet(
        self,
        endow_recovery_quartet: bool,
    ) -> Result<ResultingOrderClaims, OrderAdmissionError> {
        self.ensure_quartet_can_be_endowed(endow_recovery_quartet)?;
        let active_binding_terminals = self
            .claims
            .active_binding_terminals
            .checked_add(1)
            .ok_or(OrderAdmissionError::ActiveBindingClaimOverflow)?;
        let membership_exits = self
            .claims
            .membership_exits
            .checked_add(1)
            .ok_or(OrderAdmissionError::MembershipExitClaimOverflow)?;
        Ok(ResultingOrderClaims(OrderClaims {
            active_binding_terminals,
            membership_exits,
            recovery_operation: self.claims.recovery_operation || endow_recovery_quartet,
            recovery_replacement_terminal: self.claims.recovery_replacement_terminal
                || endow_recovery_quartet,
        }))
    }

    /// Plans optional detached attach's `A+1` claim transition.
    ///
    /// Membership and both recovery order claims are preserved.
    ///
    /// # Errors
    ///
    /// Returns [`OrderAdmissionError::ActiveBindingClaimOverflow`] if the new
    /// terminal claim cannot be represented.
    pub fn plan_detached_attach(self) -> Result<ResultingOrderClaims, OrderAdmissionError> {
        let active_binding_terminals = self
            .claims
            .active_binding_terminals
            .checked_add(1)
            .ok_or(OrderAdmissionError::ActiveBindingClaimOverflow)?;
        Ok(ResultingOrderClaims(OrderClaims {
            active_binding_terminals,
            ..self.claims
        }))
    }

    /// Plans supersession's transfer of the existing `A` claim.
    ///
    /// The aggregate `A`, `X`, `RO`, and `RA` counts are unchanged: the old
    /// binding's terminal claim moves to the replacement binding.
    #[must_use]
    pub const fn plan_supersession(self) -> ResultingOrderClaims {
        ResultingOrderClaims(self.claims)
    }

    /// Plans ordinary record admission, which creates no order claim.
    #[must_use]
    pub const fn plan_ordinary_record(self) -> ResultingOrderClaims {
        ResultingOrderClaims(self.claims)
    }

    const fn ensure_quartet_can_be_endowed(
        self,
        endow_recovery_quartet: bool,
    ) -> Result<(), OrderAdmissionError> {
        if endow_recovery_quartet
            && (self.claims.recovery_operation || self.claims.recovery_replacement_terminal)
        {
            return Err(OrderAdmissionError::RecoveryOrderReserveAlreadyPresent);
        }
        Ok(())
    }

    /// Applies fenced recovery using only its pre-owned `RO` and `RA` claims.
    ///
    /// `RO` is consumed by the recovery operation and `RA` transfers into one
    /// new active binding-terminal `A` claim. No caller major is allocated, so
    /// the order high watermark is unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`OrderAdmissionError::RecoveryOrderReserveMissing`] unless both
    /// coupled recovery claims exist, or
    /// [`OrderAdmissionError::ActiveBindingClaimOverflow`] if `RA` cannot be
    /// transferred into `A`.
    pub fn apply_fenced_recovery(self) -> Result<Self, OrderAdmissionError> {
        if !self.claims.recovery_operation || !self.claims.recovery_replacement_terminal {
            return Err(OrderAdmissionError::RecoveryOrderReserveMissing {
                recovery_operation: self.claims.recovery_operation,
                recovery_replacement_terminal: self.claims.recovery_replacement_terminal,
            });
        }
        let active_binding_terminals = self
            .claims
            .active_binding_terminals
            .checked_add(1)
            .ok_or(OrderAdmissionError::ActiveBindingClaimOverflow)?;
        Ok(Self {
            high: self.high,
            claims: OrderClaims {
                active_binding_terminals,
                membership_exits: self.claims.membership_exits,
                recovery_operation: false,
                recovery_replacement_terminal: false,
            },
        })
    }
}

/// Protocol-produced post-operation order claims.
///
/// Construction is crate-private: consuming servers restore current claims but
/// cannot invent the simulated post-state used by admission precedence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResultingOrderClaims(OrderClaims);

impl ResultingOrderClaims {
    const fn claims(self) -> OrderClaims {
        self.0
    }
}

/// Successful allocation of one unreserved transaction-order major.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderAllocation {
    major: TransactionOrder,
    resulting: OrderLedger,
}

impl OrderAllocation {
    /// Returns the newly allocated major.
    #[must_use]
    pub const fn major(self) -> TransactionOrder {
        self.major
    }

    /// Returns the complete validated post-allocation ledger.
    #[must_use]
    pub const fn resulting(self) -> OrderLedger {
        self.resulting
    }
}

/// Order admission refusal or impossible simulated state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrderAdmissionError {
    /// Canonical wire-visible order exhaustion.
    Exhausted(Box<ConversationOrderExhausted>),
    /// A protocol simulation produced more claims than the initial suffix can own.
    InitialClaimsExceedRemaining {
        /// Remaining values after allocating major zero.
        remaining: u128,
        /// Simulated resulting claim count.
        resulting_claims: u128,
    },
    /// Adding an active binding-terminal claim would exceed u64.
    ActiveBindingClaimOverflow,
    /// Adding a membership-exit claim would exceed u64.
    MembershipExitClaimOverflow,
    /// A fixed-point projection attempted to mint a second `RO`/`RA` pair.
    RecoveryOrderReserveAlreadyPresent,
    /// Fenced recovery lacks one or both coupled reserved order claims.
    RecoveryOrderReserveMissing {
        /// Whether the current edge owns recovery-operation claim `RO`.
        recovery_operation: bool,
        /// Whether the current edge owns replacement-terminal claim `RA`.
        recovery_replacement_terminal: bool,
    },
}

/// Allocates one caller major after applying the sealed simulated claim state.
///
/// External callers cannot construct [`ResultingOrderClaims`]; it is emitted by
/// protocol-owned lifecycle simulation so the consuming server cannot invent
/// post-operation obligations.
///
/// # Errors
///
/// Returns canonical [`OrderAdmissionError::Exhausted`] when no unreserved
/// major remains, or an invariant error for an impossible initial simulation.
pub fn allocate_order(
    request: OrderAllocatingEnvelope,
    current: OrderLedger,
    resulting_claims: ResultingOrderClaims,
) -> Result<OrderAllocation, OrderAdmissionError> {
    let resulting_claims = resulting_claims.claims();
    let resulting_claim_count = resulting_claims.total();
    match current.high {
        OrderHigh::Empty => {
            let major = 0;
            let resulting_remaining = u128::from(u64::MAX);
            if resulting_claim_count > resulting_remaining {
                return Err(OrderAdmissionError::InitialClaimsExceedRemaining {
                    remaining: resulting_remaining,
                    resulting_claims: resulting_claim_count,
                });
            }
            Ok(OrderAllocation {
                major,
                resulting: OrderLedger {
                    high: OrderHigh::Allocated(major),
                    claims: resulting_claims,
                },
            })
        }
        OrderHigh::Allocated(high) => {
            let order_remaining = current.remaining();
            let reserved_claims = current.claims.total();
            let next = high.checked_add(1);
            let resulting_order_remaining = next.map_or(0, |_| order_remaining - 1);
            let Some(major) = next else {
                return Err(OrderAdmissionError::Exhausted(Box::new(
                    ConversationOrderExhausted::new(
                        request,
                        high,
                        order_remaining,
                        reserved_claims,
                        resulting_order_remaining,
                        resulting_claim_count,
                    ),
                )));
            };
            if resulting_claim_count > resulting_order_remaining {
                return Err(OrderAdmissionError::Exhausted(Box::new(
                    ConversationOrderExhausted::new(
                        request,
                        high,
                        order_remaining,
                        reserved_claims,
                        resulting_order_remaining,
                        resulting_claim_count,
                    ),
                )));
            }
            Ok(OrderAllocation {
                major,
                resulting: OrderLedger {
                    high: OrderHigh::Allocated(major),
                    claims: resulting_claims,
                },
            })
        }
    }
}

fn remaining_after(high: OrderHigh) -> u128 {
    match high {
        OrderHigh::Empty => u128::from(u64::MAX) + 1,
        OrderHigh::Allocated(high) => u128::from(u64::MAX - high),
    }
}
