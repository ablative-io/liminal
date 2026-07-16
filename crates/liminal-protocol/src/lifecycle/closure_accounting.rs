use alloc::boxed::Box;

use crate::algebra::{ResourceDimension, ResourceVector, WideResourceVector};
use crate::wire::{
    ClosureCapacityReason, ClosureCheckedEnvelope, ClosureRefusalReason, ClosureSnapshot,
    MarkerClosureCapacityExceeded, ParticipantCursorProgressEdge, RepaymentEdge,
};

use super::{ClosureState, StoredEdge};

/// Invalid durable closure-accounting state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClosureAccountingError {
    /// The signed churn limit must be nonzero.
    ZeroChurnLimit,
    /// Durable churn use exceeds the signed limit.
    ChurnUsedExceedsLimit {
        /// Durable cycles already used.
        used: u64,
        /// Signed episode limit.
        limit: u64,
    },
    /// Marker anchors cannot outnumber slots owning marker credits.
    MarkerAnchorsExceedCredits {
        /// Current distinct marker anchors.
        anchors: u64,
        /// Current marker-capacity credits.
        credits: u64,
    },
    /// The durable baseline exceeds the configured capacity.
    BaselineExceedsCapacity {
        /// First failing component.
        dimension: ResourceDimension,
    },
    /// A clear edge retained edge-owned claims or recovery occupancy.
    ClearStateOwnsEdgeResources,
    /// A nonzero debt component cannot fit the frozen wire snapshot width.
    DebtOutsideWireDomain {
        /// First unencodable component.
        dimension: ResourceDimension,
    },
}

/// Validated unchanged-prestate closure accounting.
///
/// This wrapper owns every common field disclosed by a closure refusal. The
/// wire snapshot is derived from it, so a server binding cannot mix current and
/// proposed values or hand-construct an operation-specific refusal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClosureAccounting {
    state: ClosureState,
    marker_capacity_credits: u64,
    marker_anchors: u64,
    edge_sequence_claims: u64,
    edge_order_position_claims: u64,
    edge_k_remaining: ResourceVector,
    baseline: WideResourceVector,
    configured_cap: ResourceVector,
    episode_churn_used: u64,
    episode_churn_limit: u64,
}

impl ClosureAccounting {
    /// Validates one complete durable accounting snapshot.
    ///
    /// The edge-owned claim counts remain explicit durable facts because their
    /// exact occurrence plan is deliberately not the defective fixed array
    /// excluded by `docs/design/LP-EXTRACTION-GOAL.md` Fix 2.
    ///
    /// # Errors
    ///
    /// Returns [`ClosureAccountingError`] for a zero/overused churn bound,
    /// impossible anchor count, baseline outside capacity, edge resources in a
    /// clear state, or debt that cannot use the frozen `u64` wire fields.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        state: ClosureState,
        marker_capacity_credits: u64,
        marker_anchors: u64,
        edge_sequence_claims: u64,
        edge_order_position_claims: u64,
        edge_k_remaining: ResourceVector,
        baseline: WideResourceVector,
        configured_cap: ResourceVector,
        episode_churn_used: u64,
        episode_churn_limit: u64,
    ) -> Result<Self, ClosureAccountingError> {
        if episode_churn_limit == 0 {
            return Err(ClosureAccountingError::ZeroChurnLimit);
        }
        if episode_churn_used > episode_churn_limit {
            return Err(ClosureAccountingError::ChurnUsedExceedsLimit {
                used: episode_churn_used,
                limit: episode_churn_limit,
            });
        }
        if marker_anchors > marker_capacity_credits {
            return Err(ClosureAccountingError::MarkerAnchorsExceedCredits {
                anchors: marker_anchors,
                credits: marker_capacity_credits,
            });
        }
        if baseline.entries > u128::from(configured_cap.entries) {
            return Err(ClosureAccountingError::BaselineExceedsCapacity {
                dimension: ResourceDimension::Entries,
            });
        }
        if baseline.bytes > u128::from(configured_cap.bytes) {
            return Err(ClosureAccountingError::BaselineExceedsCapacity {
                dimension: ResourceDimension::Bytes,
            });
        }
        match state {
            ClosureState::Clear => {
                if edge_sequence_claims != 0
                    || edge_order_position_claims != 0
                    || edge_k_remaining != ResourceVector::default()
                {
                    return Err(ClosureAccountingError::ClearStateOwnsEdgeResources);
                }
            }
            ClosureState::Owed { debt, .. } => {
                let value = debt.value();
                if u64::try_from(value.entries).is_err() {
                    return Err(ClosureAccountingError::DebtOutsideWireDomain {
                        dimension: ResourceDimension::Entries,
                    });
                }
                if u64::try_from(value.bytes).is_err() {
                    return Err(ClosureAccountingError::DebtOutsideWireDomain {
                        dimension: ResourceDimension::Bytes,
                    });
                }
            }
        }
        Ok(Self {
            state,
            marker_capacity_credits,
            marker_anchors,
            edge_sequence_claims,
            edge_order_position_claims,
            edge_k_remaining,
            baseline,
            configured_cap,
            episode_churn_used,
            episode_churn_limit,
        })
    }

    /// Returns the typed clear-or-owed closure state.
    #[must_use]
    pub const fn state(self) -> ClosureState {
        self.state
    }

    /// Returns identity slots currently owning marker-capacity credits.
    #[must_use]
    pub const fn marker_capacity_credits(self) -> u64 {
        self.marker_capacity_credits
    }

    /// Returns current planned, undelivered, or delivered marker anchors.
    #[must_use]
    pub const fn marker_anchors(self) -> u64 {
        self.marker_anchors
    }

    /// Returns sequence claims owned by the current edge.
    #[must_use]
    pub const fn edge_sequence_claims(self) -> u64 {
        self.edge_sequence_claims
    }

    /// Returns transaction-order positions owned by the current edge.
    #[must_use]
    pub const fn edge_order_position_claims(self) -> u64 {
        self.edge_order_position_claims
    }

    /// Returns the exact current recovery-capacity occupancy.
    #[must_use]
    pub const fn edge_k_remaining(self) -> ResourceVector {
        self.edge_k_remaining
    }

    /// Returns the current retained baseline `B`.
    #[must_use]
    pub const fn baseline(self) -> WideResourceVector {
        self.baseline
    }

    /// Returns the configured entry/byte capacity.
    #[must_use]
    pub const fn configured_cap(self) -> ResourceVector {
        self.configured_cap
    }

    /// Returns activated churn cycles already used.
    #[must_use]
    pub const fn episode_churn_used(self) -> u64 {
        self.episode_churn_used
    }

    /// Returns the signed churn-cycle limit.
    #[must_use]
    pub const fn episode_churn_limit(self) -> u64 {
        self.episode_churn_limit
    }

    fn snapshot(self, delta_cycles: u64) -> ClosureSnapshot {
        let (debt, repayment_edge) = closure_state_wire(self.state);
        ClosureSnapshot {
            marker_capacity_credits: self.marker_capacity_credits,
            marker_anchors: self.marker_anchors,
            entry_debt: debt.entries,
            byte_debt: debt.bytes,
            repayment_edge,
            edge_sequence_claims: self.edge_sequence_claims,
            edge_order_position_claims: self.edge_order_position_claims,
            edge_k_remaining: self.edge_k_remaining,
            k_headroom: WideResourceVector::new(
                u128::from(self.configured_cap.entries) - self.baseline.entries,
                u128::from(self.configured_cap.bytes) - self.baseline.bytes,
            ),
            episode_churn_used: self.episode_churn_used,
            delta_cycles,
            episode_churn_limit: self.episode_churn_limit,
        }
    }
}

/// Invalid required-capacity simulation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequiredCapacityPlanError {
    /// A successor simulation supplied no reachable state.
    EmptySuccessorSet,
    /// A checked ordinary-capacity addition exceeded `u128`.
    ArithmeticOverflow {
        /// First overflowing component.
        dimension: ResourceDimension,
    },
}

/// Componentwise maximum required capacity across a finite successor plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequiredCapacityPlan {
    maximum: WideResourceVector,
}

impl RequiredCapacityPlan {
    /// Takes the componentwise maximum across every reachable successor node.
    ///
    /// # Errors
    ///
    /// Returns [`RequiredCapacityPlanError::EmptySuccessorSet`] for an empty
    /// plan, which cannot prove closure coverage.
    pub fn from_successors(
        successors: &[WideResourceVector],
    ) -> Result<Self, RequiredCapacityPlanError> {
        let Some(first) = successors.first().copied() else {
            return Err(RequiredCapacityPlanError::EmptySuccessorSet);
        };
        let maximum = successors.iter().skip(1).fold(first, |maximum, state| {
            WideResourceVector::new(
                maximum.entries.max(state.entries),
                maximum.bytes.max(state.bytes),
            )
        });
        Ok(Self { maximum })
    }

    /// Derives an ordinary caller's required `B' + Q + K` vector.
    ///
    /// # Errors
    ///
    /// Returns [`RequiredCapacityPlanError::ArithmeticOverflow`] for the first
    /// component whose canonical additions cannot fit `u128`.
    pub fn ordinary(
        resulting_baseline: WideResourceVector,
        mandatory_bound: ResourceVector,
        full_recovery_claim: ResourceVector,
    ) -> Result<Self, RequiredCapacityPlanError> {
        let Some(entries_with_q) = resulting_baseline
            .entries
            .checked_add(u128::from(mandatory_bound.entries))
        else {
            return Err(RequiredCapacityPlanError::ArithmeticOverflow {
                dimension: ResourceDimension::Entries,
            });
        };
        let Some(entries) = entries_with_q.checked_add(u128::from(full_recovery_claim.entries))
        else {
            return Err(RequiredCapacityPlanError::ArithmeticOverflow {
                dimension: ResourceDimension::Entries,
            });
        };
        let Some(bytes_with_q) = resulting_baseline
            .bytes
            .checked_add(u128::from(mandatory_bound.bytes))
        else {
            return Err(RequiredCapacityPlanError::ArithmeticOverflow {
                dimension: ResourceDimension::Bytes,
            });
        };
        let Some(bytes) = bytes_with_q.checked_add(u128::from(full_recovery_claim.bytes)) else {
            return Err(RequiredCapacityPlanError::ArithmeticOverflow {
                dimension: ResourceDimension::Bytes,
            });
        };
        Ok(Self {
            maximum: WideResourceVector::new(entries, bytes),
        })
    }

    /// Returns the simulated componentwise maximum.
    #[must_use]
    pub const fn maximum(self) -> WideResourceVector {
        self.maximum
    }
}

/// Successful stage-7 proof that no recovery fence applies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryFencePermit {
    accounting: ClosureAccounting,
}

impl RecoveryFencePermit {
    /// Returns the exact unchanged accounting checked at stage 7.
    #[must_use]
    pub const fn accounting(self) -> ClosureAccounting {
        self.accounting
    }
}

/// Stage-7 recovery-fence decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryFenceDecision {
    /// No detached-edge or second-quartet fence applies.
    Eligible(RecoveryFencePermit),
    /// Exact unchanged-prestate closure refusal.
    Respond(Box<MarkerClosureCapacityExceeded>),
}

/// Applies the stage-7 recovery-fence predicate before numeric admission.
#[must_use]
pub fn check_recovery_fence(
    request: &ClosureCheckedEnvelope,
    accounting: ClosureAccounting,
    recovery_fence: bool,
) -> RecoveryFenceDecision {
    if recovery_fence {
        return RecoveryFenceDecision::Respond(Box::new(closure_refusal(
            request.clone(),
            accounting,
            0,
            ClosureRefusalReason::RecoveryFence,
        )));
    }
    RecoveryFenceDecision::Eligible(RecoveryFencePermit { accounting })
}

/// Successful remaining closure gate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RemainingClosurePermit {
    accounting: ClosureAccounting,
    required_capacity: RequiredCapacityPlan,
    delta_cycles: u64,
}

impl RemainingClosurePermit {
    /// Returns the unchanged prestate accounting.
    #[must_use]
    pub const fn accounting(self) -> ClosureAccounting {
        self.accounting
    }

    /// Returns the checked componentwise successor maximum.
    #[must_use]
    pub const fn required_capacity(self) -> RequiredCapacityPlan {
        self.required_capacity
    }

    /// Returns the exact charged plan cycles.
    #[must_use]
    pub const fn delta_cycles(self) -> u64 {
        self.delta_cycles
    }
}

/// Stage-12 closure decision after observer admission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RemainingClosureDecision {
    /// Delivered-marker, churn, and componentwise capacity checks passed.
    Eligible(Box<RemainingClosurePermit>),
    /// Exact first remaining closure refusal.
    Respond(Box<MarkerClosureCapacityExceeded>),
}

/// Applies the remaining closure order: delivered marker, churn, then capacity.
///
/// Entries precede bytes and equality passes. A delivered-marker refusal uses
/// `delta_cycles=0`; churn and capacity serialize the exact proposed charge.
#[must_use]
pub fn check_remaining_closure(
    request: &ClosureCheckedEnvelope,
    accounting: ClosureAccounting,
    delivered_marker_awaiting_ack: bool,
    delta_cycles: u64,
    required_capacity: RequiredCapacityPlan,
) -> RemainingClosureDecision {
    if delivered_marker_awaiting_ack {
        return RemainingClosureDecision::Respond(Box::new(closure_refusal(
            request.clone(),
            accounting,
            0,
            ClosureRefusalReason::DeliveredMarkerAwaitingAck,
        )));
    }
    let resulting_cycles = u128::from(accounting.episode_churn_used) + u128::from(delta_cycles);
    if resulting_cycles > u128::from(accounting.episode_churn_limit) {
        return RemainingClosureDecision::Respond(Box::new(closure_refusal(
            request.clone(),
            accounting,
            delta_cycles,
            ClosureRefusalReason::EpisodeChurnLimit,
        )));
    }
    let maximum = required_capacity.maximum;
    if maximum.entries > u128::from(accounting.configured_cap.entries) {
        return RemainingClosureDecision::Respond(Box::new(closure_refusal(
            request.clone(),
            accounting,
            delta_cycles,
            ClosureRefusalReason::Capacity(ClosureCapacityReason {
                dimension: ResourceDimension::Entries,
                required: maximum.entries,
                limit: u128::from(accounting.configured_cap.entries),
            }),
        )));
    }
    if maximum.bytes > u128::from(accounting.configured_cap.bytes) {
        return RemainingClosureDecision::Respond(Box::new(closure_refusal(
            request.clone(),
            accounting,
            delta_cycles,
            ClosureRefusalReason::Capacity(ClosureCapacityReason {
                dimension: ResourceDimension::Bytes,
                required: maximum.bytes,
                limit: u128::from(accounting.configured_cap.bytes),
            }),
        )));
    }
    RemainingClosureDecision::Eligible(Box::new(RemainingClosurePermit {
        accounting,
        required_capacity,
        delta_cycles,
    }))
}

fn closure_refusal(
    request: ClosureCheckedEnvelope,
    accounting: ClosureAccounting,
    delta_cycles: u64,
    reason: ClosureRefusalReason,
) -> MarkerClosureCapacityExceeded {
    MarkerClosureCapacityExceeded {
        request,
        snapshot: accounting.snapshot(delta_cycles),
        reason,
    }
}

fn closure_state_wire(state: ClosureState) -> (ResourceVector, RepaymentEdge) {
    match state {
        ClosureState::Clear => (ResourceVector::default(), RepaymentEdge::None),
        ClosureState::Owed { debt, edge } => {
            let debt = debt.value();
            let wire_debt = ResourceVector::new(
                u64::try_from(debt.entries).map_or(u64::MAX, core::convert::identity),
                u64::try_from(debt.bytes).map_or(u64::MAX, core::convert::identity),
            );
            (wire_debt, stored_edge_wire(edge))
        }
    }
}

const fn stored_edge_wire(edge: StoredEdge) -> RepaymentEdge {
    match edge {
        StoredEdge::ObserverProjection(edge) => RepaymentEdge::ObserverProjection {
            through_seq: edge.through_seq(),
        },
        StoredEdge::PhysicalCompaction(edge) => RepaymentEdge::PhysicalCompaction {
            from_floor: edge.from_floor(),
            through_seq: edge.through_seq(),
        },
        StoredEdge::MarkerDelivery(edge) => RepaymentEdge::MarkerDelivery {
            participant_id: edge.participant_id(),
            binding_epoch: edge.binding_epoch(),
            marker_delivery_seq: edge.marker_delivery_seq(),
        },
        StoredEdge::ParticipantCursorProgress(edge) => {
            RepaymentEdge::ParticipantCursorProgress(ParticipantCursorProgressEdge {
                participant_id: edge.participant_id(),
                binding_epoch: edge.binding_epoch(),
                through_seq: edge.through_seq(),
                marker_delivery_seq: edge.marker_delivery_seq(),
            })
        }
        StoredEdge::DetachedCredentialRecovery(edge) => RepaymentEdge::DetachedCredentialRecovery {
            participant_id: edge.participant_id(),
            marker_delivery_seq: edge.marker_delivery_seq(),
            prior_binding_epoch: edge.prior_binding_epoch(),
        },
        StoredEdge::DetachedMarkerRelease(edge) => RepaymentEdge::DetachedMarkerRelease {
            participant_id: edge.participant_id(),
            marker_delivery_seq: edge.marker_delivery_seq(),
            last_dead_binding_epoch: edge.last_dead_binding_epoch(),
        },
        StoredEdge::DetachedCursorRelease(edge) => RepaymentEdge::DetachedCursorRelease {
            participant_id: edge.participant_id(),
            last_dead_binding_epoch: edge.last_dead_binding_epoch(),
        },
    }
}
