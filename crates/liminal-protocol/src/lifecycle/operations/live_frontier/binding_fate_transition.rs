use alloc::vec::Vec;

use crate::algebra::{AdmissibleFloor, admissible_installed_floor};
use crate::lifecycle::{ClosureAccounting, claim_frontier::BindingFateFrontierPlan};
use crate::wire::{BindingEpoch, DeliverySeq, ParticipantId};

use super::state::accounting_after_floor;
use super::{LiveFrontierError, LiveFrontierOwner, map_frontier_error};

pub(in crate::lifecycle::operations) struct BindingFateOwnerPlan {
    frontiers: BindingFateFrontierPlan,
    accounting: ClosureAccounting,
}

impl LiveFrontierOwner {
    pub(in crate::lifecycle::operations) fn prepare_binding_fate_transition(
        &self,
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        cursor: DeliverySeq,
        resulting_floor: DeliverySeq,
        reserve_finalizer: bool,
    ) -> Result<BindingFateOwnerPlan, LiveFrontierError> {
        if self.frontiers.retained_records().len() != self.retained_charges.len()
            || self
                .frontiers
                .retained_records()
                .iter()
                .zip(&self.retained_charges)
                .any(|(record, charge)| {
                    record.delivery_seq != charge.delivery_seq()
                        || record.admission_order != charge.admission_order()
                })
        {
            return Err(LiveFrontierError::RetainedCharge);
        }
        let frontiers = self
            .frontiers
            .prepare_binding_fate_transition(
                participant_id,
                binding_epoch,
                cursor,
                resulting_floor,
                reserve_finalizer,
            )
            .map_err(map_frontier_error)?;
        let released = self
            .retained_charges
            .iter()
            .copied()
            .take_while(|charge| charge.delivery_seq() < resulting_floor)
            .collect::<Vec<_>>();
        let accounting = accounting_after_floor(self.closure_accounting, &released)
            .ok_or(LiveFrontierError::ClosureAccounting)?;
        Ok(BindingFateOwnerPlan {
            frontiers,
            accounting,
        })
    }

    pub(in crate::lifecycle::operations) fn install_binding_fate_transition(
        mut self,
        plan: BindingFateOwnerPlan,
        resulting_floor: DeliverySeq,
    ) -> Self {
        self.frontiers = self
            .frontiers
            .install_binding_fate_transition(plan.frontiers);
        self.retained_charges
            .retain(|charge| charge.delivery_seq() >= resulting_floor);
        self.closure_accounting = plan.accounting;
        self
    }

    /// Applies a binding-fate floor that was MEASURED against an earlier
    /// frontier, re-minted against this one.
    ///
    /// PRECEDENCE-CLAMP M1/M1a. `PendingDiedOrdinaryFinalizer` freezes its
    /// floor at measurement time and the caller holds it across an enclosing
    /// transition, so by the time it arrives here the frontier it was measured
    /// against no longer exists: the retained floor may have advanced, markers
    /// may have been retained, and the high watermark has moved. Replaying the
    /// frozen value and re-checking it is what made a refusal here permanent —
    /// the enclosing source row is already durable, so the completion is
    /// retried on every boot and refuses identically forever.
    ///
    /// So the floor is RE-MINTED rather than replayed, against the marker set,
    /// retained floor and high watermark as they stand NOW. Clamping only
    /// downward would swap one permanent refusal for another: a downward clamp
    /// can land below the current retained floor and be refused as
    /// `ResultingFrontier`. `admissible_installed_floor` bounds BOTH ends.
    ///
    /// The subsumed case is a decision, not an accident. When the retained
    /// floor has already advanced past what this measurement could install, the
    /// fate's floor means nothing and installing the older value would drive
    /// the floor backwards over rows the frontier still owes. That is an
    /// explicit no-op success: nothing is released, no charge is dropped, and
    /// the accounting is untouched.
    ///
    /// The measured floor is never RAISED to reach the interval. Only lowering
    /// is safe: a raise would eat retained rows this fate never measured.
    /// `admissible_installed_floor` returns `min(measured, upper)`, so the
    /// installed floor is `<= measured` always.
    ///
    /// # What was traced, and what was NOT proven (PRECEDENCE-CLAMP M1b)
    ///
    /// The re-mint is structural insurance, and it is deliberately not resting
    /// on either of the reachability findings below.
    ///
    /// THE MARKER CONDITION CANNOT FIRE FROM A MARKER ADMITTED IN THE INTERVAL,
    /// and that is mechanical rather than argued. `marker_records` gains rows in
    /// exactly one place outside restore — `drain_next_marker_core`
    /// (`claim_frontier.rs`, the sole `marker_records.push`) — which refuses
    /// with `SequenceNotNext` unless the drained marker sits at exactly
    /// `high_watermark + 1`. The sequence high watermark is monotone
    /// non-decreasing, and a measured floor is at most `high_watermark + 1` at
    /// measurement time (the preferred floor is `min(member_cursor,
    /// observer_progress) + 1` with both inputs bounded by the watermark, and
    /// the retained floor it is maxed against is itself bounded by
    /// `high_watermark + 1`). So a marker retained after the measurement sits at
    /// or above the measured floor, and the enforcer refuses only on STRICTLY
    /// below. The condition can therefore only be met by a marker that was
    /// already below the floor when it was measured, which is exactly what the
    /// measurement-site clamp now prevents.
    ///
    /// THE SUBSUMED CONDITION IS MARKER-FREE AND INDEPENDENT. It reads no marker
    /// state at all: it needs only that `retained_floor` advanced past the
    /// measured floor while the finalizer waited.  `retained_floor` is written
    /// in exactly three places — the ordinary-record projection and the two
    /// binding-fate installs — and none of them appears between a
    /// `prepare_pending_died_*` and its matching completion in any server call
    /// chain that exists today (`ops_leave`, `ops_attach`, `ops_terminal_drain`
    /// are straight-line `&mut self` sequences, and boot repair drains prepared
    /// finalizers before anything else). NO REACHABLE SERVER PATH WAS FOUND.
    /// That is a negative result from tracing and NOT a proof: the concurrency
    /// of the conversation-authority seam was not exhaustively traced, and a
    /// future caller that holds a finalizer across an ordinary admission reaches
    /// it immediately. The unit
    /// `a_floor_subsumed_while_the_finalizer_waited_installs_as_a_no_op` shows
    /// the condition firing with an empty marker set, so the path is real at
    /// this crate's public boundary whatever the server does with it.
    ///
    /// # Errors
    ///
    /// Returns [`LiveFrontierError`] if the retained charges disagree with the
    /// retained rows, or if releasing the rows below the re-minted floor cannot
    /// be reconciled with the closure accounting.
    pub(in crate::lifecycle::operations) fn install_finalized_binding_fate_floor(
        mut self,
        measured_floor: DeliverySeq,
    ) -> Result<Self, LiveFrontierError> {
        // FIRST, unchanged and deliberately before the re-mint: a charge/row
        // disagreement is a real inconsistency and must still be reported as
        // `RetainedCharge`. Letting the subsumed no-op below return early would
        // swallow it.
        if self.frontiers.retained_records().len() != self.retained_charges.len()
            || self
                .frontiers
                .retained_records()
                .iter()
                .zip(&self.retained_charges)
                .any(|(record, charge)| {
                    record.delivery_seq != charge.delivery_seq()
                        || record.admission_order != charge.admission_order()
                })
        {
            return Err(LiveFrontierError::RetainedCharge);
        }
        let lowest_retained_marker_seq = self
            .frontiers
            .retained_marker_records()
            .iter()
            .map(|record| record.delivery_seq)
            .min();
        let resulting_floor = match admissible_installed_floor(
            u128::from(measured_floor),
            self.frontiers.retained_floor(),
            lowest_retained_marker_seq,
            self.frontiers.sequence().ledger().high_watermark(),
        ) {
            AdmissibleFloor::Subsumed => return Ok(self),
            AdmissibleFloor::Install(floor) => floor,
        };
        let released = self
            .retained_charges
            .iter()
            .copied()
            .take_while(|charge| u128::from(charge.delivery_seq()) < resulting_floor)
            .collect::<Vec<_>>();
        let accounting = accounting_after_floor(self.closure_accounting, &released)
            .ok_or(LiveFrontierError::ClosureAccounting)?;
        self.frontiers = self
            .frontiers
            .install_finalized_binding_fate_floor(resulting_floor)
            .map_err(map_frontier_error)?;
        self.retained_charges
            .retain(|charge| u128::from(charge.delivery_seq()) >= resulting_floor);
        self.closure_accounting = accounting;
        Ok(self)
    }
}
