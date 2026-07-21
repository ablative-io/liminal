use alloc::vec::Vec;

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
            .prepare_binding_fate_transition(participant_id, binding_epoch, cursor, resulting_floor)
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
}
