use super::{
    ActiveIdentityRanks, BindingEpoch, ClaimFrontiers, DeliverySeq, FrontierBinding,
    FrontierParticipant, LiveFrontierTransitionError, ParticipantId,
};

pub(in crate::lifecycle) struct BindingFateFrontierPlan {
    active_identities: ActiveIdentityRanks,
    resulting_floor: u128,
}

impl ClaimFrontiers {
    pub(in crate::lifecycle) fn prepare_binding_fate_transition(
        &self,
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        cursor: DeliverySeq,
        resulting_floor: DeliverySeq,
        reserve_finalizer: bool,
    ) -> Result<BindingFateFrontierPlan, LiveFrontierTransitionError> {
        let mut participants = self.active_identities.participants().to_vec();
        let Some(participant) = participants
            .iter_mut()
            .find(|participant| participant.participant_index() == participant_id)
        else {
            return Err(LiveFrontierTransitionError::Authority);
        };
        if participant.cursor() != cursor
            || participant.binding() != FrontierBinding::Detached(binding_epoch)
        {
            return Err(LiveFrontierTransitionError::Authority);
        }
        let high_watermark = self.sequence.ledger().high_watermark();
        let resulting_floor = u128::from(resulting_floor);
        let retained_end = u128::from(high_watermark) + 1;
        if resulting_floor < self.retained_floor || resulting_floor > retained_end {
            return Err(LiveFrontierTransitionError::ResultingFrontier);
        }
        if self
            .marker_records
            .iter()
            .any(|record| u128::from(record.delivery_seq) < resulting_floor)
        {
            return Err(LiveFrontierTransitionError::Precedence);
        }
        let resulting_cursor = if reserve_finalizer {
            cursor
        } else {
            high_watermark
        };
        *participant = FrontierParticipant::new(
            participant_id,
            resulting_cursor,
            FrontierBinding::Detached(binding_epoch),
        );
        let active_identities =
            ActiveIdentityRanks::try_new(participants, high_watermark, self.identity_slot_limit)
                .map_err(|_| LiveFrontierTransitionError::Authority)?;
        Ok(BindingFateFrontierPlan {
            active_identities,
            resulting_floor,
        })
    }

    pub(in crate::lifecycle) fn install_binding_fate_transition(
        mut self,
        plan: BindingFateFrontierPlan,
    ) -> Self {
        self.active_identities = plan.active_identities;
        self.retained_floor = plan.resulting_floor;
        self.retained_records
            .retain(|record| u128::from(record.delivery_seq) >= plan.resulting_floor);
        self.marker_records
            .retain(|record| u128::from(record.delivery_seq) >= plan.resulting_floor);
        self
    }
}
