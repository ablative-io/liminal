use super::{
    ActiveIdentityRanks, BindingEpoch, ClaimFrontiers, DeliverySeq, FrontierBinding,
    FrontierParticipant, LiveFrontierTransitionError, ParticipantId, PrecedenceCondition,
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
            // A retained marker sitting below the measured floor is NOT one of
            // §0.16's three clearing conditions: nothing the amendment names
            // clears it, so it must never be dressed as a settlement.
            return Err(LiveFrontierTransitionError::Precedence(
                PrecedenceCondition::Unclassified,
            ));
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

    /// Installs an already-admissible finalized binding-fate floor.
    ///
    /// The caller re-mints the floor against THIS frontier before calling
    /// (`operations/live_frontier/binding_fate_transition.rs`), so both guards
    /// below are BACKSTOPS: after the re-mint neither can fire, and either
    /// firing is a bug report rather than control flow. They stay because the
    /// rules they enforce are correct — a floor advance must not run backwards
    /// past the retained floor, and must not silently eat a retained marker.
    ///
    /// Takes the floor already widened. The admissible interval's upper end is
    /// `high_watermark + 1`, which is one past the `DeliverySeq` domain at the
    /// top of that domain, so the wide value is the honest one to carry.
    pub(in crate::lifecycle) fn install_finalized_binding_fate_floor(
        mut self,
        resulting_floor: u128,
    ) -> Result<Self, LiveFrontierTransitionError> {
        let retained_end = u128::from(self.sequence.ledger().high_watermark()) + 1;
        if resulting_floor < self.retained_floor || resulting_floor > retained_end {
            return Err(LiveFrontierTransitionError::ResultingFrontier);
        }
        if self
            .marker_records
            .iter()
            .any(|record| u128::from(record.delivery_seq) < resulting_floor)
        {
            // A retained marker sitting below the measured floor is NOT one of
            // §0.16's three clearing conditions: nothing the amendment names
            // clears it, so it must never be dressed as a settlement.
            return Err(LiveFrontierTransitionError::Precedence(
                PrecedenceCondition::Unclassified,
            ));
        }
        self.retained_floor = resulting_floor;
        self.retained_records
            .retain(|record| u128::from(record.delivery_seq) >= resulting_floor);
        self.marker_records
            .retain(|record| u128::from(record.delivery_seq) >= resulting_floor);
        Ok(self)
    }
}
