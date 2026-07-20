use alloc::boxed::Box;

use crate::wire::{BindingEpoch, ConversationId, DeliverySeq, ParticipantId};

#[cfg(test)]
use super::FencedAttachCommit;
use super::{
    CommittedDiedTerminal, Event, OrdinaryBindingFate, RecoveredBindingFate, SealedBindingFateToken,
};

/// Protocol-private measurement inputs carried by one sealed fate token.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::lifecycle) struct BindingFateMeasurementContext {
    pub(in crate::lifecycle) conversation_id: ConversationId,
    pub(in crate::lifecycle) participant_id: ParticipantId,
    pub(in crate::lifecycle) binding_epoch: BindingEpoch,
    pub(in crate::lifecycle) cursor: DeliverySeq,
}

impl SealedBindingFateToken {
    /// Reports whether this token carries recovered occurrence authority.
    #[must_use]
    pub const fn is_recovered(&self) -> bool {
        self.recovered.is_some()
    }

    #[cfg(test)]
    pub(in crate::lifecycle) const fn from_recovered_for_test(
        recovered: FencedAttachCommit,
    ) -> Self {
        Self {
            ordinary: None,
            recovered: Some(recovered),
        }
    }

    /// Returns the exact protocol-owned identity whose floor must be measured.
    pub(in crate::lifecycle) const fn measurement_context(
        &self,
    ) -> Option<BindingFateMeasurementContext> {
        match (&self.ordinary, &self.recovered) {
            (Some(authority), None) => {
                let binding = authority.binding();
                Some(BindingFateMeasurementContext {
                    conversation_id: binding.conversation_id,
                    participant_id: binding.participant_id,
                    binding_epoch: binding.binding_epoch,
                    cursor: authority.through_seq(),
                })
            }
            (None, Some(proof)) => Some(BindingFateMeasurementContext {
                conversation_id: proof.conversation_id(),
                participant_id: proof.participant_id(),
                binding_epoch: proof.new_binding_epoch(),
                cursor: proof.marker_delivery_seq(),
            }),
            (None, None) | (Some(_), Some(_)) => None,
        }
    }

    /// Consumes ordinary authority and the exact committed Died terminal.
    pub(in crate::lifecycle) fn ordinary_binding_fate(
        mut self,
        terminal: CommittedDiedTerminal,
        resulting_floor: DeliverySeq,
    ) -> Result<OrdinaryBindingFate, Box<Self>> {
        if self.recovered.is_some() {
            return Err(Box::new(self));
        }
        let Some(authority) = self.ordinary.take() else {
            return Err(Box::new(self));
        };
        match authority.binding_fate(terminal, resulting_floor) {
            Ok(fate) => Ok(fate),
            Err(authority) => {
                self.ordinary = Some(authority);
                Err(Box::new(self))
            }
        }
    }

    /// Consumes recovered authority using a protocol-measured floor.
    pub(in crate::lifecycle) fn recovered_binding_fate_measured(
        self,
        resulting_floor: DeliverySeq,
    ) -> Result<RecoveredBindingFate, Box<Self>> {
        let Some(context) = self.measurement_context() else {
            return Err(Box::new(self));
        };
        self.recovered_binding_fate(Event::binding_fate_observed(
            context.participant_id,
            context.binding_epoch,
            resulting_floor,
        ))
    }

    /// Consumes recovered authority into one fate event.
    ///
    /// # Errors
    ///
    /// Returns the same move-only token on refusal, boxed to keep the successful
    /// return path compact.
    pub fn recovered_binding_fate(
        mut self,
        event: Event,
    ) -> Result<RecoveredBindingFate, Box<Self>> {
        if self.ordinary.is_some() {
            return Err(Box::new(self));
        }
        let Some(proof) = self.recovered.take() else {
            return Err(Box::new(self));
        };
        match proof.recovered_binding_fate(event) {
            Ok(fate) => Ok(fate),
            Err(proof) => {
                self.recovered = Some(*proof);
                Err(Box::new(self))
            }
        }
    }
}
