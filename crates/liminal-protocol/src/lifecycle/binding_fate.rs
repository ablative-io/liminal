use alloc::boxed::Box;

use crate::wire::{BindingEpoch, ConversationId, DeliverySeq, ParticipantId};

#[cfg(test)]
use super::FencedAttachCommit;
use super::{
    CommittedDiedTerminal, Event, OrdinaryBindingFate, RecoveredBindingFate, SealedBindingFateToken,
};

/// Closed persistence shape carried by one sealed binding-fate token.
///
/// This projection exposes only the durable intent fields. It neither exposes
/// nor duplicates the move-only authority consumed by protocol measurement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SealedBindingFateIntent {
    /// A no-marker attachment must complete through an exact Died terminal.
    Ordinary,
    /// A fenced attachment retains the exact prior epoch and accepted marker.
    Recovered {
        /// Binding epoch whose marker authorized the fenced replacement.
        prior_binding_epoch: BindingEpoch,
        /// Exact accepted marker delivery sequence.
        marker_delivery_seq: DeliverySeq,
    },
}

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

    /// Returns the closed durable intent shape without surrendering authority.
    #[must_use]
    pub const fn intent(&self) -> Option<SealedBindingFateIntent> {
        match (&self.ordinary, &self.recovered) {
            (Some(_), None) => Some(SealedBindingFateIntent::Ordinary),
            (None, Some(proof)) => Some(SealedBindingFateIntent::Recovered {
                prior_binding_epoch: proof.prior_binding_epoch(),
                marker_delivery_seq: proof.marker_delivery_seq(),
            }),
            (None, None) | (Some(_), Some(_)) => None,
        }
    }

    #[cfg(test)]
    pub(in crate::lifecycle) const fn from_recovered_for_test(
        recovered: FencedAttachCommit,
    ) -> Self {
        let cursor = recovered.marker_delivery_seq();
        Self {
            ordinary: None,
            recovered: Some(recovered),
            cursor,
        }
    }

    /// Replays one protocol-selected normal acknowledgement into this token.
    pub(in crate::lifecycle) fn participant_ack_progressed(
        mut self,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        previous_cursor: DeliverySeq,
        through_seq: DeliverySeq,
    ) -> Result<Self, Box<Self>> {
        if previous_cursor != self.cursor || through_seq <= previous_cursor {
            return Err(Box::new(self));
        }
        match (self.ordinary.take(), self.recovered.as_ref()) {
            (Some(authority), None) => match authority.participant_ack_progressed(
                conversation_id,
                participant_id,
                binding_epoch,
                previous_cursor,
                through_seq,
            ) {
                Ok(authority) => self.ordinary = Some(authority),
                Err(authority) => {
                    self.ordinary = Some(authority);
                    return Err(Box::new(self));
                }
            },
            (None, Some(proof))
                if proof.conversation_id() == conversation_id
                    && proof.participant_id() == participant_id
                    && proof.new_binding_epoch() == binding_epoch => {}
            (ordinary, _) => {
                self.ordinary = ordinary;
                return Err(Box::new(self));
            }
        }
        self.cursor = through_seq;
        Ok(self)
    }

    /// Replays one protocol-selected MARKER acknowledgement into this token.
    ///
    /// A marker-ack advances the member's cursor exactly as an ordinary ack
    /// does, so the sealed token must follow it or the two disagree forever.
    /// The validation is therefore delegated, unchanged, to
    /// [`Self::participant_ack_progressed`]: the token tracks CURSOR
    /// PROGRESSION, and the operation that caused the progression does not
    /// change what has to hold.
    ///
    /// # ⛔ THE IDEMPOTENT ARM IS LOAD-BEARING AND ITS ABSENCE IS INVISIBLE BY MESSAGE
    ///
    /// `MarkerAckCommit::apply_to` is idempotent by documented contract:
    /// re-applying it to its own resulting cursor is a no-op returning the same
    /// outcome. The token is progressed alongside it, so the token must tolerate
    /// that same second application. Without the arm below, replaying an
    /// already-applied marker-ack fails `previous_cursor != self.cursor` and
    /// raises `ack cursor commit disagrees with sealed binding-fate authority`
    /// — **THE EXACT STRING THE MISSING-PROGRESSION DEFECT RAISES.**
    ///
    /// A fix whose failure mode is message-identical to the bug it repairs
    /// cannot be distinguished from that bug by any test asserting on the
    /// message: such a test passes in both worlds. That is why the units
    /// guarding this discriminate on STATE — whether the next ordinary ack
    /// COMMITS — and never on the refusal text.
    pub(in crate::lifecycle) fn marker_ack_progressed(
        self,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
        previous_cursor: DeliverySeq,
        through_seq: DeliverySeq,
    ) -> Result<Self, Box<Self>> {
        // ALREADY APPLIED: the token already sits at this commit's resulting
        // cursor, so the durable row is being replayed onto state that has
        // consumed it. `previous_cursor < through_seq` keeps a degenerate
        // no-progress commit out of this arm — that case falls through and is
        // refused below exactly as it always was.
        if self.cursor == through_seq && previous_cursor < through_seq {
            return Ok(self);
        }
        self.participant_ack_progressed(
            conversation_id,
            participant_id,
            binding_epoch,
            previous_cursor,
            through_seq,
        )
    }

    /// Returns the exact protocol-owned identity whose floor must be measured.
    pub(in crate::lifecycle) const fn measurement_context(
        &self,
    ) -> Option<BindingFateMeasurementContext> {
        match (&self.ordinary, &self.recovered) {
            (Some(authority), None) if authority.through_seq() == self.cursor => {
                let binding = authority.binding();
                Some(BindingFateMeasurementContext {
                    conversation_id: binding.conversation_id,
                    participant_id: binding.participant_id,
                    binding_epoch: binding.binding_epoch,
                    cursor: self.cursor,
                })
            }
            (None, Some(proof)) => Some(BindingFateMeasurementContext {
                conversation_id: proof.conversation_id(),
                participant_id: proof.participant_id(),
                binding_epoch: proof.new_binding_epoch(),
                cursor: self.cursor,
            }),
            (None | Some(_), None) | (Some(_), Some(_)) => None,
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

    pub(in crate::lifecycle) fn recovered_binding_fate(
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
