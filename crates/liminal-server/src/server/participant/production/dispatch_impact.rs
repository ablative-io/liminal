//! Exact post-commit dispatch-impact derivation under one conversation owner.

use std::collections::BTreeSet;

use liminal_protocol::lifecycle::BindingState;
use liminal_protocol::wire::ParticipantId;

use crate::server::participant::dispatch_impact::{
    DispatchEffect, DispatchImpactAccumulator, DispatchTarget,
};

use super::log::StoredOperation;
use super::outbox_log::OutboxRow;
use super::outbox_projection::{ReplayedProjectionFacts, project_committed_source};
use super::state::{ConversationAuthority, StateError};

impl ConversationAuthority {
    /// Resolves one exact current poststate binding target.
    fn dispatch_target(&self, participant_id: ParticipantId) -> Option<DispatchTarget> {
        self.slots
            .get(&participant_id)
            .and_then(|slot| match slot.binding {
                BindingState::Bound(active) => {
                    Some(DispatchTarget::new(participant_id, active.binding_epoch))
                }
                BindingState::Detached | BindingState::PendingFinalization(_) => None,
            })
    }

    /// Captures every exact current poststate binding.
    fn all_dispatch_targets(&self) -> BTreeSet<DispatchTarget> {
        self.slots
            .keys()
            .filter_map(|participant_id| self.dispatch_target(*participant_id))
            .collect()
    }

    /// Reprojects one exact installed durable source and records its Published
    /// recipients. The exhaustive projection owns the seven source kinds.
    pub(super) fn record_produced_source(
        &self,
        source_log_sequence: u64,
        source: &StoredOperation,
        facts: ReplayedProjectionFacts,
        impact: &mut DispatchImpactAccumulator,
    ) -> Result<(), StateError> {
        let projection = project_committed_source(self, source_log_sequence, source, facts)?
            .ok_or_else(|| {
                StateError::invariant("committed Produced source lost its projection")
            })?;
        self.record_published_projection(&projection, impact)
    }

    /// Records Published from the committed projection's recipient snapshot.
    /// No request kind or final outbox scan participates in this derivation.
    pub(super) fn record_published_projection(
        &self,
        projection: &OutboxRow,
        impact: &mut DispatchImpactAccumulator,
    ) -> Result<(), StateError> {
        let OutboxRow::Produced(batch) = projection else {
            return Ok(());
        };
        let recipients: BTreeSet<_> = batch
            .ordered_records()
            .iter()
            .flat_map(|record| record.recipients().iter().copied())
            .collect();
        if recipients.is_empty() {
            return Ok(());
        }
        let mut targets = BTreeSet::new();
        for participant_id in recipients {
            let target = self.dispatch_target(participant_id).ok_or_else(|| {
                StateError::invariant(format!(
                    "Produced projection recipient {participant_id} has no exact poststate binding"
                ))
            })?;
            targets.insert(target);
        }
        impact.stage(DispatchEffect::Published, targets);
        Ok(())
    }

    /// Records a binding transition, retaining the truthful effect when the
    /// affected participant has no current poststate binding.
    pub(super) fn record_binding_changed(
        &self,
        participant_id: ParticipantId,
        impact: &mut DispatchImpactAccumulator,
    ) {
        impact.stage(
            DispatchEffect::BindingChanged,
            self.dispatch_target(participant_id),
        );
    }

    /// Records one acknowledgement against its exact current binding.
    pub(super) fn record_acknowledged(
        &self,
        participant_id: ParticipantId,
        impact: &mut DispatchImpactAccumulator,
    ) {
        impact.stage(
            DispatchEffect::Acknowledged,
            self.dispatch_target(participant_id),
        );
    }

    /// Records a coupled episode/owner transition for all exact bindings whose
    /// dispatch verdict is recomputed from the installed poststate.
    pub(super) fn record_episode_changed(&self, impact: &mut DispatchImpactAccumulator) {
        impact.stage(DispatchEffect::EpisodeChanged, self.all_dispatch_targets());
    }

    /// Records permanent retirement only after a committed Left discharge.
    pub(super) fn record_retired(&self, impact: &mut DispatchImpactAccumulator) {
        impact.stage(DispatchEffect::Retired, self.all_dispatch_targets());
    }
}
