//! Exact post-commit dispatch-impact derivation under one conversation owner.

use std::collections::BTreeSet;

use liminal::durability::bridge::block_on;
use liminal_protocol::lifecycle::BindingState;
use liminal_protocol::wire::ParticipantId;

use crate::server::participant::dispatch_impact::{
    DispatchEffect, DispatchImpactAccumulator, DispatchTarget,
};

use super::log::StoredOperation;
use super::outbox::ConversationOutboxError;
use super::outbox_log::OutboxRow;
use super::outbox_projection::{ReplayedProjectionFacts, project_committed_source};
use super::state::{ConversationAuthority, DurableAppend, StateError};

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

    /// Reprojects one exact installed durable source, records its Published
    /// recipients, and COMPLETES the source in place. The exhaustive projection
    /// owns the seven source kinds.
    ///
    /// Board #60 §3c. Completing in place means writing the source's Unit 2
    /// extension row and applying it to the live outbox owner here, under the
    /// conversation lock, instead of leaving a from-zero replay to discover the
    /// row was missing and "repair" it. The two writers agree by construction:
    /// this is the SAME `project_committed_source` output the replay would
    /// compute, appended at the SAME `next_extension_sequence` the replay's
    /// repair branch would use, applied through the SAME `apply_row` inside the
    /// SAME observer-progress visit bracket
    /// (`outbox_replay.rs::ExtensionMerge::apply_boundary`, the repair arm).
    pub(super) fn record_produced_source(
        &mut self,
        source_log_sequence: u64,
        source: &StoredOperation,
        facts: ReplayedProjectionFacts,
        appender: &dyn DurableAppend,
        impact: &mut DispatchImpactAccumulator,
    ) -> Result<(), StateError> {
        let projection = project_committed_source(self, source_log_sequence, source, facts)?
            .ok_or_else(|| {
                StateError::invariant("committed Produced source lost its projection")
            })?;
        // ORDER IS LOAD-BEARING: complete the source durably BEFORE staging its
        // Published effect. A failed extension write must leave NOTHING staged,
        // or the handler's error path — which reconciles whenever staged work
        // exists — republishes a source whose durable completion just failed.
        // The pin is
        // `tests_outbox_replay::postcommit_outbox_failure_is_repaired_not_rolled_back`.
        self.complete_source_in_place(projection.clone(), appender)?;
        self.record_published_projection(&projection, impact)
    }

    /// Writes one committed source's Unit 2 extension row live, or takes the
    /// debt that keeps the from-zero replay as its writer.
    ///
    /// The durable append happens BEFORE the owner is mutated, so a failed
    /// append leaves the outbox owner untouched and the extension stream short
    /// — the exact crash shape the replay's repair branch already answers.
    fn complete_source_in_place(
        &mut self,
        projection: OutboxRow,
        appender: &dyn DurableAppend,
    ) -> Result<(), StateError> {
        let (Some(extension_log), Some(outbox)) = (appender.extension_log(), self.outbox.as_ref())
        else {
            // No in-place writer. The owed row stays owed, the operation
            // carries debt, and the handler's from-zero replay repairs it.
            return Ok(());
        };
        if appender.owed_extension_rows() != 1 {
            // An EARLIER source in this same operation owes a row it will not
            // write. Writing this one now would place a later physical row
            // ahead of the missing one, which the replay's repair branch —
            // append-at-confirmed-EOF only — cannot then fix: it refuses the
            // conversation outright. Defer the whole operation to the replay,
            // which writes both in base-log order.
            return Ok(());
        }
        let extension_sequence = outbox.next_extension_sequence();
        block_on(extension_log.append(&projection, extension_sequence))
            .map_err(|error| StateError::invariant(format!("Unit 2 extension bridge: {error}")))?
            .map_err(ConversationOutboxError::from)?;
        self.begin_observer_progress_source()?;
        self.outbox
            .as_mut()
            .ok_or_else(|| StateError::invariant("committed source outbox owner disappeared"))?
            .apply_row(extension_sequence, projection)?;
        self.end_observer_progress_source()?;
        appender.discharge_owed_extension_row();
        Ok(())
    }

    /// Records one committed Attached source and both transitions it implies.
    ///
    /// Bundled so the attach arm stays inside its own line budget while the
    /// order — complete the source, then the binding, then the episode —
    /// remains the one the replay produces.
    pub(super) fn record_attached_source(
        &mut self,
        record: AttachedSourceRecord<'_>,
        appender: &dyn DurableAppend,
        impact: &mut DispatchImpactAccumulator,
    ) -> Result<(), StateError> {
        let AttachedSourceRecord {
            source_log_sequence,
            source,
            projection_facts,
            participant_id,
        } = record;
        self.record_produced_source(
            source_log_sequence,
            source,
            projection_facts,
            appender,
            impact,
        )?;
        self.record_binding_changed(participant_id, impact);
        self.record_episode_changed(impact);
        Ok(())
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
            match self.dispatch_target(participant_id) {
                Some(target) => {
                    targets.insert(target);
                }
                None => {
                    // No live dispatch target. If the slot is still PRESENT it is a
                    // resumable `Detached` recipient: park the obligation — the
                    // durable install from the outbox append persists and replays on
                    // `CredentialAttach`, and a `Detached` binding is owed no live
                    // tell (it has no connection to notify). If the slot is ABSENT
                    // the recipient is a cleanly-departed peer that the snapshot must
                    // never name, so the hard invariant stands — departed peers mint
                    // nothing.
                    if !self.slots.contains_key(&participant_id) {
                        return Err(StateError::invariant(format!(
                            "Produced projection recipient {participant_id} has no exact poststate binding"
                        )));
                    }
                }
            }
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

/// One committed Attached source and the participant its transitions name.
pub(super) struct AttachedSourceRecord<'a> {
    pub(super) source_log_sequence: u64,
    pub(super) source: &'a StoredOperation,
    pub(super) projection_facts: ReplayedProjectionFacts,
    pub(super) participant_id: ParticipantId,
}
