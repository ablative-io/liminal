//! Candidate-lane terminal drain (PARTICIPANT-CONTRACT R-A2).
//!
//! When record admission's `DrainFirst` selects a pending binding terminal
//! as the earliest immutable candidate — the crash-restored
//! `PendingFinalization(Died)` residence — the server drains that terminal
//! as one durable candidate transaction instead of faulting: terminal-record
//! append, retention transition, candidate deletion, and binding-slot
//! release, with fate completion wired through the same
//! `record_terminal_impact` path connection-fate finalizers use. The drain
//! is the sibling terminal path beside `persist_next_marker`; the marker
//! lane itself stays structurally marker-only.

use liminal_protocol::lifecycle::{
    BindingState, ImmutableSequenceCandidate, LiveFrontierOwner, PendingDiedFinalization,
    PendingFinalization, RetainedRecordCharge,
};

use crate::server::participant::dispatch_impact::DispatchImpactAccumulator;

use super::binding_fate_completion::stored_died_cause;
use super::connection_fate::record_terminal_impact;
use super::fate_occurrence::PendingFinalizerRoute;
use super::frontier::terminal_charge;
use super::log::{
    StoredBindingEpoch, StoredDied, StoredDrainedTerminal, StoredFinalizerPresentation,
    StoredOperation, StoredOrdinaryTerminalSource, StoredPendingDiedFinalizer,
    StoredTerminalDisposition,
};
use super::observer_progress::ObserverProgressSourceMetadata;
use super::outbox_projection::capture_projection_prestate;
use super::state::{ConversationAuthority, DurableAppend, StateError};

/// Validated drain authority: the exact pending Died residence selected by
/// the earliest binding-terminal candidate.
struct DrainAuthority {
    died: PendingDiedFinalization,
    pending: PendingFinalization,
    route: PendingFinalizerRoute,
}

impl ConversationAuthority {
    /// Drains one pending binding-terminal candidate as its own durable
    /// candidate transaction, then returns for the caller's re-entry exactly
    /// as the marker `DrainFirst` continuation does.
    pub(super) fn persist_terminal_drain(
        &mut self,
        candidate: ImmutableSequenceCandidate,
        owner: LiveFrontierOwner,
        appender: &dyn DurableAppend,
        impact: &mut DispatchImpactAccumulator,
    ) -> Result<(), StateError> {
        let authority = match self.validate_drain_candidate(candidate) {
            Ok(authority) => authority,
            Err(error) => {
                self.install_frontier(owner)?;
                return Err(error);
            }
        };
        let DrainAuthority {
            died,
            pending,
            route,
        } = authority;
        let participant_id = pending.participant_id();
        let delivery_seq = candidate_delivery_seq(&candidate)?;
        let source_log_sequence = self.next_log_sequence;
        let terminal = died.commit(delivery_seq);
        let (owner, completes_ordinary) = self.prepare_pending_died_finalizer(
            participant_id,
            route.pending_source_sequence,
            terminal,
            StoredOrdinaryTerminalSource::PendingDiedFinalized {
                died_source_sequence: route.pending_source_sequence,
                finalizer: StoredPendingDiedFinalizer::Drained {
                    source_sequence: source_log_sequence,
                },
            },
            owner,
        )?;
        let charge = match terminal_charge(
            pending.conversation_id(),
            participant_id,
            pending.binding_epoch(),
            pending.admission_order().transaction_order(),
            delivery_seq,
        ) {
            Ok(charge) => charge,
            Err(error) => {
                self.install_frontier(owner)?;
                return Err(error);
            }
        };
        let terminal_row_charge =
            RetainedRecordCharge::new(delivery_seq, pending.admission_order(), charge);
        let drained = match owner.drain_pending_terminal(pending, terminal_row_charge) {
            Ok(drained) => drained,
            Err(refused) => {
                let error = refused.error();
                self.install_frontier(refused.into_owner())?;
                return Err(StateError::invariant(format!(
                    "candidate-lane terminal drain refused: {error:?}"
                )));
            }
        };
        let (owner, projection) = drained.into_parts();
        let row = StoredDied {
            participant_id,
            binding_epoch: StoredBindingEpoch::from(pending.binding_epoch()),
            cause: stored_died_cause(died.cause()),
            terminal_order: pending.admission_order().transaction_order(),
            disposition: StoredTerminalDisposition::Committed {
                terminal_seq: delivery_seq,
            },
            connection_intent_sequence: None,
            specific_fate_intent: None,
            drained: Some(StoredDrainedTerminal {
                pending_source_sequence: route.pending_source_sequence,
                finalizer_presentation: route.presentation,
            }),
        };
        let source = StoredOperation::Died { row };
        let projection_facts = capture_projection_prestate(self, &source);
        appender.append(&source, source_log_sequence)?;
        self.install_frontier(owner)?;
        self.release_drained_binding_slot(participant_id);
        self.observe_replayed_position(pending.admission_order().transaction_order(), delivery_seq)?;
        self.advance_log_head()?;
        if !matches!(
            route.presentation,
            StoredFinalizerPresentation::ConsumeRecoveredReservation { .. }
        ) {
            let metadata = ObserverProgressSourceMetadata::died(
                source_log_sequence,
                self.conversation_id,
                participant_id,
                delivery_seq,
            );
            self.record_observer_progress_projection(projection, metadata)?;
        }
        record_terminal_impact(
            self,
            source_log_sequence,
            &source,
            projection_facts,
            participant_id,
            impact,
        )?;
        if completes_ordinary {
            self.complete_prepared_ordinary_finalizer(participant_id, appender)?;
            self.record_episode_changed(impact);
        }
        Ok(())
    }

    /// Cold-replays one durable candidate-lane terminal drain row through the
    /// same protocol drain and validates every persisted allocation.
    pub(super) fn replay_terminal_drain(
        &mut self,
        row: &StoredDied,
        sequence: u64,
    ) -> Result<(), StateError> {
        let Some(drain) = row.drained else {
            return Err(StateError::invariant(
                "terminal drain replay received a source Died row",
            ));
        };
        let StoredTerminalDisposition::Committed { terminal_seq } = row.disposition else {
            return Err(StateError::invariant(
                "durable terminal drain row is not committed",
            ));
        };
        if row.specific_fate_intent.is_some() || row.connection_intent_sequence.is_some() {
            return Err(StateError::invariant(
                "durable terminal drain row carries source-row authority",
            ));
        }
        if self.next_log_sequence != sequence {
            return Err(StateError::invariant(
                "terminal drain replay log head disagrees with durable sequence",
            ));
        }
        let owner = self.take_frontier()?;
        let candidate = owner
            .frontiers()
            .sequence()
            .immutable_candidates()
            .first()
            .copied()
            .ok_or_else(|| StateError::invariant("durable terminal drain has no candidate"))?;
        let authority = match self.validate_drain_candidate(candidate) {
            Ok(authority) => authority,
            Err(error) => {
                self.install_frontier(owner)?;
                return Err(error);
            }
        };
        let DrainAuthority {
            died,
            pending,
            route,
        } = authority;
        if row.participant_id != pending.participant_id()
            || row.binding_epoch.to_epoch()? != pending.binding_epoch()
            || row.terminal_order != pending.admission_order().transaction_order()
            || row.cause != stored_died_cause(died.cause())
            || terminal_seq != self.next_seq
        {
            return Err(StateError::invariant(
                "durable terminal drain row disagrees with its pending residence",
            ));
        }
        if route.pending_source_sequence != drain.pending_source_sequence
            || route.presentation != drain.finalizer_presentation
        {
            return Err(StateError::invariant(
                "durable terminal drain finalizer route drifted",
            ));
        }
        let participant_id = pending.participant_id();
        let terminal = died.commit(terminal_seq);
        let (owner, _completes_ordinary) = self.prepare_pending_died_finalizer(
            participant_id,
            route.pending_source_sequence,
            terminal,
            StoredOrdinaryTerminalSource::PendingDiedFinalized {
                died_source_sequence: route.pending_source_sequence,
                finalizer: StoredPendingDiedFinalizer::Drained {
                    source_sequence: sequence,
                },
            },
            owner,
        )?;
        let charge = match terminal_charge(
            pending.conversation_id(),
            participant_id,
            pending.binding_epoch(),
            pending.admission_order().transaction_order(),
            terminal_seq,
        ) {
            Ok(charge) => charge,
            Err(error) => {
                self.install_frontier(owner)?;
                return Err(error);
            }
        };
        let terminal_row_charge =
            RetainedRecordCharge::new(terminal_seq, pending.admission_order(), charge);
        let drained = match owner.drain_pending_terminal(pending, terminal_row_charge) {
            Ok(drained) => drained,
            Err(refused) => {
                let error = refused.error();
                self.install_frontier(refused.into_owner())?;
                return Err(StateError::invariant(format!(
                    "durable terminal drain replay refused: {error:?}"
                )));
            }
        };
        let (owner, projection) = drained.into_parts();
        self.install_frontier(owner)?;
        self.release_drained_binding_slot(participant_id);
        self.observe_replayed_position(row.terminal_order, terminal_seq)?;
        self.advance_log_head()?;
        if !matches!(
            route.presentation,
            StoredFinalizerPresentation::ConsumeRecoveredReservation { .. }
        ) {
            let metadata = ObserverProgressSourceMetadata::died(
                sequence,
                self.conversation_id,
                participant_id,
                terminal_seq,
            );
            self.record_observer_progress_projection(projection, metadata)?;
        }
        Ok(())
    }

    /// Releases the drained binding slot (R-A2 binding-slot release,
    /// `slots.remove` semantics per the Leave precedent) together with its
    /// enrollment-token mapping.
    ///
    /// Leave pairs its removal with a protocol-minted tombstone; a drain has
    /// no Leave request and fabricating retirement authority is forbidden, so
    /// the drained identity is erased entirely. Its enrollment token no longer
    /// maps to anything (the stage-8 occupancy audit requires live-or-retired
    /// for every mapped token), later probes answer `ParticipantUnknown`, and
    /// a re-enrollment with the same token mints a fresh identity.
    fn release_drained_binding_slot(&mut self, participant_id: u64) {
        self.slots.remove(&participant_id);
        self.tokens.retain(|_, mapped| *mapped != participant_id);
    }

    /// Validates the drained candidate against the exact pending Died
    /// residence it must finalize, consuming the finalizer presentation.
    ///
    /// A binding-terminal candidate whose slot does not rest in
    /// `PendingFinalization(Died)` is genuine mis-selection and refuses here;
    /// the marker lane's own invariant stays as the backstop for terminals
    /// reaching marker work.
    fn validate_drain_candidate(
        &mut self,
        candidate: ImmutableSequenceCandidate,
    ) -> Result<DrainAuthority, StateError> {
        let ImmutableSequenceCandidate::BindingTerminal {
            delivery_seq,
            admission_order,
            owner: terminal_owner,
        } = candidate
        else {
            return Err(StateError::invariant(
                "terminal drain selected marker work instead of a binding terminal",
            ));
        };
        let participant_id = terminal_owner.participant_index;
        let Some(slot) = self.slots.get(&participant_id) else {
            return Err(StateError::invariant(
                "terminal drain candidate names an absent participant slot",
            ));
        };
        let BindingState::PendingFinalization(pending) = slot.binding else {
            return Err(StateError::invariant(
                "terminal drain candidate does not rest in PendingFinalization",
            ));
        };
        let PendingFinalization::Died(died) = pending else {
            return Err(StateError::invariant(
                "terminal drain candidate is not a pending Died residence",
            ));
        };
        if pending.binding_epoch() != terminal_owner.binding_epoch
            || pending.admission_order() != admission_order
            || delivery_seq != self.next_seq
        {
            return Err(StateError::invariant(
                "terminal drain candidate disagrees with its pending residence",
            ));
        }
        let route = self.select_leave_finalizer(participant_id)?.ok_or_else(|| {
            StateError::invariant("pending terminal drain lost its finalizer route")
        })?;
        Ok(DrainAuthority {
            died,
            pending,
            route,
        })
    }
}

fn candidate_delivery_seq(candidate: &ImmutableSequenceCandidate) -> Result<u64, StateError> {
    match candidate {
        ImmutableSequenceCandidate::BindingTerminal { delivery_seq, .. } => Ok(*delivery_seq),
        ImmutableSequenceCandidate::Marker(_) => Err(StateError::invariant(
            "terminal drain selected marker work instead of a binding terminal",
        )),
    }
}
