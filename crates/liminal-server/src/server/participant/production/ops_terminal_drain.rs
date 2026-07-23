//! Candidate-lane terminal drain (PARTICIPANT-CONTRACT R-A2).
//!
//! When record admission's `DrainFirst` selects a pending binding terminal
//! as the earliest immutable candidate — a crash-restored
//! `PendingFinalization` residence of either flavor — the server drains that
//! terminal as one durable candidate transaction instead of faulting:
//! terminal-record append, retention transition, candidate deletion, and the
//! flavor's own slot settlement, with fate completion wired through the same
//! `record_terminal_impact` path connection-fate finalizers use. The drain
//! is the sibling terminal path beside `persist_next_marker`; the marker
//! lane itself stays structurally marker-only.
//!
//! The two flavors settle their slots OPPOSITELY (PENDING-DRAIN-EMITTER
//! §3A.3, S-16):
//!
//! - **Died** — R-A2 binding-slot release is identity erasure: no resume
//!   claim survives a death with no retirement authority, so the slot and
//!   its enrollment-token mapping are removed and later probes answer
//!   `ParticipantUnknown`.
//! - **Detached** — faithful detach finalization, never erasure: the
//!   contract keeps Detached membership durable and exact-secret resumable
//!   (`PARTICIPANT-CONTRACT.md` "membership and cursor remain durable";
//!   `client.rs` Detached + exact-secret attach → Bound), so "release" is
//!   the slot's transition OUT of the pending residence into committed
//!   `BindingState::Detached` — slot and enrollment token PRESERVED, and
//!   the victim thereafter counts as a parked `produced` recipient.

use liminal_protocol::lifecycle::{
    BindingState, CommittedBindingTerminal, DetachCell, ImmutableSequenceCandidate,
    LiveFrontierOwner, ObserverProgressProjection, PendingDetachedFinalization,
    PendingDiedFinalization, PendingFinalization, RetainedRecordCharge, complete_pending_detach,
};
use liminal_protocol::wire::DetachedCause;

use crate::server::participant::dispatch_impact::DispatchImpactAccumulator;

use super::binding_fate_completion::stored_died_cause;
use super::connection_fate::record_terminal_impact;
use super::fate_occurrence::PendingFinalizerRoute;
use super::frontier::terminal_charge;
use super::log::{
    StoredBindingEpoch, StoredDetached, StoredDetachedCause, StoredDetachedSource, StoredDied,
    StoredDrainedTerminal, StoredFinalizerPresentation, StoredOperation,
    StoredOrdinaryTerminalSource, StoredPendingDiedFinalizer, StoredTerminalDisposition,
};
use super::observer_progress::ObserverProgressSourceMetadata;
use super::outbox_projection::capture_projection_prestate;
use super::state::{ConversationAuthority, DurableAppend, StateError};

/// Cause-partitioned residence flavor selected by the drain candidate.
#[derive(Clone, Copy)]
enum DrainFlavor {
    Died(PendingDiedFinalization),
    Detached(PendingDetachedFinalization),
}

/// Validated drain authority: the exact pending residence selected by the
/// earliest binding-terminal candidate, in either terminal flavor.
#[derive(Clone, Copy)]
struct DrainAuthority {
    flavor: DrainFlavor,
    pending: PendingFinalization,
    route: PendingFinalizerRoute,
}

impl ConversationAuthority {
    /// Routes one `DrainFirst` candidate to its lane: markers keep the
    /// marker-only drain (whose binding-terminal invariant stays as the
    /// mis-selection backstop); a binding terminal takes the sibling
    /// terminal drain.
    pub(super) fn persist_drain_first(
        &mut self,
        candidate: ImmutableSequenceCandidate,
        owner: LiveFrontierOwner,
        appender: &dyn DurableAppend,
        impact: &mut DispatchImpactAccumulator,
    ) -> Result<(), StateError> {
        match candidate {
            ImmutableSequenceCandidate::Marker(_) => {
                self.persist_next_marker(candidate, owner, appender, impact)
            }
            ImmutableSequenceCandidate::BindingTerminal { .. } => {
                self.persist_terminal_drain(candidate, owner, appender, impact)
            }
        }
    }

    /// Drains one pending binding-terminal candidate as its own durable
    /// candidate transaction, then returns for the caller's re-entry exactly
    /// as the marker `DrainFirst` continuation does.
    fn persist_terminal_drain(
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
        let participant_id = authority.pending.participant_id();
        let delivery_seq = candidate_delivery_seq(&candidate)?;
        let source_log_sequence = self.next_log_sequence;
        let source = drain_row_operation(authority, delivery_seq)?;
        let (owner, projection, completes_ordinary) =
            self.drain_validated_candidate(authority, delivery_seq, source_log_sequence, owner)?;
        let projection_facts = capture_projection_prestate(self, &source);
        appender.append(&source, source_log_sequence)?;
        self.install_frontier(owner)?;
        self.settle_drained_binding_slot(authority.flavor, delivery_seq)?;
        self.observe_replayed_position(
            authority.pending.admission_order().transaction_order(),
            delivery_seq,
        )?;
        self.advance_log_head()?;
        self.record_drain_presentation(
            authority,
            source_log_sequence,
            participant_id,
            delivery_seq,
            projection,
        )?;
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

    /// Cold-replays one durable Died row by its shape: a drain row replays
    /// the candidate-lane terminal drain, a source row replays connection
    /// fate.
    pub(super) fn replay_died_row(
        &mut self,
        row: &StoredDied,
        sequence: u64,
    ) -> Result<(), StateError> {
        if row.drained.is_some() {
            self.replay_terminal_drain(row, sequence)
        } else {
            self.replay_died_source(row, sequence)
        }
    }

    /// Cold-replays one durable candidate-lane terminal drain row through the
    /// same protocol drain and validates every persisted allocation.
    fn replay_terminal_drain(&mut self, row: &StoredDied, sequence: u64) -> Result<(), StateError> {
        let terminal_seq = validate_drain_row_shape(row)?;
        let (authority, owner) = self.validate_drain_replay_candidate(sequence)?;
        let DrainFlavor::Died(died) = authority.flavor else {
            self.install_frontier(owner)?;
            return Err(StateError::invariant(
                "durable Died drain row does not finalize a pending Died residence",
            ));
        };
        if let Err(error) =
            validate_drain_row_authority(row, terminal_seq, authority, died, self.next_seq)
        {
            self.install_frontier(owner)?;
            return Err(error);
        }
        self.replay_validated_drain(authority, terminal_seq, sequence, owner, row.terminal_order)
    }

    /// Cold-replays one durable Detached-flavor candidate-lane terminal drain
    /// row (`StoredDetachedSource::Drained`) through the same protocol drain,
    /// settling the slot at committed `Detached` exactly as the live drain
    /// does.
    pub(super) fn replay_detached_drain_row(
        &mut self,
        row: &StoredDetached,
        sequence: u64,
    ) -> Result<(), StateError> {
        let (terminal_seq, drained) = validate_detached_drain_row_shape(row)?;
        let (authority, owner) = self.validate_drain_replay_candidate(sequence)?;
        let DrainFlavor::Detached(detached) = authority.flavor else {
            self.install_frontier(owner)?;
            return Err(StateError::invariant(
                "durable Detached drain row does not finalize a pending Detached residence",
            ));
        };
        if let Err(error) = validate_detached_drain_row_authority(
            row,
            terminal_seq,
            drained,
            authority,
            detached,
            self.next_seq,
        ) {
            self.install_frontier(owner)?;
            return Err(error);
        }
        self.replay_validated_drain(authority, terminal_seq, sequence, owner, row.terminal_order)
    }

    /// Shared replay entry: checks the durable log head, takes the frontier,
    /// and validates the first immutable candidate as drain authority.
    fn validate_drain_replay_candidate(
        &mut self,
        sequence: u64,
    ) -> Result<(DrainAuthority, LiveFrontierOwner), StateError> {
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
        match self.validate_drain_candidate(candidate) {
            Ok(authority) => Ok((authority, owner)),
            Err(error) => {
                self.install_frontier(owner)?;
                Err(error)
            }
        }
    }

    /// Shared replay tail: commits the validated candidate through the
    /// protocol drain, settles the slot per flavor, and re-records the
    /// drain's observer presentation.
    fn replay_validated_drain(
        &mut self,
        authority: DrainAuthority,
        terminal_seq: u64,
        sequence: u64,
        owner: LiveFrontierOwner,
        terminal_order: u64,
    ) -> Result<(), StateError> {
        let participant_id = authority.pending.participant_id();
        let (owner, projection, _completes_ordinary) =
            self.drain_validated_candidate(authority, terminal_seq, sequence, owner)?;
        self.install_frontier(owner)?;
        self.settle_drained_binding_slot(authority.flavor, terminal_seq)?;
        self.observe_replayed_position(terminal_order, terminal_seq)?;
        self.advance_log_head()?;
        self.record_drain_presentation(
            authority,
            sequence,
            participant_id,
            terminal_seq,
            projection,
        )
    }

    /// Measures the open Ordinary intent (Died flavor only — a pending
    /// Detached owns no specific fate), then commits the candidate through
    /// the protocol drain: terminal-record retention, candidate deletion, and
    /// the coupled observer projection.
    fn drain_validated_candidate(
        &mut self,
        authority: DrainAuthority,
        delivery_seq: u64,
        finalizer_source_sequence: u64,
        owner: LiveFrontierOwner,
    ) -> Result<(LiveFrontierOwner, ObserverProgressProjection, bool), StateError> {
        let DrainAuthority {
            flavor,
            pending,
            route,
        } = authority;
        let participant_id = pending.participant_id();
        let (owner, completes_ordinary) = match flavor {
            DrainFlavor::Died(died) => self.prepare_pending_died_finalizer(
                participant_id,
                route.pending_source_sequence,
                died.commit(delivery_seq),
                StoredOrdinaryTerminalSource::PendingDiedFinalized {
                    died_source_sequence: route.pending_source_sequence,
                    finalizer: StoredPendingDiedFinalizer::Drained {
                        source_sequence: finalizer_source_sequence,
                    },
                },
                owner,
            )?,
            DrainFlavor::Detached(_) => (owner, false),
        };
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
        Ok((owner, projection, completes_ordinary))
    }

    /// Records the drain's observer-progress presentation exactly as a
    /// committed source of its flavor does — unless a Recovered row already
    /// reserved the presentation, in which case the drain is non-presenting,
    /// mirroring Leave.
    fn record_drain_presentation(
        &mut self,
        authority: DrainAuthority,
        source_sequence: u64,
        participant_id: u64,
        terminal_seq: u64,
        projection: ObserverProgressProjection,
    ) -> Result<(), StateError> {
        if matches!(
            authority.route.presentation,
            StoredFinalizerPresentation::ConsumeRecoveredReservation { .. }
        ) {
            return Ok(());
        }
        let metadata = match authority.flavor {
            DrainFlavor::Died(_) => ObserverProgressSourceMetadata::died(
                source_sequence,
                self.conversation_id,
                participant_id,
                terminal_seq,
            ),
            DrainFlavor::Detached(_) => ObserverProgressSourceMetadata::detached(
                source_sequence,
                self.conversation_id,
                participant_id,
                terminal_seq,
            ),
        };
        self.record_observer_progress_projection(projection, metadata)
    }

    /// Settles the drained slot per the flavor's ruled R-A2 reading: Died
    /// releases (erases) the identity; Detached commits the residence in
    /// place, preserving slot and enrollment token.
    fn settle_drained_binding_slot(
        &mut self,
        flavor: DrainFlavor,
        delivery_seq: u64,
    ) -> Result<(), StateError> {
        match flavor {
            DrainFlavor::Died(died) => {
                self.release_drained_binding_slot(died.participant_id());
                Ok(())
            }
            DrainFlavor::Detached(detached) => {
                self.commit_drained_detached_slot(detached, delivery_seq)
            }
        }
    }

    /// Releases the drained binding slot (R-A2 binding-slot release,
    /// `slots.remove` semantics per the Leave precedent) together with its
    /// enrollment-token mapping. Died flavor ONLY.
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

    /// Commits the drained pending Detached residence IN PLACE — the S-16
    /// faithful detach finalization. The slot and its enrollment-token
    /// mapping are preserved (the participant keeps its standing exact-secret
    /// resume claim), the member's terminal history gains the committed
    /// Detached terminal, and the binding settles at
    /// `BindingState::Detached`. A blocked explicit detach commits through
    /// the protocol's own `complete_pending_detach`, retaining the replayable
    /// committed detach cell; a shutdown-minted residence has no pending
    /// detach cell and commits its member terminal directly.
    fn commit_drained_detached_slot(
        &mut self,
        detached: PendingDetachedFinalization,
        delivery_seq: u64,
    ) -> Result<(), StateError> {
        let participant_id = detached.participant_id();
        let (slot_key, mut slot) = self.slots.remove_entry(&participant_id).ok_or_else(|| {
            StateError::invariant("drained Detached residence lost its participant slot")
        })?;
        let member = slot.member;
        let binding = slot.binding;
        let (member, binding, cell) = match slot.cell {
            DetachCell::Pending(pending_cell) => {
                let transition =
                    complete_pending_detach(member, binding, pending_cell, delivery_seq).map_err(
                        |error| {
                            StateError::invariant(format!(
                                "drained pending Detached completion failed: {error:?}"
                            ))
                        },
                    )?;
                let (member, _terminal, binding, committed_cell, _outcome) =
                    transition.into_parts();
                (member, binding, DetachCell::Committed(committed_cell))
            }
            settled_cell => {
                let terminal = detached.commit(delivery_seq);
                let member = member
                    .with_committed_terminal(CommittedBindingTerminal::Detached(terminal))
                    .map_err(|error| {
                        StateError::invariant(format!(
                            "drained Detached terminal history refused: {error:?}"
                        ))
                    })?;
                (member, BindingState::Detached, settled_cell)
            }
        };
        slot.member = member;
        slot.binding = binding;
        slot.cell = cell;
        self.slots.insert(slot_key, slot);
        Ok(())
    }

    /// Validates the drained candidate against the exact pending residence it
    /// must finalize, consuming the finalizer presentation. Both terminal
    /// flavors are drainable; each carries its own settlement semantics.
    ///
    /// A binding-terminal candidate whose slot does not rest in
    /// `PendingFinalization` is genuine mis-selection and refuses here; the
    /// marker lane's own invariant stays as the backstop for terminals
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
        let flavor = match pending {
            PendingFinalization::Died(died) => DrainFlavor::Died(died),
            PendingFinalization::Detached(detached) => DrainFlavor::Detached(detached),
        };
        if pending.binding_epoch() != terminal_owner.binding_epoch
            || pending.admission_order() != admission_order
            || delivery_seq != self.next_seq
        {
            return Err(StateError::invariant(
                "terminal drain candidate disagrees with its pending residence",
            ));
        }
        let route = self
            .select_leave_finalizer(participant_id)?
            .ok_or_else(|| {
                StateError::invariant("pending terminal drain lost its finalizer route")
            })?;
        Ok(DrainAuthority {
            flavor,
            pending,
            route,
        })
    }
}

/// Builds the flavor's exact durable drain row: a drained `Died` row for the
/// Died residence, a `Drained`-sourced `Detached` row (cause preserved in the
/// `StoredDetachedCause` domain) for the Detached residence.
fn drain_row_operation(
    authority: DrainAuthority,
    delivery_seq: u64,
) -> Result<StoredOperation, StateError> {
    let pending = authority.pending;
    let drained = StoredDrainedTerminal {
        pending_source_sequence: authority.route.pending_source_sequence,
        finalizer_presentation: authority.route.presentation,
    };
    match authority.flavor {
        DrainFlavor::Died(died) => Ok(StoredOperation::Died {
            row: StoredDied {
                participant_id: pending.participant_id(),
                binding_epoch: StoredBindingEpoch::from(pending.binding_epoch()),
                cause: stored_died_cause(died.cause()),
                terminal_order: pending.admission_order().transaction_order(),
                disposition: StoredTerminalDisposition::Committed {
                    terminal_seq: delivery_seq,
                },
                connection_intent_sequence: None,
                specific_fate_intent: None,
                drained: Some(drained),
            },
        }),
        DrainFlavor::Detached(detached) => Ok(StoredOperation::Detached {
            row: StoredDetached {
                participant_id: pending.participant_id(),
                binding_epoch: StoredBindingEpoch::from(pending.binding_epoch()),
                cause: stored_detached_cause(detached.cause())?,
                terminal_order: pending.admission_order().transaction_order(),
                disposition: StoredTerminalDisposition::Committed {
                    terminal_seq: delivery_seq,
                },
                source: StoredDetachedSource::Drained { drained },
            },
        }),
    }
}

/// Maps the protocol's restricted Detached cause into the durable domain. A
/// pending residence can only carry `CleanDeregister` (blocked explicit
/// detach / clean disconnect) or `ServerShutdown`; `Superseded` never pends —
/// supersession commits inside its fenced attach.
fn stored_detached_cause(cause: DetachedCause) -> Result<StoredDetachedCause, StateError> {
    match cause {
        DetachedCause::CleanDeregister => Ok(StoredDetachedCause::CleanDeregister),
        DetachedCause::ServerShutdown => Ok(StoredDetachedCause::ServerShutdown),
        DetachedCause::Superseded => Err(StateError::invariant(
            "pending Detached residence carries a Superseded cause",
        )),
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

/// Validates the durable drain row's own shape and returns its committed
/// terminal sequence.
fn validate_drain_row_shape(row: &StoredDied) -> Result<u64, StateError> {
    if row.drained.is_none() {
        return Err(StateError::invariant(
            "terminal drain replay received a source Died row",
        ));
    }
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
    Ok(terminal_seq)
}

/// Validates the durable Detached drain row's own shape and returns its
/// committed terminal sequence with the stored drain provenance.
fn validate_detached_drain_row_shape(
    row: &StoredDetached,
) -> Result<(u64, StoredDrainedTerminal), StateError> {
    let StoredDetachedSource::Drained { drained } = &row.source else {
        return Err(StateError::invariant(
            "terminal drain replay received a source Detached row",
        ));
    };
    let StoredTerminalDisposition::Committed { terminal_seq } = row.disposition else {
        return Err(StateError::invariant(
            "durable terminal drain row is not committed",
        ));
    };
    Ok((terminal_seq, *drained))
}

/// Validates the durable drain row against the live-recomputed residence and
/// its consumed finalizer route.
fn validate_drain_row_authority(
    row: &StoredDied,
    terminal_seq: u64,
    authority: DrainAuthority,
    died: PendingDiedFinalization,
    next_seq: u64,
) -> Result<(), StateError> {
    let Some(drain) = row.drained else {
        return Err(StateError::invariant(
            "terminal drain replay received a source Died row",
        ));
    };
    if row.participant_id != authority.pending.participant_id()
        || row.binding_epoch.to_epoch()? != authority.pending.binding_epoch()
        || row.terminal_order != authority.pending.admission_order().transaction_order()
        || row.cause != stored_died_cause(died.cause())
        || terminal_seq != next_seq
    {
        return Err(StateError::invariant(
            "durable terminal drain row disagrees with its pending residence",
        ));
    }
    if authority.route.pending_source_sequence != drain.pending_source_sequence
        || authority.route.presentation != drain.finalizer_presentation
    {
        return Err(StateError::invariant(
            "durable terminal drain finalizer route drifted",
        ));
    }
    Ok(())
}

/// Validates the durable Detached drain row against the live-recomputed
/// residence and its consumed finalizer route.
fn validate_detached_drain_row_authority(
    row: &StoredDetached,
    terminal_seq: u64,
    drained: StoredDrainedTerminal,
    authority: DrainAuthority,
    detached: PendingDetachedFinalization,
    next_seq: u64,
) -> Result<(), StateError> {
    if row.participant_id != authority.pending.participant_id()
        || row.binding_epoch.to_epoch()? != authority.pending.binding_epoch()
        || row.terminal_order != authority.pending.admission_order().transaction_order()
        || row.cause != stored_detached_cause(detached.cause())?
        || terminal_seq != next_seq
    {
        return Err(StateError::invariant(
            "durable terminal drain row disagrees with its pending residence",
        ));
    }
    if authority.route.pending_source_sequence != drained.pending_source_sequence
        || authority.route.presentation != drained.finalizer_presentation
    {
        return Err(StateError::invariant(
            "durable terminal drain finalizer route drifted",
        ));
    }
    Ok(())
}
