//! Participant operation schema preflight, migration, and cold replay.

use liminal_protocol::lifecycle::{BindingState, RecipientAckObligations};
use liminal_protocol::wire::ParticipantDelivery;

use crate::config::types::ParticipantConfig;

use super::barrier::ReceiptCapacityLimits;
use super::fenced_attach_terminal::ComposedTerminalValidation;
use super::log::{
    DecodedOperation, DecodedStoredOperation, OperationLog, OperationLogError,
    OperationSchemaPhase, StoredDetached, StoredDetachedCause, StoredDetachedSource,
    StoredOperation, StoredOperationV2, StoredTerminalDisposition, V2AttachedPrestate,
    migrate_v2_attached,
};
use super::ops_session::DetachReplayInputs;
use super::outbox::ConversationOutboxLimits;
use super::outbox_log::OutboxLog;
use super::outbox_projection::{capture_projection_prestate, project_committed_source};
#[cfg(test)]
use super::outbox_replay::AggregateExtensionMerge;
use super::outbox_replay::{ExtensionMerge, RestoreError};
use super::state::{ConversationAuthority, StateError};

impl ConversationAuthority {
    /// Cold-replays one conversation's complete durable log.
    pub(super) async fn replay(
        conversation_id: u64,
        log: &OperationLog,
        outbox_log: &OutboxLog,
        config: &ParticipantConfig,
        outbox_limits: ConversationOutboxLimits,
    ) -> Result<Self, RestoreError> {
        validate_operation_schema(log, config.identity_slots).await?;
        let mut authority =
            Self::empty(conversation_id, ReceiptCapacityLimits::from_config(config));
        let mut merge = ExtensionMerge::new(outbox_log, conversation_id, outbox_limits)?;
        merge.apply_boundary(&mut authority, 0, None).await?;
        let mut sequence = 0_u64;
        let mut phase = OperationSchemaPhase::V2Prefix;
        loop {
            let page = log
                .read_page(sequence, phase)
                .await
                .map_err(StateError::from)?;
            phase = page.next_phase;
            if page.rows.is_empty() {
                break;
            }
            let page_len = page.rows.len();
            for decoded in page.rows {
                let stored_sequence = decoded.sequence;
                if stored_sequence != sequence {
                    return Err(RestoreError::Semantic(StateError::Log(
                        super::log::OperationLogError::Sequence {
                            expected: sequence,
                            actual: stored_sequence,
                        },
                    )));
                }
                let operation = authority.decode_operation(decoded)?;
                if route_in_replay_dispatch(&operation) {
                    authority.route_fate_occurrence(&operation, stored_sequence)?;
                }
                let operation_for_projection = operation.clone();
                let ack_obligations = match &operation {
                    StoredOperation::ZeroDebtAck { request, .. }
                    | StoredOperation::NonzeroDebtAck { request, .. } => {
                        let acknowledged_through = authority
                            .slots
                            .get(&request.participant_id)
                            .map_or(0, |slot| slot.member.cursor());
                        Some(merge.recipient_ack_obligations(
                            request.participant_id,
                            acknowledged_through,
                        )?)
                    }
                    _ => None,
                };
                authority.begin_observer_progress_source()?;
                let mut facts = capture_projection_prestate(&authority, &operation_for_projection);
                facts.marker_delivery = authority.replay_operation(
                    operation,
                    stored_sequence,
                    config,
                    ack_obligations,
                    log.store(),
                )?;
                let expected = project_committed_source(
                    &authority,
                    stored_sequence,
                    &operation_for_projection,
                    facts,
                )?;
                authority.end_observer_progress_source()?;
                sequence = sequence
                    .checked_add(1)
                    .ok_or(StateError::AllocationExhausted {
                        domain: "log sequence",
                    })?;
                merge
                    .apply_boundary(&mut authority, sequence, expected.as_ref())
                    .await?;
            }
            if page_len < super::log::READ_BATCH_SIZE {
                break;
            }
        }
        authority.validate_replayed_seal()?;
        merge.finish(&mut authority, sequence)?;
        authority.reconcile_load_end_marker_anchors()?;
        Ok(authority)
    }

    /// F8B R-SEAL (§6.6). Closed carries its own clause: Closed => tokens
    /// empty AND frontier None AND marker set. The clause is a CONJUNCTION
    /// check, not a relaxation — a seal that retired the frontier but kept a
    /// token, or set the marker without retiring the frontier, is refused
    /// here. Bare tokens-empty-with-frontier-Some REMAINS the original refusal
    /// for every shape that is not the sealed one; the corruption tripwire is
    /// not widened.
    fn validate_replayed_seal(&self) -> Result<(), RestoreError> {
        if self.is_closed() {
            if !self.tokens.is_empty() || self.frontier().is_some() {
                return Err(RestoreError::Semantic(StateError::invariant(
                    "sealed conversation retained enrollment tokens or an executable frontier",
                )));
            }
        } else if self.tokens.is_empty() {
            if self.frontier().is_some() {
                return Err(RestoreError::Semantic(StateError::invariant(
                    "durably empty conversation rebuilt an executable frontier",
                )));
            }
        } else if self.frontier().is_none() {
            return Err(RestoreError::Semantic(StateError::invariant(
                "enrolled conversation replay completed without executable frontier ownership",
            )));
        }
        Ok(())
    }

    /// Load-end anchor reconcile: participant erasure or record retirement can
    /// strand a stored marker anchor with NO log row left that could retire it
    /// — replay alone rebuilds the wedge (the conversation-6 residue of the
    /// 2026-08-07 outage). Retires the orphaned excess, loudly; the
    /// derived-ahead direction stays untouched for the admission projection to
    /// fault on.
    fn reconcile_load_end_marker_anchors(&mut self) -> Result<(), StateError> {
        if self.frontier().is_some() {
            let mut frontier = self.take_frontier()?;
            let orphaned = frontier.reconcile_orphaned_marker_anchors();
            self.install_frontier(frontier)?;
            if orphaned > 0 {
                tracing::warn!(
                    conversation_id = self.conversation_id,
                    orphaned,
                    "reconciled orphaned marker anchors at load"
                );
            }
            self.reconcile_orphaned_marker_obligations()?;
        }
        Ok(())
    }

    /// The obligation half of the anchor reconcile (board `#76`): retires every
    /// durable marker obligation the reconciled frontier can no longer answer,
    /// and drops the volatile offer testimony that names it.
    ///
    /// An anchor and its obligation are ONE fact in two ledgers, so a
    /// retirement that touches only one of them leaves the conversation able to
    /// re-offer a marker whose delivery authority is gone — and the ack for
    /// that offer dies at `marker_progress`'s invariant, fail-closed, with the
    /// estate down. That invariant is CORRECT and is not touched: it is the
    /// right answer to a genuine divergence. This is where the divergence stops
    /// being manufactured.
    ///
    /// Run unconditionally alongside the anchor reconcile rather than only when
    /// that reconcile retired something. The two ledgers can also be separated
    /// by a retirement the anchor side accounted for correctly and the outbox
    /// never heard about (a departing member's marker record leaves the
    /// frontier with its owner while a co-recipient's push obligation stays
    /// live), and a coherence pass that only ran after a failure of its sibling
    /// would miss exactly those. On a store whose ledgers agree it retires
    /// nothing: every obligation whose marker record is still on the frontier
    /// is backed, and every obligation already behind its participant's
    /// selection cursor is unreachable and left alone.
    ///
    /// ⚠ This is a LOAD-side repair, exactly like its anchor sibling. A
    /// separation opened DURING a process's life is not healed until that
    /// process's next boot — board `#45`'s live window, which is its own
    /// measurement and is not closed here.
    pub(super) fn reconcile_orphaned_marker_obligations(&mut self) -> Result<(), StateError> {
        let Some(owner) = self.frontier() else {
            return Ok(());
        };
        let anchored: std::collections::BTreeSet<u64> = owner
            .frontiers()
            .retained_marker_records()
            .iter()
            .map(|record| record.delivery_seq)
            .collect();
        let active: std::collections::BTreeSet<u64> = owner
            .frontiers()
            .active_identities()
            .participants()
            .iter()
            .map(|participant| participant.participant_index())
            .collect();
        let Some(outbox) = self.outbox.as_mut() else {
            return Ok(());
        };
        let retired = outbox.retire_unbacked_marker_obligations(&|participant_id, delivery_seq| {
            anchored.contains(&delivery_seq) && active.contains(&participant_id)
        })?;
        if retired.is_empty() {
            return Ok(());
        }
        for pair in &retired {
            self.offered_markers.remove(pair);
        }
        tracing::warn!(
            conversation_id = self.conversation_id,
            retired = retired.len(),
            "retired marker obligations whose delivery authority the frontier no longer holds"
        );
        Ok(())
    }

    /// Frozen pre-W3 complete-vector replay used only by equivalence oracles.
    #[cfg(test)]
    pub(super) async fn replay_aggregate_reference(
        conversation_id: u64,
        log: &OperationLog,
        outbox_log: &OutboxLog,
        extension_rows: Vec<(u64, super::outbox_log::OutboxRow)>,
        config: &ParticipantConfig,
        outbox_limits: ConversationOutboxLimits,
    ) -> Result<Self, StateError> {
        let mut authority =
            Self::empty(conversation_id, ReceiptCapacityLimits::from_config(config));
        let mut merge = AggregateExtensionMerge::new(
            outbox_log,
            extension_rows,
            conversation_id,
            outbox_limits,
        )?;
        validate_operation_schema(log, config.identity_slots).await?;
        merge.apply_boundary(&mut authority, 0, None).await?;
        let mut sequence = 0_u64;
        let mut phase = OperationSchemaPhase::V2Prefix;
        loop {
            let page = log.read_page(sequence, phase).await?;
            phase = page.next_phase;
            if page.rows.is_empty() {
                break;
            }
            let page_len = page.rows.len();
            for decoded in page.rows {
                let stored_sequence = decoded.sequence;
                if stored_sequence != sequence {
                    return Err(StateError::Log(super::log::OperationLogError::Sequence {
                        expected: sequence,
                        actual: stored_sequence,
                    }));
                }
                let operation = authority.decode_operation(decoded)?;
                if route_in_replay_dispatch(&operation) {
                    authority.route_fate_occurrence(&operation, stored_sequence)?;
                }
                let operation_for_projection = operation.clone();
                let ack_obligations = match &operation {
                    StoredOperation::ZeroDebtAck { request, .. }
                    | StoredOperation::NonzeroDebtAck { request, .. } => {
                        let acknowledged_through = authority
                            .slots
                            .get(&request.participant_id)
                            .map_or(0, |slot| slot.member.cursor());
                        Some(merge.recipient_ack_obligations(
                            request.participant_id,
                            acknowledged_through,
                        )?)
                    }
                    _ => None,
                };
                authority.begin_observer_progress_source()?;
                let mut facts = capture_projection_prestate(&authority, &operation_for_projection);
                facts.marker_delivery = authority.replay_operation(
                    operation,
                    stored_sequence,
                    config,
                    ack_obligations,
                    log.store(),
                )?;
                let expected = project_committed_source(
                    &authority,
                    stored_sequence,
                    &operation_for_projection,
                    facts,
                )?;
                authority.end_observer_progress_source()?;
                sequence = sequence
                    .checked_add(1)
                    .ok_or(StateError::AllocationExhausted {
                        domain: "log sequence",
                    })?;
                merge
                    .apply_boundary(&mut authority, sequence, expected.as_ref())
                    .await?;
            }
            if page_len < super::log::READ_BATCH_SIZE {
                break;
            }
        }
        // F8B R-SEAL (§6.6) — the twin of the clause in `replay` above. Both
        // seats carry it because both are load-bearing: the production restore
        // and the equivalence oracle must agree on what a Closed conversation
        // looks like, or the oracle would report drift on every sealed store.
        if authority.is_closed() {
            if !authority.tokens.is_empty() || authority.frontier().is_some() {
                return Err(StateError::invariant(
                    "sealed conversation retained enrollment tokens or an executable frontier",
                ));
            }
        } else if authority.tokens.is_empty() {
            if authority.frontier().is_some() {
                return Err(StateError::invariant(
                    "durably empty conversation rebuilt an executable frontier",
                ));
            }
        } else if authority.frontier().is_none() {
            return Err(StateError::invariant(
                "enrolled conversation replay completed without executable frontier ownership",
            ));
        }
        merge.finish(&mut authority, sequence)?;
        Ok(authority)
    }

    /// Converts a frozen v2 row into the v3 in-memory model using exact replay
    /// prestate where the old Attached option grammar omitted its prior epoch.
    fn decode_operation(&self, decoded: DecodedOperation) -> Result<StoredOperation, StateError> {
        match (decoded.schema_version, decoded.operation) {
            (super::log::SCHEMA_VERSION, DecodedStoredOperation::V3(operation)) => Ok(operation),
            (super::log::SCHEMA_VERSION_V2, DecodedStoredOperation::V2(operation)) => {
                self.migrate_v2_operation(operation, decoded.sequence)
            }
            (actual, _) => Err(OperationLogError::SchemaVersion(actual).into()),
        }
    }

    fn migrate_v2_operation(
        &self,
        operation: StoredOperationV2,
        sequence: u64,
    ) -> Result<StoredOperation, StateError> {
        Ok(match operation {
            StoredOperationV2::Genesis { event } => StoredOperation::Genesis { event },
            StoredOperationV2::Enrolled {
                request,
                allocation,
                event,
            } => StoredOperation::Enrolled {
                request,
                allocation,
                event,
            },
            StoredOperationV2::Attached {
                request,
                secret_verified,
                allocation,
                event,
            } => {
                let prestate = match self
                    .slots
                    .get(&request.participant_id)
                    .map(|slot| slot.binding)
                {
                    Some(BindingState::Detached) => V2AttachedPrestate::Detached,
                    Some(BindingState::Bound(active)) => V2AttachedPrestate::Bound {
                        binding_epoch: active.binding_epoch.into(),
                    },
                    Some(BindingState::PendingFinalization(_)) | None => V2AttachedPrestate::Other,
                };
                migrate_v2_attached(
                    request,
                    secret_verified,
                    allocation,
                    event,
                    prestate,
                    sequence,
                )?
            }
            StoredOperationV2::Detached {
                request,
                verifier,
                receiving_epoch,
                terminal_order,
                terminal_seq,
                event,
            } => StoredOperation::Detached {
                row: StoredDetached {
                    participant_id: request.participant_id,
                    binding_epoch: receiving_epoch,
                    cause: StoredDetachedCause::CleanDeregister,
                    terminal_order,
                    disposition: StoredTerminalDisposition::Committed { terminal_seq },
                    source: StoredDetachedSource::ExplicitRequestCommitted {
                        request,
                        secret_verified: true,
                        verifier,
                        receiving_epoch,
                        event,
                    },
                },
            },
            StoredOperationV2::ZeroDebtAck {
                request,
                receiving_epoch,
                contiguously_available_through,
            } => StoredOperation::ZeroDebtAck {
                request,
                receiving_epoch,
                contiguously_available_through,
            },
            StoredOperationV2::MarkerDrained { row } => StoredOperation::MarkerDrained { row },
            StoredOperationV2::RecordAdmission { row } => StoredOperation::RecordAdmission { row },
            StoredOperationV2::Left { row } => StoredOperation::Left { row: row.into() },
        })
    }

    /// Replays one durable entry through the exact live transition cores.
    fn replay_operation(
        &mut self,
        operation: StoredOperation,
        sequence: u64,
        config: &ParticipantConfig,
        ack_obligations: Option<(RecipientAckObligations, u64)>,
        store: std::sync::Arc<dyn liminal::durability::DurableStore>,
    ) -> Result<Option<ParticipantDelivery>, StateError> {
        match operation {
            StoredOperation::Genesis { event } => self.replay_genesis(&event).map(|()| None),
            StoredOperation::Enrolled {
                request,
                allocation,
                event,
            } => self
                .replay_enrolled(request, &allocation, &event, sequence, config)
                .map(|()| None),
            StoredOperation::Attached {
                request,
                secret_verified,
                allocation,
                mode,
                event,
            } => {
                if !secret_verified {
                    return Err(StateError::invariant(
                        "durable attach entry recorded an unverified secret",
                    ));
                }
                self.replay_attached(request, &allocation, &mode, &event, sequence, store)
                    .map(|()| None)
            }
            StoredOperation::Detached { row } => self
                .replay_detached_operation(&row, sequence)
                .map(|()| None),
            StoredOperation::Died { row } => self.replay_died_row(&row, sequence).map(|()| None),
            operation @ (StoredOperation::Ordinary { .. } | StoredOperation::Recovered { .. }) => {
                self.replay_specific_fate(&operation, sequence)
                    .map(|()| None)
            }
            StoredOperation::ZeroDebtAck {
                request,
                receiving_epoch,
                contiguously_available_through,
            } => self
                .replay_zero_debt_ack_row(
                    request,
                    receiving_epoch,
                    contiguously_available_through,
                    ack_obligations,
                )
                .map(|()| None),
            StoredOperation::NonzeroDebtAck {
                request,
                receiving_epoch,
                contiguously_available_through,
                event,
            } => self
                .replay_nonzero_debt_ack(super::ops_nonzero_ack::NonzeroAckReplay {
                    stored_request: request,
                    receiving_epoch,
                    stored_scalar_audit: contiguously_available_through,
                    ack_obligations,
                    event: &event,
                    sequence,
                })
                .map(|()| None),
            StoredOperation::RecordAdmission { row } => {
                self.replay_record_admission(&row, config).map(|()| None)
            }
            StoredOperation::MarkerDrained { row } => self.replay_marker_drain(&row).map(Some),
            // R18 amendment A7 (§0.18 acceptance 3): the row replays through
            // the same installer the live commit ran, so the replayed store
            // reaches the identical generation and verifier.
            StoredOperation::CredentialReissued { row } => self
                .replay_credential_reissue(&row, sequence)
                .map(|()| None),
            StoredOperation::Left { row } => self.replay_leave(&row).map(|()| None),
        }
    }

    /// Routes one durable Detached row to its exact replay transition by
    /// disposition and source shape: connection-close and explicit sources
    /// replay their fate/detach cores; a committed `Drained` source replays
    /// the candidate-lane terminal drain.
    fn replay_detached_operation(
        &mut self,
        row: &StoredDetached,
        sequence: u64,
    ) -> Result<(), StateError> {
        match (row.disposition, row.source.clone()) {
            (_, StoredDetachedSource::ConnectionClose { .. }) => {
                self.replay_connection_detached(row, sequence)
            }
            (
                StoredTerminalDisposition::Committed { terminal_seq },
                StoredDetachedSource::ExplicitRequestCommitted {
                    request,
                    secret_verified: true,
                    verifier,
                    receiving_epoch,
                    event,
                },
            ) if row.cause == StoredDetachedCause::CleanDeregister
                && row.participant_id == request.participant_id
                && row.binding_epoch == receiving_epoch =>
            {
                self.replay_detached(
                    DetachReplayInputs {
                        request,
                        verifier,
                        receiving_epoch,
                        terminal_order: row.terminal_order,
                        terminal_seq,
                    },
                    &event,
                    sequence,
                )
            }
            (
                StoredTerminalDisposition::Pending,
                StoredDetachedSource::ExplicitRequestPending { .. },
            ) => self.replay_explicit_pending_detached(row, sequence),
            (StoredTerminalDisposition::Committed { .. }, StoredDetachedSource::Drained { .. }) => {
                self.replay_detached_drain_row(row, sequence)
            }
            _ => Err(OperationLogError::CorruptRow { sequence }.into()),
        }
    }
}

#[cfg(test)]
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct ValidationMemoryHighWater {
    pub(super) maximum_page_rows: usize,
    pub(super) maximum_active_reservations: usize,
}

/// Completes a bounded-page schema and contiguity pass before replay mutates
/// authority or reconciles any extension row. The apply pass rereads pages.
pub(super) async fn validate_operation_schema(
    log: &OperationLog,
    maximum_active_reservations: u64,
) -> Result<(), StateError> {
    validate_operation_schema_inner(log, maximum_active_reservations, |_, _| {}).await
}

#[cfg(test)]
pub(super) async fn validate_operation_schema_measured(
    log: &OperationLog,
    maximum_active_reservations: u64,
    high_water: &mut ValidationMemoryHighWater,
) -> Result<(), StateError> {
    validate_operation_schema_inner(log, maximum_active_reservations, |page_rows, active| {
        high_water.maximum_page_rows = high_water.maximum_page_rows.max(page_rows);
        high_water.maximum_active_reservations = high_water.maximum_active_reservations.max(active);
    })
    .await
}

async fn validate_operation_schema_inner(
    log: &OperationLog,
    maximum_active_reservations: u64,
    mut observe_memory: impl FnMut(usize, usize),
) -> Result<(), StateError> {
    let maximum_active_reservations =
        usize::try_from(maximum_active_reservations).map_err(|_| {
            StateError::AllocationExhausted {
                domain: "active recovered reservation count",
            }
        })?;
    let mut sequence = 0_u64;
    let mut phase = OperationSchemaPhase::V2Prefix;
    let mut composed = ComposedTerminalValidation::new(maximum_active_reservations);
    loop {
        let page = log.read_page(sequence, phase).await?;
        phase = page.next_phase;
        if page.rows.is_empty() {
            return Ok(());
        }
        let page_len = page.rows.len();
        observe_memory(page_len, composed.active_reservation_count());
        for decoded in page.rows {
            if decoded.sequence != sequence {
                return Err(OperationLogError::Sequence {
                    expected: sequence,
                    actual: decoded.sequence,
                }
                .into());
            }
            composed.validate(log, sequence, &decoded.operation).await?;
            observe_memory(page_len, composed.active_reservation_count());
            sequence = sequence
                .checked_add(1)
                .ok_or(StateError::AllocationExhausted {
                    domain: "log sequence",
                })?;
        }
        if page_len < super::log::READ_BATCH_SIZE {
            return Ok(());
        }
    }
}

/// Rows whose replay transition does not share its live append core route in
/// the dispatcher. Explicit committed Detached and both specific classes route
/// inside their shared core, exactly once. Drain rows (a `Died` row with
/// `drained: Some`, a `Detached` row with the `Drained` source) route NOWHERE:
/// their pending source row already owns the fate occurrence, and the drain
/// consumed its presentation through `select_finalizer` — exactly as a
/// pending-finalizing `Left` row does.
const fn route_in_replay_dispatch(operation: &StoredOperation) -> bool {
    matches!(
        operation,
        StoredOperation::Died {
            row: super::log::StoredDied { drained: None, .. }
        }
    ) || matches!(
        operation,
        StoredOperation::Detached {
            row: StoredDetached {
                source: StoredDetachedSource::ConnectionClose { .. }
                    | StoredDetachedSource::ExplicitRequestPending { .. },
                ..
            }
        }
    )
}
