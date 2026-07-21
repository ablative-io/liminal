//! Credential-attach arm of the production handler.
//!
//! Classification flows through the shared credential-attach lookup (token
//! phase, tombstone precedence, verifier order, live-authority checks),
//! commits through the crate's verified attach transitions — ordinary
//! detached attach or the R-C1.3 superseding handoff — mints the shell event
//! through the A3 aggregate commit, and answers through the request-bound
//! response authority. No lifecycle outcome is constructed here.
//!
//! Error contract: any [`StateError`] leaves durable state untouched (nothing
//! is published before the append succeeds) but may have consumed in-memory
//! authority. The handler therefore discards the whole in-memory conversation
//! owner on error and cold-replays durable reality on the next touch — the
//! same crash-consistency model the aggregate barrier is built for.

use std::sync::Arc;

use liminal::durability::DurableStore;
use liminal_protocol::lifecycle::{
    AggregateOperationDecision, AttachCommit, AttachCommitParameters, AttachFrontierCharges,
    AttachTransition, AttachedRecordPosition, BindingSlotDecision, BindingState,
    CredentialAttachLiveReceipt, CredentialAttachLookupResult, LiveFrontierOwner,
    PresentedIdentity, RetainedRecordCharge, SemanticConnectionCapacityDecision,
    apply_attach_frontier, commit_attach, decide_attached_operation, lookup_credential_attach,
    select_credential_attach_binding_slot,
};
use liminal_protocol::wire::{
    AttachBound, AttachEnvelope, AttachSecret, BindingEpoch, CredentialAttachRequest,
    CredentialAttachResponse, Generation, ReceiptExpiryReason,
};

use super::barrier::{ArmOutcome, CommitMode, OperationFacts, commit_through_barrier};
use super::capacity::ServerCapacity;
use super::facts::{self, Digest};
use super::frontier;
use super::log::{
    StoredAttachAllocation, StoredAttachModeV3, StoredAttachRequest, StoredOperation,
};
use super::observer_progress::ObserverProgressSourceMetadata;
use super::ops_attach_capacity::AttachStage8;
use super::ops_attach_lookup::{credential_attach_refusal, marker_bearing_attach_refusal};
use super::ops_attach_verify::{AttachVerification, verify_attach_mode};
use super::state::{
    AttachProvenanceRecord, AttachReceiptState, ConversationAuthority, DurableAppend,
    PendingBindingFate, Slot, StateError,
};

impl ConversationAuthority {
    /// Applies one credential-attach request end to end.
    ///
    /// Attach never creates a conversation: a fresh conversation id has no
    /// slots and classifies as `ParticipantUnknown` without any durable
    /// append.
    pub(super) fn apply_credential_attach(
        &mut self,
        request: &CredentialAttachRequest,
        operation_facts: &OperationFacts,
        server_capacity: &ServerCapacity,
        store: Arc<dyn DurableStore>,
        appender: &dyn DurableAppend,
    ) -> Result<ArmOutcome, StateError> {
        let envelope = attach_envelope(request);
        let now = u128::from(operation_facts.now_ms);
        // Request-time expiry of retained provenance fingerprints (contract
        // R-C0: retained only through their provenance deadlines). Safe
        // before lookup: an expired record and a pruned record classify
        // identically through the generation-window witness.
        self.prune_expired_provenance(now);
        let Some(slot) = self.slots.get(&request.participant_id) else {
            return Ok(ArmOutcome::respond(
                CredentialAttachResponse::participant_unknown(envelope).into_server_value(),
            ));
        };
        let (token_phase, secret_proof) = slot.attach_token_phase(request, now);
        let lookup = lookup_credential_attach(
            token_phase,
            PresentedIdentity::Live(&slot.member),
            &slot.binding,
            request,
            secret_proof,
        );
        if !matches!(lookup, CredentialAttachLookupResult::AuthorizedFresh { .. }) {
            return credential_attach_refusal(&lookup, envelope, slot).map(ArmOutcome::respond);
        }
        // Stage 6, first half: connection-conversation capacity (register
        // row 5641) — after the lookup stages, before binding-slot occupancy,
        // the crate's frozen stage order.
        let capacity = match operation_facts.semantic_connection_capacity() {
            SemanticConnectionCapacityDecision::Commit(value) => value,
            SemanticConnectionCapacityDecision::Respond { limit } => {
                return Ok(ArmOutcome::respond(
                    CredentialAttachResponse::connection_conversation_capacity_exceeded(
                        envelope, limit,
                    )
                    .into_server_value(),
                ));
            }
        };
        if let BindingSlotDecision::Respond(response) = select_credential_attach_binding_slot(
            request,
            self.binding_slot_occupancy(operation_facts.receiving_incarnation),
        ) {
            return Ok(ArmOutcome::respond(response.into_server_value()));
        }
        // Decision D §5.3 permits the fenced mint only after an exact durable
        // source row and the owner-held `ValidatedMarkerRecord` have both been
        // validated; it expressly forbids a raw replacement from the request.
        // This live binding has no participant-record delivery pump yet, so its
        // factual delivery/source state is empty. Preserve that authority
        // boundary by returning the total selector's typed refusal until the
        // later delivery owner can supply those facts.
        if request.accept_marker_delivery_seq.is_some() {
            return marker_bearing_attach_refusal(request, slot, operation_facts)
                .map(ArmOutcome::respond);
        }
        // Stage 8 (R-D1): credential attach's exact five-scope
        // receipt/provenance order, decided through the crate's verified
        // selector against per-participant/per-conversation occupancies from
        // this authority and server occupancies from the shared ledger; the
        // reservation is atomic with the check.
        let deadlines = operation_facts.deadlines()?;
        let (reservation, retire) = match self.attach_stage8(
            request,
            slot,
            operation_facts,
            server_capacity,
            &deadlines,
        )? {
            AttachStage8::Refused(response) => {
                return Ok(ArmOutcome::respond(response.into_server_value()));
            }
            AttachStage8::Reserved {
                reservation,
                retire,
            } => (reservation, retire),
        };
        // The rotation result: the new binding epoch carries the successor of
        // the verified current generation (the crate's ResultGeneration law).
        let next_generation = request
            .capability_generation
            .get()
            .checked_add(1)
            .and_then(Generation::new)
            .ok_or(StateError::AllocationExhausted {
                domain: "capability generation",
            })?;
        let (attached_order, attached_seq, attach_mode) =
            self.allocate_attach_mode(slot.binding)?;
        let allocation = StoredAttachAllocation {
            binding_epoch: BindingEpoch::new(
                operation_facts.receiving_incarnation,
                next_generation,
            )
            .into(),
            attach_secret: facts::mint_secret_bytes()?,
            attached_order,
            attached_seq,
            receipt_expires_at: deadlines.receipt_expires_at().into(),
            provenance_expires_at: deadlines.provenance_expires_at().into(),
            admitted_now_ms: operation_facts.now_ms,
        };
        let outcome = self.attach_commit(
            request,
            &allocation,
            &attach_mode,
            store,
            CommitMode::Live(appender),
        )?;
        // The durable append succeeded: the stage-8 reservation becomes
        // permanent and the receipts this rotation retired early (the
        // superseded attach receipt and, on the first rotation, the ended
        // enrollment receipt) leave the server-scope ledger.
        reservation.confirm(&retire);
        Ok(ArmOutcome::committed(
            CredentialAttachResponse::attach_bound(outcome).into_server_value(),
            capacity,
        ))
    }

    /// Selects the mandatory v3 Attached mode from exact binding authority and
    /// consumes only the matching checked order/sequence allocation.
    fn allocate_attach_mode(
        &mut self,
        binding: BindingState,
    ) -> Result<(u64, u64, StoredAttachModeV3), StateError> {
        match binding {
            BindingState::Detached => {
                let (order, sequence) = self.allocate_position()?;
                Ok((order, sequence, StoredAttachModeV3::Ordinary))
            }
            BindingState::Bound(active) => {
                let (order, terminal_sequence, attached_sequence) =
                    self.allocate_supersession_position()?;
                Ok((
                    order,
                    attached_sequence,
                    StoredAttachModeV3::Superseding {
                        prior_binding_epoch: active.binding_epoch.into(),
                        terminal_transaction_order: order,
                        terminal_delivery_seq: terminal_sequence,
                    },
                ))
            }
            BindingState::PendingFinalization(_) => Err(StateError::invariant(
                "pending finalization observed in a binding that commits detaches immediately",
            )),
        }
    }

    /// Replays one committed attach entry from its stored inputs.
    pub(super) fn replay_attached(
        &mut self,
        request: StoredAttachRequest,
        allocation: &StoredAttachAllocation,
        attach_mode: &StoredAttachModeV3,
        stored_event: &[u8],
        sequence: u64,
        store: Arc<dyn DurableStore>,
    ) -> Result<(), StateError> {
        let request = request.to_request()?;
        self.attach_commit(
            &request,
            allocation,
            attach_mode,
            store,
            CommitMode::Replay {
                stored_event,
                sequence,
            },
        )?;
        Ok(())
    }

    /// Shared credential-attach commit core (live and replay paths).
    ///
    /// The mode is derived from the slot's binding authority paired with the
    /// stored allocation: a detached slot with no terminal allocation binds
    /// ordinarily; a bound slot with a terminal allocation supersedes its
    /// active epoch atomically (one ordered `Detached(Superseded)`/`Attached`
    /// handoff through the crate's verified transition). Any other pairing is
    /// a drifted log and fails loudly.
    fn attach_commit(
        &mut self,
        request: &CredentialAttachRequest,
        allocation: &StoredAttachAllocation,
        attach_mode: &StoredAttachModeV3,
        store: Arc<dyn DurableStore>,
        mode: CommitMode<'_>,
    ) -> Result<AttachBound, StateError> {
        let source_sequence = self.next_log_sequence;
        let (participant_id, mut slot) = self
            .slots
            .remove_entry(&request.participant_id)
            .ok_or_else(|| {
                StateError::invariant("attach commit requires an enrolled participant slot")
            })?;
        let binding_epoch = allocation.binding_epoch.to_epoch()?;
        let result_generation = binding_epoch.capability_generation;
        let parameters = AttachCommitParameters {
            binding: liminal_protocol::lifecycle::ActiveBinding {
                participant_id: request.participant_id,
                conversation_id: request.conversation_id,
                binding_epoch,
            },
            attach_secret: AttachSecret::new(allocation.attach_secret),
            attached_position: AttachedRecordPosition::new(
                allocation.attached_order,
                allocation.attached_seq,
            ),
            receipt_expires_at: allocation.receipt_expires_at.get(),
            provenance_expires_at: allocation.provenance_expires_at.get(),
        };
        let frontier_owner = self.take_frontier()?;
        let (verified, frontier_owner) = verify_attach_mode(
            slot.member,
            slot.binding,
            frontier_owner,
            AttachVerification {
                request,
                mode: attach_mode,
                parameters,
                store,
                source_sequence,
            },
        )?;
        let committed = commit_attach(verified, slot.cell).map_err(|error| {
            StateError::invariant(format!("protocol attach transition failed: {error:?}"))
        })?;
        let observer_projection = committed.observer_progress_projection();
        let (committed, frontier_owner) =
            transition_attach_frontier(frontier_owner, committed, request, allocation)?;
        let shell = self.take_shell()?;
        let barrier = match decide_attached_operation(shell, committed) {
            AggregateOperationDecision::Commit(barrier) => barrier,
            AggregateOperationDecision::Refused(refusal) => {
                return Err(StateError::ShellRefused {
                    reason: refusal.reason(),
                });
            }
        };
        let make_operation = |event: Vec<u8>| StoredOperation::Attached {
            request: request.into(),
            secret_verified: true,
            allocation: *allocation,
            mode: Box::new(attach_mode.clone()),
            event,
        };
        let (shell, committed) =
            commit_through_barrier(barrier, mode, self.next_log_sequence, &make_operation)?;
        self.shell = Some(shell);
        self.install_frontier(frontier_owner);
        self.advance_log_head()?;
        let outcome = committed.outcome.clone();
        let (installed, fate_token) = committed.into_slot_and_fate();
        slot.member = installed.member;
        slot.binding = installed.binding_state;
        slot.binding_fate = Some(PendingBindingFate {
            attached_source_sequence: source_sequence,
            token: fate_token,
        });
        slot.cell = installed.detach_cell;
        slot.attach_secret = AttachSecret::new(allocation.attach_secret);
        install_attach_receipt(
            &mut slot,
            request,
            allocation,
            &outcome,
            installed.outcome,
            result_generation,
        );
        self.slots.insert(participant_id, slot);
        if let Some(projection) = observer_projection {
            let terminal_delivery_seq = projection.new_observer_progress();
            let metadata = attach_metadata(source_sequence, request, terminal_delivery_seq);
            self.record_observer_progress_projection(projection, metadata)?;
        }
        self.observe_replayed_position(allocation.attached_order, allocation.attached_seq)?;
        Ok(outcome)
    }
}

fn install_attach_receipt(
    slot: &mut Slot,
    request: &CredentialAttachRequest,
    allocation: &StoredAttachAllocation,
    outcome: &AttachBound,
    installed_outcome: AttachBound,
    result_generation: Generation,
) {
    // Retire the previous receipt into its bounded provenance record with the
    // exact terminal reason: `Superseded` when the newer generation ended a
    // still-live receipt, `Deadline` when its own deadline had already ended
    // it. The admitted clock makes replay reproduce the identical record.
    if let Some(previous) = slot.attach.take() {
        let reason = if u128::from(allocation.admitted_now_ms) < previous.receipt_expires_at {
            ReceiptExpiryReason::Superseded
        } else {
            ReceiptExpiryReason::Deadline
        };
        slot.attach_provenance.insert(
            previous.token.into_bytes(),
            AttachProvenanceRecord {
                result_generation: previous.result_generation,
                reason,
                provenance_expires_at: previous.provenance_expires_at,
            },
        );
    }
    // The first rotation also ends the enrollment receipt's secret body. Set
    // once and never rewrite it, preserving the exact end-of-body fact.
    if slot.enrollment_receipt_ended.is_none() {
        slot.enrollment_receipt_ended = Some(
            if u128::from(allocation.admitted_now_ms) < slot.enrollment_receipt_expires_at {
                ReceiptExpiryReason::Superseded
            } else {
                ReceiptExpiryReason::Deadline
            },
        );
    }
    slot.attach = Some(AttachReceiptState {
        token: request.attach_attempt_token,
        receipt: CredentialAttachLiveReceipt::from_commit(outcome.clone()),
        outcome: installed_outcome,
        verifier: request.attach_secret.into_bytes(),
        result_generation,
        receipt_expires_at: allocation.receipt_expires_at.get(),
        provenance_expires_at: allocation.provenance_expires_at.get(),
    });
}

const fn attach_metadata(
    source_sequence: u64,
    request: &CredentialAttachRequest,
    terminal_delivery_seq: u64,
) -> ObserverProgressSourceMetadata {
    ObserverProgressSourceMetadata::attached(
        source_sequence,
        request.conversation_id,
        request.participant_id,
        terminal_delivery_seq,
    )
}

fn transition_attach_frontier(
    owner: LiveFrontierOwner,
    committed: AttachCommit<Digest, Digest>,
    request: &CredentialAttachRequest,
    allocation: &StoredAttachAllocation,
) -> Result<(AttachCommit<Digest, Digest>, LiveFrontierOwner), StateError> {
    let attached_encoded = frontier::credential_attached_charge(
        request.conversation_id,
        request.participant_id,
        allocation,
    )?;
    let attached_charge = RetainedRecordCharge::new(
        committed.attached.delivery_seq(),
        committed.attached.admission_order(),
        attached_encoded,
    );
    let terminal = match committed.transition {
        AttachTransition::Detached => None,
        AttachTransition::Superseded { terminal } => Some(terminal.into()),
        AttachTransition::FencedRecovery {
            composed_terminal, ..
        } => composed_terminal,
    };
    let terminal_charge = terminal
        .map(|terminal| {
            frontier::terminal_charge(
                terminal.conversation_id(),
                terminal.participant_id(),
                terminal.binding_epoch(),
                terminal.admission_order().transaction_order(),
                terminal.delivery_seq(),
            )
            .map(|encoded| {
                RetainedRecordCharge::new(
                    terminal.delivery_seq(),
                    terminal.admission_order(),
                    encoded,
                )
            })
        })
        .transpose()?;
    apply_attach_frontier(
        owner,
        committed,
        AttachFrontierCharges::new(terminal_charge, attached_charge),
    )
    .map_err(|failure| {
        StateError::invariant(format!(
            "attach frontier transition failed: {:?}",
            failure.error()
        ))
    })
    .map(liminal_protocol::lifecycle::LiveFrontierCommit::into_parts)
}

/// Builds the echo envelope of one credential-attach request.
pub(super) const fn attach_envelope(request: &CredentialAttachRequest) -> AttachEnvelope {
    AttachEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        attach_attempt_token: request.attach_attempt_token,
        accept_marker_delivery_seq: request.accept_marker_delivery_seq,
    }
}
