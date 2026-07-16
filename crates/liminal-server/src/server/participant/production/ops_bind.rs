//! Enrollment and credential-attach arms of the production handler.
//!
//! Both arms classify through the shared lookup selectors, commit through
//! the crate's typed transitions, mint their shell events through the A3
//! aggregate commits, and answer through the request-bound response
//! authorities. No lifecycle outcome is constructed here.
//!
//! Error contract: any [`StateError`] leaves durable state untouched (nothing
//! is published before the append succeeds) but may have consumed in-memory
//! authority. The handler therefore discards the whole in-memory conversation
//! owner on error and cold-replays durable reality on the next touch — the
//! same crash-consistency model the aggregate barrier is built for.

use liminal_protocol::lifecycle::{
    AggregateOperationDecision, AllocatedParticipantSlot, AttachCommitParameters,
    AttachSecretProof, AttachedRecordPosition, BindingSlotDecision, BindingSlotOccupancy,
    BindingState, ClosureState, CommittedBindingTerminalPosition, CredentialAttachLiveReceipt,
    CredentialAttachLookupResult, CredentialAttachProvenance, CredentialAttachTokenPhase,
    DetachCell, EnrollmentCommitParameters, EnrollmentFingerprint, EnrollmentLiveReceipt,
    EnrollmentLookupResult, EnrollmentTokenPhase, MarkerProofDecision, MarkerProofInput,
    MarkerProofState, ParticipantSlotAllocatorProof, PresentedIdentity, ReceiptDeadlines,
    ResolvedIdentity, commit_attach, commit_enrollment, decide_attached_operation,
    decide_enrolled_operation, lookup_credential_attach, lookup_enrollment,
    select_credential_attach_binding_slot, select_enrollment_binding_slot, select_marker_proof,
};
use liminal_protocol::wire::{
    AttachBound, AttachEnvelope, AttachSecret,
    AttemptTokenBodyConflict as WireAttemptTokenBodyConflict, BindingEpoch, ConnectionIncarnation,
    CredentialAttachRequest, CredentialAttachResponse, EnrollBound, EnrollmentRequest,
    EnrollmentResponse, Generation, MarkerMismatch, MarkerNotDelivered, MarkerProofRequest,
    ReceiptExpired as WireReceiptExpired, ReceiptExpiryReason, ServerValue,
};

use super::facts::{self, Digest};
use super::log::{
    StoredAttachAllocation, StoredAttachRequest, StoredEnrollmentAllocation,
    StoredEnrollmentRequest, StoredOperation,
};
use super::state::{ConversationAuthority, DurableAppend, Slot, StateError};

/// Connection-scoped and configured facts supplied to each operation.
#[derive(Clone, Copy, Debug)]
pub(super) struct OperationFacts {
    /// Durable incarnation of the receiving connection.
    pub(super) receiving_incarnation: ConnectionIncarnation,
    /// Admitted wall-clock read for deadline derivation and receipt phases.
    pub(super) now_ms: u64,
    /// Configured identity slots per enrolled participant.
    pub(super) identity_slots: u64,
    /// Configured secret-bearing receipt TTL.
    pub(super) attach_receipt_ttl_ms: u64,
    /// Configured non-secret provenance TTL.
    pub(super) receipt_provenance_ttl_ms: u64,
}

impl OperationFacts {
    fn deadlines(&self) -> Result<ReceiptDeadlines, StateError> {
        ReceiptDeadlines::try_from_ttls(
            self.now_ms,
            self.attach_receipt_ttl_ms,
            self.receipt_provenance_ttl_ms,
        )
        .map_err(|error| {
            StateError::invariant(format!("validated TTL configuration rejected: {error:?}"))
        })
    }
}

/// Server-owned participant-slot allocation proof for one enrollment.
#[derive(Clone, Copy, Debug)]
struct ServerSlotProof {
    conversation_id: u64,
    participant_id: u64,
    identity_limit: u64,
}

impl ParticipantSlotAllocatorProof for ServerSlotProof {
    fn conversation_id(&self) -> u64 {
        self.conversation_id
    }

    fn participant_index(&self) -> u64 {
        self.participant_id
    }

    fn identity_limit(&self) -> u64 {
        self.identity_limit
    }
}

impl ConversationAuthority {
    /// Reports whether the receiving connection already owns a bound slot in
    /// this conversation, exposing only what the crate's stage-6 selector
    /// needs. Derived from binding-epoch authority; no side table exists.
    pub(super) fn binding_slot_occupancy(
        &self,
        receiving_incarnation: ConnectionIncarnation,
    ) -> BindingSlotOccupancy {
        for (participant_id, slot) in &self.slots {
            if let BindingState::Bound(active) = slot.binding {
                if active.binding_epoch.connection_incarnation == receiving_incarnation {
                    return BindingSlotOccupancy::Occupied {
                        participant_id: *participant_id,
                    };
                }
            }
        }
        BindingSlotOccupancy::Empty
    }

    /// Applies one enrollment request end to end.
    ///
    /// Every refusal (token replay, binding-slot occupancy) classifies over
    /// the replayed authority WITHOUT touching durable state; the durable
    /// shell genesis is minted only on the authorized-new arm, immediately
    /// before the enrollment's own committing append — a refused request on a
    /// never-seen conversation id leaves the durable store byte-identical.
    pub(super) fn apply_enrollment(
        &mut self,
        request: &EnrollmentRequest,
        operation_facts: &OperationFacts,
        appender: &dyn DurableAppend,
    ) -> Result<ServerValue, StateError> {
        let token_bytes = request.enrollment_token.into_bytes();
        if let Some(participant_id) = self.tokens.get(&token_bytes).copied() {
            let slot = self.slots.get(&participant_id).ok_or_else(|| {
                StateError::invariant("enrollment token maps to a missing participant slot")
            })?;
            return enrollment_replay_response(slot, request, operation_facts);
        }
        if let BindingSlotDecision::Respond(response) = select_enrollment_binding_slot(
            request,
            self.binding_slot_occupancy(operation_facts.receiving_incarnation),
        ) {
            return Ok(response.into_server_value());
        }

        let deadlines = operation_facts.deadlines()?;
        // The one conversation-creating arm: genesis is durable exactly when
        // an authorized enrollment is about to append its own entry.
        self.ensure_genesis(appender)?;
        let (attached_order, attached_seq) = self.allocate_position()?;
        let allocation = StoredEnrollmentAllocation {
            participant_id: self.next_participant,
            identity_limit: operation_facts.identity_slots,
            attach_secret: facts::mint_secret_bytes()?,
            origin_epoch: BindingEpoch::new(operation_facts.receiving_incarnation, Generation::ONE)
                .into(),
            attached_order,
            attached_seq,
            receipt_expires_at: deadlines.receipt_expires_at().into(),
            provenance_expires_at: deadlines.provenance_expires_at().into(),
            enrollment_fingerprint: facts::enrollment_fingerprint(request.enrollment_token),
        };
        let outcome = self.enroll_commit(request, &allocation, CommitMode::Live(appender))?;
        Ok(EnrollmentResponse::enroll_bound(outcome).into_server_value())
    }

    /// Replays one committed enrollment entry from its stored inputs.
    pub(super) fn replay_enrolled(
        &mut self,
        request: StoredEnrollmentRequest,
        allocation: &StoredEnrollmentAllocation,
        stored_event: &[u8],
        sequence: u64,
    ) -> Result<(), StateError> {
        let request = request.to_request();
        self.enroll_commit(
            &request,
            allocation,
            CommitMode::Replay {
                stored_event,
                sequence,
            },
        )?;
        Ok(())
    }

    /// Shared enrollment commit core for the live and replay paths.
    fn enroll_commit(
        &mut self,
        request: &EnrollmentRequest,
        allocation: &StoredEnrollmentAllocation,
        mode: CommitMode<'_>,
    ) -> Result<EnrollBound, StateError> {
        let allocated_slot = AllocatedParticipantSlot::from_allocator(ServerSlotProof {
            conversation_id: request.conversation_id,
            participant_id: allocation.participant_id,
            identity_limit: allocation.identity_limit,
        })
        .map_err(|error| {
            StateError::invariant(format!("participant slot allocation rejected: {error:?}"))
        })?;
        let committed = commit_enrollment(
            request,
            EnrollmentCommitParameters {
                allocated_slot,
                attach_secret: AttachSecret::new(allocation.attach_secret),
                origin_binding_epoch: allocation.origin_epoch.to_epoch()?,
                attached_position: AttachedRecordPosition::new(
                    allocation.attached_order,
                    allocation.attached_seq,
                ),
                receipt_expires_at: allocation.receipt_expires_at.get(),
                provenance_expires_at: allocation.provenance_expires_at.get(),
                enrollment_fingerprint: EnrollmentFingerprint::new(
                    allocation.enrollment_fingerprint,
                ),
            },
        )
        .map_err(|error| {
            StateError::invariant(format!("protocol enrollment transition failed: {error:?}"))
        })?;
        let shell = self.take_shell()?;
        let barrier = match decide_enrolled_operation(shell, committed) {
            AggregateOperationDecision::Commit(barrier) => barrier,
            AggregateOperationDecision::Refused(refusal) => {
                return Err(StateError::ShellRefused {
                    reason: refusal.reason(),
                });
            }
        };
        let make_operation = |event: Vec<u8>| StoredOperation::Enrolled {
            request: request.into(),
            allocation: *allocation,
            event,
        };
        let (shell, committed) =
            commit_through_barrier(barrier, mode, self.next_log_sequence, &make_operation)?;
        self.shell = Some(shell);
        self.advance_log_head()?;
        let outcome = committed.outcome.clone();
        self.slots.insert(
            allocation.participant_id,
            Slot {
                member: committed.member,
                binding: committed.binding_state,
                cell: DetachCell::default(),
                enrollment_receipt: EnrollmentLiveReceipt::from_commit(outcome.clone()),
                enrollment_outcome: committed.outcome,
                enrollment_receipt_expires_at: allocation.receipt_expires_at.get(),
                attach: None,
                attach_provenance: std::collections::BTreeMap::new(),
                attach_secret: AttachSecret::new(allocation.attach_secret),
                exact_detach_token: None,
            },
        );
        self.tokens.insert(
            request.enrollment_token.into_bytes(),
            allocation.participant_id,
        );
        self.next_participant = allocation
            .participant_id
            .checked_add(1)
            .ok_or(StateError::AllocationExhausted {
                domain: "participant index",
            })?
            .max(self.next_participant);
        self.observe_replayed_position(allocation.attached_order, allocation.attached_seq);
        Ok(outcome)
    }

    /// Applies one credential-attach request end to end.
    ///
    /// Attach never creates a conversation: a fresh conversation id has no
    /// slots and classifies as `ParticipantUnknown` without any durable
    /// append.
    pub(super) fn apply_credential_attach(
        &mut self,
        request: &CredentialAttachRequest,
        operation_facts: &OperationFacts,
        appender: &dyn DurableAppend,
    ) -> Result<ServerValue, StateError> {
        let envelope = attach_envelope(request);
        let Some(slot) = self.slots.get(&request.participant_id) else {
            return Ok(CredentialAttachResponse::participant_unknown(envelope).into_server_value());
        };
        let now = u128::from(operation_facts.now_ms);
        let (token_phase, secret_proof) = slot.attach_token_phase(request, now);
        let lookup = lookup_credential_attach(
            token_phase,
            PresentedIdentity::Live(&slot.member),
            &slot.binding,
            request,
            secret_proof,
        );
        if !matches!(lookup, CredentialAttachLookupResult::AuthorizedFresh { .. }) {
            return credential_attach_refusal(&lookup, envelope, slot);
        }
        if let BindingSlotDecision::Respond(response) = select_credential_attach_binding_slot(
            request,
            self.binding_slot_occupancy(operation_facts.receiving_incarnation),
        ) {
            return Ok(response.into_server_value());
        }
        // A marker-bearing attach is a fenced-recovery presentation: classify
        // it through the crate's total marker-proof selector against the
        // factual (empty) delivery state — a typed refusal, never a
        // connection-fatal invariant.
        if request.accept_marker_delivery_seq.is_some() {
            return marker_bearing_attach_refusal(request, slot, operation_facts);
        }
        // Attach mode from binding authority (contract R-C1.3): a bound slot
        // for the SAME participant supersedes — one ordered
        // Detached(Superseded)/Attached handoff, even on this connection
        // incarnation; a detached slot binds ordinarily.
        let superseding = match &slot.binding {
            BindingState::Detached => false,
            BindingState::Bound(_) => true,
            BindingState::PendingFinalization(_) => {
                return Err(StateError::invariant(
                    "pending finalization observed in a binding that commits detaches immediately",
                ));
            }
        };

        let deadlines = operation_facts.deadlines()?;
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
        let (attached_order, superseded_terminal_seq, attached_seq) = if superseding {
            let (order, terminal_seq, attached_seq) = self.allocate_supersession_position()?;
            (order, Some(terminal_seq), attached_seq)
        } else {
            let (order, seq) = self.allocate_position()?;
            (order, None, seq)
        };
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
            superseded_terminal_seq,
        };
        let outcome = self.attach_commit(request, &allocation, CommitMode::Live(appender))?;
        Ok(CredentialAttachResponse::attach_bound(outcome).into_server_value())
    }

    /// Replays one committed attach entry from its stored inputs.
    pub(super) fn replay_attached(
        &mut self,
        request: StoredAttachRequest,
        allocation: &StoredAttachAllocation,
        stored_event: &[u8],
        sequence: u64,
    ) -> Result<(), StateError> {
        let request = request.to_request()?;
        self.attach_commit(
            &request,
            allocation,
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
        mode: CommitMode<'_>,
    ) -> Result<AttachBound, StateError> {
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
        let verified =
            verify_attach_mode(slot.member, slot.binding, request, allocation, parameters)?;
        let committed = commit_attach(verified, slot.cell).map_err(|error| {
            StateError::invariant(format!("protocol attach transition failed: {error:?}"))
        })?;
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
            event,
        };
        let (shell, committed) =
            commit_through_barrier(barrier, mode, self.next_log_sequence, &make_operation)?;
        self.shell = Some(shell);
        self.advance_log_head()?;
        let outcome = committed.outcome.clone();
        slot.member = committed.member;
        slot.binding = committed.binding_state;
        slot.cell = committed.detach_cell;
        slot.attach_secret = AttachSecret::new(allocation.attach_secret);
        // Retire the previous receipt into its bounded provenance record with
        // the exact terminal reason: `Superseded` when the newer generation
        // ended a still-live receipt, `Deadline` when its own deadline had
        // already ended it. Derived from the committing operation's ADMITTED
        // clock read, so replay reproduces the identical record.
        if let Some(previous) = slot.attach.take() {
            let reason = if u128::from(allocation.admitted_now_ms) < previous.receipt_expires_at {
                ReceiptExpiryReason::Superseded
            } else {
                ReceiptExpiryReason::Deadline
            };
            slot.attach_provenance.insert(
                previous.token.into_bytes(),
                super::state::AttachProvenanceRecord {
                    result_generation: previous.result_generation,
                    reason,
                    provenance_expires_at: previous.provenance_expires_at,
                },
            );
        }
        slot.attach = Some(super::state::AttachReceiptState {
            token: request.attach_attempt_token,
            receipt: CredentialAttachLiveReceipt::from_commit(outcome.clone()),
            outcome: committed.outcome,
            verifier: request.attach_secret.into_bytes(),
            result_generation,
            receipt_expires_at: allocation.receipt_expires_at.get(),
            provenance_expires_at: allocation.provenance_expires_at.get(),
        });
        self.slots.insert(participant_id, slot);
        self.observe_replayed_position(allocation.attached_order, allocation.attached_seq);
        Ok(outcome)
    }

    /// Advances the position allocators past a replayed entry's positions.
    fn observe_replayed_position(&mut self, order: u64, seq: u64) {
        self.next_order = self.next_order.max(order.saturating_add(1));
        self.next_seq = self.next_seq.max(seq.saturating_add(1));
    }
}

/// One barrier resolution mode: live durable append or replay byte-check.
#[derive(Clone, Copy)]
pub(super) enum CommitMode<'a> {
    /// Append the operation at the optimistic head, then commit.
    Live(&'a dyn DurableAppend),
    /// Cross-check re-minted canonical bytes against the stored entry.
    Replay {
        /// Canonical event bytes read from the durable entry.
        stored_event: &'a [u8],
        /// Durable log sequence of the entry (for drift diagnostics).
        sequence: u64,
    },
}

/// Resolves one pending aggregate barrier through the selected mode.
pub(super) fn commit_through_barrier<T>(
    barrier: liminal_protocol::lifecycle::AggregateOperationCommit<T>,
    mode: CommitMode<'_>,
    next_log_sequence: u64,
    make_operation: &dyn Fn(Vec<u8>) -> StoredOperation,
) -> Result<(liminal_protocol::lifecycle::ParticipantConversation, T), StateError> {
    let event = barrier.event().encode_canonical();
    match mode {
        CommitMode::Live(appender) => {
            let operation = make_operation(event);
            appender.append(&operation, next_log_sequence)?;
        }
        CommitMode::Replay {
            stored_event,
            sequence,
        } => {
            if event != stored_event {
                return Err(StateError::ReplayedEventDrift { sequence });
            }
        }
    }
    Ok(barrier.commit())
}

/// Builds the echo envelope of one credential-attach request.
const fn attach_envelope(request: &CredentialAttachRequest) -> AttachEnvelope {
    AttachEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        attach_attempt_token: request.attach_attempt_token,
        accept_marker_delivery_seq: request.accept_marker_delivery_seq,
    }
}

impl Slot {
    /// Resolves the credential-attach token phase and its phase-scoped
    /// constant-time secret proof.
    ///
    /// The token phase resolves against the CURRENT receipt's own deadline
    /// pair first, then against the retained provenance fingerprints of ended
    /// receipts. Each receipt's windows are fixed at its own commit; a later
    /// attach never re-opens them. The verifier is phase-scoped (contract
    /// R-C0): a live receipt replay verifies against the receipt's own
    /// committed presented secret (contract row 4 recovers a lost rotation
    /// with the invalidated OLD secret); every other phase verifies against
    /// the slot's current secret. Provenance phases ignore the proof by
    /// construction of their result path.
    fn attach_token_phase(
        &self,
        request: &CredentialAttachRequest,
        now: u128,
    ) -> (
        CredentialAttachTokenPhase<'_, Digest, Digest, Digest>,
        AttachSecretProof,
    ) {
        let identity = ResolvedIdentity::<Digest, Digest, Digest>::Live(&self.member);
        let mut verifier_bytes = self.attach_secret.into_bytes();
        let token_phase = match self.attach.as_ref() {
            Some(attach) if attach.token == request.attach_attempt_token => {
                if now < attach.receipt_expires_at {
                    verifier_bytes = attach.verifier;
                    CredentialAttachTokenPhase::LiveReceipt {
                        identity,
                        receipt: &attach.receipt,
                    }
                } else if now < attach.provenance_expires_at {
                    CredentialAttachTokenPhase::Provenance {
                        identity,
                        provenance: CredentialAttachProvenance::new(
                            attach.result_generation,
                            ReceiptExpiryReason::Deadline,
                        ),
                    }
                } else {
                    CredentialAttachTokenPhase::AfterProvenance
                }
            }
            _ => match self
                .attach_provenance
                .get(&request.attach_attempt_token.into_bytes())
            {
                Some(record) if now < record.provenance_expires_at => {
                    CredentialAttachTokenPhase::Provenance {
                        identity,
                        provenance: CredentialAttachProvenance::new(
                            record.result_generation,
                            record.reason,
                        ),
                    }
                }
                Some(_) => CredentialAttachTokenPhase::AfterProvenance,
                None => CredentialAttachTokenPhase::NoMatch,
            },
        };
        let secret_proof =
            if facts::constant_time_eq(&verifier_bytes, &request.attach_secret.into_bytes()) {
                AttachSecretProof::Verified
            } else {
                AttachSecretProof::Mismatch
            };
        (token_phase, secret_proof)
    }
}

/// Verifies one attach transition in its allocation-derived mode.
///
/// A detached slot with no terminal allocation binds ordinarily; a bound
/// slot with a terminal allocation supersedes its active epoch (contract
/// R-C1.3's ordered handoff). Any other pairing is a drifted log and fails
/// loudly.
fn verify_attach_mode(
    member: liminal_protocol::lifecycle::LiveMember<Digest>,
    binding: BindingState,
    request: &CredentialAttachRequest,
    allocation: &StoredAttachAllocation,
    parameters: AttachCommitParameters,
) -> Result<liminal_protocol::lifecycle::VerifiedAttachCommit<'static, Digest>, StateError> {
    match (binding, allocation.superseded_terminal_seq) {
        (BindingState::Detached, None) => {
            let closure_admission = ClosureState::Clear
                .ordinary_detached_attach_admission()
                .map_err(|error| {
                    StateError::invariant(format!(
                        "clear closure refused detached attach admission: {error:?}"
                    ))
                })?;
            member.verify_detached_attach(
                BindingState::Detached,
                closure_admission,
                request.clone(),
                AttachSecretProof::Verified,
                parameters,
            )
        }
        (BindingState::Bound(active), Some(terminal_seq)) => member.verify_superseding_attach(
            active,
            request.clone(),
            AttachSecretProof::Verified,
            CommittedBindingTerminalPosition::new(allocation.attached_order, terminal_seq),
            parameters,
        ),
        (_, _) => {
            return Err(StateError::invariant(
                "attach allocation mode does not match the slot's binding authority",
            ));
        }
    }
    .map_err(|error| {
        StateError::invariant(format!("protocol attach verification failed: {error:?}"))
    })
}

/// Classifies a marker-bearing (fenced-recovery) attach through the crate's
/// total marker-proof selector against the factual delivery state.
///
/// This binding delivers no markers yet (no delivery pump exists for
/// participant records), so the durable marker facts are empty: no expected
/// marker, no delivery witness. The crate selects the exact typed refusal; a
/// permitted fenced attach is unreachable until delivery exists, and
/// observing one is a loud invariant failure — never a silently hand-built
/// outcome.
fn marker_bearing_attach_refusal(
    request: &CredentialAttachRequest,
    slot: &Slot,
    operation_facts: &OperationFacts,
) -> Result<ServerValue, StateError> {
    let Some(input) = MarkerProofInput::credential_attach(request) else {
        return Err(StateError::invariant(
            "marker-bearing attach classification without a presented marker",
        ));
    };
    let proof_epoch = BindingEpoch::new(
        operation_facts.receiving_incarnation,
        request.capability_generation,
    );
    let marker_state = MarkerProofState::new(slot.member.cursor(), false, None, proof_epoch, None);
    let response = match select_marker_proof(&marker_state, input) {
        MarkerProofDecision::MarkerMismatch(MarkerMismatch {
            request: MarkerProofRequest::CredentialAttach(proof),
            mismatch,
        }) => CredentialAttachResponse::marker_mismatch(proof, mismatch),
        MarkerProofDecision::MarkerNotDelivered(MarkerNotDelivered {
            request: MarkerProofRequest::CredentialAttach(proof),
            reason,
            expected_marker_delivery_seq,
        }) => CredentialAttachResponse::marker_not_delivered(
            proof,
            reason,
            expected_marker_delivery_seq,
        ),
        MarkerProofDecision::MarkerMismatch(_) | MarkerProofDecision::MarkerNotDelivered(_) => {
            return Err(StateError::invariant(
                "attach marker proof classified under a foreign operation envelope",
            ));
        }
        MarkerProofDecision::AckNoOp(_) => {
            return Err(StateError::invariant(
                "attach marker proof classified as a marker-ack no-op",
            ));
        }
        MarkerProofDecision::Permit(_) => {
            return Err(StateError::invariant(
                "marker proof permitted although no marker was ever delivered",
            ));
        }
    };
    Ok(response.into_server_value())
}

/// Maps a non-authorized credential-attach lookup onto its bound response.
///
/// Every classified fact travels FROM the crate's lookup value into the
/// response authority — the conflict kind, the provenance row's generations
/// and terminal reason — with no server-side re-derivation of any lifecycle
/// rule.
fn credential_attach_refusal(
    lookup: &CredentialAttachLookupResult<'_, Digest>,
    envelope: AttachEnvelope,
    slot: &Slot,
) -> Result<ServerValue, StateError> {
    let response = match lookup {
        CredentialAttachLookupResult::ParticipantUnknown(_) => {
            CredentialAttachResponse::participant_unknown(envelope)
        }
        CredentialAttachLookupResult::StaleAuthority(_) => {
            CredentialAttachResponse::stale_authority(envelope, slot.member.generation())
        }
        CredentialAttachLookupResult::AttemptTokenBodyConflict(value) => {
            let WireAttemptTokenBodyConflict::CredentialAttach { conflict, .. } = value else {
                return Err(StateError::invariant(
                    "leave conflict row observed in the credential-attach lookup",
                ));
            };
            CredentialAttachResponse::attempt_token_body_conflict(&envelope, *conflict)
        }
        CredentialAttachLookupResult::Bound(_) => {
            let outcome = slot
                .attach
                .as_ref()
                .map(|attach| attach.outcome.clone())
                .ok_or_else(|| {
                    StateError::invariant("attach receipt replay without a stored receipt")
                })?;
            CredentialAttachResponse::bound(outcome)
        }
        CredentialAttachLookupResult::UnboundReceipt(_) => {
            let outcome = slot
                .attach
                .as_ref()
                .map(|attach| attach.outcome.clone())
                .ok_or_else(|| {
                    StateError::invariant("attach receipt replay without a stored receipt")
                })?;
            CredentialAttachResponse::unbound_receipt(outcome)
        }
        CredentialAttachLookupResult::ReceiptExpired(value) => {
            let WireReceiptExpired::CredentialAttach {
                result_generation,
                current_generation,
                reason,
                ..
            } = value
            else {
                return Err(StateError::invariant(
                    "enrollment provenance row observed in the credential-attach lookup",
                ));
            };
            CredentialAttachResponse::receipt_expired(
                &envelope,
                *result_generation,
                *current_generation,
                *reason,
            )
        }
        CredentialAttachLookupResult::StaleOrUnknownReceipt(value) => {
            CredentialAttachResponse::stale_or_unknown_receipt(value.clone())
        }
        CredentialAttachLookupResult::Retired(_) => {
            return Err(StateError::invariant(
                "retired identity observed in a binding that mints no tombstones",
            ));
        }
        CredentialAttachLookupResult::AuthorizedFresh { .. } => {
            return Err(StateError::invariant(
                "authorized attach routed through the refusal mapper",
            ));
        }
    };
    Ok(response.into_server_value())
}

/// Builds the enrollment replay/known response for a mapped token.
fn enrollment_replay_response(
    slot: &Slot,
    request: &EnrollmentRequest,
    operation_facts: &OperationFacts,
) -> Result<ServerValue, StateError> {
    let now = u128::from(operation_facts.now_ms);
    // The provenance window's `ReceiptExpired` wrapper is crate-sealed
    // (`EnrollmentResponse::from_receipt_expired` is `pub(crate)`), so this
    // binding degrades the provenance window to the permanent lifetime
    // mapping: expired-receipt token replays answer `EnrollmentKnown`. This
    // is the closest crate-expressible row and is recorded as a residual gap
    // in the activation declaration.
    //
    // The gate reads the enrollment receipt's OWN deadline, fixed at enroll
    // commit: later attaches never re-open the generation-1 secret-bearing
    // receipt (secret receipts never outlive their signed TTL).
    let phase = if now < slot.enrollment_receipt_expires_at {
        EnrollmentTokenPhase::LiveReceipt {
            identity: ResolvedIdentity::<Digest, Digest, Digest>::Live(&slot.member),
            receipt: &slot.enrollment_receipt,
        }
    } else {
        EnrollmentTokenPhase::LifetimeMapping {
            identity: ResolvedIdentity::<Digest, Digest, Digest>::Live(&slot.member),
        }
    };
    let response = match lookup_enrollment(phase, &slot.binding, request) {
        EnrollmentLookupResult::EnrollmentKnown(value) => {
            EnrollmentResponse::enrollment_known(value)
        }
        EnrollmentLookupResult::Bound(_) => {
            EnrollmentResponse::bound(slot.enrollment_outcome.clone())
        }
        EnrollmentLookupResult::UnboundReceipt(_) => {
            EnrollmentResponse::unbound_receipt(slot.enrollment_outcome.clone())
        }
        EnrollmentLookupResult::Retired(_) => {
            return Err(StateError::invariant(
                "retired identity observed in a binding that mints no tombstones",
            ));
        }
        EnrollmentLookupResult::ReceiptExpired(_) => {
            return Err(StateError::invariant(
                "enrollment provenance phase is never constructed by this binding",
            ));
        }
        EnrollmentLookupResult::AuthorizedNew => {
            return Err(StateError::invariant(
                "mapped enrollment token classified as authorized-new",
            ));
        }
    };
    Ok(response.into_server_value())
}
