//! Enrollment arm of the production handler.
//!
//! Classification flows through the shared enrollment lookup, commits through
//! the crate's typed enrollment transition, mints the shell event through the
//! A3 aggregate commit, and answers through the request-bound response
//! authority. No lifecycle outcome is constructed here.
//!
//! Error contract: any [`StateError`] leaves durable state untouched (nothing
//! is published before the append succeeds) but may have consumed in-memory
//! authority. The handler therefore discards the whole in-memory conversation
//! owner on error and cold-replays durable reality on the next touch — the
//! same crash-consistency model the aggregate barrier is built for.

use liminal_protocol::lifecycle::{
    AggregateOperationDecision, AllocatedParticipantSlot, AttachedRecordPosition,
    BindingSlotDecision, BindingState, CapacityCounter, ClaimFrontiers,
    ConnectionConversationTracking, DetachCell, EnrollmentCapacityCounters, EnrollmentCommit,
    EnrollmentCommitParameters, EnrollmentFingerprint, EnrollmentLiveReceipt,
    EnrollmentLookupResult, EnrollmentProvenance, EnrollmentTokenPhase,
    FreshParticipantCapacityCounter, InitialEnrollmentCommitValues,
    InitialEnrollmentOperationDecision, InitialEnrollmentOperationInput, LiveFrontierError,
    LiveFrontierOwner, ParticipantSlotAllocatorProof, PrecedenceCondition, ResolvedIdentity,
    RetainedRecordCharge, SemanticConnectionCapacityDecision, apply_enrollment_frontier,
    apply_initial_enrollment, commit_enrollment, decide_enrolled_operation, lookup_enrollment,
    select_enrollment_binding_slot,
};
use liminal_protocol::wire::{
    AttachSecret, BindingEpoch, EnrollBound, EnrollmentEnvelope, EnrollmentRequest,
    EnrollmentResponse, Generation, ReceiptExpired as WireReceiptExpired, ReceiptExpiryReason,
    ServerValue,
};

use crate::config::types::ParticipantConfig;
use crate::server::participant::dispatch_impact::DispatchImpactAccumulator;

use super::barrier::{ArmOutcome, CommitMode, OperationFacts, commit_through_barrier};
use super::capacity::{ServerCapacity, Stage8Outcome};
use super::facts::{self, Digest};
use super::frontier;
use super::log::{StoredEnrollmentAllocation, StoredEnrollmentRequest, StoredOperation};
use super::outbox_projection::ReplayedProjectionFacts;
use super::presented_refusal::PresentedRefusal;
use super::state::{ConversationAuthority, DurableAppend, Slot, StateError};

/// Selects the lawful §0.16 answer for a subsequent enrollment blocked at the
/// seam.
///
/// The enrollment leg is where the amendment SPLITS. Condition 1 reuses the
/// register's existing enrollment `ObserverBackpressure` row, so it adds no wire
/// surface. Condition 2 gets `EnrollmentSettlementBackpressure` — conversation
/// id only, NO epoch, and NO pushed event of any kind — because this wrapper
/// carries no membership predicate and therefore holds no mechanism capable of
/// trading enrollee convenience against stranger disclosure. Conditions 3 and
/// Unclassified keep the pre-amendment bare close.
const fn enrollment_settlement_answer(
    condition: PrecedenceCondition,
    envelope: EnrollmentEnvelope,
    observer_progress: u64,
) -> Option<EnrollmentResponse> {
    match condition {
        PrecedenceCondition::BindingTerminal => Some(EnrollmentResponse::observer_backpressure(
            envelope,
            liminal_protocol::wire::ObserverBackpressureState::initial(observer_progress),
        )),
        PrecedenceCondition::MarkerDrain { .. } => {
            Some(EnrollmentResponse::settlement_backpressure(&envelope))
        }
        PrecedenceCondition::FencedRecovery | PrecedenceCondition::Unclassified => None,
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
    /// F8B R-SEAL (§6.6): a Closed conversation refuses enrollment NAMED and
    /// TYPED, ahead of every other classification.
    ///
    /// Falling through would mint a fresh identity on a log that already holds
    /// records, terminals and drain rows — a silent re-open of a conversation
    /// whose history has ended, which is the exact class of silent end this
    /// design forbids. Emitted before it travels, so the operator sees it even
    /// where an outer mapping reduces the error to its own phase text.
    fn refuse_enrollment_if_sealed(&self) -> Result<(), StateError> {
        if !self.is_closed() {
            return Ok(());
        }
        tracing::warn!(
            conversation_id = self.conversation_id,
            "F8B R-SEAL refused enrollment into a sealed conversation"
        );
        Err(StateError::ConversationSealed {
            conversation_id: self.conversation_id,
        })
    }

    /// Applies one enrollment request end to end.
    ///
    /// Every refusal (token replay, binding-slot occupancy) classifies over
    /// the replayed authority WITHOUT touching durable state; the durable
    /// shell genesis is minted only on the authorized-new arm, immediately
    /// before the enrollment's own committing append — a refused request on a
    /// never-seen conversation id leaves the durable store byte-identical.
    #[expect(
        clippy::too_many_lines,
        reason = "the amendment adds the position-allocator capture and its restore arm to an \
                  already-long arm; the capture must sit in the same function as the allocator \
                  it captures, or the pairing stops being checkable by reading"
    )]
    pub(super) fn apply_enrollment_with_impact(
        &mut self,
        request: &EnrollmentRequest,
        operation_facts: &OperationFacts,
        server_capacity: &ServerCapacity,
        config: &ParticipantConfig,
        appender: &dyn DurableAppend,
        impact: &mut DispatchImpactAccumulator,
    ) -> Result<ArmOutcome, StateError> {
        self.refuse_enrollment_if_sealed()?;
        let token_bytes = request.enrollment_token.into_bytes();
        if let Some(participant_id) = self.tokens.get(&token_bytes).copied() {
            let slot = self.slots.get(&participant_id).ok_or_else(|| {
                StateError::invariant("enrollment token maps to a missing participant slot")
            })?;
            return enrollment_replay_response(slot, request, operation_facts)
                .map(ArmOutcome::respond);
        }
        // Stage 6, first half: connection-conversation capacity for the
        // first semantic operation of an untracked conversation (register
        // row 5641), AFTER token-replay lookup and BEFORE the binding slot —
        // the crate's frozen order in `apply_initial_enrollment`.
        let capacity = match operation_facts.semantic_connection_capacity() {
            SemanticConnectionCapacityDecision::Commit(value) => value,
            SemanticConnectionCapacityDecision::Respond { limit } => {
                return Ok(ArmOutcome::respond(
                    EnrollmentResponse::connection_conversation_capacity_exceeded(
                        enrollment_envelope(request),
                        limit,
                    )
                    .into_server_value(),
                ));
            }
        };
        if let BindingSlotDecision::Respond(response) = select_enrollment_binding_slot(
            request,
            self.binding_slot_occupancy(operation_facts.receiving_incarnation),
        ) {
            return Ok(ArmOutcome::respond(response.into_server_value()));
        }
        // Stage 8 (R-D1): the complete runtime identity/receipt capacity
        // family in R-C0's seven-scope order — identity Server, identity
        // Conversation, LiveReceiptServer, ProvenanceServer, then
        // ProvenanceConversation can refuse; both per-participant scopes are
        // provably empty for the not-yet-minted identity. Decided BEFORE
        // genesis, secret mint, or any durable touch, so a refused request
        // provably mints nothing; the atomic check-and-reserve makes
        // concurrent enrollments on other conversations unable to admit past
        // a server scope.
        let deadlines = operation_facts.deadlines()?;
        let (reservation, enrollment_capacity) =
            match self.enrollment_stage8(request, operation_facts, server_capacity, &deadlines)? {
                Stage8Outcome::Refused(response) => {
                    return Ok(ArmOutcome::respond(response.into_server_value()));
                }
                Stage8Outcome::Reserved(reservation, capacity) => (reservation, capacity),
            };

        // The one conversation-creating arm: genesis is durable exactly when
        // an authorized enrollment is about to append its own entry.
        self.ensure_genesis(appender)?;
        let source_log_sequence = self.next_log_sequence;
        // §0.16 obligation 1: captured before the allocator runs, restored if
        // the subsequent-enrollment frontier transition presents a settlement
        // refusal.
        let captured_positions = self.position_allocators();
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
        let outcome = if self.frontier().is_none() {
            match self.initial_enrollment_operation(
                request,
                &allocation,
                operation_facts.connection_tracking,
                operation_facts.connection_capacity,
                enrollment_capacity,
                config,
            )? {
                InitialEnrollmentOperationDecision::Respond(response) => {
                    return Ok(ArmOutcome::respond(response.into_server_value()));
                }
                InitialEnrollmentOperationDecision::Fault(fault) => {
                    return Err(StateError::invariant(format!(
                        "initial enrollment selector fault: {fault:?}"
                    )));
                }
                InitialEnrollmentOperationDecision::Commit(operation) => self
                    .commit_initial_enrollment(
                        *operation,
                        request,
                        &allocation,
                        config,
                        CommitMode::Live(appender),
                    )?,
            }
        } else {
            match self.enroll_commit(request, &allocation, CommitMode::Live(appender)) {
                Ok(outcome) => outcome,
                Err(error) => {
                    if matches!(error, StateError::PresentedRefusal(_)) {
                        self.restore_position_allocators(captured_positions);
                    }
                    return Err(error);
                }
            }
        };
        let source = StoredOperation::Enrolled {
            request: request.into(),
            allocation,
            event: Vec::new(),
        };
        self.record_produced_source(
            source_log_sequence,
            &source,
            ReplayedProjectionFacts::none(),
            appender,
            impact,
        )?;
        self.record_binding_changed(allocation.participant_id, impact);
        self.record_episode_changed(impact);
        // The durable append succeeded: the stage-8 reservation becomes
        // permanent (enrollment retires no earlier receipt).
        reservation.confirm(&[], &[]);
        Ok(ArmOutcome::committed(
            EnrollmentResponse::enroll_bound(outcome).into_server_value(),
            capacity,
        ))
    }

    fn initial_enrollment_operation(
        &self,
        request: &EnrollmentRequest,
        allocation: &StoredEnrollmentAllocation,
        connection_tracking: ConnectionConversationTracking,
        connection_capacity: CapacityCounter,
        enrollment_capacity: EnrollmentCapacityCounters,
        config: &ParticipantConfig,
    ) -> Result<InitialEnrollmentOperationDecision<Digest>, StateError> {
        let attached_charge = frontier::attached_charge(request.conversation_id, allocation)?;
        let closure = frontier::initial_closure_input(config, allocation, attached_charge)?;
        let deadlines = liminal_protocol::lifecycle::ReceiptDeadlines::try_from_absolute(
            allocation.receipt_expires_at.get(),
            allocation.provenance_expires_at.get(),
        )
        .map_err(|error| {
            StateError::invariant(format!(
                "stored enrollment deadlines are invalid: {error:?}"
            ))
        })?;
        let binding = BindingState::Detached;
        let input: InitialEnrollmentOperationInput<'_, Digest, Digest, Digest> =
            InitialEnrollmentOperationInput::new(
                request,
                EnrollmentTokenPhase::Unmapped,
                &binding,
                connection_tracking,
                connection_capacity,
                self.binding_slot_occupancy(
                    allocation.origin_epoch.to_epoch()?.connection_incarnation,
                ),
                enrollment_capacity,
                closure,
            );
        Ok(apply_initial_enrollment(
            &input,
            || {
                InitialEnrollmentCommitValues::new(
                    AttachSecret::new(allocation.attach_secret),
                    deadlines,
                    EnrollmentFingerprint::new(allocation.enrollment_fingerprint),
                )
            },
            || {
                AllocatedParticipantSlot::from_allocator(ServerSlotProof {
                    conversation_id: request.conversation_id,
                    participant_id: allocation.participant_id,
                    identity_limit: allocation.identity_limit,
                })
            },
        ))
    }

    fn commit_initial_enrollment(
        &mut self,
        operation: liminal_protocol::lifecycle::InitialEnrollmentOperationCommit<Digest>,
        request: &EnrollmentRequest,
        allocation: &StoredEnrollmentAllocation,
        config: &ParticipantConfig,
        mode: CommitMode<'_>,
    ) -> Result<EnrollBound, StateError> {
        let attached_charge = frontier::attached_charge(request.conversation_id, allocation)?;
        let initial = ClaimFrontiers::from_initial_enrollment(operation, attached_charge).map_err(
            |failure| {
                StateError::invariant(format!(
                    "initial frontier acquisition failed: {:?}",
                    failure.error()
                ))
            },
        )?;
        let (operation, owner) =
            LiveFrontierOwner::from_initial_enrollment(initial, config.max_retained_record_rows);
        self.publish_enrollment(
            operation.into_enrollment(),
            owner,
            request,
            allocation,
            mode,
        )
    }

    /// Replays one committed enrollment entry from its stored inputs.
    pub(super) fn replay_enrolled(
        &mut self,
        request: StoredEnrollmentRequest,
        allocation: &StoredEnrollmentAllocation,
        stored_event: &[u8],
        sequence: u64,
        config: &ParticipantConfig,
    ) -> Result<(), StateError> {
        let request = request.to_request();
        let mode = CommitMode::Replay {
            stored_event,
            sequence,
        };
        if self.frontier().is_none() {
            let decision = self.initial_enrollment_operation(
                &request,
                allocation,
                ConnectionConversationTracking::Untracked,
                replay_counter(config.max_semantic_conversations_per_connection)?,
                replay_enrollment_capacity(config)?,
                config,
            )?;
            let InitialEnrollmentOperationDecision::Commit(operation) = decision else {
                return Err(StateError::invariant(
                    "durable initial enrollment was refused during protocol replay",
                ));
            };
            self.commit_initial_enrollment(*operation, &request, allocation, config, mode)?;
        } else {
            self.enroll_commit(&request, allocation, mode)?;
        }
        Ok(())
    }

    /// Shared enrollment commit core for the live and replay paths.
    fn enroll_commit(
        &mut self,
        request: &EnrollmentRequest,
        allocation: &StoredEnrollmentAllocation,
        mode: CommitMode<'_>,
    ) -> Result<EnrollBound, StateError> {
        let presenting = matches!(mode, CommitMode::Live(_));
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
        let encoded_charge = frontier::attached_charge(request.conversation_id, allocation)?;
        let charge = RetainedRecordCharge::new(
            committed.attached.delivery_seq(),
            committed.attached.admission_order(),
            encoded_charge,
        );
        let transitioned = match apply_enrollment_frontier(self.take_frontier()?, committed, charge)
        {
            Ok(transitioned) => transitioned,
            Err(failure) => {
                let error = failure.error();
                // Participant contract §0.16 condition 2, ENROLLMENT
                // wrapper. The row carries NO epoch and gets NO wake of any
                // kind, by ratified law rather than by omission: an
                // `EnrollmentRequest` is `{ conversation_id,
                // enrollment_token }` and that token is a replay-dedup key,
                // not a capability, so this wrapper cannot tell an invited
                // enrollee from a stranger and any label or wake would be
                // granted to both by construction. The enroller retries at
                // its own cadence.
                //
                // Condition 1 reuses the register's existing enrollment
                // `ObserverBackpressure` row, which adds no wire surface
                // anywhere. Conditions 3/Unclassified keep the bare close.
                //
                // ⛔ REPLAY IS NEVER PRESENTED.
                let answer = match (presenting, error) {
                    (true, LiveFrontierError::Precedence(condition)) => {
                        enrollment_settlement_answer(
                            condition,
                            enrollment_envelope(request),
                            self.observer_progress,
                        )
                    }
                    _ => None,
                };
                let Some(response) = answer else {
                    return Err(StateError::invariant(format!(
                        "subsequent enrollment frontier transition failed: {error:?}"
                    )));
                };
                // §0.16 obligation 1. `enroll_commit` consumed the frontier
                // and nothing else — the slot is inserted later, in
                // `publish_enrollment` — so the frontier goes straight back
                // and the arm restores the position allocators. Every
                // refusal arm on this wrapper returns before the
                // enrollment's own durable append, so the durable store is
                // byte-identical across the refusal.
                let (_, owner) = failure.into_parts();
                self.install_frontier(owner)?;
                return Err(StateError::PresentedRefusal(PresentedRefusal::enrollment(
                    response,
                )));
            }
        };
        let (committed, owner) = transitioned.into_parts();
        self.publish_enrollment(committed, owner, request, allocation, mode)
    }

    fn publish_enrollment(
        &mut self,
        committed: EnrollmentCommit<Digest>,
        owner: LiveFrontierOwner,
        request: &EnrollmentRequest,
        allocation: &StoredEnrollmentAllocation,
        mode: CommitMode<'_>,
    ) -> Result<EnrollBound, StateError> {
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
        let outcome = committed.outcome.clone();
        self.shell = Some(shell);
        self.install_frontier(owner)?;
        self.advance_log_head()?;
        self.slots.insert(
            allocation.participant_id,
            Slot {
                member: committed.member,
                binding: committed.binding_state,
                binding_fate: None,
                cell: DetachCell::default(),
                enrollment_receipt: EnrollmentLiveReceipt::from_commit(outcome.clone()),
                enrollment_outcome: committed.outcome,
                enrollment_receipt_expires_at: allocation.receipt_expires_at.get(),
                enrollment_provenance_expires_at: allocation.provenance_expires_at.get(),
                enrollment_receipt_ended: None,
                enrollment_provenance_displaced: false,
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
        self.observe_replayed_position(allocation.attached_order, allocation.attached_seq)?;
        Ok(outcome)
    }
}

fn replay_counter(limit: u64) -> Result<CapacityCounter, StateError> {
    CapacityCounter::try_new(limit, 0).map_err(|error| {
        StateError::invariant(format!(
            "validated replay capacity limit is invalid: {error:?}"
        ))
    })
}

fn replay_fresh_counter(limit: u64) -> Result<FreshParticipantCapacityCounter, StateError> {
    FreshParticipantCapacityCounter::try_new(limit, 0).map_err(|error| {
        StateError::invariant(format!(
            "validated fresh-participant replay capacity is invalid: {error:?}"
        ))
    })
}

fn replay_enrollment_capacity(
    config: &ParticipantConfig,
) -> Result<EnrollmentCapacityCounters, StateError> {
    Ok(EnrollmentCapacityCounters::new(
        replay_counter(config.max_retired_identity_slots_server)?,
        replay_counter(config.identity_slots)?,
        replay_fresh_counter(config.max_live_attach_receipts_per_participant)?,
        replay_fresh_counter(config.max_receipt_provenance_per_participant)?,
    ))
}

/// Builds the enrollment replay/known response for a mapped token.
fn enrollment_replay_response(
    slot: &Slot,
    request: &EnrollmentRequest,
    operation_facts: &OperationFacts,
) -> Result<ServerValue, StateError> {
    let now = u128::from(operation_facts.now_ms);
    // Three token phases against the enrollment receipt's OWN deadline pair,
    // fixed at enroll commit. The receipt body ends EITHER by its own
    // deadline OR when a committed credential attach mints a newer
    // generation (contract R-C0 supersession: the invalidated generation-1
    // secret payload is never re-served once rotation ended it); the
    // non-secret provenance record then explains the ended receipt with its
    // exact terminal reason through its own deadline.
    //
    // Lane p0-39 — the DOCUMENTED classification degradation. The provenance
    // phase also ends early when this participant's own retention window
    // displaced its enrollment fingerprint to make room for a newer one of
    // the SAME participant. The exact terminal reason is then gone, and the
    // permanent lifetime mapping answers `EnrollmentKnown` instead: a
    // strictly coarser, still-truthful answer. Note the displacement test is
    // deliberately NOT the occupancy predicate — occupancy additionally
    // requires proven possession (board #37), while a receipt that died by
    // its own deadline with no attach at all still classifies `Deadline`
    // here, exactly as it always has.
    let identity = ResolvedIdentity::<Digest, Digest, Digest>::Live(&slot.member);
    let receipt_live =
        slot.enrollment_receipt_ended.is_none() && now < slot.enrollment_receipt_expires_at;
    let phase = if receipt_live {
        EnrollmentTokenPhase::LiveReceipt {
            identity,
            receipt: &slot.enrollment_receipt,
        }
    } else if !slot.enrollment_provenance_displaced && now < slot.enrollment_provenance_expires_at {
        EnrollmentTokenPhase::Provenance {
            identity,
            provenance: EnrollmentProvenance::new(
                slot.enrollment_outcome.capability_generation(),
                slot.enrollment_receipt_ended
                    .unwrap_or(ReceiptExpiryReason::Deadline),
            ),
        }
    } else {
        EnrollmentTokenPhase::LifetimeMapping { identity }
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
        EnrollmentLookupResult::ReceiptExpired(value) => {
            // Every classified fact travels FROM the crate's lookup value
            // into the request-bound response authority — the same pattern as
            // the credential-attach provenance arm.
            let WireReceiptExpired::Enrollment {
                participant_id,
                result_generation,
                current_generation,
                reason,
                ..
            } = value
            else {
                return Err(StateError::invariant(
                    "credential-attach provenance row observed in the enrollment lookup",
                ));
            };
            EnrollmentResponse::receipt_expired(
                &enrollment_envelope(request),
                participant_id,
                result_generation,
                current_generation,
                reason,
            )
        }
        EnrollmentLookupResult::Retired(_) => {
            return Err(StateError::invariant(
                "retired identity observed in a binding that mints no tombstones",
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

/// Builds the echo envelope of one enrollment request.
pub(super) const fn enrollment_envelope(request: &EnrollmentRequest) -> EnrollmentEnvelope {
    EnrollmentEnvelope {
        conversation_id: request.conversation_id,
        enrollment_token: request.enrollment_token,
    }
}
