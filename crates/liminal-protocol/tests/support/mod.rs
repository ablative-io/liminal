use std::vec;

use liminal_protocol::{
    algebra::{ResourceVector, WideResourceVector},
    lifecycle::{
        ActiveBinding, AdmissionOrder, AttachCommitParameters, AttachSecretProof,
        AttachedRecordPosition, BindingFateTerminal, BindingState, BindingTerminalOwner,
        ClaimFrontiers, ClaimFrontiersRestore, ClosureAccounting, ClosureState,
        CommittedBindingTerminalPosition, DetachCell, EnrollmentFingerprint, Event,
        ExitProductRangeRestore, FencedAttachCommit, FrontierBinding, FrontierParticipant,
        ImmutableOrderCandidateMajorRestore, ImmutableSequenceCandidate, LiveFrontierOwner,
        LiveMember, LiveMemberRestore, MarkerCandidateAuthority, MarkerDelivery, MarkerProvenance,
        MarkerSequenceOwner, MeasuredBindingFate, MintFencedAttachResult, MovableOrderClaim,
        MovableSequenceClaim, OrderClaimFrontierRestore, OrderClaims, OrderDirectOwner, OrderHigh,
        OrderLedger, PendingFinalization, PrepareLeaveAuthorityError, PreparedLeaveAuthority,
        RecoveredBindingFate, RecoverySequenceReserve, RetainedCausalRecord,
        RetainedCausalRecordKind, RetainedRecordCharge, SequenceClaimFrontierRestore,
        SequenceClaims, SequenceDirectOwner, SequenceLedger, SequenceProductRangesRestore,
        StoredEdge, TerminalProductRangeRestore, commit_attach, drain_next_marker,
    },
    outcome::CandidatePhase,
    wire::{
        AttachAttemptToken, AttachSecret, BindingEpoch, ConnectionIncarnation,
        CredentialAttachRequest,
    },
};

pub fn fenced_owner(
    recovery: liminal_protocol::lifecycle::DetachedCredentialRecovery,
) -> Result<LiveFrontierOwner, String> {
    let marker_delivery_seq = recovery.marker_delivery_seq();
    let cursor = marker_delivery_seq
        .checked_sub(1)
        .ok_or_else(|| "fenced owner fixture requires a positive marker".to_owned())?;
    restored_marker_owner(
        recovery.participant_id(),
        FrontierBinding::Detached(recovery.prior_binding_epoch()),
        marker_delivery_seq,
        cursor,
    )
}

fn restored_marker_owner(
    participant_id: u64,
    binding: FrontierBinding,
    marker_delivery_seq: u64,
    cursor: u64,
) -> Result<LiveFrontierOwner, String> {
    let (frontier_restore, sequence_ledger, order_ledger) =
        marker_frontier(participant_id, binding, marker_delivery_seq, cursor)?;
    let frontiers = ClaimFrontiers::restore(frontier_restore, sequence_ledger, order_ledger)
        .map_err(|error| format!("marker owner frontier failed to restore: {error:?}"))?;
    let retained_charges = frontiers
        .retained_records()
        .iter()
        .map(|record| {
            RetainedRecordCharge::new(
                record.delivery_seq,
                record.admission_order,
                ResourceVector::new(1, 1),
            )
        })
        .collect();
    let candidate = frontiers
        .sequence()
        .immutable_candidates()
        .first()
        .copied()
        .ok_or_else(|| "marker owner frontier had no marker candidate".to_owned())?;
    let marker_charge = RetainedRecordCharge::new(
        candidate.delivery_seq(),
        candidate.admission_order(),
        ResourceVector::new(1, 1),
    );
    let accounting = ClosureAccounting::try_new(
        ClosureState::Clear,
        1,
        1,
        0,
        0,
        ResourceVector::default(),
        WideResourceVector::new(1, 1),
        ResourceVector::new(100, 100),
        0,
        2,
    )
    .map_err(|error| format!("marker owner accounting failed: {error:?}"))?;
    let existing_retained = u64::try_from(frontiers.retained_records().len())
        .map_err(|_| "marker owner retained-row count exceeds u64".to_owned())?;
    let retained_record_limit = existing_retained
        .checked_add(2)
        .ok_or_else(|| "marker owner retained-row limit overflow".to_owned())?;
    let commit = drain_next_marker(frontiers, accounting, retained_charges, marker_charge)
        .map_err(|error| format!("marker owner failed to drain: {error:?}"))?;
    let (owner, _, _) = LiveFrontierOwner::from_marker_drain(commit, retained_record_limit);
    Ok(owner)
}

pub fn mint_fenced_attach_with_owner(
    recovery: liminal_protocol::lifecycle::DetachedCredentialRecovery,
    debt: liminal_protocol::lifecycle::ClosureDebt,
    event: Event,
    successor: liminal_protocol::lifecycle::DebtCompletion,
) -> Result<(LiveFrontierOwner, FencedAttachCommit), String> {
    let marker_delivery_seq = recovery.marker_delivery_seq();
    let owner = fenced_owner(recovery)?;
    match owner.mint_fenced_attach(marker_delivery_seq, recovery, debt, event, successor) {
        MintFencedAttachResult::Minted(minted) => {
            let (spent_owner, proof) = minted.into_parts();
            Ok((spent_owner, proof))
        }
        MintFencedAttachResult::MintRefused(refused) => {
            Err(format!("fenced owner mint refused: {:?}", refused.reason()))
        }
    }
}

pub fn recovered_fate_from_fenced(
    owner: LiveFrontierOwner,
    proof: FencedAttachCommit,
    expected_resulting_floor: u64,
) -> Result<RecoveredBindingFate, String> {
    let participant_id = proof.participant_id();
    let conversation_id = proof.conversation_id();
    let prior_binding_epoch = proof.prior_binding_epoch();
    let new_binding_epoch = proof.new_binding_epoch();
    let marker_delivery_seq = proof.marker_delivery_seq();
    let terminal_sequence = marker_delivery_seq
        .checked_sub(1)
        .ok_or_else(|| "fenced fixture marker has no lower terminal position".to_owned())?;
    let attached_sequence = marker_delivery_seq
        .checked_add(1)
        .ok_or_else(|| "fenced fixture marker cannot allocate Attached position".to_owned())?;
    let active = ActiveBinding {
        participant_id,
        conversation_id,
        binding_epoch: prior_binding_epoch,
    };
    let latest_terminal = active
        .commit_clean_deregister(CommittedBindingTerminalPosition::new(1, terminal_sequence))
        .into();
    let prior_secret = AttachSecret::new([0xA5; 32]);
    let member = LiveMember::restore(LiveMemberRestore {
        participant_id,
        conversation_id,
        generation: prior_binding_epoch.capability_generation,
        attach_secret: prior_secret,
        cursor: marker_delivery_seq,
        enrollment_fingerprint: EnrollmentFingerprint::new(vec![0xA5]),
        latest_terminal: Some(latest_terminal),
    })
    .map_err(|error| format!("fenced fixture member restore failed: {error:?}"))?;
    let verified = member
        .verify_fenced_attach(
            BindingState::Detached,
            CredentialAttachRequest {
                conversation_id,
                participant_id,
                capability_generation: prior_binding_epoch.capability_generation,
                attach_secret: prior_secret,
                attach_attempt_token: AttachAttemptToken::new([0xA6; 16]),
                accept_marker_delivery_seq: Some(marker_delivery_seq),
            },
            AttachSecretProof::Verified,
            proof,
            None,
            AttachCommitParameters {
                binding: ActiveBinding {
                    participant_id,
                    conversation_id,
                    binding_epoch: new_binding_epoch,
                },
                attach_secret: AttachSecret::new([0xA7; 32]),
                attached_position: AttachedRecordPosition::new(2, attached_sequence),
                receipt_expires_at: 1,
                provenance_expires_at: 2,
            },
        )
        .map_err(|failure| format!("fenced fixture verify failed: {:?}", failure.error()))?;
    let committed = commit_attach(verified, DetachCell::<[u8; 32]>::default())
        .map_err(|error| format!("fenced fixture commit failed: {error:?}"))?;
    drop(owner);
    let (_, token) = committed.into_slot_and_fate();
    let next_marker = marker_delivery_seq
        .checked_add(1)
        .ok_or_else(|| "recovered fixture owner marker overflow".to_owned())?;
    let owner_marker = next_marker.max(expected_resulting_floor);
    let owner = restored_marker_owner(
        participant_id,
        FrontierBinding::Detached(new_binding_epoch),
        owner_marker,
        marker_delivery_seq,
    )?;
    let hard_observer_progress = expected_resulting_floor
        .checked_sub(1)
        .ok_or_else(|| "recovered fixture resulting floor has no predecessor".to_owned())?;
    let prepared = owner
        .prepare_binding_fate(
            token,
            BindingFateTerminal::Recovered,
            hard_observer_progress,
        )
        .map_err(|refused| {
            format!(
                "fenced fixture measured selector refused: {:?}",
                refused.error()
            )
        })?;
    let (owner, fate, event) = prepared.into_parts();
    drop(owner);
    let _ = event;
    match fate {
        MeasuredBindingFate::Recovered(fate)
            if fate.resulting_floor() == expected_resulting_floor =>
        {
            Ok(fate)
        }
        MeasuredBindingFate::Recovered(_) | MeasuredBindingFate::Ordinary(_) => {
            Err("fenced fixture selector returned the wrong measured fate".to_owned())
        }
    }
}

pub fn marker_delivery(
    participant_id: u64,
    binding_epoch: BindingEpoch,
    marker_delivery_seq: u64,
) -> Result<MarkerDelivery, String> {
    let (frontier_restore, sequence_ledger, order_ledger) = marker_frontier(
        participant_id,
        FrontierBinding::Bound(binding_epoch),
        marker_delivery_seq,
        marker_delivery_seq.saturating_sub(1),
    )?;
    let frontiers = ClaimFrontiers::restore(frontier_restore, sequence_ledger, order_ledger)
        .map_err(|error| format!("planned marker frontier failed to restore: {error:?}"))?;
    let retained_charges = frontiers
        .retained_records()
        .iter()
        .map(|record| {
            RetainedRecordCharge::new(
                record.delivery_seq,
                record.admission_order,
                ResourceVector::new(1, 1),
            )
        })
        .collect();
    let candidate = frontiers
        .sequence()
        .immutable_candidates()
        .first()
        .copied()
        .ok_or_else(|| "planned marker frontier had no candidate".to_owned())?;
    let marker_charge = RetainedRecordCharge::new(
        candidate.delivery_seq(),
        candidate.admission_order(),
        ResourceVector::new(1, 1),
    );
    let accounting = ClosureAccounting::try_new(
        ClosureState::Clear,
        1,
        1,
        0,
        0,
        ResourceVector::default(),
        WideResourceVector::new(1, 1),
        ResourceVector::new(100, 100),
        0,
        2,
    )
    .map_err(|error| format!("marker accounting fixture failed: {error:?}"))?;
    let commit = drain_next_marker(frontiers, accounting, retained_charges, marker_charge)
        .map_err(|error| format!("planned marker failed to drain: {error:?}"))?;
    let successor = commit.marker_successor();
    let _persisted_parts = commit.into_parts();
    let StoredEdge::MarkerDelivery(delivery) = successor else {
        return Err("bound marker transition selected a different edge".to_owned());
    };
    Ok(delivery)
}

struct SettledBindingFixture {
    binding: FrontierBinding,
    terminal: Option<BindingTerminalOwner>,
    first_order: u64,
}

struct SettledClaimsFixture {
    sequence: Vec<MovableSequenceClaim>,
    order: Vec<MovableOrderClaim>,
    products: SequenceProductRangesRestore,
    active_count: u64,
}

fn settled_binding_fixture<F>(
    member: &LiveMember<F>,
    binding_state: BindingState,
    left_transaction_order: u64,
) -> Result<SettledBindingFixture, String> {
    let participant_id = member.participant_id();
    match binding_state {
        BindingState::Bound(binding)
            if binding.conversation_id == member.conversation_id()
                && binding.participant_id == participant_id =>
        {
            Ok(SettledBindingFixture {
                binding: FrontierBinding::Bound(binding.binding_epoch),
                terminal: Some(BindingTerminalOwner {
                    participant_index: participant_id,
                    binding_epoch: binding.binding_epoch,
                }),
                first_order: left_transaction_order
                    .checked_sub(1)
                    .ok_or_else(|| "bound Leave fixture needs one A major before X".to_owned())?,
            })
        }
        BindingState::Detached => Ok(SettledBindingFixture {
            binding: FrontierBinding::Detached(member.latest_terminal().map_or_else(
                || BindingEpoch::new(ConnectionIncarnation::new(1, 1), member.generation()),
                liminal_protocol::lifecycle::CommittedBindingTerminal::binding_epoch,
            )),
            terminal: None,
            first_order: left_transaction_order,
        }),
        BindingState::Bound(_) => Err("bound Leave fixture mismatches its member".to_owned()),
        BindingState::PendingFinalization(_) => {
            Err("settled fixture cannot carry pending finalization".to_owned())
        }
    }
}

fn settled_claims_fixture(
    participant_id: u64,
    exit_seq: u64,
    left_transaction_order: u64,
    fixture: &SettledBindingFixture,
) -> Result<SettledClaimsFixture, String> {
    let mut sequence = vec![MovableSequenceClaim {
        delivery_seq: exit_seq,
        owner: SequenceDirectOwner::MembershipExit {
            participant_index: participant_id,
        },
    }];
    let mut order = vec![MovableOrderClaim {
        transaction_order: left_transaction_order,
        owner: OrderDirectOwner::MembershipExit {
            participant_index: participant_id,
        },
    }];
    let mut products = SequenceProductRangesRestore::default();
    let active_count = if let Some(owner) = fixture.terminal {
        let terminal_seq = exit_seq
            .checked_add(1)
            .ok_or_else(|| "bound fixture has no T suffix".to_owned())?;
        sequence.push(MovableSequenceClaim {
            delivery_seq: terminal_seq,
            owner: SequenceDirectOwner::BindingTerminal(owner),
        });
        products.live_times_terminal = vec![TerminalProductRangeRestore {
            start: terminal_seq
                .checked_add(1)
                .ok_or_else(|| "bound fixture has no LxT suffix".to_owned())?,
            length: 1,
            terminal: owner,
        }];
        order.push(MovableOrderClaim {
            transaction_order: fixture.first_order,
            owner: OrderDirectOwner::ActiveBindingTerminal(owner),
        });
        1
    } else {
        0
    };
    order.sort_by_key(|claim| claim.transaction_order);
    Ok(SettledClaimsFixture {
        sequence,
        order,
        products,
        active_count,
    })
}

pub fn settled_leave_authority<F>(
    member: &LiveMember<F>,
    binding_state: BindingState,
    left_transaction_order: u64,
    left_delivery_seq: u64,
) -> Result<PreparedLeaveAuthority, String> {
    let participant_id = member.participant_id();
    let fixture = settled_binding_fixture(member, binding_state, left_transaction_order)?;
    let high_watermark = left_delivery_seq
        .checked_sub(1)
        .ok_or_else(|| "settled Leave requires a positive delivery sequence".to_owned())?;
    let exit_seq = high_watermark
        .checked_add(1)
        .ok_or_else(|| "settled fixture has no E suffix".to_owned())?;
    let claims =
        settled_claims_fixture(participant_id, exit_seq, left_transaction_order, &fixture)?;
    let sequence = SequenceLedger::try_new(
        high_watermark,
        SequenceClaims::new(1, claims.active_count, 0, RecoverySequenceReserve::None),
    )
    .map_err(|error| format!("settled sequence ledger invalid: {error:?}"))?;
    let order = OrderLedger::try_new(
        high_before(fixture.first_order),
        OrderClaims::new(claims.active_count, 1, false, false)
            .map_err(|error| format!("settled order claims invalid: {error:?}"))?,
    )
    .map_err(|error| format!("settled order ledger invalid: {error:?}"))?;
    let frontiers = ClaimFrontiers::restore(
        ClaimFrontiersRestore {
            conversation_id: member.conversation_id(),
            active_identities: vec![FrontierParticipant::new(
                participant_id,
                member.cursor(),
                fixture.binding,
            )],
            identity_slot_limit: participant_id
                .checked_add(1)
                .ok_or_else(|| "participant has no half-open identity cap".to_owned())?,
            retained_floor: u128::from(high_watermark) + 1,
            retained_record_limit: 0,
            retained_records: vec![],
            active_marker_anchors: vec![],
            historical_marker_deliveries: vec![],
            historical_causal_facts: vec![],
            sequence: SequenceClaimFrontierRestore {
                movable_claims: claims.sequence,
                immutable_candidates: vec![],
                products: claims.products,
                recovery: None,
            },
            order: OrderClaimFrontierRestore {
                movable_claims: claims.order,
                immutable_candidates: vec![],
                recovery: None,
            },
            recovery_marker_delivery_seq: None,
        },
        sequence,
        order,
    )
    .map_err(|error| format!("settled frontiers invalid: {error:?}"))?;
    frontiers
        .prepare_settled_leave_authority(member, binding_state)
        .map_err(|error| format!("settled Leave authority refused: {error:?}"))
}

#[allow(dead_code)]
pub fn pending_leave_authority<F>(
    member: &LiveMember<F>,
    pending: PendingFinalization,
    terminal_delivery_seq: u64,
    left_transaction_order: u64,
) -> Result<PreparedLeaveAuthority, String> {
    let participant_id = member.participant_id();
    let terminal_order = pending.admission_order();
    let terminal_owner = BindingTerminalOwner {
        participant_index: participant_id,
        binding_epoch: pending.binding_epoch(),
    };
    let exit_seq = terminal_delivery_seq
        .checked_add(1)
        .ok_or_else(|| "pending fixture has no E suffix".to_owned())?;
    let product_seq = exit_seq
        .checked_add(1)
        .ok_or_else(|| "pending fixture has no LxT suffix".to_owned())?;
    let high_watermark = terminal_delivery_seq
        .checked_sub(1)
        .ok_or_else(|| "pending fixture requires a positive terminal sequence".to_owned())?;
    let sequence = SequenceLedger::try_new(
        high_watermark,
        SequenceClaims::new(1, 1, 0, RecoverySequenceReserve::None),
    )
    .map_err(|error| format!("pending sequence ledger invalid: {error:?}"))?;
    let order = OrderLedger::try_new(
        high_before(terminal_order.transaction_order()),
        OrderClaims::new(0, 1, false, false)
            .map_err(|error| format!("pending order claims invalid: {error:?}"))?,
    )
    .map_err(|error| format!("pending order ledger invalid: {error:?}"))?;
    let frontiers = ClaimFrontiers::restore(
        ClaimFrontiersRestore {
            conversation_id: member.conversation_id(),
            active_identities: vec![FrontierParticipant::new(
                participant_id,
                member.cursor(),
                FrontierBinding::Detached(pending.binding_epoch()),
            )],
            identity_slot_limit: participant_id
                .checked_add(1)
                .ok_or_else(|| "participant has no half-open identity cap".to_owned())?,
            retained_floor: u128::from(high_watermark) + 1,
            retained_record_limit: 0,
            retained_records: vec![],
            active_marker_anchors: vec![],
            historical_marker_deliveries: vec![],
            historical_causal_facts: vec![],
            sequence: SequenceClaimFrontierRestore {
                movable_claims: vec![MovableSequenceClaim {
                    delivery_seq: exit_seq,
                    owner: SequenceDirectOwner::MembershipExit {
                        participant_index: participant_id,
                    },
                }],
                immutable_candidates: vec![ImmutableSequenceCandidate::BindingTerminal {
                    delivery_seq: terminal_delivery_seq,
                    admission_order: terminal_order,
                    owner: terminal_owner,
                }],
                products: SequenceProductRangesRestore {
                    live_times_terminal: vec![TerminalProductRangeRestore {
                        start: product_seq,
                        length: 1,
                        terminal: terminal_owner,
                    }],
                    live_times_replacement_terminal: None,
                    other_live_times_exit: vec![],
                },
                recovery: None,
            },
            order: OrderClaimFrontierRestore {
                movable_claims: vec![MovableOrderClaim {
                    transaction_order: left_transaction_order,
                    owner: OrderDirectOwner::MembershipExit {
                        participant_index: participant_id,
                    },
                }],
                immutable_candidates: vec![ImmutableOrderCandidateMajorRestore {
                    transaction_order: terminal_order.transaction_order(),
                    candidate_keys: vec![terminal_order],
                }],
                recovery: None,
            },
            recovery_marker_delivery_seq: None,
        },
        sequence,
        order,
    )
    .map_err(|error| format!("pending frontiers invalid: {error:?}"))?;
    frontiers
        .prepare_pending_leave_authority(member, pending)
        .map_err(|error| format!("pending Leave authority refused: {error:?}"))
}

#[allow(dead_code, clippy::too_many_lines)]
pub fn intervening_pending_leave_refusal<F>(
    member: &LiveMember<F>,
    pending: PendingFinalization,
    unrelated: PendingFinalization,
    terminal_delivery_seq: u64,
    left_transaction_order: u64,
) -> Result<PrepareLeaveAuthorityError, String> {
    if pending.conversation_id() != unrelated.conversation_id()
        || pending.conversation_id() != member.conversation_id()
        || pending.participant_id() == unrelated.participant_id()
    {
        return Err("intervening fixture requires two identities in one conversation".to_owned());
    }
    let p = pending.participant_id();
    let u = unrelated.participant_id();
    let p_order = pending.admission_order();
    let u_order = unrelated.admission_order();
    if p_order.transaction_order().checked_add(1) != Some(u_order.transaction_order())
        || u_order.transaction_order().checked_add(1) != Some(left_transaction_order)
    {
        return Err(
            "intervening fixture requires adjacent P-terminal/U-terminal/X majors".to_owned(),
        );
    }
    let u_terminal_seq = terminal_delivery_seq
        .checked_add(1)
        .ok_or_else(|| "intervening terminal sequence overflows".to_owned())?;
    let p_exit_seq = u_terminal_seq
        .checked_add(1)
        .ok_or_else(|| "intervening P exit sequence overflows".to_owned())?;
    let u_exit_seq = p_exit_seq
        .checked_add(1)
        .ok_or_else(|| "intervening U exit sequence overflows".to_owned())?;
    let p_product_start = u_exit_seq
        .checked_add(1)
        .ok_or_else(|| "intervening product suffix overflows".to_owned())?;
    let u_product_start = p_product_start
        .checked_add(2)
        .ok_or_else(|| "intervening product suffix overflows".to_owned())?;
    let p_other_start = u_product_start
        .checked_add(2)
        .ok_or_else(|| "intervening exit-product suffix overflows".to_owned())?;
    let u_other_start = p_other_start
        .checked_add(1)
        .ok_or_else(|| "intervening exit-product suffix overflows".to_owned())?;
    let p_owner = BindingTerminalOwner {
        participant_index: p,
        binding_epoch: pending.binding_epoch(),
    };
    let u_owner = BindingTerminalOwner {
        participant_index: u,
        binding_epoch: unrelated.binding_epoch(),
    };
    let sequence = SequenceLedger::try_new(
        terminal_delivery_seq
            .checked_sub(1)
            .ok_or_else(|| "intervening fixture requires positive terminal sequence".to_owned())?,
        SequenceClaims::new(2, 2, 0, RecoverySequenceReserve::None),
    )
    .map_err(|error| format!("intervening sequence ledger invalid: {error:?}"))?;
    let order = OrderLedger::try_new(
        high_before(p_order.transaction_order()),
        OrderClaims::new(0, 2, false, false)
            .map_err(|error| format!("intervening order claims invalid: {error:?}"))?,
    )
    .map_err(|error| format!("intervening order ledger invalid: {error:?}"))?;
    let frontiers = ClaimFrontiers::restore(
        ClaimFrontiersRestore {
            conversation_id: member.conversation_id(),
            active_identities: vec![
                FrontierParticipant::new(
                    p,
                    member.cursor(),
                    FrontierBinding::Detached(pending.binding_epoch()),
                ),
                FrontierParticipant::new(
                    u,
                    member.cursor(),
                    FrontierBinding::Detached(unrelated.binding_epoch()),
                ),
            ],
            identity_slot_limit: p
                .max(u)
                .checked_add(1)
                .ok_or_else(|| "intervening identity cap overflows".to_owned())?,
            retained_floor: u128::from(terminal_delivery_seq),
            retained_record_limit: 0,
            retained_records: vec![],
            active_marker_anchors: vec![],
            historical_marker_deliveries: vec![],
            historical_causal_facts: vec![],
            sequence: SequenceClaimFrontierRestore {
                movable_claims: vec![
                    MovableSequenceClaim {
                        delivery_seq: p_exit_seq,
                        owner: SequenceDirectOwner::MembershipExit {
                            participant_index: p,
                        },
                    },
                    MovableSequenceClaim {
                        delivery_seq: u_exit_seq,
                        owner: SequenceDirectOwner::MembershipExit {
                            participant_index: u,
                        },
                    },
                ],
                immutable_candidates: vec![
                    ImmutableSequenceCandidate::BindingTerminal {
                        delivery_seq: terminal_delivery_seq,
                        admission_order: p_order,
                        owner: p_owner,
                    },
                    ImmutableSequenceCandidate::BindingTerminal {
                        delivery_seq: u_terminal_seq,
                        admission_order: u_order,
                        owner: u_owner,
                    },
                ],
                products: SequenceProductRangesRestore {
                    live_times_terminal: vec![
                        TerminalProductRangeRestore {
                            start: p_product_start,
                            length: 2,
                            terminal: p_owner,
                        },
                        TerminalProductRangeRestore {
                            start: u_product_start,
                            length: 2,
                            terminal: u_owner,
                        },
                    ],
                    live_times_replacement_terminal: None,
                    other_live_times_exit: vec![
                        ExitProductRangeRestore {
                            start: p_other_start,
                            length: 1,
                            exit_participant: p,
                        },
                        ExitProductRangeRestore {
                            start: u_other_start,
                            length: 1,
                            exit_participant: u,
                        },
                    ],
                },
                recovery: None,
            },
            order: OrderClaimFrontierRestore {
                movable_claims: vec![
                    MovableOrderClaim {
                        transaction_order: left_transaction_order,
                        owner: OrderDirectOwner::MembershipExit {
                            participant_index: p,
                        },
                    },
                    MovableOrderClaim {
                        transaction_order: left_transaction_order
                            .checked_add(1)
                            .ok_or_else(|| "intervening U exit major overflows".to_owned())?,
                        owner: OrderDirectOwner::MembershipExit {
                            participant_index: u,
                        },
                    },
                ],
                immutable_candidates: vec![
                    ImmutableOrderCandidateMajorRestore {
                        transaction_order: p_order.transaction_order(),
                        candidate_keys: vec![p_order],
                    },
                    ImmutableOrderCandidateMajorRestore {
                        transaction_order: u_order.transaction_order(),
                        candidate_keys: vec![u_order],
                    },
                ],
                recovery: None,
            },
            recovery_marker_delivery_seq: None,
        },
        sequence,
        order,
    )
    .map_err(|error| format!("intervening frontiers invalid: {error:?}"))?;
    match frontiers.prepare_pending_leave_authority(member, pending) {
        Ok(_) => Err("intervening candidate incorrectly minted positional authority".to_owned()),
        Err(error) => Ok(error),
    }
}

const fn high_before(first_claim: u64) -> OrderHigh {
    match first_claim.checked_sub(1) {
        Some(high) => OrderHigh::Allocated(high),
        None => OrderHigh::Empty,
    }
}

struct MarkerClaims {
    sequence: SequenceClaims,
    movable_sequence: Vec<MovableSequenceClaim>,
    products: SequenceProductRangesRestore,
    order: OrderClaims,
    movable_order: Vec<MovableOrderClaim>,
}

fn marker_claims(
    participant_id: u64,
    target_binding: FrontierBinding,
    exit_seq: u64,
    terminal_owner: BindingTerminalOwner,
) -> Result<MarkerClaims, String> {
    match target_binding {
        FrontierBinding::Bound(_) => {
            let terminal_seq = exit_seq
                .checked_add(1)
                .ok_or_else(|| "marker fixture has no terminal-claim suffix".to_owned())?;
            let product_seq = terminal_seq
                .checked_add(1)
                .ok_or_else(|| "marker fixture has no terminal-product suffix".to_owned())?;
            Ok(MarkerClaims {
                sequence: SequenceClaims::new(1, 1, 1, RecoverySequenceReserve::None),
                movable_sequence: vec![
                    MovableSequenceClaim {
                        delivery_seq: exit_seq,
                        owner: SequenceDirectOwner::MembershipExit {
                            participant_index: participant_id,
                        },
                    },
                    MovableSequenceClaim {
                        delivery_seq: terminal_seq,
                        owner: SequenceDirectOwner::BindingTerminal(terminal_owner),
                    },
                ],
                products: SequenceProductRangesRestore {
                    live_times_terminal: vec![TerminalProductRangeRestore {
                        start: product_seq,
                        length: 1,
                        terminal: terminal_owner,
                    }],
                    live_times_replacement_terminal: None,
                    other_live_times_exit: vec![],
                },
                order: OrderClaims::new(1, 1, false, false)
                    .map_err(|error| format!("bound order claims are invalid: {error:?}"))?,
                movable_order: vec![
                    MovableOrderClaim {
                        transaction_order: 1,
                        owner: OrderDirectOwner::ActiveBindingTerminal(terminal_owner),
                    },
                    MovableOrderClaim {
                        transaction_order: 2,
                        owner: OrderDirectOwner::MembershipExit {
                            participant_index: participant_id,
                        },
                    },
                ],
            })
        }
        FrontierBinding::Detached(_) => Ok(MarkerClaims {
            sequence: SequenceClaims::new(1, 0, 1, RecoverySequenceReserve::None),
            movable_sequence: vec![MovableSequenceClaim {
                delivery_seq: exit_seq,
                owner: SequenceDirectOwner::MembershipExit {
                    participant_index: participant_id,
                },
            }],
            products: SequenceProductRangesRestore::default(),
            order: OrderClaims::new(0, 1, false, false)
                .map_err(|error| format!("detached order claims are invalid: {error:?}"))?,
            movable_order: vec![MovableOrderClaim {
                transaction_order: 1,
                owner: OrderDirectOwner::MembershipExit {
                    participant_index: participant_id,
                },
            }],
        }),
    }
}

fn marker_frontier(
    participant_id: u64,
    target_binding: FrontierBinding,
    marker_delivery_seq: u64,
    cursor: u64,
) -> Result<(ClaimFrontiersRestore, SequenceLedger, OrderLedger), String> {
    let high_watermark = marker_delivery_seq
        .checked_sub(1)
        .ok_or_else(|| "planned marker fixture requires a positive sequence".to_owned())?;
    let identity_slot_limit = participant_id
        .checked_add(1)
        .ok_or_else(|| "participant index has no half-open identity limit".to_owned())?;
    let exit_seq = marker_delivery_seq
        .checked_add(1)
        .ok_or_else(|| "marker fixture has no exit-claim suffix".to_owned())?;
    let terminal_owner = BindingTerminalOwner {
        participant_index: participant_id,
        binding_epoch: binding_epoch(target_binding),
    };
    let claims = marker_claims(participant_id, target_binding, exit_seq, terminal_owner)?;
    let sequence_ledger = SequenceLedger::try_new(high_watermark, claims.sequence)
        .map_err(|error| format!("marker fixture sequence ledger is invalid: {error:?}"))?;
    let order_ledger = OrderLedger::try_new(OrderHigh::Allocated(0), claims.order)
        .map_err(|error| format!("marker fixture order ledger is invalid: {error:?}"))?;
    let ordinary_order = AdmissionOrder::new(0, CandidatePhase::OrdinaryRecord, participant_id);
    let marker_order = AdmissionOrder::new(0, CandidatePhase::CompactionMarker, participant_id);
    Ok((
        ClaimFrontiersRestore {
            conversation_id: 1,
            active_identities: vec![FrontierParticipant::new(
                participant_id,
                cursor,
                target_binding,
            )],
            identity_slot_limit,
            retained_floor: u128::from(high_watermark),
            retained_record_limit: 1,
            retained_records: vec![RetainedCausalRecord {
                delivery_seq: high_watermark,
                admission_order: ordinary_order,
                kind: RetainedCausalRecordKind::OrdinaryRecord {
                    participant_index: participant_id,
                },
            }],
            active_marker_anchors: vec![],
            historical_marker_deliveries: vec![],
            historical_causal_facts: vec![],
            sequence: SequenceClaimFrontierRestore {
                movable_claims: claims.movable_sequence,
                immutable_candidates: vec![ImmutableSequenceCandidate::Marker(
                    MarkerCandidateAuthority {
                        delivery_seq: marker_delivery_seq,
                        admission_order: marker_order,
                        target_binding,
                        provenance: MarkerProvenance::NonProductM,
                        abandoned_after: cursor,
                        abandoned_through: high_watermark,
                        physical_floor_at_decision: high_watermark,
                        current_owner: MarkerSequenceOwner::Marker,
                    },
                )],
                products: claims.products,
                recovery: None,
            },
            order: OrderClaimFrontierRestore {
                movable_claims: claims.movable_order,
                immutable_candidates: vec![ImmutableOrderCandidateMajorRestore {
                    transaction_order: marker_order.transaction_order(),
                    candidate_keys: vec![marker_order],
                }],
                recovery: None,
            },
            recovery_marker_delivery_seq: None,
        },
        sequence_ledger,
        order_ledger,
    ))
}

const fn binding_epoch(binding: FrontierBinding) -> BindingEpoch {
    match binding {
        FrontierBinding::Bound(epoch) | FrontierBinding::Detached(epoch) => epoch,
    }
}
