//! Regression for `docs/design/LP-EXTRACTION-GOAL.md` Fix 1.
//!
//! A three-variant `Empty | Pending | Committed` cell cannot compile against
//! the terminalized response path: [`VerifiedTerminalizedDetach::outcome`]
//! requires a borrow derived only from [`TerminalizedDetach`], and that state is
//! produced only by [`commit_attach`] consuming a verified attach plus the
//! committed cell.

#![allow(clippy::expect_used, clippy::panic)]

use crate::wire::{
    AttachAttemptToken, AttachSecret, BindingEpoch, BindingStateView, ConnectionIncarnation,
    CredentialAttachRequest, DetachAttemptToken, DetachRequest, Generation, ObserverBackpressure,
};

use super::{
    ActiveBinding, AttachCommitParameters, AttachSecretProof, AttachedRecordPosition, BindingState,
    BindingTerminalDisposition, ClosureState, CommittedBindingTerminalPosition, CommittedDetach,
    DetachCell, DetachCommitError, DetachReplayError, EnrollmentFingerprint, LiveMember,
    LiveMemberRestore, PendingBindingTerminalPosition, PendingDetach, PendingDrainDecision,
    PendingFinalization, PendingReplay, PendingReplayError, TerminalizedDetach, commit_attach,
    commit_detach, complete_pending_detach, start_blocked_detach,
};

type TestMember = LiveMember<[u8; 32]>;
type SuccessfulAttach = (TestMember, TerminalizedDetach<[u8; 32]>);
type BlockedDetach = (
    TestMember,
    BindingState,
    PendingDetach<[u8; 32]>,
    DetachRequest,
    [u8; 32],
);

fn generation(value: u64) -> Generation {
    Generation::new(value).expect("test generation is nonzero")
}

fn successful_attach(
    committed: CommittedDetach<[u8; 32]>,
    member: TestMember,
    new_epoch: BindingEpoch,
) -> Option<SuccessfulAttach> {
    let request = CredentialAttachRequest {
        conversation_id: member.conversation_id(),
        participant_id: member.participant_id(),
        capability_generation: member.generation(),
        attach_secret: member.attach_secret(),
        attach_attempt_token: AttachAttemptToken::new([0xA7; 16]),
        accept_marker_delivery_seq: None,
    };
    let new_binding = ActiveBinding {
        participant_id: member.participant_id(),
        conversation_id: member.conversation_id(),
        binding_epoch: new_epoch,
    };
    let verified = member
        .verify_detached_attach(
            BindingState::Detached,
            ClosureState::Clear
                .ordinary_detached_attach_admission()
                .expect("clear state admits ordinary attach"),
            request,
            AttachSecretProof::Verified,
            AttachCommitParameters {
                binding: new_binding,
                attach_secret: AttachSecret::new([0xB8; 32]),
                attached_position: AttachedRecordPosition::new(10, 45),
                receipt_expires_at: 1_000,
                provenance_expires_at: 2_000,
            },
        )
        .ok()?;
    let result = commit_attach(verified, DetachCell::Committed(committed)).ok()?;
    match result.detach_cell {
        DetachCell::Terminalized(cell) => Some((result.member, cell)),
        _ => None,
    }
}

fn live_member(
    participant_id: u64,
    conversation_id: u64,
    generation: Generation,
    attach_secret: AttachSecret,
) -> TestMember {
    LiveMember::restore(LiveMemberRestore {
        participant_id,
        conversation_id,
        generation,
        attach_secret,
        cursor: 0,
        enrollment_fingerprint: EnrollmentFingerprint::new([0xE1; 32]),
        latest_terminal: None,
    })
    .expect("fixture membership has no inconsistent retained terminal")
}

fn blocked_detach() -> BlockedDetach {
    let binding = ActiveBinding {
        participant_id: 3,
        conversation_id: 29,
        binding_epoch: BindingEpoch::new(ConnectionIncarnation::new(7, 11), generation(4)),
    };
    let member = live_member(3, 29, generation(4), AttachSecret::new([0x44; 32]));
    let request = DetachRequest {
        conversation_id: 29,
        participant_id: 3,
        capability_generation: generation(4),
        detach_attempt_token: DetachAttemptToken::new([0xD4; 16]),
    };
    let verifier = [0xA6; 32];
    let verified_request = binding
        .verify_detach_request(request.clone(), verifier)
        .expect("request matches the active binding");
    let transition = start_blocked_detach(
        member,
        verified_request,
        DetachCell::default(),
        PendingBindingTerminalPosition::new(9),
        6,
    )
    .expect("empty cell accepts blocked detach");
    let (member, state, cell, outcome) = transition.into_parts();
    let ObserverBackpressure::Detach { state: refusal, .. } = outcome else {
        panic!("blocked detach must return its operation-specific refusal");
    };
    assert_eq!(refusal.backpressure_epoch(), 6);
    assert_eq!(refusal.observer_progress(), 6);
    (member, state, cell, request, verifier)
}

#[test]
fn committed_attach_terminalized_replay_retains_old_epoch() {
    let old_epoch = BindingEpoch::new(ConnectionIncarnation::new(7, 11), generation(4));
    let binding = ActiveBinding {
        participant_id: 3,
        conversation_id: 29,
        binding_epoch: old_epoch,
    };
    let token = DetachAttemptToken::new([0xD3; 16]);
    let verifier = [0xA5; 32];
    let request = DetachRequest {
        conversation_id: 29,
        participant_id: 3,
        capability_generation: generation(4),
        detach_attempt_token: token,
    };

    let verified_request = binding
        .verify_detach_request(request.clone(), verifier)
        .expect("request matches the active binding");
    let member = live_member(3, 29, generation(4), AttachSecret::new([0x44; 32]));
    let transition = commit_detach(
        member,
        verified_request,
        DetachCell::default(),
        CommittedBindingTerminalPosition::new(9, 44),
    )
    .expect("empty cell accepts detach");
    let (member, terminal, _, committed, committed_outcome) = transition.into_parts();
    assert_eq!(committed_outcome.committed_binding_epoch(), old_epoch);
    assert_eq!(committed_outcome.detached_delivery_seq(), 44);
    assert_eq!(
        member.latest_terminal(),
        Some(super::CommittedBindingTerminal::Detached(terminal))
    );

    let new_epoch = BindingEpoch::new(ConnectionIncarnation::new(7, 12), generation(5));
    let (_, terminalized) = successful_attach(committed, member, new_epoch)
        .expect("verified attach terminalizes the committed cell");
    let verified_old = terminalized
        .verify_exact(&request, verifier)
        .expect("old request is byte-identical");
    let outcome = verified_old.outcome(
        29,
        generation(5),
        BindingStateView::Bound {
            current_binding_epoch: new_epoch,
        },
    );

    assert_eq!(outcome.committed_binding_epoch(), old_epoch);
    assert_eq!(outcome.current_generation(), generation(5));
    assert_eq!(
        outcome.binding_state(),
        BindingStateView::Bound {
            current_binding_epoch: new_epoch,
        }
    );
}

#[test]
fn exact_token_without_exact_verifier_cannot_produce_terminalized_result() {
    let old_epoch = BindingEpoch::new(ConnectionIncarnation::new(2, 3), generation(8));
    let binding = ActiveBinding {
        participant_id: 1,
        conversation_id: 5,
        binding_epoch: old_epoch,
    };
    let request = DetachRequest {
        conversation_id: 5,
        participant_id: 1,
        capability_generation: generation(8),
        detach_attempt_token: DetachAttemptToken::new([9; 16]),
    };
    let verifier = [4; 32];
    let verified_request = binding
        .verify_detach_request(request.clone(), verifier)
        .expect("request matches the active binding");
    let member = live_member(1, 5, generation(8), AttachSecret::new([0x48; 32]));
    let transition = commit_detach(
        member,
        verified_request,
        DetachCell::default(),
        CommittedBindingTerminalPosition::new(7, 12),
    )
    .expect("empty cell accepts detach");
    let (member, _, _, committed, _) = transition.into_parts();
    let (_, terminalized) = successful_attach(
        committed,
        member,
        BindingEpoch::new(ConnectionIncarnation::new(2, 4), generation(9)),
    )
    .expect("verified attach terminalizes the committed cell");

    assert!(terminalized.verify_exact(&request, [5; 32]).is_err());
}

#[test]
fn pending_replay_equal_progress_is_stable_without_a_drain() {
    let (member, binding_state, cell, request, verifier) = blocked_detach();
    let replay = cell
        .verify_exact(&request, verifier)
        .expect("request is the stored pending request")
        .prepare_replay(29, binding_state, 6)
        .apply(member.clone(), PendingDrainDecision::NotAttempted)
        .expect("equal observer progress does not drain");

    match replay {
        PendingReplay::Pending {
            member: next_member,
            binding_state: next_binding,
            cell: next_cell,
            outcome:
                ObserverBackpressure::Detach {
                    state,
                    committed_binding_epoch,
                    ..
                },
        } => {
            assert_eq!(next_member, member);
            assert_eq!(next_binding, binding_state);
            assert_eq!(next_cell, cell);
            assert_eq!(state.backpressure_epoch(), 6);
            assert_eq!(state.observer_progress(), 6);
            assert_eq!(committed_binding_epoch.capability_generation, generation(4));
        }
        _ => panic!("equal progress must retain the pending cell"),
    }
}

#[test]
fn pending_replay_requires_drain_and_rewrites_a_repeated_refusal_epoch() {
    let (member, binding_state, cell, request, verifier) = blocked_detach();
    let regression = cell
        .verify_exact(&request, verifier)
        .expect("request is exact")
        .prepare_replay(29, binding_state, 5)
        .apply(member.clone(), PendingDrainDecision::NotAttempted);
    assert_eq!(
        regression,
        Err(PendingReplayError::ObserverProgressRegression)
    );

    let false_equal_drain = cell
        .verify_exact(&request, verifier)
        .expect("request is exact")
        .prepare_replay(29, binding_state, 6)
        .apply(member.clone(), PendingDrainDecision::StillBlocked);
    assert_eq!(false_equal_drain, Err(PendingReplayError::UnexpectedDrain));

    let without_drain = cell
        .verify_exact(&request, verifier)
        .expect("request is exact")
        .prepare_replay(29, binding_state, 7)
        .apply(member.clone(), PendingDrainDecision::NotAttempted);
    assert_eq!(without_drain, Err(PendingReplayError::DrainRequired));

    let rewritten = cell
        .verify_exact(&request, verifier)
        .expect("request is exact")
        .prepare_replay(29, binding_state, 7)
        .apply(member.clone(), PendingDrainDecision::StillBlocked)
        .expect("a required drain may produce a new refusal");
    let PendingReplay::Pending {
        member: next_member,
        binding_state: next_binding,
        cell: rewritten_cell,
        outcome:
            ObserverBackpressure::Detach {
                state,
                committed_binding_epoch,
                ..
            },
    } = rewritten
    else {
        panic!("a blocked drain must retain pending state");
    };
    assert_eq!(next_member, member);
    assert_eq!(next_binding, binding_state);
    assert_ne!(rewritten_cell, cell);
    assert_eq!(state.backpressure_epoch(), 7);
    assert_eq!(state.observer_progress(), 7);
    assert_eq!(committed_binding_epoch.capability_generation, generation(4));

    let stable_at_new_epoch = rewritten_cell
        .verify_exact(&request, verifier)
        .expect("rewritten cell retains exact request")
        .prepare_replay(29, next_binding, 7)
        .apply(next_member, PendingDrainDecision::NotAttempted)
        .expect("the rewritten refusal is stable at equal progress");
    assert!(matches!(stable_at_new_epoch, PendingReplay::Pending { .. }));
}

#[test]
fn pending_replay_commits_only_the_real_drain_sequence() {
    let (member, binding_state, cell, request, verifier) = blocked_detach();
    let replay = cell
        .verify_exact(&request, verifier)
        .expect("request is exact")
        .prepare_replay(29, binding_state, 8)
        .apply(
            member,
            PendingDrainDecision::Committed {
                detached_delivery_seq: 44,
            },
        )
        .expect("advanced progress permits an ordered drain");
    let PendingReplay::Committed {
        member,
        terminal,
        binding_state: next_binding,
        cell: committed,
        outcome,
    } = replay
    else {
        panic!("successful drain must commit the cell");
    };
    assert_eq!(next_binding, BindingState::Detached);
    assert_eq!(outcome.detached_delivery_seq(), 44);
    assert_eq!(
        member.latest_terminal(),
        Some(super::CommittedBindingTerminal::Detached(terminal))
    );
    assert_eq!(
        committed
            .verify_exact(&request, verifier)
            .expect("committed cell retains the canonical request")
            .outcome(29),
        outcome
    );
}

#[test]
fn every_pending_replay_branch_validates_the_paired_finalization() {
    let (member, binding_state, cell, request, verifier) = blocked_detach();
    let BindingState::PendingFinalization(finalization) = binding_state else {
        panic!("helper must produce pending finalization");
    };
    let wrong_binding = ActiveBinding {
        participant_id: finalization.participant_id(),
        conversation_id: finalization.conversation_id(),
        binding_epoch: finalization.binding_epoch(),
    };
    let wrong_terminal = wrong_binding.connection_lost(BindingTerminalDisposition::Pending(
        PendingBindingTerminalPosition::new(finalization.admission_order().transaction_order()),
    ));
    let super::DiedBindingTransition::Pending(wrong_terminal) = wrong_terminal else {
        panic!("pending disposition must produce pending Died state");
    };
    let wrong_state = BindingState::PendingFinalization(PendingFinalization::Died(wrong_terminal));

    for (progress, decision) in [
        (6, PendingDrainDecision::NotAttempted),
        (7, PendingDrainDecision::StillBlocked),
        (
            7,
            PendingDrainDecision::Committed {
                detached_delivery_seq: 44,
            },
        ),
    ] {
        let result = cell
            .verify_exact(&request, verifier)
            .expect("request is exact")
            .prepare_replay(29, wrong_state, progress)
            .apply(member.clone(), decision);
        assert_eq!(result, Err(PendingReplayError::StatePair));
    }

    assert_eq!(
        complete_pending_detach(member, wrong_state, cell, 44),
        Err(DetachReplayError::StatePair)
    );
}

#[test]
fn a_new_detach_cannot_replace_an_accepted_pending_cell() {
    let (member, _, pending, _, _) = blocked_detach();
    let binding = ActiveBinding {
        participant_id: 3,
        conversation_id: 29,
        binding_epoch: BindingEpoch::new(ConnectionIncarnation::new(7, 12), generation(4)),
    };
    let request = DetachRequest {
        conversation_id: 29,
        participant_id: 3,
        capability_generation: generation(4),
        detach_attempt_token: DetachAttemptToken::new([0xE5; 16]),
    };
    let verified = binding
        .verify_detach_request(request.clone(), [0xB7; 32])
        .expect("request matches binding");
    assert_eq!(
        commit_detach(
            member.clone(),
            verified,
            DetachCell::Pending(pending),
            CommittedBindingTerminalPosition::new(10, 50),
        ),
        Err(DetachCommitError::PendingCell)
    );

    let verified = binding
        .verify_detach_request(request, [0xB7; 32])
        .expect("request matches binding");
    assert_eq!(
        start_blocked_detach(
            member,
            verified,
            DetachCell::Pending(pending),
            PendingBindingTerminalPosition::new(10),
            9,
        ),
        Err(DetachCommitError::PendingCell)
    );
}

#[test]
// One end-to-end history covers all three previous-cell classifications.
#[allow(clippy::too_many_lines)]
fn a_current_binding_rejects_committed_and_invalid_terminalized_cells() {
    let old_epoch = BindingEpoch::new(ConnectionIncarnation::new(12, 1), generation(4));
    let old_binding = ActiveBinding {
        participant_id: 3,
        conversation_id: 29,
        binding_epoch: old_epoch,
    };
    let old_request = DetachRequest {
        conversation_id: 29,
        participant_id: 3,
        capability_generation: generation(4),
        detach_attempt_token: DetachAttemptToken::new([0x71; 16]),
    };
    let original_member = live_member(3, 29, generation(4), AttachSecret::new([0x41; 32]));
    let verified = old_binding
        .verify_detach_request(old_request, [0x72; 32])
        .expect("old request matches its binding");
    let detached = commit_detach(
        original_member.clone(),
        verified,
        DetachCell::default(),
        CommittedBindingTerminalPosition::new(30, 40),
    )
    .expect("first detach commits");
    let (detached_member, _, _, committed, _) = detached.into_parts();

    let current_request = DetachRequest {
        conversation_id: 29,
        participant_id: 3,
        capability_generation: generation(4),
        detach_attempt_token: DetachAttemptToken::new([0x73; 16]),
    };
    let verified = old_binding
        .verify_detach_request(current_request, [0x74; 32])
        .expect("synthetic current binding matches request");
    assert_eq!(
        commit_detach(
            original_member.clone(),
            verified,
            DetachCell::Committed(committed),
            CommittedBindingTerminalPosition::new(31, 41),
        ),
        Err(DetachCommitError::CommittedCell)
    );

    let new_epoch = BindingEpoch::new(ConnectionIncarnation::new(12, 2), generation(5));
    let (current_member, terminalized) = successful_attach(committed, detached_member, new_epoch)
        .expect("attach terminalizes the old committed cell");

    let same_generation_request = DetachRequest {
        conversation_id: 29,
        participant_id: 3,
        capability_generation: generation(4),
        detach_attempt_token: DetachAttemptToken::new([0x75; 16]),
    };
    let verified = old_binding
        .verify_detach_request(same_generation_request, [0x76; 32])
        .expect("synthetic same-generation binding matches request");
    assert_eq!(
        commit_detach(
            original_member,
            verified,
            DetachCell::Terminalized(terminalized),
            CommittedBindingTerminalPosition::new(32, 42),
        ),
        Err(DetachCommitError::TerminalizedCellAuthority)
    );

    let wrong_member = live_member(4, 29, generation(5), AttachSecret::new([0x42; 32]));
    let wrong_binding = ActiveBinding {
        participant_id: 4,
        conversation_id: 29,
        binding_epoch: BindingEpoch::new(ConnectionIncarnation::new(12, 3), generation(5)),
    };
    let wrong_request = DetachRequest {
        conversation_id: 29,
        participant_id: 4,
        capability_generation: generation(5),
        detach_attempt_token: DetachAttemptToken::new([0x79; 16]),
    };
    let verified = wrong_binding
        .verify_detach_request(wrong_request, [0x7A; 32])
        .expect("cross-identity fixture matches its own binding");
    assert_eq!(
        commit_detach(
            wrong_member,
            verified,
            DetachCell::Terminalized(terminalized),
            CommittedBindingTerminalPosition::new(32, 42),
        ),
        Err(DetachCommitError::TerminalizedCellAuthority)
    );

    let current_binding = ActiveBinding {
        participant_id: 3,
        conversation_id: 29,
        binding_epoch: new_epoch,
    };
    let current_request = DetachRequest {
        conversation_id: 29,
        participant_id: 3,
        capability_generation: generation(5),
        detach_attempt_token: DetachAttemptToken::new([0x77; 16]),
    };
    let verified = current_binding
        .verify_detach_request(current_request, [0x78; 32])
        .expect("new request matches the current binding");
    let current = commit_detach(
        current_member,
        verified,
        DetachCell::Terminalized(terminalized),
        CommittedBindingTerminalPosition::new(33, 43),
    )
    .expect("strictly older terminalized state may be replaced");
    let (current_member, terminal, _, _, _) = current.into_parts();
    assert_eq!(
        current_member.latest_terminal(),
        Some(super::CommittedBindingTerminal::Detached(terminal))
    );
}
