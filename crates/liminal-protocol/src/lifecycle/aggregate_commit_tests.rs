//! Aggregate-commit coverage for the six lifecycle operations (item A3).
//!
//! Every test drives the crate's own typed operation commit, selects the
//! aggregate event, and proves the two A3 laws: commit-then-replay equals the
//! live shell state, and a crash between decide and commit leaves the shell
//! unadvanced with the operation authority intact.

#![allow(clippy::expect_used, clippy::panic)]

use alloc::boxed::Box;
use alloc::{vec, vec::Vec};

use crate::algebra::WideResourceVector;
use crate::wire::{
    AttachSecret, BindingEpoch, ConnectionIncarnation, ConversationId, Generation, ParticipantAck,
};

use super::edge::marker_delivery_for_test;
use super::operation_event_tests::{
    bound_leave_commit, detach_transition, enrollment_commit, superseding_attach_commit,
};
use super::{
    ActiveBinding, AggregateOperationCommit, AggregateOperationDecision, BindingState,
    BindingTerminalDisposition, BoundParticipantCursor, ClosureDebt, ClosureState,
    CommittedBindingTerminalPosition, ConversationDecision, ConversationEvent, ConversationGenesis,
    ConversationRefusalReason, CursorFateSuccessor, CursorProgressKey, DebtCompletion,
    DiedBindingTransition, EnrollmentFingerprint, Event, IdentityState, LiveMember,
    LiveMemberRestore, NonzeroDebtCursorEpisode, NonzeroParticipantAckCommit,
    NonzeroParticipantAckDecision, ObserverProjection, OrdinaryBindingFate,
    ParticipantConversation, PresentedIdentity, RecoveredBindingFate, StoredEdge,
    apply_nonzero_participant_ack, decide_attached_operation, decide_detached_operation,
    decide_enrolled_operation, decide_left_operation, decide_nonzero_debt_ack_operation,
    decide_ordinary_binding_fate_operation, decide_recovered_binding_fate_operation,
};

const ENROLLMENT_CONVERSATION: ConversationId = 17;
const ATTACH_CONVERSATION: ConversationId = 29;
const LEAVE_CONVERSATION: ConversationId = 11;
const MARKER_FIXTURE_CONVERSATION: ConversationId = 1;
const ACK_CONVERSATION: ConversationId = 54;

fn validated_shell(conversation_id: ConversationId) -> ParticipantConversation {
    let conversation =
        ParticipantConversation::from_genesis(ConversationGenesis::new(conversation_id));
    let ConversationDecision::Commit(commit) = conversation.decide_genesis_validation() else {
        panic!("fresh genesis must select its one-shot event");
    };
    commit.commit()
}

fn generation(value: u64) -> Generation {
    Generation::new(value).expect("test generations are nonzero")
}

fn debt(entries: u128, bytes: u128) -> ClosureDebt {
    ClosureDebt::new(WideResourceVector::new(entries, bytes)).expect("test closure debt is nonzero")
}

fn committed_died_terminal(
    binding: ActiveBinding,
    transaction_order: u64,
    delivery_seq: u64,
) -> super::CommittedDiedTerminal {
    match binding.connection_lost(BindingTerminalDisposition::Committed(
        CommittedBindingTerminalPosition::new(transaction_order, delivery_seq),
    )) {
        DiedBindingTransition::Committed(terminal) => terminal,
        DiedBindingTransition::Pending(_) => panic!("test terminal is committed"),
    }
}

fn ordinary_fate() -> OrdinaryBindingFate {
    let commit = superseding_attach_commit();
    let BindingState::Bound(binding) = commit.binding_state else {
        panic!("the superseding attach fixture installs a bound binding");
    };
    let terminal = committed_died_terminal(binding, 16, 20);
    commit
        .ordinary_binding_fate(terminal, 9)
        .expect("the exact new-binding terminal derives ordinary fate")
}

fn recovered_fate() -> RecoveredBindingFate {
    let recovery_debt = debt(2, 20);
    let attached_debt = debt(2, 18);
    let prior_epoch = BindingEpoch::new(ConnectionIncarnation::new(1, 2), generation(2));
    let recovered_epoch = BindingEpoch::new(ConnectionIncarnation::new(1, 3), generation(3));
    let delivery =
        marker_delivery_for_test(4, prior_epoch, 14).expect("validated marker fixture restores");
    let ClosureState::Owed {
        edge: StoredEdge::ParticipantCursorProgress(progress),
        ..
    } = delivery
        .delivered(recovery_debt, Event::marker_delivered(4, prior_epoch, 14))
        .expect("exact marker delivery commits")
    else {
        panic!("marker delivery must select the cursor-progress edge");
    };
    let recovery = match progress
        .binding_fate(
            recovery_debt,
            Event::binding_fate_observed(4, prior_epoch, 14),
        )
        .expect("exact marker fate commits")
    {
        CursorFateSuccessor::DetachedCredentialRecovery(edge) => edge,
        CursorFateSuccessor::DetachedCursorRelease(_) => {
            panic!("marker fate must select detached credential recovery")
        }
    };
    let fenced = recovery
        .fenced_attach(
            recovery_debt,
            Event::fenced_recovery_committed(4, 14, prior_epoch, recovered_epoch, 15),
            DebtCompletion::observer_projection(attached_debt, ObserverProjection::new(15)),
        )
        .expect("exact DCR creates the fenced marker-acceptance proof");
    fenced
        .recovered_binding_fate(Event::binding_fate_observed(4, recovered_epoch, 15))
        .expect("exact recovered-epoch fate derives suffix authority")
}

fn ack_epoch(ordinal: u64) -> BindingEpoch {
    BindingEpoch::new(ConnectionIncarnation::new(54, ordinal), Generation::ONE)
}

fn ack_member() -> LiveMember<Vec<u8>> {
    LiveMember::restore(LiveMemberRestore {
        participant_id: 0,
        conversation_id: ACK_CONVERSATION,
        generation: Generation::ONE,
        attach_secret: AttachSecret::new([0x54; 32]),
        cursor: 0,
        enrollment_fingerprint: EnrollmentFingerprint::new(vec![0x54]),
        latest_terminal: None,
    })
    .expect("ack fixture membership is internally consistent")
}

fn nonzero_ack_commit() -> Box<NonzeroParticipantAckCommit> {
    let member = ack_member();
    let episode = NonzeroDebtCursorEpisode::new(
        ACK_CONVERSATION,
        debt(1, 4),
        0,
        2,
        1,
        1,
        vec![
            BoundParticipantCursor::new(0, ack_epoch(0), 0),
            BoundParticipantCursor::new(1, ack_epoch(1), 0),
        ],
    )
    .expect("ack fixture episode has a retained suffix");
    let identity: IdentityState<Vec<u8>, Vec<u8>, Vec<u8>> = IdentityState::Live(member);
    let binding = BindingState::Bound(ActiveBinding {
        participant_id: 0,
        conversation_id: ACK_CONVERSATION,
        binding_epoch: ack_epoch(0),
    });
    let request = ParticipantAck {
        conversation_id: ACK_CONVERSATION,
        participant_id: 0,
        capability_generation: Generation::ONE,
        through_seq: 1,
    };
    let decision = apply_nonzero_participant_ack(
        PresentedIdentity::from(Some(&identity)),
        &binding,
        ack_epoch(0),
        &request,
        2,
        &episode,
    );
    let NonzeroParticipantAckDecision::Commit(commit) = decision else {
        panic!("exact forward ack must produce an aggregate commit");
    };
    commit
}

fn commit_of<T>(decision: AggregateOperationDecision<T>) -> AggregateOperationCommit<T> {
    match decision {
        AggregateOperationDecision::Commit(commit) => commit,
        AggregateOperationDecision::Refused(refusal) => {
            panic!(
                "a validated same-conversation shell must not refuse: {:?}",
                refusal.reason()
            )
        }
    }
}

fn exercise_operation<T, D>(conversation_id: ConversationId, payload: T, decide: D)
where
    T: PartialEq + core::fmt::Debug,
    D: Fn(ParticipantConversation, T) -> AggregateOperationDecision<T>,
{
    // Crash between decide and durable append: abort recovers the exact
    // unadvanced shell pre-state and the intact operation commit.
    let pending = commit_of(decide(validated_shell(conversation_id), payload));
    let encoded = pending.event().encode_canonical();
    assert_eq!(pending.event().conversation_id(), conversation_id);
    assert_eq!(pending.event().ordinal(), 1);
    let (crashed_shell, recovered_payload) = pending.abort();
    assert_eq!(
        crashed_shell,
        validated_shell(conversation_id),
        "a crash between decide and commit must leave the shell unadvanced"
    );

    // Redecide from the recovered pre-state: the identical event is selected,
    // and consuming the barrier after the durable append advances the shell.
    let pending = commit_of(decide(crashed_shell, recovered_payload));
    assert_eq!(
        pending.event().encode_canonical(),
        encoded,
        "the recovered payload must reselect the identical durable event"
    );
    let (live, _payload) = pending.commit();
    assert_eq!(live.next_event_ordinal(), 2);

    // Commit-then-replay equals live state.
    let replayed = validated_shell(conversation_id)
        .replay(ConversationEvent::decode_canonical(&encoded).expect("logged event decodes"))
        .expect("the durable event replays against the exact pre-state");
    assert_eq!(replayed, live, "replayed state must equal live state");
}

#[test]
fn enrolled_aggregate_commit_crashes_safely_and_replays_to_live_state() {
    exercise_operation(
        ENROLLMENT_CONVERSATION,
        enrollment_commit(),
        decide_enrolled_operation,
    );
}

#[test]
fn attached_aggregate_commit_crashes_safely_and_replays_to_live_state() {
    exercise_operation(
        ATTACH_CONVERSATION,
        superseding_attach_commit(),
        decide_attached_operation,
    );
}

#[test]
fn detached_aggregate_commit_crashes_safely_and_replays_to_live_state() {
    exercise_operation(
        ATTACH_CONVERSATION,
        detach_transition(3, 0xD3, 44),
        |shell, transition| {
            decide_detached_operation(shell, transition)
                .expect("a commit's own terminal/cell pair is congruent")
        },
    );
}

#[test]
fn left_aggregate_commit_crashes_safely_and_replays_to_live_state() {
    exercise_operation(LEAVE_CONVERSATION, bound_leave_commit(), |shell, commit| {
        decide_left_operation(shell, commit).expect("leave commits carry a permanent tombstone")
    });
}

#[test]
fn ordinary_fate_aggregate_commit_crashes_safely_and_replays_to_live_state() {
    exercise_operation(
        ATTACH_CONVERSATION,
        ordinary_fate(),
        decide_ordinary_binding_fate_operation,
    );
}

#[test]
fn recovered_fate_aggregate_commit_crashes_safely_and_replays_to_live_state() {
    exercise_operation(
        MARKER_FIXTURE_CONVERSATION,
        recovered_fate(),
        decide_recovered_binding_fate_operation,
    );
}

#[test]
fn nonzero_debt_ack_aggregate_commit_crashes_safely_and_replays_to_live_state() {
    exercise_operation(
        ACK_CONVERSATION,
        nonzero_ack_commit(),
        decide_nonzero_debt_ack_operation,
    );
}

#[test]
fn nonzero_debt_ack_event_records_the_committed_cursor_fact_key() {
    let payload = nonzero_ack_commit();
    let request = payload.outcome().request().clone();
    let pending = commit_of(decide_nonzero_debt_ack_operation(
        validated_shell(ACK_CONVERSATION),
        payload,
    ));
    let (_live, payload) = pending.commit();

    // Fix 2: the recorded (participant, through_seq) pair is the exact
    // per-participant (participant_index, boundary) cursor-fact key that the
    // consumed commit's resulting episode accounts for.
    let key = CursorProgressKey {
        participant_index: request.participant_id,
        boundary: request.through_seq,
    };
    assert!(
        payload.resulting_episode().facts().get(key).is_some(),
        "the resulting episode must carry the committed cursor fact"
    );
    let episode_participant = payload
        .resulting_episode()
        .participant(request.participant_id)
        .expect("the acking participant remains bound in the episode");
    assert_eq!(episode_participant.cursor(), request.through_seq);
}

#[test]
fn left_aggregate_commit_returns_the_frontiers_through_the_barrier() {
    let pending = commit_of(
        decide_left_operation(validated_shell(LEAVE_CONVERSATION), bound_leave_commit())
            .expect("leave commits carry a permanent tombstone"),
    );
    let (_live, commit) = pending.commit();
    let (identity, frontiers) = commit.into_parts();
    assert!(matches!(identity, IdentityState::Retired(_)));
    assert_eq!(frontiers.conversation_id(), LEAVE_CONVERSATION);
}

#[test]
fn aggregate_decision_refuses_a_foreign_shell_and_returns_the_commit_intact() {
    let foreign_shell = validated_shell(ENROLLMENT_CONVERSATION + 1);
    let decision = decide_enrolled_operation(foreign_shell, enrollment_commit());
    let AggregateOperationDecision::Refused(refusal) = decision else {
        panic!("provenance for another conversation must be refused");
    };
    assert_eq!(
        refusal.reason(),
        ConversationRefusalReason::OperationConversationMismatch {
            expected: ENROLLMENT_CONVERSATION + 1,
            actual: ENROLLMENT_CONVERSATION,
        }
    );
    let (shell, commit) = refusal.into_parts();
    assert_eq!(shell, validated_shell(ENROLLMENT_CONVERSATION + 1));
    assert_eq!(commit, enrollment_commit());
}

#[test]
fn aggregate_decision_requires_genesis_validation_first() {
    let fresh =
        ParticipantConversation::from_genesis(ConversationGenesis::new(ENROLLMENT_CONVERSATION));
    let decision = decide_enrolled_operation(fresh, enrollment_commit());
    let AggregateOperationDecision::Refused(refusal) = decision else {
        panic!("an unvalidated shell must refuse lifecycle operations");
    };
    assert_eq!(
        refusal.reason(),
        ConversationRefusalReason::GenesisNotValidated
    );
    let (shell, _commit) = refusal.into_parts();
    assert!(!shell.genesis_validated());
    assert_eq!(shell.next_event_ordinal(), 0);
}
