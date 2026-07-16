#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use alloc::{vec, vec::Vec};

use crate::algebra::WideResourceVector;
use crate::wire::{
    AckCommitted, AckGap, AckNoOp, AckRegression, AttachSecret, BindingEpoch,
    BindingRequiredEnvelope, ConnectionIncarnation, Generation, ParticipantAck,
    ParticipantAckEnvelope, ParticipantUnknown, ServerValue,
};

use super::{
    super::{
        ActiveBinding, BindingState, BoundParticipantCursor, ClosureDebt, CursorProgressFact,
        CursorProgressKey, EnrollmentFingerprint, IdentityState, LiveMember, LiveMemberRestore,
        NonzeroDebtCursorEpisode, PresentedIdentity,
    },
    nonzero_participant_ack::{
        NonzeroAckEpisodePosition, NonzeroParticipantAckCommit, NonzeroParticipantAckCommitError,
        NonzeroParticipantAckDecision, NonzeroParticipantAckInvariantError,
        apply_nonzero_participant_ack,
    },
};

const CONVERSATION_ID: u64 = 54;
const P0: u64 = 0;
const P1: u64 = 1;
const OBSERVER: u64 = 0;
const H: u64 = 2;

type TestIdentity = IdentityState<Vec<u8>, Vec<u8>, Vec<u8>>;

fn generation(value: u64) -> Generation {
    Generation::new(value).expect("test generation is nonzero")
}

fn epoch(generation_value: u64, ordinal: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(54, ordinal),
        generation(generation_value),
    )
}

fn member_with(
    conversation_id: u64,
    participant_id: u64,
    generation_value: u64,
    cursor: u64,
) -> LiveMember<Vec<u8>> {
    LiveMember::restore(LiveMemberRestore {
        participant_id,
        conversation_id,
        generation: generation(generation_value),
        attach_secret: AttachSecret::new([0x54; 32]),
        cursor,
        enrollment_fingerprint: EnrollmentFingerprint::new(vec![0x54]),
        latest_terminal: None,
    })
    .expect("test member has valid terminal history")
}

fn member(participant_id: u64, cursor: u64) -> LiveMember<Vec<u8>> {
    member_with(CONVERSATION_ID, participant_id, 1, cursor)
}

fn binding(participant_id: u64, binding_epoch: BindingEpoch) -> BindingState {
    BindingState::Bound(ActiveBinding {
        participant_id,
        conversation_id: CONVERSATION_ID,
        binding_epoch,
    })
}

fn request(participant_id: u64, generation_value: u64, through_seq: u64) -> ParticipantAck {
    ParticipantAck {
        conversation_id: CONVERSATION_ID,
        participant_id,
        capability_generation: generation(generation_value),
        through_seq,
    }
}

fn envelope(participant_id: u64, through_seq: u64) -> ParticipantAckEnvelope {
    ParticipantAckEnvelope {
        conversation_id: CONVERSATION_ID,
        participant_id,
        capability_generation: generation(1),
        through_seq,
    }
}

fn episode_with(
    conversation_id: u64,
    participants: Vec<BoundParticipantCursor>,
) -> NonzeroDebtCursorEpisode {
    NonzeroDebtCursorEpisode::new(
        conversation_id,
        ClosureDebt::new(WideResourceVector::new(1, 4)).expect("fixture requires nonzero debt"),
        OBSERVER,
        H,
        1,
        1,
        participants,
    )
    .expect("fixture episode has a retained suffix")
}

fn episode() -> NonzeroDebtCursorEpisode {
    episode_with(
        CONVERSATION_ID,
        vec![
            BoundParticipantCursor::new(P0, epoch(1, 0), OBSERVER),
            BoundParticipantCursor::new(P1, epoch(1, 1), OBSERVER),
        ],
    )
}

fn apply_for(
    member: &LiveMember<Vec<u8>>,
    binding_epoch: BindingEpoch,
    request: &ParticipantAck,
    available: u64,
    episode: &NonzeroDebtCursorEpisode,
) -> NonzeroParticipantAckDecision {
    let identity: TestIdentity = IdentityState::Live(member.clone());
    apply_nonzero_participant_ack(
        PresentedIdentity::from(Some(&identity)),
        &binding(member.participant_id(), binding_epoch),
        binding_epoch,
        request,
        available,
        episode,
    )
}

fn commit_for(
    member: &LiveMember<Vec<u8>>,
    binding_epoch: BindingEpoch,
    boundary: u64,
    episode: &NonzeroDebtCursorEpisode,
) -> NonzeroParticipantAckCommit {
    let decision = apply_for(
        member,
        binding_epoch,
        &request(member.participant_id(), 1, boundary),
        H,
        episode,
    );
    let NonzeroParticipantAckDecision::Commit(commit) = decision else {
        panic!("exact forward ack must produce an aggregate commit")
    };
    *commit
}

#[test]
fn common_lookup_precedes_episode_validation_and_never_mutates() {
    let mismatched_episode = episode_with(
        CONVERSATION_ID + 1,
        vec![BoundParticipantCursor::new(P0, epoch(1, 0), OBSERVER)],
    );
    let before = mismatched_episode.clone();
    let ack = request(P0, 1, 1);
    assert_eq!(
        apply_nonzero_participant_ack::<Vec<u8>, Vec<u8>, Vec<u8>>(
            PresentedIdentity::Absent,
            &BindingState::Detached,
            epoch(1, 0),
            &ack,
            H,
            &mismatched_episode,
        ),
        NonzeroParticipantAckDecision::Respond(ServerValue::ParticipantUnknown(
            ParticipantUnknown {
                request: crate::wire::ParticipantReferenceEnvelope::ParticipantAck(
                    envelope(P0, 1,)
                ),
            },
        )),
    );

    let live = member(P0, OBSERVER);
    let stale = request(P0, 2, 1);
    assert!(matches!(
        apply_for(&live, epoch(1, 0), &stale, H, &mismatched_episode),
        NonzeroParticipantAckDecision::Respond(ServerValue::StaleAuthority(_))
    ));
    assert_eq!(mismatched_episode, before);

    assert_eq!(
        apply_nonzero_participant_ack(
            PresentedIdentity::<Vec<u8>, Vec<u8>, Vec<u8>>::Live(&live),
            &BindingState::Detached,
            epoch(1, 0),
            &ack,
            H,
            &mismatched_episode,
        ),
        NonzeroParticipantAckDecision::Respond(ServerValue::NoBinding(crate::wire::NoBinding {
            request: BindingRequiredEnvelope::ParticipantAck(envelope(P0, 1)),
        },)),
    );
    assert_eq!(mismatched_episode, before);
}

#[test]
fn aggregate_invariant_checks_are_ordered_and_nonmutating() {
    let live = member(P0, OBSERVER);
    let ack = request(P0, 1, 1);
    let cases = [
        (
            episode_with(
                CONVERSATION_ID + 1,
                vec![BoundParticipantCursor::new(P0, epoch(1, 0), OBSERVER)],
            ),
            NonzeroParticipantAckInvariantError::Conversation {
                member: CONVERSATION_ID,
                episode: CONVERSATION_ID + 1,
            },
        ),
        (
            episode_with(
                CONVERSATION_ID,
                vec![BoundParticipantCursor::new(P1, epoch(1, 1), OBSERVER)],
            ),
            NonzeroParticipantAckInvariantError::ParticipantMissing { participant_id: P0 },
        ),
        (
            episode_with(
                CONVERSATION_ID,
                vec![BoundParticipantCursor::new(P0, epoch(2, 0), OBSERVER)],
            ),
            NonzeroParticipantAckInvariantError::Generation {
                member: generation(1),
                episode: generation(2),
            },
        ),
        (
            episode_with(
                CONVERSATION_ID,
                vec![BoundParticipantCursor::new(P0, epoch(1, 0), 1)],
            ),
            NonzeroParticipantAckInvariantError::Cursor {
                member: OBSERVER,
                episode: 1,
            },
        ),
        (
            episode_with(
                CONVERSATION_ID,
                vec![BoundParticipantCursor::new(P0, epoch(1, 9), OBSERVER)],
            ),
            NonzeroParticipantAckInvariantError::BindingEpoch {
                active: epoch(1, 0),
                episode: epoch(1, 9),
            },
        ),
    ];

    for (subject, expected) in cases {
        let before = subject.clone();
        assert_eq!(
            apply_for(&live, epoch(1, 0), &ack, H, &subject),
            NonzeroParticipantAckDecision::Invariant(expected),
        );
        assert_eq!(subject, before);
        assert_eq!(live.cursor(), OBSERVER);
    }
}

#[test]
fn noop_gap_and_regression_return_wire_values_without_mutating_episode() {
    let subject = episode();
    let initial = subject.clone();
    let live = member(P0, OBSERVER);
    assert_eq!(
        apply_for(&live, epoch(1, 0), &request(P0, 1, OBSERVER), H, &subject,),
        NonzeroParticipantAckDecision::Respond(ServerValue::AckNoOp(AckNoOp::participant_ack(
            envelope(P0, OBSERVER)
        ),)),
    );
    assert_eq!(
        apply_for(&live, epoch(1, 0), &request(P0, 1, H + 1), H, &subject),
        NonzeroParticipantAckDecision::Respond(ServerValue::AckGap(
            AckGap::new(envelope(P0, H + 1), OBSERVER).expect("request above H is a gap"),
        )),
    );
    assert_eq!(subject, initial);

    let cursor_one_episode = episode_with(
        CONVERSATION_ID,
        vec![
            BoundParticipantCursor::new(P0, epoch(1, 0), 1),
            BoundParticipantCursor::new(P1, epoch(1, 1), OBSERVER),
        ],
    );
    let cursor_one_before = cursor_one_episode.clone();
    let cursor_one_member = member(P0, 1);
    assert_eq!(
        apply_for(
            &cursor_one_member,
            epoch(1, 0),
            &request(P0, 1, OBSERVER),
            H,
            &cursor_one_episode,
        ),
        NonzeroParticipantAckDecision::Respond(ServerValue::AckRegression(
            AckRegression::new(envelope(P0, OBSERVER), 1).expect("request is below durable cursor"),
        )),
    );
    assert_eq!(cursor_one_episode, cursor_one_before);
}

#[test]
fn aggregate_commit_rejects_unrelated_and_split_restored_prestates() {
    let old_member = member(P0, OBSERVER);
    let old_episode = episode();
    let commit = commit_for(&old_member, epoch(1, 0), 1, &old_episode);

    let mut unrelated_member = old_member.clone();
    let mut unrelated_episode = episode_with(
        CONVERSATION_ID,
        vec![BoundParticipantCursor::new(P0, epoch(1, 0), OBSERVER)],
    );
    let unrelated_before = unrelated_episode.clone();
    assert_eq!(
        commit
            .clone()
            .apply_to(&mut unrelated_member, &mut unrelated_episode),
        Err(NonzeroParticipantAckCommitError::EpisodePrestate),
    );
    assert_eq!(unrelated_member.cursor(), OBSERVER);
    assert_eq!(unrelated_episode, unrelated_before);

    let mut old_member_with_resulting_episode = old_member;
    let mut resulting_episode = commit.resulting_episode().clone();
    let resulting_before = resulting_episode.clone();
    assert_eq!(
        commit.clone().apply_to(
            &mut old_member_with_resulting_episode,
            &mut resulting_episode,
        ),
        Err(NonzeroParticipantAckCommitError::AggregateCursorPrestate {
            episode_position: NonzeroAckEpisodePosition::Resulting,
            expected_cursor: 1,
            actual_cursor: OBSERVER,
        },),
    );
    assert_eq!(old_member_with_resulting_episode.cursor(), OBSERVER);
    assert_eq!(resulting_episode, resulting_before);

    let mut resulting_member_with_old_episode = member(P0, 1);
    let mut still_old_episode = old_episode.clone();
    assert_eq!(
        commit.clone().apply_to(
            &mut resulting_member_with_old_episode,
            &mut still_old_episode,
        ),
        Err(NonzeroParticipantAckCommitError::AggregateCursorPrestate {
            episode_position: NonzeroAckEpisodePosition::Before,
            expected_cursor: OBSERVER,
            actual_cursor: 1,
        },),
    );
    assert_eq!(resulting_member_with_old_episode.cursor(), 1);
    assert_eq!(still_old_episode, old_episode);

    let mut wrong_identity = member_with(CONVERSATION_ID + 1, P0, 1, OBSERVER);
    let mut unchanged_episode = old_episode.clone();
    assert_eq!(
        commit.apply_to(&mut wrong_identity, &mut unchanged_episode),
        Err(NonzeroParticipantAckCommitError::Conversation {
            expected: CONVERSATION_ID,
            actual: CONVERSATION_ID + 1,
        }),
    );
    assert_eq!(wrong_identity.cursor(), OBSERVER);
    assert_eq!(unchanged_episode, old_episode);
}

#[test]
fn two_participants_ack_same_retained_suffix_through_total_wrapper() {
    // LP-EXTRACTION-GOAL.md Fix 2's exact blocker history now crosses common
    // authority, aggregate validation, episode transition, and atomic commit.
    let steps = [
        (P0, epoch(1, 0), 1),
        (P1, epoch(1, 1), 1),
        (P0, epoch(1, 0), 2),
        (P1, epoch(1, 1), 2),
    ];
    let mut members = [member(P0, OBSERVER), member(P1, OBSERVER)];
    let mut subject = episode();
    let mut durable_intermediates = Vec::new();

    for (participant_id, binding_epoch, boundary) in steps {
        let index = usize::try_from(participant_id).expect("fixture ids index the member array");
        let before_member = members[index].clone();
        let before_episode = subject.clone();
        let commit = commit_for(&members[index], binding_epoch, boundary, &subject);
        let expected = AckCommitted::new(envelope(participant_id, boundary));
        assert_eq!(commit.outcome(), &expected);

        let mut crash_member = before_member;
        let mut crash_episode = before_episode;
        assert_eq!(
            commit
                .clone()
                .apply_to(&mut crash_member, &mut crash_episode),
            Ok(expected.clone()),
        );
        assert_eq!(crash_member.cursor(), boundary);
        assert_eq!(&crash_episode, commit.resulting_episode());

        assert_eq!(
            commit.clone().apply_to(&mut members[index], &mut subject),
            Ok(expected.clone()),
        );
        assert_eq!(members[index].cursor(), boundary);
        assert_eq!(
            commit.apply_to(&mut members[index], &mut subject),
            Ok(expected),
            "replay from each durable resulting pair is identical",
        );

        assert_eq!(subject.retained_suffix_start(), Some(1));
        assert!(subject.retains(1));
        assert!(subject.retains(2));
        assert_eq!(subject.floor_computation().resulting_floor, 1);
        assert_eq!(subject.cap_floor(), 1);
        assert_eq!(
            subject.facts().get(CursorProgressKey {
                participant_index: participant_id,
                boundary,
            }),
            Some(CursorProgressFact::Consumed),
        );
        durable_intermediates.push(subject.encode().expect("each variable fact map serializes"));
    }

    assert_eq!(members[0].cursor(), H);
    assert_eq!(members[1].cursor(), H);
    for participant_index in [P0, P1] {
        for boundary in [1, 2] {
            assert_eq!(
                subject.facts().get(CursorProgressKey {
                    participant_index,
                    boundary,
                }),
                Some(CursorProgressFact::Consumed),
            );
        }
    }
    assert_eq!(subject.facts().len(), 4);
    assert!(
        durable_intermediates
            .windows(2)
            .all(|states| states[0] != states[1])
    );
}
