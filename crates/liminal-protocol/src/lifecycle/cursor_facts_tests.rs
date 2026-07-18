//! Regression for `docs/design/LP-EXTRACTION-GOAL.md` Fix 2.
//!
//! Two bound participants legally acknowledge the same retained suffix while
//! closure debt is nonzero. The frozen document's record-scoped fixed array has
//! only one cursor-progress group per record and cannot serialize this history.
//! Participant/boundary keys retain all four independent facts.

#![allow(clippy::expect_used)]

use alloc::{vec, vec::Vec};

use crate::algebra::{WideResourceVector, floor_transition};
use crate::wire::{
    AckCommitted, AckGap, AckNoOp, AckRegression, BindingEpoch, ConnectionIncarnation, Generation,
    ParticipantAck, ParticipantAckEnvelope,
};

use super::cursor_facts::{
    BoundParticipantCursor, CumulativeAckAuthorizationError, CumulativeAckOutcome,
    CursorProgressFact, CursorProgressKey, NonzeroDebtCursorEpisode, RecipientAckObligations,
    RecipientAckObligationsContextError, RecipientAckObligationsError,
};
use super::edge::ClosureDebt;

const CONVERSATION_ID: u64 = 54;
const P0_ID: u64 = 0;
const P1_ID: u64 = 1;
const OBSERVER_PROGRESS: u64 = 0;
const RETAINED_SUFFIX: [u64; 2] = [1, 2];

fn generation(value: u64) -> Generation {
    Generation::new(value).expect("fixture generations are nonzero")
}

fn epoch(connection_ordinal: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(54, connection_ordinal),
        generation(1),
    )
}

fn participant_ack(participant_id: u64, through_seq: u64) -> ParticipantAck {
    ParticipantAck {
        conversation_id: CONVERSATION_ID,
        participant_id,
        capability_generation: generation(1),
        through_seq,
    }
}

fn ack_envelope(participant_id: u64, through_seq: u64) -> ParticipantAckEnvelope {
    ParticipantAckEnvelope {
        conversation_id: CONVERSATION_ID,
        participant_id,
        capability_generation: generation(1),
        through_seq,
    }
}

fn episode() -> NonzeroDebtCursorEpisode {
    let debt = ClosureDebt::new(WideResourceVector::new(1, 4))
        .expect("the Fix 2 fixture requires nonzero debt");
    NonzeroDebtCursorEpisode::new(
        CONVERSATION_ID,
        debt,
        OBSERVER_PROGRESS,
        RETAINED_SUFFIX[1],
        u128::from(RETAINED_SUFFIX[0]),
        u128::from(RETAINED_SUFFIX[0]),
        vec![
            BoundParticipantCursor::new(P0_ID, epoch(0), OBSERVER_PROGRESS),
            BoundParticipantCursor::new(P1_ID, epoch(1), OBSERVER_PROGRESS),
        ],
    )
    .expect("fixture participant identities are unique")
}

type AckStep = (u64, u64, BindingEpoch, u64);

fn assert_retained_floor(subject: &NonzeroDebtCursorEpisode) {
    assert_eq!(subject.observer_progress(), OBSERVER_PROGRESS);
    assert_eq!(subject.candidate_high_watermark(), RETAINED_SUFFIX[1]);
    assert_eq!(subject.retained_suffix_start(), Some(RETAINED_SUFFIX[0]));
    assert_eq!(subject.cap_floor(), u128::from(RETAINED_SUFFIX[0]));
    assert!(
        RETAINED_SUFFIX
            .iter()
            .all(|boundary| subject.observer_progress() < *boundary)
    );
    assert!(
        RETAINED_SUFFIX
            .iter()
            .all(|delivery_seq| subject.retains(*delivery_seq))
    );
}

fn assert_post_ack_state(
    subject: &NonzeroDebtCursorEpisode,
    prior_cursors: &mut [u64; 2],
    floor_before: u128,
    cap_floor_before: u128,
    participant_index: u64,
    boundary: u64,
) {
    for index in [P0_ID, P1_ID] {
        let participant = subject
            .participant(index)
            .expect("both bound participants remain in the episode");
        assert_eq!(participant.participant_id(), index);
        assert_eq!(participant.participant_index(), index);
        let cursor = participant.cursor();
        let array_index = usize::try_from(index).expect("fixture indices fit usize");
        assert!(cursor >= prior_cursors[array_index]);
        prior_cursors[array_index] = cursor;
    }
    let minimum_cursor = *prior_cursors
        .iter()
        .min()
        .expect("the episode has two bound participants");
    let expected_floor = floor_transition(
        floor_before,
        Some(minimum_cursor),
        subject.candidate_high_watermark(),
        subject.observer_progress(),
        cap_floor_before,
    );
    assert_eq!(subject.floor_computation(), expected_floor);
    assert_eq!(subject.cap_floor(), expected_floor.resulting_floor);
    assert_eq!(
        expected_floor.preferred_floor,
        u128::from(RETAINED_SUFFIX[0])
    );
    assert_eq!(
        expected_floor.resulting_floor,
        u128::from(RETAINED_SUFFIX[0])
    );
    assert_retained_floor(subject);
    assert_eq!(
        subject.facts().get(CursorProgressKey {
            participant_index,
            boundary,
        }),
        Some(CursorProgressFact::Consumed)
    );
}

fn assert_all_facts_serialize(subject: &NonzeroDebtCursorEpisode, serialized_steps: &[Vec<u8>]) {
    let expected_fact_count = [P0_ID, P1_ID].len() * RETAINED_SUFFIX.len();
    assert_eq!(subject.facts().len(), expected_fact_count);
    for participant_index in [P0_ID, P1_ID] {
        for boundary in RETAINED_SUFFIX {
            assert_eq!(
                subject.facts().get(CursorProgressKey {
                    participant_index,
                    boundary,
                }),
                Some(CursorProgressFact::Consumed)
            );
        }
    }
    assert!(serialized_steps.windows(2).all(|pair| pair[0] != pair[1]));
    let encoded_facts = subject
        .facts()
        .encode()
        .expect("all participant-scoped facts fit the variable format");
    assert_eq!(
        &encoded_facts[..4],
        &u32::try_from(expected_fact_count)
            .expect("four facts fit the encoded count")
            .to_be_bytes()
    );
    assert!(
        serialized_steps
            .last()
            .is_some_and(|episode_bytes| episode_bytes.ends_with(&encoded_facts))
    );
}

fn assert_crash_replay(steps: &[AckStep], serialized_steps: &[Vec<u8>]) {
    let mut replay = episode();
    for (&(participant_index, participant_id, binding_epoch, boundary), expected_bytes) in
        steps.iter().zip(serialized_steps)
    {
        let high_watermark = replay.candidate_high_watermark();
        let _ = replay
            .acknowledge(
                participant_index,
                binding_epoch,
                &participant_ack(participant_id, boundary),
                high_watermark,
            )
            .expect("the replay has the identical authority and availability");
        assert_eq!(
            replay.encode().expect("the replay remains serializable"),
            *expected_bytes
        );
    }
}

#[test]
fn two_bound_participants_ack_the_same_retained_suffix_during_nonzero_debt() {
    // This is the exact history mandated by LP-EXTRACTION-GOAL.md Fix 2: each
    // participant advances over both retained boundaries, one boundary at a
    // time, while the same nonzero debt episode remains active.
    let steps = [
        (P0_ID, P0_ID, epoch(0), RETAINED_SUFFIX[0]),
        (P1_ID, P1_ID, epoch(1), RETAINED_SUFFIX[0]),
        (P0_ID, P0_ID, epoch(0), RETAINED_SUFFIX[1]),
        (P1_ID, P1_ID, epoch(1), RETAINED_SUFFIX[1]),
    ];
    let mut subject = episode();
    assert_eq!(subject.debt().value(), WideResourceVector::new(1, 4));
    assert_retained_floor(&subject);

    let mut prior_cursors = [
        subject
            .participant(P0_ID)
            .expect("P0 remains bound")
            .cursor(),
        subject
            .participant(P1_ID)
            .expect("P1 remains bound")
            .cursor(),
    ];
    let mut serialized_steps = Vec::new();
    for (participant_index, participant_id, binding_epoch, boundary) in steps {
        let floor_before = subject.floor_computation().resulting_floor;
        let cap_floor_before = subject.cap_floor();
        let high_watermark = subject.candidate_high_watermark();
        let outcome = subject
            .acknowledge(
                participant_index,
                binding_epoch,
                &participant_ack(participant_id, boundary),
                high_watermark,
            )
            .expect("each step has exact current binding authority");
        assert_eq!(
            outcome,
            CumulativeAckOutcome::Committed(AckCommitted::new(ack_envelope(
                participant_id,
                boundary,
            )))
        );

        assert_post_ack_state(
            &subject,
            &mut prior_cursors,
            floor_before,
            cap_floor_before,
            participant_index,
            boundary,
        );

        let encoded = subject
            .encode()
            .expect("four variable participant facts fit the storage format");
        assert_eq!(
            encoded,
            subject
                .encode()
                .expect("re-encoding the same state is deterministic")
        );
        serialized_steps.push(encoded);
    }

    assert_eq!(prior_cursors, [RETAINED_SUFFIX[1]; 2]);
    assert_all_facts_serialize(&subject, &serialized_steps);
    assert_crash_replay(&steps, &serialized_steps);
}

#[test]
fn cumulative_ack_selector_never_mutates_on_noop_gap_or_regression() {
    let mut episode = episode();
    let initial = episode.encode().expect("empty fact map is serializable");
    let high_watermark = episode.candidate_high_watermark();
    let beyond_suffix = high_watermark
        .checked_add(1)
        .expect("fixture retained suffix is below sequence exhaustion");

    assert_eq!(
        episode
            .acknowledge(
                P0_ID,
                epoch(0),
                &participant_ack(P0_ID, OBSERVER_PROGRESS),
                high_watermark,
            )
            .expect("authority matches"),
        CumulativeAckOutcome::NoOp(AckNoOp::participant_ack(ack_envelope(
            P0_ID,
            OBSERVER_PROGRESS,
        )))
    );
    assert_eq!(episode.encode().expect("no-op state serializes"), initial);

    assert_eq!(
        episode
            .acknowledge(
                P0_ID,
                epoch(0),
                &participant_ack(P0_ID, beyond_suffix),
                high_watermark,
            )
            .expect("authority matches"),
        CumulativeAckOutcome::Gap(
            AckGap::new(ack_envelope(P0_ID, beyond_suffix), OBSERVER_PROGRESS,)
                .expect("fixture request is strictly above the cursor")
        )
    );
    assert_eq!(episode.encode().expect("gap state serializes"), initial);

    assert!(matches!(
        episode
            .acknowledge(
                P0_ID,
                epoch(0),
                &participant_ack(P0_ID, RETAINED_SUFFIX[1]),
                high_watermark,
            )
            .expect("authority matches"),
        CumulativeAckOutcome::Committed(_)
    ));
    let after_commit = episode.encode().expect("committed state serializes");
    assert_eq!(
        episode
            .acknowledge(
                P0_ID,
                epoch(0),
                &participant_ack(P0_ID, RETAINED_SUFFIX[0]),
                high_watermark,
            )
            .expect("authority matches"),
        CumulativeAckOutcome::Regression(
            AckRegression::new(ack_envelope(P0_ID, RETAINED_SUFFIX[0]), RETAINED_SUFFIX[1],)
                .expect("fixture request is strictly below the cursor")
        )
    );
    assert_eq!(
        episode.encode().expect("regression state serializes"),
        after_commit
    );
    assert_eq!(
        episode
            .participant(P0_ID)
            .expect("participant remains bound")
            .cursor(),
        RETAINED_SUFFIX[1]
    );
}

#[test]
fn binding_epoch_authority_failure_preserves_cursors_and_facts() {
    let mut episode = episode();
    let before = episode.encode().expect("initial episode is serializable");
    let high_watermark = episode.candidate_high_watermark();

    assert_eq!(
        episode.acknowledge(
            P0_ID,
            epoch(99),
            &participant_ack(P0_ID, RETAINED_SUFFIX[0]),
            high_watermark,
        ),
        Err(CumulativeAckAuthorizationError::BindingEpochMismatch)
    );
    assert_eq!(
        episode.encode().expect("refused episode is serializable"),
        before
    );
    assert!(episode.facts().is_empty());
}

#[test]
fn recipient_obligation_testimony_requires_a_sorted_live_index() {
    assert_eq!(
        RecipientAckObligations::try_new(P0_ID, 5, vec![5]),
        Err(RecipientAckObligationsError::NotLive {
            acknowledged_through: 5,
            delivery_seq: 5,
        })
    );
    assert_eq!(
        RecipientAckObligations::try_new(P0_ID, 5, vec![7, 7]),
        Err(RecipientAckObligationsError::NotStrictlyIncreasing {
            previous: 7,
            current: 7,
        })
    );
    assert_eq!(
        RecipientAckObligations::try_new(P0_ID, 5, vec![8, 7]),
        Err(RecipientAckObligationsError::NotStrictlyIncreasing {
            previous: 8,
            current: 7,
        })
    );
}

#[test]
fn nonzero_ack_uses_durable_endpoint_membership_not_conversation_contiguity() {
    let obligations = RecipientAckObligations::try_new(P0_ID, OBSERVER_PROGRESS, vec![2])
        .expect("sequence two is P0's only live obligation");
    let mut gap_subject = episode();
    let before = gap_subject
        .encode()
        .expect("gap prestate remains serializable");
    assert_eq!(
        gap_subject
            .acknowledge_with_obligations(
                P0_ID,
                epoch(0),
                &participant_ack(P0_ID, 1),
                &obligations,
            )
            .expect("request and testimony have exact authority"),
        CumulativeAckOutcome::Gap(
            AckGap::new(ack_envelope(P0_ID, 1), OBSERVER_PROGRESS)
                .expect("sequence one is above the cursor")
        )
    );
    assert_eq!(
        gap_subject
            .encode()
            .expect("gap poststate remains serializable"),
        before
    );

    let mut committed_subject = episode();
    assert!(matches!(
        committed_subject
            .acknowledge_with_obligations(
                P0_ID,
                epoch(0),
                &participant_ack(P0_ID, 2),
                &obligations,
            )
            .expect("endpoint two is a durable P0 obligation"),
        CumulativeAckOutcome::Committed(_)
    ));
    assert_eq!(
        committed_subject
            .participant(P0_ID)
            .expect("P0 remains bound")
            .cursor(),
        2
    );
}

#[test]
fn nonzero_ack_rejects_testimony_from_another_durable_prestate() {
    let wrong_frontier = RecipientAckObligations::try_new(P0_ID, 1, vec![2])
        .expect("testimony is structurally valid but belongs to cursor one");
    let mut subject = episode();
    assert_eq!(
        subject.acknowledge_with_obligations(
            P0_ID,
            epoch(0),
            &participant_ack(P0_ID, 2),
            &wrong_frontier,
        ),
        Err(CumulativeAckAuthorizationError::ObligationContext {
            error: RecipientAckObligationsContextError::AcknowledgedThrough {
                expected: OBSERVER_PROGRESS,
                actual: 1,
            },
        })
    );
}
