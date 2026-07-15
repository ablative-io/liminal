//! Regression for `docs/design/LP-EXTRACTION-GOAL.md` Fix 2.
//!
//! Two bound participants legally acknowledge the same retained suffix while
//! closure debt is nonzero. The frozen document's record-scoped fixed array has
//! only one cursor-progress group per record and cannot serialize this history.
//! Participant/boundary keys retain all four independent facts.

#![allow(clippy::expect_used)]

use alloc::{vec, vec::Vec};

use crate::algebra::WideResourceVector;
use crate::wire::{
    AckCommitted, AckGap, AckNoOp, AckRegression, BindingEpoch, ConnectionIncarnation, Generation,
    ParticipantAck, ParticipantAckEnvelope,
};

use super::cursor_facts::{
    BoundParticipantCursor, CumulativeAckAuthorizationError, CumulativeAckOutcome,
    CursorProgressFact, CursorProgressKey, NonzeroDebtCursorEpisode,
};
use super::edge::ClosureDebt;

const CONVERSATION_ID: u64 = 54;
const P0_ID: u64 = 0;
const P1_ID: u64 = 1;

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
        vec![
            BoundParticipantCursor::new(P0_ID, epoch(0), 0),
            BoundParticipantCursor::new(P1_ID, epoch(1), 0),
        ],
    )
    .expect("fixture participant identities are unique")
}

#[test]
fn two_bound_participants_ack_the_same_retained_suffix_during_nonzero_debt() {
    // This is the exact history mandated by LP-EXTRACTION-GOAL.md Fix 2: each
    // participant advances over both retained boundaries, one boundary at a
    // time, while the same nonzero debt episode remains active.
    let steps = [
        (0, P0_ID, epoch(0), 1),
        (1, P1_ID, epoch(1), 1),
        (0, P0_ID, epoch(0), 2),
        (1, P1_ID, epoch(1), 2),
    ];
    let mut subject = episode();
    assert_eq!(subject.debt().value(), WideResourceVector::new(1, 4));

    let mut prior_cursors = [0, 0];
    let mut serialized_steps = Vec::new();
    for (participant_index, participant_id, binding_epoch, boundary) in steps {
        let outcome = subject
            .acknowledge(
                participant_index,
                binding_epoch,
                &participant_ack(participant_id, boundary),
                2,
            )
            .expect("each step has exact current binding authority");
        assert_eq!(
            outcome,
            CumulativeAckOutcome::Committed(AckCommitted::new(ack_envelope(
                participant_id,
                boundary,
            )))
        );

        for index in [0, 1] {
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
        assert_eq!(
            subject.facts().get(CursorProgressKey {
                participant_index,
                boundary,
            }),
            Some(CursorProgressFact::Consumed)
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

    assert_eq!(prior_cursors, [2, 2]);
    assert_eq!(subject.facts().len(), 4);
    for participant_index in [0, 1] {
        for boundary in [1, 2] {
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
    assert_eq!(serialized_steps.last().map(Vec::len), Some(196));

    let mut replay = episode();
    for ((participant_index, participant_id, binding_epoch, boundary), expected_bytes) in
        steps.into_iter().zip(&serialized_steps)
    {
        let _ = replay
            .acknowledge(
                participant_index,
                binding_epoch,
                &participant_ack(participant_id, boundary),
                2,
            )
            .expect("the replay has the identical authority and availability");
        assert_eq!(
            replay.encode().expect("the replay remains serializable"),
            *expected_bytes
        );
    }
}

#[test]
fn cumulative_ack_selector_never_mutates_on_noop_gap_or_regression() {
    let mut episode = episode();
    let initial = episode.encode().expect("empty fact map is serializable");

    assert_eq!(
        episode
            .acknowledge(0, epoch(0), &participant_ack(P0_ID, 0), 2)
            .expect("authority matches"),
        CumulativeAckOutcome::NoOp(AckNoOp::participant_ack(ack_envelope(P0_ID, 0)))
    );
    assert_eq!(episode.encode().expect("no-op state serializes"), initial);

    assert_eq!(
        episode
            .acknowledge(0, epoch(0), &participant_ack(P0_ID, 3), 2)
            .expect("authority matches"),
        CumulativeAckOutcome::Gap(
            AckGap::new(ack_envelope(P0_ID, 3), 0)
                .expect("fixture request is strictly above the cursor")
        )
    );
    assert_eq!(episode.encode().expect("gap state serializes"), initial);

    assert!(matches!(
        episode
            .acknowledge(0, epoch(0), &participant_ack(P0_ID, 2), 2)
            .expect("authority matches"),
        CumulativeAckOutcome::Committed(_)
    ));
    let after_commit = episode.encode().expect("committed state serializes");
    assert_eq!(
        episode
            .acknowledge(0, epoch(0), &participant_ack(P0_ID, 1), 2)
            .expect("authority matches"),
        CumulativeAckOutcome::Regression(
            AckRegression::new(ack_envelope(P0_ID, 1), 2)
                .expect("fixture request is strictly below the cursor")
        )
    );
    assert_eq!(
        episode.encode().expect("regression state serializes"),
        after_commit
    );
    assert_eq!(
        episode
            .participant(0)
            .expect("participant remains bound")
            .cursor(),
        2
    );
}

#[test]
fn binding_epoch_authority_failure_preserves_cursors_and_facts() {
    let mut episode = episode();
    let before = episode.encode().expect("initial episode is serializable");

    assert_eq!(
        episode.acknowledge(0, epoch(99), &participant_ack(P0_ID, 1), 2),
        Err(CumulativeAckAuthorizationError::BindingEpochMismatch)
    );
    assert_eq!(
        episode.encode().expect("refused episode is serializable"),
        before
    );
    assert!(episode.facts().is_empty());
}
