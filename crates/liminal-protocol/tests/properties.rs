#![allow(clippy::expect_used)]

use liminal_protocol::algebra::{
    AdmissibleFloor, WideResourceVector, admissible_installed_floor, floor_transition,
    marker_clamped_floor,
};
use liminal_protocol::lifecycle::{
    BoundParticipantCursor, ClosureDebt, CumulativeAckOutcome, NonzeroDebtCursorEpisode,
    ObserverProjection, PhysicalCompaction,
};
use liminal_protocol::wire::{BindingEpoch, ConnectionIncarnation, Generation, ParticipantAck};
use proptest::prelude::*;

const CONVERSATION_ID: u64 = 0x5052_4f50;

const fn epoch(participant_index: usize) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(7, participant_index as u64 + 1),
        Generation::ONE,
    )
}

fn episode(participant_count: usize, high_watermark: u64) -> NonzeroDebtCursorEpisode {
    let debt = ClosureDebt::new(WideResourceVector::new(1, 1))
        .expect("the property fixture always has nonzero debt");
    let participants = (0..participant_count)
        .map(|participant_index| {
            BoundParticipantCursor::new(participant_index as u64, epoch(participant_index), 0)
        })
        .collect();

    NonzeroDebtCursorEpisode::new(CONVERSATION_ID, debt, 0, high_watermark, 1, 1, participants)
        .expect("o=0 and F=cap_floor=1 retain every generated boundary")
}

const fn ack(participant_index: usize, through_seq: u64) -> ParticipantAck {
    ParticipantAck {
        conversation_id: CONVERSATION_ID,
        participant_id: participant_index as u64,
        capability_generation: Generation::ONE,
        through_seq,
    }
}

fn legal_interleaving(
    participant_count: usize,
    high_watermark: u64,
    choices: &[u8],
) -> Vec<(usize, u64)> {
    let mut next = vec![1_u64; participant_count];
    let boundary_count = usize::try_from(high_watermark)
        .expect("property high watermarks fit every supported target");
    let mut order = Vec::with_capacity(participant_count * boundary_count);

    for &choice in choices {
        let participant_index = usize::from(choice) % participant_count;
        if next[participant_index] <= high_watermark {
            order.push((participant_index, next[participant_index]));
            next[participant_index] += 1;
        }
    }

    loop {
        let mut advanced = false;
        for (participant_index, next_boundary) in next.iter_mut().enumerate() {
            if *next_boundary <= high_watermark {
                order.push((participant_index, *next_boundary));
                *next_boundary += 1;
                advanced = true;
            }
        }
        if !advanced {
            break;
        }
    }

    order
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// LP-EXTRACTION-GOAL Phase 2: every generated legal ack interleaving for
    /// one through four participants remains serializable, participant cursors
    /// never regress, and the physical floor is monotone.
    #[test]
    fn ack_interleavings_are_serializable_and_monotone(
        participant_count in 1_usize..=4,
        high_watermark in 1_u64..=8,
        choices in prop::collection::vec(any::<u8>(), 0..96),
    ) {
        let order = legal_interleaving(participant_count, high_watermark, &choices);
        let mut subject = episode(participant_count, high_watermark);
        let mut prior_cursors = vec![0_u64; participant_count];
        let mut prior_floor = subject.floor_computation().resulting_floor;

        for (participant_index, boundary) in order {
            let outcome = subject
                .acknowledge(
                    participant_index as u64,
                    epoch(participant_index),
                    &ack(participant_index, boundary),
                    high_watermark,
                )
                .expect("the generated interleaving always uses exact authority");
            prop_assert!(matches!(outcome, CumulativeAckOutcome::Committed(_)));

            for (index, prior_cursor) in prior_cursors.iter_mut().enumerate() {
                let cursor = subject
                    .participant(index as u64)
                    .expect("every generated participant remains bound")
                    .cursor();
                prop_assert!(cursor >= *prior_cursor);
                *prior_cursor = cursor;
            }

            let floor = subject.floor_computation().resulting_floor;
            prop_assert!(floor >= prior_floor);
            prop_assert_eq!(floor, 1);
            prior_floor = floor;
            prop_assert!(subject.encode().is_ok());
        }

        prop_assert_eq!(prior_cursors, vec![high_watermark; participant_count]);
        let boundary_count = usize::try_from(high_watermark)
            .expect("property high watermarks fit every supported target");
        prop_assert_eq!(subject.facts().len(), participant_count * boundary_count);
        for boundary in 1..=high_watermark {
            prop_assert!(subject.retains(boundary));
        }
    }

    /// Replaying an authority-checked cumulative-ack transition from the same
    /// durable prestate produces the identical typed outcome and poststate.
    #[test]
    fn crash_replay_of_ack_transition_is_identical(
        high_watermark in 1_u64..=32,
        cursor_seed in any::<u8>(),
        through_seed in any::<u8>(),
        offered_seed in any::<u8>(),
    ) {
        let cursor = u64::from(cursor_seed) % (high_watermark + 1);
        let through_seq = u64::from(through_seed) % (high_watermark + 3);
        let offered_through = u64::from(offered_seed) % (high_watermark + 1);
        let debt = ClosureDebt::new(WideResourceVector::new(3, 5))
            .expect("the replay fixture always has nonzero debt");
        let prestate = NonzeroDebtCursorEpisode::new(
            CONVERSATION_ID,
            debt,
            0,
            high_watermark,
            1,
            1,
            vec![BoundParticipantCursor::new(0, epoch(0), cursor)],
        )
        .expect("the generated cursor is bounded by H'");
        let request = ack(0, through_seq);
        let mut first = prestate.clone();
        let mut replay = prestate;

        let first_outcome = first.acknowledge(0, epoch(0), &request, offered_through);
        let replay_outcome = replay.acknowledge(0, epoch(0), &request, offered_through);

        prop_assert_eq!(first_outcome, replay_outcome);
        prop_assert_eq!(first, replay);
    }

    /// Pure floor, observer-projection, and physical-compaction selectors are
    /// byte-for-byte crash-replay stable for exact and inexact events.
    #[test]
    fn crash_replay_of_storage_selectors_is_identical(
        through_seq in 1_u64..=128,
        exact in any::<bool>(),
        member_cursor in 0_u64..=128,
        observer_progress in 0_u64..=128,
        current_floor in 0_u128..=129,
        cap_floor in 0_u128..=129,
    ) {
        let event_through = if exact { through_seq } else { through_seq - 1 };
        let projection = ObserverProjection::new(through_seq);
        let projection_event = liminal_protocol::lifecycle::Event::projection_completed(event_through);
        let first_projection = projection.clear_after_completion(&projection_event);
        let replay_projection = projection.clear_after_completion(&projection_event);
        prop_assert_eq!(first_projection, replay_projection);
        prop_assert_eq!(first_projection.is_some(), exact);

        let from_floor = through_seq / 2;
        let compaction = PhysicalCompaction::new(from_floor, through_seq)
            .expect("generated compaction range is ordered");
        let compaction_event = liminal_protocol::lifecycle::Event::compaction_completed(
            from_floor,
            event_through,
            through_seq + 1,
        )
        .expect("generated completion range is ordered");
        let first_compaction = compaction.clear_after_completion(&compaction_event);
        let replay_compaction = compaction.clear_after_completion(&compaction_event);
        prop_assert_eq!(first_compaction, replay_compaction);
        prop_assert_eq!(first_compaction.is_some(), exact);

        let first_floor = floor_transition(
            current_floor,
            Some(member_cursor),
            through_seq,
            observer_progress,
            cap_floor,
        );
        let replay_floor = floor_transition(
            current_floor,
            Some(member_cursor),
            through_seq,
            observer_progress,
            cap_floor,
        );
        prop_assert_eq!(first_floor, replay_floor);
        prop_assert!(first_floor.resulting_floor >= current_floor);
        prop_assert!(first_floor.resulting_floor >= cap_floor);
        prop_assert_eq!(
            first_floor.preferred_floor,
            u128::from(member_cursor.min(observer_progress)) + 1,
        );
    }

    /// PRECEDENCE-CLAMP M7. The marker clamp and the installed-floor interval,
    /// over the whole domain including both degenerate cases: no markers at
    /// all, and a marker sitting exactly on the floor.
    ///
    /// The clamp's whole job is to be the OPPOSITE of `cap_floor`, so the
    /// direction is asserted here rather than left to the two call sites.
    #[test]
    fn the_marker_clamp_lowers_and_the_installed_floor_stays_inside_its_interval(
        computed_floor in 0_u128..=260,
        marker in proptest::option::of(0_u64..=255),
        retained_floor in 0_u128..=260,
        high_watermark in 0_u64..=255,
    ) {
        let clamped = marker_clamped_floor(computed_floor, marker);

        // Direction: the clamp only ever LOWERS. `cap_floor` only ever raises,
        // and the two must never converge.
        prop_assert!(clamped <= computed_floor);
        let raised = floor_transition(computed_floor, None, high_watermark, high_watermark, computed_floor)
            .resulting_floor;
        prop_assert!(raised >= computed_floor);

        if let Some(marker) = marker {
            // A marker pins: the clamped floor never crosses it, and it lands
            // ON it rather than below it whenever the computed floor reaches it.
            prop_assert!(clamped <= u128::from(marker));
            if computed_floor >= u128::from(marker) {
                prop_assert_eq!(clamped, u128::from(marker));
            }
        } else {
            // The empty marker set is the majority case: byte-identical
            // pass-through, never a guess about the minimum of an empty set.
            prop_assert_eq!(clamped, computed_floor);
        }

        let retained_end = u128::from(high_watermark) + 1;
        match admissible_installed_floor(computed_floor, retained_floor, marker, high_watermark) {
            AdmissibleFloor::Install(installed) => {
                // Inside the interval BOTH ends, which is the whole point: a
                // floor clamped only downward can land below the retained floor
                // and be refused there instead.
                prop_assert!(installed >= retained_floor);
                prop_assert!(installed <= retained_end);
                // Never raised to reach the interval.
                prop_assert!(installed <= computed_floor);
                // Never across a marker.
                if let Some(marker) = marker {
                    prop_assert!(installed <= u128::from(marker));
                }
                // An uncontested floor installs byte-identically (F5).
                if computed_floor <= retained_end
                    && marker.is_none_or(|marker| computed_floor <= u128::from(marker))
                {
                    prop_assert_eq!(installed, computed_floor);
                }
            }
            // Subsumed exactly when no admissible value reaches the retained
            // floor. Never a refusal, and never an install running backwards.
            AdmissibleFloor::Subsumed => {
                let upper = marker_clamped_floor(retained_end, marker);
                prop_assert!(computed_floor.min(upper) < retained_floor);
            }
        }
    }
}
