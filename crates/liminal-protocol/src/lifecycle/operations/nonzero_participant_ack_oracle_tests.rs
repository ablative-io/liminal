use alloc::{boxed::Box, format};
use std::error::Error;

use super::*;

fn sealed_obligations(
    delivery_sequences: Vec<u64>,
) -> Result<RecipientAckObligations, Box<dyn Error>> {
    RecipientAckObligations::try_new(P0, OBSERVER, delivery_sequences)
        .map_err(|error| format!("recipient obligation fixture was invalid: {error:?}").into())
}

#[test]
fn nonzero_debt_obligation_and_scalar_commit_cannot_diverge() -> Result<(), Box<dyn Error>> {
    let member = member(P0, OBSERVER);
    let identity: TestIdentity = IdentityState::Live(member.clone());
    let binding_epoch = epoch(1, 0);
    let binding = binding(P0, binding_epoch);
    let request = request(P0, 1, 1);
    let episode = episode();
    let obligations = sealed_obligations(vec![1, H])?;

    let obligation_decision = apply_nonzero_participant_ack_with_obligations(
        PresentedIdentity::from(Some(&identity)),
        &binding,
        binding_epoch,
        &request,
        &obligations,
        &episode,
    );
    let scalar_decision = apply_nonzero_participant_ack(
        PresentedIdentity::from(Some(&identity)),
        &binding,
        binding_epoch,
        &request,
        H,
        &episode,
    );
    assert_eq!(obligation_decision, scalar_decision);

    let NonzeroParticipantAckDecision::Commit(obligation_commit) = obligation_decision else {
        return Err("obligation-aware exact endpoint did not commit".into());
    };
    let NonzeroParticipantAckDecision::Commit(scalar_commit) = scalar_decision else {
        return Err("scalar exact endpoint did not commit".into());
    };
    let obligation_frontier = apply_nonzero_participant_ack_frontier(
        frontier_owner(),
        obligation_commit.as_ref().clone(),
    )
    .map_err(|failure| {
        format!(
            "obligation frontier transition failed: {:?}",
            failure.error()
        )
    })?;
    let scalar_frontier =
        apply_nonzero_participant_ack_frontier(frontier_owner(), scalar_commit.as_ref().clone())
            .map_err(|failure| {
                format!("scalar frontier transition failed: {:?}", failure.error())
            })?;
    assert_eq!(obligation_frontier.owner(), scalar_frontier.owner());

    let mut obligation_member = member.clone();
    let mut obligation_episode = episode.clone();
    let obligation_outcome = obligation_commit
        .apply_to(&mut obligation_member, &mut obligation_episode)
        .map_err(|error| format!("obligation commit application failed: {error:?}"))?;
    let mut scalar_member = member;
    let mut scalar_episode = episode;
    let scalar_outcome = scalar_commit
        .apply_to(&mut scalar_member, &mut scalar_episode)
        .map_err(|error| format!("scalar commit application failed: {error:?}"))?;
    assert_eq!(obligation_outcome, scalar_outcome);
    assert_eq!(obligation_member, scalar_member);
    assert_eq!(obligation_episode, scalar_episode);
    Ok(())
}

#[test]
fn nonzero_debt_obligation_and_scalar_refusal_cannot_diverge() -> Result<(), Box<dyn Error>> {
    let request = request(P0, 1, 1);
    let episode = episode_with(
        CONVERSATION_ID + 1,
        vec![BoundParticipantCursor::new(P0, epoch(1, 0), OBSERVER)],
    );
    let before = episode.clone();
    let obligations = sealed_obligations(vec![1])?;

    let obligation_decision =
        apply_nonzero_participant_ack_with_obligations::<Vec<u8>, Vec<u8>, Vec<u8>>(
            PresentedIdentity::Absent,
            &BindingState::Detached,
            epoch(1, 0),
            &request,
            &obligations,
            &episode,
        );
    let scalar_decision = apply_nonzero_participant_ack::<Vec<u8>, Vec<u8>, Vec<u8>>(
        PresentedIdentity::Absent,
        &BindingState::Detached,
        epoch(1, 0),
        &request,
        H,
        &episode,
    );
    assert_eq!(obligation_decision, scalar_decision);
    assert!(matches!(
        obligation_decision,
        NonzeroParticipantAckDecision::Respond(ref response)
            if response.discriminant() == crate::wire::ServerDiscriminant::ParticipantUnknown
    ));
    assert_eq!(episode, before);
    Ok(())
}

#[test]
fn nonzero_debt_sparse_gap_never_selects_scalar_fallback() -> Result<(), Box<dyn Error>> {
    let member = member(P0, OBSERVER);
    let identity: TestIdentity = IdentityState::Live(member);
    let binding_epoch = epoch(1, 0);
    let binding = binding(P0, binding_epoch);
    let request = request(P0, 1, 1);
    let episode = episode();
    let before = episode.clone();
    let obligations = sealed_obligations(vec![H])?;

    let authority = apply_nonzero_participant_ack_with_obligations(
        PresentedIdentity::from(Some(&identity)),
        &binding,
        binding_epoch,
        &request,
        &obligations,
        &episode,
    );
    let diagnostic = apply_nonzero_participant_ack(
        PresentedIdentity::from(Some(&identity)),
        &binding,
        binding_epoch,
        &request,
        H,
        &episode,
    );
    assert!(matches!(
        authority,
        NonzeroParticipantAckDecision::Respond(ref response)
            if response.discriminant() == crate::wire::ServerDiscriminant::AckGap
    ));
    assert!(matches!(
        diagnostic,
        NonzeroParticipantAckDecision::Commit(_)
    ));
    assert_eq!(episode, before);
    Ok(())
}

#[test]
fn two_participants_ack_same_retained_suffix_through_total_wrapper() -> Result<(), Box<dyn Error>> {
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
        let index = usize::try_from(participant_id)?;
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
        assert!(subject.retains(H));
        assert_eq!(subject.floor_computation().resulting_floor, 1);
        assert_eq!(subject.cap_floor(), 1);
        assert_eq!(
            subject.facts().get(CursorProgressKey {
                participant_index: participant_id,
                boundary,
            }),
            Some(CursorProgressFact::Consumed),
        );
        durable_intermediates.push(
            subject
                .encode()
                .map_err(|error| format!("variable fact map did not serialize: {error:?}"))?,
        );
    }

    assert_eq!(members[0].cursor(), H);
    assert_eq!(members[1].cursor(), H);
    for participant_index in [P0, P1] {
        for boundary in [1, H] {
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
    Ok(())
}
