#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use alloc::{vec, vec::Vec};

use crate::wire::{
    AckCommitted, AckGap, AckRegression, AttachSecret, BindingEpoch, BindingRequiredEnvelope,
    CommonStaleAuthorityEnvelope, ConnectionIncarnation, Generation, LeaveAttemptToken,
    LeaveRequest, NoBinding, ParticipantAck, ParticipantAckEnvelope, ParticipantAckResponse,
    ParticipantReferenceEnvelope, ParticipantUnknown, Retired, StaleAuthority,
};

use super::super::{
    ActiveBinding, AttachSecretProof, BindingState, DetachCell, EnrollmentFingerprint,
    IdentityState, LeaveCommitParameters, LeaveFingerprint, LiveMember, LiveMemberRestore,
    PresentedIdentity, RecipientAckObligations, RecipientAckObligationsContextError, commit_leave,
    test_support::settled_leave_authority,
};
use super::participant_ack::{
    ParticipantAckCommit, ParticipantAckCommitError, ParticipantAckDecision, apply_participant_ack,
    apply_participant_ack_with_obligations,
};

const CONVERSATION_ID: u64 = 71;
const PARTICIPANT_ID: u64 = 17;
const CURRENT_CURSOR: u64 = 5;

type TestIdentity = IdentityState<Vec<u8>, Vec<u8>, Vec<u8>>;

fn generation(value: u64) -> Generation {
    Generation::new(value).expect("test generation must be nonzero")
}

fn epoch(generation_value: u64, connection_ordinal: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(71, connection_ordinal),
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
        attach_secret: AttachSecret::new([0xA7; 32]),
        cursor,
        enrollment_fingerprint: EnrollmentFingerprint::new(vec![1, 2, 3]),
        latest_terminal: None,
    })
    .expect("test member has valid empty terminal history")
}

fn member(cursor: u64) -> LiveMember<Vec<u8>> {
    member_with(CONVERSATION_ID, PARTICIPANT_ID, 3, cursor)
}

fn binding() -> BindingState {
    BindingState::Bound(ActiveBinding {
        participant_id: PARTICIPANT_ID,
        conversation_id: CONVERSATION_ID,
        binding_epoch: epoch(3, 9),
    })
}

fn request(generation_value: u64, through_seq: u64) -> ParticipantAck {
    ParticipantAck {
        conversation_id: CONVERSATION_ID,
        participant_id: PARTICIPANT_ID,
        capability_generation: generation(generation_value),
        through_seq,
    }
}

fn envelope(generation_value: u64, through_seq: u64) -> ParticipantAckEnvelope {
    ParticipantAckEnvelope {
        conversation_id: CONVERSATION_ID,
        participant_id: PARTICIPANT_ID,
        capability_generation: generation(generation_value),
        through_seq,
    }
}

fn retired_identity() -> TestIdentity {
    let member = member(CURRENT_CURSOR);
    let authority = settled_leave_authority(&member, BindingState::Detached, 8, CURRENT_CURSOR + 1);
    let leave_request = LeaveRequest {
        conversation_id: CONVERSATION_ID,
        participant_id: PARTICIPANT_ID,
        capability_generation: generation(3),
        attach_secret: AttachSecret::new([0xA7; 32]),
        leave_attempt_token: LeaveAttemptToken::new([0x51; 16]),
    };
    let verified = member
        .verify_leave_request(
            &leave_request,
            AttachSecretProof::Verified,
            vec![4, 5],
            LeaveFingerprint::new(vec![6, 7]),
        )
        .expect("test Leave has exact authority");
    commit_leave(
        member,
        BindingState::Detached,
        DetachCell::<[u8; 1]>::default(),
        verified,
        authority,
        LeaveCommitParameters {
            left_delivery_seq: CURRENT_CURSOR + 1,
        },
    )
    .expect("test Leave creates a tombstone")
    .into_parts()
    .0
}

fn commit_for(member: &LiveMember<Vec<u8>>, through_seq: u64) -> ParticipantAckCommit {
    let identity: TestIdentity = IdentityState::Live(member.clone());
    let decision = apply_participant_ack(
        PresentedIdentity::from(Some(&identity)),
        &binding(),
        epoch(3, 9),
        &request(3, through_seq),
        through_seq,
    );
    let ParticipantAckDecision::Commit(commit) = decision else {
        panic!("test ack must produce a commit");
    };
    commit
}

#[test]
fn shared_lookup_precedence_is_preserved_before_ack_relations() {
    let stale_request = request(2, 4);
    let stale_envelope = envelope(2, 4);
    let retired = retired_identity();
    assert_eq!(
        apply_participant_ack(
            PresentedIdentity::from(Some(&retired)),
            &BindingState::Detached,
            epoch(3, 99),
            &stale_request,
            0,
        ),
        ParticipantAckDecision::Respond(ParticipantAckResponse::from_retired(
            Retired::Participant {
                request: ParticipantReferenceEnvelope::ParticipantAck(stale_envelope.clone()),
                retired_generation: generation(3),
            }
        )),
    );

    assert_eq!(
        apply_participant_ack::<Vec<u8>, Vec<u8>, Vec<u8>>(
            PresentedIdentity::Absent,
            &BindingState::Detached,
            epoch(3, 99),
            &stale_request,
            0,
        ),
        ParticipantAckDecision::Respond(ParticipantAckResponse::from_participant_unknown(
            ParticipantUnknown {
                request: ParticipantReferenceEnvelope::ParticipantAck(stale_envelope.clone()),
            }
        )),
    );

    let live: TestIdentity = IdentityState::Live(member(CURRENT_CURSOR));
    assert_eq!(
        apply_participant_ack(
            PresentedIdentity::from(Some(&live)),
            &BindingState::Detached,
            epoch(3, 99),
            &stale_request,
            0,
        ),
        ParticipantAckDecision::Respond(ParticipantAckResponse::from_stale_authority(
            StaleAuthority::Live {
                request: CommonStaleAuthorityEnvelope::ParticipantAck(stale_envelope),
                current_generation: generation(3),
            }
        )),
    );

    let current_request = request(3, 4);
    let current_envelope = envelope(3, 4);
    let expected_no_binding =
        ParticipantAckDecision::Respond(ParticipantAckResponse::from_no_binding(NoBinding {
            request: BindingRequiredEnvelope::ParticipantAck(current_envelope),
        }));
    assert_eq!(
        apply_participant_ack(
            PresentedIdentity::from(Some(&live)),
            &BindingState::Detached,
            epoch(3, 9),
            &current_request,
            0,
        ),
        expected_no_binding,
    );
}

#[test]
fn wrong_receiving_epoch_is_no_binding_before_ack_relations() {
    let identity: TestIdentity = IdentityState::Live(member(CURRENT_CURSOR));
    let ack = request(3, 4);
    assert_eq!(
        apply_participant_ack(
            PresentedIdentity::from(Some(&identity)),
            &binding(),
            epoch(3, 10),
            &ack,
            0,
        ),
        ParticipantAckDecision::Respond(ParticipantAckResponse::from_no_binding(NoBinding {
            request: BindingRequiredEnvelope::ParticipantAck(envelope(3, 4)),
        })),
    );
}

#[test]
fn all_four_ack_relations_select_exact_wire_outcomes() {
    let identity: TestIdentity = IdentityState::Live(member(CURRENT_CURSOR));
    let presented = PresentedIdentity::from(Some(&identity));

    let regression = request(3, 4);
    assert_eq!(
        apply_participant_ack(presented, &binding(), epoch(3, 9), &regression, 9),
        ParticipantAckDecision::Respond(ParticipantAckResponse::ack_regression(
            AckRegression::new(envelope(3, 4), CURRENT_CURSOR)
                .expect("four is below the current cursor"),
        )),
    );

    let no_op = request(3, CURRENT_CURSOR);
    assert_eq!(
        apply_participant_ack(presented, &binding(), epoch(3, 9), &no_op, 0),
        ParticipantAckDecision::Respond(ParticipantAckResponse::ack_no_op(envelope(
            3,
            CURRENT_CURSOR,
        ))),
    );

    let gap = request(3, 7);
    assert_eq!(
        apply_participant_ack(presented, &binding(), epoch(3, 9), &gap, 6),
        ParticipantAckDecision::Respond(ParticipantAckResponse::ack_gap(
            AckGap::new(envelope(3, 7), CURRENT_CURSOR).expect("seven is above the current cursor"),
        )),
    );

    let committed = apply_participant_ack(presented, &binding(), epoch(3, 9), &request(3, 7), 7);
    let ParticipantAckDecision::Commit(commit) = committed else {
        panic!("contiguous forward ack must commit");
    };
    assert_eq!(commit.outcome(), &AckCommitted::new(envelope(3, 7)),);
}

#[test]
fn durable_obligation_endpoint_skips_internal_gaps_but_rejects_gap_endpoint() {
    let identity: TestIdentity = IdentityState::Live(member(CURRENT_CURSOR));
    let presented = PresentedIdentity::from(Some(&identity));
    let obligations =
        RecipientAckObligations::try_new(PARTICIPANT_ID, CURRENT_CURSOR, vec![6, 8, 10])
            .expect("fixture obligations are sorted, unique, and live");

    assert_eq!(
        apply_participant_ack_with_obligations(
            presented,
            &binding(),
            epoch(3, 9),
            &request(3, 7),
            &obligations,
        ),
        Ok(ParticipantAckDecision::Respond(
            ParticipantAckResponse::ack_gap(
                AckGap::new(envelope(3, 7), CURRENT_CURSOR)
                    .expect("non-obligation endpoint is above the cursor"),
            ),
        )),
    );

    let committed = apply_participant_ack_with_obligations(
        presented,
        &binding(),
        epoch(3, 9),
        &request(3, 8),
        &obligations,
    )
    .expect("testimony belongs to the authorized durable prestate");
    let ParticipantAckDecision::Commit(commit) = committed else {
        panic!("ack may skip internal non-obligations when its endpoint is durable")
    };
    assert_eq!(commit.outcome(), &AckCommitted::new(envelope(3, 8)));
}

#[test]
fn durable_obligation_context_mismatch_is_typed_invariant_not_ack_gap() {
    let identity: TestIdentity = IdentityState::Live(member(CURRENT_CURSOR));
    let presented = PresentedIdentity::from(Some(&identity));
    let wrong_participant =
        RecipientAckObligations::try_new(PARTICIPANT_ID + 1, CURRENT_CURSOR, vec![7])
            .expect("fixture obligation is structurally valid");
    assert_eq!(
        apply_participant_ack_with_obligations(
            presented,
            &binding(),
            epoch(3, 9),
            &request(3, 7),
            &wrong_participant,
        ),
        Err(RecipientAckObligationsContextError::Participant {
            expected: PARTICIPANT_ID,
            actual: PARTICIPANT_ID + 1,
        }),
    );

    let wrong_frontier =
        RecipientAckObligations::try_new(PARTICIPANT_ID, CURRENT_CURSOR - 1, vec![7])
            .expect("fixture obligation is structurally valid");
    assert_eq!(
        apply_participant_ack_with_obligations(
            presented,
            &binding(),
            epoch(3, 9),
            &request(3, 7),
            &wrong_frontier,
        ),
        Err(RecipientAckObligationsContextError::AcknowledgedThrough {
            expected: CURRENT_CURSOR,
            actual: CURRENT_CURSOR - 1,
        }),
    );
}

#[test]
fn refusals_and_noop_never_mutate_membership() {
    let member = member(CURRENT_CURSOR);
    let identity: TestIdentity = IdentityState::Live(member.clone());
    let presented = PresentedIdentity::from(Some(&identity));
    for (through_seq, contiguous) in [(4, 9), (CURRENT_CURSOR, 0), (7, 6)] {
        assert!(matches!(
            apply_participant_ack(
                presented,
                &binding(),
                epoch(3, 9),
                &request(3, through_seq),
                contiguous,
            ),
            ParticipantAckDecision::Respond(_)
        ));
    }
    let IdentityState::Live(stored) = &identity else {
        panic!("fixture remains live");
    };
    assert_eq!(stored.cursor(), CURRENT_CURSOR);
    assert_eq!(member.cursor(), CURRENT_CURSOR);
}

#[test]
fn commit_advances_exactly_once_and_crash_replay_is_idempotent() {
    let mut old_prestate = member(CURRENT_CURSOR);
    let commit = commit_for(&old_prestate, 7);
    let expected = AckCommitted::new(envelope(3, 7));

    assert_eq!(
        commit.clone().apply_to(&mut old_prestate),
        Ok(expected.clone()),
    );
    assert_eq!(old_prestate.cursor(), 7);
    assert_eq!(commit.clone().apply_to(&mut old_prestate), Ok(expected));
    assert_eq!(old_prestate.cursor(), 7);

    let mut restored_new_prestate = member(7);
    assert_eq!(
        commit.apply_to(&mut restored_new_prestate),
        Ok(AckCommitted::new(envelope(3, 7))),
    );
    assert_eq!(restored_new_prestate.cursor(), 7);
}

#[test]
fn opaque_update_validates_identity_generation_and_from_cursor_before_mutation() {
    let commit = commit_for(&member(CURRENT_CURSOR), 7);

    let mut wrong_conversation = member_with(72, PARTICIPANT_ID, 3, CURRENT_CURSOR);
    assert_eq!(
        commit.clone().apply_to(&mut wrong_conversation),
        Err(ParticipantAckCommitError::Conversation {
            expected: CONVERSATION_ID,
            actual: 72,
        }),
    );
    assert_eq!(wrong_conversation.cursor(), CURRENT_CURSOR);

    let mut wrong_participant = member_with(CONVERSATION_ID, 18, 3, CURRENT_CURSOR);
    assert_eq!(
        commit.clone().apply_to(&mut wrong_participant),
        Err(ParticipantAckCommitError::Participant {
            expected: PARTICIPANT_ID,
            actual: 18,
        }),
    );
    assert_eq!(wrong_participant.cursor(), CURRENT_CURSOR);

    let mut wrong_generation = member_with(CONVERSATION_ID, PARTICIPANT_ID, 4, CURRENT_CURSOR);
    assert_eq!(
        commit.clone().apply_to(&mut wrong_generation),
        Err(ParticipantAckCommitError::Generation {
            expected: generation(3),
            actual: generation(4),
        }),
    );
    assert_eq!(wrong_generation.cursor(), CURRENT_CURSOR);

    let mut wrong_cursor = member(6);
    assert_eq!(
        commit.apply_to(&mut wrong_cursor),
        Err(ParticipantAckCommitError::CursorPrestate {
            expected_from_cursor: CURRENT_CURSOR,
            resulting_cursor: 7,
            actual_cursor: 6,
        }),
    );
    assert_eq!(wrong_cursor.cursor(), 6);
}
