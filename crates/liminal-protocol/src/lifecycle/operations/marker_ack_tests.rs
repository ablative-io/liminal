#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use alloc::{vec, vec::Vec};

use crate::algebra::WideResourceVector;
use crate::wire::{
    AckNoOp, AttachSecret, BindingEpoch, BindingRequiredEnvelope, CommonStaleAuthorityEnvelope,
    ConnectionIncarnation, Generation, LeaveAttemptToken, LeaveRequest, MarkerAck,
    MarkerAckCommitted, MarkerAckEnvelope, MarkerAckProof, MarkerMismatch, MarkerMismatchBody,
    MarkerNotDelivered, MarkerNotDeliveredReason, MarkerProofRequest, ParticipantReferenceEnvelope,
    ParticipantUnknown, Retired, ServerValue, StaleAuthority,
};

use super::{
    super::{
        ActiveBinding, AttachSecretProof, BindingState, DetachCell, EnrollmentFingerprint,
        IdentityState, LeaveCommitParameters, LeaveFingerprint, LiveMember, LiveMemberRestore,
        PresentedIdentity, commit_leave,
        edge::{ClosureDebt, ClosureState, Event, StoredEdge, marker_delivery_for_test},
        test_support::settled_leave_authority,
    },
    marker_ack::{MarkerAckCommit, MarkerAckCommitError, MarkerAckDecision, apply_marker_ack},
    marker_proof::{MarkerProofInput, MarkerProofState},
};

const CONVERSATION_ID: u64 = 72;
const PARTICIPANT_ID: u64 = 18;
const CURRENT_CURSOR: u64 = 10;
const MARKER_SEQ: u64 = 20;

type TestIdentity = IdentityState<Vec<u8>, Vec<u8>, Vec<u8>>;

fn generation(value: u64) -> Generation {
    Generation::new(value).expect("test generation must be nonzero")
}

fn epoch(generation_value: u64, connection_ordinal: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(72, connection_ordinal),
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

fn request(generation_value: u64, marker_delivery_seq: u64) -> MarkerAck {
    MarkerAck {
        conversation_id: CONVERSATION_ID,
        participant_id: PARTICIPANT_ID,
        capability_generation: generation(generation_value),
        marker_delivery_seq,
    }
}

fn envelope(generation_value: u64, marker_delivery_seq: u64) -> MarkerAckEnvelope {
    MarkerAckEnvelope {
        conversation_id: CONVERSATION_ID,
        participant_id: PARTICIPANT_ID,
        capability_generation: generation(generation_value),
        marker_delivery_seq,
    }
}

fn proof_request(generation_value: u64, marker_delivery_seq: u64) -> MarkerProofRequest {
    MarkerProofRequest::MarkerAck(MarkerAckProof {
        conversation_id: CONVERSATION_ID,
        participant_id: PARTICIPANT_ID,
        capability_generation: generation(generation_value),
        requested_marker_delivery_seq: marker_delivery_seq,
    })
}

fn marker_progress(
    participant_id: u64,
    binding_epoch: BindingEpoch,
    marker_delivery_seq: u64,
) -> super::super::ParticipantCursorProgress {
    let debt = ClosureDebt::new(WideResourceVector::new(1, 1))
        .expect("marker fixture has nonzero closure debt");
    let delivery = marker_delivery_for_test(participant_id, binding_epoch, marker_delivery_seq)
        .expect("validated marker fixture restores");
    let state = delivery
        .delivered(
            debt,
            Event::marker_delivered(participant_id, binding_epoch, marker_delivery_seq),
        )
        .expect("exact delivery derives cursor progress");
    let ClosureState::Owed {
        edge: StoredEdge::ParticipantCursorProgress(progress),
        ..
    } = state
    else {
        panic!("marker delivery must derive marker-backed cursor progress")
    };
    progress
}

fn state(
    supplied_cursor: u64,
    accepted_marker_at_cursor: bool,
    expected_marker_delivery_seq: Option<u64>,
    supplied_proof_epoch: BindingEpoch,
    progress: Option<super::super::ParticipantCursorProgress>,
) -> MarkerProofState {
    MarkerProofState::new(
        supplied_cursor,
        accepted_marker_at_cursor,
        expected_marker_delivery_seq,
        supplied_proof_epoch,
        progress,
    )
}

fn exact_state() -> MarkerProofState {
    state(
        CURRENT_CURSOR,
        false,
        Some(MARKER_SEQ),
        epoch(3, 9),
        Some(marker_progress(PARTICIPANT_ID, epoch(3, 9), MARKER_SEQ)),
    )
}

fn retired_identity() -> TestIdentity {
    let member = member(CURRENT_CURSOR);
    let authority =
        settled_leave_authority(&member, BindingState::Detached, 30, CURRENT_CURSOR + 1);
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

fn commit_for(member: &LiveMember<Vec<u8>>) -> MarkerAckCommit {
    let identity: TestIdentity = IdentityState::Live(member.clone());
    let decision = apply_marker_ack(
        PresentedIdentity::from(Some(&identity)),
        &binding(),
        epoch(3, 9),
        &request(3, MARKER_SEQ),
        &exact_state(),
    );
    let MarkerAckDecision::Commit(commit) = decision else {
        panic!("exact delivered marker must produce a commit")
    };
    commit
}

#[test]
fn shared_lookup_precedence_runs_before_marker_proof() {
    let stale_request = request(2, MARKER_SEQ);
    let retired = retired_identity();
    assert_eq!(
        apply_marker_ack(
            PresentedIdentity::from(Some(&retired)),
            &BindingState::Detached,
            epoch(3, 99),
            &stale_request,
            &exact_state(),
        ),
        MarkerAckDecision::Respond(ServerValue::Retired(Retired::Participant {
            request: ParticipantReferenceEnvelope::MarkerAck(envelope(2, MARKER_SEQ)),
            retired_generation: generation(3),
        })),
    );

    assert_eq!(
        apply_marker_ack::<Vec<u8>, Vec<u8>, Vec<u8>>(
            PresentedIdentity::Absent,
            &BindingState::Detached,
            epoch(3, 99),
            &stale_request,
            &exact_state(),
        ),
        MarkerAckDecision::Respond(ServerValue::ParticipantUnknown(ParticipantUnknown {
            request: ParticipantReferenceEnvelope::MarkerAck(envelope(2, MARKER_SEQ)),
        })),
    );

    let live: TestIdentity = IdentityState::Live(member(CURRENT_CURSOR));
    assert_eq!(
        apply_marker_ack(
            PresentedIdentity::from(Some(&live)),
            &BindingState::Detached,
            epoch(3, 99),
            &stale_request,
            &exact_state(),
        ),
        MarkerAckDecision::Respond(ServerValue::StaleAuthority(StaleAuthority::Live {
            request: CommonStaleAuthorityEnvelope::MarkerAck(envelope(2, MARKER_SEQ)),
            current_generation: generation(3),
        })),
    );

    assert_eq!(
        apply_marker_ack(
            PresentedIdentity::from(Some(&live)),
            &BindingState::Detached,
            epoch(3, 9),
            &request(3, MARKER_SEQ),
            &exact_state(),
        ),
        MarkerAckDecision::Respond(ServerValue::NoBinding(crate::wire::NoBinding {
            request: BindingRequiredEnvelope::MarkerAck(envelope(3, MARKER_SEQ)),
        })),
    );
}

#[test]
fn wrong_receiving_epoch_is_no_binding_before_marker_proof() {
    let identity: TestIdentity = IdentityState::Live(member(CURRENT_CURSOR));
    assert_eq!(
        apply_marker_ack(
            PresentedIdentity::from(Some(&identity)),
            &binding(),
            epoch(3, 10),
            &request(3, MARKER_SEQ),
            &exact_state(),
        ),
        MarkerAckDecision::Respond(ServerValue::NoBinding(crate::wire::NoBinding {
            request: BindingRequiredEnvelope::MarkerAck(envelope(3, MARKER_SEQ)),
        })),
    );
}

#[test]
fn marker_refusals_and_exact_accepted_replay_are_typed_and_nonmutating() {
    let original_member = member(CURRENT_CURSOR);
    let identity: TestIdentity = IdentityState::Live(original_member.clone());
    let presented = PresentedIdentity::from(Some(&identity));

    let below = request(3, CURRENT_CURSOR - 1);
    assert_eq!(
        apply_marker_ack(presented, &binding(), epoch(3, 9), &below, &exact_state(),),
        MarkerAckDecision::Respond(ServerValue::MarkerMismatch(MarkerMismatch {
            request: proof_request(3, CURRENT_CURSOR - 1),
            mismatch: MarkerMismatchBody::BelowCursor {
                current_cursor: CURRENT_CURSOR,
            },
        })),
    );

    let no_expected = state(CURRENT_CURSOR, false, None, epoch(3, 9), None);
    assert_eq!(
        apply_marker_ack(
            presented,
            &binding(),
            epoch(3, 9),
            &request(3, MARKER_SEQ),
            &no_expected,
        ),
        MarkerAckDecision::Respond(ServerValue::MarkerMismatch(MarkerMismatch {
            request: proof_request(3, MARKER_SEQ),
            mismatch: MarkerMismatchBody::NoMarkerExpected,
        })),
    );

    let undelivered = state(CURRENT_CURSOR, false, Some(MARKER_SEQ), epoch(3, 9), None);
    assert_eq!(
        apply_marker_ack(
            presented,
            &binding(),
            epoch(3, 9),
            &request(3, MARKER_SEQ),
            &undelivered,
        ),
        MarkerAckDecision::Respond(ServerValue::MarkerNotDelivered(MarkerNotDelivered {
            request: proof_request(3, MARKER_SEQ),
            reason: MarkerNotDeliveredReason::NotDeliveredToProofEpoch,
            expected_marker_delivery_seq: MARKER_SEQ,
        })),
    );

    let accepted_member = member(MARKER_SEQ);
    let accepted_identity: TestIdentity = IdentityState::Live(accepted_member.clone());
    let accepted = state(999, true, None, epoch(3, 99), None);
    assert_eq!(
        apply_marker_ack(
            PresentedIdentity::from(Some(&accepted_identity)),
            &binding(),
            epoch(3, 9),
            &request(3, MARKER_SEQ),
            &accepted,
        ),
        MarkerAckDecision::Respond(ServerValue::AckNoOp(AckNoOp::marker_ack(envelope(
            3, MARKER_SEQ,
        )))),
    );

    assert_eq!(original_member.cursor(), CURRENT_CURSOR);
    assert_eq!(accepted_member.cursor(), MARKER_SEQ);
}

#[test]
fn exact_delivery_uses_authorized_cursor_and_epoch_and_retains_proof() {
    let live_member = member(CURRENT_CURSOR);
    let identity: TestIdentity = IdentityState::Live(live_member);
    let caller_supplied_stale_values = state(
        100,
        false,
        Some(MARKER_SEQ),
        epoch(3, 99),
        Some(marker_progress(PARTICIPANT_ID, epoch(3, 9), MARKER_SEQ)),
    );
    let decision = apply_marker_ack(
        PresentedIdentity::from(Some(&identity)),
        &binding(),
        epoch(3, 9),
        &request(3, MARKER_SEQ),
        &caller_supplied_stale_values,
    );
    let MarkerAckDecision::Commit(commit) = decision else {
        panic!("member cursor and authorized epoch must replace supplied selector values")
    };
    assert_eq!(
        commit.outcome(),
        &MarkerAckCommitted::new(envelope(3, MARKER_SEQ)),
    );
    assert_eq!(commit.proof().proof_binding_epoch(), epoch(3, 9));
    assert_eq!(
        commit.proof().operation(),
        &MarkerProofInput::marker_ack(&request(3, MARKER_SEQ)),
    );
    assert_eq!(
        commit.proof().progress().marker_delivery_seq(),
        Some(MARKER_SEQ),
    );
}

#[test]
fn commit_advances_exactly_once_and_crash_replay_is_idempotent() {
    let mut old_prestate = member(CURRENT_CURSOR);
    let commit = commit_for(&old_prestate);
    let expected = MarkerAckCommitted::new(envelope(3, MARKER_SEQ));

    assert_eq!(
        commit.clone().apply_to(&mut old_prestate),
        Ok(expected.clone()),
    );
    assert_eq!(old_prestate.cursor(), MARKER_SEQ);
    assert_eq!(commit.clone().apply_to(&mut old_prestate), Ok(expected));
    assert_eq!(old_prestate.cursor(), MARKER_SEQ);

    let mut restored_new_prestate = member(MARKER_SEQ);
    assert_eq!(
        commit.apply_to(&mut restored_new_prestate),
        Ok(MarkerAckCommitted::new(envelope(3, MARKER_SEQ))),
    );
    assert_eq!(restored_new_prestate.cursor(), MARKER_SEQ);
}

#[test]
fn opaque_update_validates_identity_generation_and_from_cursor_before_mutation() {
    let commit = commit_for(&member(CURRENT_CURSOR));

    let mut wrong_conversation = member_with(73, PARTICIPANT_ID, 3, CURRENT_CURSOR);
    assert_eq!(
        commit.clone().apply_to(&mut wrong_conversation),
        Err(MarkerAckCommitError::Conversation {
            expected: CONVERSATION_ID,
            actual: 73,
        }),
    );
    assert_eq!(wrong_conversation.cursor(), CURRENT_CURSOR);

    let mut wrong_participant = member_with(CONVERSATION_ID, 19, 3, CURRENT_CURSOR);
    assert_eq!(
        commit.clone().apply_to(&mut wrong_participant),
        Err(MarkerAckCommitError::Participant {
            expected: PARTICIPANT_ID,
            actual: 19,
        }),
    );
    assert_eq!(wrong_participant.cursor(), CURRENT_CURSOR);

    let mut wrong_generation = member_with(CONVERSATION_ID, PARTICIPANT_ID, 4, CURRENT_CURSOR);
    assert_eq!(
        commit.clone().apply_to(&mut wrong_generation),
        Err(MarkerAckCommitError::Generation {
            expected: generation(3),
            actual: generation(4),
        }),
    );
    assert_eq!(wrong_generation.cursor(), CURRENT_CURSOR);

    let mut wrong_cursor = member(CURRENT_CURSOR + 1);
    assert_eq!(
        commit.apply_to(&mut wrong_cursor),
        Err(MarkerAckCommitError::CursorPrestate {
            expected_from_cursor: CURRENT_CURSOR,
            resulting_cursor: MARKER_SEQ,
            actual_cursor: CURRENT_CURSOR + 1,
        }),
    );
    assert_eq!(wrong_cursor.cursor(), CURRENT_CURSOR + 1);
}
