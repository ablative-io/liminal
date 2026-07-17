#![allow(clippy::expect_used, clippy::panic)]

use alloc::vec;
use alloc::vec::Vec;

use liminal_protocol::lifecycle::{
    ActiveBinding, AttachCommitParameters, AttachSecretProof, AttachedRecordPosition, BindingState,
    ClosureState, CommittedBindingTerminalPosition, DetachCell, EnrollmentFingerprint,
    IdentityState, LiveMember, LiveMemberRestore, ParticipantAckDecision, PresentedIdentity,
    apply_participant_ack, commit_attach, commit_detach,
};
use liminal_protocol::wire::{
    AckNoOp, AttachAttemptToken, AttachBound, AttachSecret, AuthenticationState, BindingEpoch,
    BindingStateView, ConnectionIncarnation, CredentialAttachRequest, DetachAttemptToken,
    DetachEnvelope, DetachInProgress, DetachRequest, DetachStaleAuthority, Generation,
    InboundGateContext, NegotiatedParticipantCapability, ParticipantAck,
    ParticipantCapabilityState, ParticipantFrame, ReceiverDirection, ServerValue, StaleAuthority,
    ValidatedFrameLimit, encode, encoded_len,
};

use super::*;

const CONVERSATION_ID: u64 = 71;
const PARTICIPANT_ID: u64 = 17;
const CURRENT_CURSOR: u64 = 5;

fn generation(value: u64) -> Generation {
    Generation::new(value).expect("test generation must be nonzero")
}

fn epoch(generation_value: u64, connection_ordinal: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(7, connection_ordinal),
        generation(generation_value),
    )
}

fn detach_request() -> DetachEnvelope {
    DetachEnvelope {
        conversation_id: CONVERSATION_ID,
        participant_id: PARTICIPANT_ID,
        capability_generation: generation(1),
        detach_attempt_token: DetachAttemptToken::new([0xD1; 16]),
    }
}

fn attach_bound() -> AttachBound {
    AttachBound::ordinary(
        CONVERSATION_ID,
        AttachAttemptToken::new([0xA2; 16]),
        PARTICIPANT_ID,
        generation(1),
        AttachSecret::new([0x5E; 32]),
        epoch(2, 9),
        CURRENT_CURSOR,
        100,
        200,
    )
    .expect("generation two is the successor of generation one")
}

fn context() -> InboundGateContext {
    let limit = ValidatedFrameLimit::new(1_048_576).expect("test frame limit is valid");
    InboundGateContext {
        receiver: ReceiverDirection::Client,
        authentication: AuthenticationState::Authenticated,
        participant_capability: ParticipantCapabilityState::Negotiated(
            NegotiatedParticipantCapability::v1(limit),
        ),
    }
}

fn loopback(session: &mut ParticipantLifecycle, value: ServerValue) -> ParticipantReceive {
    let frame = ParticipantFrame::ServerValue(value);
    let mut bytes = vec![0; encoded_len(&frame).expect("test server value has a wire length")];
    encode(&frame, &mut bytes).expect("test server value encodes");
    session.receive(&bytes, context())
}

#[test]
fn token_replays_after_response_loss_and_terminal_response_stops_replay() {
    let request = detach_request();
    let mut session = ParticipantLifecycle::new();
    session.record_detach(request.clone());

    assert_eq!(
        session.on_replay_event(DetachReplayEvent::ExplicitCallerAction),
        DetachReplayAction::Send(request.clone())
    );
    assert_eq!(
        session.on_replay_event(DetachReplayEvent::ProvedOnlineTransition),
        DetachReplayAction::None,
        "an in-flight attempt cannot be re-armed"
    );

    // The server committed, but its first response was lost with the connection.
    let committed = liminal_protocol::wire::DetachCommitted::new(
        CONVERSATION_ID,
        PARTICIPANT_ID,
        request.detach_attempt_token,
        epoch(1, 4),
        12,
    );
    session.replay_attempt_failed();
    assert_eq!(
        session.on_replay_event(DetachReplayEvent::EstablishedConnectionFate),
        DetachReplayAction::Send(request)
    );

    let ParticipantReceive::Outcome(outcome) = loopback(
        &mut session,
        ServerValue::DetachCommitted(committed.clone()),
    ) else {
        panic!("loopback must surface a typed outcome");
    };
    assert_eq!(outcome.value(), &ServerValue::DetachCommitted(committed));
    assert_eq!(outcome.transition(), ParticipantTransition::Detached);
    assert_eq!(session.state(), ParticipantClientState::Detached);
    assert_eq!(session.detach_replay_status(), DetachReplayStatus::Terminal);
    assert_eq!(
        session.on_replay_event(DetachReplayEvent::ExplicitCallerAction),
        DetachReplayAction::None
    );
}

#[test]
fn crash_resume_parks_the_exact_in_flight_detach_until_a_new_event() {
    let request = detach_request();
    let mut before_crash = ParticipantLifecycle::new();
    before_crash.record_detach(request.clone());
    assert_eq!(
        before_crash.on_replay_event(DetachReplayEvent::ExplicitCallerAction),
        DetachReplayAction::Send(request.clone())
    );

    let durable = before_crash.crash_state();
    let mut resumed = ParticipantLifecycle::resume(durable);

    assert_eq!(resumed.detach_replay_status(), DetachReplayStatus::Parked);
    assert_eq!(
        resumed.on_replay_event(DetachReplayEvent::ProvedOnlineTransition),
        DetachReplayAction::Send(request)
    );
}

#[test]
fn newer_attach_consumes_authority_and_old_detach_is_never_resent() {
    let mut session = ParticipantLifecycle::new();
    session.record_detach(detach_request());

    let ParticipantReceive::Outcome(outcome) =
        loopback(&mut session, ServerValue::AttachBound(attach_bound()))
    else {
        panic!("loopback must surface the attach outcome");
    };

    assert!(matches!(
        outcome.transition(),
        ParticipantTransition::AuthoritySuperseded(_)
    ));
    assert_eq!(
        session.detach_replay_status(),
        DetachReplayStatus::AuthoritySuperseded
    );
    assert_eq!(
        session.on_replay_event(DetachReplayEvent::EstablishedConnectionFate),
        DetachReplayAction::None
    );
    assert!(matches!(session.state(), ParticipantClientState::Bound(_)));
}

#[test]
fn durable_leave_cancels_old_detach_before_another_event() {
    let mut session = ParticipantLifecycle::new();
    session.record_detach(detach_request());

    session.record_leave_durable();

    assert_eq!(
        session.detach_replay_status(),
        DetachReplayStatus::LeaveSuperseded
    );
    assert_eq!(
        session.on_replay_event(DetachReplayEvent::ExplicitCallerAction),
        DetachReplayAction::None
    );
}

#[test]
fn detach_in_progress_is_a_typed_terminal_status() {
    let mut session = ParticipantLifecycle::new();
    session.record_detach(detach_request());
    let status = DetachInProgress {
        conversation_id: CONVERSATION_ID,
        participant_id: PARTICIPANT_ID,
        presented_token: DetachAttemptToken::new([0xB2; 16]),
        presented_generation: generation(1),
        committed_binding_epoch: epoch(1, 4),
    };

    let ParticipantReceive::Outcome(outcome) =
        loopback(&mut session, ServerValue::DetachInProgress(status.clone()))
    else {
        panic!("loopback must surface detach in progress");
    };

    assert_eq!(outcome.value(), &ServerValue::DetachInProgress(status));
    assert_eq!(
        outcome.transition(),
        ParticipantTransition::DetachInProgress
    );
    assert_eq!(session.detach_replay_status(), DetachReplayStatus::Terminal);
    assert_eq!(
        session.on_replay_event(DetachReplayEvent::ExplicitCallerAction),
        DetachReplayAction::None
    );
}

fn terminalized_value() -> (ServerValue, BindingEpoch) {
    let old_epoch = epoch(4, 11);
    let new_epoch = epoch(5, 12);
    let binding = ActiveBinding {
        participant_id: PARTICIPANT_ID,
        conversation_id: CONVERSATION_ID,
        binding_epoch: old_epoch,
    };
    let request = DetachRequest {
        conversation_id: CONVERSATION_ID,
        participant_id: PARTICIPANT_ID,
        capability_generation: generation(4),
        detach_attempt_token: DetachAttemptToken::new([0xD3; 16]),
    };
    let verifier = [0xA5; 32];
    let verified_detach = binding
        .verify_detach_request(request.clone(), verifier)
        .expect("request matches the old binding");
    let member = LiveMember::restore(LiveMemberRestore {
        participant_id: PARTICIPANT_ID,
        conversation_id: CONVERSATION_ID,
        generation: generation(4),
        attach_secret: AttachSecret::new([0x44; 32]),
        cursor: 0,
        enrollment_fingerprint: EnrollmentFingerprint::new([0xE1; 32]),
        latest_terminal: None,
    })
    .expect("terminalized fixture has valid membership");
    let transition = commit_detach(
        member,
        verified_detach,
        DetachCell::default(),
        CommittedBindingTerminalPosition::new(9, 44),
    )
    .expect("empty detach cell commits");
    let (member, _, _, committed, _) = transition.into_parts();
    let attach = CredentialAttachRequest {
        conversation_id: CONVERSATION_ID,
        participant_id: PARTICIPANT_ID,
        capability_generation: generation(4),
        attach_secret: member.attach_secret(),
        attach_attempt_token: AttachAttemptToken::new([0xA7; 16]),
        accept_marker_delivery_seq: None,
    };
    let verified_attach = member
        .verify_detached_attach(
            BindingState::Detached,
            ClosureState::Clear
                .ordinary_detached_attach_admission()
                .expect("clear closure admits attach"),
            attach,
            AttachSecretProof::Verified,
            AttachCommitParameters {
                binding: ActiveBinding {
                    participant_id: PARTICIPANT_ID,
                    conversation_id: CONVERSATION_ID,
                    binding_epoch: new_epoch,
                },
                attach_secret: AttachSecret::new([0xB8; 32]),
                attached_position: AttachedRecordPosition::new(10, 45),
                receipt_expires_at: 1_000,
                provenance_expires_at: 2_000,
            },
        )
        .expect("detached attach is authorized");
    let attached = commit_attach(verified_attach, DetachCell::Committed(committed))
        .expect("attach terminalizes committed detach");
    let DetachCell::Terminalized(terminalized) = attached.detach_cell else {
        panic!("attach must produce the fourth detach-cell variant");
    };
    let verified_old = terminalized
        .verify_exact(&request, verifier)
        .expect("old token and verifier are exact");
    let cell = verified_old.outcome(
        CONVERSATION_ID,
        generation(5),
        BindingStateView::Bound {
            current_binding_epoch: new_epoch,
        },
    );
    (
        ServerValue::StaleAuthority(StaleAuthority::Detach(
            DetachStaleAuthority::TerminalizedDetachCell(cell),
        )),
        new_epoch,
    )
}

#[test]
fn terminalized_detach_cell_is_surfaced_as_a_terminal_bound_status() {
    let mut session = ParticipantLifecycle::new();
    session.record_detach(detach_request());
    let (value, new_epoch) = terminalized_value();

    let ParticipantReceive::Outcome(outcome) = loopback(&mut session, value.clone()) else {
        panic!("loopback must surface terminalized detach cell");
    };

    assert_eq!(outcome.value(), &value);
    assert_eq!(
        outcome.transition(),
        ParticipantTransition::TerminalizedDetachCell
    );
    assert_eq!(session.detach_replay_status(), DetachReplayStatus::Terminal);
    let ParticipantClientState::Bound(bound) = session.state() else {
        panic!("terminalized response says the current participant is bound");
    };
    assert_eq!(bound.binding_epoch(), new_epoch);
    assert_eq!(
        session.on_replay_event(DetachReplayEvent::ExplicitCallerAction),
        DetachReplayAction::None
    );
}

fn live_member(cursor: u64) -> LiveMember<Vec<u8>> {
    LiveMember::restore(LiveMemberRestore {
        participant_id: PARTICIPANT_ID,
        conversation_id: CONVERSATION_ID,
        generation: generation(3),
        attach_secret: AttachSecret::new([0xA7; 32]),
        cursor,
        enrollment_fingerprint: EnrollmentFingerprint::new(vec![1, 2, 3]),
        latest_terminal: None,
    })
    .expect("test member has valid terminal history")
}

fn binding() -> BindingState {
    BindingState::Bound(ActiveBinding {
        participant_id: PARTICIPANT_ID,
        conversation_id: CONVERSATION_ID,
        binding_epoch: epoch(3, 9),
    })
}

fn ack(through_seq: u64) -> ParticipantAck {
    ParticipantAck {
        conversation_id: CONVERSATION_ID,
        participant_id: PARTICIPANT_ID,
        capability_generation: generation(3),
        through_seq,
    }
}

#[test]
fn loopback_ack_selector_surfaces_all_four_crate_owned_thresholds() {
    let identity: IdentityState<Vec<u8>, Vec<u8>, Vec<u8>> =
        IdentityState::Live(live_member(CURRENT_CURSOR));
    let presented = PresentedIdentity::from(Some(&identity));

    assert!(matches!(
        apply_participant_ack(presented, &binding(), epoch(3, 9), &ack(4), 9),
        ParticipantAckDecision::Respond(ServerValue::AckRegression(_))
    ));
    assert!(matches!(
        apply_participant_ack(presented, &binding(), epoch(3, 9), &ack(CURRENT_CURSOR), 0,),
        ParticipantAckDecision::Respond(ServerValue::AckNoOp(AckNoOp::ParticipantAck(_)))
    ));
    assert!(matches!(
        apply_participant_ack(presented, &binding(), epoch(3, 9), &ack(7), 6),
        ParticipantAckDecision::Respond(ServerValue::AckGap(_))
    ));
    assert!(matches!(
        apply_participant_ack(presented, &binding(), epoch(3, 9), &ack(7), 7),
        ParticipantAckDecision::Commit(_)
    ));
}
