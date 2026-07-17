//! R2.2 typed-fate wiring tests (LP-WS-TRANSPORT, folded r1.1).
//!
//! Socket facts enter the `4118aa1`-lineage client unit as typed fates through
//! the WebSocket authority binding; every reconnect/replay decision comes back
//! out of `ClientParticipantAggregate`. These tests inject socket fates at the
//! permit-before-open, open-in-progress, established, detach-in-flight,
//! already-parked, and stale-attempt points and assert aggregate-returned
//! typed decisions, one real open per fresh authorization, unchanged state on
//! refusal, failure returning `Parked`, and no timer or retry effect.

use liminal_protocol::client::{
    ClientOperationRecordDecision, ClientParticipantAggregate, ReconnectAttemptFate,
    ReconnectAttemptFateDecision, ReconnectAttemptFateRefusalReason, ReconnectFreshEvent,
    ReconnectPermitDecision, record_attempt_fate, record_explicit_reconnect, record_operation,
    redeem_attempt,
};
use liminal_protocol::client::{
    ClientResponseCorrelation, DetachReplayRefusalReason, DetachReplayStatus,
    ExplicitReconnectAction, decide_correlated_inbound,
};
use liminal_protocol::outcome::ReconnectState;
use liminal_protocol::wire::{
    AttachSecret, BindingEpoch, ClientRequest, ConnectionIncarnation, DetachAttemptToken,
    DetachRequest, EnrollBound, EnrollmentRequest, EnrollmentToken, Generation, ServerValue,
};
use liminal_sdk::remote::websocket::{
    AttemptFateOutcome, AttemptFateRefusal, DetachLossOutcome, LossRecordOutcome,
    LossRecordRefusal, OpenRequestDecision, OpenRequestRefusal, SocketFailure, TransportTerminal,
    WebSocketAuthorityBinding,
};

type TestResult<T = ()> = Result<T, String>;

const CONVERSATION: u64 = 21;
const PARTICIPANT: u64 = 22;

fn authorize(binding: &mut WebSocketAuthorityBinding) -> TestResult<ReconnectFreshEvent> {
    match binding.request_open() {
        OpenRequestDecision::Authorized { event } => Ok(event),
        OpenRequestDecision::Refused(refusal) => {
            Err(format!("open authorization must succeed: {refusal:?}"))
        }
    }
}

fn establish(binding: &mut WebSocketAuthorityBinding) -> TestResult {
    authorize(binding)?;
    match binding.connection_established() {
        AttemptFateOutcome::Recorded {
            state: ReconnectState::Online,
        } => Ok(()),
        other => Err(format!("established fate must record Online: {other:?}")),
    }
}

#[test]
fn explicit_connect_authorizes_exactly_one_open() -> TestResult {
    let mut binding = WebSocketAuthorityBinding::new();
    assert_eq!(binding.reconnect_state(), ReconnectState::Parked);

    let event = authorize(&mut binding)?;
    assert_eq!(
        event,
        ReconnectFreshEvent::ExplicitCallerAction(ExplicitReconnectAction::ReconnectNow)
    );
    assert_eq!(binding.reconnect_state(), ReconnectState::AttemptInProgress);

    // A second open request while the attempt is in progress is refused with
    // the binding unchanged: one real open per fresh authorization.
    let OpenRequestDecision::Refused(refusal) = binding.request_open() else {
        return Err("second open request during an attempt must be refused".to_string());
    };
    assert!(matches!(refusal, OpenRequestRefusal::OpenAlreadyInProgress));
    assert_eq!(binding.reconnect_state(), ReconnectState::AttemptInProgress);

    match binding.connection_established() {
        AttemptFateOutcome::Recorded { state } => assert_eq!(state, ReconnectState::Online),
        other @ AttemptFateOutcome::Refused(_) => {
            return Err(format!("established fate must be recorded: {other:?}"));
        }
    }
    Ok(())
}

#[test]
fn open_failure_returns_parked_with_no_retry_authority() -> TestResult {
    let mut binding = WebSocketAuthorityBinding::new();
    authorize(&mut binding)?;

    match binding.open_failed() {
        AttemptFateOutcome::Recorded { state } => assert_eq!(state, ReconnectState::Parked),
        other @ AttemptFateOutcome::Refused(_) => {
            return Err(format!("open failure must record Parked: {other:?}"));
        }
    }

    // No timer or retry effect: the failure minted nothing, and a stray
    // success fate afterwards is a typed refusal with unchanged state.
    match binding.connection_established() {
        AttemptFateOutcome::Refused(AttemptFateRefusal::NoOpenInProgress) => {}
        other => return Err(format!("fate without an open must refuse: {other:?}")),
    }
    assert_eq!(binding.reconnect_state(), ReconnectState::Parked);
    Ok(())
}

#[test]
fn fate_injection_before_any_open_is_refused_unchanged() -> TestResult {
    // Permit-before-open point: no attempt exists, so both socket fates are
    // typed refusals and the aggregate does not move.
    let mut binding = WebSocketAuthorityBinding::new();
    match binding.connection_established() {
        AttemptFateOutcome::Refused(AttemptFateRefusal::NoOpenInProgress) => {}
        other => return Err(format!("established before open must refuse: {other:?}")),
    }
    match binding.open_failed() {
        AttemptFateOutcome::Refused(AttemptFateRefusal::NoOpenInProgress) => {}
        other => return Err(format!("failure before open must refuse: {other:?}")),
    }
    assert_eq!(binding.reconnect_state(), ReconnectState::Parked);
    Ok(())
}

#[test]
fn established_loss_retains_permit_for_one_reconnect_open() -> TestResult {
    let mut binding = WebSocketAuthorityBinding::new();
    establish(&mut binding)?;

    let outcome = binding.established_terminal(&TransportTerminal::PeerClosed);
    assert!(matches!(outcome, LossRecordOutcome::PermitRetained));
    assert_eq!(binding.reconnect_state(), ReconnectState::PermitOutstanding);

    // A second loss report cannot mint again while the permit is outstanding.
    let outcome = binding.established_terminal(&TransportTerminal::PeerClosed);
    assert!(
        matches!(outcome, LossRecordOutcome::Refused(_)),
        "loss while a permit is outstanding must be refused: {outcome:?}"
    );

    // The retained permit authorizes exactly one reconnect open.
    let event = authorize(&mut binding)?;
    assert!(matches!(event, ReconnectFreshEvent::TransportFate(_)));
    match binding.connection_established() {
        AttemptFateOutcome::Recorded { state } => assert_eq!(state, ReconnectState::Online),
        other @ AttemptFateOutcome::Refused(_) => {
            return Err(format!("reconnect open must record Online: {other:?}"));
        }
    }
    Ok(())
}

#[test]
fn loss_reported_while_parked_is_refused_unchanged() {
    // Already-parked point: no established connection exists, so a loss report
    // is a typed refusal and the aggregate does not move.
    let mut binding = WebSocketAuthorityBinding::new();
    let outcome = binding.established_terminal(&TransportTerminal::PeerClosed);
    assert!(matches!(
        outcome,
        LossRecordOutcome::Refused(LossRecordRefusal::NotEstablished)
    ));
    assert_eq!(binding.reconnect_state(), ReconnectState::Parked);
}

#[test]
fn loss_during_open_in_progress_is_refused_unchanged() -> TestResult {
    // Open-in-progress point: an in-progress attempt owns the authorization;
    // an established-loss report is a typed refusal, not a second authority.
    let mut binding = WebSocketAuthorityBinding::new();
    authorize(&mut binding)?;
    let outcome =
        binding.established_terminal(&TransportTerminal::SocketFailed(SocketFailure::Transport));
    assert!(matches!(
        outcome,
        LossRecordOutcome::Refused(LossRecordRefusal::NotEstablished)
    ));
    assert_eq!(binding.reconnect_state(), ReconnectState::AttemptInProgress);
    Ok(())
}

#[test]
fn terminal_diagnostics_never_select_aggregate_transitions() -> TestResult {
    // A WS close code, I/O failure, or protocol violation is diagnostic data
    // only: every terminal variant reaches the identical aggregate decision.
    let terminals = [
        TransportTerminal::PeerClosed,
        TransportTerminal::CloseCompleted,
        TransportTerminal::SocketFailed(SocketFailure::Transport),
        TransportTerminal::SocketFailed(SocketFailure::UnsupportedTextMessage),
    ];
    for terminal in &terminals {
        let mut binding = WebSocketAuthorityBinding::new();
        establish(&mut binding)?;
        let outcome = binding.established_terminal(terminal);
        assert!(
            matches!(outcome, LossRecordOutcome::PermitRetained),
            "terminal {terminal:?} must reach the same Lost decision"
        );
        assert_eq!(binding.reconnect_state(), ReconnectState::PermitOutstanding);
    }
    Ok(())
}

#[test]
fn stale_attempt_from_foreign_aggregate_is_refused_unchanged() -> TestResult {
    // Stale-attempt point, at the aggregate API the binding wires: an attempt
    // authority from one aggregate cannot record a fate on another.
    let ReconnectPermitDecision::Permitted {
        aggregate, permit, ..
    } = record_explicit_reconnect(
        ClientParticipantAggregate::new(),
        ExplicitReconnectAction::ReconnectNow,
    )
    else {
        return Err("fresh aggregate must mint an explicit permit".to_string());
    };
    let liminal_protocol::client::ReconnectAttemptDecision::Started { attempt, .. } =
        redeem_attempt(aggregate, permit)
    else {
        return Err("minted permit must redeem".to_string());
    };

    let foreign = ClientParticipantAggregate::new();
    let ReconnectAttemptFateDecision::Refused { reason, .. } =
        record_attempt_fate(foreign, attempt, ReconnectAttemptFate::Connected)
    else {
        return Err("foreign attempt fate must be refused".to_string());
    };
    assert_eq!(reason, ReconnectAttemptFateRefusalReason::NoAttempt);
    Ok(())
}

/// Builds an aggregate with a bound participant and an in-flight detach send,
/// entirely through the public client-unit API, returning the released
/// correlation for the outstanding transport attempt.
fn detach_in_flight() -> TestResult<(ClientParticipantAggregate, ClientResponseCorrelation)> {
    let token = EnrollmentToken::new([7; 16]);
    let request = ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: CONVERSATION,
        enrollment_token: token,
    });
    let ClientOperationRecordDecision::Pending(pending) =
        record_operation(ClientParticipantAggregate::new(), request)
    else {
        return Err("enrollment must enter the durability barrier".to_string());
    };
    let (aggregate, operation) = pending.commit().into_parts();
    let (_, correlation) = operation.into_request();

    let generation = Generation::new(1).ok_or("generation 1 must construct")?;
    let epoch = BindingEpoch::new(ConnectionIncarnation::new(3, 4), generation);
    let bound = EnrollBound::new(
        CONVERSATION,
        token,
        PARTICIPANT,
        AttachSecret::new([9; 32]),
        epoch,
        100,
        200,
    )
    .ok_or("generation-1 enroll bound must construct")?;
    let decision =
        decide_correlated_inbound(aggregate, ServerValue::EnrollBound(bound), correlation);
    let liminal_protocol::client::ClientCorrelatedInboundDecision::Applied(applied) = decision
    else {
        return Err("correlated enroll bound must apply".to_string());
    };
    let (aggregate, _) = applied.into_parts();

    let detach = ClientRequest::Detach(DetachRequest {
        conversation_id: CONVERSATION,
        participant_id: PARTICIPANT,
        capability_generation: generation,
        detach_attempt_token: DetachAttemptToken::new([11; 16]),
    });
    let ClientOperationRecordDecision::Pending(pending) = record_operation(aggregate, detach)
    else {
        return Err("bound detach must enter the durability barrier".to_string());
    };
    let (aggregate, operation) = pending.commit().into_parts();
    let (_, correlation) = operation.into_request();
    Ok((aggregate, correlation))
}

#[test]
fn detach_in_flight_socket_loss_parks_replay_via_typed_fate() -> TestResult {
    let (aggregate, correlation) = detach_in_flight()?;
    let mut binding = WebSocketAuthorityBinding::with_aggregate(aggregate);

    match binding.detach_send_lost(correlation) {
        DetachLossOutcome::Parked => {}
        other @ DetachLossOutcome::Refused(_) => {
            return Err(format!("detach-in-flight loss must park replay: {other:?}"));
        }
    }
    assert_eq!(
        binding.aggregate().detach_replay().status(),
        Some(&DetachReplayStatus::Parked)
    );
    Ok(())
}

#[test]
fn detach_loss_without_in_flight_send_is_refused_unchanged() -> TestResult {
    let (aggregate, correlation) = detach_in_flight()?;
    let mut binding = WebSocketAuthorityBinding::with_aggregate(aggregate);
    match binding.detach_send_lost(correlation) {
        DetachLossOutcome::Parked => {}
        other @ DetachLossOutcome::Refused(_) => {
            return Err(format!("first loss must park: {other:?}"));
        }
    }

    // The correlation was consumed; a second loss report finds no in-flight
    // send and must refuse without moving replay.
    let (_, foreign_correlation) = detach_in_flight()?;
    match binding.detach_send_lost(foreign_correlation) {
        DetachLossOutcome::Refused(reason) => {
            assert_eq!(reason, DetachReplayRefusalReason::InvalidStatus);
        }
        other @ DetachLossOutcome::Parked => {
            return Err(format!("second loss must refuse: {other:?}"));
        }
    }
    assert_eq!(
        binding.aggregate().detach_replay().status(),
        Some(&DetachReplayStatus::Parked)
    );
    Ok(())
}
