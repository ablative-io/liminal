use super::*;
use crate::wire::{
    AttachSecret, BindingEpoch, ClientRequest, ConnectionIncarnation, DetachAttemptToken,
    DetachRequest, Generation, ObserverProgressStatus, ObserverRecoveryAccepted,
    ObserverRecoveryHandshake, ObserverRefusal, RecordAdmission, RecordAdmissionEnvelope,
    RecordCommitted, ServerValue,
};
use alloc::vec::Vec;

type TestResult<T = ()> = Result<T, &'static str>;

fn generation(value: u64) -> TestResult<Generation> {
    Generation::new(value).ok_or("generation must be nonzero")
}

fn epoch(value: u64) -> TestResult<BindingEpoch> {
    Ok(BindingEpoch::new(
        ConnectionIncarnation::new(101, 102),
        generation(value)?,
    ))
}

fn bound() -> TestResult<ClientParticipantAggregate> {
    let mut aggregate = ClientParticipantAggregate::new();
    aggregate.binding = ClientBindingState::Bound {
        conversation_id: 111,
        participant_id: 112,
        generation: generation(7)?,
        attach_secret: AttachSecret::new([113; 32]),
        binding_epoch: epoch(7)?,
    };
    Ok(aggregate)
}

fn detach_request() -> TestResult<ClientRequest> {
    Ok(ClientRequest::Detach(DetachRequest {
        conversation_id: 111,
        participant_id: 112,
        capability_generation: generation(7)?,
        detach_attempt_token: DetachAttemptToken::new([114; 16]),
    }))
}

#[derive(Clone, Copy, Debug)]
enum OperationKind {
    Detach,
    Record,
    Observer,
}

impl OperationKind {
    fn aggregate(self) -> TestResult<ClientParticipantAggregate> {
        match self {
            Self::Detach | Self::Record => bound(),
            Self::Observer => Ok(ClientParticipantAggregate::new()),
        }
    }

    fn request(self) -> TestResult<ClientRequest> {
        match self {
            Self::Detach => detach_request(),
            Self::Record => Ok(ClientRequest::RecordAdmission(RecordAdmission {
                conversation_id: 111,
                participant_id: 112,
                capability_generation: generation(7)?,
                payload: alloc::vec![1, 2],
            })),
            Self::Observer => Ok(ClientRequest::ObserverRecovery(ObserverRecoveryHandshake {
                observer_refusals: alloc::vec![ObserverRefusal {
                    conversation_id: 121,
                    refused_epoch: 122,
                }],
            })),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Action {
    Record,
    Commit,
    Release,
    Send,
    Recover,
    ReplayStart,
    Fate,
    Outcome,
    Restore,
    ProcessLost,
}

const ACTIONS: [Action; 10] = [
    Action::Record,
    Action::Commit,
    Action::Release,
    Action::Send,
    Action::Recover,
    Action::ReplayStart,
    Action::Fate,
    Action::Outcome,
    Action::Restore,
    Action::ProcessLost,
];
const MAX_DEPTH: usize = 7;

enum Root {
    Aggregate(ClientParticipantAggregate),
    Pending(ClientPendingOperationRecord),
    Commit(ClientOperationCommit),
}

enum LiveAuthority {
    None,
    Operation(ExpectedParticipantOperation),
    DetachAttempt(DetachTransportAttempt),
    Correlation(ClientResponseCorrelation),
}

impl LiveAuthority {
    const fn count(&self) -> usize {
        match self {
            Self::None => 0,
            Self::Operation(_) | Self::DetachAttempt(_) | Self::Correlation(_) => 1,
        }
    }
}

struct Harness {
    kind: OperationKind,
    root: Root,
    live: LiveAuthority,
    issued: usize,
    consumed: usize,
    lost: usize,
    recorded: bool,
}

impl Harness {
    fn new(kind: OperationKind) -> TestResult<Self> {
        Ok(Self {
            kind,
            root: Root::Aggregate(kind.aggregate()?),
            live: LiveAuthority::None,
            issued: 0,
            consumed: 0,
            lost: 0,
            recorded: false,
        })
    }

    fn assert_conservation(&self) {
        let live = self.live.count();
        assert!(live <= 1, "more than one live send authority");
        assert_eq!(
            self.issued,
            self.consumed + self.lost + live,
            "issued authority was duplicated, discarded, or consumed twice"
        );
    }

    fn step(mut self, action: Action) -> TestResult<Option<Self>> {
        self = match action {
            Action::Record if !self.recorded => {
                let Root::Aggregate(aggregate) = self.root else {
                    return Ok(None);
                };
                let ClientOperationRecordDecision::Pending(pending) =
                    record_operation(aggregate, self.kind.request()?)
                else {
                    return Err("property operation must enter pending");
                };
                self.root = Root::Pending(pending);
                self.recorded = true;
                self
            }
            Action::Commit => {
                let Root::Pending(pending) = self.root else {
                    return Ok(None);
                };
                self.root = Root::Commit(pending.commit());
                self
            }
            Action::Release => {
                let Root::Commit(commit) = self.root else {
                    return Ok(None);
                };
                let (aggregate, operation) = commit.into_parts();
                self.root = Root::Aggregate(aggregate);
                self.live = LiveAuthority::Operation(operation);
                self.issued += 1;
                self
            }
            Action::Send => {
                self.live = match self.live {
                    LiveAuthority::Operation(operation) => {
                        let (_, correlation) = operation.into_request();
                        LiveAuthority::Correlation(correlation)
                    }
                    LiveAuthority::DetachAttempt(attempt) => {
                        let (_, correlation) = attempt.into_request();
                        LiveAuthority::Correlation(correlation)
                    }
                    LiveAuthority::None | LiveAuthority::Correlation(_) => return Ok(None),
                };
                self
            }
            Action::Recover => {
                if self.live.count() != 0 {
                    return Ok(None);
                }
                let Root::Aggregate(aggregate) = self.root else {
                    return Ok(None);
                };
                let RecoveredExpectedOperationDecision::Recovered {
                    aggregate,
                    operation,
                } = recover_expected_operation(aggregate)
                else {
                    return Ok(None);
                };
                self.root = Root::Aggregate(aggregate);
                self.live = LiveAuthority::Operation(operation);
                self.issued += 1;
                self
            }
            Action::ReplayStart if matches!(self.kind, OperationKind::Detach) => {
                if self.live.count() != 0 {
                    return Ok(None);
                }
                let Root::Aggregate(aggregate) = self.root else {
                    return Ok(None);
                };
                let DetachTransportAttemptDecision::Started { aggregate, attempt } =
                    transport_attempt_started(aggregate)
                else {
                    return Ok(None);
                };
                self.root = Root::Aggregate(aggregate);
                self.live = LiveAuthority::DetachAttempt(attempt);
                self.issued += 1;
                self
            }
            Action::Fate => return self.step_fate(),
            Action::Outcome => return self.step_outcome(),
            Action::Restore | Action::ProcessLost => return self.step_restore(action),
            Action::Record | Action::ReplayStart => return Ok(None),
        };
        self.assert_conservation();
        Ok(Some(self))
    }

    fn step_fate(mut self) -> TestResult<Option<Self>> {
        let LiveAuthority::Correlation(correlation) = self.live else {
            return Ok(None);
        };
        let Root::Aggregate(aggregate) = self.root else {
            return Ok(None);
        };
        self.root = match self.kind {
            OperationKind::Detach => {
                let DetachTransportFateDecision::Parked(applied) = transport_fate(
                    aggregate,
                    correlation,
                    DetachTransportFate::ResponseUnavailable,
                ) else {
                    return Err("current detach fate must consume authority");
                };
                Root::Aggregate(applied.into_aggregate())
            }
            OperationKind::Record | OperationKind::Observer => {
                let ExpectedOperationFateDecision::Recorded { aggregate, .. } =
                    record_expected_operation_fate(
                        aggregate,
                        correlation,
                        ExpectedOperationTransportFate::ResponseUnavailable,
                    )
                else {
                    return Err("current operation fate must consume authority");
                };
                Root::Aggregate(aggregate)
            }
        };
        self.live = LiveAuthority::None;
        self.consumed += 1;
        self.assert_conservation();
        Ok(Some(self))
    }

    fn step_outcome(mut self) -> TestResult<Option<Self>> {
        let LiveAuthority::Correlation(correlation) = self.live else {
            return Ok(None);
        };
        let Root::Aggregate(aggregate) = self.root else {
            return Ok(None);
        };
        match self.kind {
            OperationKind::Detach => {
                let value = crate::wire::DetachCommitted::new(
                    111,
                    112,
                    DetachAttemptToken::new([114; 16]),
                    epoch(7)?,
                    0,
                );
                let ApplyDetachOutcomeDecision::Terminal(applied) = apply_detach_outcome(
                    aggregate,
                    DetachReplayOutcome::DetachCommitted(value),
                    correlation,
                ) else {
                    return Err("exact detach outcome must consume authority");
                };
                self.root = Root::Aggregate(applied.into_aggregate());
                self.live = LiveAuthority::None;
                self.consumed += 1;
            }
            OperationKind::Observer => {
                let value = ServerValue::ObserverRecoveryAccepted(ObserverRecoveryAccepted {
                    statuses: alloc::vec![ObserverProgressStatus {
                        conversation_id: 121,
                        refused_epoch: 122,
                        current_observer_progress: 0,
                        armed: false,
                        progressed: false,
                    }],
                });
                let ClientCorrelatedInboundDecision::Applied(applied) =
                    decide_correlated_inbound(aggregate, value, correlation)
                else {
                    return Err("exact observer outcome must consume authority");
                };
                self.root = Root::Aggregate(applied.into_parts().0);
                self.live = LiveAuthority::None;
                self.consumed += 1;
            }
            OperationKind::Record => {
                let value = ServerValue::RecordCommitted(RecordCommitted::new(
                    RecordAdmissionEnvelope {
                        conversation_id: 111,
                        participant_id: 112,
                        capability_generation: generation(7)?,
                    },
                    1,
                ));
                let ClientCorrelatedInboundDecision::Refused(refusal) =
                    decide_correlated_inbound(aggregate, value, correlation)
                else {
                    return Err("tokenless record outcome must stay ambiguous");
                };
                let (aggregate, _, correlation) = refusal.into_parts();
                self.root = Root::Aggregate(aggregate);
                self.live = LiveAuthority::Correlation(correlation);
            }
        }
        self.assert_conservation();
        Ok(Some(self))
    }

    fn step_restore(mut self, action: Action) -> TestResult<Option<Self>> {
        match action {
            Action::Restore => {
                let record = match &self.root {
                    Root::Aggregate(aggregate) => aggregate
                        .resume_record()
                        .map_err(|_| "aggregate must encode")?,
                    Root::Commit(commit) => {
                        commit.resume_record().map_err(|_| "commit must encode")?
                    }
                    Root::Pending(_) => return Ok(None),
                };
                let canonical = record.encode_canonical();
                let record = ClientResumeRecord::decode_canonical(&canonical)
                    .map_err(|_| "canonical record must decode")?;
                if self.live.count() == 1 {
                    self.live = LiveAuthority::None;
                    self.lost += 1;
                }
                let mut aggregate = record.restore().map_err(|_| "record must restore")?;
                if aggregate.take_restored_operation_abandonment().is_some() && self.lost == 1 {
                    self.lost -= 1;
                    self.consumed += 1;
                }
                self.root = Root::Aggregate(aggregate);
            }
            Action::ProcessLost if self.lost == 1 => {
                let Root::Aggregate(aggregate) = self.root else {
                    return Ok(None);
                };
                match record_issued_expected_operation_fate(
                    aggregate,
                    IssuedExpectedOperationFate::ProcessLost,
                ) {
                    IssuedExpectedOperationFateDecision::Recorded { aggregate, .. }
                    | IssuedExpectedOperationFateDecision::DetachParked { aggregate, .. } => {
                        self.root = Root::Aggregate(aggregate);
                        self.lost = 0;
                        self.consumed += 1;
                    }
                    IssuedExpectedOperationFateDecision::Refused { .. } => {
                        return Err("restored lost authority must have typed abandonment");
                    }
                }
            }
            _ => return Ok(None),
        }
        self.assert_conservation();
        Ok(Some(self))
    }
}

fn run_path(kind: OperationKind, path: &[Action]) -> TestResult<Option<Harness>> {
    let mut harness = Harness::new(kind)?;
    harness.assert_conservation();
    for action in path {
        let Some(next) = harness.step(*action)? else {
            return Ok(None);
        };
        harness = next;
    }
    Ok(Some(harness))
}

fn enumerate(kind: OperationKind, path: &mut Vec<Action>, visited: &mut usize) -> TestResult {
    if path.len() == MAX_DEPTH {
        return Ok(());
    }
    for action in ACTIONS {
        path.push(action);
        if run_path(kind, path)?.is_some() {
            *visited += 1;
            enumerate(kind, path, visited)?;
        }
        path.pop();
    }
    Ok(())
}

#[test]
fn exhaustive_small_scope_send_authority_conservation() -> TestResult {
    let mut visited = 0;
    for kind in [
        OperationKind::Detach,
        OperationKind::Record,
        OperationKind::Observer,
    ] {
        enumerate(kind, &mut Vec::new(), &mut visited)?;
    }
    assert_eq!(visited, 358, "bounded interleaving space changed");
    Ok(())
}
