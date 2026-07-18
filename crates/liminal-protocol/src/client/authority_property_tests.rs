//! Exhaustive send-authority conservation property (`LP-CLIENT-GOAL` Phase 2
//! item 8, r2, 2026-07-18).
//!
//! The property ATTEMPTS every action of the alphabet at every step and
//! asserts the crate's typed refusal for illegal interleavings; no avoided
//! interleaving is pruned out of the schedule. An action is recorded as
//! structurally unattemptable only when the one-use moved value it consumes
//! does not exist at the type level (a spent permit, an absent correlation, or
//! a root sealed inside the pending/commit barrier, whose encode absence is a
//! committed `compile_fail` door) — that is the crate refusing at the type
//! surface, not the harness avoiding the attempt. A refused attempt returns
//! the unchanged aggregate, so its subtree is identical to its parent's and
//! extending it explores no new state.
//!
//! The loss instrument is the CRATE's serialized testimony and durable
//! abandonment, observed through [`ClientParticipantAggregate`] accessors and
//! the typed resolution decisions — never a harness-side lost counter. The
//! conservation invariant checked after every step is:
//!
//! `issued == consumed + live + crate_pending`
//!
//! where `crate_pending` counts the crate's own pending loss atoms (operation
//! testimony, reconnect testimony, and issued-marked abandonment). The
//! explored-space counts asserted at the end are derived from the run and
//! drift-checked only; they are not offered as correctness evidence.
//!
//! The alphabet includes the mandated `ProcessLost` interleaving (`Restore`
//! destroying live authority followed by `ResolveLoss`), sequential
//! same-envelope re-record (`Record` attempted at every step), and the
//! token-bearing non-detach `CredentialAttach` class alongside detach and both
//! tokenless classes.

use super::*;
use crate::wire::{
    AttachAttemptToken, AttachSecret, BindingEpoch, ClientRequest, ConnectionIncarnation,
    CredentialAttachRequest, DetachAttemptToken, DetachRequest, Generation, ObserverProgressStatus,
    ObserverRecoveryAccepted, ObserverRecoveryHandshake, ObserverRefusal, RecordAdmission,
    RecordAdmissionEnvelope, RecordCommitted, ServerValue,
};
use alloc::{boxed::Box, vec::Vec};

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
    Attach,
    Record,
    Observer,
}

impl OperationKind {
    fn aggregate(self) -> TestResult<ClientParticipantAggregate> {
        match self {
            Self::Detach | Self::Attach | Self::Record => bound(),
            Self::Observer => Ok(ClientParticipantAggregate::new()),
        }
    }

    fn request(self) -> TestResult<ClientRequest> {
        match self {
            Self::Detach => detach_request(),
            Self::Attach => Ok(ClientRequest::CredentialAttach(CredentialAttachRequest {
                conversation_id: 111,
                participant_id: 112,
                capability_generation: generation(7)?,
                attach_secret: AttachSecret::new([113; 32]),
                attach_attempt_token: AttachAttemptToken::new([115; 16]),
                accept_marker_delivery_seq: None,
            })),
            Self::Record => Ok(ClientRequest::RecordAdmission(RecordAdmission {
                conversation_id: 111,
                participant_id: 112,
                capability_generation: generation(7)?,
                record_admission_attempt_token: crate::wire::RecordAdmissionAttemptToken::new(
                    [0xA7; 16],
                ),
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

    const fn tokenless(self) -> bool {
        matches!(self, Self::Observer)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    ResolveLoss,
    TakeAbandonment,
}

const ACTIONS: [Action; 11] = [
    Action::Record,
    Action::Commit,
    Action::Release,
    Action::Send,
    Action::Recover,
    Action::ReplayStart,
    Action::Fate,
    Action::Outcome,
    Action::Restore,
    Action::ResolveLoss,
    Action::TakeAbandonment,
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

/// Counts the crate's own serialized pending loss atoms.
fn crate_pending(aggregate: &ClientParticipantAggregate) -> usize {
    usize::from(aggregate.lost_operation_testimony().is_some())
        + usize::from(aggregate.lost_reconnect_testimony().is_some())
        + usize::from(
            aggregate
                .restored_operation_abandonment()
                .is_some_and(RestoredExpectedOperationAbandonment::was_issued),
        )
}

#[derive(Default)]
struct Stats {
    visited: usize,
    refused: usize,
    unattemptable: usize,
    incompatible_re_records: usize,
    detach_testimonies: usize,
    correlation_testimonies: usize,
    detach_parked_resolutions: usize,
    recorded_resolutions: usize,
    issued_abandonments: usize,
    attach_applications: usize,
}

enum StepOutcome {
    Applied(Box<Harness>),
    Refused,
    Unattemptable,
}

struct Harness {
    kind: OperationKind,
    root: Root,
    live: LiveAuthority,
    issued: usize,
    consumed: usize,
    /// Crate-side pending loss atoms, re-read from the aggregate whenever the
    /// root is an aggregate; the value is unchanged across the sealed
    /// pending/commit windows because no API touches the sealed state.
    cached_pending: usize,
}

impl Harness {
    fn new(kind: OperationKind) -> TestResult<Self> {
        Ok(Self {
            kind,
            root: Root::Aggregate(kind.aggregate()?),
            live: LiveAuthority::None,
            issued: 0,
            consumed: 0,
            cached_pending: 0,
        })
    }

    fn refresh_pending(&mut self) {
        if let Root::Aggregate(aggregate) = &self.root {
            self.cached_pending = crate_pending(aggregate);
        }
    }

    fn assert_conservation(&self) {
        let live = self.live.count();
        assert!(live <= 1, "more than one live send authority");
        assert_eq!(
            self.issued,
            self.consumed + live + self.cached_pending,
            "issued authority was duplicated, discarded, or consumed twice"
        );
    }

    fn step(self, action: Action, stats: &mut Stats) -> TestResult<StepOutcome> {
        let outcome = match action {
            Action::Record => self.step_record(stats)?,
            Action::Commit => self.step_commit(),
            Action::Release => self.step_release(),
            Action::Send => self.step_send(),
            Action::Recover => self.step_recover(),
            Action::ReplayStart => self.step_replay_start(),
            Action::Fate => self.step_fate()?,
            Action::Outcome => self.step_outcome(stats)?,
            Action::Restore => self.step_restore(stats)?,
            Action::ResolveLoss => self.step_resolve_loss(stats),
            Action::TakeAbandonment => self.step_take_abandonment(stats),
        };
        if let StepOutcome::Applied(harness) = &outcome {
            harness.assert_conservation();
        }
        Ok(outcome)
    }

    fn step_commit(mut self) -> StepOutcome {
        let Root::Pending(pending) = self.root else {
            return StepOutcome::Unattemptable;
        };
        self.root = Root::Commit(pending.commit());
        StepOutcome::Applied(Box::new(self))
    }

    fn step_release(mut self) -> StepOutcome {
        let live_free = self.live.count() == 0;
        let Root::Commit(commit) = self.root else {
            return StepOutcome::Unattemptable;
        };
        assert!(
            live_free,
            "the crate released a second live authority over an unconsumed one"
        );
        let (aggregate, operation) = commit.into_parts();
        self.root = Root::Aggregate(aggregate);
        self.live = LiveAuthority::Operation(operation);
        self.issued += 1;
        self.refresh_pending();
        StepOutcome::Applied(Box::new(self))
    }

    fn step_send(mut self) -> StepOutcome {
        self.live = match self.live {
            LiveAuthority::Operation(operation) => {
                let (_, correlation) = operation.into_request();
                LiveAuthority::Correlation(correlation)
            }
            LiveAuthority::DetachAttempt(attempt) => {
                let (_, correlation) = attempt.into_request();
                LiveAuthority::Correlation(correlation)
            }
            LiveAuthority::None | LiveAuthority::Correlation(_) => {
                return StepOutcome::Unattemptable;
            }
        };
        StepOutcome::Applied(Box::new(self))
    }

    fn step_recover(mut self) -> StepOutcome {
        let live_free = self.live.count() == 0;
        let Root::Aggregate(aggregate) = self.root else {
            return StepOutcome::Unattemptable;
        };
        match recover_expected_operation(aggregate) {
            RecoveredExpectedOperationDecision::Recovered {
                aggregate,
                operation,
            } => {
                assert!(
                    live_free,
                    "the crate released a second live authority over an unconsumed one"
                );
                self.root = Root::Aggregate(aggregate);
                self.live = LiveAuthority::Operation(operation);
                self.issued += 1;
                self.refresh_pending();
                StepOutcome::Applied(Box::new(self))
            }
            RecoveredExpectedOperationDecision::NotAvailable { aggregate, .. } => {
                self.root = Root::Aggregate(aggregate);
                StepOutcome::Refused
            }
        }
    }

    fn step_replay_start(mut self) -> StepOutcome {
        let live_free = self.live.count() == 0;
        let Root::Aggregate(aggregate) = self.root else {
            return StepOutcome::Unattemptable;
        };
        match transport_attempt_started(aggregate) {
            DetachTransportAttemptDecision::Started { aggregate, attempt } => {
                assert!(
                    live_free,
                    "the crate released a second live authority over an unconsumed one"
                );
                self.root = Root::Aggregate(aggregate);
                self.live = LiveAuthority::DetachAttempt(attempt);
                self.issued += 1;
                self.refresh_pending();
                StepOutcome::Applied(Box::new(self))
            }
            DetachTransportAttemptDecision::Refused(refusal) => {
                let (aggregate, ()) = refusal.into_parts();
                self.root = Root::Aggregate(aggregate);
                StepOutcome::Refused
            }
        }
    }

    fn step_take_abandonment(mut self, stats: &mut Stats) -> StepOutcome {
        let Root::Aggregate(mut aggregate) = self.root else {
            return StepOutcome::Unattemptable;
        };
        if let Some(abandonment) = aggregate.take_restored_operation_abandonment() {
            if abandonment.was_issued() {
                self.consumed += 1;
                stats.issued_abandonments += 1;
            }
            self.root = Root::Aggregate(aggregate);
            self.refresh_pending();
            StepOutcome::Applied(Box::new(self))
        } else {
            self.root = Root::Aggregate(aggregate);
            StepOutcome::Refused
        }
    }

    fn step_record(mut self, stats: &mut Stats) -> TestResult<StepOutcome> {
        let Root::Aggregate(aggregate) = self.root else {
            return Ok(StepOutcome::Unattemptable);
        };
        match record_operation(aggregate, self.kind.request()?) {
            ClientOperationRecordDecision::Pending(pending) => {
                self.root = Root::Pending(pending);
                Ok(StepOutcome::Applied(Box::new(self)))
            }
            ClientOperationRecordDecision::Continuous(_) => {
                Err("property kinds are never continuous acknowledgements")
            }
            ClientOperationRecordDecision::Refused(refusal) => {
                let reason = refusal.reason();
                assert!(
                    matches!(
                        reason,
                        ClientOperationRecordRefusalReason::OutstandingOperation
                            | ClientOperationRecordRefusalReason::BindingMismatch
                            | ClientOperationRecordRefusalReason::AlreadyDead
                            | ClientOperationRecordRefusalReason::DetachReplayIncompatible
                            | ClientOperationRecordRefusalReason::DetachReplayOutstanding
                            | ClientOperationRecordRefusalReason::AbandonmentPending
                    ),
                    "record refusal must carry a legal typed reason"
                );
                if reason == ClientOperationRecordRefusalReason::DetachReplayIncompatible {
                    stats.incompatible_re_records += 1;
                }
                let (aggregate, _) = refusal.into_parts();
                self.root = Root::Aggregate(aggregate);
                self.refresh_pending();
                self.assert_conservation();
                Ok(StepOutcome::Refused)
            }
        }
    }

    fn step_fate(mut self) -> TestResult<StepOutcome> {
        let LiveAuthority::Correlation(correlation) = self.live else {
            return Ok(StepOutcome::Unattemptable);
        };
        let Root::Aggregate(aggregate) = self.root else {
            return Ok(StepOutcome::Unattemptable);
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
            OperationKind::Attach | OperationKind::Record | OperationKind::Observer => {
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
        self.refresh_pending();
        Ok(StepOutcome::Applied(Box::new(self)))
    }

    fn step_outcome(mut self, stats: &mut Stats) -> TestResult<StepOutcome> {
        let LiveAuthority::Correlation(correlation) = self.live else {
            return Ok(StepOutcome::Unattemptable);
        };
        let Root::Aggregate(aggregate) = self.root else {
            return Ok(StepOutcome::Unattemptable);
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
            OperationKind::Attach => {
                let value = crate::wire::AttachBound::ordinary(
                    111,
                    AttachAttemptToken::new([115; 16]),
                    112,
                    generation(7)?,
                    AttachSecret::new([116; 32]),
                    epoch(8)?,
                    0,
                    0,
                    0,
                )
                .ok_or("attach outcome must carry the successor generation")?;
                let ClientCorrelatedInboundDecision::Applied(applied) = decide_correlated_inbound(
                    aggregate,
                    ServerValue::AttachBound(value),
                    correlation,
                ) else {
                    return Err("exact attach outcome must consume authority");
                };
                self.root = Root::Aggregate(applied.into_parts().0);
                self.live = LiveAuthority::None;
                self.consumed += 1;
                stats.attach_applications += 1;
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
                        record_admission_attempt_token:
                            crate::wire::RecordAdmissionAttemptToken::new([0xA7; 16]),
                    },
                    1,
                ));
                let ClientCorrelatedInboundDecision::Applied(applied) =
                    decide_correlated_inbound(aggregate, value, correlation)
                else {
                    return Err("exact record attempt token must consume authority");
                };
                self.root = Root::Aggregate(applied.into_parts().0);
                self.live = LiveAuthority::None;
                self.consumed += 1;
            }
        }
        self.refresh_pending();
        Ok(StepOutcome::Applied(Box::new(self)))
    }

    /// Encodes, decodes, and restores the current durable state. The dropped
    /// live handle models process loss; the crate must answer with its own
    /// serialized atom, which this step observes through crate accessors.
    fn step_restore(mut self, stats: &mut Stats) -> TestResult<StepOutcome> {
        let record = match &self.root {
            Root::Aggregate(aggregate) => aggregate
                .resume_record()
                .map_err(|_| "aggregate must encode")?,
            Root::Commit(commit) => commit.resume_record().map_err(|_| "commit must encode")?,
            Root::Pending(_) => return Ok(StepOutcome::Unattemptable),
        };
        let canonical = record.encode_canonical();
        let record = ClientResumeRecord::decode_canonical(&canonical)
            .map_err(|_| "canonical record must decode")?;
        let destroyed_live = self.live.count() == 1;
        self.live = LiveAuthority::None;
        let aggregate = record.restore().map_err(|_| "record must restore")?;
        if destroyed_live {
            if self.kind.tokenless() {
                assert!(
                    aggregate.lost_operation_testimony().is_none(),
                    "tokenless loss is recorded as abandonment, not testimony"
                );
                assert!(
                    aggregate
                        .restored_operation_abandonment()
                        .is_some_and(RestoredExpectedOperationAbandonment::was_issued),
                    "the crate must record destroyed tokenless authority as an issued abandonment"
                );
            } else {
                let expected_kind = match self.kind {
                    OperationKind::Detach => LostAuthorityKind::DetachTransportAttempt,
                    OperationKind::Attach | OperationKind::Record | OperationKind::Observer => {
                        LostAuthorityKind::IssuedOperationCorrelation
                    }
                };
                let minted = aggregate
                    .lost_operation_testimony()
                    .map(LostAuthorityTestimony::kind);
                assert_eq!(
                    minted,
                    Some(expected_kind),
                    "the crate must testify destroyed live authority at restore"
                );
                match expected_kind {
                    LostAuthorityKind::DetachTransportAttempt => {
                        stats.detach_testimonies += 1;
                    }
                    LostAuthorityKind::IssuedOperationCorrelation
                    | LostAuthorityKind::ReconnectPermit
                    | LostAuthorityKind::ReconnectAttempt => {
                        stats.correlation_testimonies += 1;
                    }
                }
            }
        }
        self.root = Root::Aggregate(aggregate);
        self.refresh_pending();
        Ok(StepOutcome::Applied(Box::new(self)))
    }

    fn step_resolve_loss(mut self, stats: &mut Stats) -> StepOutcome {
        let Root::Aggregate(aggregate) = self.root else {
            return StepOutcome::Unattemptable;
        };
        let pending = aggregate.lost_operation_testimony().is_some();
        match resolve_lost_operation_authority(aggregate) {
            LostOperationAuthorityDecision::Recorded {
                aggregate,
                testimony,
                ..
            } => {
                assert!(pending, "resolution without a pending testimony");
                assert_eq!(
                    testimony.kind(),
                    LostAuthorityKind::IssuedOperationCorrelation
                );
                stats.recorded_resolutions += 1;
                self.root = Root::Aggregate(aggregate);
                self.consumed += 1;
                self.refresh_pending();
                StepOutcome::Applied(Box::new(self))
            }
            LostOperationAuthorityDecision::DetachParked {
                aggregate,
                testimony,
                ..
            } => {
                assert!(pending, "resolution without a pending testimony");
                assert_eq!(testimony.kind(), LostAuthorityKind::DetachTransportAttempt);
                stats.detach_parked_resolutions += 1;
                self.root = Root::Aggregate(aggregate);
                self.consumed += 1;
                self.refresh_pending();
                StepOutcome::Applied(Box::new(self))
            }
            LostOperationAuthorityDecision::Refused { aggregate, reason } => {
                assert!(!pending, "the crate refused to consume a pending testimony");
                assert_eq!(
                    reason,
                    LostAuthorityResolutionRefusalReason::NoPendingTestimony
                );
                self.root = Root::Aggregate(aggregate);
                StepOutcome::Refused
            }
        }
    }
}

fn run_path(kind: OperationKind, path: &[Action], stats: &mut Stats) -> TestResult<StepOutcome> {
    let mut harness = Harness::new(kind)?;
    harness.assert_conservation();
    for (index, action) in path.iter().enumerate() {
        match harness.step(*action, stats)? {
            StepOutcome::Applied(next) => harness = *next,
            outcome => {
                if index + 1 != path.len() {
                    return Err("only the final action of an extension may be non-applied");
                }
                return Ok(outcome);
            }
        }
    }
    Ok(StepOutcome::Applied(Box::new(harness)))
}

fn enumerate(kind: OperationKind, path: &mut Vec<Action>, stats: &mut Stats) -> TestResult {
    if path.len() == MAX_DEPTH {
        return Ok(());
    }
    for action in ACTIONS {
        path.push(action);
        match run_path(kind, path, stats)? {
            StepOutcome::Applied(_) => {
                stats.visited += 1;
                enumerate(kind, path, stats)?;
            }
            // The final action was attempted and answered by the crate:
            // either a typed refusal over unchanged state (asserted inside
            // the step) or a type-level absence of the one-use value it
            // consumes. Neither creates a new state to extend, so the
            // identical subtree is not re-entered.
            StepOutcome::Refused => stats.refused += 1,
            StepOutcome::Unattemptable => stats.unattemptable += 1,
        }
        path.pop();
    }
    Ok(())
}

#[test]
fn exhaustive_small_scope_send_authority_conservation() -> TestResult {
    let mut stats = Stats::default();
    for kind in [
        OperationKind::Detach,
        OperationKind::Attach,
        OperationKind::Record,
        OperationKind::Observer,
    ] {
        enumerate(kind, &mut Vec::new(), &mut stats)?;
    }
    assert!(
        stats.incompatible_re_records > 0,
        "same-envelope re-record over an inactive replay was never attempted"
    );
    assert!(
        stats.detach_testimonies > 0 && stats.correlation_testimonies > 0,
        "restore never testified both destroyed-authority kinds"
    );
    assert!(
        stats.detach_parked_resolutions > 0 && stats.recorded_resolutions > 0,
        "both testimony resolutions must be reachable"
    );
    assert!(
        stats.issued_abandonments > 0,
        "issued tokenless loss was never recorded as durable abandonment"
    );
    assert!(
        stats.attach_applications > 0,
        "token-bearing non-detach consumption was never exercised"
    );
    // Derived counts, drift-checked only; equality here is not offered as
    // correctness evidence.
    assert_eq!(
        (stats.visited, stats.refused, stats.unattemptable),
        (631, 1087, 1747),
        "bounded interleaving space changed"
    );
    Ok(())
}
