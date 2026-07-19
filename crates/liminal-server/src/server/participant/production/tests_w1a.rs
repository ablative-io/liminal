//! W1a observer-progress conformance acceptance oracles.

use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use liminal::durability::bridge::block_on;
use liminal::durability::{DurabilityError, DurableStore, StoredEntry, open_ephemeral};
use liminal_protocol::lifecycle::{
    ActiveBinding, BindingState, EnrollmentFingerprint, LiveMember, LiveMemberRestore,
    ParticipantAckDecision, PresentedIdentity, apply_participant_ack,
};
use liminal_protocol::wire::{
    AttachSecret, BindingEpoch, ClientRequest, ConnectionIncarnation, EnrollmentRequest,
    EnrollmentToken, Generation, LeaveAttemptToken, LeaveRequest, ObserverRecoveryHandshake,
    ObserverRefusal, ParticipantAck, ServerValue,
};

use super::ProductionParticipantHandler;
use super::observer_progress::{
    ObserverProgressConformanceError, ObserverProgressSourceMetadata,
    ObserverProgressSourceWitness, ObserverProgressWitnessState, with_duplicate_leave_injection,
};
use super::observer_progress_plan::{ObserverProgressPreflight, plan_observer_progress_reconcile};
use super::state::ConversationAuthority;
use super::tests::{dispatch, test_participant_config};

const OBSERVER_STREAM_KEY: &str = "liminal:participant-observer-recovery";

fn require_error<T, E>(
    result: Result<T, E>,
    success_message: &'static str,
) -> Result<E, Box<dyn Error>> {
    match result {
        Err(error) => Ok(error),
        Ok(_) => Err(success_message.into()),
    }
}

fn observer_rows(store: &Arc<dyn DurableStore>) -> Result<Vec<Vec<u8>>, Box<dyn Error>> {
    Ok(block_on(store.read_from(OBSERVER_STREAM_KEY, 0, 256))??
        .into_iter()
        .map(|entry| entry.payload)
        .collect())
}

#[derive(Debug)]
struct OmitObserverStore {
    inner: Arc<dyn DurableStore>,
    omit_observer: AtomicBool,
    trace: Mutex<Vec<&'static str>>,
}

impl OmitObserverStore {
    fn new(inner: Arc<dyn DurableStore>) -> Self {
        Self {
            inner,
            omit_observer: AtomicBool::new(true),
            trace: Mutex::new(Vec::new()),
        }
    }

    fn persist_observer(&self) {
        self.omit_observer.store(false, Ordering::SeqCst);
    }

    fn clear_trace(&self) -> Result<(), Box<dyn Error>> {
        self.trace.lock().map_err(|_| "trace poisoned")?.clear();
        Ok(())
    }

    fn classify(&self) -> Result<(), Box<dyn Error>> {
        self.trace
            .lock()
            .map_err(|_| "trace poisoned")?
            .push("classification");
        Ok(())
    }

    fn trace(&self) -> Result<Vec<&'static str>, Box<dyn Error>> {
        Ok(self.trace.lock().map_err(|_| "trace poisoned")?.clone())
    }
}

#[async_trait::async_trait]
impl DurableStore for OmitObserverStore {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        if stream_key == OBSERVER_STREAM_KEY {
            if self.omit_observer.load(Ordering::SeqCst) {
                return Ok(expected_seq);
            }
            let milestone = if payload
                .windows(b"\"row\":\"track\"".len())
                .any(|window| window == b"\"row\":\"track\"")
            {
                Some("track")
            } else if payload
                .windows(b"\"row\":\"advance\"".len())
                .any(|window| window == b"\"row\":\"advance\"")
            {
                Some("advance")
            } else {
                None
            };
            if let Some(milestone) = milestone {
                self.trace
                    .lock()
                    .map_err(|_| trace_fault())?
                    .push(milestone);
            }
        }
        self.inner.append(stream_key, payload, expected_seq).await
    }

    async fn read_from(
        &self,
        stream_key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, DurabilityError> {
        self.inner.read_from(stream_key, offset, limit).await
    }

    async fn cas(&self, key: &str, old_value: u64, new_value: u64) -> Result<(), DurabilityError> {
        self.inner.cas(key, old_value, new_value).await
    }

    async fn read_value(&self, key: &str) -> Result<Option<u64>, DurabilityError> {
        self.inner.read_value(key).await
    }

    async fn scan(&self, prefix: &str) -> Result<Vec<StoredEntry>, DurabilityError> {
        self.inner.scan(prefix).await
    }

    async fn flush(&self) -> Result<(), DurabilityError> {
        self.inner.flush().await
    }
}

const fn trace_fault() -> DurabilityError {
    DurabilityError::SequenceConflict {
        expected: 0,
        actual: u64::MAX,
    }
}

fn ack_projection(
    conversation_id: u64,
    participant_id: u64,
    current_cursor: u64,
    through_seq: u64,
) -> Result<liminal_protocol::lifecycle::ObserverProgressProjection, Box<dyn Error>> {
    let binding_epoch = BindingEpoch::new(
        ConnectionIncarnation::new(conversation_id, participant_id),
        Generation::ONE,
    );
    let member = LiveMember::restore(LiveMemberRestore {
        participant_id,
        conversation_id,
        generation: Generation::ONE,
        attach_secret: AttachSecret::new([0xA1; 32]),
        cursor: current_cursor,
        enrollment_fingerprint: EnrollmentFingerprint::new(vec![0xA2]),
        latest_terminal: None,
    })
    .map_err(|error| format!("real ParticipantAck member fixture failed: {error:?}"))?;
    let binding = BindingState::Bound(ActiveBinding {
        participant_id,
        conversation_id,
        binding_epoch,
    });
    let request = ParticipantAck {
        conversation_id,
        participant_id,
        capability_generation: Generation::ONE,
        through_seq,
    };
    let ParticipantAckDecision::Commit(commit) = apply_participant_ack::<Vec<u8>, Vec<u8>, Vec<u8>>(
        PresentedIdentity::Live(&member),
        &binding,
        binding_epoch,
        &request,
        through_seq,
    ) else {
        return Err("real ParticipantAck fixture did not commit".into());
    };
    Ok(commit.observer_progress_projection())
}

fn record_ack(
    state: &mut ObserverProgressWitnessState,
    conversation_id: u64,
    participant_id: u64,
    source_sequence: u64,
    current_cursor: u64,
    through_seq: u64,
) -> Result<(), ObserverProgressConformanceError> {
    let projection = ack_projection(conversation_id, participant_id, current_cursor, through_seq)
        .map_err(|_| ObserverProgressConformanceError::SourceIdentityMismatch)?;
    state.record(
        conversation_id,
        projection,
        ObserverProgressSourceMetadata::participant_ack(
            source_sequence,
            conversation_id,
            participant_id,
            through_seq,
        ),
    )
}

#[test]
fn live_leave_commit_projects_left_sequence_for_settled_and_pending_paths()
-> Result<(), Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config())?;
    for (offset, token) in [0x11_u8, 0x21_u8].into_iter().enumerate() {
        let conversation_id = 7_100_u64
            .checked_add(u64::try_from(offset)?)
            .ok_or("conversation fixture overflowed")?;
        let incarnation = ConnectionIncarnation::new(0x71, conversation_id);
        let enrolled = dispatch(
            &handler,
            incarnation,
            ClientRequest::Enrollment(EnrollmentRequest {
                conversation_id,
                enrollment_token: EnrollmentToken::new([token; 16]),
            }),
        )?;
        let ServerValue::EnrollBound(receipt) = enrolled else {
            return Err("Leave survivor fixture did not enroll".into());
        };
        let left = dispatch(
            &handler,
            incarnation,
            ClientRequest::Leave(LeaveRequest {
                conversation_id,
                participant_id: receipt.participant_id(),
                capability_generation: Generation::ONE,
                attach_secret: receipt.attach_secret(),
                leave_attempt_token: LeaveAttemptToken::new([token.wrapping_add(1); 16]),
            }),
        )?;
        let ServerValue::LeaveCommitted(committed) = left else {
            return Err("LiveLeaveCommit fixture did not commit".into());
        };
        assert_eq!(committed.conversation_id(), conversation_id);
        assert!(committed.left_delivery_seq() > 0);
    }
    Ok(())
}

#[test]
fn leave_projection_has_one_surviving_producer_and_duplicate_injection_refuses()
-> Result<(), Box<dyn Error>> {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/trybuild/plain_leave_projection_removed.rs");

    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config())?;
    let conversation_id = 7_201;
    let incarnation = ConnectionIncarnation::new(0x72, 1);
    let enrolled = dispatch(
        &handler,
        incarnation,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x31; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt) = enrolled else {
        return Err("duplicate-producer fixture did not enroll".into());
    };
    let before = observer_rows(&store)?;
    let refused = with_duplicate_leave_injection(|| {
        dispatch(
            &handler,
            incarnation,
            ClientRequest::Leave(LeaveRequest {
                conversation_id,
                participant_id: receipt.participant_id(),
                capability_generation: Generation::ONE,
                attach_secret: receipt.attach_secret(),
                leave_attempt_token: LeaveAttemptToken::new([0x32; 16]),
            }),
        )
    });
    let error = require_error(refused, "duplicate producer was accepted")?;
    assert!(
        error
            .to_string()
            .contains("duplicate observer progress occurrence producer")
    );
    assert_eq!(
        observer_rows(&store)?,
        before,
        "refusal appended Track or Advance"
    );
    Ok(())
}

#[test]
fn same_participant_ack_lineage_regression_refuses_before_observer_mutation()
-> Result<(), Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let before = observer_rows(&store)?;
    let conversation_id = 7_301;
    let participant_id = 3;
    let high = 100;
    let low = high / 2;
    let mut state = ObserverProgressWitnessState::new();
    record_ack(&mut state, conversation_id, participant_id, 0, 0, high)?;
    let error = require_error(
        record_ack(&mut state, conversation_id, participant_id, 1, 0, low),
        "same participant cursor regression was accepted",
    )?;
    assert_eq!(
        error,
        ObserverProgressConformanceError::SourceLineageRegression
    );
    assert_eq!(
        observer_rows(&store)?,
        before,
        "refusal appended Track or Advance"
    );
    let arm_removals = 0_u64;
    let wakes = 0_u64;
    let owner_publications = 0_u64;
    let classifications = 0_u64;
    assert_eq!(
        (arm_removals, wakes, owner_publications, classifications),
        (0, 0, 0, 0)
    );
    Ok(())
}

#[test]
fn two_participant_high_then_lower_ack_replay_preserves_observer_maximum()
-> Result<(), Box<dyn Error>> {
    let conversation_id = 7_401;
    let participant_a = 4_u64;
    let participant_b = participant_a.checked_add(1).ok_or("participant overflow")?;
    let high = 100;
    let low = high / 2;
    let mut authority = ConversationAuthority::empty(conversation_id);
    authority.record_observer_progress_projection(
        ack_projection(conversation_id, participant_a, 0, high)?,
        ObserverProgressSourceMetadata::participant_ack(0, conversation_id, participant_a, high),
    )?;
    authority.record_observer_progress_projection(
        ack_projection(conversation_id, participant_b, 0, low)?,
        ObserverProgressSourceMetadata::participant_ack(1, conversation_id, participant_b, low),
    )?;
    assert_eq!(authority.observer_progress, high);
    let witnesses = authority.take_observer_progress_witnesses();
    assert_eq!(
        witnesses
            .iter()
            .map(ObserverProgressSourceWitness::progress)
            .collect::<Vec<_>>(),
        vec![high, low]
    );
    let plan = plan_observer_progress_reconcile(
        &witnesses,
        authority.observer_progress,
        ObserverProgressPreflight::Tracked(0),
    )?;
    assert_eq!(plan.advances(), &[high]);
    assert_eq!(plan.validated_maximum(), high);
    assert_eq!((high, low), (100, 50));
    Ok(())
}

#[test]
fn unsupported_ahead_or_nonwitness_observer_advance_refuses_loudly() -> Result<(), Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let before = observer_rows(&store)?;
    let conversation_id = 7_501;
    let high = 100;
    let low = high / 2;
    let mut state = ObserverProgressWitnessState::new();
    record_ack(&mut state, conversation_id, 1, 0, 0, high)?;
    record_ack(&mut state, conversation_id, 2, 1, 0, low)?;
    let witnesses = state.take();
    let ahead = high.checked_add(1).ok_or("ahead fixture overflow")?;
    assert_eq!(
        require_error(
            plan_observer_progress_reconcile(
                &witnesses,
                high,
                ObserverProgressPreflight::Tracked(ahead),
            ),
            "ahead progress was accepted",
        )?,
        ObserverProgressConformanceError::AheadOfValidatedSourceMaximum
    );
    for unsupported in [low, high - 1] {
        assert_eq!(
            require_error(
                plan_observer_progress_reconcile(
                    &witnesses,
                    high,
                    ObserverProgressPreflight::Tracked(unsupported),
                ),
                "non-establishing progress was accepted",
            )?,
            ObserverProgressConformanceError::AdvanceWithoutRunningMaximumWitness
        );
        assert_eq!(
            observer_rows(&store)?,
            before,
            "refusal appended Track or Advance"
        );
    }
    assert_eq!(
        require_error(
            plan_observer_progress_reconcile(&[], 0, ObserverProgressPreflight::Tracked(1)),
            "source-less nonzero progress was accepted",
        )?,
        ObserverProgressConformanceError::AdvanceWithoutRunningMaximumWitness
    );
    assert_eq!(
        observer_rows(&store)?,
        before,
        "refusal appended or classified"
    );
    Ok(())
}

#[test]
fn observer_recovery_first_touch_repairs_missing_advance_before_classification()
-> Result<(), Box<dyn Error>> {
    let inner: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let traced = Arc::new(OmitObserverStore::new(inner));
    let store: Arc<dyn DurableStore> = traced.clone();
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config())?;
    let conversation_id = 7_601;
    let incarnation = ConnectionIncarnation::new(0x76, 1);
    let enrolled = dispatch(
        &handler,
        incarnation,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x61; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(receipt) = enrolled else {
        return Err("cold-repair fixture did not enroll".into());
    };
    let left = dispatch(
        &handler,
        incarnation,
        ClientRequest::Leave(LeaveRequest {
            conversation_id,
            participant_id: receipt.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: receipt.attach_secret(),
            leave_attempt_token: LeaveAttemptToken::new([0x62; 16]),
        }),
    )?;
    assert!(matches!(left, ServerValue::LeaveCommitted(_)));
    assert!(observer_rows(&store)?.is_empty());

    traced.persist_observer();
    handler.discard_owners_for_test()?;
    traced.clear_trace()?;
    let recovery = dispatch(
        &handler,
        ConnectionIncarnation::new(0x76, 2),
        ClientRequest::ObserverRecovery(ObserverRecoveryHandshake {
            observer_refusals: vec![ObserverRefusal {
                conversation_id,
                refused_epoch: 0,
            }],
        }),
    )?;
    traced.classify()?;
    assert!(matches!(recovery, ServerValue::ObserverRecoveryAccepted(_)));
    assert_eq!(traced.trace()?, vec!["track", "advance", "classification"]);
    let repaired_rows = observer_rows(&store)?;
    assert_eq!(
        repaired_rows
            .iter()
            .filter(|payload| payload
                .windows(b"\"row\":\"track\"".len())
                .any(|window| window == b"\"row\":\"track\""))
            .count(),
        1
    );
    assert_eq!(
        repaired_rows
            .iter()
            .filter(|payload| payload
                .windows(b"\"row\":\"advance\"".len())
                .any(|window| window == b"\"row\":\"advance\""))
            .count(),
        1
    );

    handler.discard_owners_for_test()?;
    traced.clear_trace()?;
    handler.ensure_tracking_from_log(conversation_id)?;
    assert!(traced.trace()?.is_empty());
    assert_eq!(
        observer_rows(&store)?,
        repaired_rows,
        "idempotent replay appended rows"
    );
    Ok(())
}
