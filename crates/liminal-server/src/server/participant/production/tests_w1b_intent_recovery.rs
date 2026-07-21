use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use liminal::durability::{
    DurabilityError, DurableStore, StoredEntry, bridge::block_on, open_ephemeral,
};
use liminal_protocol::wire::{ClientRequest, EnrollmentRequest, EnrollmentToken, ServerValue};

use crate::config::types::ParticipantConfig;
use crate::server::participant::incarnation_stream::{
    ConnectionFateClass, ConnectionFateIntent, IncarnationAllocation, IncarnationStartup,
    IncarnationStream,
};
use crate::server::participant::{
    ParticipantConnectionConversations, ParticipantSemanticError, ParticipantSemanticHandler,
    ParticipantServiceFatal,
};

use super::ProductionParticipantHandler;
use super::log::{
    DecodedStoredOperation, OperationLog, STREAM_PREFIX, StoredDiedCause, StoredOperation,
};
use super::tests::{dispatch_tracked, test_participant_config};

const CONVERSATIONS: [u64; 4] = [101, 103, 107, 109];
const FAILURE_CONVERSATION: u64 = CONVERSATIONS[2];

#[derive(Debug)]
struct FailOneStreamAppend {
    inner: Arc<dyn DurableStore>,
    target: Mutex<Option<String>>,
    fired: AtomicBool,
}

impl FailOneStreamAppend {
    fn new(inner: Arc<dyn DurableStore>) -> Self {
        Self {
            inner,
            target: Mutex::new(None),
            fired: AtomicBool::new(false),
        }
    }

    fn arm(&self, stream_key: String) -> Result<(), Box<dyn Error>> {
        let mut target = self
            .target
            .lock()
            .map_err(|_| "connection-fate append gate lock was poisoned")?;
        *target = Some(stream_key);
        drop(target);
        self.fired.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl DurableStore for FailOneStreamAppend {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        let must_fail = {
            let mut target = self.target.lock().map_err(|_| {
                DurabilityError::ConfigError(
                    "connection-fate append gate lock was poisoned".to_owned(),
                )
            })?;
            if target.as_deref() == Some(stream_key) {
                target.take();
                true
            } else {
                false
            }
        };
        if must_fail {
            self.fired.store(true, Ordering::SeqCst);
            return Err(DurabilityError::ConfigError(format!(
                "injected post-middle append failure for {stream_key}"
            )));
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

fn operation_rows(
    store: Arc<dyn DurableStore>,
    conversation_id: u64,
) -> Result<Vec<StoredOperation>, Box<dyn Error>> {
    let log = OperationLog::new(store, conversation_id);
    let mut rows = Vec::new();
    let mut sequence = 0_u64;
    loop {
        let Some(decoded) = block_on(log.read_at(sequence))?? else {
            break;
        };
        let DecodedStoredOperation::V3(operation) = decoded.operation else {
            return Err(format!(
                "conversation {conversation_id} sequence {sequence} was not schema v3"
            )
            .into());
        };
        rows.push(operation);
        sequence = sequence
            .checked_add(1)
            .ok_or("operation-row fixture sequence overflow")?;
    }
    Ok(rows)
}

fn incarnation_rows(store: &Arc<dyn DurableStore>) -> Result<Vec<StoredEntry>, Box<dyn Error>> {
    let limit = usize::try_from(test_participant_config().max_retained_record_rows)?;
    Ok(block_on(store.read_from(
        IncarnationStream::stream_key(),
        0,
        limit,
    ))??)
}

struct InterruptedFold {
    store: Arc<dyn DurableStore>,
    config: ParticipantConfig,
    reference_bound: usize,
    intent: ConnectionFateIntent,
}

fn enroll_conversations(
    handler: &ProductionParticipantHandler,
    connection_incarnation: liminal_protocol::wire::ConnectionIncarnation,
) -> Result<Vec<u64>, Box<dyn Error>> {
    let mut tracked = ParticipantConnectionConversations::default();
    for (index, conversation_id) in CONVERSATIONS.iter().copied().enumerate() {
        let token_byte = u8::try_from(
            index
                .checked_add(1)
                .ok_or("enrollment-token fixture index overflow")?,
        )?;
        let enrolled = dispatch_tracked(
            handler,
            connection_incarnation,
            &mut tracked,
            ClientRequest::Enrollment(EnrollmentRequest {
                conversation_id,
                enrollment_token: EnrollmentToken::new([token_byte; 16]),
            }),
        )?;
        if !matches!(enrolled, ServerValue::EnrollBound(_)) {
            return Err(format!(
                "conversation {conversation_id} did not establish a real Bound slot: {enrolled:?}"
            )
            .into());
        }
    }
    let tracked_conversations = tracked.tracked_conversations();
    assert_eq!(tracked_conversations, CONVERSATIONS);
    Ok(tracked_conversations)
}

fn assert_interrupted_rows(
    store: &Arc<dyn DurableStore>,
    open_sequence: u64,
) -> Result<Vec<StoredOperation>, Box<dyn Error>> {
    let first_rows = operation_rows(Arc::clone(store), CONVERSATIONS[0])?;
    let middle_rows = operation_rows(Arc::clone(store), CONVERSATIONS[1])?;
    let failure_rows = operation_rows(Arc::clone(store), CONVERSATIONS[2])?;
    let tail_rows = operation_rows(Arc::clone(store), CONVERSATIONS[3])?;
    assert_eq!(first_rows.len(), middle_rows.len());
    assert_eq!(
        first_rows.len(),
        failure_rows
            .len()
            .checked_add(1)
            .ok_or("post-middle row-count fixture overflow")?,
        "the first and middle source rows must flush before the injected failure"
    );
    assert_eq!(failure_rows.len(), tail_rows.len());
    for rows in [&first_rows, &middle_rows] {
        let Some(StoredOperation::Died { row }) = rows.last() else {
            return Err("completed pre-failure conversation lacks its terminal Died row".into());
        };
        assert_eq!(row.cause, StoredDiedCause::ConnectionLost);
        assert_eq!(row.connection_intent_sequence, Some(open_sequence));
    }
    Ok(failure_rows)
}

fn interrupt_after_middle() -> Result<InterruptedFold, Box<dyn Error>> {
    let inner: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let gated = Arc::new(FailOneStreamAppend::new(Arc::clone(&inner)));
    let store: Arc<dyn DurableStore> = gated.clone();
    let config = test_participant_config();
    let reference_bound = usize::try_from(config.max_semantic_conversations_per_connection)?;
    let startup = block_on(IncarnationStream::new(Arc::clone(&store), reference_bound).startup())??;
    let IncarnationStartup::Started(mut started) = startup else {
        return Err("fresh incarnation stream did not start".into());
    };
    let allocation = block_on(started.allocate(&[]))??;
    let IncarnationAllocation::Allocated {
        connection_incarnation,
        ..
    } = allocation
    else {
        return Err("fresh incarnation allocation was exhausted".into());
    };
    let handler = Arc::new(ProductionParticipantHandler::new(
        Arc::clone(&store),
        config,
    )?);
    let tracked_conversations = enroll_conversations(handler.as_ref(), connection_incarnation)?;
    let intent = block_on(started.open_connection_fate(
        connection_incarnation,
        ConnectionFateClass::ConnectionLost,
        reference_bound,
        &tracked_conversations,
    ))??;
    gated.arm(format!("{STREAM_PREFIX}{FAILURE_CONVERSATION}"))?;

    let failed = handler.handle_connection_fate(intent.work_item());
    assert!(matches!(
        failed,
        Err(ParticipantSemanticError::Internal { .. })
    ));
    assert!(
        gated.fired(),
        "the deterministic post-middle gate did not fire"
    );
    let fatal = handler
        .service_fatal()?
        .ok_or("post-Open failure did not latch participant service fatal")?;
    assert_eq!(
        fatal,
        ParticipantServiceFatal::ConnectionFateIntentIncomplete {
            open_sequence: intent.open_sequence,
            conversation_id: FAILURE_CONVERSATION,
        }
    );
    let failure_rows = assert_interrupted_rows(&store, intent.open_sequence)?;
    let incarnation_before_refusal = incarnation_rows(&store)?;
    let refused = handler.handle_connection_fate(intent.work_item());
    assert!(matches!(
        refused,
        Err(ParticipantSemanticError::ServiceFatal(observed)) if observed == fatal
    ));
    assert_eq!(incarnation_rows(&store)?, incarnation_before_refusal);
    assert_eq!(
        operation_rows(Arc::clone(&store), FAILURE_CONVERSATION)?,
        failure_rows
    );

    let durable_before_drop: Vec<_> = CONVERSATIONS
        .iter()
        .copied()
        .map(|conversation_id| operation_rows(Arc::clone(&store), conversation_id))
        .collect::<Result<_, _>>()?;
    drop(handler);
    drop(started);
    let durable_after_drop: Vec<_> = CONVERSATIONS
        .iter()
        .copied()
        .map(|conversation_id| operation_rows(Arc::clone(&store), conversation_id))
        .collect::<Result<_, _>>()?;
    assert_eq!(durable_after_drop, durable_before_drop);
    Ok(InterruptedFold {
        store,
        config,
        reference_bound,
        intent,
    })
}

fn recover_interrupted_tail(interrupted: &InterruptedFold) -> Result<(), Box<dyn Error>> {
    let cold =
        ProductionParticipantHandler::new(Arc::clone(&interrupted.store), interrupted.config)?;
    let startup = block_on(
        IncarnationStream::new(Arc::clone(&interrupted.store), interrupted.reference_bound)
            .startup(),
    )??;
    let IncarnationStartup::RecoveryRequired(mut recovery) = startup else {
        return Err("unmatched post-Open failure did not require startup recovery".into());
    };
    let intents = recovery.intents();
    assert_eq!(
        intents.as_slice(),
        core::slice::from_ref(&interrupted.intent)
    );
    cold.handle_connection_fate(intents[0].work_item())?;
    for conversation_id in CONVERSATIONS {
        let rows = operation_rows(Arc::clone(&interrupted.store), conversation_id)?;
        let Some(StoredOperation::Died { row }) = rows.last() else {
            return Err(format!(
                "startup tail completion omitted conversation {conversation_id} terminal row"
            )
            .into());
        };
        assert_eq!(row.cause, StoredDiedCause::ConnectionLost);
        assert_eq!(
            row.connection_intent_sequence,
            Some(interrupted.intent.open_sequence)
        );
    }
    let before_complete = incarnation_rows(&interrupted.store)?;
    block_on(recovery.complete(interrupted.intent.open_sequence))??;
    let after_complete = incarnation_rows(&interrupted.store)?;
    assert_eq!(
        after_complete.len(),
        before_complete
            .len()
            .checked_add(1)
            .ok_or("incarnation-row fixture count overflow")?,
        "Complete must append only after every tail binding source is observable"
    );
    let resumed = block_on(recovery.finish_startup())??;
    assert!(matches!(resumed, IncarnationStartup::Started(_)));
    assert_eq!(cold.service_fatal()?, None);
    Ok(())
}

fn run_post_middle_failure_recovery() -> Result<(), Box<dyn Error>> {
    let interrupted = interrupt_after_middle()?;
    recover_interrupted_tail(&interrupted)
}

#[test]
fn connection_fate_intent_failure_on_middle_conversation_completes_every_tail_binding()
-> Result<(), Box<dyn Error>> {
    run_post_middle_failure_recovery()
}

#[test]
fn post_open_middle_failure_latches_service_fatal_and_startup_completes_tail()
-> Result<(), Box<dyn Error>> {
    run_post_middle_failure_recovery()
}
