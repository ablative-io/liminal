use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use super::{DurabilityError, DurableStore};

mod codec;
#[cfg(test)]
mod tests;

const READ_BATCH_SIZE: usize = 1_024;

/// Event-sourced durable conversation state transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConversationEvent {
    /// A message entered the conversation.
    MessageReceived {
        /// Stable message identifier used to correlate all later events.
        message_id: String,
        /// Caller-provided epoch-millisecond receive timestamp.
        received_at: u64,
    },
    /// Processing started for a received message.
    ProcessingStarted {
        /// Stable message identifier used to correlate all later events.
        message_id: String,
    },
    /// One processing step completed successfully.
    StepCompleted {
        /// Stable message identifier used to correlate all later events.
        message_id: String,
        /// Zero-based step index completed by the conversation worker.
        step_index: u32,
        /// Opaque, deterministic output checkpoint for the completed step.
        output: Vec<u8>,
    },
    /// Processing finished for a message.
    ProcessingFinished {
        /// Stable message identifier used to correlate all later events.
        message_id: String,
    },
    /// Processing failed for a message.
    ErrorOccurred {
        /// Stable message identifier used to correlate all later events.
        message_id: String,
        /// Error text recorded for replay and operator visibility.
        error: String,
    },
}

impl ConversationEvent {
    /// Returns the message identifier carried by every event variant.
    #[must_use]
    pub fn message_id(&self) -> &str {
        match self {
            Self::MessageReceived { message_id, .. }
            | Self::ProcessingStarted { message_id }
            | Self::StepCompleted { message_id, .. }
            | Self::ProcessingFinished { message_id }
            | Self::ErrorOccurred { message_id, .. } => message_id,
        }
    }
}

/// Replay-derived durable conversation processing state.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConversationState {
    /// Messages that have `MessageReceived` events.
    pub received_messages: HashSet<String>,
    /// Messages with `ProcessingStarted` and no later terminal event.
    pub in_progress: HashSet<String>,
    /// Completed step outputs indexed by `(message_id, step_index)`.
    pub completed_steps: HashMap<(String, u32), Vec<u8>>,
    /// Messages that have `ProcessingFinished` events.
    pub finished_messages: HashSet<String>,
    /// Messages that have `ErrorOccurred` events, mapped to the recorded error.
    pub errored_messages: HashMap<String, String>,
}

impl ConversationState {
    /// Replays events into a fresh conversation state.
    #[must_use]
    pub fn replay(events: &[ConversationEvent]) -> Self {
        let mut state = Self::default();
        for event in events {
            state.apply(event);
        }
        state
    }

    /// Applies one event to this state using idempotent set/map updates.
    pub fn apply(&mut self, event: &ConversationEvent) {
        match event {
            ConversationEvent::MessageReceived { message_id, .. } => {
                self.received_messages.insert(message_id.clone());
            }
            ConversationEvent::ProcessingStarted { message_id } => {
                self.in_progress.insert(message_id.clone());
            }
            ConversationEvent::StepCompleted {
                message_id,
                step_index,
                output,
            } => {
                self.completed_steps
                    .insert((message_id.clone(), *step_index), output.clone());
            }
            ConversationEvent::ProcessingFinished { message_id } => {
                self.finished_messages.insert(message_id.clone());
                self.in_progress.remove(message_id);
            }
            ConversationEvent::ErrorOccurred { message_id, error } => {
                self.errored_messages
                    .insert(message_id.clone(), error.clone());
                self.in_progress.remove(message_id);
            }
        }
    }

    /// Returns true when the replayed log contains `ProcessingFinished` for the message.
    #[must_use]
    pub fn is_fully_processed(&self, message_id: &str) -> bool {
        self.finished_messages.contains(message_id)
    }

    /// Returns the highest completed step for `message_id`, if any.
    #[must_use]
    pub fn last_completed_step(&self, message_id: &str) -> Option<u32> {
        self.completed_steps
            .keys()
            .filter(|(stored_id, _)| stored_id.as_str() == message_id)
            .map(|(_, step_index)| *step_index)
            .max()
    }

    fn next_step_index(&self, message_id: &str) -> Result<u32, DurabilityError> {
        self.last_completed_step(message_id).map_or(Ok(0), |step| {
            step.checked_add(1).ok_or_else(|| {
                DurabilityError::ConfigError("conversation step index overflow".to_owned())
            })
        })
    }
}

/// Decision returned when a durable conversation receives a message delivery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RedeliveryDecision {
    /// The message was already fully processed and no new event was appended.
    Skip,
    /// The message was seen before and should resume at the given step index.
    ResumeFrom(u32),
    /// The message was never seen before and normal processing should start.
    Start,
}

/// Haematite-backed event-sourced durable conversation.
#[derive(Clone)]
pub struct DurableConversation {
    conversation_id: String,
    store: Arc<dyn DurableStore>,
    state: ConversationState,
    expected_seq: u64,
}

impl DurableConversation {
    /// Creates a new durable conversation with empty replay state and expected sequence zero.
    #[must_use]
    pub fn new(conversation_id: impl Into<String>, store: Arc<dyn DurableStore>) -> Self {
        Self {
            conversation_id: conversation_id.into(),
            store,
            state: ConversationState::default(),
            expected_seq: 0,
        }
    }

    /// Recovers a durable conversation by replaying its full event log from sequence zero.
    ///
    /// # Errors
    ///
    /// Propagates store read errors, event deserialization errors, and sequence overflow.
    pub async fn recover(
        conversation_id: impl Into<String>,
        store: Arc<dyn DurableStore>,
    ) -> Result<Self, DurabilityError> {
        let conversation_id = conversation_id.into();
        let (state, expected_seq) = replay_stream(store.as_ref(), &conversation_id).await?;
        Ok(Self {
            conversation_id,
            store,
            state,
            expected_seq,
        })
    }

    /// Returns the conversation stream key used for all appends and replay reads.
    #[must_use]
    pub fn conversation_id(&self) -> &str {
        &self.conversation_id
    }

    /// Returns the current replay-derived state.
    #[must_use]
    pub const fn state(&self) -> &ConversationState {
        &self.state
    }

    /// Returns the next expected sequence used for optimistic append concurrency.
    #[must_use]
    pub const fn expected_seq(&self) -> u64 {
        self.expected_seq
    }

    /// Handles a message delivery using replay-derived duplicate detection.
    ///
    /// # Errors
    ///
    /// Returns serialization or store append errors for never-seen messages, and returns
    /// [`DurabilityError::ConfigError`] if a partial message cannot advance its step index.
    pub async fn receive_message(
        &mut self,
        message_id: impl Into<String>,
        received_at: u64,
    ) -> Result<RedeliveryDecision, DurabilityError> {
        let message_id = message_id.into();
        if self.state.is_fully_processed(&message_id) {
            return Ok(RedeliveryDecision::Skip);
        }
        if self.state.received_messages.contains(&message_id) {
            return Ok(RedeliveryDecision::ResumeFrom(
                self.state.next_step_index(&message_id)?,
            ));
        }
        self.record_message_received(message_id, received_at)
            .await?;
        Ok(RedeliveryDecision::Start)
    }

    /// Appends a `MessageReceived` event using the current expected sequence.
    ///
    /// # Errors
    ///
    /// Propagates event serialization errors and all store append errors, including
    /// [`DurabilityError::SequenceConflict`] without retrying.
    pub async fn record_message_received(
        &mut self,
        message_id: impl Into<String>,
        received_at: u64,
    ) -> Result<u64, DurabilityError> {
        self.append_event(ConversationEvent::MessageReceived {
            message_id: message_id.into(),
            received_at,
        })
        .await
    }

    /// Appends a `ProcessingStarted` event using the current expected sequence.
    ///
    /// # Errors
    ///
    /// Propagates event serialization errors and all store append errors, including
    /// [`DurabilityError::SequenceConflict`] without retrying.
    pub async fn record_processing_started(
        &mut self,
        message_id: impl Into<String>,
    ) -> Result<u64, DurabilityError> {
        self.append_event(ConversationEvent::ProcessingStarted {
            message_id: message_id.into(),
        })
        .await
    }

    /// Appends a `StepCompleted` event using the current expected sequence.
    ///
    /// # Errors
    ///
    /// Propagates event serialization errors and all store append errors, including
    /// [`DurabilityError::SequenceConflict`] without retrying.
    pub async fn record_step_completed(
        &mut self,
        message_id: impl Into<String>,
        step_index: u32,
        output: Vec<u8>,
    ) -> Result<u64, DurabilityError> {
        self.append_event(ConversationEvent::StepCompleted {
            message_id: message_id.into(),
            step_index,
            output,
        })
        .await
    }

    /// Appends a `ProcessingFinished` event using the current expected sequence.
    ///
    /// # Errors
    ///
    /// Propagates event serialization errors and all store append errors, including
    /// [`DurabilityError::SequenceConflict`] without retrying.
    pub async fn record_processing_finished(
        &mut self,
        message_id: impl Into<String>,
    ) -> Result<u64, DurabilityError> {
        self.append_event(ConversationEvent::ProcessingFinished {
            message_id: message_id.into(),
        })
        .await
    }

    /// Appends an `ErrorOccurred` event using the current expected sequence.
    ///
    /// # Errors
    ///
    /// Propagates event serialization errors and all store append errors, including
    /// [`DurabilityError::SequenceConflict`] without retrying.
    pub async fn record_error(
        &mut self,
        message_id: impl Into<String>,
        error: impl Into<String>,
    ) -> Result<u64, DurabilityError> {
        self.append_event(ConversationEvent::ErrorOccurred {
            message_id: message_id.into(),
            error: error.into(),
        })
        .await
    }

    async fn append_event(&mut self, event: ConversationEvent) -> Result<u64, DurabilityError> {
        let payload = event.serialize()?;
        let assigned_seq = self
            .store
            .append(&self.conversation_id, payload, self.expected_seq)
            .await?;
        self.expected_seq = assigned_seq.checked_add(1).ok_or_else(|| {
            DurabilityError::ConfigError(
                "sequence number overflow after conversation append".to_owned(),
            )
        })?;
        self.state.apply(&event);
        Ok(assigned_seq)
    }
}

impl fmt::Debug for DurableConversation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DurableConversation")
            .field("conversation_id", &self.conversation_id)
            .field("state", &self.state)
            .field("expected_seq", &self.expected_seq)
            .field("store", &self.store)
            .finish()
    }
}

async fn replay_stream(
    store: &dyn DurableStore,
    conversation_id: &str,
) -> Result<(ConversationState, u64), DurabilityError> {
    let mut state = ConversationState::default();
    let mut offset = 0;
    let mut last_sequence = None;
    loop {
        let batch = store
            .read_from(conversation_id, offset, READ_BATCH_SIZE)
            .await?;
        let batch_len = batch.len();
        if batch_len == 0 {
            break;
        }
        for stored in &batch {
            let event = ConversationEvent::deserialize(&stored.payload)?;
            state.apply(&event);
            last_sequence = Some(stored.sequence);
        }
        offset = offset.checked_add(len_to_u64(batch_len)?).ok_or_else(|| {
            DurabilityError::ConfigError("conversation read offset overflow".to_owned())
        })?;
        if batch_len < READ_BATCH_SIZE {
            break;
        }
    }
    let expected_seq = last_sequence.map_or(Ok(0), |sequence| {
        sequence.checked_add(1).ok_or_else(|| {
            DurabilityError::ConfigError(
                "sequence number overflow after conversation replay".to_owned(),
            )
        })
    })?;
    Ok((state, expected_seq))
}

fn len_to_u64(len: usize) -> Result<u64, DurabilityError> {
    u64::try_from(len).map_err(|error| {
        DurabilityError::ConfigError(format!("conversation entry count cannot fit u64: {error}"))
    })
}
