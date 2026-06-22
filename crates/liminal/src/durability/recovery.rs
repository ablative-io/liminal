#![allow(clippy::module_name_repetitions)]

use std::sync::Arc;

use super::{
    ConsumerCursor, DurabilityError, DurableChannel, DurableConversation, DurableStore,
    MessageEnvelope, replay_from,
};

#[cfg(test)]
mod tests;

const READ_BATCH_SIZE: usize = 1_024;

/// Cursor state plus missed messages replayed from the recovered cursor offset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveredCursor {
    /// Recovered cursor positioned at the persisted offset, or zero when absent.
    pub cursor: ConsumerCursor,
    /// Durable channel messages replayed from the cursor position to the partition head.
    pub replayed_messages: Vec<MessageEnvelope>,
}

/// Recovers all durable channel partition next-sequence counters from haematite logs.
///
/// # Errors
///
/// Propagates store read errors and returns [`DurabilityError::ConfigError`] on offset or
/// sequence overflow.
pub async fn recover_partition_sequences(
    channel_id: &str,
    partition_count: usize,
    store: &dyn DurableStore,
) -> Result<Vec<u64>, DurabilityError> {
    let mut next_sequences = Vec::with_capacity(partition_count);
    for partition_index in 0..partition_count {
        let stream_key = partition_stream_key(channel_id, partition_index);
        next_sequences.push(recover_partition_sequence(store, &stream_key).await?);
    }
    Ok(next_sequences)
}

/// Recovers a durable channel by reconstructing each partition sequence from storage.
///
/// # Errors
///
/// Propagates store read errors and channel recovery validation errors.
pub async fn recover_durable_channel(
    channel_id: impl Into<String>,
    partition_count: usize,
    store: Arc<dyn DurableStore>,
) -> Result<DurableChannel, DurabilityError> {
    let channel_id = channel_id.into();
    let next_sequences =
        recover_partition_sequences(&channel_id, partition_count, store.as_ref()).await?;
    DurableChannel::from_recovered_sequences(channel_id, partition_count, store, next_sequences)
}

/// Recovers a durable conversation by delegating to the event-log replay path.
///
/// # Errors
///
/// Propagates store read errors, event deserialization errors, and replay validation errors.
pub async fn recover_conversation(
    conversation_id: impl Into<String>,
    store: Arc<dyn DurableStore>,
) -> Result<DurableConversation, DurabilityError> {
    DurableConversation::recover(conversation_id, store).await
}

/// Recovers a consumer cursor by delegating to the cursor resume path.
///
/// # Errors
///
/// Propagates store read errors from cursor resume.
pub async fn recover_cursor(
    consumer_id: impl Into<String>,
    partition_key: impl Into<String>,
    store: &dyn DurableStore,
) -> Result<ConsumerCursor, DurabilityError> {
    ConsumerCursor::resume(consumer_id, partition_key, store).await
}

/// Recovers a consumer cursor and replays missed durable messages from that offset.
///
/// # Errors
///
/// Propagates cursor resume, store replay, and envelope deserialization errors.
pub async fn recover_cursor_with_replay(
    consumer_id: impl Into<String>,
    partition_key: impl Into<String>,
    store: &dyn DurableStore,
) -> Result<RecoveredCursor, DurabilityError> {
    let cursor = recover_cursor(consumer_id, partition_key, store).await?;
    let replayed_messages =
        replay_from(store, cursor.partition_key(), cursor.current_offset()).await?;
    Ok(RecoveredCursor {
        cursor,
        replayed_messages,
    })
}

async fn recover_partition_sequence(
    store: &dyn DurableStore,
    stream_key: &str,
) -> Result<u64, DurabilityError> {
    let mut offset = 0;
    let mut last_sequence: Option<u64> = None;
    loop {
        let batch = store.read_from(stream_key, offset, READ_BATCH_SIZE).await?;
        let batch_len = batch.len();
        if batch_len == 0 {
            break;
        }
        for stored in &batch {
            // R1 spec mandates the HIGHEST sequence in the log. Use an explicit max rather
            // than relying on haematite returning entries in ascending order, so a future
            // out-of-order read path cannot silently recover the wrong sequence.
            last_sequence = Some(last_sequence.map_or(stored.sequence, |s| s.max(stored.sequence)));
        }
        offset = offset.checked_add(len_to_u64(batch_len)?).ok_or_else(|| {
            DurabilityError::ConfigError("channel recovery read offset overflow".to_owned())
        })?;
        if batch_len < READ_BATCH_SIZE {
            break;
        }
    }
    last_sequence.map_or(Ok(0), |sequence| {
        sequence.checked_add(1).ok_or_else(|| {
            DurabilityError::ConfigError(
                "sequence number overflow after channel recovery".to_owned(),
            )
        })
    })
}

fn partition_stream_key(channel_id: &str, partition_index: usize) -> String {
    format!("{channel_id}:{partition_index}")
}

fn len_to_u64(len: usize) -> Result<u64, DurabilityError> {
    u64::try_from(len).map_err(|error| {
        DurabilityError::ConfigError(format!(
            "channel recovery entry count cannot fit u64: {error}"
        ))
    })
}
