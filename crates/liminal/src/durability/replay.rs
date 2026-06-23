use super::{DurabilityError, DurableStore, MessageEnvelope, StoredEntry};

const READ_BATCH_SIZE: usize = 1_024;

/// Replays serialized durable channel envelopes from `offset` to the partition head.
///
/// # Errors
///
/// Propagates store read errors, envelope deserialization errors, and read-offset overflow.
pub async fn replay_from(
    store: &dyn DurableStore,
    partition_key: &str,
    offset: u64,
) -> Result<Vec<MessageEnvelope>, DurabilityError> {
    let mut stored_entries = Vec::new();
    let mut next_offset = offset;
    loop {
        let batch = store
            .read_from(partition_key, next_offset, READ_BATCH_SIZE)
            .await?;
        let batch_len = batch.len();
        if batch_len == 0 {
            break;
        }
        stored_entries.extend(batch);
        next_offset = next_offset
            .checked_add(len_to_u64(batch_len)?)
            .ok_or_else(|| {
                DurabilityError::ConfigError("replay read offset overflow".to_owned())
            })?;
    }

    deserialize_in_sequence_order(stored_entries)
}

fn deserialize_in_sequence_order(
    mut stored_entries: Vec<StoredEntry>,
) -> Result<Vec<MessageEnvelope>, DurabilityError> {
    stored_entries.sort_by_key(|entry| entry.sequence);

    let mut envelopes = Vec::with_capacity(stored_entries.len());
    for stored in stored_entries {
        envelopes.push(MessageEnvelope::deserialize(&stored.payload)?);
    }
    Ok(envelopes)
}

fn len_to_u64(len: usize) -> Result<u64, DurabilityError> {
    u64::try_from(len).map_err(|error| {
        DurabilityError::ConfigError(format!("entry count cannot fit u64: {error}"))
    })
}

#[cfg(test)]
mod tests;
