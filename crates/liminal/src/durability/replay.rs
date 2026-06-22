use super::{DurabilityError, DurableStore, MessageEnvelope};

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
    let mut envelopes = Vec::new();
    let mut next_offset = offset;
    loop {
        let batch = store
            .read_from(partition_key, next_offset, READ_BATCH_SIZE)
            .await?;
        let batch_len = batch.len();
        if batch_len == 0 {
            break;
        }
        for stored in batch {
            envelopes.push(MessageEnvelope::deserialize(&stored.payload)?);
        }
        next_offset = next_offset
            .checked_add(len_to_u64(batch_len)?)
            .ok_or_else(|| {
                DurabilityError::ConfigError("replay read offset overflow".to_owned())
            })?;
        if batch_len < READ_BATCH_SIZE {
            break;
        }
    }
    Ok(envelopes)
}

fn len_to_u64(len: usize) -> Result<u64, DurabilityError> {
    u64::try_from(len).map_err(|error| {
        DurabilityError::ConfigError(format!("entry count cannot fit u64: {error}"))
    })
}
