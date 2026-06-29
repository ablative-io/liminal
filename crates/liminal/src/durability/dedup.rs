use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use super::{DurabilityError, DurableStore, ProcessingReceipt, StoredEntry};
use codec::DedupRecord;
pub use sweep::{DedupSweepReport, DedupSweeper};

mod codec;
mod sweep;
#[cfg(test)]
mod tests;

const READ_BATCH_SIZE: usize = 1_024;

/// Hashes a producer idempotency key into a deterministic stream-key suffix.
#[must_use]
pub fn key_hash(idempotency_key: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in idempotency_key.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

/// Persisted dedup cache entry for one producer idempotency key.
#[derive(Clone, PartialEq, Eq)]
pub struct DedupEntry {
    idempotency_key: String,
    receipt: Option<Vec<u8>>,
    timestamp_millis: u64,
}

impl DedupEntry {
    /// Builds an entry containing the original key, optional receipt bytes, and epoch millis.
    #[must_use]
    pub fn new(
        idempotency_key: impl Into<String>,
        receipt: Option<Vec<u8>>,
        timestamp_millis: u64,
    ) -> Self {
        Self {
            idempotency_key: idempotency_key.into(),
            receipt,
            timestamp_millis,
        }
    }

    /// Returns the original idempotency key stored with this entry.
    #[must_use]
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    /// Returns the stored opaque receipt bytes, when processing has completed.
    #[must_use]
    pub fn receipt(&self) -> Option<&[u8]> {
        self.receipt.as_deref()
    }

    /// Returns the entry timestamp in epoch milliseconds.
    #[must_use]
    pub const fn timestamp_millis(&self) -> u64 {
        self.timestamp_millis
    }

    /// Serializes this active cache entry into deterministic storage bytes.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError::EnvelopeError`] when a field length cannot be encoded.
    pub fn serialize(&self) -> Result<Vec<u8>, DurabilityError> {
        DedupRecord::Active(self.clone()).serialize()
    }

    /// Deserializes an active cache entry previously produced by [`Self::serialize`].
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError::EnvelopeError`] when bytes are malformed or contain a tombstone.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, DurabilityError> {
        match DedupRecord::deserialize(bytes)? {
            DedupRecord::Active(entry) => Ok(entry),
            DedupRecord::Tombstone { .. } => Err(DurabilityError::EnvelopeError(
                "dedup tombstone is not an active entry".to_owned(),
            )),
        }
    }
}

impl fmt::Debug for DedupEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DedupEntry")
            .field("idempotency_key", &self.idempotency_key)
            .field("receipt_bytes", &self.receipt.as_ref().map(Vec::len))
            .field("timestamp_millis", &self.timestamp_millis)
            .finish()
    }
}

/// Result of checking or claiming an idempotency key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DedupDecision {
    /// The caller claimed the key and may deliver the message for processing.
    Claimed,
    /// The key was already completed; return this receipt without re-delivery.
    Completed(ProcessingReceipt),
    /// The key is already being processed and delivery must be deferred.
    InFlight,
}

/// Haematite-backed idempotency-key cache for the lightweight dedup contract.
#[derive(Clone)]
pub struct DedupCache {
    store: Arc<dyn DurableStore>,
    namespace: String,
}

impl DedupCache {
    /// Creates a dedup cache over the given durable store and namespace prefix.
    #[must_use]
    pub fn new(store: Arc<dyn DurableStore>, namespace: impl Into<String>) -> Self {
        Self {
            store,
            namespace: namespace.into(),
        }
    }

    /// Returns the configured dedup namespace.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Formats the haematite stream key for an idempotency key.
    #[must_use]
    pub fn stream_key_for(&self, idempotency_key: &str) -> String {
        format!("{}:{}", self.namespace, key_hash(idempotency_key))
    }

    /// Looks up an existing key without appending or refreshing cache state.
    ///
    /// # Errors
    ///
    /// Propagates store read errors and returns [`DurabilityError::DedupCollision`] when
    /// the hashed stream contains a different original idempotency key.
    pub async fn lookup(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<DedupDecision>, DurabilityError> {
        let stream_key = self.stream_key_for(idempotency_key);
        let snapshot = self.load_snapshot(&stream_key, idempotency_key).await?;
        Ok(snapshot.current.as_ref().map(decision_for_entry))
    }

    /// Claims a new key or returns the existing completed/in-flight decision.
    ///
    /// Callers must only deliver the message when this returns [`DedupDecision::Claimed`].
    ///
    /// # Errors
    ///
    /// Propagates store errors and serialization errors. A concurrent first claim that wins
    /// the append race is converted into the duplicate completed/in-flight decision.
    pub async fn claim_or_get(
        &self,
        idempotency_key: &str,
        timestamp_millis: u64,
    ) -> Result<DedupDecision, DurabilityError> {
        let stream_key = self.stream_key_for(idempotency_key);
        let snapshot = self.load_snapshot(&stream_key, idempotency_key).await?;
        if let Some(entry) = snapshot.current.as_ref() {
            return Ok(decision_for_entry(entry));
        }

        let entry = DedupEntry::new(idempotency_key, None, timestamp_millis);
        match self
            .store
            .append(&stream_key, entry.serialize()?, snapshot.next_seq)
            .await
        {
            Ok(_) => Ok(DedupDecision::Claimed),
            Err(DurabilityError::SequenceConflict { expected, actual }) => {
                self.decision_after_conflict(&stream_key, idempotency_key, expected, actual)
                    .await
            }
            Err(error) => Err(error),
        }
    }

    /// Stores a processing receipt by appending a completed entry for an existing key.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError::ConfigError`] when the current system time cannot be encoded.
    /// Returns [`DurabilityError::DedupCollision`] when the key is missing or collides with
    /// another original key, and propagates serialization/store append errors.
    pub async fn complete_receipt(
        &self,
        idempotency_key: &str,
        receipt: ProcessingReceipt,
    ) -> Result<(), DurabilityError> {
        self.complete_receipt_at(idempotency_key, receipt, current_epoch_millis()?)
            .await
    }

    /// Stores a processing receipt with an explicit receipt timestamp.
    ///
    /// This is useful for deterministic tests and for callers that already have a trusted
    /// processing-completion timestamp. The timestamp is the TTL anchor for the stored receipt.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError::DedupCollision`] when the key is missing, collides with
    /// another original key, or has already completed with different receipt bytes. Propagates
    /// serialization/store append errors.
    async fn complete_receipt_at(
        &self,
        idempotency_key: &str,
        receipt: ProcessingReceipt,
        timestamp_millis: u64,
    ) -> Result<(), DurabilityError> {
        let stream_key = self.stream_key_for(idempotency_key);
        let snapshot = self.load_snapshot(&stream_key, idempotency_key).await?;
        let Some(entry) = snapshot.current.as_ref() else {
            return Err(DurabilityError::DedupCollision {
                key: idempotency_key.to_owned(),
            });
        };

        let receipt_bytes = receipt.into_bytes();
        if let Some(existing_receipt) = entry.receipt() {
            if existing_receipt == receipt_bytes.as_slice() {
                return Ok(());
            }
            return Err(DurabilityError::DedupCollision {
                key: idempotency_key.to_owned(),
            });
        }

        let completed = DedupEntry::new(
            entry.idempotency_key().to_owned(),
            Some(receipt_bytes.clone()),
            timestamp_millis,
        );
        match self
            .store
            .append(&stream_key, completed.serialize()?, snapshot.next_seq)
            .await
        {
            Ok(_) => Ok(()),
            Err(DurabilityError::SequenceConflict { expected, actual }) => {
                self.confirm_matching_receipt(
                    &stream_key,
                    idempotency_key,
                    &receipt_bytes,
                    expected,
                    actual,
                )
                .await
            }
            Err(error) => Err(error),
        }
    }

    /// Releases an in-flight claim so the key becomes re-claimable.
    ///
    /// Callers use this on the publish failure path: a key was claimed
    /// ([`DedupDecision::Claimed`]) but the downstream delivery failed before a
    /// receipt could be recorded, leaving the key dangling [`DedupDecision::InFlight`]
    /// forever and permanently suppressing every re-publish. Releasing appends a
    /// tombstone so the next claim succeeds.
    ///
    /// This NEVER clobbers a stored receipt: an already-completed key (receipt
    /// present) and an absent key are both no-ops, preserving the at-most-once
    /// guarantee. Only a current in-flight entry (active, no receipt) is tombstoned.
    ///
    /// # Errors
    ///
    /// Propagates store read/append and serialization errors. A [`DurabilityError::SequenceConflict`]
    /// on the tombstone append is re-checked: if the latest state is now completed
    /// or already a tombstone the release is treated as successful, otherwise the
    /// conflict is propagated.
    pub async fn release_claim(&self, idempotency_key: &str) -> Result<(), DurabilityError> {
        self.release_claim_at(idempotency_key, current_epoch_millis()?)
            .await
    }

    async fn release_claim_at(
        &self,
        idempotency_key: &str,
        timestamp_millis: u64,
    ) -> Result<(), DurabilityError> {
        let stream_key = self.stream_key_for(idempotency_key);
        let snapshot = self.load_snapshot(&stream_key, idempotency_key).await?;
        // No-op when there is nothing in flight: absent key, or a completed key
        // whose receipt must never be clobbered (guards at-most-once).
        let Some(entry) = snapshot.current.as_ref() else {
            return Ok(());
        };
        if entry.receipt().is_some() {
            return Ok(());
        }

        let tombstone = DedupRecord::tombstone(idempotency_key.to_owned(), timestamp_millis);
        match self
            .store
            .append(&stream_key, tombstone.serialize()?, snapshot.next_seq)
            .await
        {
            Ok(_) => Ok(()),
            Err(DurabilityError::SequenceConflict { expected, actual }) => {
                self.confirm_release_after_conflict(&stream_key, idempotency_key, expected, actual)
                    .await
            }
            Err(error) => Err(error),
        }
    }

    async fn confirm_release_after_conflict(
        &self,
        stream_key: &str,
        idempotency_key: &str,
        expected: u64,
        actual: u64,
    ) -> Result<(), DurabilityError> {
        // A concurrent writer advanced the stream after our snapshot. Re-load and
        // re-check the latest record directly (not via `into_active`, so a fresh
        // tombstone is distinguishable): if it is now completed (receipt present)
        // or already a tombstone, the in-flight claim is gone and the release goal
        // is met. A still-active no-receipt entry means a legitimate re-claim won
        // the race, so we must not clobber it -- propagate the conflict.
        let latest = self.latest_record(stream_key, idempotency_key).await?;
        match latest {
            Some(DedupRecord::Tombstone { .. }) => Ok(()),
            Some(DedupRecord::Active(entry)) if entry.receipt().is_some() => Ok(()),
            _ => Err(DurabilityError::SequenceConflict { expected, actual }),
        }
    }

    async fn latest_record(
        &self,
        stream_key: &str,
        idempotency_key: &str,
    ) -> Result<Option<DedupRecord>, DurabilityError> {
        let entries = self.read_stream(stream_key).await?;
        let mut latest = None;
        for stored in entries {
            let record = DedupRecord::deserialize(&stored.payload)?;
            if record.idempotency_key() != idempotency_key {
                return Err(DurabilityError::DedupCollision {
                    key: idempotency_key.to_owned(),
                });
            }
            latest = Some(record);
        }
        Ok(latest)
    }

    fn scan_prefix(&self) -> String {
        format!("{}:", self.namespace)
    }

    async fn decision_after_conflict(
        &self,
        stream_key: &str,
        idempotency_key: &str,
        expected: u64,
        actual: u64,
    ) -> Result<DedupDecision, DurabilityError> {
        let snapshot = self.load_snapshot(stream_key, idempotency_key).await?;
        snapshot.current.as_ref().map_or(
            Err(DurabilityError::SequenceConflict { expected, actual }),
            |entry| Ok(decision_for_entry(entry)),
        )
    }

    async fn confirm_matching_receipt(
        &self,
        stream_key: &str,
        idempotency_key: &str,
        receipt_bytes: &[u8],
        expected: u64,
        actual: u64,
    ) -> Result<(), DurabilityError> {
        let snapshot = self.load_snapshot(stream_key, idempotency_key).await?;
        if snapshot
            .current
            .as_ref()
            .and_then(DedupEntry::receipt)
            .is_some_and(|bytes| bytes == receipt_bytes)
        {
            Ok(())
        } else {
            Err(DurabilityError::SequenceConflict { expected, actual })
        }
    }

    async fn load_snapshot(
        &self,
        stream_key: &str,
        idempotency_key: &str,
    ) -> Result<StreamSnapshot, DurabilityError> {
        let entries = self.read_stream(stream_key).await?;
        let next_seq = len_to_u64(entries.len())?;
        let mut current = None;
        for stored in entries {
            let record = DedupRecord::deserialize(&stored.payload)?;
            if record.idempotency_key() != idempotency_key {
                return Err(DurabilityError::DedupCollision {
                    key: idempotency_key.to_owned(),
                });
            }
            current = Some(record);
        }
        Ok(StreamSnapshot {
            current: current.and_then(DedupRecord::into_active),
            next_seq,
        })
    }

    async fn read_stream(&self, stream_key: &str) -> Result<Vec<StoredEntry>, DurabilityError> {
        let mut entries = Vec::new();
        let mut offset = 0;
        loop {
            let batch = self
                .store
                .read_from(stream_key, offset, READ_BATCH_SIZE)
                .await?;
            let batch_len = batch.len();
            if batch_len == 0 {
                break;
            }
            entries.extend(batch);
            offset = offset.checked_add(len_to_u64(batch_len)?).ok_or_else(|| {
                DurabilityError::ConfigError("dedup read offset overflow".to_owned())
            })?;
            if batch_len < READ_BATCH_SIZE {
                break;
            }
        }
        Ok(entries)
    }
}

impl fmt::Debug for DedupCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DedupCache")
            .field("namespace", &self.namespace)
            .field("store", &self.store)
            .finish()
    }
}

struct StreamSnapshot {
    current: Option<DedupEntry>,
    next_seq: u64,
}

fn decision_for_entry(entry: &DedupEntry) -> DedupDecision {
    entry.receipt().map_or(DedupDecision::InFlight, |bytes| {
        DedupDecision::Completed(ProcessingReceipt::new(bytes.to_vec()))
    })
}

fn len_to_u64(len: usize) -> Result<u64, DurabilityError> {
    u64::try_from(len).map_err(|error| {
        DurabilityError::ConfigError(format!("dedup entry count cannot fit u64: {error}"))
    })
}

fn current_epoch_millis() -> Result<u64, DurabilityError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            DurabilityError::ConfigError(format!("system clock is before Unix epoch: {error}"))
        })?;
    u64::try_from(duration.as_millis()).map_err(|error| {
        DurabilityError::ConfigError(format!(
            "current epoch millis cannot fit u64 for dedup receipt: {error}"
        ))
    })
}
