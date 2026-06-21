use std::collections::HashMap;
use std::time::Duration;

use crate::durability::{DurabilityError, StoredEntry};

use super::codec::DedupRecord;
use super::{DedupCache, DedupEntry};

const CLOCK_SKEW_GRACE_MILLIS: u64 = 1_000;

/// Summary produced by a dedup TTL sweep.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DedupSweepReport {
    scanned: usize,
    expired: usize,
    retained: usize,
}

impl DedupSweepReport {
    /// Number of scanned stored dedup events.
    #[must_use]
    pub const fn scanned_entries(self) -> usize {
        self.scanned
    }

    /// Number of logical cache entries tombstoned by the sweep.
    #[must_use]
    pub const fn expired_entries(self) -> usize {
        self.expired
    }

    /// Number of latest active entries retained after the sweep.
    #[must_use]
    pub const fn retained_entries(self) -> usize {
        self.retained
    }
}

/// Configurable TTL sweeper for haematite-backed dedup entries.
#[derive(Clone, Debug)]
pub struct DedupSweeper {
    cache: DedupCache,
    ttl: Duration,
    sweep_interval: Duration,
}

impl DedupSweeper {
    /// Creates a sweeper with caller-configured TTL and interval.
    #[must_use]
    pub const fn new(cache: DedupCache, ttl: Duration, sweep_interval: Duration) -> Self {
        Self {
            cache,
            ttl,
            sweep_interval,
        }
    }

    /// Returns the configured dedup TTL.
    #[must_use]
    pub const fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Returns the configured sweep interval.
    #[must_use]
    pub const fn sweep_interval(&self) -> Duration {
        self.sweep_interval
    }

    /// Runs one scan-based sweep at the supplied epoch millisecond timestamp.
    ///
    /// # Errors
    ///
    /// Propagates store scan/append errors and serialized-entry decode errors.
    pub async fn sweep_once(&self, now_millis: u64) -> Result<DedupSweepReport, DurabilityError> {
        let scanned = self.cache.store.scan(&self.cache.scan_prefix()).await?;
        let scanned_entries = scanned.len();
        let latest = latest_records_by_key(scanned)?;
        let ttl_millis = duration_millis_saturating(self.ttl);
        let mut expired_entries = 0;
        let mut retained_entries = 0;

        for candidate in latest.into_values() {
            let DedupRecord::Active(entry) = candidate.record else {
                continue;
            };
            if is_expired(entry.timestamp_millis(), now_millis, ttl_millis) {
                self.tombstone(&entry, candidate.sequence).await?;
                expired_entries += 1;
            } else {
                retained_entries += 1;
            }
        }

        Ok(DedupSweepReport {
            scanned: scanned_entries,
            expired: expired_entries,
            retained: retained_entries,
        })
    }

    async fn tombstone(&self, entry: &DedupEntry, sequence: u64) -> Result<(), DurabilityError> {
        let stream_key = self.cache.stream_key_for(entry.idempotency_key());
        let expected_seq = sequence.checked_add(1).ok_or_else(|| {
            DurabilityError::ConfigError("dedup sweep sequence overflow".to_owned())
        })?;
        let tombstone =
            DedupRecord::tombstone(entry.idempotency_key().to_owned(), entry.timestamp_millis());
        self.cache
            .store
            .append(&stream_key, tombstone.serialize()?, expected_seq)
            .await?;
        Ok(())
    }
}

struct SweepCandidate {
    record: DedupRecord,
    sequence: u64,
}

impl SweepCandidate {
    const fn new(record: DedupRecord, sequence: u64) -> Self {
        Self { record, sequence }
    }
}

fn latest_records_by_key(
    entries: Vec<StoredEntry>,
) -> Result<HashMap<String, SweepCandidate>, DurabilityError> {
    let mut latest: HashMap<String, SweepCandidate> = HashMap::new();
    for stored in entries {
        let record = DedupRecord::deserialize(&stored.payload)?;
        let key = record.idempotency_key().to_owned();
        match latest.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut existing) => {
                if stored.sequence >= existing.get().sequence {
                    existing.insert(SweepCandidate::new(record, stored.sequence));
                }
            }
            std::collections::hash_map::Entry::Vacant(vacant) => {
                vacant.insert(SweepCandidate::new(record, stored.sequence));
            }
        }
    }
    Ok(latest)
}

const fn is_expired(timestamp_millis: u64, now_millis: u64, ttl_millis: u64) -> bool {
    let expiry = timestamp_millis
        .saturating_add(ttl_millis)
        .saturating_add(CLOCK_SKEW_GRACE_MILLIS);
    now_millis > expiry
}

fn duration_millis_saturating(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
