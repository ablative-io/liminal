use std::sync::Arc;

use haematite::{ApiError, Database, DatabaseConfig, Event, EventStore};
use tempfile::TempDir;

use super::DurabilityError;

/// Entry read from a durable haematite stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredEntry {
    /// Opaque stored payload bytes.
    pub payload: Vec<u8>,
    /// Sequence number assigned by the stream.
    pub sequence: u64,
    /// Store timestamp associated with the entry.
    pub timestamp: u64,
}

/// Direct durability surface matching haematite's append/read/cas/scan API.
#[async_trait::async_trait]
pub trait DurableStore: std::fmt::Debug + Send + Sync {
    /// Appends `payload` to `stream_key` if `expected_seq` matches the stream head.
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError>;

    /// Reads entries from `stream_key` beginning at `offset`, up to `limit` entries.
    async fn read_from(
        &self,
        stream_key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, DurabilityError>;

    /// Atomically replaces a stored numeric value if it equals `old_value`.
    ///
    /// An `old_value` of `0` matches a key that is currently *absent* as well as
    /// one explicitly stored as `0`: a fresh cursor is created on its first
    /// checkpoint without a prior write. See [`HaematiteStore::cas`] for how this
    /// "absent == 0" contract is preserved atomically over the real engine.
    async fn cas(&self, key: &str, old_value: u64, new_value: u64) -> Result<(), DurabilityError>;

    /// Reads a numeric value previously updated through compare-and-swap.
    async fn read_value(&self, key: &str) -> Result<Option<u64>, DurabilityError>;

    /// Scans entries by store prefix.
    async fn scan(&self, prefix: &str) -> Result<Vec<StoredEntry>, DurabilityError>;

    /// Flushes buffered writes so completed durable operations are persisted.
    ///
    /// # Errors
    /// Returns [`DurabilityError`] when the underlying store cannot complete the flush.
    async fn flush(&self) -> Result<(), DurabilityError>;
}

/// `DurableStore` implementation that delegates directly to haematite's `EventStore`.
///
/// The real [`EventStore`] is synchronous (every call blocks on the owning
/// shard actor's reply), so each `async` method below completes on its first
/// poll. The synchronous bridge in [`super::bridge`] relies on exactly that.
#[derive(Clone, Debug)]
pub struct HaematiteStore {
    event_store: Arc<EventStore>,
}

impl HaematiteStore {
    /// Wraps a haematite `EventStore` handle.
    #[must_use]
    pub const fn new(event_store: Arc<EventStore>) -> Self {
        Self { event_store }
    }
}

#[async_trait::async_trait]
impl DurableStore for HaematiteStore {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        // Contract bridge: liminal's `DurableStore::append` returns the *assigned
        // event sequence* (0-based position of the just-appended event), which is
        // exactly `expected_seq` for a single append. The real `EventStore::append`
        // instead returns the stream's new next-sequence (`expected_seq + 1`), so
        // subtract one to recover the assigned seq. A `0` next-seq is impossible
        // after a successful single append, so the `checked_sub` cannot saturate
        // silently; if it ever did the engine returned a contract-violating value.
        let next_seq = self
            .event_store
            .append(stream_key.as_bytes(), &payload, expected_seq)
            .map_err(DurabilityError::from)?;
        next_seq.checked_sub(1).ok_or_else(|| {
            DurabilityError::StoreError(ApiError::CorruptEvent(format!(
                "append returned next-seq 0 for stream {stream_key}"
            )))
        })
    }

    async fn read_from(
        &self,
        stream_key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, DurabilityError> {
        // The real `read_from` returns every event with seq >= offset and applies
        // no limit; truncate to `limit` entries to honour the trait contract.
        let mut events = self
            .event_store
            .read_from(stream_key.as_bytes(), offset)
            .map_err(DurabilityError::from)?;
        events.truncate(limit);
        Ok(events.into_iter().map(StoredEntry::from).collect())
    }

    async fn cas(&self, key: &str, old_value: u64, new_value: u64) -> Result<(), DurabilityError> {
        // Preserve liminal's "absent == 0" cursor contract faithfully over an
        // engine that distinguishes `None` (absent) from `Some(0)` (a stored
        // zero). The invariant that makes the mapping below correct: we NEVER
        // persist a physical zero, so a logical value of 0 and physical absence
        // always coincide.
        //
        // A `cas` whose target `new_value` is 0 must therefore write nothing — it
        // only asserts the precondition. This is reachable as `cas(0, 0)` (a
        // cursor checkpoint at offset 0; offsets are monotonic so they never CAS
        // down to 0 from a higher value). Were we instead to let it store a
        // physical zero, the *next* `cas(0, n)` — mapped to expect-absent `None`
        // — would wrongly fail against the now-present key and permanently stall
        // the cursor. Asserting via a read is race-free here precisely because no
        // value is written, so there is no lost-update window.
        if new_value == 0 {
            return self
                .event_store
                .read_value(key.as_bytes())
                .map_err(DurabilityError::from)?
                .map_or(Ok(()), |stored| {
                    Err(DurabilityError::CursorRegression {
                        stored,
                        attempted: old_value,
                    })
                });
        }
        // With a physical zero never stored, `old_value == 0` is exactly the
        // expect-absent expectation. Any other `old_value` maps to `Some(_)`.
        // This is a single CAS routed to the owning shard actor, where read,
        // compare, and write run with no interleaving point (haematite's
        // `ShardActor::cas`) — the engine's atomicity is preserved end to end.
        let expected = if old_value == 0 {
            None
        } else {
            Some(old_value)
        };
        self.event_store
            .cas(key.as_bytes(), expected, new_value)
            .map_err(DurabilityError::from)
    }

    async fn read_value(&self, key: &str) -> Result<Option<u64>, DurabilityError> {
        self.event_store
            .read_value(key.as_bytes())
            .map_err(DurabilityError::from)
    }

    async fn scan(&self, prefix: &str) -> Result<Vec<StoredEntry>, DurabilityError> {
        // The real `scan` predicate yields stream *metadata* (key + next_seq),
        // not events. Liminal's contract is to return the events of every stream
        // whose key matches `prefix`, so collect the matching stream keys, then
        // read each stream's full event list and flatten the results.
        let prefix_bytes = prefix.as_bytes().to_vec();
        let matches = self
            .event_store
            .scan(|meta| meta.stream_key.starts_with(&prefix_bytes))
            .map_err(DurabilityError::from)?;
        let mut entries = Vec::new();
        for stream in matches {
            let events = self
                .event_store
                .read(&stream.stream_key)
                .map_err(DurabilityError::from)?;
            entries.extend(events.into_iter().map(StoredEntry::from));
        }
        Ok(entries)
    }

    async fn flush(&self) -> Result<(), DurabilityError> {
        self.event_store.flush().map_err(DurabilityError::from)
    }
}

/// Exclusive-ownership ephemeral durable store: the sole owner of both the
/// haematite database and the temporary directory that backs it.
///
/// [`HaematiteStore::new`] takes a *caller-supplied* `Arc<EventStore>`, so a
/// clone of that inner handle can outlive any guard placed merely beside it —
/// field declaration order proves nothing across that `Arc` boundary. This
/// wrapper instead owns the database outright: [`open_ephemeral`] constructs the
/// inner `Arc` itself, this type never exposes it (no getter) and is deliberately
/// **not `Clone`**, so the only handle a caller can hold is an
/// `Arc<dyn DurableStore>` over the whole wrapper. When the last such clone
/// drops, `store` drops FIRST — the database closes, its shard actors join and
/// the data-dir writer lock releases on fd close — and only THEN does
/// `_ephemeral_dir` drop and remove the directory. Field declaration order is
/// load-bearing and must not be reordered.
#[derive(Debug)]
pub struct EphemeralHaematiteStore {
    store: HaematiteStore,
    _ephemeral_dir: Option<TempDir>,
}

impl EphemeralHaematiteStore {
    /// Takes an already-open ephemeral `Database` and the temporary directory it
    /// was opened under, becoming their single exclusive owner.
    ///
    /// The inner `Arc<EventStore>` is created here and never leaves this type, so
    /// no caller-supplied clone of it can exist to defeat the drop ordering.
    /// `ephemeral_dir` must be the directory `database` lives in and must have
    /// been created before the database was opened (so a failed open removed it
    /// via the guard's `Drop`, before this constructor was ever reached).
    fn new(database: Database, ephemeral_dir: TempDir) -> Self {
        Self {
            store: HaematiteStore::new(Arc::new(EventStore::new(database))),
            _ephemeral_dir: Some(ephemeral_dir),
        }
    }

    /// Path of the guarding temporary directory, for lifecycle assertions only.
    ///
    /// The field carries an underscore because its sole production role is to be
    /// dropped last; this test-only reader is the one place it is observed.
    #[cfg(test)]
    #[allow(clippy::used_underscore_binding)]
    pub(crate) fn ephemeral_dir_path(&self) -> Option<&std::path::Path> {
        self._ephemeral_dir.as_ref().map(TempDir::path)
    }
}

#[async_trait::async_trait]
impl DurableStore for EphemeralHaematiteStore {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        self.store.append(stream_key, payload, expected_seq).await
    }

    async fn read_from(
        &self,
        stream_key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, DurabilityError> {
        self.store.read_from(stream_key, offset, limit).await
    }

    async fn cas(&self, key: &str, old_value: u64, new_value: u64) -> Result<(), DurabilityError> {
        self.store.cas(key, old_value, new_value).await
    }

    async fn read_value(&self, key: &str) -> Result<Option<u64>, DurabilityError> {
        self.store.read_value(key).await
    }

    async fn scan(&self, prefix: &str) -> Result<Vec<StoredEntry>, DurabilityError> {
        self.store.scan(prefix).await
    }

    async fn flush(&self) -> Result<(), DurabilityError> {
        self.store.flush().await
    }
}

/// Opens a self-owning ephemeral haematite store under a fresh temporary
/// directory.
///
/// The directory is created BEFORE [`Database::create`], so every failure path —
/// including a haematite open/create error — removes it when the guard drops on
/// the error return; the returned store owns the guard on success. The database
/// is created directly in the (empty) temporary directory: haematite's `create`
/// accepts an existing empty dir and, on failure, removes only a directory *it*
/// created, never this pre-existing guard dir (haematite 0.4.1
/// `db/startup.rs`), so the `TempDir` is the sole owner of directory lifetime on
/// every path.
///
/// # Errors
/// Returns [`DurabilityError::EphemeralStoreOpen`] if haematite cannot create the
/// database; the temporary directory is already removed when this returns.
pub fn open_ephemeral(shard_count: usize) -> Result<EphemeralHaematiteStore, DurabilityError> {
    let ephemeral_dir = tempfile::Builder::new()
        .prefix("liminal-durability-")
        .tempdir()
        .map_err(|error| {
            DurabilityError::EphemeralStoreOpen(format!(
                "could not create temporary directory: {error}"
            ))
        })?;
    open_ephemeral_in(ephemeral_dir, shard_count)
}

/// Opens an ephemeral store inside an already-created guard directory.
///
/// Split out so the guard exists before `Database::create` and so lifecycle
/// tests can inject an open failure into a directory they pre-populated.
fn open_ephemeral_in(
    ephemeral_dir: TempDir,
    shard_count: usize,
) -> Result<EphemeralHaematiteStore, DurabilityError> {
    let database = Database::create(DatabaseConfig {
        data_dir: ephemeral_dir.path().to_path_buf(),
        shard_count,
        sweep_interval: None,
        distributed: None,
    })
    .map_err(|error| DurabilityError::EphemeralStoreOpen(error.to_string()))?;
    Ok(EphemeralHaematiteStore::new(database, ephemeral_dir))
}

impl From<Event> for StoredEntry {
    fn from(event: Event) -> Self {
        Self {
            payload: event.payload,
            sequence: event.seq,
            timestamp: event.timestamp,
        }
    }
}

/// Maps a real-engine [`ApiError`] onto liminal's [`DurabilityError`].
///
/// The optimistic-concurrency variants route to their dedicated `DurabilityError`
/// cases (`SequenceConflict`, `CursorRegression`); everything else is a
/// store-level failure carried verbatim.
impl From<ApiError> for DurabilityError {
    fn from(error: ApiError) -> Self {
        match error {
            ApiError::SequenceConflict(conflict) => conflict.into(),
            ApiError::CasMismatch(mismatch) => mismatch.into(),
            other @ (ApiError::CorruptEvent(_)
            | ApiError::Storage(_)
            | ApiError::HistoryCompacted(_)) => Self::StoreError(other),
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod ephemeral_lifecycle_tests {
    //! D3 §9 lifecycle gate. Each test names the gate it pins; all are permanent
    //! rule-1 assertions that the ephemeral store's directory has an enforced
    //! owner across every teardown path.

    use std::path::PathBuf;
    use std::sync::Arc;

    use super::super::bridge::block_on;
    use super::{DurableStore, open_ephemeral, open_ephemeral_in};

    const TEST_SHARD_COUNT: usize = 2;

    /// Materialises shard directories and fds so the drop path actually has a
    /// live database to close before the guard removes the directory.
    fn write_one_event(store: &dyn DurableStore) {
        block_on(store.append("lifecycle/probe", b"payload".to_vec(), 0))
            .expect("bridge completes synchronously")
            .expect("append to a fresh ephemeral stream succeeds");
        block_on(store.flush())
            .expect("bridge completes synchronously")
            .expect("flush of a live ephemeral store succeeds");
    }

    /// §9 gate — normal drop: the directory is removed once the last (here, only)
    /// handle drops.
    #[test]
    fn ephemeral_dir_removed_after_last_handle_drops() {
        let store = open_ephemeral(TEST_SHARD_COUNT).expect("ephemeral open succeeds");
        let dir = store
            .ephemeral_dir_path()
            .expect("ephemeral store carries a guard dir")
            .to_path_buf();
        assert!(
            dir.exists(),
            "the guard directory exists while the store is live"
        );

        write_one_event(&store);
        drop(store);

        assert!(
            !dir.exists(),
            "the guard directory is removed on normal drop"
        );
    }

    /// §9 gate — teardown with store-handle clones alive: the directory survives
    /// until the LAST `Arc<dyn DurableStore>` clone drops, then is removed. This
    /// is the `Arc`-shared-into-channel-handles case: clones share one wrapper,
    /// so none can close the database early.
    #[test]
    fn ephemeral_dir_survives_until_last_store_clone_drops() {
        let store = open_ephemeral(TEST_SHARD_COUNT).expect("ephemeral open succeeds");
        let dir = store
            .ephemeral_dir_path()
            .expect("ephemeral store carries a guard dir")
            .to_path_buf();
        write_one_event(&store);

        let erased: Arc<dyn DurableStore> = Arc::new(store);
        let clone_a = Arc::clone(&erased);
        let clone_b = Arc::clone(&erased);

        drop(erased);
        assert!(
            dir.exists(),
            "directory survives while store clones remain alive"
        );
        drop(clone_a);
        assert!(
            dir.exists(),
            "directory survives while one store clone remains alive"
        );

        drop(clone_b);
        assert!(
            !dir.exists(),
            "the last store clone dropping removes the directory"
        );
    }

    /// §9 gate — startup rollback: an injected haematite open failure (a
    /// conflicting `config.json` pre-seeded into the guard dir) makes the
    /// constructor return `Err` AND leaves zero residue — the guard removes the
    /// directory independently of haematite's own cleanup.
    #[test]
    fn ephemeral_open_failure_rolls_back_directory() {
        let seeded = tempfile::Builder::new()
            .prefix("liminal-durability-test-")
            .tempdir()
            .expect("test can create a temp dir");
        let dir = seeded.path().to_path_buf();
        // A pre-existing `config.json` makes haematite refuse the create with
        // `DataDirAlreadyInitialised`; because the dir pre-existed the create,
        // haematite never removes it — only the guard does.
        std::fs::write(dir.join("config.json"), b"not-a-valid-config")
            .expect("test can seed a conflicting config");

        let result = open_ephemeral_in(seeded, TEST_SHARD_COUNT);

        assert!(result.is_err(), "an injected open failure returns Err");
        assert!(
            !dir.exists(),
            "the guard removes the directory on open failure — zero residue"
        );
    }

    /// §9 gate — repeated start/stop: each cycle owns a distinct directory and
    /// leaves zero residue after it drops.
    #[test]
    fn repeated_ephemeral_cycles_each_own_distinct_dir_zero_residue() {
        let mut seen: Vec<PathBuf> = Vec::new();
        for _ in 0..5 {
            let store = open_ephemeral(TEST_SHARD_COUNT).expect("ephemeral open succeeds");
            let dir = store
                .ephemeral_dir_path()
                .expect("ephemeral store carries a guard dir")
                .to_path_buf();
            assert!(
                dir.exists(),
                "the cycle's directory exists while its store is live"
            );
            assert!(!seen.contains(&dir), "each cycle owns a distinct directory");
            seen.push(dir.clone());

            write_one_event(&store);
            drop(store);
            assert!(
                !dir.exists(),
                "the cycle's directory is removed after its store drops"
            );
        }
    }
}
