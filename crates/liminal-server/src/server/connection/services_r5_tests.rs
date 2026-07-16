use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use haematite::{Database, DatabaseConfig, EventStore};
use liminal::durability::bridge::block_on;
use liminal::durability::{
    DurabilityError, DurableStore, HaematiteStore, MessageEnvelope as DurableEnvelope, StoredEntry,
};
use liminal::protocol::{CausalContext, MessageEnvelope, SchemaId};
use tempfile::TempDir;

use super::services::{ConnectionServices, LiminalConnectionServices};
use crate::config::types::{ChannelDef, ServerConfig};

/// Builds an on-disk haematite store in a fresh tempdir, returning both the
/// store and the `TempDir` guard (which must outlive the store).
fn disk_store() -> Result<(Arc<dyn DurableStore>, TempDir), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let database = Database::create(DatabaseConfig {
        data_dir: dir.path().join("db"),
        shard_count: 4,
        sweep_interval: None,
        distributed: None,
    })?;
    let store: Arc<dyn DurableStore> =
        Arc::new(HaematiteStore::new(Arc::new(EventStore::new(database))));
    Ok((store, dir))
}

// The durable runtime channel maps onto a single partition, so its store stream
// key is "<channel>:0" (see `DurableChannel::stream_key_for`).
const ORDERS_STREAM_KEY: &str = "orders:0";

#[test]
fn shutdown_flush_persists_durable_channel_state_to_store() -> Result<(), Box<dyn std::error::Error>>
{
    let (store, _dir) = disk_store()?;
    let services =
        LiminalConnectionServices::from_config_with_store(&durable_orders_config()?, store)?;

    // Publish through the same path a connection process uses. Payloads are JSON
    // (the channel schema validates them as JSON).
    let first = br#"{"order":1}"#.to_vec();
    let second = br#"{"order":2}"#.to_vec();
    services.publish("orders", &order_envelope(first.clone()), None)?;
    services.publish("orders", &order_envelope(second.clone()), None)?;

    // Run the graceful-shutdown durable flush.
    services.flush_durable_state()?;

    // Read the state back out of the store the durable channel wrote to. If
    // `Channel::flush`/`publish` were the old `drop(lock)` no-op, the stream
    // would be empty and these assertions would fail.
    let persisted = read_payloads(services.durable_store().as_ref(), ORDERS_STREAM_KEY)?;
    assert_eq!(persisted, vec![first, second]);

    Ok(())
}

#[test]
fn persisted_durable_state_survives_fresh_services_over_same_store()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, _dir) = disk_store()?;

    // First "process lifetime": publish + shutdown flush.
    {
        let services = LiminalConnectionServices::from_config_with_store(
            &durable_orders_config()?,
            Arc::clone(&store),
        )?;
        services.publish("orders", &order_envelope(br#"{"order":7}"#.to_vec()), None)?;
        services.flush_durable_state()?;
    }

    // Restart-at-the-store-level: a fresh services built over the SAME store data
    // sees the persisted message.
    let restarted =
        LiminalConnectionServices::from_config_with_store(&durable_orders_config()?, store)?;
    let persisted = read_payloads(restarted.durable_store().as_ref(), ORDERS_STREAM_KEY)?;
    assert_eq!(persisted, vec![br#"{"order":7}"#.to_vec()]);

    Ok(())
}

/// H2 (= ledger G1): a durable channel recovers its per-partition next-sequence
/// counter from the store on restart. Build services over a real persistence
/// path, publish a keyed and an unkeyed message, drop the services, then rebuild
/// over the SAME path and publish again. Pre-fix the durable channel starts every
/// partition sequence at zero, so the first post-restart append collides with the
/// occupied stream head and fails with a `SequenceConflict`-derived error; post-fix
/// the sequence derives from the store at construction and the append lands at the
/// tail. The blessed durable read then shows old + new messages in order.
#[test]
fn durable_channel_recovers_sequence_across_restart_over_same_path()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let config = durable_orders_config_at(dir.path())?;

    // First process lifetime: a keyed publish (drives dedup + channel append) and
    // an unkeyed publish (channel append only), flushed and dropped.
    {
        let services = LiminalConnectionServices::from_config(&config)?;
        services.publish(
            "orders",
            &order_envelope(br#"{"order":1}"#.to_vec()),
            Some("k1"),
        )?;
        services.publish("orders", &order_envelope(br#"{"order":2}"#.to_vec()), None)?;
        services.flush_durable_state()?;
    }

    // Restart over the SAME persistence path. The durable channel must derive its
    // next sequence from the store so this publish appends at the tail rather than
    // colliding with the occupied head (the pre-fix SequenceConflict).
    let restarted = LiminalConnectionServices::from_config(&config)?;
    restarted.publish("orders", &order_envelope(br#"{"order":3}"#.to_vec()), None)?;
    restarted.flush_durable_state()?;

    // Fresh durable read through the blessed read path: old + new, in order.
    let persisted = read_payloads(restarted.durable_store().as_ref(), ORDERS_STREAM_KEY)?;
    assert_eq!(
        persisted,
        vec![
            br#"{"order":1}"#.to_vec(),
            br#"{"order":2}"#.to_vec(),
            br#"{"order":3}"#.to_vec(),
        ],
        "durable log must hold the pre-restart messages followed by the post-restart message"
    );

    Ok(())
}

/// 13-L1 load-bearing: a duplicate publish carrying the SAME idempotency key is
/// delivered to a live subscriber EXACTLY ONCE (dedup-on-delivery), while the
/// genuine delivery ack reports `delivered` truthfully on each publish.
///
/// This exercises the real server publish path (the same `services.publish` a
/// connection process drives) with a real channel subscriber, then drains that
/// subscriber's inbox to prove the duplicate never reached it.
#[test]
fn duplicate_idempotency_key_delivers_to_subscriber_exactly_once()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, _dir) = disk_store()?;
    let services =
        LiminalConnectionServices::from_config_with_store(&ephemeral_orders_config()?, store)?;

    // A live subscriber on the channel so a delivery is genuinely observable.
    let subscription = services.subscribe_handle_for_test("orders")?;

    let payload = br#"{"order":1}"#.to_vec();

    // First publish with key "k1": fresh dedup claim, one subscriber => the
    // genuine delivery ack is `true` and the message reaches the subscriber.
    let first = services.publish("orders", &order_envelope(payload.clone()), Some("k1"))?;
    assert!(
        first.delivered,
        "first publish of a fresh key with a subscriber must report a genuine delivery"
    );

    // Duplicate publish with the SAME key "k1": dedup suppresses fan-out, so the
    // ack reports NOT delivered and the subscriber must NOT receive a second copy.
    let duplicate = services.publish("orders", &order_envelope(payload.clone()), Some("k1"))?;
    assert!(
        !duplicate.delivered,
        "a duplicate idempotency key must be suppressed (no second delivery)"
    );

    // A different key "k2" is a distinct message: delivered again.
    let other_payload = br#"{"order":2}"#.to_vec();
    let other = services.publish("orders", &order_envelope(other_payload.clone()), Some("k2"))?;
    assert!(
        other.delivered,
        "a different idempotency key must be delivered"
    );

    // Drain the subscriber inbox: it must hold EXACTLY the two distinct messages
    // (k1 once, k2 once), never the suppressed duplicate.
    let mut received = Vec::new();
    while let Some(envelope) = subscription.try_next()? {
        received.push(envelope.payload);
    }
    assert_eq!(
        received,
        vec![payload, other_payload],
        "subscriber must receive each distinct key once and never the duplicate"
    );

    Ok(())
}

/// 13-L1: with NO live subscriber, a publish succeeds but the genuine delivery
/// ack reports `delivered = false` (accepted by the bus, received by no one).
#[test]
fn publish_without_subscriber_reports_not_delivered() -> Result<(), Box<dyn std::error::Error>> {
    let (store, _dir) = disk_store()?;
    let services =
        LiminalConnectionServices::from_config_with_store(&ephemeral_orders_config()?, store)?;

    let outcome = services.publish("orders", &order_envelope(br#"{"order":9}"#.to_vec()), None)?;
    assert!(
        !outcome.delivered,
        "a publish that reaches no subscriber must report a non-delivery ack"
    );

    Ok(())
}

/// Regression for the "dedup claim leaks `InFlight` forever on publish failure"
/// bug. A failed `publish_with_delivery` must release the dedup claim it took, so
/// a re-publish of the same key is DELIVERED, not permanently suppressed.
///
/// The failure is injected with a store double that rejects appends to the
/// durable channel stream (`orders:0`) while letting dedup-namespace appends
/// through, so the claim succeeds, the durable persist fails, and the release can
/// still write its tombstone. This test MUST fail without the release fix (the
/// re-publish would see `InFlight` and report `delivered = false`).
#[test]
fn publish_failure_releases_claim_so_reclaim_is_delivered() -> Result<(), Box<dyn std::error::Error>>
{
    let (inner, _dir) = disk_store()?;
    let failing = Arc::new(FailingAppendStore::new(inner, |stream_key| {
        // Fail the durable-channel append, but never the dedup-namespace append.
        stream_key == ORDERS_STREAM_KEY
    }));
    let store: Arc<dyn DurableStore> = Arc::clone(&failing) as Arc<dyn DurableStore>;
    let services =
        LiminalConnectionServices::from_config_with_store(&durable_orders_config()?, store)?;

    // A live subscriber so a successful delivery is genuinely observable.
    let subscription = services.subscribe_handle_for_test("orders")?;
    let payload = br#"{"order":1}"#.to_vec();

    // First publish with key "k1": the claim is taken, then the durable persist
    // fails -> publish returns Err. Without the fix the claim is left InFlight.
    let first = services.publish("orders", &order_envelope(payload.clone()), Some("k1"));
    assert!(
        first.is_err(),
        "publish must surface the injected durable-append failure"
    );

    // Re-publish the SAME key "k1" after the failure is cleared. With the claim
    // released, this is a fresh claim and is genuinely delivered. Without the
    // release fix this would be suppressed (delivered = false).
    failing.clear_failure();
    let retry = services.publish("orders", &order_envelope(payload.clone()), Some("k1"))?;
    assert!(
        retry.delivered,
        "after a failed publish releases its claim, the re-publish must be delivered, not suppressed"
    );

    // The subscriber receives exactly one copy (the successful retry).
    let mut received = Vec::new();
    while let Some(envelope) = subscription.try_next()? {
        received.push(envelope.payload);
    }
    assert_eq!(
        received,
        vec![payload],
        "the retry delivers exactly once; the failed publish delivered nothing"
    );
    Ok(())
}

/// Best-effort release: when `release_claim` ITSELF errors, `publish` still
/// returns the ORIGINAL publish error (not the release error) and does not panic.
#[test]
fn publish_failure_with_failing_release_returns_original_error()
-> Result<(), Box<dyn std::error::Error>> {
    let (inner, _dir) = disk_store()?;
    // The dedup CLAIM (first dedup-stream append) must succeed so the publish path
    // reaches publish_with_delivery; the channel persist then fails (the ORIGINAL
    // error) and the release tombstone append (second dedup-stream append) also
    // fails (so release_claim ITSELF errors). The decorator fails every append
    // except the very first one per stream, which lets the claim through.
    let store = Arc::new(FailingAppendStore::fail_after_first_per_stream(inner));
    let store: Arc<dyn DurableStore> = store;
    let services =
        LiminalConnectionServices::from_config_with_store(&durable_orders_config()?, store)?;

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        services.publish(
            "orders",
            &order_envelope(br#"{"order":1}"#.to_vec()),
            Some("k1"),
        )
    }));
    let publish_result = result.map_err(|_| "publish panicked under failing release")?;
    let error = publish_result
        .err()
        .ok_or("publish must surface the original failure")?;
    let message = error.to_string();
    assert!(
        message.contains("liminal publish failed"),
        "must return the ORIGINAL publish error, got: {message}"
    );
    Ok(())
}

fn ephemeral_orders_config() -> Result<ServerConfig, Box<dyn std::error::Error>> {
    Ok(ServerConfig {
        listen_address: "127.0.0.1:0".parse()?,
        health_listen_address: "127.0.0.1:0".parse()?,
        drain_timeout_ms: 30_000,
        channels: vec![ChannelDef {
            name: "orders".to_owned(),
            schema_ref: None,
            durable: false,
            loaded_schema: None,
        }],
        routing_rules: Vec::new(),
        persistence_path: None,
        cluster: None,
        auth: None,
        services: crate::config::types::ServicesConfig::default(),
        limits: crate::config::types::LimitsConfig::default(),
        participant: None,
    })
}

fn durable_orders_config() -> Result<ServerConfig, Box<dyn std::error::Error>> {
    Ok(ServerConfig {
        listen_address: "127.0.0.1:0".parse()?,
        health_listen_address: "127.0.0.1:0".parse()?,
        drain_timeout_ms: 30_000,
        channels: vec![ChannelDef {
            name: "orders".to_owned(),
            schema_ref: None,
            durable: true,
            loaded_schema: None,
        }],
        routing_rules: Vec::new(),
        persistence_path: None,
        cluster: None,
        auth: None,
        services: crate::config::types::ServicesConfig::default(),
        limits: crate::config::types::LimitsConfig::default(),
        participant: None,
    })
}

fn durable_orders_config_at(
    persistence_path: &std::path::Path,
) -> Result<ServerConfig, Box<dyn std::error::Error>> {
    Ok(ServerConfig {
        persistence_path: Some(persistence_path.to_path_buf()),
        ..durable_orders_config()?
    })
}

fn order_envelope(payload: Vec<u8>) -> MessageEnvelope {
    MessageEnvelope::new(
        SchemaId::new([0_u8; SchemaId::WIRE_LEN]),
        CausalContext::independent(),
        payload,
    )
}

fn read_payloads(
    store: &dyn DurableStore,
    stream_key: &str,
) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error>> {
    // Outer `?` surfaces a bridge timeout; inner `?` surfaces a store error.
    let entries: Vec<StoredEntry> = block_on(store.read_from(stream_key, 0, 1024))??;
    let mut payloads = Vec::with_capacity(entries.len());
    for entry in entries {
        // The durable channel stores the serialized durability envelope; the
        // original application payload is recovered by deserializing it.
        payloads.push(DurableEnvelope::deserialize(&entry.payload)?.payload);
    }
    Ok(payloads)
}

/// Append-failure strategy for [`FailingAppendStore`].
enum FailMode {
    /// Fail every append whose stream key matches the predicate.
    Predicate(fn(&str) -> bool),
    /// Fail the durable channel append unconditionally, and fail every dedup-stream
    /// append EXCEPT the first one per stream. This lets the dedup CLAIM (first
    /// dedup-stream append) succeed so the publish path reaches the channel persist,
    /// which fails (the ORIGINAL error), and then the dedup RELEASE tombstone
    /// (second dedup-stream append) also fails (so `release_claim` ITSELF errors).
    ChannelAlwaysDedupAfterFirst(Mutex<HashMap<String, u32>>),
}

/// `DurableStore` decorator that injects append failures for testing the publish
/// failure path. While armed, an `append` selected by the [`FailMode`] returns a
/// store error; every other operation delegates to the inner store.
/// [`Self::clear_failure`] disarms the injection so a retry can succeed.
#[derive(Debug)]
struct FailingAppendStore {
    inner: Arc<dyn DurableStore>,
    armed: AtomicBool,
    mode: FailMode,
}

impl std::fmt::Debug for FailMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Predicate(_) => formatter.write_str("Predicate"),
            Self::ChannelAlwaysDedupAfterFirst(_) => {
                formatter.write_str("ChannelAlwaysDedupAfterFirst")
            }
        }
    }
}

impl FailingAppendStore {
    fn new(inner: Arc<dyn DurableStore>, should_fail: fn(&str) -> bool) -> Self {
        Self {
            inner,
            armed: AtomicBool::new(true),
            mode: FailMode::Predicate(should_fail),
        }
    }

    fn fail_after_first_per_stream(inner: Arc<dyn DurableStore>) -> Self {
        Self {
            inner,
            armed: AtomicBool::new(true),
            mode: FailMode::ChannelAlwaysDedupAfterFirst(Mutex::new(HashMap::new())),
        }
    }

    fn clear_failure(&self) {
        self.armed.store(false, Ordering::SeqCst);
    }

    fn should_fail(&self, stream_key: &str) -> Result<bool, DurabilityError> {
        if !self.armed.load(Ordering::SeqCst) {
            return Ok(false);
        }
        match &self.mode {
            FailMode::Predicate(predicate) => Ok(predicate(stream_key)),
            FailMode::ChannelAlwaysDedupAfterFirst(seen) => {
                if stream_key == ORDERS_STREAM_KEY {
                    return Ok(true);
                }
                let mut seen = seen
                    .lock()
                    .map_err(|_| DurabilityError::ConfigError("test lock poisoned".to_owned()))?;
                let count = seen.entry(stream_key.to_owned()).or_insert(0);
                let fail = *count > 0;
                *count += 1;
                drop(seen);
                Ok(fail)
            }
        }
    }
}

#[async_trait::async_trait]
impl DurableStore for FailingAppendStore {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        if self.should_fail(stream_key)? {
            return Err(DurabilityError::ConfigError(format!(
                "injected append failure for stream '{stream_key}'"
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
