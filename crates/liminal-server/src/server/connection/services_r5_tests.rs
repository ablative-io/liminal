use std::sync::Arc;

use haematite::{Database, DatabaseConfig, EventStore};
use liminal::durability::bridge::block_on;
use liminal::durability::{
    DurableStore, HaematiteStore, MessageEnvelope as DurableEnvelope, StoredEntry,
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

fn ephemeral_orders_config() -> Result<ServerConfig, Box<dyn std::error::Error>> {
    Ok(ServerConfig {
        listen_address: "127.0.0.1:0".parse()?,
        health_listen_address: "127.0.0.1:0".parse()?,
        drain_timeout_ms: 30_000,
        channels: vec![ChannelDef {
            name: "orders".to_owned(),
            schema_ref: "schemas/orders.json".to_owned(),
            durable: false,
        }],
        routing_rules: Vec::new(),
        persistence_path: None,
        cluster: None,
    })
}

fn durable_orders_config() -> Result<ServerConfig, Box<dyn std::error::Error>> {
    Ok(ServerConfig {
        listen_address: "127.0.0.1:0".parse()?,
        health_listen_address: "127.0.0.1:0".parse()?,
        drain_timeout_ms: 30_000,
        channels: vec![ChannelDef {
            name: "orders".to_owned(),
            schema_ref: "schemas/orders.json".to_owned(),
            durable: true,
        }],
        routing_rules: Vec::new(),
        persistence_path: None,
        cluster: None,
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
