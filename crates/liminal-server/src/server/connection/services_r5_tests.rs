use std::sync::Arc;

use haematite::EventStore;
use liminal::durability::bridge::block_on;
use liminal::durability::{
    DurableStore, HaematiteStore, MessageEnvelope as DurableEnvelope, StoredEntry,
};
use liminal::protocol::{CausalContext, MessageEnvelope, SchemaId};

use super::services::{ConnectionServices, LiminalConnectionServices};
use crate::config::types::{ChannelDef, ServerConfig};

// The durable runtime channel maps onto a single partition, so its store stream
// key is "<channel>:0" (see `DurableChannel::stream_key_for`).
const ORDERS_STREAM_KEY: &str = "orders:0";

#[test]
fn shutdown_flush_persists_durable_channel_state_to_store() -> Result<(), Box<dyn std::error::Error>>
{
    let store: Arc<dyn DurableStore> = Arc::new(HaematiteStore::new(Arc::new(EventStore::new())));
    let services =
        LiminalConnectionServices::from_config_with_store(&durable_orders_config()?, store)?;

    // Publish through the same path a connection process uses. Payloads are JSON
    // (the channel schema validates them as JSON).
    let first = br#"{"order":1}"#.to_vec();
    let second = br#"{"order":2}"#.to_vec();
    services.publish("orders", &order_envelope(first.clone()))?;
    services.publish("orders", &order_envelope(second.clone()))?;

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
    let store: Arc<dyn DurableStore> = Arc::new(HaematiteStore::new(Arc::new(EventStore::new())));

    // First "process lifetime": publish + shutdown flush.
    {
        let services = LiminalConnectionServices::from_config_with_store(
            &durable_orders_config()?,
            Arc::clone(&store),
        )?;
        services.publish("orders", &order_envelope(br#"{"order":7}"#.to_vec()))?;
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
