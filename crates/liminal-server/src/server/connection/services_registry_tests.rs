//! The three pinned tests for runtime channel registration.
//!
//! A child module of `services` on purpose: test 2 has to hold the exact
//! `Arc<ConfiguredChannel>` the admission funnel handed out, which is the only
//! way to pin the quiesce race at its linearisation point rather than by racing
//! threads and a sleep.

use std::error::Error;
use std::sync::Arc;

use haematite::{Database, DatabaseConfig, EventStore};
use liminal::channel::{ChannelMode, InboxInstall};
use liminal::durability::bridge::block_on;
use liminal::durability::{
    DurableStore, HaematiteStore, MessageEnvelope as DurableEnvelope, StoredEntry,
};
use liminal::protocol::{CausalContext, MessageEnvelope, SchemaId};
use liminal_protocol::reason_code::{CHANNEL_NOT_REGISTERED_CODE, CHANNEL_QUIESCED_CODE};
use tempfile::TempDir;

use super::super::channel_registry::{
    ChannelAccessError, ChannelOrigin, ChannelRegistration, ChannelRegistryError, ChannelState,
    ChannelStatus, Registered,
};
use super::{ConnectionServices, LiminalConnectionServices};
use crate::config::types::{ChannelDef, LimitsConfig, ServerConfig};

// ---------------------------------------------------------------------------
// Test 1 — the restart contract
// ---------------------------------------------------------------------------

/// A runtime-registered channel does NOT survive a process restart: nothing
/// persists the roster, which is rebuilt from `config.channels` alone.
///
/// The positive control is what makes this a claim about the restart CONTRACT
/// rather than about a broken rebuild. Without it, every assertion below is
/// equally consistent with "the rebuilt services are simply broken": the
/// re-registration proves the rebuilt roster still works, and the resumed
/// durable log proves the half that survives really does survive — a reader who
/// takes "gone after restart" to mean "the data is gone" would double-write.
#[test]
fn runtime_registered_channels_are_absent_after_restart() -> Result<(), Box<dyn Error>> {
    let (store, _dir) = disk_store()?;
    // `limits.max_channels` is part of the fixture, not scenery: without it
    // every `register_channel` below refuses `CapNotConfigured`.
    let config = config_with(vec![boot_channel("boot", false)], Some(4));

    let first_payload = br#"{"order":1}"#.to_vec();
    {
        let services =
            LiminalConnectionServices::from_config_with_store(&config, Arc::clone(&store))?;
        assert_eq!(
            services.register_channel(&registration("runtime", true))?,
            Registered::Created
        );

        services.publish("boot", &envelope(br#"{"boot":1}"#.to_vec()), None)?;
        services.publish("runtime", &envelope(first_payload.clone()), None)?;

        // The durable flush a graceful shutdown runs. It is explicit here
        // because this type has no `Drop` impl — dropping the services alone
        // flushes nothing.
        services.flush_durable_state()?;
    }

    // The restart: the SAME config and the SAME store, a fresh roster.
    let restarted = LiminalConnectionServices::from_config_with_store(&config, Arc::clone(&store))?;

    let boot_status = restarted.channel_status("boot")?;
    assert!(
        matches!(
            boot_status,
            ChannelStatus::Active {
                origin: ChannelOrigin::BootConfigured,
                mode: ChannelMode::Ephemeral,
                ..
            }
        ),
        "the boot channel is rebuilt from the config file and keeps its origin, got {boot_status:?}"
    );
    assert_eq!(
        restarted.channel_status("runtime")?,
        ChannelStatus::NotRegistered,
        "a runtime-registered channel is simply absent after a restart"
    );

    // The refusal is typed at the admission funnel and carries the reserved
    // roster code — the wire leg that renders it is a later step's claim, not
    // this test's.
    let refusal = restarted
        .admit_channel("runtime")
        .err()
        .ok_or("a publish to an absent channel must be refused")?;
    assert!(
        matches!(&refusal, ChannelAccessError::NotRegistered { name } if name == "runtime"),
        "got {refusal:?}"
    );
    assert_eq!(refusal.reason_code(), CHANNEL_NOT_REGISTERED_CODE);
    assert!(
        restarted
            .publish("runtime", &envelope(br#"{"order":2}"#.to_vec()), None)
            .is_err(),
        "the ordinary publish path refuses the absent name too"
    );

    // --- Positive control -------------------------------------------------
    assert_eq!(
        restarted.register_channel(&registration("runtime", true))?,
        Registered::Created,
        "re-registering after a restart CREATES: the roster really was empty of it"
    );
    let second_payload = br#"{"order":3}"#.to_vec();
    restarted.publish("runtime", &envelope(second_payload.clone()), None)?;
    restarted.flush_durable_state()?;

    // The roster is ephemeral; the LOG is not. The re-registered channel
    // resumed its stream where it left off instead of colliding at sequence
    // zero, so both messages are present and in order.
    assert_eq!(
        read_payloads(store.as_ref(), "runtime:0")?,
        vec![first_payload, second_payload],
        "a re-registered durable channel resumes its log rather than restarting it"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Test 2 — the quiesce race
// ---------------------------------------------------------------------------

/// A subscribe that has already READ the entry as active is ADMITTED, even when
/// a quiesce commits before it completes.
///
/// The interleave is pinned at the linearisation point, not by racing threads: a
/// thread pair and a sleep can pass by luck and would prove nothing about WHICH
/// point is the decision. Holding the admitted entry across the quiesce
/// exercises the decision directly.
#[test]
fn quiesce_admits_a_subscribe_that_read_active() -> Result<(), Box<dyn Error>> {
    let (store, _dir) = disk_store()?;
    let services =
        LiminalConnectionServices::from_config_with_store(&config_with(Vec::new(), Some(4)), store)?;
    assert_eq!(
        services.register_channel(&registration("orders", false))?,
        Registered::Created
    );

    // 1. The "read Active" step, made explicit: the funnel's decision, and the
    //    entry it decided on, held across everything below.
    let admitted = services
        .admit_channel("orders")
        .map_err(|error| format!("an active channel must admit: {error}"))?;

    // 2. The quiesce commits while that admission is in flight.
    services.quiesce_channel("orders", "archived")?;
    assert_eq!(
        services.channel_status("orders")?,
        ChannelStatus::Quiesced {
            reason: "archived".to_owned(),
            origin: ChannelOrigin::RuntimeRegistered,
            mode: ChannelMode::Ephemeral,
        }
    );

    // 3. The subscribe completes from the held entry, and SUCCEEDS.
    let subscription = admitted.handle.subscribe_with_install(InboxInstall {
        budget: liminal::channel::ConnectionInboxBudget::new(1024 * 1024),
        depth_cap: 64,
        notifier: None,
    })?;

    // 4. The stream is LIVE, not merely a non-error: a publish through the held
    //    handle reaches it. "Existing subscribers keep their stream" is a
    //    delivery claim, so it is checked as one.
    let payload = br#"{"order":7}"#.to_vec();
    admitted.handle.publish_with_delivery(
        &payload,
        liminal::envelope::PublisherId::default(),
        None,
    )?;
    let delivered = subscription
        .try_next()?
        .ok_or("an admitted subscriber must still receive deliveries after the quiesce")?;
    assert_eq!(delivered.payload, payload);

    // 5. Both directions. Without this arm, step 3 passing would be equally
    //    consistent with quiesce doing nothing at all.
    let refusal = services
        .admit_channel("orders")
        .err()
        .ok_or("a subscribe admitted AFTER the quiesce must be refused")?;
    assert!(
        matches!(
            &refusal,
            ChannelAccessError::Quiesced { name, reason } if name == "orders" && reason == "archived"
        ),
        "got {refusal:?}"
    );
    assert_eq!(refusal.reason_code(), CHANNEL_QUIESCED_CODE);

    let publish_error = services
        .publish("orders", &envelope(br#"{"order":8}"#.to_vec()), None)
        .err()
        .ok_or("the ordinary publish path must refuse a quiesced channel")?;
    assert_eq!(
        publish_error.to_string(),
        "listener accept failed: channel 'orders' is quiesced: archived"
    );
    assert!(
        services.subscribe("orders", &[], None).is_err(),
        "the ordinary subscribe path must refuse a quiesced channel too"
    );

    // The actor was never touched by the quiesce: it is still the same running
    // channel, which is why the held subscription kept delivering above.
    assert!(
        admitted.handle.is_actor_spawned(),
        "quiesce is a roster-level admission decision, not an actor command"
    );

    // Re-quiescing is idempotent under the IDENTICAL reason and refuses a
    // different one, carrying the reason already on record.
    services.quiesce_channel("orders", "archived")?;
    let second = services
        .quiesce_channel("orders", "decommissioned")
        .err()
        .ok_or("a second, different quiesce reason must refuse")?;
    assert!(
        matches!(
            &second,
            ChannelRegistryError::AlreadyQuiesced { name, reason }
                if name == "orders" && reason == "archived"
        ),
        "got {second:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Test 3 — idle cost, keepalive-honest
// ---------------------------------------------------------------------------

/// Registering a channel spawns NOTHING. Sixteen registered, untouched channels
/// own no actor, while a seventeenth channel that is actually used moves every
/// counter the harness is watching.
///
/// Both arms are one instrument. A flat arm with no grow arm cannot distinguish
/// "the idle channels spawned nothing" from "the harness measured nothing at
/// all", so the grow arm is not decoration.
///
/// The flat arm carries TWO witnesses. `is_actor_spawned` answers `false` for an
/// attempted-and-failed spawn as well as for an untouched channel, so on its own
/// it cannot catch a registration that ATTEMPTED to spawn; the supervisor's own
/// process table can, and it is checked against a baseline taken before the
/// sixteen registrations. That census is itself positively controlled below: it
/// has to MOVE when something really is spawned, or its flatness proves nothing.
///
/// The claim this test does not make: it bounds spawn BY REGISTRATION. It is not
/// a census of scheduler occupancy for a channel that has been used.
#[test]
fn registered_idle_channels_spawn_no_actor() -> Result<(), Box<dyn Error>> {
    const IDLE_CHANNELS: usize = 16;
    const CONTROL_PUBLISHES: u64 = 4;

    let (store, _dir) = disk_store()?;
    // Seventeen: the sixteen idle channels plus the control channel, all
    // runtime-registered and so all counted against the cap.
    let services = LiminalConnectionServices::from_config_with_store(
        &config_with(Vec::new(), Some(IDLE_CHANNELS + 1)),
        store,
    )?;

    let scheduler = services.channel_cluster().supervisor().scheduler();
    let baseline_processes = scheduler.process_table().len();

    let idle: Vec<String> = (0..IDLE_CHANNELS)
        .map(|index| format!("idle-{index}"))
        .collect();
    for name in &idle {
        assert_eq!(
            services.register_channel(&registration(name, false))?,
            Registered::Created
        );
    }

    // The independent witness, taken before anything is USED: sixteen
    // registrations added no process to the supervisor's scheduler.
    assert_eq!(
        scheduler.process_table().len(),
        baseline_processes,
        "{IDLE_CHANNELS} registrations must add no process to the shared scheduler"
    );

    // --- The grow arm -----------------------------------------------------
    crate::metrics::init();
    let publishes_before = crate::metrics::publishes_total_value()
        .ok_or("the publish counter must be readable once metrics are initialized")?;

    assert_eq!(
        services.register_channel(&registration("control", false))?,
        Registered::Created
    );
    let subscription = services.subscribe_handle_for_test("control")?;
    for index in 0..CONTROL_PUBLISHES {
        let outcome = services.publish("control", &envelope(control_payload(index)), None)?;
        assert!(
            outcome.delivered,
            "publish {index} to the control channel must reach its subscriber"
        );
    }
    let mut received = Vec::new();
    while let Some(delivered) = subscription.try_next()? {
        received.push(delivered.payload);
    }
    let expected: Vec<Vec<u8>> = (0..CONTROL_PUBLISHES).map(control_payload).collect();
    assert_eq!(
        received, expected,
        "the control subscriber must receive every control publish"
    );

    let publishes_after = crate::metrics::publishes_total_value()
        .ok_or("the publish counter must still be readable")?;
    // AT LEAST, not EXACTLY: this counter is process-global and shared with
    // every other test in this binary, which cargo runs concurrently. An
    // equality here would be a false red the moment a sibling test publishes.
    assert!(
        publishes_after >= publishes_before + CONTROL_PUBLISHES,
        "the process-wide publish counter must advance by at least {CONTROL_PUBLISHES}: \
         {publishes_before} -> {publishes_after}"
    );

    // The census instrument's own positive control: a channel that is really
    // used DOES move the process table, so the flat reading above is a
    // measurement rather than a broken gauge.
    assert!(
        scheduler.process_table().len() > baseline_processes,
        "using a channel must spawn on the same scheduler the flat arm measured"
    );

    // --- The flat arm -----------------------------------------------------
    for name in &idle {
        let entry = services
            .admit_channel(name)
            .map_err(|error| format!("idle channel '{name}' must still be admitted: {error}"))?;
        assert!(
            !entry.handle.is_actor_spawned(),
            "registered-but-untouched channel '{name}' must own no actor"
        );
    }

    // The enumerator sees all seventeen, every one of them runtime-registered
    // and active: a by-name probe could confirm the names it expected, but only
    // a census can show there is nothing else on the roster.
    let descriptors = services.registered_channels()?;
    assert_eq!(descriptors.len(), IDLE_CHANNELS + 1);
    for descriptor in &descriptors {
        assert_eq!(descriptor.origin, ChannelOrigin::RuntimeRegistered);
        assert_eq!(descriptor.state, ChannelState::Active);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The surface the three tests lean on, checked directly
// ---------------------------------------------------------------------------

/// Registration is idempotent when all three compared fields match, and refuses
/// naming the FIRST field that differs.
#[test]
fn registration_is_idempotent_only_for_an_identical_configuration()
-> Result<(), Box<dyn Error>> {
    let (store, _dir) = disk_store()?;
    let services =
        LiminalConnectionServices::from_config_with_store(&config_with(Vec::new(), Some(4)), store)?;

    let spec = ChannelRegistration {
        name: "orders".to_owned(),
        schema_bytes: Some(br#"{"type":"object"}"#.to_vec()),
        durable: false,
    };
    assert_eq!(services.register_channel(&spec)?, Registered::Created);
    assert_eq!(
        services.register_channel(&spec)?,
        Registered::AlreadyIdentical,
        "the identical spec must be idempotent, not a refusal"
    );

    let differing_mode = ChannelRegistration {
        durable: true,
        ..spec.clone()
    };
    assert_field_conflict(&services, &differing_mode, "mode")?;

    // The SAME document with different bytes: the id derives from the raw bytes,
    // so whitespace changes it — and must, because the id is on the wire.
    let differing_id = ChannelRegistration {
        schema_bytes: Some(br#"{"type": "object"}"#.to_vec()),
        ..spec.clone()
    };
    assert_field_conflict(&services, &differing_id, "schema id")?;

    // A genuinely different document differs at the id first, which is the
    // documented order: the id is the cheaper comparison and the one the wire
    // carries.
    let differing_document = ChannelRegistration {
        schema_bytes: Some(br#"{"type":"array"}"#.to_vec()),
        ..spec
    };
    assert_field_conflict(&services, &differing_document, "schema id")?;
    Ok(())
}

/// The origin never flips: an identical runtime registration against a
/// boot-configured entry answers `AlreadyIdentical` and leaves it
/// `BootConfigured`, so it never migrates into the counted population.
#[test]
fn registering_over_a_boot_channel_never_flips_its_origin() -> Result<(), Box<dyn Error>> {
    let (store, _dir) = disk_store()?;
    let services = LiminalConnectionServices::from_config_with_store(
        &config_with(vec![boot_channel("boot", false)], Some(1)),
        store,
    )?;

    assert_eq!(
        services.register_channel(&registration("boot", false))?,
        Registered::AlreadyIdentical
    );
    assert!(
        matches!(
            services.channel_status("boot")?,
            ChannelStatus::Active {
                origin: ChannelOrigin::BootConfigured,
                ..
            }
        ),
        "an identical registration must not re-stamp a boot channel's origin"
    );

    // The cap of 1 is still entirely unspent: the boot entry was never counted.
    assert_eq!(
        services.register_channel(&registration("runtime", false))?,
        Registered::Created
    );
    Ok(())
}

/// The cap refuses BOTH ways it can: undeclared, and reached. It counts
/// runtime-registered entries only.
#[test]
fn the_channel_cap_refuses_undeclared_and_reached() -> Result<(), Box<dyn Error>> {
    let (store, _dir) = disk_store()?;
    let undeclared = LiminalConnectionServices::from_config_with_store(
        &config_with(Vec::new(), None),
        Arc::clone(&store),
    )?;
    let refusal = undeclared
        .register_channel(&registration("orders", false))
        .err()
        .ok_or("registration with no declared cap must refuse")?;
    assert!(
        matches!(
            refusal,
            ChannelRegistryError::CapNotConfigured {
                cap: "limits.max_channels"
            }
        ),
        "got {refusal:?}"
    );
    // Nothing was built: the refusal happens before the roster is touched.
    assert!(undeclared.registered_channels()?.is_empty());

    let bounded =
        LiminalConnectionServices::from_config_with_store(&config_with(Vec::new(), Some(1)), store)?;
    assert_eq!(
        bounded.register_channel(&registration("first", false))?,
        Registered::Created
    );
    let reached = bounded
        .register_channel(&registration("second", false))
        .err()
        .ok_or("registration past the declared cap must refuse")?;
    assert!(
        matches!(
            reached,
            ChannelRegistryError::CapReached {
                cap: "limits.max_channels",
                limit: 1
            }
        ),
        "got {reached:?}"
    );
    Ok(())
}

/// Quiesce and probe of a name that is not on the roster refuse typed, and the
/// probe answers `NotRegistered` rather than erroring.
#[test]
fn quiesce_refuses_an_unregistered_name_and_the_probe_reports_it() -> Result<(), Box<dyn Error>> {
    let (store, _dir) = disk_store()?;
    let services =
        LiminalConnectionServices::from_config_with_store(&config_with(Vec::new(), Some(2)), store)?;

    assert_eq!(
        services.channel_status("absent")?,
        ChannelStatus::NotRegistered
    );
    let refusal = services
        .quiesce_channel("absent", "archived")
        .err()
        .ok_or("quiescing an unregistered name must refuse")?;
    assert!(
        matches!(&refusal, ChannelRegistryError::NotRegistered { name } if name == "absent"),
        "got {refusal:?}"
    );
    Ok(())
}

/// Schema bytes that do not parse, and bytes that parse but do not compile as a
/// JSON Schema, are both refused typed and build nothing.
#[test]
fn a_rejected_schema_registers_nothing() -> Result<(), Box<dyn Error>> {
    let (store, _dir) = disk_store()?;
    let services =
        LiminalConnectionServices::from_config_with_store(&config_with(Vec::new(), Some(4)), store)?;

    let unparseable = ChannelRegistration {
        name: "orders".to_owned(),
        schema_bytes: Some(b"not json at all".to_vec()),
        durable: false,
    };
    let refusal = services
        .register_channel(&unparseable)
        .err()
        .ok_or("schema bytes that are not JSON must refuse")?;
    assert!(
        matches!(&refusal, ChannelRegistryError::SchemaRejected { name, .. } if name == "orders"),
        "got {refusal:?}"
    );
    assert_eq!(
        services.channel_status("orders")?,
        ChannelStatus::NotRegistered,
        "a refused registration must leave the roster untouched"
    );
    Ok(())
}

/// The probe and the enumerator touch no actor. Reading the status of an idle
/// channel repeatedly must not materialise the thing being read about.
#[test]
fn probing_and_enumerating_never_spawn_an_actor() -> Result<(), Box<dyn Error>> {
    let (store, _dir) = disk_store()?;
    let services =
        LiminalConnectionServices::from_config_with_store(&config_with(Vec::new(), Some(2)), store)?;
    let scheduler = services.channel_cluster().supervisor().scheduler();
    let baseline = scheduler.process_table().len();

    services.register_channel(&registration("orders", false))?;
    for _probe in 0..3 {
        let _status = services.channel_status("orders")?;
        let _census = services.registered_channels()?;
    }

    assert_eq!(
        scheduler.process_table().len(),
        baseline,
        "probing and enumerating must add no process to the scheduler"
    );
    let entry = services
        .admit_channel("orders")
        .map_err(|error| format!("the channel must still be admitted: {error}"))?;
    assert!(!entry.handle.is_actor_spawned());
    Ok(())
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn assert_field_conflict(
    services: &LiminalConnectionServices,
    spec: &ChannelRegistration,
    expected_field: &str,
) -> Result<(), Box<dyn Error>> {
    let refusal = services
        .register_channel(spec)
        .err()
        .ok_or("a differing configuration must refuse")?;
    assert_eq!(
        refusal.to_string(),
        format!("channel 'orders' is already registered with a different {expected_field}")
    );
    Ok(())
}

fn registration(name: &str, durable: bool) -> ChannelRegistration {
    ChannelRegistration {
        name: name.to_owned(),
        schema_bytes: None,
        durable,
    }
}

fn boot_channel(name: &str, durable: bool) -> ChannelDef {
    ChannelDef {
        name: name.to_owned(),
        schema_ref: None,
        durable,
        loaded_schema: None,
    }
}

fn config_with(channels: Vec<ChannelDef>, max_channels: Option<usize>) -> ServerConfig {
    ServerConfig {
        listen_address: local_address(),
        health_listen_address: local_address(),
        drain_timeout_ms: 30_000,
        channels,
        routing_rules: Vec::new(),
        persistence_path: None,
        cluster: None,
        auth: None,
        services: crate::config::types::ServicesConfig::default(),
        limits: LimitsConfig {
            max_channels,
            ..LimitsConfig::default()
        },
        participant: None,
        websocket: None,
    }
}

/// `127.0.0.1:0` without a fallible parse: neither listener is ever bound by
/// these tests, and a hand-built address keeps the fixture infallible.
fn local_address() -> std::net::SocketAddr {
    std::net::SocketAddr::from(([127, 0, 0, 1], 0))
}

fn control_payload(index: u64) -> Vec<u8> {
    format!(r#"{{"control":{index}}}"#).into_bytes()
}

fn envelope(payload: Vec<u8>) -> MessageEnvelope {
    MessageEnvelope::new(
        SchemaId::new([0_u8; SchemaId::WIRE_LEN]),
        CausalContext::independent(),
        payload,
    )
}

/// An on-disk haematite store in a fresh tempdir, with the `TempDir` guard the
/// caller must outlive the store with.
fn disk_store() -> Result<(Arc<dyn DurableStore>, TempDir), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let database = Database::create(DatabaseConfig {
        data_dir: dir.path().join("db"),
        shard_count: 4,
        distributed: None,
        executor_threads: None,
    })?;
    let store: Arc<dyn DurableStore> =
        Arc::new(HaematiteStore::new(Arc::new(EventStore::new(database))));
    Ok((store, dir))
}

fn read_payloads(store: &dyn DurableStore, stream_key: &str) -> Result<Vec<Vec<u8>>, Box<dyn Error>> {
    let entries: Vec<StoredEntry> = block_on(store.read_from(stream_key, 0, 1024))??;
    let mut payloads = Vec::with_capacity(entries.len());
    for entry in entries {
        payloads.push(DurableEnvelope::deserialize(&entry.payload)?.payload);
    }
    Ok(payloads)
}
