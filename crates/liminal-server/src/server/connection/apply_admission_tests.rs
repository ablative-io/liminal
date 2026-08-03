//! The wire leg: the roster's admission decision, observed at the response
//! frame.
//!
//! Every instrument here runs `apply_frame` against a REAL
//! `LiminalConnectionServices` over a real roster. That is the point: steps 1–4
//! proved the funnel refuses TYPED, and this step's whole claim is that the type
//! survives the trip to a frame. A stand-in that answered the admission question
//! itself would be asserting on its own fixture.

use std::error::Error;
use std::sync::{Arc, Mutex};

use haematite::{Database, DatabaseConfig, EventStore};
use liminal::channel::ChannelMode;
use liminal::durability::{DurableStore, HaematiteStore};
use liminal::protocol::{CausalContext, MessageEnvelope, SchemaId};
use liminal_protocol::reason_code::{CHANNEL_NOT_REGISTERED_CODE, CHANNEL_QUIESCED_CODE};
use tempfile::TempDir;

use super::*;
use crate::config::types::{LimitsConfig, ServerConfig};
use crate::server::connection::channel_registry::{
    ChannelAccessError, ChannelOrigin, ChannelRegistration, ChannelStatus,
};
use crate::server::connection::conversation::ConnectionConversation;
use crate::server::connection::services::{
    ConnectionSubscription, LiminalConnectionServices, PublishOutcome,
};

/// Fixed connection pid, as in the sibling `apply_frame` unit tests.
const TEST_PID: u64 = 1;

/// The message an absent channel has ALWAYS produced on the wire, spelled out
/// rather than derived.
///
/// A test that built this string from the same code the server builds it from
/// would pass for any wording, which is the one thing a byte-preservation claim
/// may not do. The full rendering is asserted — `ServerError::ListenerAccept`'s
/// prefix included — because the frame carries the whole string, not the part of
/// it this lane happens to be discussing.
const ABSENT_CHANNEL_MESSAGE: &str = "listener accept failed: channel 'ghost' is not configured";

// ---------------------------------------------------------------------------
// Absent channel — 0x0101 on both frames, and the string is untouched
// ---------------------------------------------------------------------------

/// A publish and a subscribe against a channel that is not on the roster answer
/// with the reserved not-registered code and today's exact bytes.
///
/// Both directions of the semver promise in one test: the CODE moves (from the
/// undifferentiated `0xFFFF` to `0x0101`, which is the whole point of the lane)
/// and the MESSAGE does not move at all (which is what keeps the cut minor). A
/// test that checked only the code would let the string drift; one that checked
/// only the string would pass on the unchanged server.
#[test]
fn an_absent_channel_is_refused_with_the_roster_code_and_the_preserved_string()
-> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(&[])?;

    let (publish_code, publish_message) =
        publish_error(fixture.apply(publish_frame("ghost", br#"{"order":1}"#.to_vec())))?;
    assert_eq!(publish_code, CHANNEL_NOT_REGISTERED_CODE);
    assert_eq!(publish_message, ABSENT_CHANNEL_MESSAGE);

    let (subscribe_code, subscribe_message) =
        subscribe_error(fixture.apply(subscribe_frame("ghost")))?;
    assert_eq!(subscribe_code, CHANNEL_NOT_REGISTERED_CODE);
    assert_eq!(subscribe_message, ABSENT_CHANNEL_MESSAGE);

    // The service path a direct caller reaches renders the SAME bytes. The two
    // renderings share one function on purpose, and this is the assertion that
    // notices if they ever stop sharing it.
    let direct = fixture
        .services
        .publish("ghost", &envelope(br#"{"order":1}"#.to_vec()), None)
        .err()
        .ok_or("a publish to an absent channel must fail at the service too")?;
    assert_eq!(direct.to_string(), ABSENT_CHANNEL_MESSAGE);
    Ok(())
}

// ---------------------------------------------------------------------------
// Quiesced channel — 0x0102, carrying the recorded reason
// ---------------------------------------------------------------------------

/// A publish and a subscribe against a quiesced channel answer with the reserved
/// quiesced code, and the frame carries the reason the operator recorded at the
/// transition.
///
/// The reason is asserted as content, not as presence: a refusal that named the
/// channel but dropped the cause would leave a client knowing it was refused and
/// not why, which is the state the second code exists to end.
#[test]
fn a_quiesced_channel_is_refused_with_its_own_code_and_carries_the_recorded_reason()
-> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(&["orders"])?;
    fixture.services.quiesce_channel("orders", "archived")?;

    let expected = "listener accept failed: channel 'orders' is quiesced: archived";

    let (publish_code, publish_message) =
        publish_error(fixture.apply(publish_frame("orders", br#"{"order":1}"#.to_vec())))?;
    assert_eq!(publish_code, CHANNEL_QUIESCED_CODE);
    assert_eq!(publish_message, expected);

    let (subscribe_code, subscribe_message) =
        subscribe_error(fixture.apply(subscribe_frame("orders")))?;
    assert_eq!(subscribe_code, CHANNEL_QUIESCED_CODE);
    assert_eq!(subscribe_message, expected);
    Ok(())
}

// ---------------------------------------------------------------------------
// The degraded arm — admitted, then refused
// ---------------------------------------------------------------------------

/// An operation that PASSES admission and is then refused by the service carries
/// the undifferentiated code, even when the cause really was the roster.
///
/// This is the attribution window made deterministic rather than argued about.
/// The adapter here is a real `LiminalConnectionServices` wrapped in a decorator
/// that quiesces the channel in the one place a concurrent operator call could
/// have: after the admission decision returned Active, before the delegation.
/// Neither call is simulated — both reach the real roster — so the frame that
/// comes back is the frame a live race produces.
///
/// The message is IDENTICAL to the one the previous test's admission refusal
/// carried, and only the code differs. That equality is the assertion that
/// matters: it proves the code is the sole discriminator, and it proves the
/// degraded path is degraded rather than lying — `0xFFFF` claims nothing about
/// the roster, where a `0x0102` recovered by a second read would have claimed
/// something it could not know.
#[test]
fn an_admitted_operation_the_service_then_refuses_stays_undifferentiated()
-> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(&["orders"])?;
    let racing = Arc::new(QuiesceAfterAdmission {
        inner: Arc::clone(&fixture.services),
        channel: "orders".to_owned(),
        reason: "archived".to_owned(),
        quiesce_failures: Mutex::new(Vec::new()),
    });
    let runtime = ConnectionRuntime::for_tests(Arc::clone(&racing) as Arc<dyn ConnectionServices>);
    let mut state = ConnectionProcessState::default();

    let action = apply_frame(
        TEST_PID,
        &runtime,
        &mut state,
        publish_frame("orders", br#"{"order":1}"#.to_vec()),
    );
    let (code, message) = publish_error(action)?;

    assert_eq!(code, SERVER_ERROR_CODE);
    assert_eq!(
        message,
        "listener accept failed: channel 'orders' is quiesced: archived"
    );
    assert_ne!(code, CHANNEL_QUIESCED_CODE);

    // The fixture's own control. Without it, an interleave that never happened —
    // a quiesce that silently failed, leaving the publish to fail for some other
    // reason — would look exactly like a pass.
    let failures = racing
        .quiesce_failures
        .lock()
        .map_err(|_poisoned| "the racing adapter's failure log is poisoned")?;
    assert!(
        failures.is_empty(),
        "the decorator's quiesce must have succeeded, got {failures:?}"
    );
    drop(failures);
    assert_eq!(
        fixture.services.channel_status("orders")?,
        ChannelStatus::Quiesced {
            reason: "archived".to_owned(),
            origin: ChannelOrigin::RuntimeRegistered,
            mode: ChannelMode::Ephemeral,
        },
        "the channel really is quiesced after the window closed"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Positive control — the consultation admits what it should
// ---------------------------------------------------------------------------

/// A healthy channel publishes and subscribes THROUGH the new consultation.
///
/// Every refusal above is equally consistent with a consultation that refuses
/// everything, so this arm is not decoration: it is the only thing separating
/// "the roster code reaches the wire" from "the wire leg broke publish". The
/// subscribe arm additionally proves the admission sits where it claims to —
/// before the service call and after the §5 cap — because a subscription really
/// is created and recorded on the connection.
#[test]
fn a_healthy_channel_is_admitted_through_the_same_consultation() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(&["orders"])?;

    let action = fixture.apply(publish_frame("orders", br#"{"order":1}"#.to_vec()));
    assert!(
        matches!(
            action,
            FrameAction::Respond(Frame::PublishAck { stream_id: 3, .. })
        ),
        "got {action:?}"
    );

    let mut state = ConnectionProcessState::default();
    let action = apply_frame(
        TEST_PID,
        &fixture.runtime,
        &mut state,
        subscribe_frame("orders"),
    );
    let FrameAction::Respond(Frame::SubscribeAck {
        subscription_id, ..
    }) = action
    else {
        return Err(format!("expected a SubscribeAck, got {action:?}").into());
    };
    assert!(
        state.subscriptions.contains_key(&subscription_id),
        "an admitted subscribe leaves its subscription on the connection"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// A real adapter over a real roster, plus the runtime `apply_frame` needs.
struct Fixture {
    services: Arc<LiminalConnectionServices>,
    runtime: ConnectionRuntime,
    /// Kept alive for the store's lifetime; dropping it removes the database.
    _dir: TempDir,
}

impl Fixture {
    /// Builds the adapter with `registered` runtime-registered ephemeral
    /// channels.
    fn new(registered: &[&str]) -> Result<Self, Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let database = Database::create(DatabaseConfig {
            data_dir: dir.path().join("db"),
            shard_count: 4,
            distributed: None,
            executor_threads: None,
        })?;
        let store: Arc<dyn DurableStore> =
            Arc::new(HaematiteStore::new(Arc::new(EventStore::new(database))));
        let services = Arc::new(LiminalConnectionServices::from_config_with_store(
            &config(),
            store,
        )?);
        for name in registered {
            services.register_channel(&ChannelRegistration {
                name: (*name).to_owned(),
                schema_bytes: None,
                durable: false,
            })?;
        }
        let runtime =
            ConnectionRuntime::for_tests(Arc::clone(&services) as Arc<dyn ConnectionServices>);
        Ok(Self {
            services,
            runtime,
            _dir: dir,
        })
    }

    /// Applies one frame on a fresh connection state.
    fn apply(&self, frame: Frame) -> FrameAction {
        let mut state = ConnectionProcessState::default();
        apply_frame(TEST_PID, &self.runtime, &mut state, frame)
    }
}

/// A real adapter whose ADMISSION decision is followed by a quiesce, in the
/// window the design names and refuses to close.
///
/// Every method delegates to the real adapter. The only addition is the quiesce,
/// placed exactly where a concurrent operator call would land: after admission
/// answered Active and before the service is delegated to.
#[derive(Debug)]
struct QuiesceAfterAdmission {
    inner: Arc<LiminalConnectionServices>,
    channel: String,
    reason: String,
    /// Quiesce failures, so a fixture that did not actually open the window
    /// fails loudly instead of passing for the wrong reason.
    quiesce_failures: Mutex<Vec<String>>,
}

impl ConnectionServices for QuiesceAfterAdmission {
    fn admit_channel(
        &self,
        operation: ChannelOperation,
        channel: &str,
    ) -> Result<(), ChannelAccessError> {
        ConnectionServices::admit_channel(self.inner.as_ref(), operation, channel)?;
        if let Err(error) = self.inner.quiesce_channel(&self.channel, &self.reason) {
            match self.quiesce_failures.lock() {
                Ok(mut failures) => failures.push(error.to_string()),
                Err(poisoned) => drop(poisoned),
            }
        }
        Ok(())
    }

    fn publish(
        &self,
        channel: &str,
        envelope: &MessageEnvelope,
        idempotency_key: Option<&str>,
    ) -> Result<PublishOutcome, ServerError> {
        self.inner.publish(channel, envelope, idempotency_key)
    }

    fn subscribe(
        &self,
        channel: &str,
        accepted_schemas: &[ProtocolSchemaId],
        install: Option<liminal::channel::InboxInstall>,
    ) -> Result<ConnectionSubscription, ServerError> {
        self.inner.subscribe(channel, accepted_schemas, install)
    }

    fn unsubscribe(&self, subscription: ConnectionSubscription) -> Result<(), ServerError> {
        self.inner.unsubscribe(subscription)
    }

    fn open_conversation(
        &self,
        conversation_id: u64,
        subject: &str,
    ) -> Result<ConnectionConversation, ServerError> {
        self.inner.open_conversation(conversation_id, subject)
    }

    fn conversation_message(
        &self,
        conversation: &ConnectionConversation,
        envelope: &MessageEnvelope,
    ) -> Result<(), ServerError> {
        self.inner.conversation_message(conversation, envelope)
    }

    fn close_conversation(&self, conversation: ConnectionConversation) -> Result<(), ServerError> {
        self.inner.close_conversation(conversation)
    }

    fn flush_durable_state(&self) -> Result<(), ServerError> {
        self.inner.flush_durable_state()
    }
}

/// The channel-free config the roster starts from, with a cap declared: without
/// `limits.max_channels` every registration in this file refuses
/// `CapNotConfigured`.
fn config() -> ServerConfig {
    ServerConfig {
        listen_address: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        health_listen_address: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        drain_timeout_ms: 30_000,
        channels: Vec::new(),
        routing_rules: Vec::new(),
        persistence_path: None,
        cluster: None,
        auth: None,
        services: crate::config::types::ServicesConfig::default(),
        limits: LimitsConfig {
            max_channels: Some(4),
            ..LimitsConfig::default()
        },
        participant: None,
        websocket: None,
    }
}

fn envelope(payload: Vec<u8>) -> MessageEnvelope {
    MessageEnvelope::new(
        SchemaId::new([0_u8; SchemaId::WIRE_LEN]),
        CausalContext::independent(),
        payload,
    )
}

fn publish_frame(channel: &str, payload: Vec<u8>) -> Frame {
    Frame::Publish {
        flags: 0,
        stream_id: 3,
        channel: channel.to_owned(),
        envelope: envelope(payload),
        idempotency_key: None,
    }
}

fn subscribe_frame(channel: &str) -> Frame {
    Frame::Subscribe {
        flags: 0,
        stream_id: 5,
        channel: channel.to_owned(),
        accepted_schemas: Vec::new(),
        max_in_flight: 16,
    }
}

fn publish_error(action: FrameAction) -> Result<(u16, String), Box<dyn Error>> {
    let FrameAction::Respond(Frame::PublishError {
        reason_code,
        message,
        stream_id,
        ..
    }) = action
    else {
        return Err(format!("expected a PublishError response, got {action:?}").into());
    };
    // The refusal rides the REQUEST's stream: a client that published on 3 must
    // read its refusal on 3, or the refusal is a silent timeout wearing a frame.
    assert_eq!(stream_id, 3);
    let message = message.ok_or("a refusal frame must carry its message")?;
    Ok((reason_code, message))
}

fn subscribe_error(action: FrameAction) -> Result<(u16, String), Box<dyn Error>> {
    let FrameAction::Respond(Frame::SubscribeError {
        reason_code,
        message,
        stream_id,
        ..
    }) = action
    else {
        return Err(format!("expected a SubscribeError response, got {action:?}").into());
    };
    assert_eq!(stream_id, 5);
    let message = message.ok_or("a refusal frame must carry its message")?;
    Ok((reason_code, message))
}
