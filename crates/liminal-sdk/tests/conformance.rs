use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use liminal_sdk::embedded::{EmbeddedChannelBackend, EmbeddedChannelMessage};
use liminal_sdk::{
    ChannelHandle, ConnectionLifecycle, ConnectionState, ConversationEvent, ConversationId,
    DisconnectReason, EmbeddedChannelHandle, EmbeddedConfig, PressureResponse, ReconnectEvent,
    SchemaMetadata, SchemaValidate, SdkError, SubscriptionId, SubscriptionRecovery,
};
use serde::Deserialize;
use serde::Serialize;
use serde_json::{Value, json};

const SCENARIOS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/conformance/scenarios.json"
));

#[derive(Debug, Deserialize)]
struct ScenarioSuite {
    scenarios: Vec<Scenario>,
}

#[derive(Debug, Deserialize)]
struct Scenario {
    name: String,
    expected: Value,
}

#[derive(Debug)]
struct ScriptedPressureBackend {
    responses: Mutex<VecDeque<PressureResponse>>,
}

impl ScriptedPressureBackend {
    fn new(responses: impl Into<VecDeque<PressureResponse>>) -> Self {
        Self {
            responses: Mutex::new(responses.into()),
        }
    }
}

impl EmbeddedChannelBackend for ScriptedPressureBackend {
    fn publish(&self, message: &dyn EmbeddedChannelMessage) -> Result<PressureResponse, SdkError> {
        let schema = message.schema_metadata();
        std::hint::black_box(schema);

        let mut responses = self.responses.lock().map_err(|_error| SdkError::Protocol {
            description: "scripted pressure backend lock was poisoned".to_string(),
        })?;
        responses.pop_front().ok_or_else(|| SdkError::Protocol {
            description: "scripted pressure backend exhausted responses".to_string(),
        })
    }
}

#[derive(Serialize)]
struct ConformanceMessage {
    id: u64,
}

impl SchemaValidate for ConformanceMessage {
    fn schema_metadata() -> SchemaMetadata {
        SchemaMetadata::new(
            "conformance.message",
            "1",
            br#"{"type":"object","required":["id"]}"#.as_slice(),
        )
    }
}

#[test]
fn rust_sdk_conformance_scenarios_match_shared_expectations() -> Result<(), SdkError> {
    let suite = load_scenarios()?;
    let mut results = Vec::with_capacity(suite.scenarios.len());
    let mut all_passed = true;

    for scenario in suite.scenarios {
        let observed = observe_scenario(scenario.name.as_str())?;
        let pass = observed == scenario.expected;
        all_passed &= pass;
        results.push(json!({
            "scenario": scenario.name,
            "pass": pass,
            "expected": scenario.expected,
            "observed": observed,
        }));
    }

    let output = json!({
        "sdk": "rust",
        "results": results,
    });
    let text =
        serde_json::to_string_pretty(&output).map_err(|error| serialization_error(&error))?;
    println!("{text}");
    write_result_if_requested("rust", &text)?;

    assert!(all_passed, "rust SDK conformance scenarios diverged");
    Ok(())
}

fn load_scenarios() -> Result<ScenarioSuite, SdkError> {
    serde_json::from_str(SCENARIOS).map_err(|error| serialization_error(&error))
}

fn observe_scenario(name: &str) -> Result<Value, SdkError> {
    match name {
        "connection.normal_connect" => observe_normal_connect(),
        "connection.reconnect_after_drop" => observe_reconnect_after_drop(),
        "connection.clean_disconnect" => observe_clean_disconnect(),
        "subscription.resume_from_last_acknowledged" => observe_subscription_recovery(),
        "backpressure.publish_variants" => observe_backpressure_variants(),
        "conversation.open_message_close" => Ok(observe_conversation_lifecycle()),
        other => Err(SdkError::Protocol {
            description: format!("unknown conformance scenario {other}"),
        }),
    }
}

fn observe_normal_connect() -> Result<Value, SdkError> {
    let mut lifecycle = lifecycle();
    let mut transitions = vec![state_name(lifecycle.state())];

    lifecycle.connected()?;
    transitions.push(state_name(lifecycle.state()));

    Ok(json!({
        "state_transitions": transitions,
        "final_state": state_name(lifecycle.state()),
    }))
}

fn observe_reconnect_after_drop() -> Result<Value, SdkError> {
    let mut lifecycle = lifecycle();
    let mut transitions = vec![state_name(lifecycle.state())];
    let mut attempts = Vec::new();

    lifecycle.connected()?;
    transitions.push(state_name(lifecycle.state()));

    lifecycle.reconnect(ReconnectEvent::EstablishedConnectionFate)?;
    transitions.push(state_name(lifecycle.state()));
    if let ConnectionState::Reconnecting { attempt } = lifecycle.state() {
        attempts.push(*attempt);
    }

    lifecycle.connected()?;
    transitions.push(state_name(lifecycle.state()));

    Ok(json!({
        "state_transitions": transitions,
        "final_state": state_name(lifecycle.state()),
        "reconnect_attempts": attempts,
    }))
}

fn observe_clean_disconnect() -> Result<Value, SdkError> {
    let mut lifecycle = lifecycle();
    let mut transitions = vec![state_name(lifecycle.state())];

    lifecycle.connected()?;
    transitions.push(state_name(lifecycle.state()));
    lifecycle.disconnect(DisconnectReason::Normal)?;
    transitions.push(state_name(lifecycle.state()));

    Ok(json!({
        "state_transitions": transitions,
        "final_state": state_name(lifecycle.state()),
        "disconnect_reason": disconnect_reason(lifecycle.state()),
    }))
}

fn observe_subscription_recovery() -> Result<Value, SdkError> {
    let mut recovery = SubscriptionRecovery::new();
    let subscription_id = SubscriptionId::new(1);

    recovery.acknowledge(subscription_id, 5);
    let requests = recovery.resume_requests()?;
    let from_sequence = match requests.as_slice() {
        [request] if request.subscription_id == subscription_id => request.from_sequence,
        _ => {
            return Err(SdkError::Store {
                description: "subscription recovery did not produce one resume request".to_string(),
            });
        }
    };

    Ok(json!({
        "subscription": "orders",
        "last_acknowledged_sequence": recovery.last_acknowledged_sequence(subscription_id),
        "from_sequence": from_sequence,
    }))
}

fn observe_backpressure_variants() -> Result<Value, SdkError> {
    let backend = ScriptedPressureBackend::new(VecDeque::from([
        PressureResponse::Accept,
        PressureResponse::Defer {
            delay: Duration::from_millis(250),
        },
        PressureResponse::Reject {
            reason: "consumer overloaded".to_string(),
        },
    ]));
    let config = EmbeddedConfig::new("orders", "conv-1").with_channel_backend(Arc::new(backend));
    let channel = EmbeddedChannelHandle::new(&config);

    let responses = vec![
        normalize_pressure(channel.publish(ConformanceMessage { id: 1 })?)?,
        normalize_pressure(channel.publish(ConformanceMessage { id: 2 })?)?,
        normalize_pressure(channel.publish(ConformanceMessage { id: 3 })?)?,
    ];

    Ok(json!({ "responses": responses }))
}

fn observe_conversation_lifecycle() -> Value {
    let conversation_id = ConversationId::new("conv-1");
    let events = [
        ConversationEvent::Opened {
            conversation_id: conversation_id.clone(),
        },
        ConversationEvent::Message {
            conversation_id: conversation_id.clone(),
        },
        ConversationEvent::Closing {
            conversation_id: conversation_id.clone(),
        },
        ConversationEvent::Closed { conversation_id },
    ];
    let normalized = events
        .iter()
        .map(conversation_event_kind)
        .collect::<Vec<_>>();

    json!({ "events": normalized })
}

const fn lifecycle() -> ConnectionLifecycle {
    ConnectionLifecycle::new()
}

const fn state_name(state: &ConnectionState) -> &'static str {
    match state {
        ConnectionState::Connecting => "connecting",
        ConnectionState::Connected => "connected",
        ConnectionState::Reconnecting { .. } => "reconnecting",
        ConnectionState::Disconnected { .. } => "disconnected",
    }
}

const fn disconnect_reason(state: &ConnectionState) -> &'static str {
    match state {
        ConnectionState::Disconnected {
            reason: DisconnectReason::Normal,
        } => "normal",
        ConnectionState::Disconnected {
            reason: DisconnectReason::Error,
        } => "error",
        ConnectionState::Disconnected {
            reason: DisconnectReason::Timeout,
        } => "timeout",
        _ => "none",
    }
}

fn normalize_pressure(response: PressureResponse) -> Result<Value, SdkError> {
    match response {
        PressureResponse::Accept => Ok(json!({ "kind": "accept" })),
        PressureResponse::Defer { delay } => Ok(json!({
            "kind": "defer",
            "delay": duration_millis(delay)?,
        })),
        PressureResponse::Reject { reason } => Ok(json!({
            "kind": "reject",
            "reason": reason,
        })),
    }
}

fn duration_millis(duration: Duration) -> Result<u64, SdkError> {
    u64::try_from(duration.as_millis()).map_err(|error| SdkError::Protocol {
        description: format!("pressure delay exceeded u64 milliseconds: {error}"),
    })
}

const fn conversation_event_kind(event: &ConversationEvent) -> &'static str {
    match event {
        ConversationEvent::Opened { .. } => "opened",
        ConversationEvent::Message { .. } => "message",
        ConversationEvent::Closing { .. } => "closing",
        ConversationEvent::Closed { .. } => "closed",
        ConversationEvent::Error { .. } => "error",
    }
}

fn write_result_if_requested(sdk: &str, text: &str) -> Result<(), SdkError> {
    let Ok(directory) = std::env::var("CONFORMANCE_RESULTS_DIR") else {
        return Ok(());
    };

    let directory = PathBuf::from(directory);
    std::fs::create_dir_all(&directory).map_err(|error| io_error(&error))?;
    let path = directory.join(format!("{sdk}.json"));
    std::fs::write(path, text).map_err(|error| io_error(&error))
}

fn serialization_error(error: &serde_json::Error) -> SdkError {
    SdkError::Serialization {
        description: error.to_string(),
    }
}

fn io_error(error: &std::io::Error) -> SdkError {
    SdkError::Store {
        description: error.to_string(),
    }
}
