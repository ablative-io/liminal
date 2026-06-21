use std::error::Error;
use std::sync::{Arc, Mutex};

use serde_json::json;

use super::*;
use crate::aion::types::Payload;
use crate::channel::{ChannelMode, Schema};

#[test]
fn start_creates_typed_signal_channel_when_handlers_are_declared() -> Result<(), Box<dyn Error>> {
    let context = SignalContext::default();
    let config = workflow_config(
        "prod",
        "wf-123",
        vec![declaration("approve", "application/json")],
    );

    let channel = context
        .start_workflow_signals(config)?
        .ok_or_else(|| std::io::Error::other("expected signal channel"))?;

    assert_eq!(channel.channel_name.as_str(), "aion.signal.prod.wf-123");
    assert_eq!(channel.handle.config().name, "aion.signal.prod.wf-123");
    assert_eq!(channel.mode, ChannelMode::Ephemeral);
    assert!(context.has_signal_channel("prod", "wf-123")?);
    assert_schema_declares(
        channel.handle.config().schema.definition(),
        "approve",
        "application/json",
    );
    Ok(())
}

#[test]
fn start_without_signal_handlers_does_not_create_channel() -> Result<(), Box<dyn Error>> {
    let context = SignalContext::default();
    let config = workflow_config("prod", "wf-no-signals", Vec::new());

    let channel = context.start_workflow_signals(config)?;

    assert!(channel.is_none());
    assert!(!context.has_signal_channel("prod", "wf-no-signals")?);
    Ok(())
}

#[test]
fn restart_reuses_existing_channel_without_duplicate() -> Result<(), Box<dyn Error>> {
    let context = SignalContext::default();
    let declarations = vec![declaration("approve", "application/json")];
    let first = context
        .start_workflow_signals(workflow_config("prod", "wf-restart", declarations.clone()))?
        .ok_or_else(|| std::io::Error::other("expected first channel"))?;
    let first_schema_id = first.handle.current_schema_id()?;

    let second = context
        .start_workflow_signals(workflow_config("prod", "wf-restart", declarations))?
        .ok_or_else(|| std::io::Error::other("expected second channel"))?;

    assert_eq!(second.channel_name, first.channel_name);
    assert_eq!(second.handle.current_schema_id()?, first_schema_id);
    assert!(context.has_signal_channel("prod", "wf-restart")?);
    Ok(())
}

#[test]
fn valid_publish_delivers_original_signal_to_workflow_mailbox() -> Result<(), Box<dyn Error>> {
    let deliverer = Arc::new(TestDeliverer::default());
    let recorder = Arc::new(TestRecorder::default());
    let context = SignalContext::new(deliverer.clone(), recorder);
    let signal = signal("approve", "application/json", b"{\"ok\":true}".to_vec());
    start_approval_workflow(&context, "wf-mailbox", ChannelMode::Ephemeral)?;

    context.publish_signal("prod", "wf-mailbox", signal.clone())?;

    assert_eq!(
        deliverer.deliveries()?,
        vec![(ParticipantPid::new(7), signal)]
    );
    Ok(())
}

#[test]
fn unknown_signal_name_is_rejected_before_channel_publish() -> Result<(), Box<dyn Error>> {
    let deliverer = Arc::new(TestDeliverer::default());
    let context = SignalContext::new(deliverer.clone(), Arc::new(TestRecorder::default()));
    let channel = start_approval_workflow(&context, "wf-unknown", ChannelMode::Ephemeral)?;
    let observer = channel.handle.subscribe()?;

    let result = context.publish_signal(
        "prod",
        "wf-unknown",
        signal("cancel", "application/json", b"{}".to_vec()),
    );

    assert!(matches!(
        result,
        Err(AionSurfaceError::SignalValidationFailed { .. })
    ));
    assert!(observer.try_next()?.is_none());
    assert!(deliverer.deliveries()?.is_empty());
    Ok(())
}

#[test]
fn wrong_content_type_error_includes_expected_and_actual_types() -> Result<(), Box<dyn Error>> {
    let context = SignalContext::default();
    start_approval_workflow(&context, "wf-wrong-type", ChannelMode::Ephemeral)?;

    let error = context
        .publish_signal(
            "prod",
            "wf-wrong-type",
            signal("approve", "text/plain", b"not-json".to_vec()),
        )
        .err()
        .ok_or_else(|| std::io::Error::other("expected validation error"))?;

    match error {
        AionSurfaceError::SignalValidationFailed {
            signal_name,
            message,
            ..
        } => {
            assert_eq!(signal_name, "approve");
            assert!(message.contains("application/json"));
            assert!(message.contains("text/plain"));
        }
        other => return Err(Box::new(other)),
    }
    Ok(())
}

#[test]
fn payload_schema_mismatch_is_rejected_before_delivery() -> Result<(), Box<dyn Error>> {
    let deliverer = Arc::new(TestDeliverer::default());
    let context = SignalContext::new(deliverer.clone(), Arc::new(TestRecorder::default()));
    let declaration = SignalDeclaration::with_payload_schema(
        "approve",
        "application/json",
        Schema::new(json!({
            "type": "object",
            "properties": { "ok": { "type": "boolean" } },
            "required": ["ok"],
            "additionalProperties": false
        }))?,
    );
    let channel = context
        .start_workflow_signals(workflow_config("prod", "wf-schema", vec![declaration]))?
        .ok_or_else(|| std::io::Error::other("expected signal channel"))?;
    let observer = channel.handle.subscribe()?;

    let result = context.publish_signal(
        "prod",
        "wf-schema",
        signal("approve", "application/json", b"{\"ok\":\"yes\"}".to_vec()),
    );

    assert!(matches!(
        result,
        Err(AionSurfaceError::SignalValidationFailed { .. })
    ));
    assert!(observer.try_next()?.is_none());
    assert!(deliverer.deliveries()?.is_empty());
    Ok(())
}

#[test]
fn terminal_workflow_tears_down_channel_and_rejects_later_publish() -> Result<(), Box<dyn Error>> {
    for (workflow_id, status) in [
        ("wf-completed", WorkflowTerminalStatus::Completed),
        ("wf-failed", WorkflowTerminalStatus::Failed),
        ("wf-cancelled", WorkflowTerminalStatus::Cancelled),
        ("wf-timeout", WorkflowTerminalStatus::TimedOut),
    ] {
        let context = SignalContext::default();
        start_approval_workflow(&context, workflow_id, ChannelMode::Ephemeral)?;

        context.complete_workflow_signals("prod", workflow_id, status)?;
        let result = context.publish_signal(
            "prod",
            workflow_id,
            signal("approve", "application/json", b"{}".to_vec()),
        );

        assert!(!context.has_signal_channel("prod", workflow_id)?);
        assert_terminated_delivery_error(result, workflow_id);
    }
    Ok(())
}

#[test]
fn durable_signal_channel_records_delivery_events() -> Result<(), Box<dyn Error>> {
    let recorder = Arc::new(TestRecorder::default());
    let context = SignalContext::new(Arc::new(TestDeliverer::default()), recorder.clone());
    start_approval_workflow(&context, "wf-durable", ChannelMode::Durable)?;
    let delivered = signal("approve", "application/json", b"{}".to_vec());

    context.publish_signal("prod", "wf-durable", delivered.clone())?;

    assert_eq!(
        recorder.operations()?,
        vec![SignalOperation::delivered(
            &signal_channel("prod", "wf-durable")?,
            "wf-durable",
            &delivered,
            ChannelMode::Durable,
        )]
    );
    Ok(())
}

#[test]
fn replay_returns_recorded_signals_without_live_channel_delivery() -> Result<(), Box<dyn Error>> {
    let deliverer = Arc::new(TestDeliverer::default());
    let recorder = Arc::new(TestRecorder::with_replay(vec![
        RecordedSignalDelivery::new(
            String::from(signal_channel("prod", "wf-replay")?),
            "wf-replay".to_owned(),
            signal("approve", "application/json", b"{}".to_vec()),
        ),
    ]));
    let context = SignalContext::new(deliverer.clone(), recorder);

    let replayed = context.replay_signal_deliveries("prod", "wf-replay")?;

    assert_eq!(replayed.len(), 1);
    assert!(deliverer.deliveries()?.is_empty());
    assert!(!context.has_signal_channel("prod", "wf-replay")?);
    Ok(())
}

#[test]
fn ephemeral_signal_channel_does_not_record_delivery_events() -> Result<(), Box<dyn Error>> {
    let recorder = Arc::new(TestRecorder::default());
    let context = SignalContext::new(Arc::new(TestDeliverer::default()), recorder.clone());
    start_approval_workflow(&context, "wf-ephemeral", ChannelMode::Ephemeral)?;

    context.publish_signal(
        "prod",
        "wf-ephemeral",
        signal("approve", "application/json", b"{}".to_vec()),
    )?;

    assert!(recorder.operations()?.is_empty());
    Ok(())
}

#[test]
fn durable_mode_is_configured_per_workflow() -> Result<(), Box<dyn Error>> {
    let recorder = Arc::new(TestRecorder::default());
    let context = SignalContext::new(Arc::new(TestDeliverer::default()), recorder.clone());
    start_approval_workflow(&context, "wf-live", ChannelMode::Ephemeral)?;
    start_approval_workflow(&context, "wf-log", ChannelMode::Durable)?;

    context.publish_signal(
        "prod",
        "wf-live",
        signal("approve", "application/json", b"{}".to_vec()),
    )?;
    context.publish_signal(
        "prod",
        "wf-log",
        signal("approve", "application/json", b"{}".to_vec()),
    )?;

    let operations = recorder.operations()?;
    assert_eq!(operations.len(), 1);
    assert_eq!(operations[0].workflow_id, "wf-log");
    Ok(())
}

#[derive(Debug, Default)]
struct TestDeliverer {
    delivered: Mutex<Vec<(ParticipantPid, SignalPayload)>>,
}

impl TestDeliverer {
    fn deliveries(&self) -> Result<Vec<(ParticipantPid, SignalPayload)>, AionSurfaceError> {
        self.delivered.lock().map_or_else(
            |error| Err(test_delivery_error(error)),
            |deliveries| Ok(deliveries.clone()),
        )
    }
}

impl SignalDeliverer for TestDeliverer {
    fn deliver(
        &self,
        workflow_pid: ParticipantPid,
        signal: SignalPayload,
    ) -> Result<(), AionSurfaceError> {
        self.delivered.lock().map_or_else(
            |error| Err(test_delivery_error(error)),
            |mut deliveries| {
                deliveries.push((workflow_pid, signal));
                Ok(())
            },
        )
    }
}

#[derive(Debug, Default)]
struct TestRecorder {
    operations: Mutex<Vec<SignalOperation>>,
    replay: Mutex<Vec<RecordedSignalDelivery>>,
}

impl TestRecorder {
    fn with_replay(replay: Vec<RecordedSignalDelivery>) -> Self {
        Self {
            operations: Mutex::new(Vec::new()),
            replay: Mutex::new(replay),
        }
    }

    fn operations(&self) -> Result<Vec<SignalOperation>, AionSurfaceError> {
        self.operations.lock().map_or_else(
            |error| Err(test_delivery_error(error)),
            |operations| Ok(operations.clone()),
        )
    }
}

impl SignalRecorder for TestRecorder {
    fn replay_deliveries(
        &self,
        channel_name: &str,
        workflow_id: &str,
    ) -> Result<Vec<RecordedSignalDelivery>, AionSurfaceError> {
        let _ = (channel_name, workflow_id);
        self.replay.lock().map_or_else(
            |error| Err(test_delivery_error(error)),
            |replay| Ok(replay.clone()),
        )
    }

    fn record(&self, operation: SignalOperation) -> Result<(), AionSurfaceError> {
        self.operations.lock().map_or_else(
            |error| Err(test_delivery_error(error)),
            |mut operations| {
                operations.push(operation);
                Ok(())
            },
        )
    }
}

fn workflow_config(
    namespace: &str,
    workflow_id: &str,
    declarations: Vec<SignalDeclaration>,
) -> SignalWorkflowConfig {
    SignalWorkflowConfig::new(namespace, workflow_id, ParticipantPid::new(7), declarations)
}

fn start_approval_workflow(
    context: &SignalContext,
    workflow_id: &str,
    mode: ChannelMode,
) -> Result<SignalChannel, AionSurfaceError> {
    context
        .start_workflow_signals(
            workflow_config(
                "prod",
                workflow_id,
                vec![declaration("approve", "application/json")],
            )
            .with_mode(mode),
        )?
        .ok_or_else(|| AionSurfaceError::ChannelLifecycleError {
            channel_name: signal_channel("prod", workflow_id)
                .map_or_else(|_| workflow_id.to_owned(), String::from),
            message: "expected signal channel".to_owned(),
        })
}

fn declaration(name: &str, content_type: &str) -> SignalDeclaration {
    SignalDeclaration::new(name, content_type)
}

fn signal(name: &str, content_type: &str, data: Vec<u8>) -> SignalPayload {
    SignalPayload {
        signal_name: name.to_owned(),
        payload: Payload {
            data,
            content_type: content_type.to_owned(),
        },
    }
}

fn assert_schema_declares(schema: &serde_json::Value, signal_name: &str, content_type: &str) {
    let schema_text = schema.to_string();
    assert!(schema_text.contains(signal_name));
    assert!(schema_text.contains(content_type));
}

fn assert_terminated_delivery_error(result: Result<(), AionSurfaceError>, workflow_id: &str) {
    match result {
        Err(AionSurfaceError::SignalDeliveryFailed {
            workflow_id: id,
            message,
            ..
        }) => {
            assert_eq!(id, workflow_id);
            assert!(message.contains("terminated"));
        }
        other => assert!(
            matches!(other, Err(AionSurfaceError::SignalDeliveryFailed { .. })),
            "expected terminated delivery failure"
        ),
    }
}

fn test_delivery_error(error: impl std::fmt::Display) -> AionSurfaceError {
    AionSurfaceError::SignalDeliveryFailed {
        channel_name: "test".to_owned(),
        workflow_id: "test".to_owned(),
        signal_name: "test".to_owned(),
        message: error.to_string(),
    }
}
