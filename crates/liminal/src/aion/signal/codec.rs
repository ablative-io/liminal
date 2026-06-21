use serde_json::{Value, json};

use super::{
    SignalDeclaration, SignalPayload, SignalSession, delivery_failed, lifecycle_failed,
    validation_failed,
};
use crate::aion::AionSurfaceError;
use crate::aion::channels::ChannelName;
use crate::aion::types::Payload;
use crate::channel::Schema;

pub(super) fn build_signal_schema(
    channel_name: &ChannelName,
    declarations: &[SignalDeclaration],
) -> Result<Schema, AionSurfaceError> {
    let variants: Vec<Value> = declarations.iter().map(signal_variant_schema).collect();
    Schema::new(json!({ "oneOf": variants })).map_err(|error| lifecycle_failed(channel_name, error))
}

pub(super) fn validate_signal(
    session: &SignalSession,
    signal: &SignalPayload,
) -> Result<(), AionSurfaceError> {
    let Some(declaration) = session
        .declarations
        .iter()
        .find(|declaration| declaration.signal_name == signal.signal_name)
    else {
        let expected = expected_types(&session.declarations);
        return Err(validation_failed(
            &session.channel_name,
            session.workflow_id.as_str(),
            signal,
            expected.as_str(),
            "signal name is not declared",
        ));
    };

    if signal.payload.content_type != declaration.content_type {
        return Err(validation_failed(
            &session.channel_name,
            session.workflow_id.as_str(),
            signal,
            declaration.content_type.as_str(),
            "content type does not match declaration",
        ));
    }

    if let Some(schema) = &declaration.payload_schema {
        schema.validate(&signal.payload.data).map_err(|error| {
            validation_failed(
                &session.channel_name,
                session.workflow_id.as_str(),
                signal,
                declaration.content_type.as_str(),
                format!("payload schema mismatch: {error}"),
            )
        })?;
    }
    Ok(())
}

pub(super) fn encode_signal(
    channel_name: &ChannelName,
    workflow_id: &str,
    signal: &SignalPayload,
) -> Result<Vec<u8>, AionSurfaceError> {
    serde_json::to_vec(&json!({
        "signal_name": signal.signal_name.as_str(),
        "content_type": signal.payload.content_type.as_str(),
        "data": signal.payload.data.as_slice()
    }))
    .map_err(|error| delivery_failed(channel_name, workflow_id, &signal.signal_name, error))
}

pub(super) fn drain_delivery(session: &SignalSession) -> Result<SignalPayload, AionSurfaceError> {
    let envelope = session
        .subscription
        .try_next()
        .map_err(|error| {
            delivery_failed(
                &session.channel_name,
                session.workflow_id.as_str(),
                "<unknown>",
                error,
            )
        })?
        .ok_or_else(|| {
            delivery_failed(
                &session.channel_name,
                session.workflow_id.as_str(),
                "<unknown>",
                "signal channel delivered no envelope",
            )
        })?;
    decode_signal(
        &session.channel_name,
        session.workflow_id.as_str(),
        &envelope.payload,
    )
}

fn signal_variant_schema(declaration: &SignalDeclaration) -> Value {
    json!({
        "type": "object",
        "properties": {
            "signal_name": { "type": "string", "enum": [declaration.signal_name.as_str()] },
            "content_type": { "type": "string", "enum": [declaration.content_type.as_str()] },
            "data": {
                "type": "array",
                "items": { "type": "integer", "minimum": 0, "maximum": 255 }
            }
        },
        "required": ["signal_name", "content_type", "data"],
        "additionalProperties": false
    })
}

fn decode_signal(
    channel_name: &ChannelName,
    workflow_id: &str,
    payload: &[u8],
) -> Result<SignalPayload, AionSurfaceError> {
    let value: Value = serde_json::from_slice(payload)
        .map_err(|error| delivery_failed(channel_name, workflow_id, "<unknown>", error))?;
    let signal_name = value
        .get("signal_name")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            delivery_failed(
                channel_name,
                workflow_id,
                "<unknown>",
                "missing signal_name",
            )
        })?;
    let content_type = value
        .get("content_type")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            delivery_failed(
                channel_name,
                workflow_id,
                signal_name,
                "missing content_type",
            )
        })?;
    let data =
        serde_json::from_value(value.get("data").cloned().ok_or_else(|| {
            delivery_failed(channel_name, workflow_id, signal_name, "missing data")
        })?)
        .map_err(|error| delivery_failed(channel_name, workflow_id, signal_name, error))?;

    Ok(SignalPayload {
        signal_name: signal_name.to_owned(),
        payload: Payload {
            data,
            content_type: content_type.to_owned(),
        },
    })
}

fn expected_types(declarations: &[SignalDeclaration]) -> String {
    declarations
        .iter()
        .map(|declaration| format!("{}={}", declaration.signal_name, declaration.content_type))
        .collect::<Vec<_>>()
        .join(", ")
}
