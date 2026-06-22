use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

use super::{lifecycle_failed, streaming_failed};
use crate::aion::channels::ChannelName;
use crate::aion::error::AionSurfaceError;
use crate::aion::types::{HistoryEvent, Payload};
use crate::channel::Schema;

const HISTORY_EVENT_CONTENT_TYPE: &str = "application/vnd.aion.history.event+json";

pub(super) fn history_schema(channel_name: &ChannelName) -> Result<Schema, AionSurfaceError> {
    Schema::new(json!({
        "type": "object",
        "properties": {
            "content_type": {"const": HISTORY_EVENT_CONTENT_TYPE},
            "sequence": {"type": "integer", "minimum": 0},
            "event_type": {"type": "string"},
            "timestamp_ms": {"type": "integer", "minimum": 0},
            "payload": {
                "type": "object",
                "properties": {
                    "content_type": {"type": "string"},
                    "data": {
                        "type": "array",
                        "items": {"type": "integer", "minimum": 0, "maximum": 255}
                    }
                },
                "required": ["content_type", "data"],
                "additionalProperties": false
            }
        },
        "required": ["content_type", "sequence", "event_type", "timestamp_ms", "payload"],
        "additionalProperties": false
    }))
    .map_err(|error| lifecycle_failed(channel_name, error))
}

pub(super) fn encode_history_event(
    channel_name: &ChannelName,
    workflow_id: &str,
    event: &HistoryEvent,
) -> Result<Vec<u8>, AionSurfaceError> {
    let timestamp_ms = timestamp_to_millis(event.timestamp)
        .map_err(|error| streaming_failed(channel_name, workflow_id, error))?;
    serde_json::to_vec(&json!({
        "content_type": HISTORY_EVENT_CONTENT_TYPE,
        "sequence": event.sequence,
        "event_type": event.event_type,
        "timestamp_ms": timestamp_ms,
        "payload": {
            "content_type": event.payload.content_type,
            "data": event.payload.data
        }
    }))
    .map_err(|error| streaming_failed(channel_name, workflow_id, error))
}

pub(super) fn decode_history_event(
    channel_name: &ChannelName,
    workflow_id: &str,
    payload: &[u8],
) -> Result<HistoryEvent, AionSurfaceError> {
    let value: Value = serde_json::from_slice(payload)
        .map_err(|error| streaming_failed(channel_name, workflow_id, error))?;
    let sequence = required_u64(&value, "sequence", channel_name, workflow_id)?;
    let event_type = required_string(&value, "event_type", channel_name, workflow_id)?.to_owned();
    let timestamp_ms = required_u64(&value, "timestamp_ms", channel_name, workflow_id)?;
    let payload_value = value
        .get("payload")
        .ok_or_else(|| streaming_failed(channel_name, workflow_id, "missing payload"))?;
    let content_type =
        required_string(payload_value, "content_type", channel_name, workflow_id)?.to_owned();
    let data = serde_json::from_value(
        payload_value
            .get("data")
            .cloned()
            .ok_or_else(|| streaming_failed(channel_name, workflow_id, "missing payload.data"))?,
    )
    .map_err(|error| streaming_failed(channel_name, workflow_id, error))?;
    let timestamp = UNIX_EPOCH
        .checked_add(Duration::from_millis(timestamp_ms))
        .ok_or_else(|| streaming_failed(channel_name, workflow_id, "timestamp overflow"))?;

    Ok(HistoryEvent {
        sequence,
        event_type,
        timestamp,
        payload: Payload { data, content_type },
    })
}

fn required_u64(
    value: &Value,
    field: &str,
    channel_name: &ChannelName,
    workflow_id: &str,
) -> Result<u64, AionSurfaceError> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| streaming_failed(channel_name, workflow_id, format!("missing {field}")))
}

fn required_string<'a>(
    value: &'a Value,
    field: &str,
    channel_name: &ChannelName,
    workflow_id: &str,
) -> Result<&'a str, AionSurfaceError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| streaming_failed(channel_name, workflow_id, format!("missing {field}")))
}

fn timestamp_to_millis(timestamp: SystemTime) -> Result<u64, &'static str> {
    let duration = timestamp
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "timestamp precedes Unix epoch")?;
    u64::try_from(duration.as_millis()).map_err(|_| "timestamp milliseconds exceed u64")
}
