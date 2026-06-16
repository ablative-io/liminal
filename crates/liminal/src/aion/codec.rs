#![allow(clippy::module_name_repetitions)]

use serde_json::{Value, json};

use super::types::{ActivityRequest, ActivityResult, Payload};
use crate::channel::{Schema, SchemaValidationError};

/// Content type for activity dispatch request messages.
pub const DISPATCH_REQUEST_CONTENT_TYPE: &str = "application/vnd.aion.dispatch.request+json";
/// Content type for activity dispatch response messages.
pub const DISPATCH_RESPONSE_CONTENT_TYPE: &str = "application/vnd.aion.dispatch.response+json";
/// Content type for activity dispatch acknowledgement messages.
pub const DISPATCH_ACK_CONTENT_TYPE: &str = "application/vnd.aion.dispatch.ack+json";

/// Typed request sent through an activity dispatch conversation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DispatchRequest {
    /// Conversation correlation identifier.
    pub conversation_id: String,
    /// Activity request scheduled by the workflow.
    pub request: ActivityRequest,
}

impl DispatchRequest {
    /// Content type expected by typed dispatch request channels.
    pub const CONTENT_TYPE: &'static str = DISPATCH_REQUEST_CONTENT_TYPE;

    /// Creates a typed dispatch request.
    #[must_use]
    pub const fn new(conversation_id: String, request: ActivityRequest) -> Self {
        Self {
            conversation_id,
            request,
        }
    }
}

/// Typed response returned by a worker through an activity dispatch conversation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DispatchResponse {
    /// Worker that produced the response.
    pub worker_id: String,
    /// Activity result returned by the worker.
    pub result: ActivityResult,
}

impl DispatchResponse {
    /// Content type expected by typed dispatch response channels.
    pub const CONTENT_TYPE: &'static str = DISPATCH_RESPONSE_CONTENT_TYPE;

    /// Creates a typed dispatch response.
    #[must_use]
    pub const fn new(worker_id: String, result: ActivityResult) -> Self {
        Self { worker_id, result }
    }
}

/// Delivery acknowledgement status for dispatch request publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DispatchAckStatus {
    /// Request was accepted for delivery.
    Accepted,
    /// Request was deferred by backpressure and remains buffered.
    Deferred,
    /// Request was rejected by backpressure or validation.
    Rejected,
}

impl DispatchAckStatus {
    /// Returns the stable wire string for the acknowledgement status.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Deferred => "deferred",
            Self::Rejected => "rejected",
        }
    }
}

/// Typed acknowledgement for a dispatch request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DispatchAck {
    /// Conversation correlation identifier.
    pub conversation_id: String,
    /// Delivery acknowledgement status.
    pub status: DispatchAckStatus,
}

impl DispatchAck {
    /// Content type expected by typed dispatch acknowledgement channels.
    pub const CONTENT_TYPE: &'static str = DISPATCH_ACK_CONTENT_TYPE;

    /// Creates a typed dispatch acknowledgement.
    #[must_use]
    pub const fn new(conversation_id: String, status: DispatchAckStatus) -> Self {
        Self {
            conversation_id,
            status,
        }
    }
}

/// Builds the JSON Schema used to validate dispatch request payloads.
///
/// # Errors
///
/// Returns [`SchemaValidationError`] if the schema definition cannot be compiled.
pub fn dispatch_request_schema() -> Result<Schema, SchemaValidationError> {
    typed_schema(
        DISPATCH_REQUEST_CONTENT_TYPE,
        &json!({
            "conversation_id": {"type": "string", "minLength": 1},
            "request": {"type": "object"},
            "worker_id": false,
            "result": false,
            "status": false
        }),
        &["conversation_id", "request"],
    )
}

/// Builds the JSON Schema used to validate dispatch response payloads.
///
/// # Errors
///
/// Returns [`SchemaValidationError`] if the schema definition cannot be compiled.
pub fn dispatch_response_schema() -> Result<Schema, SchemaValidationError> {
    typed_schema(
        DISPATCH_RESPONSE_CONTENT_TYPE,
        &json!({
            "conversation_id": false,
            "request": false,
            "worker_id": {"type": "string", "minLength": 1},
            "result": {"type": "object"},
            "status": false
        }),
        &["worker_id", "result"],
    )
}

/// Builds the JSON Schema used to validate dispatch acknowledgement payloads.
///
/// # Errors
///
/// Returns [`SchemaValidationError`] if the schema definition cannot be compiled.
pub fn dispatch_ack_schema() -> Result<Schema, SchemaValidationError> {
    typed_schema(
        DISPATCH_ACK_CONTENT_TYPE,
        &json!({
            "conversation_id": {"type": "string", "minLength": 1},
            "request": false,
            "worker_id": false,
            "result": false,
            "status": {"enum": ["accepted", "deferred", "rejected"]}
        }),
        &["conversation_id", "status"],
    )
}

/// Encodes a typed request as JSON bytes for external producers.
///
/// # Errors
///
/// Returns [`serde_json::Error`] if the JSON payload cannot be serialized.
pub fn encode_dispatch_request(message: &DispatchRequest) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&json!({
        "content_type": DispatchRequest::CONTENT_TYPE,
        "conversation_id": message.conversation_id,
        "request": activity_request_value(&message.request)
    }))
}

/// Encodes a typed response as JSON bytes for external producers.
///
/// # Errors
///
/// Returns [`serde_json::Error`] if the JSON payload cannot be serialized.
pub fn encode_dispatch_response(message: &DispatchResponse) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&json!({
        "content_type": DispatchResponse::CONTENT_TYPE,
        "worker_id": message.worker_id,
        "result": activity_result_value(&message.result)
    }))
}

/// Encodes a typed acknowledgement as JSON bytes for external producers.
///
/// # Errors
///
/// Returns [`serde_json::Error`] if the JSON payload cannot be serialized.
pub fn encode_dispatch_ack(message: &DispatchAck) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&json!({
        "content_type": DispatchAck::CONTENT_TYPE,
        "conversation_id": message.conversation_id,
        "status": message.status.as_str()
    }))
}

fn typed_schema(
    content_type: &str,
    properties: &Value,
    required: &[&str],
) -> Result<Schema, SchemaValidationError> {
    let mut required_fields = vec!["content_type"];
    required_fields.extend_from_slice(required);

    Schema::new(json!({
        "type": "object",
        "properties": {
            "content_type": {"const": content_type}
        },
        "required": required_fields,
        "allOf": [{
            "type": "object",
            "properties": properties
        }]
    }))
}

fn activity_request_value(request: &ActivityRequest) -> Value {
    json!({
        "activity_type": request.activity_type,
        "input": payload_value(&request.input),
        "task_queue": request.task_queue,
        "schedule_to_close_timeout_ms": request
            .schedule_to_close_timeout
            .map(|timeout| timeout.as_millis()),
        "start_to_close_timeout_ms": request
            .start_to_close_timeout
            .map(|timeout| timeout.as_millis())
    })
}

fn activity_result_value(result: &ActivityResult) -> Value {
    match result {
        ActivityResult::Completed { output } => json!({
            "status": "completed",
            "output": payload_value(output)
        }),
        ActivityResult::Failed { error } => json!({
            "status": "failed",
            "error": error.to_string()
        }),
    }
}

fn payload_value(payload: &Payload) -> Value {
    json!({
        "content_type": payload.content_type,
        "data": payload.data
    })
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::{
        DISPATCH_ACK_CONTENT_TYPE, DISPATCH_REQUEST_CONTENT_TYPE, DispatchAck, DispatchAckStatus,
        DispatchRequest, DispatchResponse, dispatch_request_schema, dispatch_response_schema,
        encode_dispatch_ack, encode_dispatch_request, encode_dispatch_response,
    };
    use crate::aion::{ActivityRequest, Payload, dispatch_channel};
    use crate::channel::{ChannelConfig, ChannelHandle, ChannelMode};
    use crate::error::LiminalError;

    fn activity_request() -> ActivityRequest {
        ActivityRequest {
            activity_type: "send-email".to_owned(),
            input: Payload {
                data: b"{}".to_vec(),
                content_type: "application/json".to_owned(),
            },
            task_queue: "email".to_owned(),
            schedule_to_close_timeout: None,
            start_to_close_timeout: None,
        }
    }

    #[test]
    fn typed_dispatch_messages_carry_content_types() {
        assert_eq!(DispatchRequest::CONTENT_TYPE, DISPATCH_REQUEST_CONTENT_TYPE);
        assert_eq!(DispatchAck::CONTENT_TYPE, DISPATCH_ACK_CONTENT_TYPE);
        assert_eq!(DispatchAckStatus::Accepted.as_str(), "accepted");
    }

    #[test]
    fn request_schema_accepts_matching_content_type() -> Result<(), Box<dyn Error>> {
        let message = DispatchRequest::new("conversation-1".to_owned(), activity_request());
        let schema = dispatch_request_schema()?;
        let payload = encode_dispatch_request(&message)?;

        schema.validate(payload)?;
        Ok(())
    }

    #[test]
    fn channel_rejects_mismatched_dispatch_content_type() -> Result<(), Box<dyn Error>> {
        let channel_name = dispatch_channel("prod", "email")?;
        let config = ChannelConfig::new(
            String::from(channel_name),
            dispatch_request_schema()?,
            ChannelMode::Ephemeral,
        );
        let channel = ChannelHandle::new(config);
        let rejected = channel
            .publish(br#"{"content_type":"application/json","conversation_id":"c1","request":{}}"#);

        assert!(matches!(rejected, Err(LiminalError::SchemaMismatch { .. })));
        Ok(())
    }

    #[test]
    fn acknowledgement_encoding_uses_status_string() -> Result<(), Box<dyn Error>> {
        let ack = DispatchAck::new("conversation-1".to_owned(), DispatchAckStatus::Deferred);
        let encoded = encode_dispatch_ack(&ack)?;
        let payload = std::str::from_utf8(&encoded)?;

        assert!(payload.contains("deferred"));
        Ok(())
    }

    #[test]
    fn request_schema_rejects_response_shape_even_with_request_content_type()
    -> Result<(), Box<dyn Error>> {
        let schema = dispatch_request_schema()?;
        let response = DispatchResponse::new(
            "worker-1".to_owned(),
            crate::aion::ActivityResult::Completed {
                output: Payload {
                    data: b"ok".to_vec(),
                    content_type: "application/octet-stream".to_owned(),
                },
            },
        );
        let mut payload: serde_json::Value =
            serde_json::from_slice(&encode_dispatch_response(&response)?)?;
        payload["content_type"] =
            serde_json::Value::String(DispatchRequest::CONTENT_TYPE.to_owned());

        let result = schema.validate(serde_json::to_vec(&payload)?);

        assert!(matches!(
            result,
            Err(crate::channel::SchemaValidationError::Mismatch { .. })
        ));
        Ok(())
    }

    #[test]
    fn response_schema_rejects_request_shape_even_with_response_content_type()
    -> Result<(), Box<dyn Error>> {
        let schema = dispatch_response_schema()?;
        let request = DispatchRequest::new("conversation-1".to_owned(), activity_request());
        let mut payload: serde_json::Value =
            serde_json::from_slice(&encode_dispatch_request(&request)?)?;
        payload["content_type"] =
            serde_json::Value::String(DispatchResponse::CONTENT_TYPE.to_owned());

        let result = schema.validate(serde_json::to_vec(&payload)?);

        assert!(matches!(
            result,
            Err(crate::channel::SchemaValidationError::Mismatch { .. })
        ));
        Ok(())
    }
}
