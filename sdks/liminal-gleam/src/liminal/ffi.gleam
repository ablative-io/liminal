//// Thin Erlang FFI declarations for the Rust-backed liminal SDK NIF.
////
//// This module intentionally contains no frame encoding, retry loops,
//// subscription recovery, or backpressure policy. Those concerns remain in the
//// Rust `liminal-sdk` crate and its Erlang NIF module.

pub type FfiChannelHandle

pub type FfiSubscriptionHandle

pub type ConnectionConfig {
  RemoteConnection(
    server_address: String,
    channel_name: String,
    conversation_id: String,
    max_connections: Int,
    timeout_millis: Int,
    buffer_size: Int,
  )
}

pub type SchemaMetadata {
  SchemaMetadata(
    name: String,
    version: String,
    fields: List(SchemaField),
    encoded_schema: String,
  )
}

pub type SchemaField {
  SchemaField(name: String, field_type: String)
}

pub type PressureResponse {
  Accept
  Defer(delay_millis: Int)
  Reject(reason: String)
}

pub type SdkError {
  Connection(description: String)
  Protocol(description: String)
  Serialization(description: String)
  TypeValidation(description: String)
  Backpressure(reason: String)
  Conversation(conversation_id: String, description: String)
  Store(description: String)
}

@external(erlang, "liminal_sdk_ffi", "connect")
pub fn connect(config: ConnectionConfig) -> Result(FfiChannelHandle, SdkError)

@external(erlang, "liminal_sdk_ffi", "publish")
pub fn publish(
  handle: FfiChannelHandle,
  schema: SchemaMetadata,
  message: message,
) -> Result(PressureResponse, SdkError)

@external(erlang, "liminal_sdk_ffi", "subscribe")
pub fn subscribe(
  handle: FfiChannelHandle,
  schema: SchemaMetadata,
  message_type: message,
) -> Result(FfiSubscriptionHandle, SdkError)

@external(erlang, "liminal_sdk_ffi", "receive")
pub fn receive(subscription: FfiSubscriptionHandle) -> Result(message, SdkError)

@external(erlang, "liminal_sdk_ffi", "schema_metadata")
pub fn schema_metadata(message_type: message) -> SchemaMetadata
