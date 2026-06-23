import gleam/list
import liminal/ffi

pub opaque type Channel(message) {
  Channel(
    handle: ffi.FfiChannelHandle,
    schema: SchemaMetadata,
    ffi_schema: ffi.SchemaMetadata,
    message_type: message,
  )
}

pub opaque type MessageStream(message) {
  MessageStream(handle: ffi.FfiSubscriptionHandle)
}

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

pub fn connect(
  config: ConnectionConfig,
  message_type: message,
) -> Result(Channel(message), SdkError) {
  case ffi.connect(to_ffi_config(config)) {
    Ok(handle) -> Ok(from_handle(handle, message_type))
    Error(error) -> Error(to_sdk_error(error))
  }
}

pub fn from_handle(
  handle: ffi.FfiChannelHandle,
  message_type: message,
) -> Channel(message) {
  let ffi_schema = ffi.schema_metadata(message_type)

  Channel(
    handle: handle,
    schema: from_ffi_schema(ffi_schema),
    ffi_schema: ffi_schema,
    message_type: message_type,
  )
}

pub fn publish(
  channel: Channel(message),
  message: message,
) -> Result(PressureResponse, SdkError) {
  let schema = ffi.schema_metadata(message)

  case ffi.publish(channel.handle, schema, message) {
    Ok(pressure) -> Ok(from_ffi_pressure(pressure))
    Error(error) -> Error(to_sdk_error(error))
  }
}

pub fn message_schema(message: message) -> SchemaMetadata {
  ffi.schema_metadata(message)
  |> from_ffi_schema
}

pub fn subscribe(
  channel: Channel(message),
) -> Result(MessageStream(message), SdkError) {
  case ffi.subscribe(channel.handle, channel.ffi_schema, channel.message_type) {
    Ok(handle) -> Ok(MessageStream(handle: handle))
    Error(error) -> Error(to_sdk_error(error))
  }
}

pub fn receive(stream: MessageStream(message)) -> Result(message, SdkError) {
  case ffi.receive(stream.handle) {
    Ok(message) -> Ok(message)
    Error(error) -> Error(to_sdk_error(error))
  }
}

pub fn schema(channel: Channel(message)) -> SchemaMetadata {
  channel.schema
}

fn to_ffi_config(config: ConnectionConfig) -> ffi.ConnectionConfig {
  case config {
    RemoteConnection(
      server_address:,
      channel_name:,
      conversation_id:,
      max_connections:,
      timeout_millis:,
      buffer_size:,
    ) ->
      ffi.RemoteConnection(
        server_address:,
        channel_name:,
        conversation_id:,
        max_connections:,
        timeout_millis:,
        buffer_size:,
      )
  }
}

fn from_ffi_schema(schema: ffi.SchemaMetadata) -> SchemaMetadata {
  case schema {
    ffi.SchemaMetadata(name:, version:, fields:, encoded_schema:) ->
      SchemaMetadata(
        name:,
        version:,
        fields: list.map(fields, from_ffi_schema_field),
        encoded_schema:,
      )
  }
}

fn from_ffi_schema_field(field: ffi.SchemaField) -> SchemaField {
  case field {
    ffi.SchemaField(name:, field_type:) -> SchemaField(name:, field_type:)
  }
}

fn from_ffi_pressure(pressure: ffi.PressureResponse) -> PressureResponse {
  case pressure {
    ffi.Accept -> Accept
    ffi.Defer(delay_millis:) -> Defer(delay_millis:)
    ffi.Reject(reason:) -> Reject(reason:)
  }
}

fn to_sdk_error(error: ffi.SdkError) -> SdkError {
  case error {
    ffi.Connection(description:) -> Connection(description:)
    ffi.Protocol(description:) -> Protocol(description:)
    ffi.Serialization(description:) -> Serialization(description:)
    ffi.TypeValidation(description:) -> TypeValidation(description:)
    ffi.Backpressure(reason:) -> Backpressure(reason:)
    ffi.Conversation(conversation_id:, description:) ->
      Conversation(conversation_id:, description:)
    ffi.Store(description:) -> Store(description:)
  }
}
