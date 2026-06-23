import liminal/channel
import liminal/ffi

pub type Channel(message) =
  channel.Channel(message)

pub type MessageStream(message) =
  channel.MessageStream(message)

pub type ConnectionConfig =
  channel.ConnectionConfig

pub type SchemaMetadata =
  channel.SchemaMetadata

pub type SchemaField =
  channel.SchemaField

pub type PressureResponse =
  channel.PressureResponse

pub type SdkError =
  channel.SdkError

pub type FfiChannelHandle =
  ffi.FfiChannelHandle

pub type FfiSubscriptionHandle =
  ffi.FfiSubscriptionHandle

pub fn connect(
  config: ConnectionConfig,
  message_type: message,
) -> Result(Channel(message), SdkError) {
  channel.connect(config, message_type)
}

pub fn from_handle(
  handle: FfiChannelHandle,
  message_type: message,
) -> Channel(message) {
  channel.from_handle(handle, message_type)
}

pub fn publish(
  channel: Channel(message),
  message: message,
) -> Result(PressureResponse, SdkError) {
  channel.publish(channel, message)
}

pub fn message_schema(message: message) -> SchemaMetadata {
  channel.message_schema(message)
}

pub fn subscribe(
  channel: Channel(message),
) -> Result(MessageStream(message), SdkError) {
  channel.subscribe(channel)
}

pub fn receive(stream: MessageStream(message)) -> Result(message, SdkError) {
  channel.receive(stream)
}

pub fn schema(channel: Channel(message)) -> SchemaMetadata {
  channel.schema(channel)
}
