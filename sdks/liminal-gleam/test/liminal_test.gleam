//// Tests for the SDK-005 typed channel facade.
////
//// IMPORTANT — why these tests look the way they do:
////
//// The whole point of this SDK is that `channel.publish` is *parametric* over
//// the message type: a `Channel(message)` only accepts a `message` value, and
//// the Gleam type checker rejects any mismatch at COMPILE time. SDK-005's
//// verification gate is therefore primarily a *compile-time* property, not a
//// runtime one.
////
//// The runtime catch: every public entry point that handles a `message` value
//// (`connect`, `from_handle`, `publish`, `subscribe`, `message_schema`) routes
//// through the Erlang NIF module `liminal_sdk_ffi` (see src/liminal/ffi.gleam).
//// That NIF is provided by the Rust `liminal-sdk` crate and does NOT exist in
//// this Gleam-only build, so actually *invoking* any of those functions would
//// fail at load time with `undef`. We deliberately do NOT stub or fake the NIF.
////
//// So the suite is split into two honest halves:
////
////   1. RUNTIME assertions over the parts that are pure Gleam and touch no NIF:
////      construction of the typed message record and of the public facade types
////      (`ConnectionConfig`, `SchemaMetadata`, `SchemaField`, `PressureResponse`,
////      `SdkError`). These genuinely execute.
////
////   2. COMPILE-TIME assertions: `typed_publish_surface` is a function whose
////      body calls `channel.publish` with a correctly-typed record. It is fully
////      type-checked by `gleam build`/`gleam test` (the suite would not compile
////      if the typed API surface were wrong), but it is never *called* at
////      runtime, so it never reaches the missing NIF. The companion
////      commented-out `wrong_typed_publish_surface` demonstrates the "wrong field
////      types produce a compile error" gate — it cannot live uncommented in a
////      passing suite precisely because it would (correctly) fail to compile.

import gleeunit
import gleeunit/should
import liminal
import liminal/channel

pub fn main() {
  gleeunit.main()
}

/// A typed record message type, exactly as an SDK consumer would define one.
pub type ChatMessage {
  ChatMessage(author: String, body: String, sent_at_millis: Int)
}

// --- Compile-time surface (type-checked, never executed) ----------------------

/// Proves that `channel.publish` accepts a correctly-typed record on a
/// `Channel(ChatMessage)` and returns `Result(PressureResponse, SdkError)`.
///
/// This function is type-checked by the compiler but is intentionally never
/// invoked, so it never reaches the missing `liminal_sdk_ffi` NIF. If the typed
/// API were broken (wrong arity, non-parametric channel, wrong return type) this
/// module would fail to compile and the whole suite would fail to build.
pub fn typed_publish_surface(
  chat: channel.Channel(ChatMessage),
) -> Result(channel.PressureResponse, channel.SdkError) {
  let message = ChatMessage(author: "ada", body: "hello", sent_at_millis: 1)
  channel.publish(chat, message)
}

/// Same, exercised through the top-level `liminal` re-export, to prove the
/// facade preserves the parametric typing.
pub fn typed_publish_surface_via_facade(
  chat: liminal.Channel(ChatMessage),
) -> Result(liminal.PressureResponse, liminal.SdkError) {
  liminal.publish(
    chat,
    ChatMessage(author: "ada", body: "hi", sent_at_millis: 2),
  )
}

// COMPILE-ERROR DEMONSTRATION (must stay commented out).
//
// Uncommenting the function below makes `gleam build`/`gleam test` fail with a
// type mismatch, because `channel.publish` on a `Channel(ChatMessage)` requires
// a `ChatMessage`, and these values have the wrong field types. This is the
// "publishing a record with wrong field types produces a compile error" gate;
// it is demonstrated rather than executed, since an actually-failing line cannot
// coexist with a passing test suite.
//
// pub fn wrong_typed_publish_surface(
//   chat: channel.Channel(ChatMessage),
// ) -> Result(channel.PressureResponse, channel.SdkError) {
//   // `body` is an Int here, but ChatMessage.body is a String -> compile error.
//   let bad = ChatMessage(author: "ada", body: 42, sent_at_millis: 1)
//   channel.publish(chat, bad)
//
//   // A wholly unrelated record is also rejected, because the channel is
//   // parameterised over ChatMessage, not over `dynamic`:
//   //   channel.publish(chat, "not even a record")  // compile error
// }

// --- Runtime assertions over pure, NIF-free construction ----------------------

/// The typed message record constructs and field-accesses as plain Gleam data.
pub fn typed_record_constructs_test() {
  let message = ChatMessage(author: "grace", body: "ship it", sent_at_millis: 7)

  message.author |> should.equal("grace")
  message.body |> should.equal("ship it")
  message.sent_at_millis |> should.equal(7)
}

/// `ConnectionConfig` is a pure public facade type and constructs without a NIF.
pub fn connection_config_constructs_test() {
  let config =
    channel.RemoteConnection(
      server_address: "liminal://localhost:9000",
      channel_name: "chat",
      conversation_id: "conv-1",
      max_connections: 4,
      timeout_millis: 5000,
      buffer_size: 64,
    )

  case config {
    channel.RemoteConnection(channel_name:, buffer_size:, ..) -> {
      channel_name |> should.equal("chat")
      buffer_size |> should.equal(64)
    }
  }
}

/// `SchemaMetadata` / `SchemaField` are pure public facade types.
pub fn schema_metadata_constructs_test() {
  let schema =
    channel.SchemaMetadata(
      name: "ChatMessage",
      version: "1",
      fields: [
        channel.SchemaField(name: "author", field_type: "string"),
        channel.SchemaField(name: "body", field_type: "string"),
        channel.SchemaField(name: "sent_at_millis", field_type: "int"),
      ],
      encoded_schema: "{}",
    )

  schema.name |> should.equal("ChatMessage")
  schema.fields
  |> field_names
  |> should.equal(["author", "body", "sent_at_millis"])
}

fn field_names(fields: List(channel.SchemaField)) -> List(String) {
  case fields {
    [] -> []
    [channel.SchemaField(name:, ..), ..rest] -> [name, ..field_names(rest)]
  }
}

/// `PressureResponse` variants construct and pattern-match (pure).
pub fn pressure_response_constructs_test() {
  channel.Accept |> should.equal(channel.Accept)
  channel.Defer(delay_millis: 100) |> should.equal(channel.Defer(100))
  channel.Reject(reason: "full") |> should.equal(channel.Reject("full"))
}

/// `SdkError` variants construct and pattern-match (pure).
pub fn sdk_error_constructs_test() {
  // The indirection through `classify_error` keeps the value typed as the full
  // `SdkError` sum type, so the exhaustive match below proves every variant is
  // in scope (not just the one we constructed).
  let error: channel.SdkError = classify_error("schema mismatch")
  let described = case error {
    channel.TypeValidation(description:)
    | channel.Connection(description:)
    | channel.Protocol(description:)
    | channel.Serialization(description:)
    | channel.Store(description:) -> description
    channel.Backpressure(reason:) -> reason
    channel.Conversation(description:, ..) -> description
  }
  described |> should.equal("schema mismatch")
}

/// Returns a `TypeValidation` error typed as the full `SdkError` sum type, so
/// the caller's match is not narrowed to a single variant by flow analysis.
fn classify_error(description: String) -> channel.SdkError {
  channel.TypeValidation(description: description)
}
