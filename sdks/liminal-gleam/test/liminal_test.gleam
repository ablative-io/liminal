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

import gleam/option.{type Option, Some}
import gleeunit
import gleeunit/should
import liminal
import liminal/channel
import liminal/conversation
import liminal/schema

pub fn main() {
  gleeunit.main()
}

/// A typed record message type, exactly as an SDK consumer would define one.
pub type ChatMessage {
  ChatMessage(author: String, body: String, sent_at_millis: Int)
}

/// A typed record used by schema derivation tests.
pub type Person {
  Person(name: String, age: Int)
}

pub type CounterConfig {
  CounterConfig(start: Int)
}

pub type CounterMessage {
  CounterMessage(increment: Int)
}

pub type CounterReply {
  CounterReply(total: Int)
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

/// Proves that the schema module exports a type-checked derive function for
/// Gleam record schema providers.
pub fn derive_schema_surface(
  person_schema: schema.Schema(Person),
) -> schema.SchemaMetadata {
  schema.derive_schema(person_schema)
}

/// Proves that all four conversation callbacks are required by the handler type.
pub fn conversation_handler_surface(
  handler: conversation.Handler(
    CounterConfig,
    Int,
    CounterMessage,
    CounterReply,
  ),
  config: CounterConfig,
  message: CounterMessage,
) -> Result(#(Int, Option(CounterReply)), channel.SdkError) {
  let state = conversation.init(handler, config) |> should.be_ok
  conversation.handle_message(handler, state, message)
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

// COMPILE-ERROR DEMONSTRATION (must stay commented out).
//
// Uncommenting the function below makes `gleam build`/`gleam test` fail because
// the `Handler` constructor requires init, handle_message, handle_timeout, and
// terminate. Leaving out any one callback is a compile-time error. The review
// suite also verifies this with an isolated negative `gleam check` fixture.
//
// pub fn missing_callback_handler() {
//   conversation.Handler(
//     init: counter_init,
//     handle_message: counter_handle_message,
//     handle_timeout: counter_handle_timeout,
//   )
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

/// Conversation handlers construct as a four-callback contract and helpers
/// invoke each callback with the state threaded through the exchange.
pub fn conversation_handler_callbacks_test() {
  let handler = counter_handler()
  let state =
    conversation.init(handler, CounterConfig(start: 2))
    |> should.be_ok

  state |> should.equal(2)

  let #(next_state, maybe_reply) =
    conversation.handle_message(handler, state, CounterMessage(increment: 3))
    |> should.be_ok

  next_state |> should.equal(5)
  let reply = maybe_reply |> should.be_some
  reply.total |> should.equal(5)

  conversation.handle_timeout(handler, next_state)
  |> should.be_ok
  |> should.equal(conversation.Continue)

  conversation.terminate(handler, next_state, conversation.Completed)
}

/// Timeout and termination lifecycle types are public and exhaustive to match.
pub fn conversation_lifecycle_types_construct_test() {
  conversation.Close |> should.equal(conversation.Close)

  let reason = classify_terminate_reason("handler failed")

  let description = case reason {
    conversation.Completed -> "completed"
    conversation.Closed -> "closed"
    conversation.TimedOut -> "timed out"
    conversation.Failed(channel.Conversation(description:, ..)) -> description
    conversation.Failed(_) -> "other failure"
  }

  description |> should.equal("handler failed")
}

/// Schema derivation from a typed record provider produces field names, Gleam
/// type names, and a Rust-SDK-compatible encoded object schema without any
/// external schema file.
pub fn derive_schema_contains_person_fields_test() {
  let person_schema = schema.derive_schema(person_schema())

  person_schema.name |> should.equal("Person")
  person_schema.version |> should.equal("1")
  person_schema.fields |> field_names |> should.equal(["name", "age"])
  person_schema.fields |> field_types |> should.equal(["String", "Int"])
  person_schema.encoded_schema
  |> should.equal(
    "{\"type\":\"object\",\"required\":[\"name\",\"age\"],\"properties\":{\"name\":{\"type\":\"String\"},\"age\":{\"type\":\"Int\"}}}",
  )
}

/// Schema encoding escapes control characters so the Rust SDK receives valid
/// JSON schema bytes even for unusual record or field names.
pub fn record_schema_escapes_json_strings_test() {
  let weird_schema =
    schema.record_schema(name: "Weird", version: "1", fields: [
      schema.field(name: "line\nbreak", field_type: "Quote\\\"Tab\t"),
    ])

  weird_schema.encoded_schema
  |> should.equal(
    "{\"type\":\"object\",\"required\":[\"line\\nbreak\"],\"properties\":{\"line\\nbreak\":{\"type\":\"Quote\\\\\\\"Tab\\t\"}}}",
  )
}

fn person_schema() -> schema.Schema(Person) {
  schema.record(name: "Person", version: "1", fields: [
    schema.string_field(name: "name"),
    schema.int_field(name: "age"),
  ])
}

fn classify_terminate_reason(
  description: String,
) -> conversation.TerminateReason {
  conversation.Failed(channel.Conversation(
    conversation_id: "conv-1",
    description: description,
  ))
}

fn counter_handler() -> conversation.Handler(
  CounterConfig,
  Int,
  CounterMessage,
  CounterReply,
) {
  conversation.Handler(
    init: counter_init,
    handle_message: counter_handle_message,
    handle_timeout: counter_handle_timeout,
    terminate: counter_terminate,
  )
}

fn counter_init(config: CounterConfig) -> Result(Int, channel.SdkError) {
  Ok(config.start)
}

fn counter_handle_message(
  state: Int,
  message: CounterMessage,
) -> Result(#(Int, Option(CounterReply)), channel.SdkError) {
  let next_state = state + message.increment
  Ok(#(next_state, Some(CounterReply(total: next_state))))
}

fn counter_handle_timeout(
  state: Int,
) -> Result(conversation.TimeoutAction, channel.SdkError) {
  case state < 10 {
    True -> Ok(conversation.Continue)
    False -> Ok(conversation.Close)
  }
}

fn counter_terminate(
  _state: Int,
  _reason: conversation.TerminateReason,
) -> Nil {
  Nil
}

fn field_types(fields: List(channel.SchemaField)) -> List(String) {
  case fields {
    [] -> []
    [channel.SchemaField(field_type:, ..), ..rest] -> [
      field_type,
      ..field_types(rest)
    ]
  }
}
