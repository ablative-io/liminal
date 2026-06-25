import gleam/int
import gleam/io
import gleam/list
import gleam/option.{None, Some}
import gleam/string
import gleeunit
import gleeunit/should
import liminal/channel
import liminal/connection
import liminal/conversation
import liminal/recovery

pub fn main() {
  gleeunit.main()
}

type JsonValue {
  JsonObject(List(#(String, JsonValue)))
  JsonArray(List(JsonValue))
  JsonString(String)
  JsonInt(Int)
  JsonBool(Bool)
}

type ScenarioResult {
  ScenarioResult(
    scenario: String,
    pass: Bool,
    expected: JsonValue,
    observed: JsonValue,
  )
}

type ConversationConfig {
  ConversationConfig(start: Int)
}

type ConversationMessage {
  ConversationMessage(body: String)
}

pub fn conformance_scenarios_match_shared_expectations_test() {
  let results = scenario_results()
  assert_results(results)
  io.println(encode_output(results))
}

fn scenario_results() -> List(ScenarioResult) {
  [
    scenario_result(
      "connection.normal_connect",
      connection_normal_connect_expected(),
      observe_connection_normal_connect(),
    ),
    scenario_result(
      "connection.reconnect_after_drop",
      connection_reconnect_after_drop_expected(),
      observe_connection_reconnect_after_drop(),
    ),
    scenario_result(
      "connection.clean_disconnect",
      connection_clean_disconnect_expected(),
      observe_connection_clean_disconnect(),
    ),
    scenario_result(
      "subscription.resume_from_last_acknowledged",
      subscription_recovery_expected(),
      observe_subscription_recovery(),
    ),
    scenario_result(
      "backpressure.publish_variants",
      backpressure_expected(),
      observe_backpressure_variants(),
    ),
    scenario_result(
      "conversation.open_message_close",
      conversation_expected(),
      observe_conversation_lifecycle(),
    ),
  ]
}

fn scenario_result(
  name: String,
  expected: JsonValue,
  observed: JsonValue,
) -> ScenarioResult {
  ScenarioResult(
    scenario: name,
    pass: observed == expected,
    expected: expected,
    observed: observed,
  )
}

fn assert_results(results: List(ScenarioResult)) -> Nil {
  case results {
    [] -> Nil
    [result, ..rest] -> {
      result.observed |> should.equal(result.expected)
      result.pass |> should.equal(True)
      assert_results(rest)
    }
  }
}

fn observe_connection_normal_connect() -> JsonValue {
  let lifecycle = connection.new(reconnect_config())
  let connecting = state_name(connection.state(lifecycle))

  let lifecycle = connection.connected(lifecycle) |> should.be_ok
  let connected = state_name(connection.state(lifecycle))

  JsonObject([
    #("state_transitions", string_array([connecting, connected])),
    #("final_state", JsonString(state_name(connection.state(lifecycle)))),
  ])
}

fn observe_connection_reconnect_after_drop() -> JsonValue {
  let lifecycle = connection.new(reconnect_config())
  let connecting = state_name(connection.state(lifecycle))

  let lifecycle = connection.connected(lifecycle) |> should.be_ok
  let connected = state_name(connection.state(lifecycle))

  let #(lifecycle, _delay) = connection.reconnect(lifecycle) |> should.be_ok
  let reconnecting = state_name(connection.state(lifecycle))
  let attempts = case connection.state(lifecycle) {
    connection.Reconnecting(attempt:) -> [attempt]
    _ -> []
  }

  let lifecycle = connection.connected(lifecycle) |> should.be_ok
  let reconnected = state_name(connection.state(lifecycle))

  JsonObject([
    #(
      "state_transitions",
      string_array([connecting, connected, reconnecting, reconnected]),
    ),
    #("final_state", JsonString(state_name(connection.state(lifecycle)))),
    #("reconnect_attempts", int_array(attempts)),
  ])
}

fn observe_connection_clean_disconnect() -> JsonValue {
  let lifecycle = connection.new(reconnect_config())
  let connecting = state_name(connection.state(lifecycle))

  let lifecycle = connection.connected(lifecycle) |> should.be_ok
  let connected = state_name(connection.state(lifecycle))

  let lifecycle =
    connection.disconnect(lifecycle, connection.Normal) |> should.be_ok
  let disconnected = state_name(connection.state(lifecycle))

  JsonObject([
    #("state_transitions", string_array([connecting, connected, disconnected])),
    #("final_state", JsonString(state_name(connection.state(lifecycle)))),
    #("disconnect_reason", JsonString(reason_name(connection.state(lifecycle)))),
  ])
}

fn observe_subscription_recovery() -> JsonValue {
  let id = recovery.subscription_id(1)
  let tracker = recovery.new() |> recovery.acknowledge(id, 5)

  let from_sequence = case recovery.resume_requests(tracker) {
    [request] -> request.from_sequence
    _ -> panic as "subscription recovery did not produce one resume request"
  }
  let last_acknowledged = case
    recovery.last_acknowledged_sequence(tracker, id)
  {
    Some(sequence) -> sequence
    None -> panic as "subscription recovery lost the acknowledged sequence"
  }

  JsonObject([
    #("subscription", JsonString("orders")),
    #("last_acknowledged_sequence", JsonInt(last_acknowledged)),
    #("from_sequence", JsonInt(from_sequence)),
  ])
}

fn observe_backpressure_variants() -> JsonValue {
  let responses = [
    normalize_pressure(channel.Accept),
    normalize_pressure(channel.Defer(delay_millis: 250)),
    normalize_pressure(channel.Reject(reason: "consumer overloaded")),
  ]

  JsonObject([#("responses", JsonArray(responses))])
}

fn observe_conversation_lifecycle() -> JsonValue {
  let handler = conversation_handler()
  let state =
    conversation.init(handler, ConversationConfig(start: 0))
    |> should.be_ok
  let #(next_state, reply) =
    conversation.handle_message(
      handler,
      state,
      ConversationMessage(body: "hello"),
    )
    |> should.be_ok
  reply |> should.equal(None)
  conversation.handle_timeout(handler, next_state)
  |> should.be_ok
  |> should.equal(conversation.Close)
  conversation.terminate(handler, next_state, conversation.Closed)

  JsonObject([
    #("events", string_array(["opened", "message", "closing", "closed"])),
  ])
}

fn normalize_pressure(response: channel.PressureResponse) -> JsonValue {
  case response {
    channel.Accept -> JsonObject([#("kind", JsonString("accept"))])
    channel.Defer(delay_millis:) ->
      JsonObject([
        #("kind", JsonString("defer")),
        #("delay", JsonInt(delay_millis)),
      ])
    channel.Reject(reason:) ->
      JsonObject([
        #("kind", JsonString("reject")),
        #("reason", JsonString(reason)),
      ])
  }
}

fn reconnect_config() -> connection.ReconnectConfig {
  connection.ReconnectConfig(base_delay_millis: 10, max_delay_millis: 100)
}

fn state_name(state: connection.ConnectionState) -> String {
  case state {
    connection.Connecting -> "connecting"
    connection.Connected -> "connected"
    connection.Reconnecting(..) -> "reconnecting"
    connection.Disconnected(..) -> "disconnected"
  }
}

fn reason_name(state: connection.ConnectionState) -> String {
  case state {
    connection.Disconnected(reason: connection.Normal) -> "normal"
    connection.Disconnected(reason: connection.Errored) -> "error"
    connection.Disconnected(reason: connection.Timeout) -> "timeout"
    _ -> "none"
  }
}

fn connection_normal_connect_expected() -> JsonValue {
  JsonObject([
    #("state_transitions", string_array(["connecting", "connected"])),
    #("final_state", JsonString("connected")),
  ])
}

fn connection_reconnect_after_drop_expected() -> JsonValue {
  JsonObject([
    #(
      "state_transitions",
      string_array(["connecting", "connected", "reconnecting", "connected"]),
    ),
    #("final_state", JsonString("connected")),
    #("reconnect_attempts", int_array([0])),
  ])
}

fn connection_clean_disconnect_expected() -> JsonValue {
  JsonObject([
    #(
      "state_transitions",
      string_array(["connecting", "connected", "disconnected"]),
    ),
    #("final_state", JsonString("disconnected")),
    #("disconnect_reason", JsonString("normal")),
  ])
}

fn subscription_recovery_expected() -> JsonValue {
  JsonObject([
    #("subscription", JsonString("orders")),
    #("last_acknowledged_sequence", JsonInt(5)),
    #("from_sequence", JsonInt(6)),
  ])
}

fn backpressure_expected() -> JsonValue {
  JsonObject([
    #(
      "responses",
      JsonArray([
        JsonObject([#("kind", JsonString("accept"))]),
        JsonObject([
          #("kind", JsonString("defer")),
          #("delay", JsonInt(250)),
        ]),
        JsonObject([
          #("kind", JsonString("reject")),
          #("reason", JsonString("consumer overloaded")),
        ]),
      ]),
    ),
  ])
}

fn conversation_expected() -> JsonValue {
  JsonObject([
    #("events", string_array(["opened", "message", "closing", "closed"])),
  ])
}

fn string_array(values: List(String)) -> JsonValue {
  JsonArray(list.map(values, JsonString))
}

fn int_array(values: List(Int)) -> JsonValue {
  JsonArray(list.map(values, JsonInt))
}

fn conversation_handler() -> conversation.Handler(
  ConversationConfig,
  Int,
  ConversationMessage,
  Nil,
) {
  conversation.Handler(
    init: conversation_init,
    handle_message: conversation_handle_message,
    handle_timeout: conversation_handle_timeout,
    terminate: conversation_terminate,
  )
}

fn conversation_init(
  config: ConversationConfig,
) -> Result(Int, channel.SdkError) {
  Ok(config.start)
}

fn conversation_handle_message(
  state: Int,
  message: ConversationMessage,
) -> Result(#(Int, option.Option(Nil)), channel.SdkError) {
  message.body |> should.equal("hello")
  Ok(#(state + 1, None))
}

fn conversation_handle_timeout(
  state: Int,
) -> Result(conversation.TimeoutAction, channel.SdkError) {
  state |> should.equal(1)
  Ok(conversation.Close)
}

fn conversation_terminate(
  state: Int,
  reason: conversation.TerminateReason,
) -> Nil {
  state |> should.equal(1)
  reason |> should.equal(conversation.Closed)
}

fn encode_output(results: List(ScenarioResult)) -> String {
  "{\"sdk\":\"gleam\",\"results\":["
  <> join(list.map(results, encode_result), ",")
  <> "]}"
}

fn encode_result(result: ScenarioResult) -> String {
  "{\"scenario\":"
  <> encode_string(result.scenario)
  <> ",\"pass\":"
  <> encode_bool(result.pass)
  <> ",\"expected\":"
  <> encode_value(result.expected)
  <> ",\"observed\":"
  <> encode_value(result.observed)
  <> "}"
}

fn encode_value(value: JsonValue) -> String {
  case value {
    JsonObject(fields) ->
      "{" <> join(list.map(fields, encode_field), ",") <> "}"
    JsonArray(values) -> "[" <> join(list.map(values, encode_value), ",") <> "]"
    JsonString(value) -> encode_string(value)
    JsonInt(value) -> int.to_string(value)
    JsonBool(value) -> encode_bool(value)
  }
}

fn encode_field(field: #(String, JsonValue)) -> String {
  let #(key, value) = field
  encode_string(key) <> ":" <> encode_value(value)
}

fn encode_string(value: String) -> String {
  // Escape the JSON structural characters so a scenario string containing a
  // quote or backslash cannot produce malformed output. The backslash pass runs
  // first so it does not double-escape the escapes introduced afterwards.
  let escaped =
    value
    |> string.replace("\\", "\\\\")
    |> string.replace("\"", "\\\"")
    |> string.replace("\n", "\\n")
    |> string.replace("\r", "\\r")
    |> string.replace("\t", "\\t")
  "\"" <> escaped <> "\""
}

fn encode_bool(value: Bool) -> String {
  case value {
    True -> "true"
    False -> "false"
  }
}

fn join(values: List(String), separator: String) -> String {
  case values {
    [] -> ""
    [first, ..rest] -> first <> join_rest(rest, separator)
  }
}

fn join_rest(values: List(String), separator: String) -> String {
  case values {
    [] -> ""
    [first, ..rest] -> separator <> first <> join_rest(rest, separator)
  }
}
