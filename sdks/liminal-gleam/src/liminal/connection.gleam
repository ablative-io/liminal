//// Client-side connection lifecycle for the Gleam liminal SDK.
////
//// This mirrors the Rust SDK's `ConnectionLifecycle` (crates/liminal-sdk) so the
//// two SDKs observe identical state transitions under the cross-SDK conformance
//// suite. The lifecycle is an immutable record: each transition returns a new
//// `ConnectionLifecycle` (or an `SdkError` for an invalid transition), and the
//// caller reads `state` after each step. Reconnect backoff is computed the same
//// way as the Rust SDK — `min(base * 2^attempt, max)` plus bounded jitter — so a
//// reconnect attempt never collapses into a fixed retry interval.

import gleam/int
import liminal/channel.{type SdkError, Connection}

/// Application-visible lifecycle state for a remote SDK connection.
pub type ConnectionState {
  /// The SDK is establishing a connection.
  Connecting

  /// The SDK has an active connection.
  Connected

  /// The SDK is attempting to reconnect after a disruption. `attempt` is the
  /// zero-based reconnect counter for the current disruption.
  Reconnecting(attempt: Int)

  /// The SDK is disconnected and will not reconnect without an explicit connect.
  Disconnected(reason: DisconnectReason)
}

/// Reason a connection entered the disconnected state.
pub type DisconnectReason {
  /// The connection was closed intentionally.
  Normal

  /// The connection closed because of an error.
  Errored

  /// The connection closed because a timeout elapsed.
  Timeout
}

/// Event describing a connection lifecycle transition.
pub type ConnectionEvent {
  ConnectionEvent(previous: ConnectionState, current: ConnectionState)
}

/// Exponential reconnect backoff configuration, in milliseconds.
pub type ReconnectConfig {
  ReconnectConfig(base_delay_millis: Int, max_delay_millis: Int)
}

/// Owns the SDK connection lifecycle state and validates transitions.
pub opaque type ConnectionLifecycle {
  ConnectionLifecycle(
    state: ConnectionState,
    reconnect_config: ReconnectConfig,
    next_reconnect_attempt: Int,
  )
}

/// Creates a lifecycle in the `Connecting` state.
pub fn new(reconnect_config: ReconnectConfig) -> ConnectionLifecycle {
  ConnectionLifecycle(
    state: Connecting,
    reconnect_config: reconnect_config,
    next_reconnect_attempt: 0,
  )
}

/// Returns the current connection state.
pub fn state(lifecycle: ConnectionLifecycle) -> ConnectionState {
  lifecycle.state
}

/// Returns the reconnect backoff configuration.
pub fn reconnect_config(lifecycle: ConnectionLifecycle) -> ReconnectConfig {
  lifecycle.reconnect_config
}

/// Transitions from `Disconnected` back to `Connecting`.
pub fn connect(
  lifecycle: ConnectionLifecycle,
) -> Result(ConnectionLifecycle, SdkError) {
  case lifecycle.state {
    Disconnected(..) -> Ok(with_state(lifecycle, Connecting))
    _ -> Error(invalid_transition(lifecycle.state, "Connecting"))
  }
}

/// Transitions from `Connecting` or `Reconnecting` to `Connected`, resetting the
/// reconnect attempt counter.
pub fn connected(
  lifecycle: ConnectionLifecycle,
) -> Result(ConnectionLifecycle, SdkError) {
  case lifecycle.state {
    Connecting | Reconnecting(..) ->
      Ok(with_state(
        ConnectionLifecycle(..lifecycle, next_reconnect_attempt: 0),
        Connected,
      ))
    _ -> Error(invalid_transition(lifecycle.state, "Connected"))
  }
}

/// Transitions to `Reconnecting` and returns the next retry delay in
/// milliseconds.
///
/// The first reconnect after a successful connection uses attempt zero; each
/// subsequent reconnect increments the counter until `connected` resets it. The
/// returned delay uses zero jitter so the transition is deterministic; callers
/// that want jittered delays use `retry_delay_with_jitter`.
pub fn reconnect(
  lifecycle: ConnectionLifecycle,
) -> Result(#(ConnectionLifecycle, Int), SdkError) {
  case lifecycle.state {
    Connecting | Connected | Reconnecting(..) -> {
      let attempt = lifecycle.next_reconnect_attempt
      let delay = capped_delay(lifecycle.reconnect_config, attempt)
      let next =
        with_state(
          ConnectionLifecycle(..lifecycle, next_reconnect_attempt: attempt + 1),
          Reconnecting(attempt: attempt),
        )
      Ok(#(next, delay))
    }
    Disconnected(..) ->
      Error(invalid_transition(lifecycle.state, "Reconnecting"))
  }
}

/// Transitions to `Disconnected` with the supplied reason.
pub fn disconnect(
  lifecycle: ConnectionLifecycle,
  reason: DisconnectReason,
) -> Result(ConnectionLifecycle, SdkError) {
  case lifecycle.state {
    Connecting | Connected | Reconnecting(..) ->
      Ok(with_state(lifecycle, Disconnected(reason: reason)))
    Disconnected(..) ->
      Error(invalid_transition(lifecycle.state, "Disconnected"))
  }
}

/// Computes `min(base * 2^attempt, max)` milliseconds before jitter. A zero base
/// or max yields a zero delay.
pub fn capped_delay(config: ReconnectConfig, attempt: Int) -> Int {
  case config.base_delay_millis <= 0 || config.max_delay_millis <= 0 {
    True -> 0
    False ->
      int.min(config.base_delay_millis * pow2(attempt), config.max_delay_millis)
  }
}

/// Computes the retry delay for an attempt using a precomputed jitter value, in
/// milliseconds. The jitter must be between zero and 50% of the capped delay so
/// reconnection never falls back to a fixed retry interval.
pub fn retry_delay_with_jitter(
  config: ReconnectConfig,
  attempt: Int,
  jitter_millis: Int,
) -> Result(Int, SdkError) {
  let capped = capped_delay(config, attempt)
  let limit = capped / 2
  case jitter_millis < 0 || jitter_millis > limit {
    True ->
      Error(Connection(
        "reconnect jitter "
        <> int.to_string(jitter_millis)
        <> "ms outside [0, "
        <> int.to_string(limit)
        <> "ms] 50% bound",
      ))
    False -> Ok(capped + jitter_millis)
  }
}

fn with_state(
  lifecycle: ConnectionLifecycle,
  next: ConnectionState,
) -> ConnectionLifecycle {
  ConnectionLifecycle(..lifecycle, state: next)
}

fn pow2(exponent: Int) -> Int {
  case exponent <= 0 {
    True -> 1
    False -> 2 * pow2(exponent - 1)
  }
}

fn invalid_transition(
  previous: ConnectionState,
  requested: String,
) -> SdkError {
  Connection(
    "invalid connection transition from "
    <> state_label(previous)
    <> " to "
    <> requested,
  )
}

fn state_label(state: ConnectionState) -> String {
  case state {
    Connecting -> "Connecting"
    Connected -> "Connected"
    Reconnecting(..) -> "Reconnecting"
    Disconnected(..) -> "Disconnected"
  }
}
