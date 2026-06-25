//// Unit tests for the Gleam SDK connection lifecycle, mirroring the Rust SDK's
//// `connection::lifecycle` tests so both SDKs enforce the same state machine.

import gleeunit/should
import liminal/connection.{
  Connected, Connecting, Disconnected, Normal, Reconnecting, Timeout,
}

fn config() -> connection.ReconnectConfig {
  connection.ReconnectConfig(base_delay_millis: 10, max_delay_millis: 100)
}

pub fn new_starts_connecting_test() {
  connection.new(config())
  |> connection.state
  |> should.equal(Connecting)
}

pub fn connecting_to_connected_test() {
  let lifecycle = connection.new(config())
  let lifecycle = connection.connected(lifecycle) |> should.be_ok
  connection.state(lifecycle) |> should.equal(Connected)
}

pub fn reconnect_from_connected_uses_attempt_zero_test() {
  let lifecycle = connection.new(config())
  let lifecycle = connection.connected(lifecycle) |> should.be_ok
  let #(lifecycle, delay) = connection.reconnect(lifecycle) |> should.be_ok
  connection.state(lifecycle) |> should.equal(Reconnecting(attempt: 0))
  delay |> should.equal(10)
}

pub fn reconnect_increments_attempt_test() {
  let lifecycle = connection.new(config())
  let lifecycle = connection.connected(lifecycle) |> should.be_ok
  let #(lifecycle, _) = connection.reconnect(lifecycle) |> should.be_ok
  let #(lifecycle, delay) = connection.reconnect(lifecycle) |> should.be_ok
  connection.state(lifecycle) |> should.equal(Reconnecting(attempt: 1))
  delay |> should.equal(20)
}

pub fn connected_resets_attempt_test() {
  let lifecycle = connection.new(config())
  let lifecycle = connection.connected(lifecycle) |> should.be_ok
  let #(lifecycle, _) = connection.reconnect(lifecycle) |> should.be_ok
  let lifecycle = connection.connected(lifecycle) |> should.be_ok
  // After a clean reconnect, the next reconnect counts from attempt zero again.
  let #(lifecycle, _) = connection.reconnect(lifecycle) |> should.be_ok
  connection.state(lifecycle) |> should.equal(Reconnecting(attempt: 0))
}

pub fn clean_disconnect_test() {
  let lifecycle = connection.new(config())
  let lifecycle = connection.connected(lifecycle) |> should.be_ok
  let lifecycle = connection.disconnect(lifecycle, Normal) |> should.be_ok
  connection.state(lifecycle) |> should.equal(Disconnected(reason: Normal))
}

pub fn connect_after_disconnect_test() {
  let lifecycle = connection.new(config())
  let lifecycle = connection.connected(lifecycle) |> should.be_ok
  let lifecycle = connection.disconnect(lifecycle, Normal) |> should.be_ok
  let lifecycle = connection.connect(lifecycle) |> should.be_ok
  connection.state(lifecycle) |> should.equal(Connecting)
}

pub fn connect_when_connected_is_invalid_test() {
  let lifecycle = connection.new(config())
  let lifecycle = connection.connected(lifecycle) |> should.be_ok
  connection.connect(lifecycle) |> should.be_error
}

pub fn connected_when_disconnected_is_invalid_test() {
  let lifecycle = connection.new(config())
  let lifecycle = connection.connected(lifecycle) |> should.be_ok
  let lifecycle = connection.disconnect(lifecycle, Normal) |> should.be_ok
  connection.connected(lifecycle) |> should.be_error
}

pub fn disconnect_when_already_disconnected_is_invalid_test() {
  let lifecycle = connection.new(config())
  let lifecycle = connection.connected(lifecycle) |> should.be_ok
  let lifecycle = connection.disconnect(lifecycle, Normal) |> should.be_ok
  connection.disconnect(lifecycle, Timeout) |> should.be_error
}

pub fn reconnect_when_disconnected_is_invalid_test() {
  let lifecycle = connection.new(config())
  let lifecycle = connection.connected(lifecycle) |> should.be_ok
  let lifecycle = connection.disconnect(lifecycle, Normal) |> should.be_ok
  connection.reconnect(lifecycle) |> should.be_error
}

pub fn capped_delay_grows_then_caps_test() {
  let reconnect = config()
  connection.capped_delay(reconnect, 0) |> should.equal(10)
  connection.capped_delay(reconnect, 1) |> should.equal(20)
  connection.capped_delay(reconnect, 2) |> should.equal(40)
  connection.capped_delay(reconnect, 3) |> should.equal(80)
  // 10 * 2^4 = 160, capped to the configured maximum of 100.
  connection.capped_delay(reconnect, 4) |> should.equal(100)
}

pub fn retry_delay_accepts_jitter_within_half_test() {
  // Capped delay at attempt 1 is 20; half is 10.
  connection.retry_delay_with_jitter(config(), 1, 10)
  |> should.be_ok
  |> should.equal(30)
}

pub fn retry_delay_rejects_jitter_over_half_test() {
  connection.retry_delay_with_jitter(config(), 1, 11)
  |> should.be_error
}

pub fn retry_delay_rejects_negative_jitter_test() {
  connection.retry_delay_with_jitter(config(), 1, -1)
  |> should.be_error
}
