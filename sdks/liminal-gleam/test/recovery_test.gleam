//// Unit tests for the Gleam SDK subscription recovery, mirroring the Rust SDK's
//// `connection::recovery` tests so both SDKs resume from the same sequence.

import gleam/option.{None, Some}
import gleeunit/should
import liminal/connection.{Connected, Connecting, Reconnecting}
import liminal/recovery.{ResumeRequest}

pub fn without_acknowledgement_resumes_from_zero_test() {
  let id = recovery.subscription_id(7)
  let tracker = recovery.new() |> recovery.track_subscription(id)
  recovery.resume_sequence(tracker, id) |> should.equal(0)
  recovery.resume_requests(tracker)
  |> should.equal([ResumeRequest(subscription_id: id, from_sequence: 0)])
}

pub fn acknowledged_resumes_after_last_sequence_test() {
  let id = recovery.subscription_id(9)
  let tracker = recovery.new() |> recovery.acknowledge(id, 41)
  recovery.last_acknowledged_sequence(tracker, id) |> should.equal(Some(41))
  recovery.resume_sequence(tracker, id) |> should.equal(42)
  recovery.resume_requests(tracker)
  |> should.equal([ResumeRequest(subscription_id: id, from_sequence: 42)])
}

pub fn acknowledge_keeps_maximum_sequence_test() {
  let id = recovery.subscription_id(3)
  let tracker =
    recovery.new()
    |> recovery.acknowledge(id, 10)
    |> recovery.acknowledge(id, 5)
  recovery.last_acknowledged_sequence(tracker, id) |> should.equal(Some(10))
}

pub fn unsubscribe_clears_state_test() {
  let id = recovery.subscription_id(11)
  let tracker =
    recovery.new()
    |> recovery.acknowledge(id, 2)
    |> recovery.unsubscribe(id)
  recovery.is_active(tracker, id) |> should.be_false
  recovery.last_acknowledged_sequence(tracker, id) |> should.equal(None)
  recovery.resume_requests(tracker) |> should.equal([])
}

pub fn reconnect_to_connected_transition_builds_requests_test() {
  let id = recovery.subscription_id(13)
  let tracker = recovery.new() |> recovery.acknowledge(id, 5)
  let event =
    connection.ConnectionEvent(
      previous: Reconnecting(attempt: 2),
      current: Connected,
    )
  recovery.resume_requests_for_transition(tracker, event)
  |> should.equal([ResumeRequest(subscription_id: id, from_sequence: 6)])
}

pub fn non_recovery_transition_builds_no_requests_test() {
  let id = recovery.subscription_id(15)
  let tracker = recovery.new() |> recovery.acknowledge(id, 5)
  let event =
    connection.ConnectionEvent(previous: Connecting, current: Connected)
  recovery.resume_requests_for_transition(tracker, event) |> should.equal([])
}

pub fn track_subscription_dedups_and_sorts_test() {
  let id_a = recovery.subscription_id(5)
  let id_b = recovery.subscription_id(2)
  let tracker =
    recovery.new()
    |> recovery.track_subscription(id_a)
    |> recovery.track_subscription(id_b)
    |> recovery.track_subscription(id_a)
  // The active set is sorted ascending, so resume requests come back in id
  // order: 2 then 5.
  recovery.resume_requests(tracker)
  |> should.equal([
    ResumeRequest(subscription_id: id_b, from_sequence: 0),
    ResumeRequest(subscription_id: id_a, from_sequence: 0),
  ])
}
