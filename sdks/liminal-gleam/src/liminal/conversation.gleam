//// Conversation handler contract for Gleam-authored liminal conversations.
////
//// Gleam 1.17 does not have native OTP-style `behaviour` declarations in
//// ordinary source files, so this module exposes the contract as a typed record
//// of required callbacks. A module participates in a conversation by exporting a
//// value of this type; omitting any callback is a compile-time error.

import gleam/option.{type Option}
import liminal/channel.{type SdkError}

/// Decision returned when a conversation timeout is handled.
pub type TimeoutAction {
  /// Keep the conversation open with the updated handler state.
  Continue

  /// Close the conversation after timeout handling completes.
  Close
}

/// Reason supplied to a handler when the conversation is terminating.
pub type TerminateReason {
  /// The conversation completed normally.
  Completed

  /// The conversation was closed because the handler chose to close it.
  Closed

  /// The conversation timed out and the runtime is closing it.
  TimedOut

  /// The conversation failed with an SDK error.
  Failed(SdkError)
}

/// Required callback set for a Gleam conversation handler.
///
/// The type parameters carry the handler configuration, long-lived state,
/// inbound message type, and optional reply type. Constructing this record is the
/// declaration that a module implements the handler contract.
pub type Handler(config, state, message, reply) {
  Handler(
    init: fn(config) -> Result(state, SdkError),
    handle_message: fn(state, message) ->
      Result(#(state, Option(reply)), SdkError),
    handle_timeout: fn(state) -> Result(TimeoutAction, SdkError),
    terminate: fn(state, TerminateReason) -> Nil,
  )
}

/// Invoke the handler's init callback when a conversation is opened.
pub fn init(
  handler: Handler(config, state, message, reply),
  config: config,
) -> Result(state, SdkError) {
  handler.init(config)
}

/// Invoke the handler's message callback with the current state and message.
pub fn handle_message(
  handler: Handler(config, state, message, reply),
  state: state,
  message: message,
) -> Result(#(state, Option(reply)), SdkError) {
  handler.handle_message(state, message)
}

/// Invoke the handler's timeout callback with the current state.
pub fn handle_timeout(
  handler: Handler(config, state, message, reply),
  state: state,
) -> Result(TimeoutAction, SdkError) {
  handler.handle_timeout(state)
}

/// Invoke the handler's terminate callback when the conversation ends.
pub fn terminate(
  handler: Handler(config, state, message, reply),
  state: state,
  reason: TerminateReason,
) -> Nil {
  handler.terminate(state, reason)
}
