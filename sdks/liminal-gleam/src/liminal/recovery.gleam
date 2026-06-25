//// Subscription recovery for the Gleam liminal SDK.
////
//// Mirrors the Rust SDK's `SubscriptionRecovery` (crates/liminal-sdk): it tracks
//// active subscriptions and their last acknowledged sequence numbers so that,
//// after a reconnect, each subscription resumes from the sequence *after* the
//// last one acknowledged — never from zero. The tracker is immutable; every
//// mutating operation returns a new `SubscriptionRecovery`.

import gleam/dict.{type Dict}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import liminal/connection.{type ConnectionEvent, Connected, Reconnecting}

/// Application-visible identifier for an active subscription.
pub opaque type SubscriptionId {
  SubscriptionId(value: Int)
}

/// Creates a subscription identifier from its wire-level numeric value.
pub fn subscription_id(value: Int) -> SubscriptionId {
  SubscriptionId(value)
}

/// Returns the wire-level numeric value of a subscription identifier.
pub fn subscription_id_value(id: SubscriptionId) -> Int {
  id.value
}

/// Resume request produced for an active subscription after reconnecting.
pub type ResumeRequest {
  ResumeRequest(subscription_id: SubscriptionId, from_sequence: Int)
}

/// Tracks active subscriptions and their last acknowledged sequence numbers.
pub opaque type SubscriptionRecovery {
  SubscriptionRecovery(acknowledged: Dict(Int, Int), active: List(Int))
}

/// Creates an empty subscription recovery tracker.
pub fn new() -> SubscriptionRecovery {
  SubscriptionRecovery(acknowledged: dict.new(), active: [])
}

/// Marks a subscription as active for reconnect recovery, keeping the active set
/// sorted and deduplicated.
pub fn track_subscription(
  recovery: SubscriptionRecovery,
  id: SubscriptionId,
) -> SubscriptionRecovery {
  case list.contains(recovery.active, id.value) {
    True -> recovery
    False ->
      SubscriptionRecovery(
        ..recovery,
        active: list.sort([id.value, ..recovery.active], int.compare),
      )
  }
}

/// Records the last acknowledged sequence for a subscription, keeping the
/// maximum seen. Acknowledging also tracks the subscription as active.
pub fn acknowledge(
  recovery: SubscriptionRecovery,
  id: SubscriptionId,
  sequence: Int,
) -> SubscriptionRecovery {
  let tracked = track_subscription(recovery, id)
  let next = case dict.get(tracked.acknowledged, id.value) {
    Ok(existing) -> int.max(existing, sequence)
    Error(Nil) -> sequence
  }
  SubscriptionRecovery(
    ..tracked,
    acknowledged: dict.insert(tracked.acknowledged, id.value, next),
  )
}

/// Returns the last acknowledged sequence for a subscription, if any.
pub fn last_acknowledged_sequence(
  recovery: SubscriptionRecovery,
  id: SubscriptionId,
) -> Option(Int) {
  case dict.get(recovery.acknowledged, id.value) {
    Ok(sequence) -> Some(sequence)
    Error(Nil) -> None
  }
}

/// Computes the next sequence that should be requested for a subscription. A
/// subscription with no prior acknowledgement resumes from zero; one last
/// acknowledged at `N` resumes from `N + 1`.
pub fn resume_sequence(
  recovery: SubscriptionRecovery,
  id: SubscriptionId,
) -> Int {
  case dict.get(recovery.acknowledged, id.value) {
    Ok(sequence) -> sequence + 1
    Error(Nil) -> 0
  }
}

/// Produces resume requests for every active subscription, in subscription-id
/// order.
pub fn resume_requests(recovery: SubscriptionRecovery) -> List(ResumeRequest) {
  list.map(recovery.active, fn(value) {
    let id = SubscriptionId(value)
    ResumeRequest(
      subscription_id: id,
      from_sequence: resume_sequence(recovery, id),
    )
  })
}

/// Produces resume requests only when a transition moves from `Reconnecting` to
/// `Connected`; every other transition produces an empty list.
pub fn resume_requests_for_transition(
  recovery: SubscriptionRecovery,
  event: ConnectionEvent,
) -> List(ResumeRequest) {
  case event.previous, event.current {
    Reconnecting(..), Connected -> resume_requests(recovery)
    _, _ -> []
  }
}

/// Clears recovery state for an explicitly unsubscribed subscription.
pub fn unsubscribe(
  recovery: SubscriptionRecovery,
  id: SubscriptionId,
) -> SubscriptionRecovery {
  SubscriptionRecovery(
    acknowledged: dict.delete(recovery.acknowledged, id.value),
    active: list.filter(recovery.active, fn(value) { value != id.value }),
  )
}

/// Returns true when the subscription is currently tracked as active.
pub fn is_active(recovery: SubscriptionRecovery, id: SubscriptionId) -> Bool {
  list.contains(recovery.active, id.value)
}
