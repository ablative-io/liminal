//! The server->client delivery pump.
//!
//! On each connection scheduler slice — after inbound socket and control work —
//! the connection process drains its subscriptions here: for every subscription
//! it owns, it pulls ready envelopes (non-blocking) up to a per-slice budget and
//! encodes each as a [`Frame::Deliver`] into the connection's [`OutboundWriter`],
//! on that subscription's own application stream.
//!
//! PARK-FLIP (R3, §1.2(2)): this pump currently relies on the every-slice busy
//! loop — the connection process already runs every slice, so a message that lands
//! in a subscriber inbox is picked up on the next poll. That every-slice assumption
//! IS the permanent-runnable cost being removed. The R3 subscription-inbox notifier
//! (installed at subscribe time — see `apply::subscribe_response`) already fires the
//! connection's `READY` marker for every envelope admitted to the inbox; the
//! park-flip commit DELETES this every-slice assumption and drives the pump from
//! that marker. Until then the marker is redundant but harmless.
//!
//! The notifier is LEVEL-triggered because of a property this file owns: because
//! this pump drains at most `limits.delivery_slice_budget` envelopes per slice, a
//! subscriber more than one slice behind never empties its inbox, so an
//! edge-triggered notifier would attach a wake to the first envelope of a burst
//! and to none of the rest.
//!
//! That is a lost-wake hazard, but measurement says it is NOT what sheds a
//! subscriber at today's bytes, and this file is where the reason lives: R6
//! coalescing collapses N wakes into one slice, so extra wakes cannot buy extra
//! SLICES, and the number of slices is what that budget converts
//! into drained envelopes. A burst that needs 13 slices at 32/slice is exposed to
//! 13 scheduling round trips; the same burst needs 2 at 256/slice. Raising this
//! constant took the P0 #55 harness from ~52% of boots losing a subscriber to
//! 0/120 — but it is a cross-connection fairness knob (see its own doc), so
//! trading it is a ruling, not a refactor.
//!
//! P0 #55 part 2 made that ruling, and it did NOT raise this budget. The slice
//! budget is how fast a behind subscriber CATCHES UP; what decides whether it is
//! killed while behind is how deep its inbox may get first, and that cap
//! (`LimitsConfig::max_subscription_inbox_depth`) was tripping at ~1% of the
//! memory the connection was already permitted. Raising the depth default removes
//! the kill without spending anyone else's scheduler time; raising this budget
//! would have removed it by spending exactly that. So the DEFAULT stays 32 and
//! the number became operator-configurable instead — the pump is now called with
//! `limits.delivery_slice_budget`, whose default and full ruling live on
//! [`crate::config::LimitsConfig::DEFAULT_DELIVERY_SLICE_BUDGET`].
//!
//! # Envelope bridging
//!
//! The library hands us a core [`liminal::envelope::Envelope`]; the wire carries
//! a protocol [`MessageEnvelope`] (the same type publish and conversation frames
//! use), which the SDK decodes directly through the protocol codec. We therefore
//! construct a protocol envelope from the core one: the payload is carried
//! verbatim (delivery fidelity), and the schema id is the one negotiated for this
//! subscription at subscribe time. Causal metadata is left independent for v1 —
//! the core (UUID parent-chain) and protocol (string parent id + vector clock)
//! causal models differ, so faithfully bridging them is deferred to the v2 credit
//! work; payload, schema, and `delivery_seq` are what the pump must carry now.

use liminal::protocol::{
    CausalContext, Frame, MessageEnvelope, SchemaId as ProtocolSchemaId, encoded_len,
};

use super::outbound::{OutboundError, OutboundWriter};
use super::state::ConnectionProcessState;

/// Reason code carried on the `SubscribeError` frame that sheds an overflowed
/// subscription (§5). Matches the server-error code the subscribe/pressure paths
/// use, so a shed reads as a server-side subscription failure to the client.
const SERVER_ERROR_CODE: u16 = 0xFFFF;

/// The pump's outbound seam (LP-WS-TRANSPORT R1.3): a bounded frame sink the
/// delivery pump enqueues `Deliver`/`SubscribeError` frames into.
///
/// [`OutboundWriter`] (the TCP byte queue) and the WebSocket sibling's
/// message-framed queue both implement it, so BOTH transports run this ONE
/// pump — the WebSocket path reuses the delivery logic rather than reimplement
/// it, and the TCP call sites compile unchanged against the same methods they
/// always called. The trait deliberately mirrors `OutboundWriter`'s inherent
/// signatures exactly; it adds no new behaviour.
pub(super) trait DeliverySink {
    /// The sink's fixed capacity in bytes (the spec-inherent single-frame bound).
    fn capacity(&self) -> usize;
    /// Whether `needed` more bytes fit within the remaining capacity.
    fn has_room(&self, needed: usize) -> bool;
    /// Encodes `frame` into the sink.
    ///
    /// # Errors
    /// Returns [`OutboundError`] when the frame overflows the bounded sink or
    /// cannot be encoded — fatal for the connection, exactly as before.
    fn enqueue_frame(&mut self, frame: &Frame) -> Result<(), OutboundError>;
}

impl DeliverySink for OutboundWriter {
    fn capacity(&self) -> usize {
        Self::capacity(self)
    }

    fn has_room(&self, needed: usize) -> bool {
        Self::has_room(self, needed)
    }

    fn enqueue_frame(&mut self, frame: &Frame) -> Result<(), OutboundError> {
        Self::enqueue_frame(self, frame)
    }
}

/// Drains ready envelopes from this connection's subscriptions into `outbound`,
/// encoding each as a `Deliver` frame, up to `budget` frames total this slice.
///
/// # Errors
/// Returns [`OutboundError`] when the outbound buffer overflows or a delivery
/// frame cannot be encoded — both are fatal and tear the connection down, since a
/// dropped or truncated delivery would desync the subscription stream.
pub(super) fn service_subscriptions<Sink: DeliverySink>(
    state: &mut ConnectionProcessState,
    outbound: &mut Sink,
    budget: usize,
) -> Result<Vec<u64>, OutboundError> {
    // Destructure so the subscription map, the delivery-sequence map, and the
    // held-frame map are borrowed disjointly (all are mutated in the loop).
    let ConnectionProcessState {
        subscriptions,
        delivery_seqs,
        held_deliveries,
        mount,
        ..
    } = state;
    // The transport this connection was admitted on, already stamped by its spawn
    // path — the pump does not need to be told which door it is behind.
    let transport = *mount;
    let mut remaining = budget;
    let mut delivered = 0_u64;
    // §5 shed: subscriptions whose inbox overflowed the connection byte budget or
    // the fairness trip. Each is sent a typed `SubscribeError` here and removed by
    // the caller after the loop (a subscription cannot be removed mid-iteration).
    let mut shed = Vec::new();
    for (subscription_id, subscription) in subscriptions.iter_mut() {
        if remaining == 0 {
            break;
        }
        let stream_id = subscription.stream_id();
        let selected_schema = subscription.selected_schema();
        // §5: a subscription whose inbox overflowed is shed — the offending
        // subscription is released with an explicit typed error frame to the
        // subscriber, mirroring the outbound overflow policy (a slow consumer
        // sheds its own subscription; it cannot grow server memory without bound).
        //
        // The shed must never itself tear the connection down under outbound
        // pressure (review round 1 item 6 — the promise is shed-NOT-teardown, and
        // it must hold exactly under the pressure that causes sheds): a shed frame
        // that fits an empty buffer but not the CURRENT free space is DEFERRED
        // work, not a fatal overflow. The subscription stays marked (the sticky
        // overflow flag; delivery below is skipped by this same branch) and the
        // typed error retries on a later slice once the drain frees room. The
        // subscription is removed/released only AFTER its error frame is
        // successfully enqueued. Only a frame that cannot fit an EMPTY buffer
        // falls through to the fatal overflow (the spec-inherent single-frame
        // bound) — unreachable for this small fixed-text frame in practice.
        if subscription.is_overflowed() {
            let frame = Frame::SubscribeError {
                flags: 0,
                stream_id,
                reason_code: SERVER_ERROR_CODE,
                message: Some(
                    "subscription shed: connection inbox byte budget or per-inbox fairness \
                     limit exceeded"
                        .to_owned(),
                ),
            };
            let needed = encoded_len(&frame).map_err(OutboundError::Encode)?;
            if needed <= outbound.capacity() && !outbound.has_room(needed) {
                // Deferred: retry the shed notification next slice.
                continue;
            }
            outbound.enqueue_frame(&frame)?;
            // LOUD (P0 #55). A SUCCESSFUL shed used to say nothing: the only
            // event on this path fired when RELEASING a shed subscription
            // FAILED, which a shed that works never reaches. A subscriber
            // therefore went permanently silent on an open socket whose
            // publishes kept acking, and no instrument on the server named it.
            // The counter is what an alert fires on; this line is what the alert
            // is then read against, and it carries the three facts needed to act
            // -- which channel, which subscription, which door.
            tracing::warn!(
                subscription_id = *subscription_id,
                channel = subscription.channel(),
                transport = transport.as_str(),
                stream_id,
                "subscription shed: inbox overflowed the connection byte budget or the \
                 per-inbox fairness limit; the subscriber is released and will receive \
                 nothing further on this subscription"
            );
            crate::metrics::subscription_shed();
            shed.push(*subscription_id);
            continue;
        }
        while remaining > 0 {
            // Flush a frame held back from an earlier slice first (its delivery_seq
            // is already assigned, so this preserves order), otherwise pull and
            // frame the next ready envelope.
            let frame = if let Some(held) = held_deliveries.remove(subscription_id) {
                held
            } else {
                let Some(envelope) = subscription.try_next() else {
                    break;
                };
                let sequence = delivery_seqs.entry(*subscription_id).or_insert(0);
                *sequence = sequence.saturating_add(1);
                build_deliver_frame(stream_id, *sequence, selected_schema, envelope)?
            };
            let needed = encoded_len(&frame).map_err(OutboundError::Encode)?;
            // Hold back a frame that fits an empty buffer but not the current free
            // space: it rides out on a later slice once the outbound drain frees
            // room, so a pipelined burst is delivered completely without tearing
            // down a healthy fast-reading connection. Stop the whole slice — the
            // buffer is full, so no other subscription can enqueue either. A frame
            // larger than the whole buffer can never fit, so it falls through to
            // `enqueue_frame` and its fatal Overflow tears the connection down (the
            // spec-inherent single-frame bound).
            if needed <= outbound.capacity() && !outbound.has_room(needed) {
                held_deliveries.insert(*subscription_id, frame);
                // This slice ends here, so its deliveries are recorded here too:
                // an early return that skipped the counter would under-report
                // exactly the slices that ran under outbound pressure.
                crate::metrics::transport_deliveries_recorded(transport, delivered);
                return Ok(shed);
            }
            outbound.enqueue_frame(&frame)?;
            remaining -= 1;
            delivered += 1;
        }
    }
    crate::metrics::transport_deliveries_recorded(transport, delivered);
    Ok(shed)
}

/// Builds a `Deliver` frame from a drained core envelope: the protocol envelope
/// carries the payload verbatim under the subscription's negotiated schema, with
/// an independent causal context (see the module note on causal bridging).
fn build_deliver_frame(
    stream_id: u32,
    delivery_seq: u64,
    selected_schema: ProtocolSchemaId,
    envelope: liminal::envelope::Envelope,
) -> Result<Frame, OutboundError> {
    let protocol_envelope = MessageEnvelope::new(
        selected_schema,
        CausalContext::independent(),
        envelope.payload,
    );
    Frame::new_deliver(stream_id, delivery_seq, protocol_envelope).map_err(OutboundError::Encode)
}

#[cfg(test)]
#[path = "delivery_tests.rs"]
mod tests;
