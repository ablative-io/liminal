//! The server->client delivery pump.
//!
//! On each connection scheduler slice — after inbound socket and control work —
//! the connection process drains its subscriptions here: for every subscription
//! it owns, it pulls ready envelopes (non-blocking) up to a per-slice budget and
//! encodes each as a [`Frame::Deliver`] into the connection's [`OutboundWriter`],
//! on that subscription's own application stream. No new wakeup plumbing is
//! required: the connection process already runs every slice, so a message that
//! lands in a subscriber inbox is picked up on the next poll.
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

use liminal::protocol::{CausalContext, Frame, MessageEnvelope, SchemaId as ProtocolSchemaId};

use super::outbound::{OutboundError, OutboundWriter};
use super::state::ConnectionProcessState;

/// Per-slice cap on the total number of `Deliver` frames enqueued across all of a
/// connection's subscriptions. Bounds the work one connection does per slice so a
/// fast producer cannot starve other connections sharing the scheduler thread.
pub(super) const DELIVERY_SLICE_BUDGET: usize = 32;

/// Drains ready envelopes from this connection's subscriptions into `outbound`,
/// encoding each as a `Deliver` frame, up to `budget` frames total this slice.
///
/// # Errors
/// Returns [`OutboundError`] when the outbound buffer overflows or a delivery
/// frame cannot be encoded — both are fatal and tear the connection down, since a
/// dropped or truncated delivery would desync the subscription stream.
pub(super) fn service_subscriptions(
    state: &mut ConnectionProcessState,
    outbound: &mut OutboundWriter,
    budget: usize,
) -> Result<(), OutboundError> {
    // Destructure so the subscription map and the delivery-sequence map are
    // borrowed disjointly (both are mutated in the loop).
    let ConnectionProcessState {
        subscriptions,
        delivery_seqs,
        ..
    } = state;
    let mut remaining = budget;
    for (subscription_id, subscription) in subscriptions.iter_mut() {
        if remaining == 0 {
            break;
        }
        let stream_id = subscription.stream_id();
        let selected_schema = subscription.selected_schema();
        while remaining > 0 {
            let Some(envelope) = subscription.try_next() else {
                break;
            };
            let sequence = delivery_seqs.entry(*subscription_id).or_insert(0);
            *sequence = sequence.saturating_add(1);
            let frame = build_deliver_frame(stream_id, *sequence, selected_schema, envelope)?;
            outbound.enqueue_frame(&frame)?;
            remaining -= 1;
        }
    }
    Ok(())
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
