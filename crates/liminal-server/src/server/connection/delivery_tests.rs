use std::collections::VecDeque;

use liminal::channel::SchemaId;
use liminal::envelope::{Envelope, PublisherId};
use liminal::protocol::{Frame, SchemaId as ProtocolSchemaId, decode};

use super::{DELIVERY_SLICE_BUDGET, service_subscriptions};
use crate::ServerError;
use crate::server::connection::outbound::OutboundWriter;
use crate::server::connection::services::{ConnectionSubscription, SubscriptionResource};
use crate::server::connection::state::ConnectionProcessState;

/// A subscription resource that hands out preset envelopes in order, standing in
/// for a live subscriber inbox.
#[derive(Debug)]
struct FakeResource {
    queue: VecDeque<Envelope>,
}

impl SubscriptionResource for FakeResource {
    fn unsubscribe(self: Box<Self>) -> Result<(), ServerError> {
        Ok(())
    }

    fn try_next(&mut self) -> Option<Envelope> {
        self.queue.pop_front()
    }
}

fn core_envelope(payload: Vec<u8>) -> Envelope {
    Envelope::new(payload, None, SchemaId::new(), PublisherId::default())
}

fn subscription_with(id: u64, stream_id: u32, payloads: Vec<Vec<u8>>) -> ConnectionSubscription {
    let queue = payloads.into_iter().map(core_envelope).collect();
    let mut subscription = ConnectionSubscription::new(
        id,
        ProtocolSchemaId::new([9; ProtocolSchemaId::WIRE_LEN]),
        Box::new(FakeResource { queue }),
    );
    subscription.set_stream_id(stream_id);
    subscription
}

fn decode_all(bytes: &[u8]) -> Result<Vec<Frame>, ServerError> {
    let mut frames = Vec::new();
    let mut rest = bytes;
    while !rest.is_empty() {
        let (frame, consumed) = decode(rest).map_err(|error| ServerError::ListenerAccept {
            message: format!("test decode failed: {error}"),
        })?;
        frames.push(frame);
        rest = rest.get(consumed..).unwrap_or(&[]);
    }
    Ok(frames)
}

#[test]
fn drains_in_order_with_monotonic_delivery_seq() -> Result<(), ServerError> {
    let mut state = ConnectionProcessState::default();
    state.subscriptions.insert(
        1,
        subscription_with(1, 5, vec![b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()]),
    );
    let mut outbound = OutboundWriter::new();

    service_subscriptions(&mut state, &mut outbound, DELIVERY_SLICE_BUDGET).map_err(|error| {
        ServerError::ListenerAccept {
            message: format!("delivery drain failed: {error}"),
        }
    })?;

    let frames = decode_all(&outbound.take_bytes())?;
    assert_eq!(frames.len(), 3, "all three ready envelopes are delivered");
    let expected_payloads = [b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()];
    for (index, frame) in frames.iter().enumerate() {
        let Frame::Deliver {
            stream_id,
            delivery_seq,
            envelope,
            ..
        } = frame
        else {
            return Err(ServerError::ListenerAccept {
                message: format!("expected a Deliver frame, got {frame:?}"),
            });
        };
        assert_eq!(*stream_id, 5, "delivery rides the subscription's stream id");
        assert_eq!(
            *delivery_seq,
            u64::try_from(index).unwrap_or(0) + 1,
            "delivery_seq is monotonic from 1"
        );
        assert_eq!(
            envelope.payload,
            expected_payloads.get(index).cloned().unwrap_or_default(),
            "payload is carried verbatim in order"
        );
    }
    Ok(())
}

#[test]
fn per_slice_budget_caps_deliveries() -> Result<(), ServerError> {
    let mut state = ConnectionProcessState::default();
    let payloads: Vec<Vec<u8>> = (0..10).map(|value| vec![value]).collect();
    state
        .subscriptions
        .insert(1, subscription_with(1, 3, payloads));
    let mut outbound = OutboundWriter::new();

    // A budget of 4 delivers only the first four envelopes this slice.
    service_subscriptions(&mut state, &mut outbound, 4).map_err(|error| {
        ServerError::ListenerAccept {
            message: format!("delivery drain failed: {error}"),
        }
    })?;
    assert_eq!(decode_all(&outbound.take_bytes())?.len(), 4);

    // The next slice continues where it left off, and the sequence keeps climbing.
    service_subscriptions(&mut state, &mut outbound, 4).map_err(|error| {
        ServerError::ListenerAccept {
            message: format!("delivery drain failed: {error}"),
        }
    })?;
    let frames = decode_all(&outbound.take_bytes())?;
    assert_eq!(frames.len(), 4);
    let Some(Frame::Deliver { delivery_seq, .. }) = frames.first() else {
        return Err(ServerError::ListenerAccept {
            message: "expected a Deliver frame".to_owned(),
        });
    };
    assert_eq!(
        *delivery_seq, 5,
        "the second slice resumes at delivery_seq 5"
    );
    Ok(())
}
