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
/// for a live subscriber inbox. `overflowed` stands in for a §5 inbox overflow so
/// the delivery pump's shed path can be exercised without a live channel.
#[derive(Debug)]
struct FakeResource {
    queue: VecDeque<Envelope>,
    overflowed: bool,
}

impl SubscriptionResource for FakeResource {
    fn unsubscribe(self: Box<Self>) -> Result<(), ServerError> {
        Ok(())
    }

    fn try_next(&mut self) -> Option<Envelope> {
        self.queue.pop_front()
    }

    fn has_pending(&self) -> bool {
        !self.queue.is_empty()
    }

    fn is_overflowed(&self) -> bool {
        self.overflowed
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
        Box::new(FakeResource {
            queue,
            overflowed: false,
        }),
    );
    subscription.set_stream_id(stream_id);
    subscription
}

/// An overflowed subscription (its inbox tripped the §5 byte budget or fairness
/// cap), which the pump must shed with a typed `SubscribeError`.
fn overflowed_subscription(id: u64, stream_id: u32) -> ConnectionSubscription {
    let mut subscription = ConnectionSubscription::new(
        id,
        ProtocolSchemaId::new([9; ProtocolSchemaId::WIRE_LEN]),
        Box::new(FakeResource {
            queue: VecDeque::new(),
            overflowed: true,
        }),
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

/// A pipelined burst of large frames whose total size far exceeds the 4 MiB
/// outbound buffer is delivered completely across multiple slices, without ever
/// tearing down a connection whose client reads normally between slices. This is
/// the headroom-aware pump regression: before it, one slice enqueued up to the full
/// 32-frame budget and overflowed the buffer, killing a healthy fast reader.
#[test]
fn pipelined_large_burst_is_delivered_without_teardown() -> Result<(), ServerError> {
    const FRAME_COUNT: usize = 40;
    const PAYLOAD_LEN: usize = 200 * 1024;
    // 40 x 200 KiB = 8 MiB of payload through a 4 MiB buffer: no single slice can
    // hold the whole burst, so the pump must span it across slices.
    let payloads: Vec<Vec<u8>> = (0..FRAME_COUNT)
        .map(|index| {
            let mut payload = vec![0_u8; PAYLOAD_LEN];
            // Stamp the index in the first byte so arrival order can be verified.
            if let Some(first) = payload.first_mut() {
                *first = u8::try_from(index).unwrap_or(0);
            }
            payload
        })
        .collect();
    let mut state = ConnectionProcessState::default();
    state
        .subscriptions
        .insert(1, subscription_with(1, 7, payloads));
    let mut outbound = OutboundWriter::new();

    // A normally-reading client: fully drain (empty) the buffer after each slice.
    let mut delivered = Vec::new();
    for _ in 0..(FRAME_COUNT * 4) {
        service_subscriptions(&mut state, &mut outbound, DELIVERY_SLICE_BUDGET).map_err(
            |error| ServerError::ListenerAccept {
                message: format!("headroom-aware pump tore down a healthy connection: {error}"),
            },
        )?;
        delivered.extend(decode_all(&outbound.take_bytes())?);
        if delivered.len() >= FRAME_COUNT {
            break;
        }
    }

    assert_eq!(
        delivered.len(),
        FRAME_COUNT,
        "every frame in the burst is delivered across slices"
    );
    for (index, frame) in delivered.iter().enumerate() {
        let Frame::Deliver {
            delivery_seq,
            envelope,
            ..
        } = frame
        else {
            return Err(ServerError::ListenerAccept {
                message: format!("expected a Deliver frame, got {frame:?}"),
            });
        };
        assert_eq!(
            *delivery_seq,
            u64::try_from(index).unwrap_or(0) + 1,
            "monotonic order is preserved across slices"
        );
        assert_eq!(
            envelope.payload.len(),
            PAYLOAD_LEN,
            "payload carried verbatim"
        );
        assert_eq!(
            envelope.payload.first().copied().unwrap_or_default(),
            u8::try_from(index).unwrap_or(0),
            "frames arrive in the order they were queued"
        );
    }
    Ok(())
}

/// §5 shed (server half): the delivery pump sheds an overflowed subscription with
/// a typed `SubscribeError` on its stream and reports its id for removal, while a
/// healthy sibling subscription continues to deliver normally.
#[test]
fn overflowed_subscription_is_shed_with_a_typed_error() -> Result<(), ServerError> {
    let mut state = ConnectionProcessState::default();
    state.subscriptions.insert(1, overflowed_subscription(1, 7));
    state
        .subscriptions
        .insert(2, subscription_with(2, 9, vec![b"ok".to_vec()]));
    let mut outbound = OutboundWriter::new();

    let shed = service_subscriptions(&mut state, &mut outbound, DELIVERY_SLICE_BUDGET).map_err(
        |error| ServerError::ListenerAccept {
            message: format!("delivery drain failed: {error}"),
        },
    )?;

    assert_eq!(
        shed,
        vec![1],
        "the overflowed subscription is reported for shed"
    );
    let frames = decode_all(&outbound.take_bytes())?;
    // A SubscribeError on stream 7 (the shed) and a Deliver on stream 9 (healthy).
    assert!(
        frames.iter().any(|frame| matches!(
            frame,
            Frame::SubscribeError { stream_id: 7, message: Some(m), .. } if m.contains("shed")
        )),
        "the shed subscription gets a typed SubscribeError naming the shed: {frames:?}"
    );
    assert!(
        frames
            .iter()
            .any(|frame| matches!(frame, Frame::Deliver { stream_id: 9, .. })),
        "the healthy sibling subscription still delivers: {frames:?}"
    );
    Ok(())
}

/// Review round 1 item 6: the shed path must never itself tear the connection
/// down under outbound pressure. When the outbound buffer's free space cannot
/// hold the shed's `SubscribeError` frame, the shed is DEFERRED (the subscription
/// stays marked, delivery stays skipped) and retried once the drain frees room —
/// the connection survives, drains, reports the error, and sheds exactly one
/// subscription.
#[test]
fn shed_error_frame_defers_under_outbound_pressure() -> Result<(), ServerError> {
    let mut state = ConnectionProcessState::default();
    state.subscriptions.insert(1, overflowed_subscription(1, 7));

    // A small-capacity outbound buffer pre-filled so LESS than one SubscribeError
    // of room remains.
    let mut outbound = OutboundWriter::with_capacity(160);
    outbound
        .enqueue_frame(&deliver_frame_for_test(vec![0_u8; 96]))
        .map_err(|error| ServerError::ListenerAccept {
            message: format!("pre-fill failed: {error}"),
        })?;
    let residue = outbound.queued_len();
    assert!(residue > 0);

    // Under pressure: the shed is deferred, NOT fatal, and nothing is removed.
    let shed = service_subscriptions(&mut state, &mut outbound, DELIVERY_SLICE_BUDGET).map_err(
        |error| ServerError::ListenerAccept {
            message: format!("the deferred shed must not be fatal: {error}"),
        },
    )?;
    assert!(
        shed.is_empty(),
        "the shed is deferred while no room remains"
    );
    assert!(
        state.subscriptions.contains_key(&1),
        "the subscription stays (marked) until its error frame is enqueued"
    );
    assert_eq!(
        outbound.queued_len(),
        residue,
        "nothing was enqueued under pressure"
    );

    // The drain frees room (simulated by taking the queued bytes): the retry on
    // the next slice enqueues the typed error and sheds exactly this subscription.
    let _ = outbound.take_bytes();
    let shed = service_subscriptions(&mut state, &mut outbound, DELIVERY_SLICE_BUDGET).map_err(
        |error| ServerError::ListenerAccept {
            message: format!("the retried shed failed: {error}"),
        },
    )?;
    assert_eq!(shed, vec![1], "the shed lands once room exists");
    let frames = decode_all(&outbound.take_bytes())?;
    assert!(
        frames.iter().any(|frame| matches!(
            frame,
            Frame::SubscribeError { stream_id: 7, message: Some(m), .. } if m.contains("shed")
        )),
        "the typed shed error is reported after the deferral: {frames:?}"
    );
    Ok(())
}

/// Builds a deliver frame with an arbitrary payload for buffer pre-filling.
fn deliver_frame_for_test(payload: Vec<u8>) -> Frame {
    Frame::Deliver {
        flags: 0,
        stream_id: 3,
        delivery_seq: 1,
        envelope: liminal::protocol::MessageEnvelope::new(
            ProtocolSchemaId::new([9; ProtocolSchemaId::WIRE_LEN]),
            liminal::protocol::CausalContext::independent(),
            payload,
        ),
    }
}
