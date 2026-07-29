use alloc::sync::Arc;
use core::pin::pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::Waker;

use serde::{Deserialize, Serialize};

use super::*;

#[derive(Debug, Deserialize, Serialize)]
struct TestMessage {
    id: u64,
}

impl SchemaValidate for TestMessage {
    fn schema_metadata() -> SchemaMetadata {
        SchemaMetadata::new("embedded.message", "1", br#"{"type":"object"}"#.as_slice())
    }
}

#[derive(Debug, Default)]
struct RecordingChannelBackend {
    saw_reference: AtomicBool,
}

impl EmbeddedChannelBackend for RecordingChannelBackend {
    fn publish(&self, message: &dyn EmbeddedChannelMessage) -> Result<PressureResponse, SdkError> {
        assert_eq!(message.schema_metadata().name.as_ref(), "embedded.message");
        assert!(message.type_name().contains("TestMessage"));
        self.saw_reference.store(true, Ordering::SeqCst);
        Ok(PressureResponse::Accept)
    }
}

#[test]
fn embedded_publish_uses_direct_message_reference() -> Result<(), SdkError> {
    let backend = Arc::new(RecordingChannelBackend::default());
    let config =
        EmbeddedConfig::new("events", "conversation").with_channel_backend(backend.clone());
    let handle = EmbeddedChannelHandle::new(&config);

    assert_eq!(
        handle.publish(TestMessage { id: 1 })?,
        PressureResponse::Accept
    );
    assert!(backend.saw_reference.load(Ordering::SeqCst));
    assert_eq!(handle.channel_name(), "events");
    Ok(())
}

#[test]
fn embedded_config_does_not_require_server_address() {
    let config = EmbeddedConfig::new("events", "conversation");
    let handle = EmbeddedChannelHandle::new(&config);

    assert_eq!(handle.channel_name(), "events");
}

/// This used to assert `subscription.is_empty()` — it pinned the lie. An empty
/// stream is `Ready(None)` on the first poll and forever after, so it read to a
/// caller as a live subscription with nothing published yet, when in truth
/// embedded mode has no delivery path behind `subscribe` at all.
#[test]
fn embedded_subscribe_refuses_instead_of_returning_an_empty_stream() {
    let config = EmbeddedConfig::new("events", "conversation");
    let handle = EmbeddedChannelHandle::new(&config);
    let subscription = handle.subscribe::<TestMessage>();
    let mut subscription = pin!(subscription);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);

    let first = subscription.as_mut().poll_next(&mut context);
    let second = subscription.as_mut().poll_next(&mut context);

    assert!(
        matches!(&first, Poll::Ready(Some(Err(SdkError::Unwired { .. })))),
        "embedded typed subscribe must refuse loudly, got {first:?}"
    );
    assert!(
        matches!(&second, Poll::Ready(None)),
        "the refusal must be one item and then end, got {second:?}"
    );
}
