use alloc::sync::Arc;
use core::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};

use super::*;

#[derive(Deserialize, Serialize)]
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

#[test]
fn embedded_subscribe_is_local_and_empty() {
    let config = EmbeddedConfig::new("events", "conversation");
    let handle = EmbeddedChannelHandle::new(&config);
    let subscription = handle.subscribe::<TestMessage>();

    assert!(subscription.is_empty());
}
