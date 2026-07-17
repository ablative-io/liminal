use serde::Serialize;

use super::*;
use crate::{
    ChannelHandle, ConnectionPoolConfig, ConnectionState, EmbeddedConfig, PressureResponse,
    ReconnectEvent, ResumeRequest, SchemaMetadata, SchemaValidate,
};

#[derive(Serialize)]
struct TestMessage {
    id: u64,
}

impl SchemaValidate for TestMessage {
    fn schema_metadata() -> SchemaMetadata {
        SchemaMetadata::new("test.message", "1", br#"{"type":"object"}"#.as_slice())
    }
}

#[test]
fn remote_config_requires_server_address() {
    let pool_config = ConnectionPoolConfig::new(1, 10, 16);

    assert!(RemoteConfig::new(" ", "events", "conversation", pool_config).is_err());
}

#[test]
fn builder_switches_channel_mode_by_config() -> Result<(), SdkError> {
    let embedded = SdkConfig::embedded(EmbeddedConfig::new("events", "conversation"));
    let remote = SdkConfig::remote(RemoteConfig::new(
        "127.0.0.1:9000",
        "events",
        "conversation",
        ConnectionPoolConfig::new(1, 10, 16),
    )?);

    publish_with_generic_handle(&build_channel_handle(&embedded)?)?;
    publish_with_generic_handle(&build_channel_handle(&remote)?)?;
    Ok(())
}

#[test]
fn remote_handle_uses_lifecycle_and_recovery_on_reconnect() -> Result<(), SdkError> {
    let config = RemoteConfig::new(
        "127.0.0.1:9000",
        "events",
        "conversation",
        ConnectionPoolConfig::new(2, 10, 16),
    )?;
    let handle = RemoteChannelHandle::new(&config)?;
    let subscription_id = handle.track_subscription()?;

    handle.acknowledge(subscription_id, 7)?;
    handle.reconnect(ReconnectEvent::EstablishedConnectionFate)?;
    let resume_requests = handle.connected()?;

    assert_eq!(handle.connection_state(), ConnectionState::Connected);
    assert_eq!(
        resume_requests,
        vec![ResumeRequest::new(subscription_id, 8)]
    );
    Ok(())
}

fn publish_with_generic_handle<H>(handle: &H) -> Result<(), SdkError>
where
    H: ChannelHandle,
{
    let response = handle.publish(TestMessage { id: 1 })?;
    assert_eq!(response, PressureResponse::Accept);
    Ok(())
}
