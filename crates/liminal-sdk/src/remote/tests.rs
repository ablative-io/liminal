use serde::Serialize;

use super::*;
use crate::{
    ChannelHandle, ConnectionPoolConfig, ConnectionState, EmbeddedConfig, PressureResponse,
    SchemaMetadata, SchemaValidate,
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

/// The builder still selects the mode from configuration alone, and the same
/// application code still runs against either handle.
///
/// What this no longer asserts is that either mode ACCEPTS. It used to demand
/// `PressureResponse::Accept` from both, which was a proof of fake success on
/// both sides: the embedded default backend discarded the message, and the
/// remote config was never connected to anything. The mode selection is the
/// claim; the per-mode outcome is now each void's own typed refusal.
#[test]
fn builder_switches_channel_mode_by_config() -> Result<(), SdkError> {
    let embedded = SdkConfig::embedded(EmbeddedConfig::new("events", "conversation"));
    let remote = SdkConfig::remote(RemoteConfig::new(
        "127.0.0.1:9000",
        "events",
        "conversation",
        ConnectionPoolConfig::new(1, 10, 16),
    )?);

    assert_eq!(
        publish_with_generic_handle(&build_channel_handle(&embedded)?)?,
        PressureResponse::Accept
    );

    let remote_result = publish_with_generic_handle(&build_channel_handle(&remote)?);
    assert!(
        matches!(remote_result, Err(SdkError::NotConnected { .. })),
        "a remote handle built from an unconnected config must refuse, got {remote_result:?}"
    );
    Ok(())
}

/// The lifecycle transition and the recovery bookkeeping still run on
/// reconnect; the never-connected transport now refuses to carry the resume.
///
/// This test used to read the resume requests out of `connected()`'s `Ok`. That
/// `Ok` was fake: `resume` encoded a Resume frame and threw it away. The
/// transition itself is still observable, so the lifecycle half of the claim is
/// kept and the transport half becomes the refusal. Resume-request CONTENT is
/// pinned without a transport by the `SubscriptionRecovery` and connection-pool
/// suites, and end to end over a real socket by the server e2e tests.
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
    handle.reconnect_started()?;
    let resume_result = handle.connected();

    assert!(
        matches!(resume_result, Err(SdkError::NotConnected { .. })),
        "resume over a never-connected transport must refuse, got {resume_result:?}"
    );
    assert_eq!(handle.connection_state(), ConnectionState::Connected);
    Ok(())
}

fn publish_with_generic_handle<H>(handle: &H) -> Result<PressureResponse, SdkError>
where
    H: ChannelHandle,
{
    handle.publish(TestMessage { id: 1 })
}
