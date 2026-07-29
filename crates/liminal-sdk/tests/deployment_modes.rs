use liminal_sdk::{
    ChannelHandle, ConnectionPoolConfig, EmbeddedConfig, PressureResponse, RemoteConfig,
    SchemaMetadata, SchemaValidate, SdkConfig, SdkError, build_channel_handle,
};
use serde::Serialize;

#[derive(Serialize)]
struct SwitchMessage {
    id: u64,
}

impl SchemaValidate for SwitchMessage {
    fn schema_metadata() -> SchemaMetadata {
        SchemaMetadata::new("switch.message", "1", br#"{"type":"object"}"#.as_slice())
    }
}

/// Switching deployment mode is still a configuration change and nothing else:
/// the identical application call compiles and runs against either handle.
///
/// This used to assert `Accept` from both modes, which proved only that both
/// defaults faked success — the embedded backend discarded the message and the
/// remote config had never been connected to anything. The portability claim is
/// kept; the outcome each mode reports is now its own typed refusal.
#[test]
fn switching_between_embedded_and_remote_changes_only_config() -> Result<(), SdkError> {
    let embedded = SdkConfig::embedded(EmbeddedConfig::new("events", "conversation"));
    let remote = SdkConfig::remote(RemoteConfig::new(
        "127.0.0.1:9000",
        "events",
        "conversation",
        ConnectionPoolConfig::new(1, 10, 16),
    )?);

    assert_eq!(
        publish_with_same_application_code(&build_channel_handle(&embedded)?)?,
        PressureResponse::Accept
    );

    let remote_result = publish_with_same_application_code(&build_channel_handle(&remote)?);
    assert!(
        matches!(remote_result, Err(SdkError::NotConnected { .. })),
        "an unconnected remote config must refuse the publish, got {remote_result:?}"
    );
    Ok(())
}

fn publish_with_same_application_code<H>(handle: &H) -> Result<PressureResponse, SdkError>
where
    H: ChannelHandle,
{
    handle.publish(SwitchMessage { id: 1 })
}
