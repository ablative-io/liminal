use crate::embedded::EmbeddedConfig;
use crate::{ChannelHandle, ConversationHandle, SdkError};

use super::{RemoteConfig, SdkChannelHandle, SdkConversationHandle};

/// Deployment-mode configuration used by the SDK builders.
#[derive(Clone, Debug)]
pub enum SdkConfig {
    /// Direct in-process deployment with no server address or sockets.
    Embedded(EmbeddedConfig),
    /// Remote deployment using the SDK-internal wire protocol transport.
    Remote(RemoteConfig),
}

impl SdkConfig {
    /// Creates embedded deployment configuration.
    #[must_use]
    pub const fn embedded(config: EmbeddedConfig) -> Self {
        Self::Embedded(config)
    }

    /// Creates remote deployment configuration.
    #[must_use]
    pub const fn remote(config: RemoteConfig) -> Self {
        Self::Remote(config)
    }

    /// Builds a channel handle selected only by this configuration.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] if the selected mode cannot be initialized.
    pub fn channel_handle(&self) -> Result<SdkChannelHandle, SdkError> {
        SdkChannelHandle::new(self)
    }

    /// Builds a conversation handle selected only by this configuration.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] if the selected mode cannot be initialized.
    pub fn conversation_handle(&self) -> Result<SdkConversationHandle, SdkError> {
        SdkConversationHandle::new(self)
    }
}

/// Builds a channel handle selected by [`SdkConfig`].
///
/// # Errors
///
/// Returns [`SdkError`] if the selected mode cannot be initialized.
pub fn build_channel_handle(config: &SdkConfig) -> Result<impl ChannelHandle, SdkError> {
    config.channel_handle()
}

/// Builds a conversation handle selected by [`SdkConfig`].
///
/// # Errors
///
/// Returns [`SdkError`] if the selected mode cannot be initialized.
pub fn build_conversation_handle(config: &SdkConfig) -> Result<impl ConversationHandle, SdkError> {
    config.conversation_handle()
}
