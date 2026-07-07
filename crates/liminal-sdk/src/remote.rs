mod config;
mod handles;
mod protocol;
#[cfg(feature = "std")]
mod tcp;

#[cfg(feature = "std")]
pub use tcp::{OBSERVABILITY_CHANNEL, PushClient, PushWriter, PushedFrame, TcpRemoteTransport};

pub use config::{SdkConfig, build_channel_handle, build_conversation_handle};
pub use handles::{
    RemoteChannelHandle, RemoteConversationHandle, SdkChannelHandle, SdkConversationHandle,
};

#[cfg(test)]
mod tests;

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use core::time::Duration;

use crate::connection::{ConnectionPoolConfig, ReconnectConfig, ReconnectJitter};
use crate::{ConversationId, SdkError};

use self::protocol::{ProtocolRemoteTransport, RemoteTransport};

/// Application-level address for a remote liminal server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerAddress(String);

impl ServerAddress {
    /// Creates and validates a remote server address.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] when the supplied address is empty.
    pub fn new(value: impl Into<String>) -> Result<Self, SdkError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(connection_error("remote mode requires a server address"));
        }
        Ok(Self(value))
    }

    /// Returns the server address string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// Configuration for remote SDK handles.
#[derive(Clone, Debug)]
pub struct RemoteConfig {
    /// Remote server address. Remote mode cannot be created without this value.
    pub server_address: ServerAddress,
    /// Application-visible channel name.
    pub channel_name: String,
    /// Application-visible conversation identifier.
    pub conversation_id: ConversationId,
    /// Caller/runtime-supplied connection pool configuration.
    pub pool_config: ConnectionPoolConfig,
    /// Reconnect policy used by the SDK-003 lifecycle state machine.
    pub reconnect_config: ReconnectConfig,
    transport: Arc<dyn RemoteTransport>,
}

impl RemoteConfig {
    /// Creates remote configuration with a required server address and pool config.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] if the address or pool configuration is invalid.
    pub fn new(
        server_address: impl Into<String>,
        channel_name: impl Into<String>,
        conversation_id: impl Into<ConversationId>,
        pool_config: ConnectionPoolConfig,
    ) -> Result<Self, SdkError> {
        Ok(Self {
            server_address: ServerAddress::new(server_address)?,
            channel_name: channel_name.into(),
            conversation_id: conversation_id.into(),
            pool_config: pool_config.validate()?,
            reconnect_config: ReconnectConfig::default(),
            transport: Arc::new(ProtocolRemoteTransport),
        })
    }

    /// Replaces the reconnect configuration used by remote handles.
    #[must_use]
    pub const fn with_reconnect_config(mut self, reconnect_config: ReconnectConfig) -> Self {
        self.reconnect_config = reconnect_config;
        self
    }

    /// Opens a real TCP connection to the configured server and installs the
    /// live wire transport, replacing the in-process protocol transport.
    ///
    /// This performs the protocol handshake (`Connect` -> `ConnectAck`) eagerly,
    /// so a returned configuration is already connected to the server. Subsequent
    /// publish, subscribe, and conversation calls traverse the socket.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when the TCP connection cannot be
    /// established and [`SdkError::Protocol`] when the handshake is rejected.
    #[cfg(feature = "std")]
    pub fn connect_tcp(mut self) -> Result<Self, SdkError> {
        let transport = self::tcp::TcpRemoteTransport::connect(&self.server_address)?;
        self.transport = Arc::new(transport);
        Ok(self)
    }

    /// Opens a real TCP connection whose handshake carries `auth_token`, for a
    /// server gated by an `[auth]` section, and installs the live wire transport.
    ///
    /// Additive to [`connect_tcp`]: an empty token behaves identically to it. The
    /// server compares the token during the handshake and closes the connection on
    /// a mismatch, which surfaces here as [`SdkError::Connection`].
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when the TCP connection cannot be
    /// established or the token is rejected, and [`SdkError::Protocol`] when the
    /// handshake frames cannot be encoded or sent.
    ///
    /// [`connect_tcp`]: Self::connect_tcp
    #[cfg(feature = "std")]
    pub fn connect_tcp_with_auth(mut self, auth_token: &[u8]) -> Result<Self, SdkError> {
        let transport =
            self::tcp::TcpRemoteTransport::connect_with_auth(&self.server_address, auth_token)?;
        self.transport = Arc::new(transport);
        Ok(self)
    }
}

/// Deterministic jitter source for lifecycle integration tests and explicit reconnect calls.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoJitter;

impl ReconnectJitter for NoJitter {
    fn jitter(&mut self, attempt: u32, capped_delay: Duration) -> Duration {
        core::hint::black_box((attempt, capped_delay));
        Duration::ZERO
    }
}

fn connection_error(description: &str) -> SdkError {
    SdkError::Connection {
        description: description.to_string(),
    }
}
