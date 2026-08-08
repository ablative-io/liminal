mod config;
/// The byte-stream framing layer every real transport shares: one handshake,
/// one partial-frame buffer, one conversation drain. It is generic over
/// [`framing::FrameStream`] rather than duplicated per transport — the third
/// parallel copy is what `docs/design/IN-PROCESS-TRANSPORT.md` §9 ruling 2
/// refuses.
#[cfg(feature = "std")]
mod framing;
mod handles;
#[cfg(feature = "embedded")]
mod loopback;
mod participant;
mod protocol;
#[cfg(feature = "std")]
mod tcp;
pub mod websocket;

#[cfg(feature = "std")]
pub use tcp::{
    DeliveredMessage, FlushMode, FlushOutcome, OBSERVABILITY_CHANNEL, PublishRejection, PushClient,
    PushWriter, PushedFrame, SubscriptionStream, TcpRemoteTransport,
};
#[cfg(feature = "std")]
pub use websocket::{
    WebSocketDeliveredMessage, WebSocketRemoteTransport, WebSocketSubscriptionStream,
};

pub use config::{SdkConfig, build_channel_handle, build_conversation_handle};
pub use handles::{
    RemoteChannelHandle, RemoteConversationHandle, RemoteParticipantHandle, SdkChannelHandle,
    SdkConversationHandle,
};
pub use participant::{
    ParticipantResponseProvenance, ParticipantResumeStore, RemoteDetachReplayOutcome,
    RemoteExpectedOperationRecovery, RemoteLostOperationResolution, RemoteLostReconnectResolution,
    RemoteOperationRecordOutcome, RemoteOperationTransportFate, RemoteParticipantError,
    RemoteParticipantInbound, RemoteParticipantOperation, RemoteParticipantSendOutcome,
    RemoteReconnectAttemptOutcome, RemoteReconnectPermit, RemoteReconnectPermitOutcome,
    RemoteReconnectPermitRecovery, RemoteReplayApplyOutcome, RemoteTransportLossOutcome,
};

#[cfg(test)]
mod tests;

use alloc::string::{String, ToString};
use alloc::sync::Arc;

use crate::connection::ConnectionPoolConfig;
use crate::{ConversationId, SdkError};

use self::protocol::{ProtocolRemoteTransport, RemoteTransport};

/// The one named deadline every reader gives a synchronous control-frame reply.
///
/// Five seconds — the estate's already-ratified value, generalized rather than
/// re-chosen: the WebSocket socket layer and the TCP subscription reader both
/// already read `Duration::from_secs(5)`, and the TCP push reader is the odd one
/// out. Ruled 2026-07-28 by Waffles the Terrible, coordinator seat
/// (PUSH-HANDSHAKE-DEADLINE; see `docs/design/WIRING-LEDGER.md` and
/// `docs/design/sdk/briefs/SDK-010.json`).
///
/// It is armed for the control exchange ONLY — `Connect`/`ConnectAck`,
/// `WorkerRegister`/`WorkerRegisterAck`, `Subscribe`/`SubscribeAck` — and
/// disarmed with `set_read_timeout(None)` before any background reader starts.
/// It MUST NOT survive into steady state: a deadline that outlives its exchange
/// is just a slower cadence, and LAW-1 refuses cadences whatever their period.
///
/// What it replaces was never chosen. `connect_socket` armed a 100 ms reader
/// poll cadence before the handshake, and the synchronous setup read was fatal
/// on the first timeout — composing, by accident, into a 100 ms-per-read fatal
/// deadline on connect. Nobody chose it.
#[cfg(feature = "std")]
pub(crate) const SETUP_TIMEOUT: core::time::Duration = core::time::Duration::from_secs(5);

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
    transport: Arc<dyn RemoteTransport>,
    /// The concretely typed WebSocket transport, retained when
    /// [`connect_websocket`](Self::connect_websocket) installed it so callers
    /// can drive its typed reconnect path.
    #[cfg(feature = "std")]
    websocket: Option<Arc<websocket::WebSocketRemoteTransport>>,
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
            transport: Arc::new(ProtocolRemoteTransport),
            #[cfg(feature = "std")]
            websocket: None,
        })
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
        self.websocket = None;
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
        self.websocket = None;
        Ok(self)
    }

    /// Opens an in-process connection to `server` and installs the loopback
    /// wire transport, replacing the in-process protocol transport.
    ///
    /// Same shape as [`connect_tcp`], same guarantee, different mount. This
    /// performs the protocol handshake (`Connect` -> `ConnectAck`) eagerly
    /// against a REAL server — the same admission slot pool, the same durable
    /// connection incarnation, the same constant-time token compare, the same
    /// frame preflight and participant gate — so a returned configuration is
    /// already connected and every later call traverses the identical framed
    /// wire image a socket would have carried. What it removes is the syscall,
    /// the kernel copy, the descriptor lifecycle, and the round trip; what it
    /// does not remove is any part of the record path.
    ///
    /// The mount is TRUSTED CODE. A co-resident caller already reaches the host
    /// process's heap, descriptors and store handle without this transport, so
    /// what the record vouches for here is that the append came through the
    /// same door — never that its author was contained.
    ///
    /// `self.server_address` is untouched and stays a diagnostic label: nothing
    /// on this path reads a socket fact, and the server's own record carries
    /// `peer_addr: None` for the same reason.
    ///
    /// The server is taken as an [`Arc`] because the participant contract
    /// includes reconnect, and a transport that can open a second connection
    /// later must outlive the call that built it.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when the server refuses to admit the
    /// connection — at `max_connections` this is the same refusal a socket
    /// connect receives — and [`SdkError::Protocol`] when the handshake is
    /// rejected or its frames cannot be encoded.
    ///
    /// [`connect_tcp`]: Self::connect_tcp
    #[cfg(feature = "embedded")]
    pub fn connect_loopback(
        self,
        server: Arc<liminal_server::server::embedded::EmbeddedServer>,
    ) -> Result<Self, SdkError> {
        self.connect_loopback_with_auth(server, &[])
    }

    /// Opens an in-process connection whose handshake carries `auth_token`, for
    /// a server gated by an `[auth]` section, and installs the loopback wire
    /// transport. Additive to [`connect_loopback`]: an empty token behaves
    /// identically to it.
    ///
    /// **Admission is admission.** An embedded caller presenting the wrong
    /// token is refused on its own loopback by the same `connect_response`
    /// compare that refuses a socket caller, and the refusal surfaces here as
    /// the same [`SdkError::Connection`] the socket path produces.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when the connection cannot be admitted
    /// or the token is rejected, and [`SdkError::Protocol`] when the handshake
    /// frames cannot be encoded or sent.
    ///
    /// [`connect_loopback`]: Self::connect_loopback
    #[cfg(feature = "embedded")]
    pub fn connect_loopback_with_auth(
        mut self,
        server: Arc<liminal_server::server::embedded::EmbeddedServer>,
        auth_token: &[u8],
    ) -> Result<Self, SdkError> {
        let transport = self::loopback::LoopbackRemoteTransport::connect_with_auth(
            server,
            auth_token,
        )?;
        self.transport = Arc::new(transport);
        self.websocket = None;
        Ok(self)
    }

    /// Opens a real WebSocket connection to the configured `ws://` server
    /// address and installs the live wire transport, replacing the in-process
    /// protocol transport.
    ///
    /// This performs the WebSocket upgrade and the protocol handshake
    /// (`Connect` -> `ConnectAck`) eagerly through the client unit's typed
    /// permit path, so a returned configuration is already connected.
    /// Subsequent publish, subscribe, and conversation calls traverse the
    /// socket; the concretely typed transport stays reachable through
    /// [`websocket_transport`](Self::websocket_transport) for the typed
    /// reconnect path.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when the address is not a usable
    /// `ws://` URL, the connection cannot be established, or the handshake is
    /// rejected, and [`SdkError::Protocol`] when frames cannot be encoded.
    #[cfg(feature = "std")]
    pub fn connect_websocket(self) -> Result<Self, SdkError> {
        self.connect_websocket_with_auth(&[])
    }

    /// Opens a real WebSocket connection whose handshake carries
    /// `auth_token`, for a server gated by an `[auth]` section, and installs
    /// the live wire transport. Additive to [`connect_websocket`]: an empty
    /// token behaves identically to it.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when the connection cannot be
    /// established or the token is rejected, and [`SdkError::Protocol`] when
    /// the handshake frames cannot be encoded or sent.
    ///
    /// [`connect_websocket`]: Self::connect_websocket
    #[cfg(feature = "std")]
    pub fn connect_websocket_with_auth(mut self, auth_token: &[u8]) -> Result<Self, SdkError> {
        let transport = Arc::new(websocket::WebSocketRemoteTransport::connect_with_auth(
            &self.server_address,
            auth_token,
        )?);
        self.transport = Arc::clone(&transport) as Arc<dyn RemoteTransport>;
        self.websocket = Some(transport);
        Ok(self)
    }

    /// The concretely typed WebSocket transport installed by
    /// [`connect_websocket`](Self::connect_websocket), when one is installed.
    #[cfg(feature = "std")]
    #[must_use]
    pub fn websocket_transport(&self) -> Option<Arc<websocket::WebSocketRemoteTransport>> {
        self.websocket.clone()
    }
}

fn connection_error(description: &str) -> SdkError {
    SdkError::Connection {
        description: description.to_string(),
    }
}
