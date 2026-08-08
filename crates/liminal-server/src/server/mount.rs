//! The mount fact: which door admitted a connection.
//!
//! The hardened face-substrate draft (r2 §5) requires that every append
//! admitted through the in-process transport carry a **mount attestation from
//! the admitting door**. The record being attested is the consumer's, and the
//! admitting door for those appends is the consumer's participant machinery
//! riding on liminal, so liminal's obligation is to supply the unforgeable
//! fact — not to stamp its own rows
//! (`docs/design/IN-PROCESS-TRANSPORT.md` §10).
//!
//! **Unforgeable means exactly this:** the server stamps a [`MountKind`] at
//! spawn, from its own knowledge of which spawn path it is executing. The value
//! is not negotiated, is carried in no frame, is not derived from anything a
//! client sends, and no inbound byte can move it. A client that presents any
//! frame content whatsoever reaches [`apply_frame`] with the mount its door
//! already stamped.
//!
//! **Liminal's own durable op-log rows carry NO mount field.** That absence is
//! load-bearing: it is what keeps the design's discriminating test exact, so
//! that liminal record outcomes stay byte-identical across mounts and the mount
//! fact lives only on the context surface where the consumer's door reads it.
//! If the consume side ever needs the fact somewhere other than the handler
//! context, that is a declaration-time conversation — never a silent widening.
//!
//! [`apply_frame`]: crate::server::connection

/// Which transport door admitted a connection.
///
/// Stamped by the server at spawn and carried on both the connection registry
/// record and the participant handler context.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MountKind {
    /// Admitted by the TCP listener's accept loop.
    Tcp,
    /// Admitted by the WebSocket acceptor, after a completed HTTP upgrade.
    WebSocket,
    /// Admitted in-process over a loopback duplex, with no socket and no
    /// listener. **Trusted code:** a co-resident caller reaches the host's
    /// heap, its descriptors, and its store handle without ever calling the
    /// loopback, so this fact says the append came through the same record path
    /// a socket append comes through — never that its author was contained.
    Loopback,
}

impl MountKind {
    /// A stable lowercase name for logs and diagnostics.
    ///
    /// Diagnostics only. Nothing in the protocol carries this string, and
    /// nothing parses it back into a [`MountKind`] — a mount fact that could be
    /// round-tripped through text is a mount fact a client could eventually
    /// supply.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::WebSocket => "websocket",
            Self::Loopback => "loopback",
        }
    }
}

impl Default for MountKind {
    /// The socket door.
    ///
    /// This default exists for ONE reason: [`ConnectionProcessState`] derives
    /// `Default`, and dozens of scheduler-free unit fixtures build a state by
    /// hand with `..Default::default()`. Those fixtures are socket-shaped, so
    /// the socket door is the truthful value for them.
    ///
    /// No production path relies on it. Each of the three spawn paths stamps
    /// its own kind explicitly, and the mount pins assert each door's stamp
    /// rather than trusting this fallback — so a door that forgot to stamp
    /// fails a test instead of quietly reporting `Tcp`.
    ///
    /// [`ConnectionProcessState`]: crate::server::connection
    fn default() -> Self {
        Self::Tcp
    }
}

impl std::fmt::Display for MountKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}
