//! The embedding handle: a running liminal server with no listener, whose one
//! connection-granting surface is the in-process loopback
//! (design `docs/design/IN-PROCESS-TRANSPORT.md` §2, §4, §9 ruling 4).
//!
//! **What an [`EmbeddedServer`] secures is the RECORD PATH** (hardened
//! face-substrate draft r2 §5). A connection minted here is admitted by the
//! same door a socket connection is admitted by: the same
//! `try_reserve_admission` against the same slot pool, a real durable
//! connection incarnation from the same authority, the same registry record,
//! the same `Connect`/`ConnectAck` handshake with the same constant-time token
//! compare, the same frame preflight, the same participant gate, and the same
//! `apply_frame` seam. No append reaches the record except through that door,
//! and the door does not know which mount knocked. An embedded caller with the
//! wrong token is refused on its own loopback, and an embedded caller arriving
//! at capacity is refused exactly as a socket connect is.
//!
//! **What an [`EmbeddedServer`] does NOT secure is the mount.** A co-resident
//! caller is TRUSTED CODE: it reaches the host process's heap, its descriptors,
//! and its store handle without ever calling this type, so the record vouches
//! for a co-resident mount only as far as the host process itself is trusted.
//! That is inherent to the mount, not a defect of it. Every append admitted
//! here carries the mount fact the admitting door stamped
//! ([`MountKind::Loopback`](crate::server::mount::MountKind::Loopback), §10)
//! precisely because the mount is what a consumer must weigh; this type is not
//! a sandbox and must never be read as one.
//!
//! **The surface is deliberately one door wide.** This handle exposes no
//! supervisor, no services, no store, no handler, no registry, and no scheduler
//! — the module privacy that keeps those unreachable is the structural half of
//! the no-side-door guarantee, and a convenience accessor here would undo it as
//! surely as a public spawn seam would. `connect_loopback` is the whole grant.

use std::fmt;
use std::sync::Arc;

use crate::ServerError;
use crate::config::types::LimitsConfig;
use crate::server::connection::{
    ConnectionServices, ConnectionSupervisor, LoopbackClientEnd, LoopbackDuplex,
};

/// Bytes each direction of an embedded connection's duplex may hold.
///
/// 256 KiB, chosen to sit in the same order as the kernel socket buffers the
/// loopback replaces: a default `SO_SNDBUF`/`SO_RCVBUF` pair on Linux and macOS
/// is tens to a couple of hundred kilobytes, so an embedded writer meets
/// backpressure at roughly the point a socket writer meets it and the mount
/// does not quietly buy itself a deeper queue than every other mount has. It is
/// a BOUND, not a reservation — each ring is a `VecDeque` that grows toward
/// this ceiling only under load, so an idle embedded connection costs two empty
/// queues.
///
/// The value is per ring rather than shared, so a backed-up inbound direction
/// cannot starve the server's replies out of the outbound one.
const LOOPBACK_RING_CAPACITY_BYTES: usize = 256 * 1024;

/// A running liminal server with no listener, granting in-process connections.
///
/// Built from the same ingredients the production stack is built from —
/// services, an optional connection auth token, and the operational limits —
/// and torn down on drop, so an embedded server's lifetime is its handle's.
pub struct EmbeddedServer {
    supervisor: ConnectionSupervisor,
}

impl fmt::Debug for EmbeddedServer {
    /// Prints nothing about the server.
    ///
    /// A `Debug` that rendered the supervisor would be an accessor by another
    /// name: it would put the runtime's contents, the admission counter, and
    /// the registry into any log line that formatted this handle.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EmbeddedServer")
            .finish_non_exhaustive()
    }
}

impl EmbeddedServer {
    /// Starts an embedded server over `services`, open-access and at the
    /// default limits.
    ///
    /// # Errors
    /// Returns [`ServerError`] when incarnation startup or scheduler startup
    /// fails.
    pub fn with_services(services: Arc<dyn ConnectionServices>) -> Result<Self, ServerError> {
        Self::with_services_auth_and_limits(services, None, LimitsConfig::default())
    }

    /// Starts an embedded server over `services`, gated by `auth_token` when
    /// one is configured, under `limits`.
    ///
    /// The three arguments are exactly the three
    /// [`ConnectionSupervisor::with_services_auth_and_limits`] takes, because
    /// an embedded server is a production stack with the listener removed and
    /// nothing else: `None` for the token is the open-access server an absent
    /// `[auth]` section produces, and `limits` carries the same
    /// `max_connections` bound that both connection admission and the durable
    /// incarnation stream enforce.
    ///
    /// # Errors
    /// Returns [`ServerError`] when incarnation startup or scheduler startup
    /// fails.
    pub fn with_services_auth_and_limits(
        services: Arc<dyn ConnectionServices>,
        auth_token: Option<Vec<u8>>,
        limits: LimitsConfig,
    ) -> Result<Self, ServerError> {
        // P0 #56 R3. `server::run` was the only production caller of
        // `metrics::init`, and an embedder never reaches `run` — so an embedded
        // deployment had an entirely INERT metrics surface: every recording
        // helper guards on the uninstalled registry and silently does nothing.
        // That is how a field estate refused 82,166 consecutive connections
        // with nothing to scrape. `init` is idempotent, so a host that also
        // calls `run` (or calls this twice) pays nothing.
        crate::metrics::init();
        Ok(Self {
            supervisor: ConnectionSupervisor::with_services_auth_and_limits(
                services, auth_token, limits,
            )?,
        })
    }

    /// Admits one in-process connection and returns the caller's end of it.
    ///
    /// This is the whole grant. It replaces exactly the listener's `accept()` +
    /// `spawn_connection` pair and nothing else about admission: the returned
    /// end is a byte stream that has not yet handshaken, so the caller still
    /// sends `Connect` and still receives `ConnectAck` or `ConnectError` from
    /// the same `connect_response` a socket client reaches.
    ///
    /// Dropping the returned end tears the connection down by the same
    /// end-of-file a socket hangup produces, releasing its admission slot.
    ///
    /// # Errors
    /// Returns [`ServerError::ConnectionLimitReached`] when the server is at
    /// its `max_connections` bound — the identical typed refusal a socket
    /// connect receives at capacity, surfaced as an error rather than a panic —
    /// and other [`ServerError`] values when incarnation allocation or process
    /// spawn fails. On every refusal the duplex is dropped whole, so no half of
    /// a rejected connection survives.
    pub fn connect_loopback(&self) -> Result<LoopbackClientEnd, ServerError> {
        let (client, server) = LoopbackDuplex::bounded(LOOPBACK_RING_CAPACITY_BYTES);
        // The handle is deliberately discarded: it is a pid plus an incarnation,
        // and handing it back would be a second surface onto the connection the
        // supervisor now owns. The registry record is what keeps the connection
        // addressable, and the client end is what keeps it alive.
        self.supervisor.spawn_loopback_connection(server)?;
        Ok(client)
    }
}

impl Drop for EmbeddedServer {
    /// Stops the connection scheduler, mirroring the in-tree socket fixtures'
    /// teardown (`SdkSocketFixture::stop`): every host record is removed while
    /// the readiness owner is still live, then the scheduler is shut down.
    ///
    /// Teardown is `Drop` alone rather than a `shutdown` method plus `Drop`
    /// because Rust drop points are already deterministic — a caller that wants
    /// the server stopped at a particular moment drops it at that moment — and
    /// a second spelling of the same act is a second surface to keep honest.
    fn drop(&mut self) {
        self.supervisor.shutdown();
    }
}
