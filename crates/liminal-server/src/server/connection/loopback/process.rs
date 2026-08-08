//! The loopback connection process (design §8 step 3).
//!
//! There is no second process type here, and that is the point. The WebSocket
//! sibling had to grow a parallel slice loop, a parallel probe, and a parallel
//! close path, and the design names that outcome — "copy three" — as the thing
//! this build refuses. What this module supplies instead is the ONE thing that
//! genuinely differs between a socket-admitted connection and an in-process
//! one: the read half, the write half, and how the reader is told. Everything
//! else — the slice ordering, [`apply_frame`], the pending-reply table, the
//! delivery and publication pumps, the control vocabulary, and every close and
//! fate path — is the shared [`TransportConnectionProcess`], reached through
//! [`LoopbackConnectionProcess`], which is that process over this transport.
//!
//! [`apply_frame`]: super::super::apply::apply_frame

use std::io::{Read, Write};
use std::sync::Arc;

use beamr::native::native_process::NativeContext;
use beamr::scheduler::Interest;

use liminal_protocol::wire::ConnectionIncarnation;

use super::super::process::{
    ConnectionTransport, InboundPending, READ_BUFFER_BYTES, ReadStatus, TransportConnectionProcess,
};
use super::super::supervisor::ConnectionRuntime;
use super::duplex::LoopbackServerEnd;
use crate::ServerError;
use crate::server::mount::MountKind;

#[cfg(test)]
#[path = "process_tests.rs"]
mod tests;

/// A supervised connection whose transport is a loopback duplex.
///
/// The third member of the transport family, and a type ALIAS rather than a
/// type: a loopback connection is the ordinary connection process, over the
/// ordinary runtime, with the ordinary lifecycle — reading from a ring instead
/// of a socket.
pub(in super::super) type LoopbackConnectionProcess = TransportConnectionProcess<LoopbackTransport>;

/// The loopback transport: the server end of a bounded byte duplex.
///
/// **No readiness, no spinning.** The duplex has no descriptor, so beamr's
/// `RawFd` readiness facility cannot arm it; and returning `Continue` to poll
/// the ring is the retired busy loop this codebase deliberately left behind.
/// Instead the connection is TOLD: [`Self::install_wake`] registers this
/// connection's `READY` waker on the duplex, and the client end fires it when a
/// write takes the inbound ring from empty to non-empty, and again when the
/// client end is dropped. That is the same wake vocabulary participant
/// publications, subscription inboxes, and reply deadlines already speak, so a
/// parked loopback connection wakes exactly as a parked socket connection does.
#[derive(Debug)]
pub(in super::super) struct LoopbackTransport {
    /// `None` once an orderly server-forced close has released the duplex —
    /// which also drops the client's peer ring, so the embedded caller learns
    /// about the close by the same end-of-file a socket peer would see.
    end: Option<LoopbackServerEnd>,
}

impl ConnectionTransport for LoopbackTransport {
    const MOUNT: MountKind = MountKind::Loopback;

    fn is_connected(&self) -> bool {
        self.end.is_some()
    }

    fn read_available(&mut self, buffer: &mut Vec<u8>) -> Result<ReadStatus, ServerError> {
        let Some(end) = self.end.as_mut() else {
            // Unreachable: the caller checks `is_connected` first. Kept total so
            // the read half binds without an unwrap.
            return Ok(ReadStatus::Closed);
        };
        let mut chunk = [0_u8; READ_BUFFER_BYTES];
        match end.read(&mut chunk) {
            // The client end was dropped and its ring is drained. This is the
            // hangup a socket reports as `Ok(0)`, and it takes the identical
            // branch: fold `ConnectionLost`, release, finish.
            Ok(0) => Ok(ReadStatus::Closed),
            Ok(bytes_read) => {
                buffer.extend_from_slice(chunk.get(..bytes_read).unwrap_or(&[]));
                Ok(ReadStatus::Read)
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                Ok(ReadStatus::WouldBlock)
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {
                Ok(ReadStatus::WouldBlock)
            }
            Err(error) => Err(ServerError::ListenerAccept {
                message: format!("failed to read loopback connection: {error}"),
            }),
        }
    }

    fn sink(&mut self) -> Option<&mut dyn Write> {
        self.end.as_mut().map(|end| end as &mut dyn Write)
    }

    fn probe_inbound(&self) -> Result<bool, ServerError> {
        self.end
            .as_ref()
            .map_or(Ok(true), InboundPending::inbound_pending)
    }

    /// Arms nothing.
    ///
    /// A loopback connection has no descriptor to register and needs none: its
    /// wake was installed on the first slice and its writer fires it. The
    /// `interest` a socket would arm is derived from the outbound drain, and the
    /// residue case it exists for cannot strand this transport — the client end
    /// draining its ring is itself a write into the duplex, which wakes this
    /// connection to try the residue again.
    fn arm_readiness(
        &mut self,
        _pid: u64,
        _ctx: &NativeContext<'_>,
        _interest: Interest,
        _runtime: &ConnectionRuntime,
    ) -> Result<(), ServerError> {
        Ok(())
    }

    /// Registers this connection's `READY` waker on the duplex.
    ///
    /// A duplex with no waker tells nobody, so a failure to build the waker is
    /// fatal rather than tolerated: a connection that can neither be told nor
    /// poll would park on live bytes forever. The waker is `None` only when the
    /// connection scheduler is gone or the host record is missing, and the
    /// caller has already established the record — so this refusal names a real
    /// teardown, not a routine absence.
    fn install_wake(&mut self, pid: u64, runtime: &ConnectionRuntime) -> Result<(), ServerError> {
        let Some(end) = self.end.as_ref() else {
            return Ok(());
        };
        let waker = runtime
            .ready_waker(pid)
            .ok_or_else(|| ServerError::ListenerAccept {
                message: format!(
                    "loopback connection {pid} has no READY waker; it could never be told \
                     about inbound bytes"
                ),
            })?;
        end.set_waker(Box::new(move || {
            waker.fire();
        }));
        Ok(())
    }

    fn release(&mut self) {
        self.end.take();
    }

    /// Nothing to observe: a loopback connection holds no descriptor, so the
    /// descriptor-allocator boundary the socket transport reports has no
    /// counterpart here.
    #[cfg(test)]
    fn note_process_drop(&mut self, _runtime: &ConnectionRuntime) {}
}

impl LoopbackConnectionProcess {
    /// Builds the loopback connection process the supervisor spawns.
    ///
    /// Takes the server end out of the spawn holder exactly once, the same
    /// interior-mutability handoff the socket and WebSocket paths use: the
    /// native handler factory is `Fn + Send + Sync` and may in principle be
    /// invoked more than once, so the transport cannot be moved into it. A
    /// poisoned holder yields no end, and the process then stops immediately
    /// through the ordinary missing-transport branch — loudly logged, never a
    /// mystery crash.
    pub(in super::super) fn from_loopback_holder(
        runtime: Arc<ConnectionRuntime>,
        holder: &Arc<std::sync::Mutex<Option<LoopbackServerEnd>>>,
        connection_incarnation: Option<ConnectionIncarnation>,
    ) -> Self {
        let end = match holder.lock() {
            Ok(mut held) => held.take(),
            Err(poisoned) => {
                tracing::error!(
                    error = %poisoned,
                    "loopback connection handoff failed: duplex holder mutex was poisoned; \
                     the connection process will start without a transport and stop immediately"
                );
                None
            }
        };
        // `peer_addr: None` is the loopback's honest description of itself: it
        // has no socket and therefore no address. Nothing on the admission path
        // reads a socket fact — identity is purely protocol-level — so the
        // absence is visible in diagnostics and invisible to semantics.
        Self::over_transport(
            runtime,
            None,
            LoopbackTransport { end },
            connection_incarnation,
        )
    }
}
