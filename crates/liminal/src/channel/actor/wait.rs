//! Host-side reply waiting with dead-actor fast-fail (LIM-002 MINOR-2).
//!
//! A command's reply [`mpsc::SyncSender`] is moved into the actor's command
//! queue, which OUTLIVES the beamr process. So an actor that dies after a
//! command is enqueued never drops the sender, and a plain `recv_timeout` would
//! block the full [`COMMAND_TIMEOUT`]. These helpers poll the scheduler's
//! process table while waiting, so a command targeting a dead actor returns
//! promptly with a clear error instead of stalling.

use std::sync::mpsc;
use std::time::{Duration, Instant};

use beamr::scheduler::Scheduler;

use crate::channel::schema::{SchemaId, SchemaValidationError};
use crate::error::LiminalError;

/// Default bound on how long a handle blocks for a command reply.
pub(super) const COMMAND_TIMEOUT: Duration = Duration::from_secs(5);

/// How often a blocked command re-checks that its target actor is still alive,
/// so a command waiting on a process that dies after enqueue returns promptly
/// instead of stalling the full [`COMMAND_TIMEOUT`].
const LIVENESS_POLL: Duration = Duration::from_millis(10);

/// Why a reply wait ended without the actor replying.
enum WaitFailure {
    /// The reply channel disconnected before a reply arrived.
    Disconnected,
    /// The target actor left the process table before replying.
    Dead,
    /// The full command timeout elapsed with the actor still live.
    TimedOut,
}

/// Block for a command reply, failing FAST when actor `pid` leaves the process
/// table. Surfaces a [`LiminalError`] for the standard command reply path.
pub(super) fn wait_live<T>(
    scheduler: &Scheduler,
    response: &mpsc::Receiver<Result<T, LiminalError>>,
    pid: u64,
) -> Result<Result<T, LiminalError>, LiminalError> {
    poll_reply(scheduler, response, pid).map_err(|failure| match failure {
        WaitFailure::Dead => LiminalError::DeliveryFailed {
            message: format!("channel actor pid {pid} died before replying"),
        },
        WaitFailure::Disconnected => LiminalError::DeliveryFailed {
            message: "channel command reply channel disconnected".to_owned(),
        },
        WaitFailure::TimedOut => LiminalError::DeliveryFailed {
            message: "channel command timed out".to_owned(),
        },
    })
}

/// Schema-evolution variant of [`wait_live`]: same dead-actor fast-fail, but
/// surfaces a [`SchemaValidationError`] for the schema-typed reply path.
pub(super) fn wait_schema_live(
    scheduler: &Scheduler,
    response: &mpsc::Receiver<Result<SchemaId, SchemaValidationError>>,
    pid: u64,
) -> Result<Result<SchemaId, SchemaValidationError>, SchemaValidationError> {
    poll_reply(scheduler, response, pid).map_err(|failure| match failure {
        WaitFailure::Dead => SchemaValidationError::InvalidSchema {
            message: format!("channel actor pid {pid} died before evolving the schema"),
        },
        WaitFailure::Disconnected | WaitFailure::TimedOut => SchemaValidationError::InvalidSchema {
            message: "channel schema-evolution reply unavailable".to_owned(),
        },
    })
}

/// Block for a reply of payload type `R`, polling actor liveness so a command
/// targeting a dead actor (whose reply sender outlives it in the queue) returns
/// promptly instead of stalling the full [`COMMAND_TIMEOUT`].
fn poll_reply<R>(
    scheduler: &Scheduler,
    response: &mpsc::Receiver<R>,
    pid: u64,
) -> Result<R, WaitFailure> {
    let deadline = Instant::now() + COMMAND_TIMEOUT;
    loop {
        match response.recv_timeout(LIVENESS_POLL) {
            Ok(reply) => return Ok(reply),
            Err(mpsc::RecvTimeoutError::Disconnected) => return Err(WaitFailure::Disconnected),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if scheduler.process_table().get(pid).is_none() {
                    return Err(WaitFailure::Dead);
                }
                if Instant::now() >= deadline {
                    return Err(WaitFailure::TimedOut);
                }
            }
        }
    }
}
