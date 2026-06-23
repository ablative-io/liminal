use std::sync::{Mutex, MutexGuard, mpsc};

use crate::error::LiminalError;

pub(super) fn send_reply<T>(
    reply: &mpsc::SyncSender<Result<T, LiminalError>>,
    result: Result<T, LiminalError>,
) {
    match reply.send(result) {
        Ok(()) | Err(_) => {}
    }
}

pub(super) fn wait_for<T>(
    response: &mpsc::Receiver<Result<T, LiminalError>>,
    operation: &str,
) -> Result<T, LiminalError> {
    response
        .recv()
        .map_err(|error| LiminalError::ConversationFailed {
            message: format!("{operation} response channel closed: {error}"),
        })?
}

/// Waits up to `timeout` for a reply. A timeout maps to
/// [`LiminalError::ConversationTimeout`]; a closed channel (e.g. the actor shut
/// down) maps to [`LiminalError::ConversationFailed`].
pub(super) fn wait_for_timeout<T>(
    response: &mpsc::Receiver<Result<T, LiminalError>>,
    operation: &str,
    timeout: std::time::Duration,
) -> Result<T, LiminalError> {
    match response.recv_timeout(timeout) {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => Err(LiminalError::ConversationTimeout {
            message: format!("{operation} timed out after {timeout:?}"),
        }),
        Err(mpsc::RecvTimeoutError::Disconnected) => Err(LiminalError::ConversationFailed {
            message: format!("{operation} response channel closed"),
        }),
    }
}

pub(super) fn lock<'a, T>(
    mutex: &'a Mutex<T>,
    name: &str,
) -> Result<MutexGuard<'a, T>, LiminalError> {
    mutex
        .lock()
        .map_err(|error| LiminalError::ConversationFailed {
            message: format!("{name} lock poisoned: {error}"),
        })
}
