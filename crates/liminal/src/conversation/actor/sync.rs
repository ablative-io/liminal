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
