//! Connection-keyed notifier hook for worker registration lifecycle.
//!
//! This is the application seam for self-describing worker registration. When a
//! worker sends a [`Frame::WorkerRegister`](liminal::protocol::Frame) over its
//! established connection, the server associates the registration with the
//! connection's beamr process id and invokes the configured
//! [`ConnectionNotifier`]; on connection close it invokes the matching
//! deregistration. The notifier is connection-keyed (by pid), which is distinct
//! from the subject-keyed responder registry in [`super::services`].
//!
//! Keeping the hook a `liminal-server` trait — rather than a liminal-core
//! concern — preserves liminal's generality: liminal still runs standalone with
//! no notifier configured, and the application (aion, in Stage 2) plugs its
//! registry in without liminal depending on it.

use liminal::protocol::WorkerRegistration;

use crate::ServerError;

/// Application hook invoked when a worker registers or unregisters on a
/// connection.
///
/// Implementations associate the connection's beamr process id (`pid`) with the
/// worker's declared [`WorkerRegistration`] so the application can route work to
/// it, and release that association on disconnect. The hook is synchronous: a
/// registration is acknowledged to the worker only after
/// [`on_worker_registered`](Self::on_worker_registered) returns, so a rejecting
/// application surfaces a `Rejected` ack instead of leaving the worker silently
/// connected but never dispatched-to.
pub trait ConnectionNotifier: std::fmt::Debug + Send + Sync {
    /// Called when a worker registers on the connection identified by `pid`.
    ///
    /// Returning `Ok(())` accepts the registration (the worker receives an
    /// `Accepted` ack). Returning [`ServerError`] rejects it (the worker receives
    /// a `Rejected` ack carrying the error text), so a failed association never
    /// leaves the worker believing it is registered.
    ///
    /// # Errors
    /// Returns [`ServerError`] when the application declines the registration.
    fn on_worker_registered(
        &self,
        pid: u64,
        registration: &WorkerRegistration,
    ) -> Result<(), ServerError>;

    /// Called when the connection identified by `pid` — which had a stored
    /// registration — closes, so the application can release the association.
    ///
    /// Deregistration is best-effort and infallible from the connection's
    /// perspective: it runs on the close path where there is no peer to report an
    /// error to.
    fn on_worker_unregistered(&self, pid: u64);

    /// Called when the connection identified by `pid` publishes to `channel`,
    /// carrying the opaque envelope `payload`, BEFORE the normal channel fan-out.
    ///
    /// Returns `true` when the application CONSUMED the publish out-of-band (an
    /// observability-drain tap): the connection process then does NOT route it to the
    /// channel-fan-out cluster and answers with no wire response, so a tapped channel
    /// need not be a declared fan-out channel. Returns `false` (the default) to let
    /// the publish flow through the normal channel machinery unchanged.
    ///
    /// This is the observability-drain hook: a worker publishing an agent transcript
    /// event to the reserved observability channel is consumed here — the hosting
    /// application (aion) persists and live-fans-out the event without a second
    /// connection. It is fire-and-forget: a publish is a one-way notification, so
    /// there is no reply and a failed persist is the application's concern to log.
    ///
    /// The default returns `false`, so liminal still runs standalone: with no
    /// notifier, or a notifier that does not recognise the channel, every publish
    /// routes to the normal fan-out exactly as before.
    fn on_channel_publish(&self, pid: u64, channel: &str, payload: &[u8]) -> bool {
        let _ = (pid, channel, payload);
        false
    }
}
