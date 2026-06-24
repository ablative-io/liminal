//! SRV-005: the cluster observer seam.
//!
//! Clustering lives in `liminal-server`, but it must learn about three channel
//! events that only the library can see at first hand: a subscriber joining, a
//! subscriber leaving, and a message being published. Rather than leak the
//! cluster's distribution types into the library (which would violate the
//! embedded-first boundary), the library exposes this thin observer trait. The
//! server installs a single observer on the shared [`ChannelSupervisor`]; the
//! [`ChannelHandle`](crate::channel::ChannelHandle) calls it host-side after the
//! corresponding channel-actor operation succeeds.
//!
//! The library itself never implements this trait beyond a no-op default — it
//! adds NO clustering behaviour. The observer is the entire contract between the
//! channel layer and the cluster `sync` module.

use crate::envelope::Envelope;

/// Receives channel lifecycle events so an out-of-library clusterer can map them
/// onto distributed process-group membership and cross-node fan-out.
///
/// Every method is called from the host thread that drove the originating
/// [`ChannelHandle`](crate::channel::ChannelHandle) operation, AFTER the channel
/// actor has applied it. Implementations must be cheap and non-blocking on the
/// publish path; a slow observer would stall the publishing caller.
pub trait ClusterObserver: Send + Sync + std::fmt::Debug {
    /// A local subscriber (beamr pid `subscriber_pid`) joined `channel`.
    fn on_subscribe(&self, channel: &str, subscriber_pid: u64);

    /// A local subscriber (beamr pid `subscriber_pid`) left `channel`.
    fn on_unsubscribe(&self, channel: &str, subscriber_pid: u64);

    /// A message was published to `channel` on this node; the observer may fan it
    /// out to remote subscribers.
    fn on_publish(&self, channel: &str, envelope: &Envelope);
}
