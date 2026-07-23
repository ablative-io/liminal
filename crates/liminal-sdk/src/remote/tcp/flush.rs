//! Flush surface types and the verdict ledger behind [`PushClient::flush`].
//!
//! The push connection's `publish` is fire-and-forget: bytes hit the socket and
//! the call returns. The server answers every publish on an ordinary channel
//! with a `Frame::PublishAck` or `Frame::PublishError`; before 0.4.0 the
//! background reader discarded both, so a caller could never learn a publish's
//! fate. This module holds the machinery that captures those verdicts and
//! returns them at an explicit [`PushClient::flush`]/[`PushClient::close`]:
//!
//! * the public outcome types ([`FlushOutcome`], [`FlushMode`],
//!   [`PublishRejection`]);
//! * the crate-internal [`FlushLedger`] that counts response-eliciting
//!   publishes, receives verdicts from the background reader, and drains them
//!   under the serial flush guard.
//!
//! Design of record: `docs/design/SDK-PUSH-FLUSH.md` r2 (torn). The decision
//! sockets referenced below («FLUSH-UNRESOLVED-DISCLOSURE» = T1,
//! «CONCURRENT-FLUSH-SERIAL» = T2, «FIFO-MINIMAL-CORRELATION-DEFERRED» = R1,
//! «RAW-REASON-NO-MAPPING» = R4, «OBSERVABILITY-UNACKED» = D5) are rulings.
//!
//! [`PushClient::flush`]: super::PushClient::flush
//! [`PushClient::close`]: super::PushClient::close

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::time::Duration;

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::time::Instant;

use crate::SdkError;

/// Total wall-clock budget bounding one flush, in the spirit of the drop-time
/// `DROP_DRAIN_BUDGET`: a flush blocks on a deadline'd channel receive for the
/// outstanding verdicts and returns when they have all arrived or the budget
/// elapses — never an unbounded wait, never a poll loop (LAW-1).
pub const FLUSH_BUDGET: Duration = Duration::from_secs(5);

/// A single publish the server rejected, reported verbatim from the wire.
///
/// Carries the raw `Frame::PublishError` fields with **no mapping** onto the
/// SDK error taxonomy (ruling R4 «RAW-REASON-NO-MAPPING»): today the server
/// sets a blanket `reason_code` of `0xFFFF` for every publish failure, so a
/// schema mismatch is not wire-distinguishable from any other rejection —
/// fabricating a typed error from the message string would be a lie wearing a
/// type. Only the `message` text differs per failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishRejection {
    /// The server's numeric reason, verbatim. Today always `0xFFFF`.
    reason_code: u16,
    /// The server's human-readable detail (carries the schema-mismatch text).
    message: Option<String>,
}

impl PublishRejection {
    /// Builds a rejection from the raw `PublishError` wire fields.
    pub(crate) const fn new(reason_code: u16, message: Option<String>) -> Self {
        Self {
            reason_code,
            message,
        }
    }

    /// The server's numeric reason code, verbatim from the wire.
    #[must_use]
    pub const fn reason_code(&self) -> u16 {
        self.reason_code
    }

    /// The server's human-readable rejection detail, verbatim from the wire.
    #[must_use]
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }
}

/// What a [`FlushOutcome`]'s collection actually did to the socket, disclosed
/// so a degraded close is never silent (ruling R2 «SHARED-SOCKET-VERDICT-ONLY»).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FlushMode {
    /// Sole owner at [`PushClient::close`]: acks drained AND the write half was
    /// FIN'd, so the server saw a graceful half-close.
    ///
    /// [`PushClient::close`]: super::PushClient::close
    FlushedAndHalfClosed,
    /// Verdicts collected, no FIN. Every plain [`PushClient::flush`] reports
    /// this mode (a flush leaves the connection fully usable), as does a
    /// [`PushClient::close`] degraded by a live [`PushWriter`] clone still
    /// sharing the socket — a write-half `shutdown` would break the clone's
    /// publishes, so close MUST NOT half-close and says so here instead of
    /// degrading silently.
    ///
    /// [`PushClient::flush`]: super::PushClient::flush
    /// [`PushClient::close`]: super::PushClient::close
    /// [`PushWriter`]: super::PushWriter
    VerdictOnly,
}

/// The typed result of a [`PushClient::flush`] or [`PushClient::close`].
///
/// T1 «FLUSH-UNRESOLVED-DISCLOSURE» invariant: **`failures.is_empty() &&
/// unresolved == 0` is the ONLY shape that reads as proven-accepted** (see
/// [`FlushOutcome::is_proven_accepted`]). Budget expiry with unresolved
/// publishes is a NORMAL outcome the caller must inspect, never an `Err`; the
/// outer `Err` is reserved for failures of the flush mechanism itself.
///
/// [`PushClient::flush`]: super::PushClient::flush
/// [`PushClient::close`]: super::PushClient::close
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlushOutcome {
    /// Per-publish rejections observed among the flushed publishes, in wire
    /// order (D4 «FIFO-VERDICT-NO-CORRELATION»: there is no correlation id on
    /// the wire, so order is the only binding).
    failures: Vec<PublishRejection>,
    /// Flushed publishes still unresolved when the budget expired — neither
    /// acked nor rejected. A NORMAL outcome the caller MUST inspect, not an
    /// error: a nonzero `unresolved` means those publishes' fate is
    /// connection-indeterminate.
    unresolved: usize,
    /// Whether this call also half-closed the socket or only collected
    /// verdicts.
    mode: FlushMode,
}

impl FlushOutcome {
    /// Assembles an outcome from a drained verdict window.
    pub(crate) const fn new(
        failures: Vec<PublishRejection>,
        unresolved: usize,
        mode: FlushMode,
    ) -> Self {
        Self {
            failures,
            unresolved,
            mode,
        }
    }

    /// Per-publish rejections among the flushed publishes, in wire order.
    #[must_use]
    pub fn failures(&self) -> &[PublishRejection] {
        &self.failures
    }

    /// Consumes the outcome, returning the owned rejections in wire order.
    #[must_use]
    pub fn into_failures(self) -> Vec<PublishRejection> {
        self.failures
    }

    /// Flushed publishes whose verdict never arrived inside the budget.
    #[must_use]
    pub const fn unresolved(&self) -> usize {
        self.unresolved
    }

    /// Whether the call half-closed the socket or only collected verdicts.
    #[must_use]
    pub const fn mode(&self) -> FlushMode {
        self.mode
    }

    /// `true` ONLY when every flushed publish was proven accepted by the
    /// server: no failures AND no unresolved publishes (the T1
    /// «FLUSH-UNRESOLVED-DISCLOSURE» invariant). Any other shape is the
    /// caller's to inspect.
    #[must_use]
    pub fn is_proven_accepted(&self) -> bool {
        self.failures.is_empty() && self.unresolved == 0
    }
}

/// One server response to a response-eliciting publish, forwarded by the
/// background reader in wire order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublishVerdict {
    /// A `Frame::PublishAck`: the server accepted the publish.
    Accepted,
    /// A `Frame::PublishError`: the server rejected the publish.
    Rejected(PublishRejection),
}

/// Verdict receiver plus the resolution cursor, owned by the flush guard so no
/// two flushes ever consume from the FIFO response sequence at once (T2).
struct VerdictInbox {
    /// Wire-ordered verdicts forwarded by the background reader.
    verdicts: Receiver<PublishVerdict>,
    /// Response-eliciting publishes already resolved by earlier flushes.
    resolved: u64,
}

/// Shared accounting between the publish writers, the background reader, and
/// `flush()`/`close()`.
///
/// * `written` counts response-eliciting publishes whose bytes reached the
///   socket (incremented under the writer lock, so the count follows wire
///   order); reserved observability-channel publishes are never counted — the
///   server answers them with no frame by design (D5 «OBSERVABILITY-UNACKED»).
/// * `arrived` counts publish responses the reader has captured.
/// * `inbox` is the T2 serial flush guard: a second concurrent flush WAITS on
///   this mutex (a blocking lock, not a poll) and then covers only its own
///   write-boundary.
#[derive(Debug)]
pub struct FlushLedger {
    /// Response-eliciting publishes written to the socket, ever.
    written: AtomicU64,
    /// Publish responses (`PublishAck`/`PublishError`) captured by the reader,
    /// ever. `arrived > written` can only mean the elicits-response
    /// classification broke — the R1 fail-loud tripwire.
    arrived: AtomicU64,
    /// Serial flush guard owning the verdict receiver and resolution cursor.
    inbox: Mutex<VerdictInbox>,
}

impl core::fmt::Debug for VerdictInbox {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("VerdictInbox")
            .field("resolved", &self.resolved)
            .finish_non_exhaustive()
    }
}

impl FlushLedger {
    /// Builds the ledger and the sender half the background reader forwards
    /// verdicts on.
    pub(crate) fn new() -> (Self, Sender<PublishVerdict>) {
        let (sender, verdicts) = channel();
        let ledger = Self {
            written: AtomicU64::new(0),
            arrived: AtomicU64::new(0),
            inbox: Mutex::new(VerdictInbox {
                verdicts,
                resolved: 0,
            }),
        };
        (ledger, sender)
    }

    /// Records one response-eliciting publish written to the socket. Called
    /// under the writer lock so the count follows wire order.
    pub(crate) fn record_written(&self) {
        self.written.fetch_add(1, Ordering::SeqCst);
    }

    /// Records one publish response captured by the background reader.
    pub(crate) fn record_arrival(&self) {
        self.arrived.fetch_add(1, Ordering::SeqCst);
    }

    /// Drains verdicts for every response-eliciting publish written before
    /// this call, bounded by `budget`.
    ///
    /// Returns the wire-ordered rejections plus the count of publishes still
    /// unresolved at budget expiry (T1: a normal outcome, not an error).
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Connection`] when the flush guard is poisoned, and
    /// [`SdkError::Protocol`] when more publish responses arrived than
    /// response-eliciting publishes were written — a broken pairing invariant;
    /// per R1 the flush fails loudly rather than ever mispairing a verdict.
    pub(crate) fn drain(
        &self,
        budget: Duration,
    ) -> Result<(Vec<PublishRejection>, usize), SdkError> {
        // Snapshot the write boundary BEFORE waiting on the guard (T2): a
        // flush that waited behind another covers the publishes written before
        // *it* was called, minus those the earlier flush already resolved.
        let written_at_call = self.written.load(Ordering::SeqCst);
        let mut inbox = self.inbox.lock().map_err(|error| SdkError::Connection {
            description: format!("flush guard poisoned: {error}"),
        })?;
        self.check_pairing_invariant()?;
        let window = written_at_call.saturating_sub(inbox.resolved);
        let deadline = Instant::now() + budget;
        let mut failures = Vec::new();
        let mut collected: u64 = 0;
        while collected < window {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            match inbox.verdicts.recv_timeout(deadline.duration_since(now)) {
                Ok(verdict) => {
                    collected += 1;
                    inbox.resolved += 1;
                    if let PublishVerdict::Rejected(rejection) = verdict {
                        failures.push(rejection);
                    }
                }
                // Timeout: the budget elapsed with verdicts still outstanding.
                // Disconnected: the reader ended (connection gone), so the
                // outstanding verdicts can never arrive. Both leave the
                // remainder connection-indeterminate — counted as unresolved,
                // never an `Err` (T1).
                Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => break,
            }
        }
        // Release the serial guard before the final invariant check (which
        // reads only the atomics): a waiting flush may proceed immediately.
        drop(inbox);
        self.check_pairing_invariant()?;
        let unresolved = usize::try_from(window.saturating_sub(collected)).map_err(|error| {
            SdkError::Protocol {
                description: format!("unresolved publish count overflowed usize: {error}"),
            }
        })?;
        Ok((failures, unresolved))
    }

    /// R1 fail-loud tripwire: more publish responses than response-eliciting
    /// publishes means the classification (and therefore any FIFO pairing) is
    /// broken — return a typed mechanism error, never mispair.
    fn check_pairing_invariant(&self) -> Result<(), SdkError> {
        let arrived = self.arrived.load(Ordering::SeqCst);
        let written = self.written.load(Ordering::SeqCst);
        if arrived > written {
            return Err(SdkError::Protocol {
                description: format!(
                    "publish response-count mismatch: {arrived} publish responses arrived for \
                     {written} response-eliciting publishes; refusing to pair verdicts"
                ),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString;
    use alloc::vec;

    /// A short budget so expiry pins stay brisk; the drain is a deadline'd
    /// blocking receive, so the pin's duration IS the budget.
    const SHORT_BUDGET: Duration = Duration::from_millis(50);

    /// T1 pin (unit leg): budget expiry with outstanding publishes is a NORMAL
    /// outcome — unresolved counted, empty failures, `Ok` — and its shape is
    /// distinguishable from proven-accepted.
    #[test]
    fn budget_expiry_counts_unresolved_and_is_not_an_error() -> Result<(), SdkError> {
        let (ledger, _sender) = FlushLedger::new();
        ledger.record_written();
        ledger.record_written();
        ledger.record_written();
        let (failures, unresolved) = ledger.drain(SHORT_BUDGET)?;
        assert!(failures.is_empty());
        assert_eq!(unresolved, 3);
        let expired = FlushOutcome::new(failures, unresolved, FlushMode::VerdictOnly);
        assert!(!expired.is_proven_accepted());
        let clean = FlushOutcome::new(Vec::new(), 0, FlushMode::VerdictOnly);
        assert!(clean.is_proven_accepted());
        Ok(())
    }

    /// R1 pin (unit leg): a verdict with no response-eliciting publish written
    /// is a broken pairing invariant — a typed mechanism `Err`, not a
    /// per-publish failure.
    #[test]
    fn surplus_verdict_is_a_typed_mechanism_error() {
        let (ledger, sender) = FlushLedger::new();
        assert!(sender.send(PublishVerdict::Accepted).is_ok());
        ledger.record_arrival();
        let result = ledger.drain(SHORT_BUDGET);
        assert!(matches!(result, Err(SdkError::Protocol { .. })));
    }

    /// D4/R4 pin (unit leg): rejections surface in wire order, verbatim.
    #[test]
    fn rejections_surface_in_wire_order_verbatim() -> Result<(), SdkError> {
        let (ledger, sender) = FlushLedger::new();
        let first = PublishRejection::new(0xFFFF, Some("first".to_string()));
        let second = PublishRejection::new(0xFFFF, None);
        for verdict in [
            PublishVerdict::Rejected(first.clone()),
            PublishVerdict::Accepted,
            PublishVerdict::Rejected(second.clone()),
        ] {
            ledger.record_written();
            ledger.record_arrival();
            assert!(sender.send(verdict).is_ok());
        }
        let (failures, unresolved) = ledger.drain(SHORT_BUDGET)?;
        assert_eq!(failures, vec![first, second]);
        assert_eq!(unresolved, 0);
        Ok(())
    }

    /// T2 pin: two threads drain concurrently. The flush guard serializes
    /// them (a mutex wait, not a poll), no verdict is misattributed or
    /// double-consumed, and whichever drain proceeds second covers only its
    /// own write-boundary (already fully resolved here, so a zero window).
    ///
    /// This pin drives the ledger directly because the hazard lives here: the
    /// guard is the single reader of the FIFO verdict sequence for every
    /// `flush()`/`close()` above it.
    #[test]
    fn concurrent_drains_serialize_without_misattribution() -> Result<(), SdkError> {
        use std::sync::{Arc, Barrier};
        let (ledger, sender) = FlushLedger::new();
        let ledger = Arc::new(ledger);
        let rejection = PublishRejection::new(0xFFFF, Some("boom".to_string()));
        for verdict in [
            PublishVerdict::Accepted,
            PublishVerdict::Accepted,
            PublishVerdict::Rejected(rejection.clone()),
            PublishVerdict::Accepted,
        ] {
            ledger.record_written();
            ledger.record_arrival();
            assert!(sender.send(verdict).is_ok());
        }
        let barrier = Arc::new(Barrier::new(2));
        let spawn_drain = |ledger: Arc<FlushLedger>, barrier: Arc<Barrier>| {
            std::thread::spawn(move || {
                barrier.wait();
                ledger.drain(FLUSH_BUDGET)
            })
        };
        let first = spawn_drain(Arc::clone(&ledger), Arc::clone(&barrier));
        let second = spawn_drain(Arc::clone(&ledger), barrier);
        let joined =
            |handle: std::thread::JoinHandle<Result<(Vec<PublishRejection>, usize), SdkError>>| {
                handle.join().map_err(|_| SdkError::Protocol {
                    description: "drain thread panicked".to_string(),
                })
            };
        let (failures_a, unresolved_a) = joined(first)??;
        let (failures_b, unresolved_b) = joined(second)??;
        // Exactly one drain consumed the four-verdict window (the guard
        // winner); the other covered its already-resolved zero window. The one
        // rejection surfaces exactly once, never split or duplicated.
        let combined: Vec<PublishRejection> = failures_a.into_iter().chain(failures_b).collect();
        assert_eq!(combined, vec![rejection]);
        assert_eq!(unresolved_a, 0);
        assert_eq!(unresolved_b, 0);
        Ok(())
    }

    /// T2 boundary math (unit leg): a drain after everything was resolved
    /// covers a zero window and returns immediately clean.
    #[test]
    fn second_drain_covers_only_its_own_boundary() -> Result<(), SdkError> {
        let (ledger, sender) = FlushLedger::new();
        ledger.record_written();
        ledger.record_arrival();
        assert!(sender.send(PublishVerdict::Accepted).is_ok());
        let (_, unresolved) = ledger.drain(SHORT_BUDGET)?;
        assert_eq!(unresolved, 0);
        let started = Instant::now();
        let (failures, unresolved) = ledger.drain(FLUSH_BUDGET)?;
        assert!(failures.is_empty());
        assert_eq!(unresolved, 0);
        // A zero window never waits on the budget: the guard is the only wait.
        assert!(started.elapsed() < FLUSH_BUDGET);
        Ok(())
    }
}
