//! R6 connection READY-wake vocabulary.
//!
//! One `READY` atom per connection. Any marker (or N coalesced) triggers exactly
//! one full slice that services ALL sources — inbound socket, controls,
//! subscriptions, and (once R1(vi) lands) pending replies. Under the current busy
//! loop the connection already runs every slice, so a READY marker is redundant
//! and structurally harmless (coalescing and duplicates cannot double-apply work,
//! because the handler drains its whole mailbox before running one slice). The
//! marker exists so the PARK-FLIP changes nothing about wake semantics: when the
//! connection parks (`NativeOutcome::Wait`), a READY marker is exactly what wakes
//! it, and every wake source (subscription inbox — R3, reply availability —
//! R1(vi), reply-deadline expiry, control traffic) speaks this one vocabulary.
//!
//! A [`ReadyWaker`] is the connection scheduler's enqueue handle, captured at
//! notifier-install time. It is normative (§1.2(2), Vesper advisory 3) that a
//! notifier fire the CONNECTION scheduler's enqueue — never the firing caller's
//! ambient scheduler: the channel actor and the conversation core run on their own
//! schedulers' slices, and an enqueue routed to the wrong scheduler is a silently
//! lost wake. Capturing the connection scheduler handle here, once, at install,
//! is what closes that hazard.

use std::sync::Weak;

use beamr::atom::Atom;
use beamr::scheduler::Scheduler;

/// A cheap, cloneable, non-blocking handle that delivers the connection's `READY`
/// marker to its owning beamr pid on the CONNECTION scheduler.
///
/// Held by every wake source's notifier slot (the subscription inbox — R3, the
/// reply-availability notifier — R1(vi)). Firing is safe on any actor's slice: it
/// is a single [`Scheduler::enqueue_atom_message`], which the beamr 0.13 contract
/// (§src `scheduler/mod.rs`) documents as a host-to-process wake primitive that
/// merges the atom into the target mailbox and wakes a parked process, preserving
/// the execute-to-wait race. The scheduler is held [`Weak`] so a waker never keeps
/// the scheduler (and through it every connection process) alive — a cycle would
/// leak the whole connection scheduler. A fire after the scheduler is gone, or to
/// a dead pid, is a no-op: the connection is already being torn down, so a lost
/// wake for it is correct, not a defect (R5 — stale markers are discarded).
#[derive(Clone)]
pub struct ReadyWaker {
    scheduler: Weak<Scheduler>,
    pid: u64,
    ready_atom: Atom,
    #[cfg(test)]
    fire_probe: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
}

impl std::fmt::Debug for ReadyWaker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReadyWaker")
            .field("pid", &self.pid)
            .field("scheduler_live", &(self.scheduler.strong_count() > 0))
            .finish_non_exhaustive()
    }
}

impl ReadyWaker {
    /// Captures the connection scheduler's enqueue handle for `pid`. Called at
    /// notifier-install time on the connection's own slice, so the handle is the
    /// connection scheduler's, not the firing caller's ambient one (§1.2(2)).
    pub(crate) fn new(scheduler: &std::sync::Arc<Scheduler>, pid: u64, ready_atom: Atom) -> Self {
        Self {
            scheduler: std::sync::Arc::downgrade(scheduler),
            pid,
            ready_atom,
            #[cfg(test)]
            fire_probe: None,
        }
    }

    /// Creates an event-counting waker without a scheduler for readiness tests.
    #[cfg(test)]
    pub(crate) const fn for_test(probe: std::sync::Arc<std::sync::atomic::AtomicU64>) -> Self {
        Self {
            scheduler: Weak::new(),
            pid: 0,
            ready_atom: Atom::OK,
            fire_probe: Some(probe),
        }
    }

    /// Delivers one `READY` marker to the connection's pid, waking it if parked.
    ///
    /// Returns whether the marker was delivered (`false` when the scheduler is
    /// gone or the pid is no longer live — a benign discard on a connection that
    /// is already tearing down). Idempotent by construction: N fires coalesce to
    /// at most N mailbox atoms, all drained before one slice runs, so duplicates
    /// never double-apply work (R6).
    pub(crate) fn fire(&self) -> bool {
        #[cfg(test)]
        if let Some(probe) = &self.fire_probe {
            probe.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            return true;
        }
        let Some(scheduler) = self.scheduler.upgrade() else {
            return false;
        };
        scheduler.enqueue_atom_message(self.pid, self.ready_atom)
    }
}
