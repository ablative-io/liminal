//! Test-only exact W2 work observation points for idle-bound fixtures.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct ObligationDispatchWorkCounters {
    pub selector_calls: AtomicU64,
    pub authority_lock_acquisitions: AtomicU64,
    pub outbox_delivery_probes: AtomicU64,
    pub publication_allocations: AtomicU64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObligationDispatchWorkSnapshot {
    pub selector_calls: u64,
    pub authority_lock_acquisitions: u64,
    pub outbox_delivery_probes: u64,
    pub publication_allocations: u64,
}

impl ObligationDispatchWorkCounters {
    pub fn snapshot(&self) -> ObligationDispatchWorkSnapshot {
        ObligationDispatchWorkSnapshot {
            selector_calls: self.selector_calls.load(Ordering::SeqCst),
            authority_lock_acquisitions: self.authority_lock_acquisitions.load(Ordering::SeqCst),
            outbox_delivery_probes: self.outbox_delivery_probes.load(Ordering::SeqCst),
            publication_allocations: self.publication_allocations.load(Ordering::SeqCst),
        }
    }
}
