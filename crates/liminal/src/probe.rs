//! WORKTREE-ONLY measurement clock for the ws-parked-delivery lane.
//!
//! MEASUREMENT INSTRUMENT, NOT FOR LANDING.
//!
//! One monotonic origin shared by every crate in the workspace, so a probe line
//! emitted by the channel actor and a probe line emitted by a connection process
//! carry timestamps that can be subtracted. `std::time::Instant` has no absolute
//! zero, so a per-crate origin would make cross-crate deltas meaningless; this
//! module fixes ONE origin at first touch and hands out microseconds since it.
//!
//! [`enabled`] is the same `LIMINAL_WS_PROBE` gate the rest of the lane's probes
//! use, cached so a per-envelope call is an atomic load rather than an
//! environment lookup — the burst arms enqueue hundreds of envelopes per
//! iteration and the probe must not itself decide the race it is measuring.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Instant;

static ORIGIN: OnceLock<Instant> = OnceLock::new();
static ENABLED: AtomicU8 = AtomicU8::new(0);
static ENVELOPE_ENABLED: AtomicU8 = AtomicU8::new(0);

fn gate(cell: &AtomicU8, variable: &str) -> bool {
    match cell.load(Ordering::Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = std::env::var_os(variable).is_some();
            cell.store(if on { 2 } else { 1 }, Ordering::Relaxed);
            on
        }
    }
}

/// Whether the lane's SLICE-level probes are switched on for this process.
#[must_use]
pub fn enabled() -> bool {
    gate(&ENABLED, "LIMINAL_WS_PROBE")
}

/// Whether the lane's PER-ENVELOPE probes are switched on.
///
/// Separate from [`enabled`] because they are not the same instrument. A burst
/// arm moves hundreds of envelopes per iteration, and one `eprintln` per
/// envelope per subscriber costs enough wall-clock inside the connection's own
/// slice to CHANGE the race it is trying to measure — the first timeline run
/// with these on pushed the websocket subscriber from 64 delivered down to 32,
/// and starved the burst publisher's ack reader past its window. Slice-level
/// timing runs with this OFF; the per-envelope detail is a second, separate
/// pass whose numbers are read as ordering, never as timing.
#[must_use]
pub fn envelope_enabled() -> bool {
    gate(&ENVELOPE_ENABLED, "LIMINAL_WS_PROBE_ENVELOPE")
}

/// Microseconds since this process's shared probe origin.
#[must_use]
pub fn micros() -> u128 {
    ORIGIN.get_or_init(Instant::now).elapsed().as_micros()
}
