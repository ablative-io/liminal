//! Synchronous bridge for driving durable-store futures to completion.
//!
//! # Why a hand-rolled executor and why it cannot deadlock
//!
//! The runtime channel API ([`crate::channel::ChannelHandle::publish`] and
//! [`crate::channel::ChannelHandle::flush`]) is synchronous: it is called from a
//! beamr scheduler worker thread that drives a connection process. The durable
//! storage surface ([`crate::durability::DurableStore`]) is `async`. This helper
//! polls a single store future to completion on the *calling* thread without
//! spawning a runtime or a thread.
//!
//! It is deadlock-free **because the current store backend is synchronous
//! underneath**: the vendored haematite [`crate::durability::HaematiteStore`]
//! completes every `append`/`cas`/`read`/`flush` under an in-memory lock and
//! returns `Poll::Ready` on the first poll — there is no external I/O await, no
//! cross-task channel, and nothing that hands control to another task that would
//! need *this* thread to make progress. Concretely:
//!
//! * No runtime is created, so no other task contends for an executor.
//! * No thread is spawned and no lock is held across the poll by this helper, so
//!   it cannot form a wait cycle with another thread.
//! * The future returns immediately, so the calling scheduler worker is not
//!   blocked for any meaningful time.
//!
//! If a future-yielding, real-I/O store backend is ever introduced (the
//! downstream mock -> on-disk-haematite swap), this synchronous bridge must be
//! replaced with a real executor on a dedicated I/O thread.
//!
//! # Bounded, loud failure on a suspending future
//!
//! A synchronous-underneath future returns `Poll::Ready` on the FIRST poll. To
//! make the unreachable suspending-backend case fail loudly instead of spinning
//! forever, the poll loop is **bounded** to [`MAX_POLLS`] (a tiny margin above
//! the single poll a correct future needs). If the future is still `Pending`
//! after that bound, [`block_on`] returns [`BridgeError::DidNotComplete`] rather
//! than busy-yielding indefinitely: a still-pending future here is a contract
//! violation (a suspending `DurableStore` backend was wired without swapping in
//! a real executor), and a loud, bounded error is far safer than a silent
//! worker-starving hang. Production callers surface that error up their normal
//! error path (no panic in production code). The happy path is unchanged:
//! `Ready` on the first poll returns immediately with zero extra overhead.
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

/// Maximum number of polls [`block_on`] performs before failing loudly.
///
/// A synchronous-underneath store future completes in exactly one poll; the
/// margin tolerates a trivially-chained adapter future while still bounding the
/// loop so a genuinely-suspending future fails fast instead of spinning.
const MAX_POLLS: usize = 8;

/// Failure raised when a future driven by [`block_on`] does not complete
/// synchronously within the bounded poll budget.
#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    /// The future was still `Poll::Pending` after [`MAX_POLLS`] polls.
    #[error(
        "durable bridge future did not complete synchronously after {polls} polls; \
         a suspending DurableStore backend requires a real executor on a dedicated I/O \
         thread — see bridge module docs"
    )]
    DidNotComplete {
        /// Number of polls performed before giving up.
        polls: usize,
    },
}

/// Drives `future` to completion on the calling thread.
///
/// See the module documentation for the deadlock-freedom contract: this is only
/// sound for store backends whose futures complete synchronously without
/// awaiting external I/O.
///
/// # Errors
///
/// Returns [`BridgeError::DidNotComplete`] if `future` is still `Poll::Pending`
/// after [`MAX_POLLS`] polls. This never happens for a synchronous-underneath
/// store backend (which completes on the first poll); it signals that a
/// suspending backend was introduced and requires a real executor on a
/// dedicated I/O thread (see the module docs).
pub fn block_on<F: Future>(future: F) -> Result<F::Output, BridgeError> {
    let waker = Waker::from(Arc::new(NoopWaker));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    for _ in 0..MAX_POLLS {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return Ok(output);
        }
        std::thread::yield_now();
    }
    Err(BridgeError::DidNotComplete { polls: MAX_POLLS })
}

struct NoopWaker;

impl Wake for NoopWaker {
    fn wake(self: Arc<Self>) {}
}

#[cfg(test)]
mod tests {
    use std::future::{pending, ready};

    use super::{BridgeError, block_on};

    #[test]
    fn block_on_returns_value_from_ready_future() -> Result<(), BridgeError> {
        assert_eq!(block_on(ready(42_u32))?, 42);
        Ok(())
    }

    #[test]
    fn block_on_fails_loudly_and_fast_on_always_pending_future() {
        // `pending()` never resolves; the bound guarantees this terminates fast
        // with the loud diagnostic instead of hanging.
        let result = block_on(pending::<()>());
        assert!(matches!(
            result,
            Err(BridgeError::DidNotComplete { polls }) if polls == super::MAX_POLLS
        ));
    }
}
