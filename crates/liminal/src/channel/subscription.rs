//! LIM-002 R2/R3: subscriptions backed by real beamr processes.
//!
//! Each subscription owns a real, scheduler-supervised beamr native process
//! (a [`SubscriberProcess`]) plus the in-memory inbox the channel actor delivers
//! matching envelopes into. The channel actor LINKS to this process's pid on
//! `Subscribe`; when the [`SubscriptionHandle`] is dropped (or the caller
//! unsubscribes) the process is terminated, the link fires an `{EXIT, pid, _}`
//! signal, and the trapping channel actor removes the dead subscriber from its
//! fan-out list. There is NO weak-Arc polling: liveness is observed structurally
//! through the beamr link/EXIT path, exactly as the conversation actor observes
//! its participants (`conversation/actor/beam.rs`).
//!
//! R3 predicates live INSIDE the channel actor process: a [`SubscriptionPredicate`]
//! is a boxed `Fn(&Envelope) -> bool` owned by the actor's subscriber
//! registration and evaluated at delivery time. This mirrors the participant
//! `behaviour` pattern (a boxed trait object the process owns); for an in-memory
//! ephemeral channel there is no need for a serialisable predicate, so a closure
//! the actor holds is the simplest faithful design.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use beamr::native::native_process::{NativeContext, NativeHandler, NativeOutcome};
use beamr::process::ExitReason;
use beamr::scheduler::Scheduler;
use beamr::term::binary_ref::BinaryRef;

use crate::channel::wire::{decode_envelope, encode_envelope};
use crate::envelope::Envelope;
use crate::error::LiminalError;

/// A shared, cloneable in-memory inbox a subscriber receives delivered envelopes
/// on. See [`SubscriptionInbox`].
pub(crate) type SubscriberInbox = Arc<SubscriptionInbox>;

/// A wake callback fired on the inbox's empty→non-empty transition (R3, §1.2(2)).
///
/// The server installs one that fires the CONNECTION scheduler's `READY` marker,
/// so a publish into a parked connection's inbox wakes it. It is called from the
/// PUBLISHING actor's slice (the channel actor for local delivery, the subscriber
/// process for a remote frame), so it must be cheap and non-blocking — a single
/// `enqueue_atom_message`. `None` (no notifier installed) is the standalone
/// library / test case: nothing to wake, delivery still lands in the inbox.
pub type InboxNotifier = Arc<dyn Fn() + Send + Sync>;

/// One shared inbox-byte budget per connection (§5).
///
/// Spent across ALL that connection's subscription inboxes. The accounting unit is
/// serialized envelope bytes AS ADMITTED — charged at enqueue, released at dequeue
/// — so the signed 4 MiB product is exact and envelope-size-independent, not a
/// per-inbox count bounding a variable the design does not control.
#[derive(Debug)]
pub struct ConnectionInboxBudget {
    used: AtomicUsize,
    cap: usize,
}

impl ConnectionInboxBudget {
    /// Creates a shared budget with `cap` bytes of headroom across all the
    /// connection's inboxes.
    #[must_use]
    pub fn new(cap: usize) -> Arc<Self> {
        Arc::new(Self {
            used: AtomicUsize::new(0),
            cap,
        })
    }

    /// Attempts to charge `bytes`. Returns `true` and reserves the bytes when they
    /// fit within the remaining budget, `false` (reserving nothing) on overflow. A
    /// CAS loop keeps the reservation exact under concurrent charges from several
    /// inboxes — no transient over-charge is ever observable.
    fn try_charge(&self, bytes: usize) -> bool {
        let mut current = self.used.load(Ordering::Acquire);
        loop {
            let Some(projected) = current.checked_add(bytes) else {
                return false;
            };
            if projected > self.cap {
                return false;
            }
            match self.used.compare_exchange_weak(
                current,
                projected,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    /// Releases `bytes` previously charged (at dequeue). Saturating so a double
    /// release can never wrap the counter below zero.
    fn release(&self, bytes: usize) {
        let mut current = self.used.load(Ordering::Acquire);
        loop {
            let next = current.saturating_sub(bytes);
            match self.used.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    /// Bytes currently reserved across the connection's inboxes.
    #[cfg(test)]
    pub(crate) fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
}

/// Everything a server connection installs onto a subscription's inbox.
///
/// Carries the shared §5 byte budget, the per-inbox fairness cap, and the R3
/// wake notifier. Passed INTO the subscribe call so the installation happens at
/// inbox construction — strictly BEFORE the registration is published to the
/// channel actor — closing the pre-install window in which envelopes could be
/// admitted uncharged or without a wake.
pub struct InboxInstall {
    /// Shared per-connection byte budget (§5).
    pub budget: Arc<ConnectionInboxBudget>,
    /// Per-inbox envelope-count fairness trip (§5).
    pub depth_cap: usize,
    /// R3 wake notifier fired on the inbox's empty→non-empty transition. `None`
    /// when the caller has no waker (scheduler-free unit tests).
    pub notifier: Option<InboxNotifier>,
}

impl std::fmt::Debug for InboxInstall {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InboxInstall")
            .field("depth_cap", &self.depth_cap)
            .field("has_notifier", &self.notifier.is_some())
            .finish_non_exhaustive()
    }
}

/// Mutable inbox state guarded by one lock: the queued envelopes (each carrying
/// the exact bytes CHARGED for it, so release is symmetric with charge), the
/// installed shared budget and per-inbox fairness cap, the wake notifier, and
/// the closed marker.
struct InboxState {
    /// Each entry is `(envelope, charged_bytes)` — the amount actually charged to
    /// the shared budget at enqueue (0 when no budget was installed at admit
    /// time), released verbatim at dequeue/close. Storing the CHARGE, not the
    /// size, makes release byte-identical to charge on every entry even across a
    /// budget install, so the budget can never under- or over-release.
    queue: VecDeque<(Envelope, usize)>,
    /// Shared per-connection byte budget (§5). `None` = unbounded (standalone
    /// library use / tests), preserving the pre-bounding behaviour exactly.
    budget: Option<Arc<ConnectionInboxBudget>>,
    /// Per-inbox envelope-count secondary fairness trip (§5). `usize::MAX` = off;
    /// stops one subscription starving its siblings inside the shared byte budget.
    depth_cap: usize,
    /// Wake callback (R3). `None` until the connection installs one.
    notifier: Option<InboxNotifier>,
    /// Terminal marker set by [`SubscriptionInbox::close`]: admissions are refused
    /// WITHOUT charging, and all queued charges have been released. Closing is the
    /// release-by-construction seam — every teardown path (explicit unsubscribe,
    /// overflow shed, connection teardown, and the `Drop` backstop) funnels
    /// through it, so queued bytes can never be stranded on the connection-lifetime
    /// budget.
    closed: bool,
}

/// The shared subscription inbox (R3 + §5). Replaces the bare
/// `Arc<Mutex<VecDeque<Envelope>>>`: it fires a wake notifier on the
/// empty→non-empty transition and enforces the connection-scoped byte budget plus
/// the per-inbox fairness trip, shedding the offending subscription on overflow.
pub(crate) struct SubscriptionInbox {
    state: Mutex<InboxState>,
    /// Sticky overflow marker: set when an admission is refused by the byte budget
    /// or the fairness trip. The server-side delivery pump observes it and sheds
    /// this subscription with a typed error frame, mirroring the outbound overflow
    /// policy (a slow consumer sheds its own subscription; it cannot grow server
    /// memory without bound). Sticky (never cleared) because a shed is terminal.
    overflowed: AtomicBool,
}

impl std::fmt::Debug for SubscriptionInbox {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SubscriptionInbox")
            .field("overflowed", &self.overflowed.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

/// Why an inbox admission was refused (§5). The budget/fairness refusals set the
/// sticky overflow marker and drop the envelope rather than growing memory; a
/// closed inbox refuses without charging and without marking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InboxAdmission {
    /// The envelope was admitted (and, when it made the inbox non-empty, the wake
    /// notifier was fired).
    Admitted,
    /// The shared connection byte budget (§5) had no room; the subscription is
    /// shed.
    BudgetExceeded,
    /// The per-inbox fairness trip (§5) is full; the subscription is shed.
    FairnessTripped,
    /// The inbox was closed (unsubscribe/shed/teardown): the envelope is dropped
    /// without charging the budget — a closed inbox can never re-accumulate cost.
    Closed,
}

impl SubscriptionInbox {
    /// Creates an unbounded, notifier-less inbox — the standalone/default shape,
    /// byte-identical to the pre-bounding behaviour. A server connection passes an
    /// [`InboxInstall`] through subscribe so budget/cap/notifier are installed at
    /// construction instead.
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(InboxState {
                queue: VecDeque::new(),
                budget: None,
                depth_cap: usize::MAX,
                notifier: None,
                closed: false,
            }),
            overflowed: AtomicBool::new(false),
        })
    }

    /// Installs the connection's shared byte budget and per-inbox fairness cap
    /// (§5). Runs at inbox construction (via [`InboxInstall`]) — before the
    /// registration is published to the channel actor — so no envelope can be
    /// admitted uncharged.
    pub(crate) fn install_budget(&self, budget: Arc<ConnectionInboxBudget>, depth_cap: usize) {
        if let Ok(mut state) = self.state.lock() {
            state.budget = Some(budget);
            state.depth_cap = depth_cap;
        }
    }

    /// Installs the wake notifier (R3), fired on the empty→non-empty transition,
    /// capturing the connection scheduler's enqueue handle (§1.2(2)).
    ///
    /// Defensive invariant: the install RECHECKS non-emptiness under the lock and
    /// fires the notifier (outside the lock) when envelopes are already queued —
    /// an install onto an already-non-empty inbox produces the wake whose edge was
    /// consumed before the notifier existed, so a wake can never be lost to
    /// install ordering. On the normal construction path the queue is empty and
    /// this is a no-op.
    pub(crate) fn install_notifier(&self, notifier: InboxNotifier) {
        let fire = {
            let Ok(mut state) = self.state.lock() else {
                return;
            };
            let pending = !state.queue.is_empty();
            let handle = notifier.clone();
            state.notifier = Some(notifier);
            pending.then_some(handle)
        };
        if let Some(notifier) = fire {
            notifier();
        }
    }

    /// Admits `envelope` under the byte budget and fairness trip, charging the
    /// serialized bytes as admitted and firing the wake notifier on the
    /// empty→non-empty transition. On budget/fairness refusal the sticky overflow
    /// marker is set and the envelope dropped (memory never grows past the
    /// bound); a closed inbox refuses without charging or marking.
    ///
    /// The notifier fires OUTSIDE the state lock so the publishing actor's slice
    /// never holds the inbox lock across the scheduler enqueue.
    pub(crate) fn admit(&self, envelope: Envelope) -> InboxAdmission {
        // Serialize once, before the lock: the admitted byte count is the wire
        // size (§5 denomination). The entry stores the amount actually CHARGED
        // (0 when no budget is installed), so dequeue/close releases exactly
        // what enqueue charged.
        let bytes = encode_envelope(&envelope).len();
        let notifier = {
            let Ok(mut state) = self.state.lock() else {
                // A poisoned inbox lock is terminal for this subscription; treat it
                // as a shed rather than silently dropping into a dead inbox.
                self.overflowed.store(true, Ordering::Release);
                return InboxAdmission::BudgetExceeded;
            };
            if state.closed {
                return InboxAdmission::Closed;
            }
            if state.queue.len() >= state.depth_cap {
                self.overflowed.store(true, Ordering::Release);
                return InboxAdmission::FairnessTripped;
            }
            let charged = match state.budget.as_ref() {
                Some(budget) => {
                    if !budget.try_charge(bytes) {
                        self.overflowed.store(true, Ordering::Release);
                        return InboxAdmission::BudgetExceeded;
                    }
                    bytes
                }
                None => 0,
            };
            let was_empty = state.queue.is_empty();
            state.queue.push_back((envelope, charged));
            // Fire only on the empty→non-empty edge: a parked connection needs one
            // wake to drain the whole burst, and coalescing is harmless (R6).
            if was_empty {
                state.notifier.clone()
            } else {
                None
            }
        };
        if let Some(notifier) = notifier {
            notifier();
        }
        InboxAdmission::Admitted
    }

    /// Removes and returns the next envelope, releasing its CHARGED bytes back to
    /// the shared budget (exact charge/release symmetry).
    pub(crate) fn pop(&self) -> Option<Envelope> {
        let (envelope, charged, budget) = {
            let mut state = self.state.lock().ok()?;
            let (envelope, charged) = state.queue.pop_front()?;
            (envelope, charged, state.budget.clone())
        };
        // Release the charged bytes AFTER dropping the state lock so the shared
        // budget's atomic is never touched while the inbox lock is held.
        if let Some(budget) = budget {
            budget.release(charged);
        }
        Some(envelope)
    }

    /// Atomically closes the inbox, releasing every queued charge back to the
    /// shared budget: under the lock it marks the inbox closed, drains all
    /// entries, and detaches the notifier and budget; the summed release happens
    /// outside the lock. Idempotent. Admissions after close are refused without
    /// charging ([`InboxAdmission::Closed`]).
    ///
    /// This is the release-by-construction seam: explicit unsubscribe, overflow
    /// shed, and connection teardown ALL reach it through the subscription
    /// handle's drop (see [`SubscriptionInner::drop`]), and the inbox's own `Drop`
    /// is the final backstop — no teardown path can strand queued bytes on the
    /// connection-lifetime budget.
    pub(crate) fn close(&self) {
        let (released, budget) = {
            let Ok(mut state) = self.state.lock() else {
                return;
            };
            if state.closed {
                return;
            }
            state.closed = true;
            let released: usize = state
                .queue
                .drain(..)
                .map(|(_envelope, charged)| charged)
                .sum();
            state.notifier = None;
            (released, state.budget.take())
        };
        if let Some(budget) = budget {
            budget.release(released);
        }
    }

    /// Whether this subscription has been marked for shedding by an overflow.
    pub(crate) fn is_overflowed(&self) -> bool {
        self.overflowed.load(Ordering::Acquire)
    }

    /// Number of queued envelopes (test observability).
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.state.lock().map_or(0, |state| state.queue.len())
    }
}

impl Drop for SubscriptionInbox {
    fn drop(&mut self) {
        // Backstop: if no teardown path ever called `close`, release the queued
        // charges here so the last Arc dropping can never strand budget bytes.
        // Idempotent against an earlier close (the closed marker short-circuits).
        self.close();
    }
}

/// A delivery predicate evaluated by the channel actor against each published
/// envelope. `None` (no predicate) means deliver everything.
pub(crate) type SubscriptionPredicate = Arc<dyn Fn(&Envelope) -> bool + Send + Sync>;

/// Real beamr native process backing one subscription.
///
/// For LOCAL delivery it is an idle handler (mirroring
/// `aion::worker::link::IdleWorkerProcess`): local envelopes travel through the
/// shared [`SubscriberInbox`] the channel actor writes and
/// [`SubscriptionHandle::try_next`] reads. Its other job is to BE a first-class
/// linkable, killable process whose lifetime equals the subscription's, so the
/// channel actor detects the subscription dying via a real EXIT signal rather
/// than by polling a weak pointer.
///
/// For CROSS-NODE delivery (SRV-005) it is also the landing point for a remote
/// publish: a remote node sends a published envelope, encoded by
/// [`crate::channel::wire::encode_envelope`], as a single beamr binary directly
/// to this process's pid (the pid the cluster registered in the channel's
/// distributed process group). The binary lands in this process's mailbox; the
/// handler decodes it back into an [`Envelope`] and pushes it onto the SAME
/// inbox a local publish would, so a subscriber observes local and remote
/// messages identically. Non-binary wakeups (trapped `{EXIT, _, _}` signals) are
/// drained and ignored.
struct SubscriberProcess {
    inbox: SubscriberInbox,
}

impl NativeHandler for SubscriberProcess {
    fn handle(&mut self, ctx: &mut NativeContext<'_>) -> NativeOutcome {
        // Trapping is set authoritatively at spawn (see `SubscriptionHandle::spawn`)
        // so it holds before the actor ever links — re-assert it here defensively
        // for any future restart of this handler.
        ctx.set_trap_exit(true);
        // Drain every queued wakeup. A binary message is a remote envelope frame
        // (SRV-005) to decode and enqueue; everything else (e.g. a trapped
        // `{EXIT, _, _}` tuple from a crashed actor this subscriber outlives) is
        // ignored. Death is driven only by an explicit `terminate_process` on
        // unsubscribe/handle drop.
        while let Some(message) = ctx.recv() {
            if let Some(binary) = BinaryRef::new(message) {
                self.accept_remote_frame(binary.as_bytes());
            }
        }
        NativeOutcome::Wait
    }
}

impl SubscriberProcess {
    /// Decode a remote envelope frame and push it onto the inbox. A frame that
    /// fails to decode is dropped: a corrupt cross-node payload must never crash
    /// the subscriber or stall delivery of well-formed messages.
    fn accept_remote_frame(&self, bytes: &[u8]) {
        let Ok(envelope) = decode_envelope(bytes) else {
            return;
        };
        // R3: the remote-delivery leg fires the same wake notifier and obeys the
        // same §5 byte budget as the local leg. An overflow marks the subscription
        // for shedding (inside `admit`); the frame is dropped rather than growing
        // server memory.
        self.inbox.admit(envelope);
    }
}

/// The actor-side record of one subscriber: the inbox to deliver into and the
/// optional predicate to gate delivery. Held by the channel actor INSIDE its
/// process, keyed by the subscriber process's pid.
pub(crate) struct SubscriberRegistration {
    pid: u64,
    inbox: SubscriberInbox,
    predicate: Option<SubscriptionPredicate>,
}

impl SubscriberRegistration {
    pub(crate) const fn pid(&self) -> u64 {
        self.pid
    }

    /// Delivers `envelope` to this subscriber when its predicate accepts it (or
    /// it has no predicate). Returns `true` when the envelope was pushed onto
    /// the inbox, `false` when a predicate filtered it out, the inbox refused it
    /// (§5 overflow — the subscription is then marked for shedding), or the inbox
    /// is closed. The boolean lets the channel actor count genuine deliveries for
    /// the delivery-ack signal.
    pub(crate) fn deliver(&self, envelope: &Envelope) -> bool {
        if let Some(predicate) = self.predicate.as_ref() {
            if !predicate(envelope) {
                return false;
            }
        }
        // R3 + §5: admission charges the connection byte budget, fires the wake
        // notifier on the empty→non-empty edge, and — on overflow — marks the
        // subscription for shedding (the server pump sheds it with a typed error
        // frame). An overflowed envelope is NOT counted as a genuine delivery, so
        // the delivery-ack signal reflects only envelopes that entered the inbox.
        matches!(self.inbox.admit(envelope.clone()), InboxAdmission::Admitted)
    }
}

impl std::fmt::Debug for SubscriberRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SubscriberRegistration")
            .field("pid", &self.pid)
            .field("has_predicate", &self.predicate.is_some())
            .finish_non_exhaustive()
    }
}

/// Handle returned by channel subscriptions for receiving validated envelopes.
///
/// Owns the subscriber's beamr pid, the shared inbox, and a clone of the
/// scheduler so the process can be terminated when the subscription ends. The
/// handle is the subscription's lifetime: dropping the last clone terminates the
/// subscriber process, whose EXIT prunes the channel actor's fan-out list.
#[derive(Clone)]
pub struct SubscriptionHandle {
    inner: Arc<SubscriptionInner>,
}

struct SubscriptionInner {
    pid: u64,
    inbox: SubscriberInbox,
    scheduler: Arc<Scheduler>,
}

impl SubscriptionHandle {
    /// Spawns a real subscriber process on `scheduler` and returns the handle
    /// plus its actor-side registration record (carrying any predicate).
    ///
    /// # Errors
    /// Returns [`LiminalError::SubscriptionFailed`] when the scheduler cannot
    /// spawn the subscriber process.
    pub(crate) fn spawn(
        scheduler: &Arc<Scheduler>,
        predicate: Option<SubscriptionPredicate>,
        install: Option<InboxInstall>,
    ) -> Result<(Self, SubscriberRegistration), LiminalError> {
        let inbox: SubscriberInbox = SubscriptionInbox::new();
        // Install the §5 budget/fairness cap and the R3 wake notifier AT
        // CONSTRUCTION — strictly before the registration is handed to the
        // channel actor — so there is no window in which a publish can be
        // admitted uncharged, past the depth cap, or without a wake.
        if let Some(install) = install {
            inbox.install_budget(install.budget, install.depth_cap);
            if let Some(notifier) = install.notifier {
                inbox.install_notifier(notifier);
            }
        }
        let process_inbox = Arc::clone(&inbox);
        let factory = Box::new(move || {
            Box::new(SubscriberProcess {
                inbox: Arc::clone(&process_inbox),
            }) as Box<dyn NativeHandler>
        });
        let pid =
            scheduler
                .spawn_native(factory)
                .map_err(|error| LiminalError::SubscriptionFailed {
                    message: format!("failed to spawn subscriber process: {error:?}"),
                })?;
        // Set trap_exit BEFORE the channel actor links to this pid, so an
        // abnormal actor crash is trapped (delivered as a message the subscriber
        // drains) instead of cascading across the link and killing the
        // subscriber. This makes the subscriber outlive a channel-actor crash so
        // the restarted actor can re-link to it on boot (R2/R4). Setting it
        // host-side (not in the handler's first slice) removes any race against
        // the crash: the flag is in place the instant `subscribe` proceeds.
        scheduler
            .set_trap_exit(pid, true)
            .map_err(|error| LiminalError::SubscriptionFailed {
                message: format!("failed to set trap_exit on subscriber process {pid}: {error:?}"),
            })?;
        let handle = Self {
            inner: Arc::new(SubscriptionInner {
                pid,
                inbox: Arc::clone(&inbox),
                scheduler: Arc::clone(scheduler),
            }),
        };
        let registration = SubscriberRegistration {
            pid,
            inbox,
            predicate,
        };
        Ok((handle, registration))
    }

    /// Returns the beamr pid of the subscriber process this handle owns.
    #[must_use]
    pub(crate) fn pid(&self) -> u64 {
        self.inner.pid
    }

    /// Attempts to receive the next delivered envelope without blocking.
    ///
    /// # Errors
    ///
    /// Returns [`LiminalError::SubscriptionFailed`] when the subscription inbox cannot be read.
    pub fn try_next(&self) -> Result<Option<Envelope>, LiminalError> {
        // Dequeue releases the envelope's admitted bytes back to the shared
        // connection budget (§5 charge/release symmetry).
        Ok(self.inner.inbox.pop())
    }

    /// Whether an overflow has marked this subscription for shedding (§5).
    #[must_use]
    pub fn is_overflowed(&self) -> bool {
        self.inner.inbox.is_overflowed()
    }
}

impl std::fmt::Debug for SubscriptionHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SubscriptionHandle")
            .field("pid", &self.inner.pid)
            .finish_non_exhaustive()
    }
}

impl Drop for SubscriptionInner {
    fn drop(&mut self) {
        // Close the inbox FIRST: atomically mark it closed, drain queued entries,
        // and release every charged byte back to the shared connection budget
        // (§5 — queued bytes must never be stranded on the connection-lifetime
        // budget by unsubscribe, shed, or teardown; all of them funnel through
        // this drop). Post-close deliveries from the channel actor (whose EXIT
        // prune below is asynchronous) are refused without charging.
        self.inbox.close();
        // Terminating the subscriber process fires the bidirectional link to the
        // channel actor, which traps the EXIT and removes this subscriber from
        // its fan-out list. This is the real-beamr unsubscribe-on-drop path.
        self.scheduler
            .terminate_process(self.pid, ExitReason::Normal);
    }
}

/// WR-9b: the REAL [`SubscriberProcess`] running on beamr's cooperative
/// (single-threaded / wasm) [`beamr::scheduler::WasmScheduler`].
///
/// This proves the production subscriber handler — the same `NativeHandler` the
/// threaded [`SubscriptionHandle::spawn`] spawns — runs unchanged on the
/// cooperative scheduler that a browser host drives. There is no toy stand-in:
/// the test spawns the genuine [`SubscriberProcess`], delivers a genuine
/// [`crate::channel::wire::encode_envelope`] frame as a real beamr binary, pumps
/// cooperative `run_until_idle` turns, and asserts the envelope is decoded by the
/// handler's own `accept_remote_frame` path and lands in the shared inbox a
/// [`SubscriptionHandle::try_next`] would read.
///
/// The handler runs cooperatively AS-IS: its `handle` only touches
/// platform-neutral [`NativeContext`] capabilities (`set_trap_exit`, `recv`),
/// [`BinaryRef`], and [`decode_envelope`] — none of which reach for threads,
/// tokio, sockets, or a `SharedState`. The only wiring the smoke supplies is the
/// cooperative driver (spawn + owned-binary delivery + turn pump), exactly the
/// host-side seam the threaded `SubscriptionHandle`/channel-actor provide on
/// native.
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod cooperative_smoke {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;

    use beamr::atom::AtomTable;
    use beamr::ets::copy_term_to_ets;
    use beamr::module::ModuleRegistry;
    use beamr::native::BifRegistryImpl;
    use beamr::process::heap::Heap;
    use beamr::scheduler::WasmScheduler;
    use beamr::term::shared_binary::{SharedBinary, write_proc_bin};

    use super::{SubscriberInbox, SubscriberProcess, SubscriptionInbox};
    use crate::channel::SchemaId;
    use crate::channel::wire::encode_envelope;
    use crate::envelope::{Envelope, PublisherId};

    /// Build a cooperative scheduler the way a wasm host holds it (single
    /// `Rc<RefCell<…>>` on one thread).
    fn cooperative_scheduler() -> Rc<RefCell<WasmScheduler>> {
        let atom_table = Arc::new(AtomTable::with_common_atoms());
        let modules = Arc::new(ModuleRegistry::new());
        let bifs = Arc::new(BifRegistryImpl::new());
        Rc::new(RefCell::new(WasmScheduler::new(atom_table, modules, bifs)))
    }

    /// Encode `envelope` into the production wire frame and wrap it as a
    /// heap-independent beamr binary term ready for `send_owned`, mirroring how a
    /// remote node hands a published frame to a subscriber pid (SRV-005).
    fn frame_as_owned_binary(envelope: &Envelope) -> beamr::ets::OwnedTerm {
        let bytes = encode_envelope(envelope);
        let shared = SharedBinary::new(bytes);
        // A ProcBin reference needs three heap words; copy it into ETS-owned
        // memory so the scratch heap can be dropped before delivery.
        let mut scratch = Heap::new(8);
        let words = scratch
            .alloc_slice(3)
            .expect("scratch heap holds a proc-bin reference");
        let term = write_proc_bin(words, &shared).expect("proc-bin term writes");
        copy_term_to_ets(term).expect("frame copies into an owned binary")
    }

    fn sample_envelope() -> Envelope {
        // A whole-millisecond timestamp so the round-trip through the wire codec
        // (which carries millisecond resolution, see `channel::wire`) is exact;
        // `Utc::now()` sub-millisecond precision would otherwise be truncated on
        // decode and is irrelevant to what this smoke proves.
        let timestamp = chrono::TimeZone::timestamp_millis_opt(&chrono::Utc, 1_700_000_000_123)
            .single()
            .expect("valid fixed millisecond timestamp");
        Envelope::with_timestamp(
            b"{\"value\":42}".to_vec(),
            None,
            SchemaId::new(),
            PublisherId::from("publisher-cooperative"),
            timestamp,
        )
    }

    #[test]
    fn real_subscriber_process_delivers_a_published_envelope_cooperatively() {
        let scheduler = cooperative_scheduler();

        // The shared inbox the subscriber pushes decoded envelopes onto — the
        // exact channel the threaded `SubscriptionHandle::try_next` reads.
        let inbox: SubscriberInbox = SubscriptionInbox::new();
        let process_inbox = Arc::clone(&inbox);

        // Spawn the GENUINE production subscriber handler as a first-class native
        // process on the cooperative scheduler.
        let pid = scheduler.borrow_mut().spawn_native_root(Box::new(move || {
            Box::new(SubscriberProcess {
                inbox: Arc::clone(&process_inbox),
            }) as Box<dyn beamr::native::native_process::NativeHandler>
        }));

        // First turn: the handler runs once, asserts trap_exit, finds an empty
        // mailbox, and parks (`Wait`). No envelope has been delivered yet.
        scheduler.borrow_mut().run_until_idle();
        assert_eq!(
            inbox.len(),
            0,
            "no envelope is delivered before one is published"
        );

        // Publish: deliver a real encoded frame as a beamr binary straight to the
        // subscriber pid, exactly as a remote publish lands (SRV-005). This wakes
        // the parked process.
        let published = sample_envelope();
        let frame = frame_as_owned_binary(&published);
        scheduler
            .borrow_mut()
            .send_owned(pid, &frame)
            .expect("frame is delivered to the live subscriber pid");

        // Pump turns: the woken handler drains the binary, decodes it through its
        // own `accept_remote_frame` path, and pushes the envelope onto the inbox.
        let mut delivered = None;
        for _ in 0..8 {
            scheduler.borrow_mut().run_until_idle();
            let next = inbox.pop();
            if let Some(envelope) = next {
                delivered = Some(envelope);
                break;
            }
        }

        assert_eq!(
            delivered.as_ref(),
            Some(&published),
            "the real subscriber decoded and delivered the published envelope"
        );
    }
}

/// R3 (§1.2(2)) + §5 inbox-bounding library core: the notifier fires on the
/// empty→non-empty edge; the shared byte budget is spent across ALL a
/// connection's inboxes; overflow sheds the offending subscription; the per-inbox
/// fairness trip stops one inbox starving its siblings; and charge/release is
/// exact. These exercise [`SubscriptionInbox`]/[`ConnectionInboxBudget`] directly,
/// with no scheduler — the server-side wake wiring and shed are tested there.
#[cfg(test)]
#[allow(clippy::expect_used)]
mod inbox_bounding {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{ConnectionInboxBudget, InboxAdmission, SubscriptionInbox};
    use crate::channel::SchemaId;
    use crate::channel::wire::encode_envelope;
    use crate::envelope::{Envelope, PublisherId};

    fn envelope(payload: &[u8]) -> Envelope {
        Envelope::new(
            payload.to_vec(),
            None,
            SchemaId::new(),
            PublisherId::from("inbox-bounding-test"),
        )
    }

    fn admitted_bytes(env: &Envelope) -> usize {
        encode_envelope(env).len()
    }

    #[test]
    fn notifier_fires_only_on_empty_to_non_empty_transition() {
        let inbox = SubscriptionInbox::new();
        let fires = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&fires);
        inbox.install_notifier(Arc::new(move || {
            counter.fetch_add(1, Ordering::Relaxed);
        }));

        // First admit into an empty inbox: the edge fires exactly once.
        assert_eq!(inbox.admit(envelope(b"a")), InboxAdmission::Admitted);
        assert_eq!(
            fires.load(Ordering::Relaxed),
            1,
            "empty→non-empty fires once"
        );

        // A second admit into a NON-empty inbox does not re-fire: one wake drains
        // the whole burst (coalescing is R6-harmless).
        assert_eq!(inbox.admit(envelope(b"b")), InboxAdmission::Admitted);
        assert_eq!(
            fires.load(Ordering::Relaxed),
            1,
            "no re-fire while the inbox stays non-empty"
        );

        // Drain to empty, then admit again: the edge fires a second time.
        assert!(inbox.pop().is_some());
        assert!(inbox.pop().is_some());
        assert_eq!(inbox.admit(envelope(b"c")), InboxAdmission::Admitted);
        assert_eq!(
            fires.load(Ordering::Relaxed),
            2,
            "a fresh empty→non-empty edge fires again"
        );
    }

    #[test]
    fn shared_budget_is_spent_across_all_a_connections_inboxes() {
        let one = envelope(b"payload-one");
        let two = envelope(b"payload-two");
        // A budget large enough for exactly ONE of the two envelopes.
        let cap = admitted_bytes(&one);
        let budget = ConnectionInboxBudget::new(cap);

        let inbox_a = SubscriptionInbox::new();
        let inbox_b = SubscriptionInbox::new();
        inbox_a.install_budget(Arc::clone(&budget), usize::MAX);
        inbox_b.install_budget(Arc::clone(&budget), usize::MAX);

        // Inbox A admits its envelope, consuming the whole shared budget.
        assert_eq!(inbox_a.admit(one), InboxAdmission::Admitted);
        assert_eq!(budget.used(), cap, "the shared budget is now fully spent");

        // Inbox B — a SIBLING subscription — is refused: the budget is connection
        // scoped, not per-inbox, so A's fill denies B.
        assert_eq!(inbox_b.admit(two), InboxAdmission::BudgetExceeded);
        assert!(
            inbox_b.is_overflowed(),
            "the sibling that overflowed the shared budget is shed"
        );
        assert!(!inbox_a.is_overflowed(), "the inbox that fit is not shed");

        // Draining A releases its bytes back to the SHARED budget, so B could then
        // admit (charge/release symmetry across siblings).
        assert!(inbox_a.pop().is_some());
        assert_eq!(
            budget.used(),
            0,
            "release returns bytes to the shared budget"
        );
    }

    #[test]
    fn overflow_sheds_and_does_not_grow_memory() {
        let env = envelope(b"x");
        let budget = ConnectionInboxBudget::new(admitted_bytes(&env)); // room for one
        let inbox = SubscriptionInbox::new();
        inbox.install_budget(budget, usize::MAX);

        assert_eq!(inbox.admit(env.clone()), InboxAdmission::Admitted);
        // The next admit overflows: refused, marked for shedding, and NOT queued —
        // the queue length does not grow past the bound.
        assert_eq!(inbox.admit(env), InboxAdmission::BudgetExceeded);
        assert!(inbox.is_overflowed());
        assert_eq!(
            inbox.len(),
            1,
            "the overflowed envelope is dropped, not queued"
        );
    }

    #[test]
    fn per_inbox_fairness_trip_stops_one_inbox_starving_siblings() {
        // A huge byte budget so the FAIRNESS count — not the budget — is the trip.
        let budget = ConnectionInboxBudget::new(usize::MAX);
        let inbox = SubscriptionInbox::new();
        inbox.install_budget(budget, 2); // depth cap of 2 envelopes

        assert_eq!(inbox.admit(envelope(b"1")), InboxAdmission::Admitted);
        assert_eq!(inbox.admit(envelope(b"2")), InboxAdmission::Admitted);
        // The third trips the fairness cap even though bytes are available.
        assert_eq!(inbox.admit(envelope(b"3")), InboxAdmission::FairnessTripped);
        assert!(inbox.is_overflowed());
        assert_eq!(
            inbox.len(),
            2,
            "the fairness trip holds the inbox at its cap"
        );
    }

    #[test]
    fn charge_and_release_are_exact() {
        let budget = ConnectionInboxBudget::new(1024 * 1024);
        let inbox = SubscriptionInbox::new();
        inbox.install_budget(Arc::clone(&budget), usize::MAX);

        let a = envelope(b"first-envelope");
        let b = envelope(b"second-longer-envelope-payload");
        let charge = admitted_bytes(&a) + admitted_bytes(&b);
        assert_eq!(inbox.admit(a), InboxAdmission::Admitted);
        assert_eq!(inbox.admit(b), InboxAdmission::Admitted);
        assert_eq!(budget.used(), charge, "used == sum of admitted bytes");

        assert!(inbox.pop().is_some());
        assert!(inbox.pop().is_some());
        assert_eq!(
            budget.used(),
            0,
            "every admitted byte is released on dequeue — exact symmetry"
        );
    }

    /// Review round 1 item 4: closing an inbox with a QUEUED backlog (the shed
    /// shape — an overflowed inbox is near-full by construction) releases every
    /// charged byte back to the shared budget, so a sibling subscription can
    /// admit again. Without the close-release, one shed strands its whole share
    /// of the 4 MiB budget forever.
    #[test]
    fn close_releases_queued_charges_so_siblings_recover() {
        // Budget sized so inbox A can queue a 256-envelope backlog (the §5
        // fairness-cap depth) and exhaust the shared budget doing it.
        let one = envelope(b"backlog-envelope-payload");
        let unit = admitted_bytes(&one);
        let budget = ConnectionInboxBudget::new(unit * 256);

        let inbox_a = SubscriptionInbox::new();
        let inbox_b = SubscriptionInbox::new();
        inbox_a.install_budget(Arc::clone(&budget), usize::MAX);
        inbox_b.install_budget(Arc::clone(&budget), usize::MAX);

        // Queue the full 256-envelope backlog on A, consuming the whole budget.
        for _ in 0..256 {
            assert_eq!(inbox_a.admit(one.clone()), InboxAdmission::Admitted);
        }
        assert_eq!(budget.used(), unit * 256, "the backlog holds the budget");
        // The sibling is starved (the shed trigger condition).
        assert_eq!(inbox_b.admit(one.clone()), InboxAdmission::BudgetExceeded);

        // Shed/unsubscribe/teardown all funnel through close: EVERY queued charge
        // returns to the shared budget in one atomic close.
        inbox_a.close();
        assert_eq!(
            budget.used(),
            0,
            "close releases the entire queued backlog back to the shared budget"
        );
        // The sibling recovers: it can admit again.
        assert_eq!(
            inbox_b.admit(one),
            InboxAdmission::Admitted,
            "a sibling admits again after the other inbox is shed"
        );
    }

    /// Review round 1 item 4: a closed inbox refuses admissions WITHOUT charging
    /// the budget, so a shed subscription can never re-accumulate cost while the
    /// channel actor's asynchronous EXIT prune is still in flight.
    #[test]
    fn closed_inbox_refuses_without_charging() {
        let env = envelope(b"post-close");
        let budget = ConnectionInboxBudget::new(1024 * 1024);
        let inbox = SubscriptionInbox::new();
        inbox.install_budget(Arc::clone(&budget), usize::MAX);

        inbox.close();
        assert_eq!(inbox.admit(env), InboxAdmission::Closed);
        assert_eq!(budget.used(), 0, "a closed inbox never charges the budget");
        assert_eq!(inbox.len(), 0, "a closed inbox never queues");
    }

    /// Review round 1 item 4: the `Drop` backstop — if no teardown path ever
    /// called `close`, the last handle dropping still releases the queued charges
    /// (release-by-construction: a release that cannot be omitted).
    #[test]
    fn drop_backstop_releases_queued_charges() {
        let env = envelope(b"dropped-while-queued");
        let unit = admitted_bytes(&env);
        let budget = ConnectionInboxBudget::new(1024 * 1024);
        {
            let inbox = SubscriptionInbox::new();
            inbox.install_budget(Arc::clone(&budget), usize::MAX);
            assert_eq!(inbox.admit(env.clone()), InboxAdmission::Admitted);
            assert_eq!(inbox.admit(env), InboxAdmission::Admitted);
            assert_eq!(budget.used(), unit * 2);
            // No close() call: the Arc drops here.
        }
        assert_eq!(
            budget.used(),
            0,
            "dropping the last inbox handle releases every queued charge"
        );
    }

    /// Review round 1 item 4: close is idempotent, and pop-after-close finds
    /// nothing (the queue was drained into the release).
    #[test]
    fn close_is_idempotent_and_drains_the_queue() {
        let env = envelope(b"x");
        let budget = ConnectionInboxBudget::new(1024 * 1024);
        let inbox = SubscriptionInbox::new();
        inbox.install_budget(Arc::clone(&budget), usize::MAX);
        assert_eq!(inbox.admit(env), InboxAdmission::Admitted);

        inbox.close();
        inbox.close(); // second close is a no-op, not a double release
        assert_eq!(budget.used(), 0);
        assert!(inbox.pop().is_none(), "a closed inbox holds nothing");
    }

    /// Review round 1 item 5 (charge ownership): an envelope admitted BEFORE the
    /// budget was installed carries a charge of 0 — its dequeue releases exactly
    /// 0 against the later-installed budget, never bytes it did not charge. The
    /// production subscribe path installs the budget at inbox construction so
    /// this window is structurally closed; this pins the defensive invariant
    /// that makes release byte-identical to charge on EVERY entry regardless.
    #[test]
    fn per_entry_charge_ownership_survives_budget_install() {
        let uncharged = envelope(b"admitted-before-budget-install");
        let charged = envelope(b"admitted-after-budget-install");
        let inbox = SubscriptionInbox::new();

        // Admitted with no budget installed: charge ownership 0.
        assert_eq!(inbox.admit(uncharged), InboxAdmission::Admitted);

        let budget = ConnectionInboxBudget::new(1024 * 1024);
        inbox.install_budget(Arc::clone(&budget), usize::MAX);
        let unit = admitted_bytes(&charged);
        assert_eq!(inbox.admit(charged), InboxAdmission::Admitted);
        assert_eq!(budget.used(), unit, "only the post-install entry charged");

        // Popping the uncharged entry releases exactly 0 — the budget cannot
        // under-count (over-admitting past the signed 4 MiB) by releasing bytes
        // that were never charged.
        assert!(inbox.pop().is_some());
        assert_eq!(budget.used(), unit, "the uncharged entry released nothing");
        assert!(inbox.pop().is_some());
        assert_eq!(
            budget.used(),
            0,
            "the charged entry released its exact charge"
        );
    }

    /// Review round 1 item 5 (install recheck): installing a notifier onto an
    /// ALREADY-NON-EMPTY inbox fires it exactly once — the wake whose
    /// empty→non-empty edge was consumed before the notifier existed is
    /// regenerated at install, so a wake can never be lost to install ordering.
    /// (The production subscribe path installs at construction, when the queue is
    /// guaranteed empty; this pins the defensive invariant.)
    #[test]
    fn notifier_install_onto_non_empty_inbox_fires_once() {
        let inbox = SubscriptionInbox::new();
        assert_eq!(
            inbox.admit(envelope(b"pre-install")),
            InboxAdmission::Admitted
        );

        let fires = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&fires);
        inbox.install_notifier(Arc::new(move || {
            counter.fetch_add(1, Ordering::Relaxed);
        }));
        assert_eq!(
            fires.load(Ordering::Relaxed),
            1,
            "install onto a non-empty inbox regenerates exactly one wake"
        );

        // A subsequent admit onto the still-non-empty inbox does not re-fire.
        assert_eq!(inbox.admit(envelope(b"second")), InboxAdmission::Admitted);
        assert_eq!(fires.load(Ordering::Relaxed), 1);
    }
}
