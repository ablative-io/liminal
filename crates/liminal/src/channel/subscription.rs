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
use std::sync::{Arc, Mutex};

use beamr::native::native_process::{NativeContext, NativeHandler, NativeOutcome};
use beamr::process::ExitReason;
use beamr::scheduler::Scheduler;
use beamr::term::binary_ref::BinaryRef;

use crate::channel::wire::decode_envelope;
use crate::envelope::Envelope;
use crate::error::LiminalError;

/// In-memory inbox a subscriber receives delivered envelopes on.
pub(crate) type SubscriberInbox = Arc<Mutex<VecDeque<Envelope>>>;

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
        if let Ok(mut inbox) = self.inbox.lock() {
            inbox.push_back(envelope);
        }
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
    /// it has no predicate). Returns `Ok(true)` when the envelope was pushed onto
    /// the inbox, `Ok(false)` when a predicate filtered it out, and `Err` only
    /// when the inbox lock is poisoned. The boolean lets the channel actor count
    /// genuine deliveries for the delivery-ack signal.
    pub(crate) fn deliver(&self, envelope: &Envelope) -> Result<bool, LiminalError> {
        if let Some(predicate) = self.predicate.as_ref() {
            if !predicate(envelope) {
                return Ok(false);
            }
        }
        self.inbox
            .lock()
            .map_err(|error| LiminalError::DeliveryFailed {
                message: format!("subscriber inbox unavailable: {error}"),
            })?
            .push_back(envelope.clone());
        Ok(true)
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
    ) -> Result<(Self, SubscriberRegistration), LiminalError> {
        let inbox: SubscriberInbox = Arc::new(Mutex::new(VecDeque::new()));
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
        let mut messages =
            self.inner
                .inbox
                .lock()
                .map_err(|error| LiminalError::SubscriptionFailed {
                    message: format!("subscription inbox unavailable: {error}"),
                })?;
        Ok(messages.pop_front())
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
    use std::collections::VecDeque;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};

    use beamr::atom::AtomTable;
    use beamr::ets::copy_term_to_ets;
    use beamr::module::ModuleRegistry;
    use beamr::native::BifRegistryImpl;
    use beamr::process::heap::Heap;
    use beamr::scheduler::WasmScheduler;
    use beamr::term::shared_binary::{SharedBinary, write_proc_bin};

    use super::{SubscriberInbox, SubscriberProcess};
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
        let inbox: SubscriberInbox = Arc::new(Mutex::new(VecDeque::new()));
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
        assert!(
            inbox.lock().expect("inbox lock").is_empty(),
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
            // Pop in a scoped statement so the mutex guard is released before the
            // `if let` body (no significant-drop guard held across the scrutinee).
            let next = inbox.lock().expect("inbox lock").pop_front();
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
