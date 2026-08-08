//! THE CONTAINMENT GATE, RED BY DESIGN.
//!
//! Tom's bar, 2026-08-06: *"this whole thing has to be robust beyond belief...
//! the fact that something can kill something on that... constitutes an
//! absolute failure of the project."*
//!
//! The property that bar reduces to, mechanically:
//!
//! > No single unloadable durable record can prevent the node from starting,
//! > and no single unloadable conversation can take down the others. Refuse it,
//! > NAME it, keep serving.
//!
//! The property does not hold today. Boot enumerates every registered
//! conversation and replays each one in a single loop:
//!
//! ```text
//! handler.rs:111 pub fn new -> :137 handler.restore_all_conversations()?;
//! handler.rs:237 restore_all_conversations -> the loop at :239-:280
//! ```
//!
//! Any `?` inside that loop body turns ONE conversation's failure into a
//! constructor failure at `:137`, and the node does not start. The per-request
//! gate at `handler.rs:350` already behaves correctly — it fails exactly one
//! conversation and the node keeps serving — so the defect is the boot loop
//! alone.
//!
//! # THE ROUTE AXIS, AND WHY THIS FILE HAS ONE
//!
//! A gate built only from fault SHAPES is blind to fault ROUTES. Every
//! corrupt-payload arm below trips `:250` first and never reaches the rest of
//! the loop body, so a fix applied at `:250` alone would turn every one of them
//! green while the remaining routes stayed fatal — the fix and its test would
//! agree with each other and both be wrong. So the arms below are chosen by
//! WHERE CONTROL GOES, one per propagation point, and each one proves it
//! actually drove its own route rather than trusting that it did.
//!
//! The loop body carries SIX `?`-bearing statements. THREE of them are live at
//! boot and THREE cannot fire there:
//!
//! | site   | statement                    | at boot                            |
//! |--------|------------------------------|------------------------------------|
//! | `:240` | `self.cell(id)?`             | UNREACHABLE — see below            |
//! | `:243` | owner `lock()` poison        | UNREACHABLE — see below            |
//! | `:250` | `replay_and_repair(..)?`     | LIVE — driven by `CorruptRecordNow`|
//! | `:254` | `evict_uncommitted(..)?`     | UNREACHABLE — see below            |
//! | `:265` | `verdict.observe()?`         | LIVE — driven by `RefuseAppend`    |
//! | `:275` | post-drain `replay_and_repair(..)?` | LIVE — `CorruptAfterAppend` |
//!
//! THE THREE UNREACHABLE ONES ARE NAMED, NOT DROPPED, because a gate that
//! silently omits coverage reads as though it covered everything. `:240` and
//! `:254` both fail only when `self.conversations` is poisoned; `:243` only
//! when a per-conversation cell mutex is poisoned. Every one of those mutexes
//! is constructed inside `new` itself — the map at `handler.rs:127`, each cell
//! at `handler.rs:303` in the same loop iteration that locks it — and
//! `restore_all_conversations` has EXACTLY ONE CALLER, `handler.rs:137`, which
//! runs before `new` returns and before any other thread can hold a reference.
//! A mutex is poisoned only by a panic while it is held, and no such panic can
//! have happened yet. So the three are guards over a state boot cannot be in.
//!
//! ⚠ THAT IS A CLAIM ABOUT THE CURRENT PROGRAM AND IT EXPIRES. The moment
//! anything calls `restore_all_conversations` on a live shared handler — a
//! re-restore, a reload, a second boot phase — all three become live routes and
//! this file is under-covering by half. The one-caller census above is the
//! thing to re-run, not this sentence.
//!
//! # ATTRIBUTION IS PART OF THE SAME PROPERTY, NOT A SECOND TICKET
//!
//! Containment without attribution ships a node that boots clean and silently
//! serves nothing on one conversation forever — worse than the crash, because
//! the crash is the only thing currently telling anyone.
//!
//! Attribution is presently SPLIT across the three live routes, which is itself
//! worth recording: `:265` refuses through
//! `ParticipantSemanticError::BootDrainRefused`, which CARRIES its
//! `conversation_id`; `:250` and `:275` both surface a bare decode failure
//! ("expected value at line 1 column 1") with no conversation anywhere in it.
//! The four sealed-binding-fate refusals one layer down (`ops_acks.rs:590`,
//! `ops_nonzero_ack.rs:424`, `binding_fate_completion.rs:83`,
//! `connection_fate_replay.rs:155`) do the same thing: they name the INVARIANT
//! and never the SUBJECT.
//!
//! # THE FAULTS
//!
//! Every fault here is scoped to EXACTLY ONE conversation's stream and nothing
//! else — the stream is present, its length and sequences are unchanged, and
//! every other conversation reads and writes through verbatim. That is the
//! shape of this week's failures: intact storage, one conversation the node
//! cannot accept. It is deliberately NOT an I/O outage; a store that is wholly
//! gone is a different property and fatal is the right answer to it.
//!
//! These tests are EXPECTED TO FAIL until the containment change lands. They
//! are the red arm for it. Do not weaken the assertions to make them pass.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use liminal::durability::{DurabilityError, DurableStore, StoredEntry, open_ephemeral};
use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, EnrollmentRequest, EnrollmentToken, ServerValue,
};

use crate::config::types::ParticipantConfig;
use crate::server::participant::{ParticipantConnectionConversations, ParticipantSemanticError};

use super::ProductionParticipantHandler;
use super::log::STREAM_PREFIX;
use super::tests::{dispatch_tracked, test_participant_config};
use super::tests_w1b_pending_died_restart::pending_restart_fixture;

pub(super) const POISONED_CONVERSATION: u64 = 7_201;
pub(super) const HEALTHY_CONVERSATION: u64 = 7_202;
const POISONED_TOKEN: [u8; 16] = [0xC1; 16];
const HEALTHY_TOKEN: [u8; 16] = [0xC2; 16];

/// The conversation `pending_restart_fixture` writes. Asserted against the
/// fixture's own field rather than trusted, so a fixture reshape cannot leave
/// this file quietly targeting a stream that no longer exists.
const PENDING_CONVERSATION: u64 = 67;

/// Which single-conversation fault a [`OneConversationFault`] injects.
#[derive(Debug, Clone, Copy)]
pub(super) enum FaultMode {
    /// One record's payload is unloadable from the first read onward, so the
    /// row still reads but no longer decodes. Trips `handler.rs:250`.
    CorruptRecordNow(u64),
    /// Every append to this conversation's stream is refused, so the boot
    /// drain cannot persist the drain it decided on. Trips `handler.rs:265`.
    RefuseAppend,
    /// One record's payload becomes unloadable ONLY AFTER the first append to
    /// this conversation's stream lands. The boot drain's own append is that
    /// trigger, so the FIRST replay reads clean and the post-drain replay does
    /// not. Trips `handler.rs:275` and nothing before it.
    CorruptAfterAppend(u64),
}

/// A durable store that breaks exactly one conversation, exactly one way, and
/// records what it actually did so an arm can prove which route it drove.
#[derive(Debug)]
pub(super) struct OneConversationFault {
    inner: Arc<dyn DurableStore>,
    target_key: String,
    mode: FaultMode,
    /// Set once an append to the target stream has landed.
    armed: Arc<AtomicBool>,
    /// Number of reads this store actually served a corrupted payload into.
    corrupted_reads: Arc<AtomicUsize>,
}

impl OneConversationFault {
    pub(super) fn new(inner: Arc<dyn DurableStore>, conversation_id: u64, mode: FaultMode) -> Self {
        Self {
            inner,
            target_key: format!("{STREAM_PREFIX}{conversation_id}"),
            mode,
            armed: Arc::new(AtomicBool::new(false)),
            corrupted_reads: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The record this store corrupts RIGHT NOW, if any. `CorruptAfterAppend`
    /// answers `None` until its append lands, which is what makes it a route
    /// discriminator rather than a second copy of `CorruptRecordNow`.
    fn armed_sequence(&self) -> Option<u64> {
        match self.mode {
            FaultMode::CorruptRecordNow(sequence) => Some(sequence),
            FaultMode::CorruptAfterAppend(sequence) if self.armed.load(Ordering::SeqCst) => {
                Some(sequence)
            }
            FaultMode::CorruptAfterAppend(_) | FaultMode::RefuseAppend => None,
        }
    }

    fn corrupt(&self, stream_key: &str, mut entries: Vec<StoredEntry>) -> Vec<StoredEntry> {
        if stream_key != self.target_key {
            return entries;
        }
        let Some(target_sequence) = self.armed_sequence() else {
            return entries;
        };
        for entry in &mut entries {
            if entry.sequence == target_sequence {
                entry.payload = vec![0xFF; 16];
                self.corrupted_reads.fetch_add(1, Ordering::SeqCst);
            }
        }
        entries
    }
}

#[async_trait::async_trait]
impl DurableStore for OneConversationFault {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        if stream_key == self.target_key && matches!(self.mode, FaultMode::RefuseAppend) {
            return Err(DurabilityError::SequenceConflict {
                expected: expected_seq,
                actual: expected_seq.saturating_add(1),
            });
        }
        let sequence = self.inner.append(stream_key, payload, expected_seq).await?;
        if stream_key == self.target_key {
            self.armed.store(true, Ordering::SeqCst);
        }
        Ok(sequence)
    }

    async fn read_from(
        &self,
        stream_key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, DurabilityError> {
        let entries = self.inner.read_from(stream_key, offset, limit).await?;
        Ok(self.corrupt(stream_key, entries))
    }

    async fn cas(&self, key: &str, old_value: u64, new_value: u64) -> Result<(), DurabilityError> {
        self.inner.cas(key, old_value, new_value).await
    }

    async fn read_value(&self, key: &str) -> Result<Option<u64>, DurabilityError> {
        self.inner.read_value(key).await
    }

    async fn scan(&self, prefix: &str) -> Result<Vec<StoredEntry>, DurabilityError> {
        self.inner.scan(prefix).await
    }

    async fn flush(&self) -> Result<(), DurabilityError> {
        self.inner.flush().await
    }
}

/// Enrols one conversation through the production request seam.
fn enroll(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    token: [u8; 16],
    connection: ConnectionIncarnation,
) -> Result<ServerValue, Box<dyn Error>> {
    let mut conversations = ParticipantConnectionConversations::default();
    let value = dispatch_tracked(
        handler,
        connection,
        &mut conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new(token),
        }),
    )?;
    Ok(value)
}

/// Seeds two independent conversations through the production request seam and
/// returns the shared store they were written to.
pub(super) fn seed_two_conversations() -> Result<Arc<dyn DurableStore>, Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config())?;

    for (conversation_id, token, incarnation) in [
        (POISONED_CONVERSATION, POISONED_TOKEN, 1_u64),
        (HEALTHY_CONVERSATION, HEALTHY_TOKEN, 2_u64),
    ] {
        let enrolled = enroll(
            &handler,
            conversation_id,
            token,
            ConnectionIncarnation::new(701, incarnation),
        )?;
        if !matches!(enrolled, ServerValue::EnrollBound(_)) {
            return Err(format!("seed enrollment did not bind: {enrolled:?}").into());
        }
    }

    drop(handler);
    Ok(store)
}

/// The write-side shape `pending_restart_fixture` bound. A boot over its bytes
/// must present the same configuration for its replay audits to hold.
fn pending_boot_config() -> ParticipantConfig {
    let mut config = test_participant_config();
    config.max_retained_record_rows = 4;
    config
}

/// Seeds the store the two drain-route arms boot over: one conversation whose
/// immutable-candidate lane holds a pending binding terminal (so boot's drain
/// runs, reaches `:265`, and on success reaches `:275`), plus one ordinary
/// healthy neighbour on the same store to carry the containment assertion.
fn pending_lane_store_with_a_healthy_neighbour() -> Result<Arc<dyn DurableStore>, Box<dyn Error>> {
    let fixture = pending_restart_fixture()?;
    if fixture.conversation_id != PENDING_CONVERSATION {
        return Err(format!(
            "the pending-restart fixture moved to conversation {}; this file targets \
             {PENDING_CONVERSATION} and would otherwise fault a stream nothing reads",
            fixture.conversation_id
        )
        .into());
    }
    let enrolled = enroll(
        &fixture.handler,
        HEALTHY_CONVERSATION,
        HEALTHY_TOKEN,
        ConnectionIncarnation::new(711, 1),
    )?;
    if !matches!(enrolled, ServerValue::EnrollBound(_)) {
        return Err(format!("neighbour enrollment did not bind: {enrolled:?}").into());
    }
    let store = Arc::clone(&fixture.handler.store);
    drop(fixture);
    Ok(store)
}

/// Asserts the healthy neighbour still answers on a booted handler.
fn healthy_neighbour_is_served(
    handler: &ProductionParticipantHandler,
    connection_id: u64,
) -> Result<(), Box<dyn Error>> {
    let served = enroll(
        handler,
        HEALTHY_CONVERSATION,
        [0xC5; 16],
        ConnectionIncarnation::new(connection_id, 1),
    )?;
    assert!(
        matches!(served, ServerValue::EnrollBound(_)),
        "CONTAINMENT: the healthy conversation {HEALTHY_CONVERSATION} stopped serving because \
         another conversation on the same store is unloadable: {served:?}"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// ROUTE handler.rs:250 — replay refuses the conversation's own log
// ─────────────────────────────────────────────────────────────────────────────

/// The control: the identical fixture with NOTHING corrupted.
///
/// Without this arm a failure below would prove only that the fixture cannot
/// boot, not that corruption is what stops it.
#[test]
fn control_uncorrupted_store_boots_and_serves_both_conversations() -> Result<(), Box<dyn Error>> {
    let store = seed_two_conversations()?;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config())?;
    let ids = handler.registered_conversation_ids()?;
    assert!(
        ids.contains(&POISONED_CONVERSATION) && ids.contains(&HEALTHY_CONVERSATION),
        "control boot lost a conversation: {ids:?}"
    );
    Ok(())
}

/// ASSERTION 1 — THE NODE STILL BOOTS.
///
/// Fails today at `handler.rs:250`'s `?`, which climbs to `:137` inside
/// `ProductionParticipantHandler::new`.
#[test]
fn one_unloadable_record_must_not_prevent_the_node_from_starting() -> Result<(), Box<dyn Error>> {
    let inner = seed_two_conversations()?;
    let poisoned: Arc<dyn DurableStore> = Arc::new(OneConversationFault::new(
        inner,
        POISONED_CONVERSATION,
        FaultMode::CorruptRecordNow(0),
    ));

    let outcome = ProductionParticipantHandler::new(poisoned, test_participant_config());

    assert!(
        outcome.is_ok(),
        "CONTAINMENT: one unloadable record on conversation {POISONED_CONVERSATION} took the \
         whole node down. The boot loop at handler.rs:250 propagated a per-conversation replay \
         failure out of a loop over every conversation. Error: {:?}",
        outcome.err()
    );
    Ok(())
}

/// ASSERTION 2 — THE OTHER CONVERSATIONS ARE STILL SERVED.
#[test]
fn one_unloadable_conversation_must_not_take_down_the_others() -> Result<(), Box<dyn Error>> {
    let inner = seed_two_conversations()?;
    let poisoned: Arc<dyn DurableStore> = Arc::new(OneConversationFault::new(
        inner,
        POISONED_CONVERSATION,
        FaultMode::CorruptRecordNow(0),
    ));

    let handler = ProductionParticipantHandler::new(poisoned, test_participant_config())
        .map_err(|error| format!("node did not boot (assertion 1 already covers this): {error}"))?;

    healthy_neighbour_is_served(&handler, 702)
}

/// ASSERTION 3 — THE REFUSAL NAMES THE CONVERSATION IT REFUSED.
///
/// This is the assertion that stops containment becoming a silent-success
/// property. A node that boots clean and serves nothing on one conversation,
/// without ever naming it, is worse than the crash it replaced.
#[test]
fn the_refusal_must_name_the_conversation_it_refused() -> Result<(), Box<dyn Error>> {
    let inner = seed_two_conversations()?;
    let poisoned: Arc<dyn DurableStore> = Arc::new(OneConversationFault::new(
        inner,
        POISONED_CONVERSATION,
        FaultMode::CorruptRecordNow(0),
    ));

    let handler = ProductionParticipantHandler::new(poisoned, test_participant_config())
        .map_err(|error| format!("node did not boot (assertion 1 already covers this): {error}"))?;

    let refusal = enroll(
        &handler,
        POISONED_CONVERSATION,
        [0xC4; 16],
        ConnectionIncarnation::new(703, 1),
    );

    let error = match refusal {
        Ok(value) => {
            return Err(format!(
                "the poisoned conversation answered a request instead of refusing: {value:?}"
            )
            .into());
        }
        Err(error) => error.to_string(),
    };

    assert!(
        error.contains(&POISONED_CONVERSATION.to_string()),
        "ATTRIBUTION: the refusal names the invariant and not its subject. Nothing in this \
         message identifies conversation {POISONED_CONVERSATION}: {error}"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// ROUTE handler.rs:265 — the boot drain's verdict refuses the boot
// ─────────────────────────────────────────────────────────────────────────────

/// The control for BOTH drain-route arms: the pending-terminal fixture plus its
/// healthy neighbour boots clean with no fault at all.
///
/// This is what makes the two reds below mean something. Without it they would
/// prove only that a lane-holding fixture and an ordinary enrolment cannot
/// share a store.
#[test]
fn control_pending_terminal_lane_and_neighbour_boot_clean() -> Result<(), Box<dyn Error>> {
    let store = pending_lane_store_with_a_healthy_neighbour()?;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), pending_boot_config())?;
    let ids = handler.registered_conversation_ids()?;
    assert!(
        ids.contains(&PENDING_CONVERSATION) && ids.contains(&HEALTHY_CONVERSATION),
        "control boot lost a conversation: {ids:?}"
    );
    healthy_neighbour_is_served(&handler, 712)
}

/// ROUTE `handler.rs:265` — one conversation whose boot drain cannot persist
/// takes the whole node down.
///
/// The drain decides on a drain it cannot append, so the verdict is
/// `RefusedShape` and `verdict.observe()?` propagates out of the loop. A fix
/// applied at `:250` alone does not touch this route.
#[test]
fn a_refused_boot_drain_must_not_take_down_the_node() -> Result<(), Box<dyn Error>> {
    let inner = pending_lane_store_with_a_healthy_neighbour()?;
    let faulted: Arc<dyn DurableStore> = Arc::new(OneConversationFault::new(
        inner,
        PENDING_CONVERSATION,
        FaultMode::RefuseAppend,
    ));

    match ProductionParticipantHandler::new(faulted, pending_boot_config()) {
        Ok(handler) => healthy_neighbour_is_served(&handler, 713),
        Err(ParticipantSemanticError::BootDrainRefused {
            conversation_id,
            refusal,
            reason,
            ..
        }) => {
            panic!(
                "CONTAINMENT: the boot drain refused conversation {conversation_id} \
                 ({refusal:?}: {reason}) and handler.rs:265 propagated that refusal out of a loop \
                 over every conversation, so the whole node did not start."
            )
        }
        Err(other) => Err(format!(
            "ROUTE AXIS BROKEN, NOT A CONTAINMENT FAILURE: this arm exists to drive \
             handler.rs:265, the boot-drain verdict, but the fault took a different route and \
             surfaced as: {other}. Redesign the fault — do not read this as coverage of :265."
        )
        .into()),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ROUTE handler.rs:275 — the post-drain replay refuses
// ─────────────────────────────────────────────────────────────────────────────

/// THE ROUTE DISCRIMINATOR, PROVED IN THE NEGATIVE DIRECTION.
///
/// `CorruptAfterAppend` must be INERT on a store where boot appends nothing:
/// no append, no arming, no corrupted read, clean boot. That is what licenses
/// the arm below to conclude "this failure is at `:275`" from a decode error
/// that reads identically to `:250`'s.
#[test]
fn control_a_deferred_fault_is_inert_when_boot_appends_nothing() -> Result<(), Box<dyn Error>> {
    let inner = seed_two_conversations()?;
    let fault = Arc::new(OneConversationFault::new(
        inner,
        POISONED_CONVERSATION,
        FaultMode::CorruptAfterAppend(0),
    ));
    let armed = Arc::clone(&fault.armed);
    let corrupted_reads = Arc::clone(&fault.corrupted_reads);
    let store: Arc<dyn DurableStore> = fault;

    ProductionParticipantHandler::new(store, test_participant_config()).map_err(|error| {
        format!("a deferred fault that never armed still broke the boot: {error}")
    })?;

    assert!(
        !armed.load(Ordering::SeqCst),
        "the deferred fault armed on a boot that was supposed to append nothing"
    );
    assert_eq!(
        corrupted_reads.load(Ordering::SeqCst),
        0,
        "the deferred fault served a corrupted read before it was armed"
    );
    Ok(())
}

/// ROUTE `handler.rs:275` — a conversation whose log stops being loadable
/// BECAUSE OF BOOT'S OWN DRAIN APPEND takes the whole node down.
///
/// The first replay reads clean, the drain appends, the fault arms, and the
/// post-drain `replay_and_repair` at `:275` refuses. Its error text is
/// indistinguishable from `:250`'s, so the route is proved by the fault's own
/// instrumentation instead: it armed (the drain appended) and it served a
/// corrupted read (a reader consumed the corruption after that point), which
/// together place the failure after the drain and nowhere else.
#[test]
fn a_post_drain_replay_failure_must_not_take_down_the_node() -> Result<(), Box<dyn Error>> {
    let inner = pending_lane_store_with_a_healthy_neighbour()?;
    let fault = Arc::new(OneConversationFault::new(
        inner,
        PENDING_CONVERSATION,
        FaultMode::CorruptAfterAppend(0),
    ));
    let armed = Arc::clone(&fault.armed);
    let corrupted_reads = Arc::clone(&fault.corrupted_reads);
    let store: Arc<dyn DurableStore> = fault;

    let outcome = ProductionParticipantHandler::new(store, pending_boot_config());

    // Route proof FIRST: if the fault never armed, or armed but was never
    // read, this arm did not reach `:275` and its verdict means nothing either
    // way.
    if !armed.load(Ordering::SeqCst) {
        return Err(format!(
            "ROUTE AXIS BROKEN, NOT A CONTAINMENT FAILURE: boot never appended to conversation \
             {PENDING_CONVERSATION}, so the deferred fault never armed and handler.rs:275 was \
             never reached. Boot outcome was {:?}. Redesign the fixture — do not read this as \
             coverage of :275.",
            outcome.as_ref().err()
        )
        .into());
    }
    if corrupted_reads.load(Ordering::SeqCst) == 0 {
        return Err(format!(
            "ROUTE AXIS BROKEN, NOT A CONTAINMENT FAILURE: the fault armed but nothing read the \
             corrupted record afterwards, so handler.rs:275 did not consume it. Boot outcome was \
             {:?}.",
            outcome.as_ref().err()
        )
        .into());
    }

    let handler = match outcome {
        Ok(handler) => handler,
        Err(error) => panic!(
            "CONTAINMENT: conversation {PENDING_CONVERSATION} became unloadable through boot's \
             OWN drain append, and handler.rs:275 propagated that out of a loop over every \
             conversation, so the whole node did not start. A fix at handler.rs:250 alone leaves \
             this route fatal. Error: {error}"
        ),
    };
    healthy_neighbour_is_served(&handler, 714)
}
