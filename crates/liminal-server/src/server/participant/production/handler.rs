//! The single production participant semantic handler.
//!
//! One handler per server. Each conversation has exactly one live in-memory
//! authority owner, rebuilt from its durable transition-input log on first
//! touch (and after any failed operation, which discards the owner so the
//! next touch cold-replays durable reality). A short registry lock selects
//! the conversation cell; a per-conversation lock covers replay and each
//! operation. Everything is event-driven: cells are created on request
//! arrival, discarded on error, and evicted entirely when the touched
//! conversation has no durable log — no timer, sweep, or polling loop
//! exists, and refused probes of unknown conversation ids leave neither
//! durable nor in-memory residue.

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use liminal::durability::DurableStore;
use liminal::durability::bridge::block_on;
use liminal_protocol::lifecycle::{CapacityCounter, ObserverRecoveryAggregate};
use liminal_protocol::wire::ConversationId;

use crate::config::types::ParticipantConfig;
use crate::health::reissue::{
    OperatorCredentialReissueError, OperatorCredentialReissueOutcome,
    OperatorCredentialReissueRequest, OperatorCredentialReissuer,
};
use crate::health::unloadable::{UnloadableConversation, UnloadableConversationRecord};
use crate::server::participant::{
    ObserverPublicationTarget, ParticipantConnectionContext, ParticipantConnectionConversations,
    ParticipantSemanticError, ParticipantServiceFatal, dispatch_impact::DispatchImpactAccumulator,
};

use super::barrier::{OperationFacts, ReceiptCapacityLimits};
use super::capacity::ServerCapacity;
#[cfg(test)]
use super::dispatch_work::{ObligationDispatchWorkCounters, ObligationDispatchWorkSnapshot};
use super::facts;
use super::log::{OperationLog, OperationLogError, StoredOperation};
use super::outbox::ConversationOutboxLimits;
use super::outbox_log::{OutboxLog, OutboxLogError};
use super::outbox_projection::owes_extension_row;
use super::outbox_replay::RestoreError;
use super::registry::ConversationRegistry;
use super::state::{ConversationAuthority, DurableAppend, StateError};

#[derive(Debug)]
pub(super) struct ObserverArmTarget {
    pub(super) refused_epoch: u64,
    pub(super) connection_incarnation: liminal_protocol::wire::ConnectionIncarnation,
    pub(super) target: ObserverPublicationTarget,
}

/// One exclusively serialized observer owner: durable protocol aggregate/head
/// plus volatile weak live targets for its installed arms. Restoring durable
/// arms intentionally starts with an empty target map because no socket
/// survives a process restart.
#[derive(Debug)]
pub(super) struct ObserverOwner {
    pub(super) aggregate: ObserverRecoveryAggregate,
    pub(super) head: u64,
    pub(super) arm_targets: BTreeMap<ConversationId, ObserverArmTarget>,
}

/// Production semantic handler backed by the shared durable store.
///
/// Constructed exactly once per server by the connection-services layer when
/// the deployment's `[participant]` configuration is present.
#[derive(Debug)]
pub struct ProductionParticipantHandler {
    pub(super) store: Arc<dyn DurableStore>,
    pub(super) config: ParticipantConfig,
    outbox_limits: ConversationOutboxLimits,
    conversations: Mutex<HashMap<ConversationId, Arc<Mutex<Option<ConversationAuthority>>>>>,
    /// First post-Open fatal; once set, every semantic/publication entry refuses.
    service_fatal: Mutex<Option<ParticipantServiceFatal>>,
    /// Server-wide observer-recovery aggregate paired with its durable row
    /// head (`None` until first restored).
    pub(super) observer: Mutex<Option<ObserverOwner>>,
    /// Server-scope stage-8 occupancy ledger (identity slots, live receipts,
    /// provenance fingerprints), restored from every durable conversation at
    /// construction and kept exact by commit reservations and replay folds.
    pub(super) capacity: ServerCapacity,
    /// Durable registry of created conversations: one row appended before
    /// each conversation's genesis append, read at startup to enumerate
    /// every durable conversation for the capacity restore.
    pub(super) registry: ConversationRegistry,
    /// CONTAINMENT: conversations whose durable state could not be loaded,
    /// each against the load failure's own class and text.
    ///
    /// A conversation lands here instead of taking the node down with it. The
    /// record is the node's answer to "which one is broken?", which is the half
    /// of the property that a boot-succeeds assertion cannot see: containment
    /// without attribution is a node that serves nothing on one conversation
    /// forever and never says so.
    ///
    /// The record is SHARED, not private: a clone of it is published onto the
    /// health endpoint's `GET /unloadable-conversations` at server startup
    /// ([`Self::unloadable_record`]), so the answer this handler writes is the
    /// same answer an operator reads. Nothing pushes — the endpoint snapshots
    /// this record only when it is scraped.
    unloadable: UnloadableConversationRecord,
    /// Exact W2 work observation points, isolated per handler and test-only.
    #[cfg(test)]
    pub(super) obligation_dispatch_work: ObligationDispatchWorkCounters,
    /// Harness-owned clock override for deterministic time-window tests.
    ///
    /// `0` is the released/unset state, under which [`Self::now_ms`] reads the
    /// live wall clock exactly as production does; any nonzero value pins the
    /// participant clock to that fixed Unix-millisecond reading so a test can
    /// step through the deterministic receipt/provenance window states instead
    /// of racing the wall clock. This field and every branch that consults it
    /// are `#[cfg(test)]`-only — production never compiles, observes, or is
    /// slowed by it, and the non-test clock path is byte-identical to a direct
    /// [`facts::now_unix_millis`] call.
    #[cfg(test)]
    now_override: AtomicU64,
}

impl ProductionParticipantHandler {
    /// Creates the handler over the server's shared durable store, replaying
    /// every durable conversation so the server-scope capacity ledger is
    /// exact against durable truth from the first request (a restart must
    /// not forget reserved identity slots or in-window receipts).
    ///
    /// # Errors
    ///
    /// Returns [`ParticipantSemanticError`] when the durable store cannot be
    /// scanned or a conversation log fails replay — the server refuses to
    /// start over state it cannot account for.
    pub fn new(
        store: Arc<dyn DurableStore>,
        config: ParticipantConfig,
    ) -> Result<Self, ParticipantSemanticError> {
        let outbox_limits = ConversationOutboxLimits::try_new(
            config.max_retained_record_rows,
            config.identity_slots,
        )
        .map_err(|error| ParticipantSemanticError::Internal {
            message: format!("participant outbox limit configuration failed: {error}"),
        })?;
        let registry = ConversationRegistry::new(Arc::clone(&store));
        let handler = Self {
            store,
            config,
            outbox_limits,
            conversations: Mutex::new(HashMap::new()),
            service_fatal: Mutex::new(None),
            observer: Mutex::new(None),
            capacity: ServerCapacity::default(),
            registry,
            unloadable: UnloadableConversationRecord::default(),
            #[cfg(test)]
            obligation_dispatch_work: ObligationDispatchWorkCounters::default(),
            #[cfg(test)]
            now_override: AtomicU64::new(0),
        };
        handler.restore_all_conversations()?;
        Ok(handler)
    }

    /// Reads the participant clock as Unix milliseconds.
    ///
    /// Production builds forward directly to [`facts::now_unix_millis`]; the
    /// non-test compilation of this method is byte-identical to the direct
    /// call the two call sites (owner-restore expiry and operation facts)
    /// previously made.
    ///
    /// `&self` is unused here but retained deliberately so both compilations
    /// share the one `self.now_ms()` call form; the `#[cfg(test)]` sibling
    /// reads `self.now_override`.
    #[cfg(not(test))]
    #[allow(clippy::unused_self)]
    fn now_ms(&self) -> Result<u64, facts::FactsError> {
        facts::now_unix_millis()
    }

    /// Reads the participant clock as Unix milliseconds, honouring the
    /// harness-owned [`Self::now_override`] pin.
    ///
    /// When the override is released (`0`) this is exactly the production
    /// wall-clock read; when a test has pinned it, the fixed reading is
    /// returned so deterministic time-window tests never race the wall clock.
    /// Test-only: production compiles the `#[cfg(not(test))]` sibling, which
    /// has no branch and no field access.
    #[cfg(test)]
    fn now_ms(&self) -> Result<u64, facts::FactsError> {
        match self.now_override.load(Ordering::SeqCst) {
            0 => facts::now_unix_millis(),
            fixed => Ok(fixed),
        }
    }

    /// Pins the participant clock to a fixed Unix-millisecond reading, or
    /// releases it back to the wall clock when passed `0`.
    ///
    /// Harness-owned and test-only: it drives [`Self::now_override`], which
    /// exists only under `#[cfg(test)]`. Time-window tests set an explicit
    /// base and step it across the deterministic window states they assert.
    #[cfg(test)]
    pub(super) fn pin_clock_ms(&self, now_ms: u64) {
        self.now_override.store(now_ms, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn obligation_dispatch_work_snapshot(&self) -> ObligationDispatchWorkSnapshot {
        self.obligation_dispatch_work.snapshot()
    }

    pub(super) fn current_service_fatal(
        &self,
    ) -> Result<Option<ParticipantServiceFatal>, ParticipantSemanticError> {
        let fatal = self
            .service_fatal
            .lock()
            .map_err(|_| ParticipantSemanticError::Internal {
                message: "participant service fatal latch is poisoned".to_owned(),
            })?
            .clone();
        Ok(fatal)
    }

    pub(super) fn ensure_service_live(&self) -> Result<(), ParticipantSemanticError> {
        if let Some(fatal) = self.current_service_fatal()? {
            return Err(ParticipantSemanticError::ServiceFatal(fatal));
        }
        Ok(())
    }

    pub(super) fn latch_connection_fate_fatal(
        &self,
        open_sequence: u64,
        conversation_id: ConversationId,
    ) -> Result<ParticipantServiceFatal, ParticipantSemanticError> {
        let mut fatal =
            self.service_fatal
                .lock()
                .map_err(|_| ParticipantSemanticError::Internal {
                    message: "participant service fatal latch is poisoned".to_owned(),
                })?;
        let selected = fatal
            .get_or_insert_with(|| ParticipantServiceFatal::ConnectionFateIntentIncomplete {
                open_sequence,
                conversation_id,
            })
            .clone();
        drop(fatal);
        Ok(selected)
    }

    /// Startup restore: enumerates every registered conversation and replays
    /// it, folding each conversation's server-scope contribution into the
    /// capacity ledger.
    ///
    /// A registry row whose conversation never got its genesis append (the
    /// crash window between the two ordered appends) replays empty and is
    /// evicted exactly like a refused probe.
    fn restore_all_conversations(&self) -> Result<(), ParticipantSemanticError> {
        // THE REGISTRY READ STAYS FATAL, and that is the property stated
        // precisely rather than an exception carved out of it. Containment
        // here is PER-CONVERSATION containment: without the registry read
        // there is no per-conversation subject to attribute a failure to, so a
        // failure with no conversation to name is the one thing that still
        // should stop the node.
        let conversation_ids = self.registry.restore().map_err(|error| log_error(&error))?;
        for conversation_id in conversation_ids {
            // ONE FALLIBLE UNIT, not a guard per `?`. A guard per `?` is the
            // same defect with a longer fuse: the next `?` added to the
            // restore body is fatal again and nothing complains.
            if let Err(error) = self.restore_one_conversation(conversation_id) {
                self.record_unloadable(conversation_id, &error);
            }
        }
        Ok(())
    }

    /// Restores exactly one conversation, and fails as exactly one
    /// conversation.
    ///
    /// Every propagation point in here belongs to a single conversation, so
    /// its `Err` is that conversation's answer and nobody else's. The caller
    /// is what makes that true — this function is deliberately allowed to be
    /// fallible, and deliberately not allowed to be fatal.
    fn restore_one_conversation(
        &self,
        conversation_id: ConversationId,
    ) -> Result<(), ParticipantSemanticError> {
        {
            let cell = self.cell(conversation_id)?;
            let mut owner = cell
                .lock()
                .map_err(|_| ParticipantSemanticError::Internal {
                    message: format!(
                        "participant conversation {conversation_id} owner lock is poisoned"
                    ),
                })?;
            if owner.is_none() {
                let log = OperationLog::new(Arc::clone(&self.store), conversation_id);
                let mut replayed = self.replay_and_repair(conversation_id, &log)?;
                let durably_empty = replayed.next_log_sequence == 0;
                if durably_empty {
                    drop(owner);
                    self.evict_uncommitted(conversation_id, &cell)?;
                    return Ok(());
                }
                // R-BOOT-DRAIN (F8B §6.2): empty this conversation's
                // immutable-candidate lane before any retained connection-fate
                // `Open` replays, and before the owner is installed. The drain
                // lives HERE and not in `replay_and_repair`, which has four
                // live non-boot callers whose behaviour must not change.
                let verdict =
                    self.drain_restored_candidate_lane(conversation_id, &mut replayed, &log);
                let drained = verdict.drained_any();
                verdict.observe()?;
                if drained {
                    // The drain appended durable rows, so the restored owner is
                    // now behind its own log: it recorded new observer-progress
                    // projections and it released a binding slot the capacity
                    // ledger was folded against. Re-running `replay_and_repair`
                    // performs BOTH repairs — the observer reconciliation at
                    // :464-477 and the capacity re-fold at :484-496 — from
                    // durable truth, exactly as every other post-append
                    // reconciliation in this handler does.
                    replayed = self.replay_and_repair(conversation_id, &log)?;
                }
                *owner = Some(replayed);
            }
            drop(owner);
        }
        Ok(())
    }

    /// Records one conversation as unloadable and renders the NAMED refusal
    /// every later request on it is answered with.
    ///
    /// This is the attribution half of containment, and it is deliberately the
    /// same call on both sides: the boot loop records and continues, the
    /// request path records and refuses, and both produce a refusal carrying
    /// the conversation id. An operator therefore never has to correlate a
    /// bare decode failure against a log to learn which conversation stopped.
    fn record_unloadable(
        &self,
        conversation_id: ConversationId,
        error: &ParticipantSemanticError,
    ) -> ParticipantSemanticError {
        let reason = error.to_string();
        let class = error.class();
        // The operator's first tell, and it carries the class as its own field:
        // the refusal text of a failed load ("expected value at line 1 column
        // 1") names no class at all, so a reader with only the message has to
        // discriminate on a substring or not at all.
        tracing::error!(
            conversation_id,
            class,
            reason = %reason,
            "CONTAINMENT: participant conversation is unloadable and is refused on its own; every \
             other conversation keeps being served"
        );
        if !self.unloadable.record(UnloadableConversation {
            conversation_id,
            class,
            reason: reason.clone(),
        }) {
            // Never a silent drop: a refusal that is reported but not retained
            // is a different, weaker state than one that was retained, and an
            // operator reading `GET /unloadable-conversations` must not be told
            // a shorter story than the log tells.
            tracing::error!(
                conversation_id,
                "the unloadable-conversation record is poisoned; this refusal is reported but not \
                 retained"
            );
        }
        ParticipantSemanticError::ConversationUnloadable {
            conversation_id,
            reason,
        }
    }

    /// Retires an unloadable record once the conversation has actually loaded,
    /// so the map never keeps reporting a conversation that recovered.
    fn clear_unloadable(&self, conversation_id: ConversationId) {
        if !self.unloadable.retire(conversation_id) {
            tracing::error!(
                conversation_id,
                "the unloadable-conversation record is poisoned; a conversation that recovered may \
                 still be reported as unloadable"
            );
        }
    }

    /// The shared refused-load record, for publication onto the operator read
    /// surface.
    ///
    /// The clone shares the record rather than copying it, so a refusal
    /// recorded after publication is still the answer the surface gives. This
    /// is the only way out of the handler for the containment record, and the
    /// server's startup path (`server/runtime.rs`) is its one production
    /// caller.
    pub(crate) fn unloadable_record(&self) -> UnloadableConversationRecord {
        self.unloadable.clone()
    }

    /// Every conversation this node has refused as unloadable, against the
    /// load failure's own text.
    ///
    /// In-process view for the tests that assert on containment attribution.
    /// The operator's view of the same record is
    /// `GET /unloadable-conversations`, served from the clone published by
    /// [`Self::unloadable_record`]; this accessor exists so a test can read the
    /// record without standing up an HTTP endpoint.
    #[cfg(test)]
    pub(super) fn unloadable_conversations(&self) -> BTreeMap<ConversationId, String> {
        self.unloadable
            .snapshot()
            .into_iter()
            .map(|entry| (entry.conversation_id, entry.reason))
            .collect()
    }

    pub(super) fn registered_conversation_ids(
        &self,
    ) -> Result<Vec<ConversationId>, ParticipantSemanticError> {
        self.registry.restore().map_err(|error| log_error(&error))
    }

    pub(super) fn cell(
        &self,
        conversation_id: ConversationId,
    ) -> Result<Arc<Mutex<Option<ConversationAuthority>>>, ParticipantSemanticError> {
        let mut conversations =
            self.conversations
                .lock()
                .map_err(|_| ParticipantSemanticError::Internal {
                    message: "participant conversation registry lock is poisoned".to_owned(),
                })?;
        let cell = Arc::clone(
            conversations
                .entry(conversation_id)
                .or_insert_with(|| Arc::new(Mutex::new(None))),
        );
        drop(conversations);
        Ok(cell)
    }

    pub(super) fn with_conversation_impact<T>(
        &self,
        conversation_id: ConversationId,
        impact: &mut DispatchImpactAccumulator,
        operation: impl FnOnce(
            &mut ConversationAuthority,
            &dyn DurableAppend,
            &mut DispatchImpactAccumulator,
        ) -> Result<T, StateError>,
    ) -> Result<T, ParticipantSemanticError> {
        self.with_conversation_reconciliation(
            conversation_id,
            true,
            Some(impact),
            |authority, appender, impact| {
                impact.map_or_else(
                    || Err(StateError::invariant("impact owner is unavailable")),
                    |impact| operation(authority, appender, impact),
                )
            },
        )
    }

    /// Runs a fate-source append and reconciles its exact Unit 2 projection before
    /// retaining the transitioned live owner. Died/Detached joined the exhaustive
    /// v3 replay projection pass with W1b, so this boundary must use the same
    /// post-append repair as semantic sources to keep live and cold owners equal.
    pub(super) fn with_conversation_fate_source<T>(
        &self,
        conversation_id: ConversationId,
        impact: Option<&mut DispatchImpactAccumulator>,
        operation: impl FnOnce(
            &mut ConversationAuthority,
            &dyn DurableAppend,
            Option<&mut DispatchImpactAccumulator>,
        ) -> Result<T, StateError>,
    ) -> Result<T, ParticipantSemanticError> {
        self.with_conversation_reconciliation(conversation_id, true, impact, operation)
    }

    fn with_conversation_reconciliation<T>(
        &self,
        conversation_id: ConversationId,
        reconcile_appended_source: bool,
        mut impact: Option<&mut DispatchImpactAccumulator>,
        operation: impl FnOnce(
            &mut ConversationAuthority,
            &dyn DurableAppend,
            Option<&mut DispatchImpactAccumulator>,
        ) -> Result<T, StateError>,
    ) -> Result<T, ParticipantSemanticError> {
        let cell = self.cell(conversation_id)?;
        let mut owner: MutexGuard<'_, Option<ConversationAuthority>> =
            cell.lock()
                .map_err(|_| ParticipantSemanticError::Internal {
                    message: format!(
                        "participant conversation {conversation_id} owner lock is poisoned"
                    ),
                })?;
        let log = OperationLog::new(Arc::clone(&self.store), conversation_id);
        let outbox_log = OutboxLog::new(Arc::clone(&self.store), conversation_id);
        if owner.is_none() {
            // The request-side half of containment. A conversation the node
            // cannot load refuses HERE, by name, instead of surfacing a bare
            // decode failure that names no subject at all.
            let replayed = self
                .replay_and_repair(conversation_id, &log)
                .map_err(|error| self.record_unloadable(conversation_id, &error))?;
            self.clear_unloadable(conversation_id);
            *owner = Some(replayed);
        }
        let Some(authority) = owner.as_mut() else {
            return Err(ParticipantSemanticError::Internal {
                message: format!("participant conversation {conversation_id} owner is absent"),
            });
        };
        let appender = LogAppender {
            log: &log,
            registry: &self.registry,
            conversation_id,
            outbox_log: &outbox_log,
            outstanding_extension_rows: Cell::new(0),
        };
        let starting_log_sequence = authority.next_log_sequence;
        let operation_result = operation(authority, &appender, impact.as_deref_mut());
        let (result, durably_empty) = match operation_result {
            Ok(value)
                if reconcile_appended_source
                    && authority.next_log_sequence > starting_log_sequence =>
            {
                // The v2 source barrier crossed. Complete its exact Unit 2
                // projection under this same conversation lock before the
                // caller can publish the correlated terminal response.
                match self.complete_appended_source(conversation_id, &log, authority, &appender) {
                    Ok(Some(reconciled)) => {
                        let durably_empty = reconciled.next_log_sequence == 0;
                        if let Some(impact) = impact.as_deref_mut() {
                            impact.install_staged();
                        }
                        *owner = Some(reconciled);
                        (Ok(value), durably_empty)
                    }
                    Ok(None) => {
                        if let Some(impact) = impact.as_deref_mut() {
                            impact.install_staged();
                        }
                        let durably_empty = authority.next_log_sequence == 0;
                        (Ok(value), durably_empty)
                    }
                    Err(error) => {
                        *owner = None;
                        (Err(error), false)
                    }
                }
            }
            Ok(value) => {
                if let Some(impact) = impact.as_deref_mut() {
                    impact.install_staged();
                }
                let durably_empty = authority.next_log_sequence == 0;
                (Ok(value), durably_empty)
            }
            Err(error) => {
                let mut durably_empty = authority.next_log_sequence == 0;
                let staged = impact
                    .as_deref()
                    .is_some_and(DispatchImpactAccumulator::has_staged);
                if staged {
                    match self.replay_and_repair(conversation_id, &log) {
                        Ok(reconciled) => {
                            durably_empty = reconciled.next_log_sequence == 0;
                            *owner = Some(reconciled);
                            if let Some(impact) = impact.as_mut() {
                                impact.install_staged();
                            }
                        }
                        Err(_) => {
                            *owner = None;
                        }
                    }
                } else {
                    // No committed prefix awaits a tell. Discard the possibly
                    // part-consumed owner and replay durable truth next touch.
                    *owner = None;
                }
                (Err(state_error(&error)), durably_empty)
            }
        };
        drop(owner);
        if durably_empty {
            self.evict_uncommitted(conversation_id, &cell)?;
        }
        result
    }

    /// Completes the sources this operation appended, and says whether the
    /// owner was replaced.
    ///
    /// Board #60 §3c. `Ok(None)` means every appended source ALREADY wrote its
    /// own Unit 2 extension row and applied it to the live outbox owner
    /// (`record_produced_source`), so the from-zero replay is no longer the
    /// writer of anything: the commit owed only the load-end reconciles the
    /// replay used to carry along with it, and the live owner stands.
    /// `Ok(Some(owner))` is the replay's answer, taken whenever that is not
    /// PROVABLY true — a nonzero owed-row count means some appended source's
    /// row is still missing, which is exactly the shape the repair branch
    /// answers. The gate is an outcome, not a prediction: it reads what the
    /// operation actually did.
    fn complete_appended_source(
        &self,
        conversation_id: ConversationId,
        log: &OperationLog,
        authority: &mut ConversationAuthority,
        appender: &LogAppender<'_>,
    ) -> Result<Option<ConversationAuthority>, ParticipantSemanticError> {
        if appender.outstanding_extension_rows() == 0 {
            self.complete_live_commit(conversation_id, authority, appender)?;
            return Ok(None);
        }
        self.replay_and_repair(conversation_id, log).map(Some)
    }

    /// Completes one in-process commit without re-deriving the prefix.
    ///
    /// Board #60 §3c. This is the tail of [`Self::replay_and_repair`] applied
    /// to the live owner — the same calls, in the same order, minus the three
    /// passes whose only job was to rebuild a prefix this owner never lost:
    ///
    /// - `repair_pending_specific_fates` is KEPT. It is not a load-time repair
    ///   in disguise: a live Died/Detached commit can leave a pending specific
    ///   fate whose finalizer must be appended before the response publishes,
    ///   and today's post-append replay is what performs it.
    /// - `reconcile_observer_progress` is KEPT, over the owner's OWN witness
    ///   vector, which is now retained rather than drained. The planner reads
    ///   the whole source history (it validates the durable observer prefix
    ///   against it), and the retained vector is that history — the same
    ///   vector, source for source, that a from-zero replay would rebuild.
    /// - `prune_expired_provenance` and the capacity fold are KEPT: both are
    ///   per-touch clock work over the current owner, not prefix derivation.
    ///
    /// Deliberately NOT carried over: `validate_operation_schema`, the base-log
    /// replay, the extension merge, `validate_replayed_seal` and
    /// `reconcile_load_end_marker_anchors`. The first three re-derive a prefix
    /// the live owner already holds; the last two are load-end repairs of crash
    /// residue, and a source committed in-process under this lock has no crash
    /// residue to repair.
    fn complete_live_commit(
        &self,
        conversation_id: ConversationId,
        authority: &mut ConversationAuthority,
        appender: &LogAppender<'_>,
    ) -> Result<(), ParticipantSemanticError> {
        authority
            .repair_pending_specific_fates(appender)
            .map_err(|error| state_error(&error))?;
        let observer_progress = authority.observer_progress;
        let witnesses = authority.observer_progress_witnesses();
        if !authority.tokens.is_empty() || authority.is_closed() {
            self.reconcile_observer_progress(conversation_id, witnesses, observer_progress)?;
        } else if !witnesses.is_empty() {
            return Err(ParticipantSemanticError::Internal {
                message: format!(
                    "unenrolled conversation {conversation_id} projected observer progress"
                ),
            });
        }
        let now = self
            .now_ms()
            .map_err(|error| ParticipantSemanticError::Internal {
                message: format!("participant clock read failed: {error}"),
            })?;
        let now = u128::from(now);
        authority.prune_expired_provenance(now);
        let contribution = authority
            .capacity_contribution(now)
            .map_err(|error| state_error(&error))?;
        self.capacity
            .fold_conversation(conversation_id, contribution)
            .map_err(|error| state_error(&error))?;
        Ok(())
    }

    /// Cold-replays one conversation's durable log and repairs its observer
    /// registration.
    ///
    /// An enrolled conversation whose durable observer `Track` row was lost
    /// to a crash between the enrollment append and the tracking append is
    /// re-registered idempotently here, so observer recovery is derivable
    /// from the conversation log itself on any first touch.
    pub(super) fn replay_and_repair(
        &self,
        conversation_id: ConversationId,
        log: &OperationLog,
    ) -> Result<ConversationAuthority, ParticipantSemanticError> {
        let outbox_log = OutboxLog::new(Arc::clone(&self.store), conversation_id);
        block_on(outbox_log.restore_cursor().validate_all())
            .map_err(|error| bridge_error(&error))?
            .map_err(|error| outbox_log_error(&error))?;
        let mut replayed = block_on(ConversationAuthority::replay(
            conversation_id,
            log,
            &outbox_log,
            &self.config,
            self.outbox_limits,
        ))
        .map_err(|error| bridge_error(&error))?
        .map_err(|error| match error {
            RestoreError::Extension(error) => outbox_log_error(&error),
            RestoreError::Semantic(error) => state_error(&error),
        })?;
        let appender = LogAppender {
            log,
            registry: &self.registry,
            conversation_id,
            outbox_log: &outbox_log,
            outstanding_extension_rows: Cell::new(0),
        };
        replayed
            .repair_pending_specific_fates(&appender)
            .map_err(|error| state_error(&error))?;
        let observer_witnesses = replayed.observer_progress_witnesses();
        // F8B R-SEAL (§6.6). This is the THIRD site that reads `tokens` empty
        // as "never enrolled" — the two the ruling names are the replay
        // invariant twins. A Closed conversation is the state that proxy never
        // anticipated: no tokens, but a log full of records, terminals and
        // drain rows whose observer-progress witnesses are durable truth. It
        // must reconcile exactly as an enrolled conversation does, or the
        // handler's observer state is left behind its own durable log — the
        // failure §6.2's post-drain reconciliation exists to prevent. Bare
        // tokens-empty-with-witnesses REMAINS a refusal for the never-enrolled
        // shape.
        if !replayed.tokens.is_empty() || replayed.is_closed() {
            self.reconcile_observer_progress(
                conversation_id,
                observer_witnesses,
                replayed.observer_progress,
            )?;
        } else if !observer_witnesses.is_empty() {
            return Err(ParticipantSemanticError::Internal {
                message: format!(
                    "unenrolled conversation {conversation_id} projected observer progress"
                ),
            });
        }
        // Request-time expiry over the replayed state (replay rebuilds every
        // retired rotation's fingerprint; the ones past their deadlines are
        // dropped under this touch's clock read), then fold the
        // conversation's complete server-scope contribution into the ledger
        // — a replace, so a discarded-and-replayed owner never double
        // counts and the ledger self-heals from durable truth.
        let now = self
            .now_ms()
            .map_err(|error| ParticipantSemanticError::Internal {
                message: format!("participant clock read failed: {error}"),
            })?;
        let now = u128::from(now);
        replayed.prune_expired_provenance(now);
        let contribution = replayed
            .capacity_contribution(now)
            .map_err(|error| state_error(&error))?;
        self.capacity
            .fold_conversation(conversation_id, contribution)
            .map_err(|error| state_error(&error))?;
        Ok(replayed)
    }

    /// Runs the frozen pre-W3 aggregate reference without installing any
    /// owner, observer, capacity, or publication state.
    #[cfg(test)]
    pub(super) fn replay_aggregate_reference(
        &self,
        conversation_id: ConversationId,
        log: &OperationLog,
    ) -> Result<ConversationAuthority, ParticipantSemanticError> {
        let outbox_log = OutboxLog::new(Arc::clone(&self.store), conversation_id);
        let extension_rows = block_on(outbox_log.read_all())
            .map_err(|error| bridge_error(&error))?
            .map_err(|error| outbox_log_error(&error))?;
        block_on(ConversationAuthority::replay_aggregate_reference(
            conversation_id,
            log,
            &outbox_log,
            extension_rows,
            &self.config,
            self.outbox_limits,
        ))
        .map_err(|error| bridge_error(&error))?
        .map_err(|error| state_error(&error))
    }

    /// Removes a conversation's registry cell after a durably empty touch.
    ///
    /// Only the exact cell this operation used is removed (a racing request
    /// may have installed a fresh cell already); a concurrent holder of the
    /// evicted cell stays correct because every durable append is optimistic
    /// on its exact sequence and cold replay is the source of truth.
    pub(super) fn evict_uncommitted(
        &self,
        conversation_id: ConversationId,
        cell: &Arc<Mutex<Option<ConversationAuthority>>>,
    ) -> Result<(), ParticipantSemanticError> {
        let mut conversations =
            self.conversations
                .lock()
                .map_err(|_| ParticipantSemanticError::Internal {
                    message: "participant conversation registry lock is poisoned".to_owned(),
                })?;
        if let Some(existing) = conversations.get(&conversation_id) {
            if Arc::ptr_eq(existing, cell) {
                conversations.remove(&conversation_id);
            }
        }
        drop(conversations);
        Ok(())
    }

    /// Number of live conversation registry cells (test observability).
    #[cfg(test)]
    pub(super) fn registry_len(&self) -> usize {
        self.conversations
            .lock()
            .map_or(usize::MAX, |conversations| conversations.len())
    }

    /// Drops only volatile participant/observer owners for cold-first-touch tests.
    #[cfg(test)]
    pub(super) fn discard_owners_for_test(&self) -> Result<(), ParticipantSemanticError> {
        self.conversations
            .lock()
            .map_err(|_| ParticipantSemanticError::Internal {
                message: "participant conversation registry lock is poisoned".to_owned(),
            })?
            .clear();
        self.observer
            .lock()
            .map_err(|_| ParticipantSemanticError::Internal {
                message: "observer recovery aggregate lock is poisoned".to_owned(),
            })?
            .take();
        Ok(())
    }

    /// Runs one `OperatorCredentialReissue` at the serialized
    /// participant-state point (R18 amendment A7, §0.18).
    ///
    /// It reaches the conversation through the SAME owner lock, cold-replay,
    /// and post-commit reconciliation seam as enrollment and credential
    /// attach — never a second path — so every guard reads the same exact
    /// state those operations read, and an unknown conversation is evicted
    /// with no residue exactly as a refused probe already is.
    ///
    /// # Errors
    ///
    /// Returns [`ParticipantSemanticError`] when the operation could not be
    /// DECIDED (a latched service fatal, an unloadable conversation, a durable
    /// failure). Every decided refusal is an `Ok`.
    pub(crate) fn operator_credential_reissue(
        &self,
        request: OperatorCredentialReissueRequest,
    ) -> Result<OperatorCredentialReissueOutcome, ParticipantSemanticError> {
        self.ensure_service_live()?;
        let now_ms = self
            .now_ms()
            .map_err(|error| ParticipantSemanticError::Internal {
                message: format!("participant clock read failed: {error}"),
            })?;
        self.with_conversation_fate_source(request.conversation_id, None, |authority, appender, _| {
            authority.apply_operator_credential_reissue(request, now_ms, appender)
        })
    }

    pub(super) fn operation_facts(
        &self,
        context: ParticipantConnectionContext,
        conversation_id: ConversationId,
        conversations: &ParticipantConnectionConversations,
    ) -> Result<OperationFacts, ParticipantSemanticError> {
        let now_ms = self
            .now_ms()
            .map_err(|error| ParticipantSemanticError::Internal {
                message: format!("participant clock read failed: {error}"),
            })?;
        // The connection map only grows through capacity commits, so its
        // occupancy always fits the validated nonzero signed limit; a counter
        // rejection here is genuine internal drift and fails closed.
        let connection_capacity = CapacityCounter::try_new(
            self.config.max_semantic_conversations_per_connection,
            conversations.occupied(),
        )
        .map_err(|error| ParticipantSemanticError::Internal {
            message: format!(
                "connection-conversation occupancy disagrees with its signed limit: {error:?}"
            ),
        })?;
        Ok(OperationFacts {
            receiving_incarnation: context.connection_incarnation(),
            now_ms,
            identity_slots: self.config.identity_slots,
            attach_receipt_ttl_ms: self.config.attach_receipt_ttl_ms,
            receipt_provenance_ttl_ms: self.config.receipt_provenance_ttl_ms,
            receipt_limits: ReceiptCapacityLimits::from_config(&self.config),
            connection_tracking: conversations.tracking(conversation_id),
            connection_capacity,
        })
    }
}

/// The operator surface reaches the participant through the handler itself
/// (R18 amendment A7, §0.18): possession of the operator surface is the
/// authority, and this impl is the only bridge between it and the serialized
/// participant-state point.
impl OperatorCredentialReissuer for ProductionParticipantHandler {
    fn reissue(
        &self,
        request: OperatorCredentialReissueRequest,
    ) -> Result<OperatorCredentialReissueOutcome, OperatorCredentialReissueError> {
        self.operator_credential_reissue(request)
            .map_err(|error| OperatorCredentialReissueError {
                message: error.to_string(),
            })
    }
}

/// Bridges the synchronous state seam onto the async durable log, and keeps
/// the conversation registry complete by construction: the one
/// conversation-creating append (genesis at sequence zero) is preceded by a
/// durable registry row, so startup can enumerate every conversation stream
/// that exists.
pub(super) struct LogAppender<'a> {
    pub(super) log: &'a OperationLog,
    pub(super) registry: &'a ConversationRegistry,
    pub(super) conversation_id: ConversationId,
    /// Board #60 §3c. The Unit 2 extension log a committed source completes
    /// itself into, under the same conversation lock as its base append.
    pub(super) outbox_log: &'a OutboxLog,
    /// Owed-minus-written Unit 2 extension rows for the operation this
    /// appender is serving. Incremented by every appended row that owes one
    /// ([`owes_extension_row`]), decremented by every row written in place.
    /// Zero at the end of the operation is the OUTCOME GATE that lets the
    /// commit path skip its from-zero replay.
    pub(super) outstanding_extension_rows: Cell<u64>,
}

impl LogAppender<'_> {
    /// Rows this operation appended that owe an extension row and have not
    /// been given one in place.
    pub(super) fn outstanding_extension_rows(&self) -> u64 {
        self.outstanding_extension_rows.get()
    }
}

impl DurableAppend for LogAppender<'_> {
    fn append(
        &self,
        operation: &StoredOperation,
        expected_sequence: u64,
    ) -> Result<(), OperationLogError> {
        if expected_sequence == 0 && matches!(operation, StoredOperation::Genesis { .. }) {
            self.registry.register(self.conversation_id)?;
        }
        block_on(self.log.append(operation, expected_sequence))??;
        if owes_extension_row(operation) {
            self.outstanding_extension_rows
                .set(self.outstanding_extension_rows.get().saturating_add(1));
        }
        Ok(())
    }

    fn extension_log(&self) -> Option<&OutboxLog> {
        Some(self.outbox_log)
    }

    fn owed_extension_rows(&self) -> u64 {
        self.outstanding_extension_rows.get()
    }

    fn discharge_owed_extension_row(&self) {
        self.outstanding_extension_rows
            .set(self.outstanding_extension_rows.get().saturating_sub(1));
    }
}

pub(super) fn state_error(error: &StateError) -> ParticipantSemanticError {
    // The two refusals that must not be flattened. Every other state failure
    // is a diagnostic string because nothing downstream branches on it.
    if let StateError::BindingTerminalAdmissionRefused { error } = error {
        return ParticipantSemanticError::BindingTerminalAdmissionRefused { error: *error };
    }
    // F8B R-SEAL (§6.6): a late arrival must be able to tell a sealed
    // conversation from every other failure by type, not by substring.
    if let StateError::ConversationSealed { conversation_id } = error {
        return ParticipantSemanticError::ConversationSealed {
            conversation_id: *conversation_id,
        };
    }
    ParticipantSemanticError::Internal {
        message: format!("participant production operation failed: {error}"),
    }
}

pub(super) fn log_error(error: &OperationLogError) -> ParticipantSemanticError {
    ParticipantSemanticError::Internal {
        message: format!("participant production log failed: {error}"),
    }
}

pub(super) fn outbox_log_error(error: &OutboxLogError) -> ParticipantSemanticError {
    ParticipantSemanticError::Internal {
        message: format!("participant Unit 2 extension log failed: {error}"),
    }
}

pub(super) fn bridge_error(
    error: &liminal::durability::bridge::BridgeError,
) -> ParticipantSemanticError {
    ParticipantSemanticError::Internal {
        message: format!("participant durability bridge failed: {error}"),
    }
}
