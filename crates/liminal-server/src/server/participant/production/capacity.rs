//! Server-scope occupancy ledger for R-D1 stage-8 identity/receipt capacity.
//!
//! The ledger owns the three SERVER-scope occupancy facts the crate's
//! stage-8 selectors consume: total reserved identity slots, live
//! secret-bearing receipts, and retained non-secret provenance fingerprints.
//! Per-conversation and per-participant occupancies are computed from the
//! conversation authority itself at request time and never live here.
//!
//! Accounting model (derived, never durable): the ledger is rebuilt from the
//! same replayed conversation authorities the durable transition-input logs
//! produce — [`ConversationAuthority::capacity_contribution`] folds one
//! conversation's complete durable truth in, replacing any previous fold, so
//! a discarded-and-replayed owner cannot double count. Live commits reserve
//! through [`ServerCapacity::admit`] BEFORE their durable append and either
//! confirm (append succeeded) or roll back on drop (any failure before the
//! append), so concurrent operations on different conversations can never
//! admit past a signed server cap.
//!
//! Expiry is request-time, never a sweep (contract R-C0: "admitted durable
//! deadline events plus request-time checks — never a sweep"): entries are
//! deadline-ordered, and every stage-8 check first removes the expired
//! prefix under the same admitted clock read that drives the operation.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Mutex, MutexGuard, PoisonError};

use liminal_protocol::lifecycle::{CapacityCounter, CapacityCounterInvariantError};

use super::state::StateError;

/// One stage-8 scope's occupancy against its signed limit: the crate's
/// validated counter, or the true numbers of a scope whose persisted
/// occupancy exceeds a limit that was lowered underneath durable state.
pub(super) enum ScopeCounter {
    /// The crate's bounded-counter algebra decides this scope.
    Valid(CapacityCounter),
    /// Configuration was lowered beneath retained occupancy: the scope is
    /// unable to admit one and refuses with its true numbers rather than
    /// admitting past the signed cap. This state is outside the contract's
    /// occupancy model (in-model occupancy never exceeds its limit), so its
    /// refusal is selected at counter-construction time under the same
    /// frozen scope order and first-full precedence the crate applies: an
    /// exactly-full in-model scope EARLIER in the order answers first, with
    /// its own numbers, so no later occupancy is disclosed past it.
    OverLimit {
        /// Signed limit currently configured.
        limit: u64,
        /// True retained occupancy.
        occupied: u64,
    },
}

/// Builds one scope counter from a validated nonzero limit.
///
/// # Errors
///
/// Returns a [`StateError`] invariant for a zero limit — configuration
/// validation makes that unreachable.
pub(super) fn scope_counter(limit: u64, occupied: u64) -> Result<ScopeCounter, StateError> {
    match CapacityCounter::try_new(limit, occupied) {
        Ok(counter) => Ok(ScopeCounter::Valid(counter)),
        Err(CapacityCounterInvariantError::OccupiedExceedsLimit { occupied, limit }) => {
            Ok(ScopeCounter::OverLimit { limit, occupied })
        }
        Err(CapacityCounterInvariantError::ZeroLimit) => Err(StateError::invariant(
            "validated capacity configuration rejected: zero limit",
        )),
    }
}

/// Which bounded server-scope resource one ledger entry occupies.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum ResourceKind {
    /// Secret-bearing enrollment receipt body (live-receipt scope).
    EnrollmentReceipt,
    /// Secret-bearing credential-attach receipt body (live-receipt scope).
    AttachReceipt,
    /// Non-secret enrollment provenance fingerprint.
    EnrollmentProvenance,
    /// Non-secret credential-attach provenance fingerprint.
    AttachProvenance,
}

impl ResourceKind {
    /// Whether this kind counts against the live-receipt scopes (the
    /// alternative is the provenance-fingerprint scopes).
    const fn is_live_receipt(self) -> bool {
        matches!(self, Self::EnrollmentReceipt | Self::AttachReceipt)
    }
}

/// One deadline-ordered server-scope occupancy entry.
///
/// Ordering is lexicographic with the deadline FIRST, so the expired prefix
/// of a set is one contiguous range. The remaining fields make every entry
/// unique: at most one enrollment receipt/fingerprint exists per participant,
/// at most one CURRENT attach receipt exists per participant, and retired
/// attach fingerprints are keyed by their distinct attempt tokens.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct OccupancyEntry {
    /// Deadline after which the entry stops occupying (epoch milliseconds).
    pub(super) expires_at: u128,
    /// Owning conversation.
    pub(super) conversation_id: u64,
    /// Owning participant.
    pub(super) participant_id: u64,
    /// Occupied resource kind.
    pub(super) kind: ResourceKind,
    /// Disambiguating token bytes (enrollment or attach attempt token).
    pub(super) token: [u8; 16],
}

impl OccupancyEntry {
    /// Least entry with the given deadline — the `split_off` boundary that
    /// keeps every entry whose deadline is at least `expires_at`.
    const fn floor(expires_at: u128) -> Self {
        Self {
            expires_at,
            conversation_id: 0,
            participant_id: 0,
            kind: ResourceKind::EnrollmentReceipt,
            token: [0; 16],
        }
    }
}

/// One conversation's complete server-scope contribution, derived from its
/// replayed durable state.
#[derive(Debug)]
pub(super) struct ConversationContribution {
    /// Reserved identity slots (live participants plus future tombstones).
    pub(super) identity: u64,
    /// Every in-window receipt and provenance entry.
    pub(super) entries: Vec<OccupancyEntry>,
}

/// Server-scope occupancy snapshot consumed by the stage-8 selectors.
#[derive(Clone, Copy, Debug)]
pub(super) struct ServerOccupancy {
    /// Total reserved identity slots across all conversations.
    pub(super) identity: u64,
    /// Live secret-bearing receipts across all conversations.
    pub(super) live_receipts: u64,
    /// Retained in-window provenance fingerprints across all conversations.
    pub(super) provenance: u64,
}

/// Effects one admitted operation reserves before its durable append.
#[derive(Debug)]
pub(super) struct ReservationEffects {
    /// Conversation the reservation belongs to.
    pub(super) conversation_id: u64,
    /// Whether one identity slot is reserved (enrollment only).
    pub(super) identity_reserved: bool,
    /// Receipt/provenance entries the commit will create.
    pub(super) inserts: Vec<OccupancyEntry>,
}

/// The arm-level stage-8 verdict produced inside the ledger's critical
/// section (the crate selector's decision translated onto this operation).
pub(super) enum Stage8Choice<R> {
    /// Every scope admits; reserve and continue toward commit.
    Admit,
    /// Exact first-full-scope refusal, bound to the requesting operation.
    Refuse(R),
}

/// Result of one atomic check-and-reserve stage-8 pass.
pub(super) enum Stage8Outcome<'a, R> {
    /// Reserved; the guard rolls back unless confirmed after the append.
    Reserved(CapacityReservation<'a>),
    /// Typed refusal response; nothing was reserved.
    Refused(R),
}

#[derive(Debug, Default)]
struct CapacityLedger {
    identity_total: u64,
    identity_by_conversation: HashMap<u64, u64>,
    live_receipts: BTreeSet<OccupancyEntry>,
    provenance: BTreeSet<OccupancyEntry>,
}

impl CapacityLedger {
    /// Removes every entry whose deadline has passed at `now` (request-time
    /// expiry over the deadline-ordered prefix; never a sweep).
    fn prune(&mut self, now: u128) {
        prune_set(&mut self.live_receipts, now);
        prune_set(&mut self.provenance, now);
    }

    fn occupancy(&self) -> Result<ServerOccupancy, StateError> {
        Ok(ServerOccupancy {
            identity: self.identity_total,
            live_receipts: entry_count(self.live_receipts.len())?,
            provenance: entry_count(self.provenance.len())?,
        })
    }

    fn insert(&mut self, entry: OccupancyEntry) {
        if entry.kind.is_live_receipt() {
            self.live_receipts.insert(entry);
        } else {
            self.provenance.insert(entry);
        }
    }

    /// Removes one exact entry. Absence is legal: the entry may already have
    /// been removed by the deadline prune — two legal removal paths for the
    /// same fact, both derived from the same durable deadlines.
    fn remove(&mut self, entry: &OccupancyEntry) {
        if entry.kind.is_live_receipt() {
            self.live_receipts.remove(entry);
        } else {
            self.provenance.remove(entry);
        }
    }
}

fn prune_set(set: &mut BTreeSet<OccupancyEntry>, now: u128) {
    match now.checked_add(1) {
        Some(bound) => {
            let keep = set.split_off(&OccupancyEntry::floor(bound));
            *set = keep;
        }
        None => set.clear(),
    }
}

fn entry_count(len: usize) -> Result<u64, StateError> {
    u64::try_from(len).map_err(|_| {
        StateError::invariant("server-scope occupancy exceeds the u64 counting domain")
    })
}

/// Shared server-scope capacity ledger (one per production handler).
#[derive(Debug, Default)]
pub(super) struct ServerCapacity {
    ledger: Mutex<CapacityLedger>,
}

impl ServerCapacity {
    /// Locks the ledger, recovering from poison: this module contains no
    /// panicking operation (panics are denied crate-wide), every critical
    /// section below completes without early exit, and the ledger is
    /// self-healing through replay folds — so recovering a poisoned guard
    /// cannot observe a torn invariant.
    fn ledger(&self) -> MutexGuard<'_, CapacityLedger> {
        self.ledger.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Atomically prunes expired entries, exposes server occupancy to the
    /// caller's stage-8 decision, and reserves `effects` when it admits.
    ///
    /// The whole check-and-reserve runs under one lock, so two concurrent
    /// operations can never both admit the last slot of a server scope.
    ///
    /// # Errors
    ///
    /// Propagates the decision's [`StateError`] (invalid validated
    /// configuration or a counting-domain violation) without reserving.
    pub(super) fn admit<R>(
        &self,
        now: u128,
        effects: ReservationEffects,
        decide: impl FnOnce(ServerOccupancy) -> Result<Stage8Choice<R>, StateError>,
    ) -> Result<Stage8Outcome<'_, R>, StateError> {
        let mut ledger = self.ledger();
        ledger.prune(now);
        let occupancy = ledger.occupancy()?;
        match decide(occupancy)? {
            Stage8Choice::Refuse(response) => Ok(Stage8Outcome::Refused(response)),
            Stage8Choice::Admit => {
                if effects.identity_reserved {
                    let total = ledger.identity_total.checked_add(1).ok_or_else(|| {
                        StateError::invariant("server identity reservation overflows u64")
                    })?;
                    let per_conversation = ledger
                        .identity_by_conversation
                        .get(&effects.conversation_id)
                        .copied()
                        .unwrap_or(0)
                        .checked_add(1)
                        .ok_or_else(|| {
                            StateError::invariant("conversation identity reservation overflows u64")
                        })?;
                    ledger.identity_total = total;
                    ledger
                        .identity_by_conversation
                        .insert(effects.conversation_id, per_conversation);
                }
                for entry in &effects.inserts {
                    ledger.insert(*entry);
                }
                drop(ledger);
                Ok(Stage8Outcome::Reserved(CapacityReservation {
                    capacity: self,
                    effects: Some(effects),
                }))
            }
        }
    }

    /// Replaces one conversation's complete contribution with its freshly
    /// replayed durable truth.
    ///
    /// Identity is replaced (subtract the previous fold, add the new one);
    /// entries are inserted idempotently — their keys are deterministic
    /// functions of durable facts, so re-folding after an owner discard
    /// cannot double count. A zero contribution removes the conversation's
    /// identity record entirely, so refused probes leave no ledger residue.
    /// An entry retired early by a commit whose in-memory application failed
    /// after its durable append (the rollback re-kept it) is NOT removed by
    /// the fold: it self-expires at its own deadline, over-counting by at
    /// most that commit's retirements until then — which can only refuse
    /// early, never admit past a signed cap.
    ///
    /// # Errors
    ///
    /// Returns a [`StateError`] invariant when the identity totals leave the
    /// u64 domain.
    pub(super) fn fold_conversation(
        &self,
        conversation_id: u64,
        contribution: ConversationContribution,
    ) -> Result<(), StateError> {
        let mut ledger = self.ledger();
        let previous = ledger
            .identity_by_conversation
            .get(&conversation_id)
            .copied()
            .unwrap_or(0);
        let total = ledger
            .identity_total
            .checked_sub(previous)
            .and_then(|total| total.checked_add(contribution.identity))
            .ok_or_else(|| {
                StateError::invariant("server identity fold leaves the u64 counting domain")
            })?;
        ledger.identity_total = total;
        if contribution.identity == 0 {
            ledger.identity_by_conversation.remove(&conversation_id);
        } else {
            ledger
                .identity_by_conversation
                .insert(conversation_id, contribution.identity);
        }
        for entry in contribution.entries {
            ledger.insert(entry);
        }
        drop(ledger);
        Ok(())
    }

    fn rollback(&self, effects: &ReservationEffects) {
        let mut ledger = self.ledger();
        if effects.identity_reserved {
            // The paired reservation incremented both values under this same
            // lock; saturation is unreachable and would only under-count,
            // never admit past a cap.
            ledger.identity_total = ledger.identity_total.saturating_sub(1);
            match ledger
                .identity_by_conversation
                .get(&effects.conversation_id)
                .copied()
                .unwrap_or(0)
                .saturating_sub(1)
            {
                0 => {
                    ledger
                        .identity_by_conversation
                        .remove(&effects.conversation_id);
                }
                remaining => {
                    ledger
                        .identity_by_conversation
                        .insert(effects.conversation_id, remaining);
                }
            }
        }
        for entry in &effects.inserts {
            ledger.remove(entry);
        }
    }
}

/// Reserved-but-unconfirmed stage-8 effects.
///
/// Dropping the guard before [`Self::confirm`] rolls the reservation back —
/// the refusal-or-error path reserves nothing durable. In the exotic window
/// where the durable append succeeded but a later in-memory step failed, the
/// rollback under-counts until the discarded owner's next replay fold heals
/// the ledger from durable truth (under-counting can only admit, never
/// falsely refuse).
pub(super) struct CapacityReservation<'a> {
    capacity: &'a ServerCapacity,
    effects: Option<ReservationEffects>,
}

impl CapacityReservation<'_> {
    /// Makes the reservation permanent and removes the receipts this commit
    /// retired early (the superseded attach receipt and, on the first
    /// rotation, the ended enrollment receipt).
    pub(super) fn confirm(mut self, retire: &[OccupancyEntry]) {
        if self.effects.take().is_some() {
            let mut ledger = self.capacity.ledger();
            for entry in retire {
                ledger.remove(entry);
            }
        }
    }
}

impl Drop for CapacityReservation<'_> {
    fn drop(&mut self) {
        if let Some(effects) = self.effects.take() {
            self.capacity.rollback(&effects);
        }
    }
}

impl std::fmt::Debug for CapacityReservation<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CapacityReservation")
            .field("confirmed", &self.effects.is_none())
            .finish()
    }
}
