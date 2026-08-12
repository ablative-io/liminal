//! Per-conversation and per-participant stage-8 occupancy, derived from the
//! conversation authority's own replayed state (split from
//! [`super::capacity`] under the 500-code-line lens).
//!
//! Nothing here is durable or shared: every value is a pure function of one
//! conversation's slots and the operation's admitted clock read. The
//! server-scope ledger lives in [`super::capacity`].

use crate::metrics::SharedReceiptPool;

use super::capacity::{ConversationContribution, OccupancyEntry, ResourceKind};
use super::state::{ConversationAuthority, Slot, StateError};

/// One member of a participant's provenance retention window.
///
/// The window holds two shapes of fingerprint — the participant's proven
/// enrollment fingerprint (a fixed pair of fields on the slot) and one retired
/// attach fingerprint per committed rotation (entries in a map) — and
/// displacement has to order them together, so they get one type here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ProvenanceMember {
    /// The proven enrollment fingerprint.
    Enrollment {
        /// Its own provenance deadline (epoch milliseconds).
        expires_at: u128,
    },
    /// One retired attach fingerprint, keyed by its exact attempt token.
    Attach {
        /// Its own provenance deadline (epoch milliseconds).
        expires_at: u128,
        /// Exact attach attempt token.
        token: [u8; 16],
    },
}

impl ProvenanceMember {
    /// Total displacement order: OLDEST DEADLINE FIRST, then enrollment before
    /// attach, then token bytes.
    ///
    /// This is byte-for-byte the order the server ledger's own
    /// [`OccupancyEntry`] set already sorts by within one participant
    /// (`expires_at`, then `ResourceKind` where `EnrollmentProvenance` sorts
    /// before `AttachProvenance`, then `token`) — which is what lets the
    /// ledger and the slot apply ONE plan and land on the same retained set.
    /// The enrollment arm's token component is never load-bearing: a
    /// participant has at most one enrollment fingerprint, and the kind rank
    /// already separates it from every attach fingerprint on a deadline tie.
    const fn order_key(self) -> (u128, u8, [u8; 16]) {
        match self {
            Self::Enrollment { expires_at } => (expires_at, 0, [0; 16]),
            Self::Attach { expires_at, token } => (expires_at, 1, token),
        }
    }
}

impl ConversationAuthority {
    /// Request-time expiry of retained provenance fingerprints (contract
    /// R-C0: the non-secret fingerprint remains only through its provenance
    /// deadline). Classification never depends on physical retention — the
    /// generation-window witness in the attach token phase reproduces the
    /// same answers — so pruning is purely the memory bound.
    pub(super) fn prune_expired_provenance(&mut self, now: u128) {
        for slot in self.slots.values_mut() {
            slot.attach_provenance
                .retain(|_, record| now < record.provenance_expires_at);
        }
    }

    /// In-window provenance-fingerprint occupancy across every slot (the
    /// stage-8 `ProvenanceConversation` scope).
    ///
    /// # Errors
    ///
    /// Returns a [`StateError`] invariant if the sum leaves the u64 domain.
    pub(super) fn provenance_occupancy(&self, now: u128) -> Result<u64, StateError> {
        let mut total: u64 = 0;
        for slot in self.slots.values() {
            total = total
                .checked_add(slot.provenance_occupancy(now)?)
                .ok_or_else(|| {
                    StateError::invariant(
                        "conversation provenance occupancy exceeds the u64 domain",
                    )
                })?;
        }
        Ok(total)
    }

    /// Observes this conversation's shared provenance pool against its
    /// configured reporting threshold.
    ///
    /// A TRIPWIRE, never a gate. One participant's churn must never refuse the
    /// next participant of the same conversation — that participant is a third
    /// party to the churn and has consumed nothing — so this call cannot
    /// return a refusal. It counts every observation at or above the threshold
    /// and warns once per rising edge, naming the conversation.
    ///
    /// # Errors
    ///
    /// Propagates the occupancy sum's own u64-domain invariant.
    pub(super) fn observe_conversation_provenance_pool(&self, now: u128) -> Result<(), StateError> {
        let occupied = self.provenance_occupancy(now)?;
        let threshold = self
            .receipt_limits
            .shared_pool_tripwires
            .provenance_conversation;
        if occupied < threshold {
            self.provenance_tripwire_warned.set(false);
            return Ok(());
        }
        crate::metrics::receipt_pool_runaway_observed(SharedReceiptPool::ProvenanceConversation);
        if !self.provenance_tripwire_warned.replace(true) {
            tracing::warn!(
                pool = SharedReceiptPool::ProvenanceConversation.label(),
                conversation_id = self.conversation_id,
                occupied,
                threshold,
                "shared receipt pool runaway: this conversation's in-window provenance occupancy \
                 reached its reporting threshold; the pool refuses nothing and is bounded by its \
                 TTL alone"
            );
        }
        Ok(())
    }

    /// Enrollment-token bytes of one enrolled participant (the permanent
    /// token→identity index inverted for ledger entry keys).
    ///
    /// # Errors
    ///
    /// Returns a [`StateError`] invariant for a slot without a token — the
    /// enrollment commit always writes both.
    pub(super) fn enrollment_token_bytes(
        &self,
        participant_id: u64,
    ) -> Result<[u8; 16], StateError> {
        self.tokens
            .iter()
            .find_map(|(token, mapped)| (*mapped == participant_id).then_some(*token))
            .ok_or_else(|| {
                StateError::invariant("enrolled participant slot has no enrollment token mapping")
            })
    }

    /// Derives this conversation's complete server-scope contribution from
    /// its replayed state: every in-window receipt body and provenance
    /// fingerprint plus the reserved identity slots.
    ///
    /// # Errors
    ///
    /// Returns a [`StateError`] invariant when the token index and the slot
    /// map disagree (a drifted replay).
    pub(super) fn capacity_contribution(
        &self,
        now: u128,
    ) -> Result<ConversationContribution, StateError> {
        let mut entries = Vec::new();
        for (token, participant_id) in &self.tokens {
            if let Some(slot) = self.slots.get(participant_id) {
                if slot.enrollment_receipt_ended.is_none()
                    && now < slot.enrollment_receipt_expires_at
                {
                    entries.push(OccupancyEntry {
                        expires_at: slot.enrollment_receipt_expires_at,
                        conversation_id: self.conversation_id,
                        participant_id: *participant_id,
                        kind: ResourceKind::EnrollmentReceipt,
                        token: *token,
                    });
                }
                // Board #37: an enrollment fingerprint occupies only once the
                // client PROVED possession of the secret that receipt minted,
                // and the receipt's own end is that proof — it is set by the
                // first attach, which had to verify against this secret.
                //
                // Lane p0-39: and only while the participant's own window has
                // not displaced it for a newer fingerprint of the same
                // participant. A displaced fingerprint contributes nothing
                // here, exactly as it answers nothing at lookup.
                if slot.enrollment_fingerprint_retained(now) {
                    entries.push(OccupancyEntry {
                        expires_at: slot.enrollment_provenance_expires_at,
                        conversation_id: self.conversation_id,
                        participant_id: *participant_id,
                        kind: ResourceKind::EnrollmentProvenance,
                        token: *token,
                    });
                }
            } else if !self.retired.contains_key(participant_id) {
                return Err(StateError::invariant(
                    "enrollment token maps to neither a live nor retired participant",
                ));
            }
        }
        for (participant_id, slot) in &self.slots {
            if let Some(attach) = slot.attach.as_ref() {
                if now < attach.receipt_expires_at {
                    entries.push(OccupancyEntry {
                        expires_at: attach.receipt_expires_at,
                        conversation_id: self.conversation_id,
                        participant_id: *participant_id,
                        kind: ResourceKind::AttachReceipt,
                        token: attach.token.into_bytes(),
                    });
                }
                // Board #37: the CURRENT attach receipt's fingerprint does not
                // occupy. Nothing has verified against the secret it minted —
                // if anything had, that attach would have retired it into
                // `attach_provenance`, which is counted below.
            }
            for (token, record) in &slot.attach_provenance {
                if now < record.provenance_expires_at {
                    entries.push(OccupancyEntry {
                        expires_at: record.provenance_expires_at,
                        conversation_id: self.conversation_id,
                        participant_id: *participant_id,
                        kind: ResourceKind::AttachProvenance,
                        token: *token,
                    });
                }
            }
        }
        Ok(ConversationContribution {
            identity: self.next_participant,
            entries,
        })
    }
}

impl Slot {
    /// Live secret-bearing receipt occupancy for this participant (the
    /// stage-8 `LiveReceiptParticipant` scope): the enrollment receipt while
    /// unended and inside its own window, plus the current attach receipt
    /// inside its window (superseded receipts were already retired into
    /// provenance records).
    pub(super) fn live_receipt_occupancy(&self, now: u128) -> u64 {
        u64::from(
            self.enrollment_receipt_ended.is_none() && now < self.enrollment_receipt_expires_at,
        ) + u64::from(
            self.attach
                .as_ref()
                .is_some_and(|attach| now < attach.receipt_expires_at),
        )
    }

    /// In-window provenance-fingerprint occupancy for this participant (the
    /// stage-8 `ProvenanceParticipant` scope).
    ///
    /// # Board #37: occupancy is DELIVERY-OBSERVED provenance
    ///
    /// A fingerprint occupies only once the client has proven it possesses
    /// the secret its receipt minted (ruling 2026-08-12). Only credential
    /// attach is secret-bearing against the slot's current secret, and a
    /// committed attach necessarily supersedes the receipt that minted it, so
    /// the proof is structural: the retired records in `attach_provenance`
    /// are exactly the attach fingerprints whose possession was proven, and
    /// `enrollment_receipt_ended` is set by the one attach that proved the
    /// enrollment secret.
    ///
    /// The unproven pair — the current attach receipt and an unended
    /// enrollment receipt — therefore counts zero. That is not an unbounded
    /// hole: each is a FIXED FIELD on a slot that exists anyway, so the
    /// unproven population is bounded by the identity cap, while the
    /// genuinely unbounded population (`attach_provenance` grows once per
    /// committed rotation) is exactly what these scopes still bound.
    ///
    /// ⚠ Occupancy is not classification. Every fingerprint still classifies
    /// through its own window in `ops_attach_lookup`; this function decides
    /// only what consumes a signed stage-8 slot.
    ///
    /// # Errors
    ///
    /// Returns a [`StateError`] invariant if the count leaves the u64 domain.
    pub(super) fn provenance_occupancy(&self, now: u128) -> Result<u64, StateError> {
        let retained = self
            .attach_provenance
            .values()
            .filter(|record| now < record.provenance_expires_at)
            .count();
        let retained = u64::try_from(retained).map_err(|_| {
            StateError::invariant("participant provenance occupancy exceeds the u64 domain")
        })?;
        retained
            .checked_add(u64::from(self.enrollment_fingerprint_retained(now)))
            .ok_or_else(|| {
                StateError::invariant("participant provenance occupancy exceeds the u64 domain")
            })
    }

    /// Whether this participant's enrollment fingerprint is still retained and
    /// in window at `now`.
    ///
    /// Three ways it is not: possession of the enrollment secret was never
    /// proven (board #37), its own provenance deadline has passed, or the
    /// participant's own window DISPLACED it to make room for a newer
    /// fingerprint of the same participant (lane p0-39).
    pub(super) const fn enrollment_fingerprint_retained(&self, now: u128) -> bool {
        self.enrollment_receipt_ended.is_some()
            && !self.enrollment_provenance_displaced
            && now < self.enrollment_provenance_expires_at
    }

    /// The provenance fingerprint one committing credential attach will
    /// RETAIN, if any.
    ///
    /// Board #37: a committed attach is the proof of delivery for the receipt
    /// it SUPERSEDES, never for the one it mints — only a fresh attempt token
    /// verified against the slot's current secret reaches a commit, and that
    /// secret was minted by and delivered in exactly the predecessor. So the
    /// fingerprint this operation causes to be retained belongs to the
    /// predecessor: the previous attach receipt's, or on the first rotation
    /// the enrollment receipt's.
    ///
    /// The two arms are mutually exclusive (`attach.is_some()` implies
    /// `enrollment_receipt_ended.is_some()`, both written only by
    /// `install_attach_receipt`), so at most one fingerprint is ever retained
    /// per commit.
    ///
    /// ONE definition, called by the ledger's displacement plan and by the
    /// slot's own commit. Two sites deriving this separately is exactly how a
    /// live sequence and its cold replay drift apart.
    pub(super) const fn incoming_provenance_member(&self) -> Option<ProvenanceMember> {
        match self.attach.as_ref() {
            Some(previous) => Some(ProvenanceMember::Attach {
                expires_at: previous.provenance_expires_at,
                token: previous.token.into_bytes(),
            }),
            None if self.enrollment_receipt_ended.is_none() => Some(ProvenanceMember::Enrollment {
                expires_at: self.enrollment_provenance_expires_at,
            }),
            None => None,
        }
    }

    /// Plans the displacement one committing attach applies to this
    /// participant's provenance window.
    ///
    /// `incoming` is the fingerprint this commit will retain (the predecessor
    /// attach receipt's, or — on the first rotation — the enrollment
    /// receipt's, whose possession this very attach has just proven). The
    /// returned members are the ones the window drops, OLDEST FIRST, so that
    /// post-commit retention is exactly `window` members.
    ///
    /// # This bounds the PHYSICALLY retained set, not the in-window count
    ///
    /// Deliberately clock-free. Cold replay re-executes the same committed
    /// attaches in the same durable order but under a much later clock, so a
    /// plan that consulted `now` could not reproduce the live retained set.
    /// This one does: the plan is a function of durable structure and the
    /// configured window alone.
    ///
    /// The in-window survivors match either way, because expiry and
    /// displacement remove members in the SAME order — an expired member has a
    /// smaller deadline than every in-window member, so oldest-first sheds
    /// expired fingerprints before it ever touches a live one. A replayed slot
    /// may therefore physically hold an expired fingerprint a live slot had
    /// already pruned, and still answer every classification identically.
    pub(super) fn plan_provenance_displacement(
        &self,
        incoming: ProvenanceMember,
        window: u64,
    ) -> Vec<ProvenanceMember> {
        let mut members: Vec<ProvenanceMember> = self
            .attach_provenance
            .iter()
            .map(|(token, record)| ProvenanceMember::Attach {
                expires_at: record.provenance_expires_at,
                token: *token,
            })
            .collect();
        // The enrollment fingerprint is retained post-commit either because it
        // already was, or because THIS commit is the proof that retains it —
        // and never both, since the incoming enrollment arm is chosen only
        // when `enrollment_receipt_ended` is still unset.
        if self.enrollment_receipt_ended.is_some() && !self.enrollment_provenance_displaced {
            members.push(ProvenanceMember::Enrollment {
                expires_at: self.enrollment_provenance_expires_at,
            });
        }
        members.push(incoming);
        let over = members.len().saturating_sub(usize_window(window));
        if over == 0 {
            return Vec::new();
        }
        members.sort_unstable_by_key(|member| member.order_key());
        members.truncate(over);
        members
    }
}

/// The configured window as a `usize` count, saturating on a 32-bit host.
///
/// Saturation can only make the window LARGER than the platform could hold
/// entries for, so it can never displace more than configured — and a window
/// that big is unreachable long before it matters.
fn usize_window(window: u64) -> usize {
    usize::try_from(window).unwrap_or(usize::MAX)
}
