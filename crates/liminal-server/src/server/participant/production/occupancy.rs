//! Per-conversation and per-participant stage-8 occupancy, derived from the
//! conversation authority's own replayed state (split from
//! [`super::capacity`] under the 500-code-line lens).
//!
//! Nothing here is durable or shared: every value is a pure function of one
//! conversation's slots and the operation's admitted clock read. The
//! server-scope ledger lives in [`super::capacity`].

use super::capacity::{ConversationContribution, OccupancyEntry, ResourceKind};
use super::state::{ConversationAuthority, Slot, StateError};

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
                if slot.enrollment_receipt_ended.is_some()
                    && now < slot.enrollment_provenance_expires_at
                {
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
            .checked_add(u64::from(
                self.enrollment_receipt_ended.is_some()
                    && now < self.enrollment_provenance_expires_at,
            ))
            .ok_or_else(|| {
                StateError::invariant("participant provenance occupancy exceeds the u64 domain")
            })
    }
}
