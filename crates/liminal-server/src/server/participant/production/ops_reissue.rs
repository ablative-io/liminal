//! Authorized operator credential re-issue (R18 amendment A7, §0.18).
//!
//! `OperatorCredentialReissue` runs at the ONE serialized participant-state
//! point — the same conversation lock that runs enrollment and credential
//! attach — because every one of its guards reads participant state that only
//! that lock makes exact.
//!
//! # The shape of the operation
//!
//! Two pre-guard lookups (unknown conversation, unknown participant) and four
//! guards (`Retired`, live binding, live receipt, generation compare-and-set),
//! then ONE atomic durable commit: checked-increment G→G+1, mint a fresh
//! secret, invalidate G's verifier, append exactly one durable row. It binds
//! nothing, appends no `Attached`/`Detached` lifecycle record, and mints no
//! R-C0 receipt row.
//!
//! # Every refusal is provably mutation-free
//!
//! Not by inspection — by construction. Nothing in this module mutates
//! anything before the append, and the refusal arms return before
//! [`ConversationAuthority::mint_reissued_credential`] is ever called. In
//! particular this path deliberately does NOT call
//! `prune_expired_provenance`: the guards compare the admitted clock against
//! each receipt's own deadline directly, so the operation needs no pruning,
//! and a refusal therefore leaves even the volatile provenance map untouched.
//! `handler.rs`'s post-commit reconciliation (which does prune) runs only when
//! the log head advanced, i.e. only after a commit.
//!
//! # No polling
//!
//! §0.18 item 5. Nothing here arms a timer, sweeps, scans, or retries.
//! Repetition is operator-driven only: a lost response is repaired by calling
//! the operation again, and the compare-and-set refusal carries the generation
//! pair that repair needs.

use liminal_protocol::lifecycle::{
    BindingState, EnrollmentFingerprint, LiveMember, LiveMemberRestore, RetiredIdentity,
};
use liminal_protocol::wire::{AttachSecret, Generation};

use crate::health::reissue::{
    OperatorCredentialReissueOutcome, OperatorCredentialReissueRefusal,
    OperatorCredentialReissueRequest, OperatorCredentialReissued, encode_hex,
};

use super::facts::{self, Digest};
use super::log::{StoredCredentialReissue, StoredOperation};
use super::state::{ConversationAuthority, DurableAppend, Slot, StateError};

/// Which live binding state refused, as the operator reads it.
const BOUND: &str = "bound";
const PENDING_FINALIZATION: &str = "pending_finalization";
/// Which live receipt refused, as the operator reads it.
const ATTACH_RECEIPT: &str = "attach";
const ENROLLMENT_RECEIPT: &str = "enrollment";

impl Slot {
    /// Whether this slot still holds a live attach receipt at `now`.
    ///
    /// The predicate is the ONE `attach_token_phase` resolves its live-receipt
    /// phase with (`ops_attach_lookup.rs`), against the receipt's OWN deadline
    /// fixed at its own commit. Guard (c) and R-C0 classification therefore
    /// cannot disagree about whether a recovery window is open.
    pub(super) fn attach_receipt_live(&self, now: u128) -> bool {
        self.attach
            .as_ref()
            .is_some_and(|attach| now < attach.receipt_expires_at)
    }

    /// Whether this slot still holds a live enrollment receipt at `now`.
    ///
    /// The predicate is the ONE `enrollment_replay_response` resolves its
    /// live-receipt phase with (`ops_enroll.rs`): the receipt body ends either
    /// by its own deadline or when a committed attach ended it.
    pub(super) const fn enrollment_receipt_live(&self, now: u128) -> bool {
        self.enrollment_receipt_ended.is_none() && now < self.enrollment_receipt_expires_at
    }
}

impl ConversationAuthority {
    /// Applies one `OperatorCredentialReissue` end to end (§0.18).
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] only when the durable append, the entropy
    /// source, or a protocol invariant fails. Every DECIDED answer — including
    /// all six refusals — is an `Ok`.
    pub(super) fn apply_operator_credential_reissue(
        &mut self,
        request: OperatorCredentialReissueRequest,
        now_ms: u64,
        appender: &dyn DurableAppend,
    ) -> Result<OperatorCredentialReissueOutcome, StateError> {
        let conversation_id = request.conversation_id;
        let participant_id = request.participant_id;
        let now = u128::from(now_ms);
        // Pre-guard lookup 1: a conversation with no durable log resolves to
        // nothing. `next_log_sequence == 0` is the handler's own definition of
        // a durably empty conversation (it is what `evict_uncommitted` keys
        // on), so this refusal and the eviction that follows it agree by
        // construction and the probe leaves no residue of any kind.
        if self.next_log_sequence == 0 {
            return Ok(refused(
                OperatorCredentialReissueRefusal::ConversationUnknown { conversation_id },
            ));
        }
        // Pre-guard lookup 2 and guard (a) share one identity resolution: a
        // tombstone is a RESOLVED identity, not a lookup miss, so `Retired`
        // must not be swallowed by the unknown-participant arm. A retired
        // identity is removed from `slots` and lives in `retired`
        // (`ops_leave.rs`), which is exactly the pair probed here.
        if let Some(retired) = self.retired.get(&participant_id) {
            return Ok(refused(OperatorCredentialReissueRefusal::Retired {
                conversation_id,
                participant_id,
                retired_generation: retired_generation(retired),
            }));
        }
        let Some(slot) = self.slots.get(&participant_id) else {
            return Ok(refused(
                OperatorCredentialReissueRefusal::ParticipantUnknown {
                    conversation_id,
                    participant_id,
                },
            ));
        };
        let current_generation = slot.member.generation();
        // Guard (b): a live binding. `PendingFinalization` is refused with the
        // same row and names itself: its terminal has not been appended, so the
        // binding is still live authority in every sense this guard cares about
        // and re-issuing against it would be the seat revocation A7 refuses to
        // create.
        let live_binding = match slot.binding {
            BindingState::Detached => None,
            BindingState::Bound(_) => Some(BOUND),
            BindingState::PendingFinalization(_) => Some(PENDING_FINALIZATION),
        };
        if let Some(binding_state) = live_binding {
            return Ok(refused(OperatorCredentialReissueRefusal::LiveBinding {
                conversation_id,
                participant_id,
                current_generation: current_generation.get(),
                binding_state,
            }));
        }
        // Guard (c): a live attach or enrollment receipt means the R-C0
        // recovery window is still open and the ordinary recovery path must be
        // exhausted first.
        let live_receipt = if slot.attach_receipt_live(now) {
            Some(ATTACH_RECEIPT)
        } else if slot.enrollment_receipt_live(now) {
            Some(ENROLLMENT_RECEIPT)
        } else {
            None
        };
        if let Some(receipt) = live_receipt {
            return Ok(refused(OperatorCredentialReissueRefusal::LiveReceipt {
                conversation_id,
                participant_id,
                current_generation: current_generation.get(),
                receipt,
            }));
        }
        // Guard (d): the compare-and-set. Its refusal payload is NORMATIVE —
        // see `health::reissue::OperatorCredentialReissueRefusal`.
        if request.expected_current_generation != current_generation.get() {
            return Ok(refused(
                OperatorCredentialReissueRefusal::GenerationMismatch {
                    conversation_id,
                    participant_id,
                    presented_generation: request.expected_current_generation,
                    current_generation: current_generation.get(),
                },
            ));
        }
        self.mint_reissued_credential(request, current_generation, now_ms, appender)
    }

    /// The one atomic durable commit (§0.18 item 3).
    ///
    /// Order is load-bearing: the increment and the secret are minted, the row
    /// is appended and flushed, and ONLY THEN does the in-memory credential
    /// move. A failed append therefore leaves the slot holding G exactly as it
    /// was, which is the same crash-consistency model every other commit in
    /// this module uses.
    fn mint_reissued_credential(
        &mut self,
        request: OperatorCredentialReissueRequest,
        current_generation: Generation,
        now_ms: u64,
        appender: &dyn DurableAppend,
    ) -> Result<OperatorCredentialReissueOutcome, StateError> {
        // R-C1's checked increment, under the same joint-domain proof every
        // credential-bearing success uses: never wraps, saturates, aliases, or
        // rebases. The exhaustion arm is unreachable by the contract's own
        // sequence argument and is answered loudly rather than assumed away.
        let issued_generation = next_generation(current_generation)?;
        let issued_secret = facts::mint_secret_bytes()?;
        let row = StoredCredentialReissue {
            participant_id: request.participant_id,
            presented_generation: current_generation.get(),
            issued_generation: issued_generation.get(),
            attach_secret_verifier: issued_secret,
            admitted_now_ms: now_ms,
        };
        appender.append(
            &StoredOperation::CredentialReissued { row },
            self.next_log_sequence,
        )?;
        self.install_reissued_credential(&row)?;
        self.advance_log_head()?;
        Ok(OperatorCredentialReissueOutcome::Issued(
            OperatorCredentialReissued {
                conversation_id: request.conversation_id,
                participant_id: request.participant_id,
                presented_generation: current_generation.get(),
                issued_generation: issued_generation.get(),
                attach_secret: encode_hex(&issued_secret),
            },
        ))
    }

    /// Replays one committed re-issue row from its stored inputs.
    ///
    /// This is the SAME installer the live commit runs, so a replayed store
    /// reaches the identical generation and the identical verifier by
    /// construction rather than by a second implementation agreeing with the
    /// first.
    pub(super) fn replay_credential_reissue(
        &mut self,
        row: &StoredCredentialReissue,
        sequence: u64,
    ) -> Result<(), StateError> {
        if row.presented_generation.checked_add(1) != Some(row.issued_generation) {
            return Err(StateError::Log(
                super::log::OperationLogError::CorruptRow { sequence },
            ));
        }
        self.install_reissued_credential(row)
    }

    /// Installs one re-issued credential onto its slot.
    ///
    /// Invalidating G's verifier is not a separate step: the slot holds exactly
    /// one current credential, in `member` and in `attach_secret`, and both are
    /// replaced here. The retained provenance fingerprints of ENDED receipts
    /// are deliberately left alone — they are non-secret, their windows were
    /// fixed at their own commits, and destroying them would move R-C0's
    /// classification answers, which A7 explicitly does not do.
    fn install_reissued_credential(
        &mut self,
        row: &StoredCredentialReissue,
    ) -> Result<(), StateError> {
        let issued_generation = Generation::new(row.issued_generation)
            .ok_or(super::log::OperationLogError::ZeroGeneration)?;
        let slot = self.slots.get_mut(&row.participant_id).ok_or_else(|| {
            StateError::invariant("credential re-issue requires an enrolled participant slot")
        })?;
        if slot.member.generation().get() != row.presented_generation {
            return Err(StateError::invariant(
                "credential re-issue row presents a generation the slot does not hold",
            ));
        }
        let issued_secret = AttachSecret::new(row.attach_secret_verifier);
        // The protocol's own membership constructor, which re-validates the
        // retained binding terminal against the NEW generation (a terminal may
        // never belong to a generation newer than the credential). Nothing is
        // re-derived here: identity, cursor, enrollment fingerprint and terminal
        // history all travel through unchanged, and only the credential moves.
        let member: LiveMember<Digest> = LiveMember::restore(LiveMemberRestore {
            participant_id: slot.member.participant_id(),
            conversation_id: slot.member.conversation_id(),
            generation: issued_generation,
            attach_secret: issued_secret,
            cursor: slot.member.cursor(),
            enrollment_fingerprint: EnrollmentFingerprint::new(
                *slot.member.enrollment_fingerprint().value(),
            ),
            latest_terminal: slot.member.latest_terminal(),
        })
        .map_err(|error| {
            StateError::invariant(format!(
                "protocol membership refused a re-issued credential: {error:?}"
            ))
        })?;
        slot.member = member;
        slot.attach_secret = issued_secret;
        Ok(())
    }
}

/// R-C1's checked generation increment.
fn next_generation(current: Generation) -> Result<Generation, StateError> {
    current
        .get()
        .checked_add(1)
        .and_then(Generation::new)
        .ok_or(StateError::AllocationExhausted {
            domain: "capability generation",
        })
}

/// The retired identity's permanent generation, for guard (a)'s payload.
const fn retired_generation(retired: &RetiredIdentity<Digest, Digest, Digest>) -> u64 {
    retired.retired_generation().get()
}

const fn refused(
    refusal: OperatorCredentialReissueRefusal,
) -> OperatorCredentialReissueOutcome {
    OperatorCredentialReissueOutcome::Refused(refusal)
}
