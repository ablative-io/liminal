//! R-D1 stage-8 capacity pass for the credential-attach arm (split from
//! [`super::ops_attach`] under the 500-code-line lens).
//!
//! # Lane p0-39: this arm has no refusal left
//!
//! Every scope this pass once walked has stopped refusing. The three shared
//! pools are TTL-bounded with a reporting tripwire; the two per-participant
//! scopes are retention windows that displace their own oldest member rather
//! than turn an arrival away. What the pass still does is real work: it
//! observes the shared pools, plans the displacement the commit will apply,
//! and reserves the ledger entries atomically with that plan.

use liminal_protocol::lifecycle::{
    CredentialAttachCapacityCounters, ReceiptDeadlines, select_credential_attach_capacity,
};
use liminal_protocol::wire::CredentialAttachRequest;

use crate::metrics::ReceiptWindowScope;

use super::barrier::OperationFacts;
use super::capacity::{
    CapacityReservation, OccupancyEntry, ReservationEffects, ResourceKind, ServerCapacity,
    Stage8Choice, Stage8Outcome,
};
use super::occupancy::ProvenanceMember;
use super::state::{ConversationAuthority, Slot, StateError};

impl ConversationAuthority {
    /// Runs the stage-8 receipt/provenance window pass for one authorized
    /// credential attach.
    ///
    /// # Errors
    ///
    /// Propagates the ledger's own u64-domain and configuration invariants.
    pub(super) fn attach_stage8<'cap>(
        &self,
        request: &CredentialAttachRequest,
        slot: &Slot,
        operation_facts: &OperationFacts,
        server_capacity: &'cap ServerCapacity,
        deadlines: &ReceiptDeadlines,
    ) -> Result<AttachStage8<'cap>, StateError> {
        let now = u128::from(operation_facts.now_ms);
        let limits = operation_facts.receipt_limits;
        // Shared pools: observed and reported, never consulted for admission.
        self.observe_conversation_provenance_pool(now)?;
        let token = request.attach_attempt_token.into_bytes();
        let mut inserts = vec![OccupancyEntry {
            expires_at: deadlines.receipt_expires_at(),
            conversation_id: self.conversation_id,
            participant_id: request.participant_id,
            kind: ResourceKind::AttachReceipt,
            token,
        }];
        // Which fingerprint this commit retains is board #37's question, and
        // `Slot::incoming_provenance_member` is its single answer — see there.
        //
        // ONE displacement plan, applied to the ledger here and to the slot in
        // `install_attach_receipt`. Two sites deriving "the oldest" separately
        // is exactly how a live sequence and its cold replay drift apart, so
        // they call the same two functions over the same pre-commit slot.
        //
        // Every key below is byte-identical to the one `capacity_contribution`
        // re-derives from the post-commit slot, so a later replay fold is
        // idempotent and cannot double count.
        let incoming = slot.incoming_provenance_member();
        let displaced = incoming.map_or_else(Vec::new, |incoming| {
            slot.plan_provenance_displacement(incoming, limits.provenance_participant_window)
        });
        let enrollment_token = self.enrollment_token_bytes(request.participant_id)?;
        if let Some(incoming) = incoming
            && !displaced.contains(&incoming)
        {
            inserts.push(self.provenance_entry(request.participant_id, incoming, enrollment_token));
        }
        let displaced_entries: Vec<OccupancyEntry> = displaced
            .iter()
            .filter(|member| Some(**member) != incoming)
            .map(|member| self.provenance_entry(request.participant_id, *member, enrollment_token))
            .collect();
        let effects = ReservationEffects {
            conversation_id: self.conversation_id,
            identity_reserved: false,
            inserts,
        };
        // Receipts this commit will retire early, applied only at confirm.
        //
        // This list is also what makes the LiveReceiptParticipant window's
        // displacement a no-op in practice: every live receipt that window
        // counts for this participant is retired by this very commit, so
        // post-commit occupancy is exactly one whatever the window size is.
        // That is the wedge the old wall created — an attach refused by the
        // receipt it was about to end.
        let retire = self.retired_receipts(request.participant_id, slot, enrollment_token);
        let counters = attach_window_counters(
            limits.live_receipt_participant_window,
            slot.live_receipt_occupancy(now),
            limits.provenance_participant_window,
            slot.provenance_occupancy(now)?,
        )?;
        let tripwires = limits.shared_pool_tripwires;
        let outcome = server_capacity.admit(now, tripwires, effects, |_server| {
            // The crate selector is TOTAL: it produces a commit for every
            // input, so this arm has no `Refuse` branch to build.
            Ok(Stage8Choice::Admit(select_credential_attach_capacity(
                counters,
            )))
        })?;
        Ok(match outcome {
            Stage8Outcome::Refused(response) => AttachStage8::Refused(response),
            Stage8Outcome::Reserved(reservation, commit) => {
                self.record_displacements(
                    request.participant_id,
                    commit.live_receipt_participant().displaced(),
                    displaced.len(),
                    limits.provenance_participant_window,
                )?;
                AttachStage8::Reserved {
                    reservation,
                    retire,
                    displace: displaced_entries,
                }
            }
        })
    }

    /// The receipt entries this commit ends early: the superseded attach
    /// receipt and, on the first rotation, the ended enrollment receipt.
    ///
    /// This list is also why the `LiveReceiptParticipant` window needs no
    /// eviction of its own: every live receipt that window counts for this
    /// participant is retired here, so post-commit occupancy is exactly one
    /// whatever the window size is. The old wall's wedge was precisely this —
    /// an attach refused by the receipt it was about to end.
    fn retired_receipts(
        &self,
        participant_id: u64,
        slot: &Slot,
        enrollment_token: [u8; 16],
    ) -> Vec<OccupancyEntry> {
        let mut retire = Vec::new();
        if let Some(previous) = slot.attach.as_ref() {
            retire.push(OccupancyEntry {
                expires_at: previous.receipt_expires_at,
                conversation_id: self.conversation_id,
                participant_id,
                kind: ResourceKind::AttachReceipt,
                token: previous.token.into_bytes(),
            });
        }
        if slot.enrollment_receipt_ended.is_none() {
            retire.push(OccupancyEntry {
                expires_at: slot.enrollment_receipt_expires_at,
                conversation_id: self.conversation_id,
                participant_id,
                kind: ResourceKind::EnrollmentReceipt,
                token: enrollment_token,
            });
        }
        retire
    }

    /// Counts and logs the displacements this commit performs.
    ///
    /// Displacement is silent to the arriving client by design, so this is the
    /// only record that a bound did work — "silent to experience, loud to
    /// record". A bound that neither refuses nor discloses would hide exactly
    /// what the old wall at least made loud.
    ///
    /// # Errors
    ///
    /// Returns a [`StateError`] invariant if the displaced count leaves the
    /// u64 domain.
    fn record_displacements(
        &self,
        participant_id: u64,
        live_receipt_displaced: bool,
        provenance_displaced: usize,
        window: u64,
    ) -> Result<(), StateError> {
        if live_receipt_displaced {
            crate::metrics::receipt_entries_displaced(
                ReceiptWindowScope::LiveReceiptParticipant,
                1,
            );
        }
        let displaced = u64::try_from(provenance_displaced).map_err(|_| {
            StateError::invariant("displaced provenance count exceeds the u64 domain")
        })?;
        if displaced > 0 {
            crate::metrics::receipt_entries_displaced(
                ReceiptWindowScope::ProvenanceParticipant,
                displaced,
            );
            tracing::debug!(
                conversation_id = self.conversation_id,
                participant_id,
                displaced,
                window,
                "participant provenance window displaced its oldest fingerprints so a newer one \
                 of the same participant could land"
            );
        }
        Ok(())
    }

    /// Builds the ledger entry one provenance-window member occupies.
    const fn provenance_entry(
        &self,
        participant_id: u64,
        member: ProvenanceMember,
        enrollment_token: [u8; 16],
    ) -> OccupancyEntry {
        match member {
            ProvenanceMember::Enrollment { expires_at } => OccupancyEntry {
                expires_at,
                conversation_id: self.conversation_id,
                participant_id,
                kind: ResourceKind::EnrollmentProvenance,
                token: enrollment_token,
            },
            ProvenanceMember::Attach { expires_at, token } => OccupancyEntry {
                expires_at,
                conversation_id: self.conversation_id,
                participant_id,
                kind: ResourceKind::AttachProvenance,
                token,
            },
        }
    }
}

/// Builds credential attach's two per-participant window counters.
///
/// A window whose configured size was lowered beneath retained occupancy is
/// NOT an error here, and that is the whole point of the change: the counter is
/// clamped to its full state, the selector displaces, and the commit's own plan
/// sheds however many members it takes to reach the new size. A lowered number
/// can no longer wedge a boot.
fn attach_window_counters(
    live_receipt_window: u64,
    live_receipt_occupied: u64,
    provenance_window: u64,
    provenance_occupied: u64,
) -> Result<CredentialAttachCapacityCounters, StateError> {
    Ok(CredentialAttachCapacityCounters::new(
        window_counter(live_receipt_window, live_receipt_occupied)?,
        window_counter(provenance_window, provenance_occupied)?,
    ))
}

/// One window counter, clamping over-window occupancy to exactly full.
fn window_counter(
    window: u64,
    occupied: u64,
) -> Result<liminal_protocol::lifecycle::CapacityCounter, StateError> {
    liminal_protocol::lifecycle::CapacityCounter::try_new(window, occupied.min(window)).map_err(
        |error| {
            StateError::invariant(format!(
                "validated per-participant window rejected: {error:?}"
            ))
        },
    )
}

/// Arm-level result of the attach stage-8 pass.
pub(super) enum AttachStage8<'cap> {
    /// A refusal produced by the ledger's own invariants. The receipt scopes
    /// contribute no refusal of their own; this arm exists because
    /// [`Stage8Outcome`] is shared with enrollment, which still refuses on
    /// identity capacity.
    Refused(liminal_protocol::wire::CredentialAttachResponse),
    /// Reserved; confirmed with `retire` and `displace` after the durable
    /// append.
    Reserved {
        /// Stage-8 reservation guard (rolls back unless confirmed).
        reservation: CapacityReservation<'cap>,
        /// Receipt entries the commit ends early.
        retire: Vec<OccupancyEntry>,
        /// Provenance entries the participant's own window displaced.
        displace: Vec<OccupancyEntry>,
    },
}
