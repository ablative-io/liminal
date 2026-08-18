//! Cold-restore replay of the two durable ack shapes.
//!
//! The seam is the direction the inputs travel. Live arms read state to build a
//! classification; these read a DURABLE ROW and must reproduce the
//! classification the original commit reached — so every function here is an
//! audit before it is a mutation: the stored census is cross-checked against
//! what the frozen selector says today, and any disagreement is a divergence
//! from history rather than a refusal to answer. Nothing here consults
//! connection-scoped facts, because none of them are durable classification
//! inputs.

use liminal_protocol::lifecycle::{
    BindingState, MarkerAckDecision, MarkerProofState, PresentedIdentity, RecipientAckObligations,
    apply_marker_ack, apply_marker_ack_frontier,
};
use liminal_protocol::wire::ServerValue;

use super::super::facts::Digest;
use super::super::log::{StoredAck, StoredBindingEpoch};
use super::super::marker_progress::marker_replay_progress;
use super::super::observer_progress::ObserverProgressSourceMetadata;
use super::super::outbox_log::StoredMarkerAckCommitted;
use super::super::state::{ConversationAuthority, StateError};

use super::binding_fate::progress_pending_marker_binding_fate;

impl ConversationAuthority {
    /// Replays one committed zero-debt row with testimony sealed at its exact
    /// outbox merge boundary.
    pub(in crate::server::participant::production) fn replay_zero_debt_ack_row(
        &mut self,
        request: StoredAck,
        receiving_epoch: StoredBindingEpoch,
        contiguously_available_through: u64,
        ack_obligations: Option<(RecipientAckObligations, u64)>,
    ) -> Result<(), StateError> {
        let (obligations, reconciled_available_through) = ack_obligations.ok_or_else(|| {
            StateError::invariant("zero-debt ack replay is missing recipient obligations")
        })?;
        self.replay_zero_debt_ack(
            request,
            receiving_epoch,
            contiguously_available_through,
            reconciled_available_through,
            &obligations,
        )
    }

    /// Replays one committed zero-debt ack entry from its stored inputs.
    fn replay_zero_debt_ack(
        &mut self,
        request: StoredAck,
        receiving_epoch: StoredBindingEpoch,
        contiguously_available_through: u64,
        reconciled_available_through: u64,
        obligations: &RecipientAckObligations,
    ) -> Result<(), StateError> {
        if contiguously_available_through != reconciled_available_through {
            return Err(StateError::invariant(format!(
                "durable zero-debt ack availability {contiguously_available_through} differs from reconciled recipient availability {reconciled_available_through}"
            )));
        }
        let request = request.to_request()?;
        let outcome = self.ack_commit(
            &request,
            receiving_epoch,
            obligations,
            contiguously_available_through,
            None,
        )?;
        // A durable ack entry is appended only for a committed decision, so a
        // replay that classifies as anything else diverged from history.
        if !matches!(outcome.value, ServerValue::AckCommitted(_)) {
            return Err(StateError::invariant(
                "durable zero-debt ack entry replayed to a non-committed decision",
            ));
        }
        self.advance_log_head()?;
        Ok(())
    }

    /// Replays one extension `MarkerAck` through the authoritative selector and
    /// checks the complete stored commit census before installing any state.
    pub(in crate::server::participant::production) fn replay_marker_ack_extension(
        &mut self,
        row: &StoredMarkerAckCommitted,
    ) -> Result<(), StateError> {
        if row.request.conversation_id != self.conversation_id
            || row.offered_marker_delivery_seq != row.request.marker_delivery_seq
            || row.receiving_binding_epoch != row.delivered_binding_epoch
        {
            return Err(StateError::invariant(
                "stored MarkerAck request and delivery witness disagree",
            ));
        }
        let progress = marker_replay_progress(self, row)?;
        let identity = self
            .slots
            .get(&row.request.participant_id)
            .map_or(PresentedIdentity::Absent, |slot| {
                PresentedIdentity::<Digest, Digest, Digest>::Live(&slot.member)
            });
        let detached = BindingState::Detached;
        let binding = self
            .slots
            .get(&row.request.participant_id)
            .map_or(&detached, |slot| &slot.binding);
        let cursor = self
            .slots
            .get(&row.request.participant_id)
            .map_or(0, |slot| slot.member.cursor());
        // A log written before ordinary acks retired crossed marker anchors
        // can hold an ordinary-ack row that crosses this marker FOLLOWED by
        // this marker-ack row — both committed under the old accounting.
        // Replaying the ordinary row through the fixed applies already
        // advanced the cursor to (or past) the marker and retired its anchor;
        // re-committing this row would retire the anchor a second time and
        // kill the load. The acceptance this row witnesses is already durably
        // reflected in the replayed state, so it is a no-op here. The sealed
        // binding-fate token is safe to skip with it: the crossing row
        // progressed it, and token replay is idempotent.
        if cursor >= row.offered_marker_delivery_seq {
            return Ok(());
        }
        let marker_state = MarkerProofState::new(
            cursor,
            false,
            Some(row.offered_marker_delivery_seq),
            row.delivered_binding_epoch,
            Some(progress),
        );
        let MarkerAckDecision::Commit(commit) = apply_marker_ack(
            identity,
            binding,
            row.receiving_binding_epoch,
            &row.request,
            &marker_state,
        ) else {
            return Err(StateError::invariant(
                "stored MarkerAck replayed to a non-commit decision",
            ));
        };
        if commit.canonical_request() != row.request
            || commit.receiving_binding_epoch() != row.receiving_binding_epoch
            || commit.offered_marker_delivery_seq() != row.offered_marker_delivery_seq
            || commit.delivered_binding_epoch() != row.delivered_binding_epoch
            || commit.from_cursor() != row.from_cursor
            || commit.resulting_cursor() != row.resulting_cursor
        {
            return Err(StateError::invariant(
                "stored MarkerAck post-transition audit drifted",
            ));
        }
        let observer_projection = commit.observer_progress_projection();
        let metadata = ObserverProgressSourceMetadata::marker_ack(
            row.base_log_head,
            row.extension_sequence,
            row.request.conversation_id,
            row.request.participant_id,
            row.request.marker_delivery_seq,
            row.resulting_cursor,
        );
        let transitioned =
            apply_marker_ack_frontier(self.take_frontier()?, commit).map_err(|failure| {
                StateError::invariant(format!(
                    "stored MarkerAck frontier transition failed: {:?}",
                    failure.error()
                ))
            })?;
        let (commit, frontier) = transitioned.into_parts();
        let slot = self
            .slots
            .get_mut(&row.request.participant_id)
            .ok_or_else(|| StateError::invariant("stored MarkerAck participant is absent"))?;
        // SITE TWO of `#26`, and the one that made the fault survive restarts.
        // This is the load path — `ops_session_replay` -> `recipient_ack_obligations`
        // -> `outbox_replay` -> here — so without this call a boot did not clear
        // the frozen token, it REBUILT it from durable rows every time.
        progress_pending_marker_binding_fate(slot, &commit)?;
        let outcome = commit.apply_to(&mut slot.member).map_err(|error| {
            StateError::invariant(format!("stored MarkerAck cursor commit failed: {error:?}"))
        })?;
        let request = outcome.request();
        if request.conversation_id != row.request.conversation_id
            || request.participant_id != row.request.participant_id
            || request.capability_generation != row.request.capability_generation
            || request.marker_delivery_seq != row.request.marker_delivery_seq
        {
            return Err(StateError::invariant(
                "stored MarkerAck outcome request drifted",
            ));
        }
        self.install_frontier(frontier)?;
        self.record_observer_progress_projection(observer_projection, metadata)?;
        Ok(())
    }
}
