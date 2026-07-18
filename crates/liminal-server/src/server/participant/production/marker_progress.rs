//! Protocol-owned marker delivery progress projected from coupled frontier authority.

use liminal_protocol::lifecycle::{ClosureState, Event, ParticipantCursorProgress, StoredEdge};
use liminal_protocol::wire::BindingEpoch;

use super::outbox_log::StoredMarkerAckCommitted;
use super::state::{ConversationAuthority, StateError};

pub(super) fn marker_replay_progress(
    authority: &ConversationAuthority,
    row: &StoredMarkerAckCommitted,
) -> Result<ParticipantCursorProgress, StateError> {
    marker_delivery_progress(
        authority,
        row.request.participant_id,
        row.delivered_binding_epoch,
        row.offered_marker_delivery_seq,
    )
}

pub(super) fn marker_delivery_progress(
    authority: &ConversationAuthority,
    participant_id: u64,
    delivered_binding_epoch: BindingEpoch,
    offered_marker_delivery_seq: u64,
) -> Result<ParticipantCursorProgress, StateError> {
    let frontier = authority
        .frontier
        .as_ref()
        .ok_or(StateError::FrontierUnavailable)?;
    let delivered_event = || {
        Event::marker_delivered(
            participant_id,
            delivered_binding_epoch,
            offered_marker_delivery_seq,
        )
    };
    if let Some(progress) = frontier.frontiers().project_offered_marker_progress(
        participant_id,
        delivered_binding_epoch,
        offered_marker_delivery_seq,
        delivered_event(),
    ) {
        return Ok(progress);
    }
    let closure = frontier.closure_accounting().state();
    match closure {
        ClosureState::Owed {
            debt,
            edge: StoredEdge::MarkerDelivery(delivery),
        } => {
            let delivered = delivery.delivered(debt, delivered_event()).map_err(|_| {
                StateError::invariant(
                    "stored MarkerAck witness does not match marker delivery authority",
                )
            })?;
            match delivered {
                ClosureState::Owed {
                    edge: StoredEdge::ParticipantCursorProgress(progress),
                    ..
                } if progress.marker_delivery_seq() == Some(offered_marker_delivery_seq) => {
                    Ok(progress)
                }
                ClosureState::Clear | ClosureState::Owed { .. } => Err(StateError::invariant(
                    "stored MarkerAck witness did not produce marker progress",
                )),
            }
        }
        ClosureState::Owed {
            edge: StoredEdge::ParticipantCursorProgress(progress),
            ..
        } if progress.marker_delivery_seq() == Some(offered_marker_delivery_seq)
            && progress.binding_epoch() == delivered_binding_epoch =>
        {
            Ok(progress)
        }
        ClosureState::Clear | ClosureState::Owed { .. } => Err(StateError::invariant(
            "stored MarkerAck has no matching marker delivery authority",
        )),
    }
}
