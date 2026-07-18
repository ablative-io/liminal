//! Transport-neutral participant `ServerPush` delivery pump.
//!
//! This path deliberately shares only [`DeliverySink`] with subscription
//! delivery. Participant sequence, durable recipient selection, holdback, and
//! acknowledgement ownership remain independent of `Frame::Deliver`.

use std::collections::{BTreeSet, VecDeque};

use liminal::protocol::{Frame, encoded_len};
use liminal_protocol::wire::{CodecError, ConversationId, ServerPush};

use super::delivery::DeliverySink;
use super::outbound::OutboundError;
use super::state::ConnectionProcessState;
use crate::server::participant::{
    InstalledParticipantService, ParticipantOfferedProgress, ParticipantPublication,
    ParticipantPublicationError, ParticipantSemanticError, encode_server_push,
};

/// Signed participant/observer push budget for one connection scheduler slice.
pub(super) const UNIT2_PUSH_SLICE_BUDGET: usize = 32;

/// Exact encoded head retained under current-room pressure.
///
/// Move-only by construction: neither this wrapper nor connection state is
/// cloneable, and only the owning connection process can resume it.
#[derive(Debug)]
pub(super) struct HeldParticipantHead {
    publication: ParticipantPublication,
    frame: Frame,
    needed: usize,
}

/// Typed publication fault. Current-room pressure is not an error and never
/// appears here; a complete frame larger than an empty sink is configuration or
/// durable-schema corruption.
#[derive(Debug, thiserror::Error)]
pub(super) enum ParticipantPumpError {
    #[error(transparent)]
    Publication(#[from] ParticipantPublicationError),
    #[error(transparent)]
    Semantic(#[from] ParticipantSemanticError),
    #[error("participant push codec failed: {0:?}")]
    ParticipantCodec(CodecError),
    #[error(transparent)]
    Outbound(#[from] OutboundError),
    #[error(
        "participant push frame for conversation {conversation_id} sequence {delivery_seq} is {needed} bytes, exceeding empty sink capacity {capacity}"
    )]
    Oversize {
        conversation_id: ConversationId,
        delivery_seq: u64,
        needed: usize,
        capacity: usize,
    },
    #[error("participant publication inbox disappeared during its owning connection slice")]
    MissingInbox,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConversationOutcome {
    Done,
    Enqueued,
    Held,
}

/// Runs the connection's fair participant publication slice.
///
/// Ready conversations are sorted and de-duplicated by the inbox. Each gets at
/// most one offer before any still-ready conversation gets a second. Held heads
/// enter the same queue but are always resumed before selecting a later durable
/// sequence for their conversation.
pub(super) fn service_participant_publications<Sink: DeliverySink>(
    state: &mut ConnectionProcessState,
    service: &InstalledParticipantService,
    sink: &mut Sink,
    budget: usize,
) -> Result<usize, ParticipantPumpError> {
    let ready_conversations = match state.participant_publication.as_ref() {
        Some(inbox) => inbox.take_ready()?,
        None => return Ok(0),
    };
    let Some(connection_incarnation) = state.connection_incarnation else {
        return Ok(0);
    };

    let mut ready: BTreeSet<_> = ready_conversations.into_iter().collect();
    ready.extend(state.held_participant_pushes.keys().copied());
    let mut queue: VecDeque<_> = ready.into_iter().collect();
    let mut remaining = budget;
    let mut enqueued = 0;

    while remaining > 0 {
        let Some(conversation_id) = queue.pop_front() else {
            break;
        };
        match service_one_conversation(
            state,
            service,
            sink,
            connection_incarnation,
            conversation_id,
        )? {
            ConversationOutcome::Done | ConversationOutcome::Held => {}
            ConversationOutcome::Enqueued => {
                queue.push_back(conversation_id);
                remaining -= 1;
                enqueued += 1;
            }
        }
    }

    if !queue.is_empty() {
        let Some(inbox) = state.participant_publication.as_ref() else {
            return Err(ParticipantPumpError::MissingInbox);
        };
        inbox.requeue(queue)?;
    }
    Ok(enqueued)
}

fn service_one_conversation<Sink: DeliverySink>(
    state: &mut ConnectionProcessState,
    service: &InstalledParticipantService,
    sink: &mut Sink,
    connection_incarnation: liminal_protocol::wire::ConnectionIncarnation,
    conversation_id: ConversationId,
) -> Result<ConversationOutcome, ParticipantPumpError> {
    let publication_and_frame =
        if let Some(held) = state.held_participant_pushes.remove(&conversation_id) {
            if !service.publication_binding_is_current(
                conversation_id,
                held.publication.participant_id,
                held.publication.binding_epoch,
            )? {
                return Ok(ConversationOutcome::Done);
            }
            (held.publication, held.frame, held.needed)
        } else {
            let offered = state.participant_offered.get(&conversation_id).copied();
            let Some(publication) =
                service.next_publication(connection_incarnation, conversation_id, offered)?
            else {
                return Ok(ConversationOutcome::Done);
            };
            let frame = encode_server_push(ServerPush::ParticipantDelivery(
                publication.delivery.clone(),
            ))
            .map_err(ParticipantPumpError::ParticipantCodec)?;
            let needed = encoded_len(&frame).map_err(OutboundError::Encode)?;
            (publication, frame, needed)
        };
    let (publication, frame, needed) = publication_and_frame;

    if needed > sink.capacity() {
        return Err(ParticipantPumpError::Oversize {
            conversation_id,
            delivery_seq: publication.delivery_seq(),
            needed,
            capacity: sink.capacity(),
        });
    }
    if !sink.has_room(needed) {
        state.held_participant_pushes.insert(
            conversation_id,
            HeldParticipantHead {
                publication,
                frame,
                needed,
            },
        );
        return Ok(ConversationOutcome::Held);
    }

    sink.enqueue_frame(&frame)?;
    service.record_publication_offer(&publication)?;
    let through_seq = publication.delivery_seq();
    state.participant_offered.insert(
        conversation_id,
        ParticipantOfferedProgress {
            binding_epoch: publication.binding_epoch,
            through_seq,
        },
    );
    Ok(ConversationOutcome::Enqueued)
}

/// Whether an exact participant head waits for current outbound room.
#[must_use]
pub(super) fn has_held_participant_head(state: &ConnectionProcessState) -> bool {
    !state.held_participant_pushes.is_empty()
}

#[cfg(test)]
#[path = "participant_delivery_tests.rs"]
mod tests;
