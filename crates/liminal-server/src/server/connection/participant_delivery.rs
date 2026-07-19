//! Transport-neutral participant `ServerPush` delivery pump.
//!
//! This path deliberately shares only [`DeliverySink`] with subscription
//! delivery. Participant sequence, durable recipient selection, holdback, and
//! acknowledgement ownership remain independent of `Frame::Deliver`.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use liminal::protocol::{Frame, encoded_len};
use liminal_protocol::wire::{CodecError, ConversationId, ServerPush};

use super::delivery::DeliverySink;
use super::outbound::OutboundError;
use super::state::ConnectionProcessState;
use crate::server::participant::publication::ReadyPublicationBatch;
use crate::server::participant::{
    InstalledParticipantService, ObserverPublication, ParticipantOfferedProgress,
    ParticipantPublication, ParticipantPublicationError, ParticipantSemanticError,
    encode_server_push,
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

/// Exact encoded observer wake retained under current-room pressure.
///
/// Like participant heads, this is connection-owned and move-only.
#[derive(Debug)]
pub(super) struct HeldObserverHead {
    publication: ObserverPublication,
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
    #[error(
        "observer push frame for conversation {conversation_id} is {needed} bytes, exceeding empty sink capacity {capacity}"
    )]
    ObserverOversize {
        conversation_id: ConversationId,
        needed: usize,
        capacity: usize,
    },
    #[error("participant publication inbox disappeared during its owning connection slice")]
    MissingInbox,
}

impl ParticipantPumpError {
    /// Whether this pump result is the signed held-head capacity refusal rather
    /// than a transport, codec, or durable-state fault.
    pub(super) const fn is_capacity_refusal(&self) -> bool {
        matches!(
            self,
            Self::Publication(ParticipantPublicationError::InboxCapacity { .. })
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConversationOutcome {
    Done,
    Enqueued { fresh_encode: bool },
    Held { fresh_encode: bool },
}

#[derive(Debug)]
enum ObserverWork {
    Pending(ObserverPublication),
    Held,
}

#[derive(Debug)]
struct ConversationWork {
    conversation_id: ConversationId,
    observer: Option<ObserverWork>,
    participant: bool,
}

fn prepare_ready_queue(
    state: &mut ConnectionProcessState,
    ready_batch: ReadyPublicationBatch,
) -> VecDeque<ConversationWork> {
    let mut participant_ready: BTreeSet<_> = ready_batch.conversations.into_iter().collect();
    participant_ready.extend(state.held_pushes.participant_keys().copied());

    // A newly fired wake is the latest durable progress for its conversation,
    // so it replaces an older held wake before either can be handed off.
    let mut pending_observers: BTreeMap<_, _> = ready_batch
        .observer_progressed
        .into_iter()
        .map(|publication| (publication.conversation_id, publication))
        .collect();
    for conversation_id in pending_observers.keys().copied() {
        state.held_pushes.remove_observer(conversation_id);
    }

    let mut ready = participant_ready.clone();
    ready.extend(pending_observers.keys().copied());
    ready.extend(state.held_pushes.observer_keys().copied());
    ready
        .into_iter()
        .map(|conversation_id| ConversationWork {
            conversation_id,
            observer: pending_observers
                .remove(&conversation_id)
                .map(ObserverWork::Pending)
                .or_else(|| {
                    state
                        .held_pushes
                        .contains_observer(conversation_id)
                        .then_some(ObserverWork::Held)
                }),
            participant: participant_ready.contains(&conversation_id),
        })
        .collect()
}

/// Runs the connection's fair participant-and-observer publication slice.
///
/// Ready conversations are sorted and de-duplicated across both push classes.
/// Each gets at most one push before any still-ready conversation gets a second.
/// Within one conversation an observer wake is serviced before participant work
/// so a refusal wake cannot starve behind durable replay; an existing participant
/// head still precedes every later participant sequence. Every fresh encode in
/// either class debits the same `remaining` counter. Resuming an exact held head
/// does not debit again because its encode was charged when the head was created.
pub(super) fn service_participant_publications<Sink: DeliverySink>(
    state: &mut ConnectionProcessState,
    service: &InstalledParticipantService,
    sink: &mut Sink,
    budget: usize,
) -> Result<usize, ParticipantPumpError> {
    state.held_pushes.clear_capacity_refused();
    let held_limit = service.publication_conversation_limit();
    let ready_batch = match state.participant_publication.as_ref() {
        Some(inbox) => inbox.take_ready()?,
        None => return Ok(0),
    };
    let Some(connection_incarnation) = state.connection_incarnation else {
        return Ok(0);
    };

    let mut queue = prepare_ready_queue(state, ready_batch);
    let mut remaining = budget;
    let mut enqueued = 0;
    let mut deferred_participants = BTreeSet::new();

    while remaining > 0 {
        let Some(mut work) = queue.pop_front() else {
            break;
        };
        if let Some(observer) = work.observer.take() {
            let outcome = match service_one_observer(
                state,
                sink,
                work.conversation_id,
                &observer,
                held_limit,
            ) {
                Ok(outcome) => outcome,
                Err(error) if error.is_capacity_refusal() => {
                    // Preserve the exact typed observer payload and every later
                    // work item in the bounded inbox. The incumbent encoded
                    // participant head remains held and unoffered.
                    work.observer = Some(observer);
                    queue.push_front(work);
                    state.held_pushes.mark_capacity_refused();
                    requeue_deferred_work(state, queue, deferred_participants)?;
                    return Err(error);
                }
                Err(error) => return Err(error),
            };
            match outcome {
                ConversationOutcome::Enqueued { fresh_encode } => {
                    if fresh_encode {
                        remaining -= 1;
                    }
                    enqueued += 1;
                    if work.participant {
                        queue.push_back(work);
                    }
                }
                ConversationOutcome::Held { fresh_encode } => {
                    if fresh_encode {
                        remaining -= 1;
                    }
                    if work.participant {
                        deferred_participants.insert(work.conversation_id);
                    }
                }
                ConversationOutcome::Done => {}
            }
            continue;
        }
        if !work.participant {
            continue;
        }
        let outcome = match service_one_conversation(
            state,
            service,
            sink,
            connection_incarnation,
            work.conversation_id,
            held_limit,
        ) {
            Ok(outcome) => outcome,
            Err(error) if error.is_capacity_refusal() => {
                // Durable participant progress was not advanced, so requeueing
                // the conversation preserves its exact next obligation without
                // allocating another encoded head.
                queue.push_front(work);
                state.held_pushes.mark_capacity_refused();
                requeue_deferred_work(state, queue, deferred_participants)?;
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        match outcome {
            ConversationOutcome::Done => {}
            ConversationOutcome::Held { fresh_encode } => {
                if fresh_encode {
                    remaining -= 1;
                }
            }
            ConversationOutcome::Enqueued { fresh_encode } => {
                queue.push_back(work);
                if fresh_encode {
                    remaining -= 1;
                }
                enqueued += 1;
            }
        }
    }

    if !queue.is_empty() || !deferred_participants.is_empty() {
        requeue_deferred_work(state, queue, deferred_participants)?;
    }
    Ok(enqueued)
}

fn requeue_deferred_work(
    state: &ConnectionProcessState,
    queue: VecDeque<ConversationWork>,
    mut conversations: BTreeSet<ConversationId>,
) -> Result<(), ParticipantPumpError> {
    let Some(inbox) = state.participant_publication.as_ref() else {
        return Err(ParticipantPumpError::MissingInbox);
    };
    let mut observers = Vec::new();
    for work in queue {
        if work.participant {
            conversations.insert(work.conversation_id);
        }
        if let Some(ObserverWork::Pending(publication)) = work.observer {
            observers.push(publication);
        }
    }
    inbox.requeue(conversations)?;
    inbox.requeue_observers(observers)?;
    Ok(())
}

fn service_one_observer<Sink: DeliverySink>(
    state: &mut ConnectionProcessState,
    sink: &mut Sink,
    conversation_id: ConversationId,
    work: &ObserverWork,
    held_limit: u64,
) -> Result<ConversationOutcome, ParticipantPumpError> {
    let fresh_encode = matches!(work, ObserverWork::Pending(_));
    let (publication, frame, needed) = match work {
        ObserverWork::Pending(publication) => {
            let publication = *publication;
            let frame = encode_server_push(publication.into_server_push())
                .map_err(ParticipantPumpError::ParticipantCodec)?;
            let needed = encoded_len(&frame).map_err(OutboundError::Encode)?;
            (publication, frame, needed)
        }
        ObserverWork::Held => {
            let Some(held) = state.held_pushes.remove_observer(conversation_id) else {
                return Ok(ConversationOutcome::Done);
            };
            (held.publication, held.frame, held.needed)
        }
    };
    if needed > sink.capacity() {
        return Err(ParticipantPumpError::ObserverOversize {
            conversation_id,
            needed,
            capacity: sink.capacity(),
        });
    }
    if !sink.has_room(needed) {
        state.held_pushes.try_insert_observer(
            conversation_id,
            HeldObserverHead {
                publication,
                frame,
                needed,
            },
            held_limit,
        )?;
        return Ok(ConversationOutcome::Held { fresh_encode });
    }
    sink.enqueue_frame(&frame)?;
    Ok(ConversationOutcome::Enqueued { fresh_encode })
}

fn service_one_conversation<Sink: DeliverySink>(
    state: &mut ConnectionProcessState,
    service: &InstalledParticipantService,
    sink: &mut Sink,
    connection_incarnation: liminal_protocol::wire::ConnectionIncarnation,
    conversation_id: ConversationId,
    held_limit: u64,
) -> Result<ConversationOutcome, ParticipantPumpError> {
    let fresh_encode = !state.held_pushes.contains_participant(conversation_id);
    let publication_and_frame =
        if let Some(held) = state.held_pushes.remove_participant(conversation_id) {
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
        state.held_pushes.try_insert_participant(
            conversation_id,
            HeldParticipantHead {
                publication,
                frame,
                needed,
            },
            held_limit,
        )?;
        return Ok(ConversationOutcome::Held { fresh_encode });
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
    Ok(ConversationOutcome::Enqueued { fresh_encode })
}

/// Whether an exact participant or observer head waits for current outbound
/// room.
#[must_use]
pub(super) fn has_held_participant_head(state: &ConnectionProcessState) -> bool {
    !state.held_pushes.is_empty()
}

#[cfg(test)]
#[path = "participant_delivery_tests.rs"]
mod tests;
