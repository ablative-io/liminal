use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use liminal::durability::{DurableStore, open_ephemeral};
use liminal::protocol::{Frame, encode as encode_generic, encoded_len};
use liminal_protocol::wire::{
    BindingEpoch, ClientRequest, ConnectionIncarnation, Generation, ParticipantDelivery,
    ParticipantFrame, ParticipantRecord, ReceiverDirection, ServerPush, ServerValue,
    decode as decode_participant,
};

use super::{ParticipantPumpError, UNIT2_PUSH_SLICE_BUDGET, service_participant_publications};
use crate::server::connection::delivery::DeliverySink;
use crate::server::connection::outbound::{OutboundError, OutboundWriter};
use crate::server::connection::state::ConnectionProcessState;
use crate::server::connection::websocket::outbound::WebSocketOutbound;
use crate::server::participant::{
    InstalledParticipantService, ObserverPublication, ParticipantConnectionContext,
    ParticipantConnectionConversations, ParticipantOfferedProgress, ParticipantPublication,
    ParticipantPublicationError, ParticipantSemanticError, ParticipantSemanticHandler,
};

const INCARNATION: ConnectionIncarnation = ConnectionIncarnation {
    server_incarnation: 9,
    connection_ordinal: 4,
};
const BINDING: BindingEpoch = BindingEpoch {
    connection_incarnation: INCARNATION,
    capability_generation: Generation::ONE,
};
const PARTICIPANT: u64 = 7;

#[derive(Debug)]
struct FixtureSource {
    deliveries: BTreeMap<u64, Vec<ParticipantDelivery>>,
    conversation_limit: u64,
    offered: Mutex<Vec<(u64, u64)>>,
}

impl FixtureSource {
    fn new(deliveries: impl IntoIterator<Item = ParticipantDelivery>) -> Self {
        let mut by_conversation: BTreeMap<u64, Vec<ParticipantDelivery>> = BTreeMap::new();
        for delivery in deliveries {
            by_conversation
                .entry(delivery.conversation_id)
                .or_default()
                .push(delivery);
        }
        Self {
            deliveries: by_conversation,
            conversation_limit: 64,
            offered: Mutex::new(Vec::new()),
        }
    }

    fn with_conversation_limit(
        deliveries: impl IntoIterator<Item = ParticipantDelivery>,
        conversation_limit: u64,
    ) -> Self {
        let mut source = Self::new(deliveries);
        source.conversation_limit = conversation_limit;
        source
    }

    fn offered(&self) -> Vec<(u64, u64)> {
        self.offered
            .lock()
            .map_or_else(|_| Vec::new(), |offered| offered.clone())
    }
}

impl ParticipantSemanticHandler for FixtureSource {
    fn publication_conversation_limit(&self) -> u64 {
        self.conversation_limit
    }

    fn next_publication(
        &self,
        connection_incarnation: ConnectionIncarnation,
        conversation_id: u64,
        offered: Option<ParticipantOfferedProgress>,
    ) -> Result<Option<ParticipantPublication>, ParticipantSemanticError> {
        if connection_incarnation != INCARNATION {
            return Ok(None);
        }
        let through = offered
            .filter(|progress| progress.binding_epoch == BINDING)
            .map_or(0, |progress| progress.through_seq);
        Ok(self
            .deliveries
            .get(&conversation_id)
            .and_then(|deliveries| {
                deliveries
                    .iter()
                    .find(|delivery| delivery.delivery_seq > through)
            })
            .cloned()
            .map(|delivery| ParticipantPublication {
                participant_id: PARTICIPANT,
                binding_epoch: BINDING,
                delivery,
            }))
    }

    fn publication_binding_is_current(
        &self,
        _conversation_id: u64,
        participant_id: u64,
        binding_epoch: BindingEpoch,
    ) -> Result<bool, ParticipantSemanticError> {
        Ok(participant_id == PARTICIPANT && binding_epoch == BINDING)
    }

    fn record_publication_offer(
        &self,
        publication: &ParticipantPublication,
    ) -> Result<(), ParticipantSemanticError> {
        self.offered
            .lock()
            .map_err(|_| ParticipantSemanticError::Internal {
                message: "fixture offer lock poisoned".to_owned(),
            })?
            .push((publication.conversation_id(), publication.delivery_seq()));
        Ok(())
    }

    fn handle(
        &self,
        _context: ParticipantConnectionContext,
        _conversations: &mut ParticipantConnectionConversations,
        _request: ClientRequest,
    ) -> Result<ServerValue, ParticipantSemanticError> {
        Err(ParticipantSemanticError::Unavailable)
    }
}

#[derive(Debug)]
struct RecordingSink {
    capacity: usize,
    used: usize,
    frames: Vec<Frame>,
}

impl RecordingSink {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            used: 0,
            frames: Vec::new(),
        }
    }

    fn fill_current_room(&mut self) {
        self.used = self.capacity;
    }

    fn writable(&mut self) {
        self.used = 0;
    }
}

impl DeliverySink for RecordingSink {
    fn capacity(&self) -> usize {
        self.capacity
    }

    fn has_room(&self, needed: usize) -> bool {
        self.used.saturating_add(needed) <= self.capacity
    }

    fn enqueue_frame(&mut self, frame: &Frame) -> Result<(), OutboundError> {
        let needed = encoded_len(frame).map_err(OutboundError::Encode)?;
        if !self.has_room(needed) {
            return Err(OutboundError::Overflow {
                queued: self.used,
                needed,
                capacity: self.capacity,
            });
        }
        self.used += needed;
        self.frames.push(frame.clone());
        Ok(())
    }
}

fn delivery(conversation_id: u64, delivery_seq: u64) -> ParticipantDelivery {
    ParticipantDelivery {
        conversation_id,
        delivery_seq,
        record: ParticipantRecord::OrdinaryRecord {
            sender_participant_id: 2,
            payload: vec![
                conversation_id.to_le_bytes()[0],
                delivery_seq.to_le_bytes()[0],
            ],
        },
    }
}

fn service(
    source: Arc<FixtureSource>,
) -> Result<InstalledParticipantService, Box<dyn std::error::Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    InstalledParticipantService::new(source, store, u64::MAX)
        .map_err(|error| {
            std::io::Error::other(format!(
                "participant fixture configuration failed: {error:?}"
            ))
        })
        .map_err(Into::into)
}

fn state(
    service: &InstalledParticipantService,
    ready: &[u64],
) -> Result<ConnectionProcessState, String> {
    let inbox = service.new_publication_inbox();
    inbox
        .requeue(ready.iter().copied())
        .map_err(|error| error.to_string())?;
    Ok(ConnectionProcessState {
        connection_incarnation: Some(INCARNATION),
        participant_publication: Some(inbox),
        ..ConnectionProcessState::default()
    })
}

fn decode_push(frame: &Frame) -> Result<ServerPush, String> {
    let mut bytes = vec![0; encoded_len(frame).map_err(|error| error.to_string())?];
    let written = encode_generic(frame, &mut bytes).map_err(|error| error.to_string())?;
    bytes.truncate(written);
    match decode_participant(&bytes, ReceiverDirection::Client)
        .map_err(|error| format!("{error:?}"))?
    {
        ParticipantFrame::ServerPush(push) => Ok(push),
        other => Err(format!("expected participant ServerPush, got {other:?}")),
    }
}

fn decoded_deliveries(frames: &[Frame]) -> Result<Vec<ParticipantDelivery>, String> {
    frames
        .iter()
        .map(|frame| match decode_push(frame)? {
            ServerPush::ParticipantDelivery(delivery) => Ok(delivery),
            other @ ServerPush::ObserverProgressed { .. } => {
                Err(format!("expected participant delivery, got {other:?}"))
            }
        })
        .collect()
}

#[test]
fn slow_recipient_holds_only_its_head() -> Result<(), Box<dyn std::error::Error>> {
    let slow_source = Arc::new(FixtureSource::new([delivery(1, 1)]));
    let fast_source = Arc::new(FixtureSource::new([delivery(2, 1)]));
    let slow_service = service(Arc::clone(&slow_source))?;
    let fast_service = service(Arc::clone(&fast_source))?;
    let mut slow_state = state(&slow_service, &[1])?;
    let mut fast_state = state(&fast_service, &[2])?;
    let mut slow_sink = RecordingSink::new(4096);
    let mut fast_sink = RecordingSink::new(4096);
    slow_sink.fill_current_room();

    assert_eq!(
        service_participant_publications(&mut slow_state, &slow_service, &mut slow_sink, 1)?,
        0
    );
    assert_eq!(slow_state.held_pushes.participant_len(), 1);
    assert!(slow_source.offered().is_empty());

    assert_eq!(
        service_participant_publications(&mut fast_state, &fast_service, &mut fast_sink, 1)?,
        1
    );
    assert_eq!(decoded_deliveries(&fast_sink.frames)?, vec![delivery(2, 1)]);
    assert_eq!(fast_source.offered(), vec![(2, 1)]);
    assert!(fast_state.held_pushes.is_empty());

    slow_sink.writable();
    assert_eq!(
        service_participant_publications(&mut slow_state, &slow_service, &mut slow_sink, 1)?,
        1
    );
    assert_eq!(decoded_deliveries(&slow_sink.frames)?, vec![delivery(1, 1)]);
    Ok(())
}

#[test]
fn held_head_precedes_later_sequence_after_writable_ready_and_duplicate_ready_is_idempotent()
-> Result<(), Box<dyn std::error::Error>> {
    let source = Arc::new(FixtureSource::new([delivery(1, 1), delivery(1, 2)]));
    let service = service(Arc::clone(&source))?;
    let mut state = state(&service, &[1])?;
    let mut sink = RecordingSink::new(4096);
    sink.fill_current_room();

    assert_eq!(
        service_participant_publications(&mut state, &service, &mut sink, 32)?,
        0
    );
    assert_eq!(state.held_pushes.participant_len(), 1);
    assert!(source.offered().is_empty());

    sink.writable();
    assert_eq!(
        service_participant_publications(&mut state, &service, &mut sink, 32)?,
        2
    );
    let observed: Vec<_> = decoded_deliveries(&sink.frames)?
        .into_iter()
        .map(|delivery| delivery.delivery_seq)
        .collect();
    assert_eq!(observed, vec![1, 2]);

    state
        .participant_publication
        .as_ref()
        .ok_or("missing publication inbox")?
        .requeue([1, 1])?;
    assert_eq!(
        service_participant_publications(&mut state, &service, &mut sink, 32)?,
        0
    );
    assert_eq!(source.offered(), vec![(1, 1), (1, 2)]);
    Ok(())
}

#[test]
fn push_slice_budget_and_round_robin_are_exact() -> Result<(), Box<dyn std::error::Error>> {
    let deliveries = (1..=20).flat_map(|sequence| [delivery(1, sequence), delivery(2, sequence)]);
    let source = Arc::new(FixtureSource::new(deliveries));
    let service = service(source)?;
    let mut state = state(&service, &[1, 2])?;
    let mut sink = RecordingSink::new(1_000_000);
    state
        .participant_publication
        .as_ref()
        .ok_or("missing publication inbox")?
        .requeue_observers((3..=6).map(|conversation_id| ObserverPublication {
            conversation_id,
            refused_epoch: 10 + conversation_id,
            observer_progress: 20 + conversation_id,
        }))?;

    assert_eq!(
        service_participant_publications(&mut state, &service, &mut sink, UNIT2_PUSH_SLICE_BUDGET,)?,
        UNIT2_PUSH_SLICE_BUDGET
    );
    let first = sink
        .frames
        .iter()
        .map(decode_push)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        first
            .iter()
            .take(6)
            .map(|push| match push {
                ServerPush::ParticipantDelivery(delivery) => {
                    (delivery.conversation_id, delivery.delivery_seq)
                }
                ServerPush::ObserverProgressed {
                    conversation_id,
                    refused_epoch: _,
                    observer_progress: _,
                } => (*conversation_id, 0),
            })
            .collect::<Vec<_>>(),
        vec![(1, 1), (2, 1), (3, 0), (4, 0), (5, 0), (6, 0)]
    );
    assert_eq!(
        first
            .iter()
            .filter(|push| matches!(push, ServerPush::ObserverProgressed { .. }))
            .count(),
        4,
        "observer wakes debit the same exact 32-push slice"
    );
    assert_eq!(
        first
            .iter()
            .filter(|push| matches!(push, ServerPush::ParticipantDelivery(_)))
            .count(),
        UNIT2_PUSH_SLICE_BUDGET - 4
    );
    assert_eq!(
        service_participant_publications(&mut state, &service, &mut sink, UNIT2_PUSH_SLICE_BUDGET,)?,
        12
    );
    assert_eq!(sink.frames.len(), 44);
    Ok(())
}

#[test]
fn held_fresh_encodes_share_the_exact_slice_budget_across_push_classes()
-> Result<(), Box<dyn std::error::Error>> {
    let participant_conversations = 1..=20;
    let source = Arc::new(FixtureSource::new(
        participant_conversations
            .clone()
            .map(|conversation_id| delivery(conversation_id, 1)),
    ));
    let service = service(source)?;
    let mut state = state(&service, &participant_conversations.collect::<Vec<_>>())?;
    state
        .participant_publication
        .as_ref()
        .ok_or("missing publication inbox")?
        .requeue_observers((21..=40).map(|conversation_id| ObserverPublication {
            conversation_id,
            refused_epoch: 10 + conversation_id,
            observer_progress: 20 + conversation_id,
        }))?;
    let mut sink = RecordingSink::new(4096);
    sink.fill_current_room();

    assert_eq!(
        service_participant_publications(&mut state, &service, &mut sink, UNIT2_PUSH_SLICE_BUDGET,)?,
        0
    );
    let held_count = state
        .held_pushes
        .participant_len()
        .checked_add(state.held_pushes.observer_len())
        .ok_or("combined held push count overflowed")?;
    assert_eq!(
        held_count, UNIT2_PUSH_SLICE_BUDGET,
        "fresh held encodes must stop at the shared signed slice budget"
    );
    Ok(())
}

#[path = "participant_delivery_held_cap_tests.rs"]
mod held_cap;

#[path = "participant_delivery_observer_requeue_tests.rs"]
mod observer_requeue;

#[test]
fn tcp_and_websocket_publish_identical_participant_bytes() -> Result<(), Box<dyn std::error::Error>>
{
    let expected = delivery(41, 9);
    let tcp_source = Arc::new(FixtureSource::new([expected.clone()]));
    let ws_source = Arc::new(FixtureSource::new([expected.clone()]));
    let tcp_service = service(tcp_source)?;
    let ws_service = service(ws_source)?;
    let mut tcp_state = state(&tcp_service, &[41])?;
    let mut ws_state = state(&ws_service, &[41])?;
    let mut tcp = OutboundWriter::new();
    let mut websocket = WebSocketOutbound::new();

    assert_eq!(
        service_participant_publications(&mut tcp_state, &tcp_service, &mut tcp, 1)?,
        1
    );
    assert_eq!(
        service_participant_publications(&mut ws_state, &ws_service, &mut websocket, 1)?,
        1
    );
    let tcp_bytes = tcp.take_bytes();
    let mut websocket_messages = websocket.take_messages();
    assert_eq!(websocket_messages.len(), 1);
    let websocket_bytes = websocket_messages
        .pop()
        .ok_or("WebSocket participant message was absent")?;
    assert_eq!(tcp_bytes, websocket_bytes);
    let decoded = decode_participant(&tcp_bytes, ReceiverDirection::Client)
        .map_err(|error| format!("{error:?}"))?;
    assert_eq!(
        decoded,
        ParticipantFrame::ServerPush(ServerPush::ParticipantDelivery(expected))
    );
    Ok(())
}

#[test]
fn oversize_is_config_corruption_not_pressure_policy() -> Result<(), Box<dyn std::error::Error>> {
    let maximal = ParticipantDelivery {
        conversation_id: 1,
        delivery_seq: 1,
        record: ParticipantRecord::OrdinaryRecord {
            sender_participant_id: 2,
            payload: vec![0; 65_433],
        },
    };
    let tcp_service = service(Arc::new(FixtureSource::new([maximal.clone()])))?;
    let ws_service = service(Arc::new(FixtureSource::new([maximal])))?;
    let mut tcp_state = state(&tcp_service, &[1])?;
    let mut ws_state = state(&ws_service, &[1])?;
    let mut tcp = OutboundWriter::new();
    let mut websocket = WebSocketOutbound::new();
    assert_eq!(
        service_participant_publications(&mut tcp_state, &tcp_service, &mut tcp, 1)?,
        1
    );
    assert_eq!(
        service_participant_publications(&mut ws_state, &ws_service, &mut websocket, 1)?,
        1
    );

    let source = Arc::new(FixtureSource::new([delivery(1, 1)]));
    let service = service(source)?;
    let mut state = state(&service, &[1])?;
    let mut sink = RecordingSink::new(8);

    let Err(error) = service_participant_publications(&mut state, &service, &mut sink, 1) else {
        return Err("an encoded participant frame fit an eight-byte sink".into());
    };
    assert!(matches!(error, ParticipantPumpError::Oversize { .. }));
    assert!(state.held_pushes.is_empty());
    assert!(state.participant_offered.is_empty());
    Ok(())
}
