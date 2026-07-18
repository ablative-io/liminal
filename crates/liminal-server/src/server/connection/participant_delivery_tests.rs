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
use crate::server::connection::outbound::OutboundError;
use crate::server::connection::state::ConnectionProcessState;
use crate::server::participant::{
    InstalledParticipantService, ParticipantConnectionContext, ParticipantConnectionConversations,
    ParticipantOfferedProgress, ParticipantPublication, ParticipantSemanticError,
    ParticipantSemanticHandler,
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
            offered: Mutex::new(Vec::new()),
        }
    }

    fn offered(&self) -> Vec<(u64, u64)> {
        self.offered
            .lock()
            .map_or_else(|_| Vec::new(), |offered| offered.clone())
    }
}

impl ParticipantSemanticHandler for FixtureSource {
    fn publication_conversation_limit(&self) -> u64 {
        64
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
            payload: vec![conversation_id as u8, delivery_seq as u8],
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

fn decoded_deliveries(frames: &[Frame]) -> Result<Vec<ParticipantDelivery>, String> {
    frames
        .iter()
        .map(|frame| {
            let mut bytes = vec![0; encoded_len(frame).map_err(|error| error.to_string())?];
            let written = encode_generic(frame, &mut bytes).map_err(|error| error.to_string())?;
            bytes.truncate(written);
            match decode_participant(&bytes, ReceiverDirection::Client)
                .map_err(|error| format!("{error:?}"))?
            {
                ParticipantFrame::ServerPush(ServerPush::ParticipantDelivery(delivery)) => {
                    Ok(delivery)
                }
                other => Err(format!("expected participant ServerPush, got {other:?}")),
            }
        })
        .collect()
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
    assert_eq!(state.held_participant_pushes.len(), 1);
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

    assert_eq!(
        service_participant_publications(&mut state, &service, &mut sink, UNIT2_PUSH_SLICE_BUDGET,)?,
        UNIT2_PUSH_SLICE_BUDGET
    );
    let first = decoded_deliveries(&sink.frames)?;
    assert_eq!(
        first
            .iter()
            .take(4)
            .map(|delivery| (delivery.conversation_id, delivery.delivery_seq))
            .collect::<Vec<_>>(),
        vec![(1, 1), (2, 1), (1, 2), (2, 2)]
    );
    assert_eq!(
        service_participant_publications(&mut state, &service, &mut sink, UNIT2_PUSH_SLICE_BUDGET,)?,
        8
    );
    Ok(())
}

#[test]
fn oversize_is_config_corruption_not_pressure_policy() -> Result<(), Box<dyn std::error::Error>> {
    let source = Arc::new(FixtureSource::new([delivery(1, 1)]));
    let service = service(source)?;
    let mut state = state(&service, &[1])?;
    let mut sink = RecordingSink::new(8);

    let error = service_participant_publications(&mut state, &service, &mut sink, 1)
        .expect_err("an encoded participant frame cannot fit an eight-byte sink");
    assert!(matches!(error, ParticipantPumpError::Oversize { .. }));
    assert!(state.held_participant_pushes.is_empty());
    assert!(state.participant_offered.is_empty());
    Ok(())
}
