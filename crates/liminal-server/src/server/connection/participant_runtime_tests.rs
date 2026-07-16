//! End-to-end provenance from durable supervisor allocation to semantic context.

use std::error::Error;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use liminal::durability::{DurableStore, bridge::block_on, open_ephemeral};
use liminal::protocol::{
    Frame, MessageEnvelope, ProtocolError, ProtocolVersion, SchemaId, decode as decode_generic,
    encode as encode_generic, encoded_len as generic_encoded_len,
};
use liminal_protocol::{
    lifecycle::{
        BindingRequiredLookupResult, BindingState, ParticipantBindingRequest, PresentedIdentity,
        lookup_binding_required,
    },
    wire::{
        ClientRequest, Generation, PARTICIPANT_FRAME_TYPE, ParticipantFrame, RecordAdmission,
        ServerValue, encode as encode_participant, encoded_len as participant_encoded_len,
    },
};

use super::conversation::ConnectionConversation;
use super::services::{ConnectionServices, ConnectionSubscription, PublishOutcome};
use super::supervisor::ConnectionSupervisor;
use crate::ServerError;
use crate::server::participant::incarnation_stream::IncarnationStream;
use crate::server::participant::{
    InstalledParticipantService, PARTICIPANT_CAPABILITY_BIT, ParticipantConnectionContext,
    ParticipantSemanticError, ParticipantSemanticHandler,
};

#[derive(Debug)]
struct RecordingParticipantHandler {
    observations: mpsc::Sender<(ParticipantConnectionContext, ClientRequest)>,
}

impl ParticipantSemanticHandler for RecordingParticipantHandler {
    fn handle(
        &self,
        context: ParticipantConnectionContext,
        request: ClientRequest,
    ) -> Result<ServerValue, ParticipantSemanticError> {
        self.observations
            .send((context, request.clone()))
            .map_err(|error| ParticipantSemanticError::Internal {
                message: format!("participant observation receiver unavailable: {error}"),
            })?;
        let ClientRequest::RecordAdmission(record) = request else {
            return Err(ParticipantSemanticError::Internal {
                message: "runtime provenance fixture expected record admission".to_owned(),
            });
        };
        let lookup = lookup_binding_required::<[u8; 1], [u8; 1], [u8; 1]>(
            PresentedIdentity::Absent,
            &BindingState::Detached,
            None,
            &ParticipantBindingRequest::RecordAdmission(record),
        );
        let BindingRequiredLookupResult::ParticipantUnknown(value) = lookup else {
            return Err(ParticipantSemanticError::Internal {
                message: "runtime provenance lookup did not select ParticipantUnknown".to_owned(),
            });
        };
        Ok(ServerValue::ParticipantUnknown(value))
    }
}

#[derive(Debug)]
struct ParticipantOnlyServices {
    participant_service: InstalledParticipantService,
}

impl ParticipantOnlyServices {
    fn unsupported(operation: &str) -> ServerError {
        ServerError::ListenerAccept {
            message: format!("participant runtime fixture does not support {operation}"),
        }
    }
}

impl ConnectionServices for ParticipantOnlyServices {
    fn participant_service(&self) -> Option<InstalledParticipantService> {
        Some(self.participant_service.clone())
    }

    fn publish(
        &self,
        _channel: &str,
        _envelope: &MessageEnvelope,
        _idempotency_key: Option<&str>,
    ) -> Result<PublishOutcome, ServerError> {
        Err(Self::unsupported("publish"))
    }

    fn subscribe(
        &self,
        _channel: &str,
        _accepted_schemas: &[SchemaId],
        _install: Option<liminal::channel::InboxInstall>,
    ) -> Result<ConnectionSubscription, ServerError> {
        Err(Self::unsupported("subscribe"))
    }

    fn unsubscribe(&self, _subscription: ConnectionSubscription) -> Result<(), ServerError> {
        Err(Self::unsupported("unsubscribe"))
    }

    fn open_conversation(
        &self,
        _conversation_id: u64,
        _subject: &str,
    ) -> Result<ConnectionConversation, ServerError> {
        Err(Self::unsupported("conversation open"))
    }

    fn conversation_message(
        &self,
        _conversation: &ConnectionConversation,
        _envelope: &MessageEnvelope,
    ) -> Result<(), ServerError> {
        Err(Self::unsupported("conversation message"))
    }

    fn close_conversation(&self, _conversation: ConnectionConversation) -> Result<(), ServerError> {
        Err(Self::unsupported("conversation close"))
    }

    fn flush_durable_state(&self) -> Result<(), ServerError> {
        Ok(())
    }

    fn supports_channel_operations(&self) -> bool {
        false
    }
}

fn tcp_pair() -> Result<(TcpStream, TcpStream), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address: SocketAddr = listener.local_addr()?;
    let client = TcpStream::connect(address)?;
    let (server, _) = listener.accept()?;
    Ok((client, server))
}

fn encode_frame(frame: &Frame) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut bytes = vec![0; generic_encoded_len(frame)?];
    let written = encode_generic(frame, &mut bytes)?;
    bytes.truncate(written);
    Ok(bytes)
}

fn encode_request(request: ClientRequest) -> Result<Vec<u8>, String> {
    let frame = ParticipantFrame::ClientRequest(request);
    let mut bytes = vec![0; participant_encoded_len(&frame).map_err(|error| format!("{error:?}"))?];
    let written = encode_participant(&frame, &mut bytes).map_err(|error| format!("{error:?}"))?;
    bytes.truncate(written);
    Ok(bytes)
}

fn read_frame(socket: &mut TcpStream, buffer: &mut Vec<u8>) -> Result<Frame, Box<dyn Error>> {
    loop {
        match decode_generic(buffer) {
            Ok((frame, consumed)) => {
                buffer.drain(..consumed);
                return Ok(frame);
            }
            Err(
                ProtocolError::IncompleteHeader { .. } | ProtocolError::TruncatedPayload { .. },
            ) => {
                let mut chunk = [0_u8; 512];
                let read = socket.read(&mut chunk)?;
                if read == 0 {
                    return Err("connection closed before a complete frame arrived".into());
                }
                buffer.extend_from_slice(chunk.get(..read).unwrap_or(&[]));
            }
            Err(error) => return Err(Box::new(error)),
        }
    }
}

#[test]
fn supervisor_allocation_reaches_real_process_handler_as_exact_context()
-> Result<(), Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let (observations, received_observations) = mpsc::channel();
    let participant_service = InstalledParticipantService::new(
        Arc::new(RecordingParticipantHandler { observations }),
        Arc::clone(&store),
        u64::MAX,
    )
    .map_err(|error| ServerError::ConfigValidation {
        message: format!("invalid participant test wire-frame limit: {error:?}"),
    })?;
    let services: Arc<dyn ConnectionServices> = Arc::new(ParticipantOnlyServices {
        participant_service,
    });
    let supervisor = ConnectionSupervisor::with_services(services)?;
    let (mut client, server) = tcp_pair()?;
    client.set_read_timeout(Some(Duration::from_secs(5)))?;
    client.set_write_timeout(Some(Duration::from_secs(5)))?;

    let handle = supervisor.spawn_connection(server)?;
    let expected_incarnation = handle
        .connection_incarnation()
        .ok_or("participant-enabled connection had no durable incarnation")?;
    let durable_events = block_on(store.read_from(IncarnationStream::stream_key(), 0, 8))??;
    assert_eq!(
        durable_events.len(),
        2,
        "startup and connection allocation must both be durable before spawn returns"
    );

    client.write_all(&encode_frame(&Frame::Connect {
        flags: 0,
        min_version: ProtocolVersion::new(1, 0),
        max_version: ProtocolVersion::new(1, 0),
        auth_token: Vec::new(),
    })?)?;
    let mut inbound = Vec::new();
    assert!(matches!(
        read_frame(&mut client, &mut inbound)?,
        Frame::ConnectAck { capabilities, .. }
            if capabilities == PARTICIPANT_CAPABILITY_BIT
    ));

    let request = ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: 70,
        participant_id: 2,
        capability_generation: Generation::new(3).ok_or("fixture generation was zero")?,
        payload: vec![1, 2, 3],
    });
    client.write_all(&encode_request(request.clone())?)?;
    assert!(matches!(
        read_frame(&mut client, &mut inbound)?,
        Frame::Unknown {
            type_id: PARTICIPANT_FRAME_TYPE,
            ..
        }
    ));
    let (context, observed_request) = received_observations.recv_timeout(Duration::from_secs(5))?;
    assert_eq!(context.connection_incarnation(), expected_incarnation);
    assert_eq!(observed_request, request);

    drop(client);
    supervisor.shutdown();
    Ok(())
}
