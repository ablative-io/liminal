//! Reusable real-socket participant server fixture and deterministic replay gate.

use std::collections::VecDeque;
use std::error::Error;
use std::io::Write;
use std::net::{Shutdown, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::Receiver;
use std::time::Duration;

use liminal::durability::DurableStore;
use liminal::protocol::{Frame, ProtocolVersion};
use liminal_protocol::wire::{
    BindingEpoch, ClientRequest, ConnectionIncarnation, ConversationId, ObserverRecoveryHandshake,
    ParticipantFrame, ParticipantId, ReceiverDirection, ServerPush, ServerValue,
    decode as decode_participant,
};
use tungstenite::Message;
use tungstenite::client::IntoClientRequest;
use tungstenite::protocol::WebSocket;

use crate::config::types::{
    LimitsConfig, ParticipantConfig, ServerConfig, ServicesConfig, WebSocketConfig,
};
use crate::server::connection::{
    ConnectionHandle, ConnectionServices, ConnectionSupervisor, WebSocketListener,
};
use crate::server::listener::ServerListener;
use crate::server::participant::{
    ConnectionFateWorkItem, InstalledParticipantService, ObserverPublicationTarget,
    PARTICIPANT_CAPABILITY_BIT, ParticipantConnectionContext, ParticipantConnectionConversations,
    ParticipantConnectionFateOutcome, ParticipantOfferedProgress, ParticipantPublication,
    ParticipantSemanticError, ParticipantSemanticHandler, ParticipantSemanticOutcome,
    ParticipantServiceFatal,
};
use crate::server::shutdown::wait_after_force_close;

use super::super::ProductionParticipantHandler;
use super::super::tests::{open_disk_store_for_tests, test_participant_config};
use super::super::tests_outbox_barrier_fixture::{OutboxBarrierKind, OutboxBarrierStore};
use super::{
    ParticipantOnlyServices, encode_frame, encode_request, next_push, read_frame, roundtrip,
    tcp_pair,
};

const WEBSOCKET_PATH: &str = "/participant";
const WEBSOCKET_ORIGIN: &str = "https://participant.test";

/// Frame-count bound on the WebSocket response demultiplex loop; mirrors the
/// TCP `MAX_DEMUX_FRAMES`.
const WEBSOCKET_DEMUX_FRAMES: usize = 4096;

#[derive(Debug)]
struct PublicationGate {
    open: AtomicBool,
    blocked_scans: AtomicU64,
}

impl PublicationGate {
    const fn closed() -> Self {
        Self {
            open: AtomicBool::new(false),
            blocked_scans: AtomicU64::new(0),
        }
    }

    const fn open() -> Self {
        Self {
            open: AtomicBool::new(true),
            blocked_scans: AtomicU64::new(0),
        }
    }
}

#[derive(Debug)]
struct ReplayGatedHandler {
    inner: Arc<ProductionParticipantHandler>,
    gate: Arc<PublicationGate>,
}

impl ParticipantSemanticHandler for ReplayGatedHandler {
    fn service_fatal(&self) -> Result<Option<ParticipantServiceFatal>, ParticipantSemanticError> {
        self.inner.service_fatal()
    }

    fn latch_connection_fate_intent_incomplete(
        &self,
        open_sequence: u64,
        conversation_id: ConversationId,
    ) -> Result<ParticipantServiceFatal, ParticipantSemanticError> {
        self.inner
            .latch_connection_fate_intent_incomplete(open_sequence, conversation_id)
    }

    fn handle_connection_fate(
        &self,
        work_item: ConnectionFateWorkItem,
    ) -> Result<(), ParticipantSemanticError> {
        self.inner.handle_connection_fate(work_item)
    }

    fn handle_connection_fate_with_impact(
        &self,
        work_item: ConnectionFateWorkItem,
    ) -> ParticipantConnectionFateOutcome {
        self.inner.handle_connection_fate_with_impact(work_item)
    }

    fn repair_unclean_server_restart(
        &self,
        current_server_incarnation: u64,
    ) -> Result<(), ParticipantSemanticError> {
        self.inner
            .repair_unclean_server_restart(current_server_incarnation)
    }

    fn connection_has_bound_participant(
        &self,
        connection_incarnation: ConnectionIncarnation,
        conversations: &[ConversationId],
    ) -> Result<bool, ParticipantSemanticError> {
        self.inner
            .connection_has_bound_participant(connection_incarnation, conversations)
    }

    fn publication_conversation_limit(&self) -> u64 {
        self.inner.publication_conversation_limit()
    }

    fn ready_connection_incarnations(
        &self,
        conversation_id: ConversationId,
    ) -> Result<Vec<ConnectionIncarnation>, ParticipantSemanticError> {
        self.inner.ready_connection_incarnations(conversation_id)
    }

    fn next_publication(
        &self,
        connection_incarnation: ConnectionIncarnation,
        conversation_id: ConversationId,
        offered: Option<ParticipantOfferedProgress>,
    ) -> Result<Option<ParticipantPublication>, ParticipantSemanticError> {
        if !self.gate.open.load(Ordering::SeqCst) {
            self.gate.blocked_scans.fetch_add(1, Ordering::SeqCst);
            return Ok(None);
        }
        self.inner
            .next_publication(connection_incarnation, conversation_id, offered)
    }

    fn publication_binding_is_current(
        &self,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        binding_epoch: BindingEpoch,
    ) -> Result<bool, ParticipantSemanticError> {
        self.inner
            .publication_binding_is_current(conversation_id, participant_id, binding_epoch)
    }

    fn publication_is_current(
        &self,
        publication: &ParticipantPublication,
        offered: Option<ParticipantOfferedProgress>,
    ) -> Result<bool, ParticipantSemanticError> {
        self.inner.publication_is_current(publication, offered)
    }

    fn record_publication_offer(
        &self,
        publication: &ParticipantPublication,
    ) -> Result<(), ParticipantSemanticError> {
        self.inner.record_publication_offer(publication)
    }

    fn handle_observer_recovery(
        &self,
        context: ParticipantConnectionContext,
        conversations: &mut ParticipantConnectionConversations,
        request: ObserverRecoveryHandshake,
        target: Option<ObserverPublicationTarget>,
    ) -> Result<ServerValue, ParticipantSemanticError> {
        self.inner
            .handle_observer_recovery(context, conversations, request, target)
    }

    fn handle_with_impact(
        &self,
        context: ParticipantConnectionContext,
        conversations: &mut ParticipantConnectionConversations,
        request: ClientRequest,
    ) -> ParticipantSemanticOutcome<ServerValue> {
        self.inner
            .handle_with_impact(context, conversations, request)
    }

    fn handle(
        &self,
        context: ParticipantConnectionContext,
        conversations: &mut ParticipantConnectionConversations,
        request: ClientRequest,
    ) -> Result<ServerValue, ParticipantSemanticError> {
        self.inner.handle(context, conversations, request)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::server::participant::production) struct ParticipantOwnerFacts {
    pub(in crate::server::participant::production) frontier_cursor: u64,
    pub(in crate::server::participant::production) outbox_ack_through: u64,
    pub(in crate::server::participant::production) next_live_obligation: Option<u64>,
    pub(in crate::server::participant::production) live_record_count: usize,
    pub(in crate::server::participant::production) charged_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::server::participant::production) struct OutboxOwnerFacts {
    pub(in crate::server::participant::production) ack_through: u64,
    pub(in crate::server::participant::production) next_live_obligation: Option<u64>,
    pub(in crate::server::participant::production) live_record_count: usize,
    pub(in crate::server::participant::production) source_batch_count: usize,
    pub(in crate::server::participant::production) charged_bytes: u64,
}

pub(in crate::server::participant::production) struct SocketFixture {
    client: TcpStream,
    inbound: Vec<u8>,
    /// Pushes demultiplexed ahead of a response by [`roundtrip`], drained FIFO
    /// by [`Self::read_push`] so per-connection order is preserved.
    pushes: VecDeque<ServerPush>,
    handler: Arc<ProductionParticipantHandler>,
    participant_service: InstalledParticipantService,
    store: Arc<dyn DurableStore>,
    publication_gate: Option<Arc<PublicationGate>>,
    barriers: Option<Arc<OutboxBarrierStore>>,
    supervisor: ConnectionSupervisor,
    connection: ConnectionHandle,
}

pub(in crate::server::participant::production) struct SocketPeer {
    client: TcpStream,
    inbound: Vec<u8>,
    pushes: VecDeque<ServerPush>,
    connection: Option<ConnectionHandle>,
}

pub(in crate::server::participant::production) struct WebSocketPeer {
    socket: WebSocket<TcpStream>,
    pushes: VecDeque<ServerPush>,
    pid: u64,
}

pub(in crate::server::participant::production) struct WebSocketEndpoint {
    listener: WebSocketListener,
    pub(in crate::server::participant::production) peer: WebSocketPeer,
}

pub(in crate::server::participant::production) struct SdkSocketFixture {
    listener: Option<ServerListener>,
    supervisor: ConnectionSupervisor,
    handler: Arc<ProductionParticipantHandler>,
    barriers: Option<Arc<OutboxBarrierStore>>,
}

impl SocketFixture {
    pub(in crate::server::participant::production) fn start(
        data_dir: &Path,
    ) -> Result<Self, Box<dyn Error>> {
        Self::start_inner(data_dir, None, test_participant_config(), false)
    }

    pub(in crate::server::participant::production) fn start_with_barriers(
        data_dir: &Path,
    ) -> Result<Self, Box<dyn Error>> {
        Self::start_inner(data_dir, None, test_participant_config(), true)
    }

    pub(in crate::server::participant::production) fn start_replay_gated(
        data_dir: &Path,
    ) -> Result<Self, Box<dyn Error>> {
        Self::start_inner(
            data_dir,
            Some(Arc::new(PublicationGate::closed())),
            test_participant_config(),
            false,
        )
    }

    pub(in crate::server::participant::production) fn start_replay_gated_with_config(
        data_dir: &Path,
        config: ParticipantConfig,
    ) -> Result<Self, Box<dyn Error>> {
        Self::start_inner(
            data_dir,
            Some(Arc::new(PublicationGate::closed())),
            config,
            false,
        )
    }

    pub(in crate::server::participant::production) fn start_replay_gated_with_barriers(
        data_dir: &Path,
        config: ParticipantConfig,
    ) -> Result<Self, Box<dyn Error>> {
        Self::start_inner(
            data_dir,
            Some(Arc::new(PublicationGate::closed())),
            config,
            true,
        )
    }

    pub(in crate::server::participant::production) fn start_with_replay_gate(
        data_dir: &Path,
    ) -> Result<Self, Box<dyn Error>> {
        Self::start_inner(
            data_dir,
            Some(Arc::new(PublicationGate::open())),
            test_participant_config(),
            false,
        )
    }

    fn start_inner(
        data_dir: &Path,
        publication_gate: Option<Arc<PublicationGate>>,
        config: ParticipantConfig,
        barrier_gated: bool,
    ) -> Result<Self, Box<dyn Error>> {
        let inner = open_disk_store_for_tests(data_dir)?;
        let (store, barriers): (Arc<dyn DurableStore>, Option<Arc<OutboxBarrierStore>>) =
            if barrier_gated {
                let barriers = Arc::new(OutboxBarrierStore::new(inner));
                (barriers.clone(), Some(barriers))
            } else {
                (inner, None)
            };
        let handler = Arc::new(ProductionParticipantHandler::new(
            Arc::clone(&store),
            config,
        )?);
        let semantic_handler: Arc<dyn ParticipantSemanticHandler> =
            publication_gate.as_ref().map_or_else(
                || Arc::clone(&handler) as Arc<dyn ParticipantSemanticHandler>,
                |gate| {
                    Arc::new(ReplayGatedHandler {
                        inner: Arc::clone(&handler),
                        gate: Arc::clone(gate),
                    })
                },
            );
        let fixture_store = Arc::clone(&store);
        let participant_service =
            InstalledParticipantService::new(semantic_handler, store, config.wire_frame_limit)
                .map_err(|error| format!("{error:?}"))?;
        let fixture_service = participant_service.clone();
        let services: Arc<dyn ConnectionServices> = Arc::new(ParticipantOnlyServices {
            participant_service,
        });
        let supervisor = ConnectionSupervisor::with_services(services)?;
        let (client, inbound, connection) = connect_socket(&supervisor)?;
        Ok(Self {
            client,
            inbound,
            pushes: VecDeque::new(),
            handler,
            participant_service: fixture_service,
            store: fixture_store,
            publication_gate,
            barriers,
            supervisor,
            connection,
        })
    }

    pub(in crate::server::participant::production) fn request(
        &mut self,
        request: ClientRequest,
    ) -> Result<ServerValue, Box<dyn Error>> {
        roundtrip(
            &mut self.client,
            &mut self.inbound,
            &mut self.pushes,
            request,
        )
    }

    pub(in crate::server::participant::production) fn spawn_peer(
        &self,
    ) -> Result<SocketPeer, Box<dyn Error>> {
        let (client, inbound, connection) = connect_socket(&self.supervisor)?;
        Ok(SocketPeer {
            client,
            inbound,
            pushes: VecDeque::new(),
            connection: Some(connection),
        })
    }

    pub(in crate::server::participant::production) fn pid(&self) -> u64 {
        self.connection.pid()
    }

    pub(in crate::server::participant::production) fn observe_next_park(
        &self,
        pid: u64,
    ) -> Receiver<u64> {
        self.supervisor.observe_next_park(pid)
    }

    pub(in crate::server::participant::production) fn observe_settled_park(
        &self,
        pid: u64,
    ) -> Receiver<u64> {
        self.supervisor.observe_settled_park(pid)
    }

    pub(in crate::server::participant::production) fn observe_next_slice(
        &self,
        pid: u64,
    ) -> Receiver<u64> {
        self.supervisor.observe_next_slice(pid)
    }

    pub(in crate::server::participant::production) fn slice_count(&self, pid: u64) -> u64 {
        self.supervisor.slice_count(pid)
    }

    pub(in crate::server::participant::production) fn spawn_websocket_peer(
        &self,
    ) -> Result<WebSocketEndpoint, Box<dyn Error>> {
        self.spawn_websocket_peer_with_ping_interval(None)
    }

    pub(in crate::server::participant::production) fn spawn_websocket_peer_with_ping_interval(
        &self,
        ping_interval_ms: Option<u64>,
    ) -> Result<WebSocketEndpoint, Box<dyn Error>> {
        let listener = WebSocketListener::bind(
            &WebSocketConfig {
                listen_address: "127.0.0.1:0".parse()?,
                path: WEBSOCKET_PATH.to_owned(),
                allowed_origins: vec![WEBSOCKET_ORIGIN.to_owned()],
                ping_interval_ms,
            },
            self.supervisor.clone(),
        )?;
        let before = self.supervisor.active_connection_pids();
        let stream = TcpStream::connect(listener.local_addr())?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        stream.set_write_timeout(Some(Duration::from_secs(10)))?;
        let mut request =
            format!("ws://{}{WEBSOCKET_PATH}", listener.local_addr()).into_client_request()?;
        request
            .headers_mut()
            .insert("Origin", WEBSOCKET_ORIGIN.parse()?);
        let (mut socket, _) = tungstenite::client::client(request, stream)?;
        socket.send(Message::Binary(
            encode_frame(&Frame::Connect {
                flags: 0,
                min_version: ProtocolVersion::new(1, 0),
                max_version: ProtocolVersion::new(1, 0),
                auth_token: Vec::new(),
            })?
            .into(),
        ))?;
        let ack_bytes = read_websocket_binary(&mut socket)?;
        let (ack, consumed) = liminal::protocol::decode(&ack_bytes)?;
        if consumed != ack_bytes.len()
            || !matches!(
                ack,
                Frame::ConnectAck { capabilities, .. }
                    if capabilities == PARTICIPANT_CAPABILITY_BIT
            )
        {
            return Err(
                format!("WebSocket participant capability was not advertised: {ack:?}").into(),
            );
        }
        let pid = self
            .supervisor
            .active_connection_pids()
            .into_iter()
            .find(|pid| !before.contains(pid))
            .ok_or("WebSocket connection process was not registered")?;
        Ok(WebSocketEndpoint {
            listener,
            peer: WebSocketPeer {
                socket,
                pushes: VecDeque::new(),
                pid,
            },
        })
    }

    pub(in crate::server::participant::production) fn block_publication_replay(
        &self,
    ) -> Result<(), Box<dyn Error>> {
        let gate = self
            .publication_gate
            .as_ref()
            .ok_or("socket fixture has no publication gate")?;
        gate.open.store(false, Ordering::SeqCst);
        Ok(())
    }

    pub(in crate::server::participant::production) fn open_publication_replay(
        &self,
    ) -> Result<(), Box<dyn Error>> {
        let gate = self
            .publication_gate
            .as_ref()
            .ok_or("socket fixture has no publication gate")?;
        gate.open.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub(in crate::server::participant::production) fn arm_outbox_barriers(
        &self,
        gates: impl IntoIterator<Item = OutboxBarrierKind>,
    ) -> Result<(), Box<dyn Error>> {
        self.barriers
            .as_ref()
            .ok_or("socket fixture has no durability gates")?
            .arm(gates)
    }

    pub(in crate::server::participant::production) fn fail_next_outbox_append(
        &self,
    ) -> Result<(), Box<dyn Error>> {
        self.barriers
            .as_ref()
            .ok_or("socket fixture has no durability gates")?
            .fail_next(OutboxBarrierKind::OutboxAppend)
    }

    pub(in crate::server::participant::production) fn wait_for_outbox_barrier(
        &self,
        expected: OutboxBarrierKind,
    ) -> Result<(), Box<dyn Error>> {
        self.barriers
            .as_ref()
            .ok_or("socket fixture has no durability gates")?
            .wait_for(expected)
    }

    pub(in crate::server::participant::production) fn release_outbox_barrier(
        &self,
        expected: OutboxBarrierKind,
    ) -> Result<(), Box<dyn Error>> {
        self.barriers
            .as_ref()
            .ok_or("socket fixture has no durability gates")?
            .release(expected)
    }

    pub(in crate::server::participant::production) fn read_push(
        &mut self,
    ) -> Result<ServerPush, Box<dyn Error>> {
        next_push(&mut self.client, &mut self.inbound, &mut self.pushes)
    }

    pub(in crate::server::participant::production) fn participant_owner_facts(
        &self,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
    ) -> Result<ParticipantOwnerFacts, Box<dyn Error>> {
        let cell = self.handler.cell(conversation_id)?;
        let owner = cell
            .lock()
            .map_err(|_| "conversation authority lock was poisoned")?;
        let authority = owner
            .as_ref()
            .ok_or("conversation authority was not restored")?;
        let frontier = authority
            .frontier()
            .ok_or("conversation frontier owner was absent")?;
        let frontier_cursor = frontier
            .frontiers()
            .active_identities()
            .participants()
            .iter()
            .find(|participant| participant.participant_index() == participant_id)
            .map(|participant| participant.cursor())
            .ok_or("participant was absent from the live frontier")?;
        let outbox = authority
            .outbox
            .as_ref()
            .ok_or("conversation outbox owner was absent")?;
        let facts = ParticipantOwnerFacts {
            frontier_cursor,
            outbox_ack_through: outbox.ack_through(participant_id),
            next_live_obligation: outbox.next_live(participant_id),
            live_record_count: outbox.live_record_count(),
            charged_bytes: outbox.charged_bytes(),
        };
        drop(owner);
        Ok(facts)
    }

    pub(in crate::server::participant::production) fn outbox_owner_facts(
        &self,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
    ) -> Result<OutboxOwnerFacts, Box<dyn Error>> {
        let cell = self.handler.cell(conversation_id)?;
        let owner = cell
            .lock()
            .map_err(|_| "conversation authority lock was poisoned")?;
        let authority = owner
            .as_ref()
            .ok_or("conversation authority was not restored")?;
        let outbox = authority
            .outbox
            .as_ref()
            .ok_or("conversation outbox owner was absent")?;
        let facts = OutboxOwnerFacts {
            ack_through: outbox.ack_through(participant_id),
            next_live_obligation: outbox.next_live(participant_id),
            live_record_count: outbox.live_record_count(),
            source_batch_count: outbox.source_batch_count(),
            charged_bytes: outbox.charged_bytes(),
        };
        drop(owner);
        Ok(facts)
    }

    pub(in crate::server::participant::production) fn immutable_candidate_counts(
        &self,
        conversation_id: ConversationId,
    ) -> Result<(usize, usize), Box<dyn Error>> {
        let cell = self.handler.cell(conversation_id)?;
        let owner = cell
            .lock()
            .map_err(|_| "conversation authority lock was poisoned")?;
        let authority = owner
            .as_ref()
            .ok_or("conversation authority was not restored")?;
        let frontier = authority
            .frontier()
            .ok_or("conversation frontier owner was absent")?;
        let counts = (
            frontier.frontiers().sequence().immutable_candidates().len(),
            frontier.frontiers().order().immutable_candidates().len(),
        );
        drop(owner);
        Ok(counts)
    }

    pub(in crate::server::participant::production) fn blocked_publication_scans(
        &self,
    ) -> Result<u64, Box<dyn Error>> {
        self.publication_gate
            .as_ref()
            .map(|gate| gate.blocked_scans.load(Ordering::SeqCst))
            .ok_or_else(|| "socket fixture has no publication gate".into())
    }

    pub(in crate::server::participant::production) fn durable_store(
        &self,
    ) -> Arc<dyn DurableStore> {
        Arc::clone(&self.store)
    }

    pub(in crate::server::participant::production) fn obligation_dispatch_work_snapshot(
        &self,
    ) -> super::super::dispatch_work::ObligationDispatchWorkSnapshot {
        self.handler.obligation_dispatch_work_snapshot()
    }

    pub(in crate::server::participant::production) fn publication_ready_fire_count(&self) -> u64 {
        self.participant_service
            .publication_registry()
            .ready_fire_count()
    }

    pub(in crate::server::participant::production) fn request_force_close(&self) {
        self.supervisor.force_close_active_connections();
    }

    pub(in crate::server::participant::production) fn force_close_and_wait(&self) {
        self.request_force_close();
        wait_after_force_close(&self.supervisor);
    }

    pub(in crate::server::participant::production) fn stop(self) {
        let Self {
            client,
            inbound,
            pushes,
            handler,
            participant_service,
            store,
            publication_gate,
            barriers,
            supervisor,
            connection,
        } = self;
        self::wait_after_force_close_request(&supervisor);
        drop(client);
        drop(inbound);
        drop(pushes);
        supervisor.shutdown();
        drop(connection);
        drop(supervisor);
        drop(handler);
        drop(participant_service);
        drop(store);
        drop(publication_gate);
        drop(barriers);
    }
}

fn wait_after_force_close_request(supervisor: &ConnectionSupervisor) {
    supervisor.force_close_active_connections();
    wait_after_force_close(supervisor);
}

impl SocketPeer {
    pub(in crate::server::participant::production) fn request(
        &mut self,
        request: ClientRequest,
    ) -> Result<ServerValue, Box<dyn Error>> {
        roundtrip(
            &mut self.client,
            &mut self.inbound,
            &mut self.pushes,
            request,
        )
    }

    pub(in crate::server::participant::production) fn shutdown_transport(
        &self,
    ) -> Result<(), Box<dyn Error>> {
        self.client.shutdown(Shutdown::Both).map_err(Into::into)
    }

    pub(in crate::server::participant::production) fn read_push(
        &mut self,
    ) -> Result<ServerPush, Box<dyn Error>> {
        next_push(&mut self.client, &mut self.inbound, &mut self.pushes)
    }
}

impl WebSocketPeer {
    pub(in crate::server::participant::production) const fn pid(&self) -> u64 {
        self.pid
    }

    pub(in crate::server::participant::production) fn read_keepalive_ping(
        &mut self,
    ) -> Result<(), Box<dyn Error>> {
        let message = self.socket.read()?;
        let Message::Ping(_) = message else {
            return Err(format!("expected transport keepalive Ping, got {message:?}").into());
        };
        self.socket.flush()?;
        Ok(())
    }

    pub(in crate::server::participant::production) fn request(
        &mut self,
        request: ClientRequest,
    ) -> Result<ServerValue, Box<dyn Error>> {
        self.socket
            .send(Message::Binary(encode_request(request)?.into()))?;
        // Demultiplex like the TCP `roundtrip`: an unsolicited push may arrive
        // ahead of the response even over WebSocket. Stash pushes in order and
        // read until the response, bounded by frame count and the socket read
        // deadline.
        for _ in 0..WEBSOCKET_DEMUX_FRAMES {
            let bytes = read_websocket_binary(&mut self.socket)?;
            let decoded = decode_participant(&bytes, ReceiverDirection::Client)
                .map_err(|error| format!("{error:?}"))?;
            match decoded {
                ParticipantFrame::ServerValue(value) => return Ok(value),
                ParticipantFrame::ServerPush(push) => self.pushes.push_back(push),
                ParticipantFrame::ClientRequest(unexpected) => {
                    return Err(format!(
                        "WebSocket client received a ClientRequest frame: {unexpected:?}"
                    )
                    .into());
                }
            }
        }
        Err(format!(
            "no WebSocket ServerValue response arrived within {WEBSOCKET_DEMUX_FRAMES} frames"
        )
        .into())
    }

    pub(in crate::server::participant::production) fn read_push(
        &mut self,
    ) -> Result<ServerPush, Box<dyn Error>> {
        if let Some(push) = self.pushes.pop_front() {
            return Ok(push);
        }
        let bytes = read_websocket_binary(&mut self.socket)?;
        let decoded = decode_participant(&bytes, ReceiverDirection::Client)
            .map_err(|error| format!("{error:?}"))?;
        let ParticipantFrame::ServerPush(push) = decoded else {
            return Err(format!("expected WebSocket participant push, got {decoded:?}").into());
        };
        Ok(push)
    }
}

impl WebSocketEndpoint {
    pub(in crate::server::participant::production) fn stop(self) -> Result<(), Box<dyn Error>> {
        let Self { listener, peer } = self;
        drop(peer);
        listener.shutdown()?;
        Ok(())
    }
}

impl SdkSocketFixture {
    pub(in crate::server::participant::production) fn start(
        data_dir: &Path,
    ) -> Result<Self, Box<dyn Error>> {
        Self::start_inner(data_dir, false)
    }

    pub(in crate::server::participant::production) fn start_gated(
        data_dir: &Path,
    ) -> Result<Self, Box<dyn Error>> {
        Self::start_inner(data_dir, true)
    }

    fn start_inner(data_dir: &Path, gated: bool) -> Result<Self, Box<dyn Error>> {
        let inner = open_disk_store_for_tests(data_dir)?;
        let (store, barriers): (Arc<dyn DurableStore>, Option<Arc<OutboxBarrierStore>>) = if gated {
            let barriers = Arc::new(OutboxBarrierStore::new(inner));
            (barriers.clone(), Some(barriers))
        } else {
            (inner, None)
        };
        let participant_config = test_participant_config();
        let handler = Arc::new(ProductionParticipantHandler::new(
            Arc::clone(&store),
            participant_config,
        )?);
        let participant_service = InstalledParticipantService::new(
            Arc::clone(&handler) as Arc<dyn ParticipantSemanticHandler>,
            store,
            participant_config.wire_frame_limit,
        )
        .map_err(|error| format!("{error:?}"))?;
        let services: Arc<dyn ConnectionServices> = Arc::new(ParticipantOnlyServices {
            participant_service,
        });
        let supervisor = ConnectionSupervisor::with_services(services)?;
        let config = ServerConfig {
            listen_address: "127.0.0.1:0".parse()?,
            health_listen_address: "127.0.0.1:0".parse()?,
            drain_timeout_ms: 30_000,
            channels: Vec::new(),
            routing_rules: Vec::new(),
            persistence_path: None,
            cluster: None,
            auth: None,
            services: ServicesConfig::default(),
            limits: LimitsConfig::default(),
            websocket: None,
            participant: None,
        };
        let listener = ServerListener::bind(&config, supervisor.clone())?;
        Ok(Self {
            listener: Some(listener),
            supervisor,
            handler,
            barriers,
        })
    }

    pub(in crate::server::participant::production) fn address(
        &self,
    ) -> Result<std::net::SocketAddr, Box<dyn Error>> {
        self.listener
            .as_ref()
            .map(ServerListener::local_addr)
            .ok_or_else(|| "SDK socket listener has stopped".into())
    }

    pub(in crate::server::participant::production) fn active_connection_pids(&self) -> Vec<u64> {
        self.supervisor.active_connection_pids()
    }

    pub(in crate::server::participant::production) fn queue_next_outbound_capacity(
        &self,
        capacity: usize,
    ) {
        self.supervisor.queue_next_outbound_capacity(capacity);
    }

    pub(in crate::server::participant::production) fn install_participant_holdback_pause(
        &self,
        pid: u64,
    ) -> Receiver<()> {
        self.supervisor.install_participant_holdback_pause(pid)
    }

    pub(in crate::server::participant::production) fn resume_process(&self, pid: u64) -> bool {
        self.supervisor.resume_test_process(pid)
    }

    pub(in crate::server::participant::production) fn fail_next_outbox_append(
        &self,
    ) -> Result<(), Box<dyn Error>> {
        self.barriers
            .as_ref()
            .ok_or("SDK socket fixture has no durability gates")?
            .fail_next(OutboxBarrierKind::OutboxAppend)
    }

    pub(in crate::server::participant::production) fn stop(mut self) -> Result<(), Box<dyn Error>> {
        if let Some(listener) = self.listener.take() {
            listener.shutdown()?;
        }
        self.supervisor.shutdown();
        drop(self.supervisor);
        drop(self.handler);
        drop(self.barriers);
        Ok(())
    }
}

fn read_websocket_binary(socket: &mut WebSocket<TcpStream>) -> Result<Vec<u8>, Box<dyn Error>> {
    let message = socket.read()?;
    let Message::Binary(bytes) = message else {
        return Err(format!("expected one binary WebSocket message, got {message:?}").into());
    };
    Ok(bytes.to_vec())
}

fn connect_socket(
    supervisor: &ConnectionSupervisor,
) -> Result<(TcpStream, Vec<u8>, ConnectionHandle), Box<dyn Error>> {
    let (mut client, server) = tcp_pair()?;
    client.set_read_timeout(Some(Duration::from_secs(10)))?;
    client.set_write_timeout(Some(Duration::from_secs(10)))?;
    let connection = supervisor.spawn_connection(server)?;

    client.write_all(&encode_frame(&Frame::Connect {
        flags: 0,
        min_version: ProtocolVersion::new(1, 0),
        max_version: ProtocolVersion::new(1, 0),
        auth_token: Vec::new(),
    })?)?;
    let mut inbound = Vec::new();
    let ack = read_frame(&mut client, &mut inbound)?;
    if !matches!(
        ack,
        Frame::ConnectAck { capabilities, .. }
            if capabilities == PARTICIPANT_CAPABILITY_BIT
    ) {
        return Err(format!("participant capability was not advertised: {ack:?}").into());
    }
    Ok((client, inbound, connection))
}

impl Drop for SocketPeer {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            drop(connection);
        }
    }
}
