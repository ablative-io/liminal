use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use liminal::durability::{
    DurabilityError, DurableStore, StoredEntry, bridge::block_on, open_ephemeral,
};
use liminal_protocol::wire::{ClientRequest, ServerValue};
use liminal_protocol::{
    lifecycle::ConnectionIncarnationAllocatorRestore, wire::ConnectionIncarnation,
};

use super::ConnectionSupervisor;
use super::incarnation::ConnectionIncarnationAuthority;
use super::services::{ConnectionServices, LiminalConnectionServices};
use super::worker_front_door::WorkerFrontDoorServices;
use crate::ServerError;
use crate::config::types::{LimitsConfig, ServerConfig, ServicesConfig};
use crate::server::listener::ServerListener;
use crate::server::participant::incarnation_stream::{
    ConnectionFateClass, IncarnationStream, encode_allocate_event_fixture,
    encode_complete_connection_fate_event_fixture, encode_open_connection_fate_event_fixture,
    encode_startup_event_fixture,
};
use crate::server::participant::{
    ConnectionFateWorkItem, InstalledParticipantService, ParticipantConnectionContext,
    ParticipantConnectionConversations, ParticipantSemanticError, ParticipantSemanticHandler,
};

fn store() -> Result<Arc<dyn DurableStore>, Box<dyn std::error::Error>> {
    Ok(Arc::new(open_ephemeral(1)?))
}

fn config() -> Result<ServerConfig, Box<dyn std::error::Error>> {
    Ok(ServerConfig {
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
        participant: None,
        websocket: None,
    })
}

fn services(
    config: &ServerConfig,
    store: Arc<dyn DurableStore>,
) -> Result<Arc<dyn ConnectionServices>, ServerError> {
    let connection_services =
        LiminalConnectionServices::from_config_with_store(config, Arc::clone(&store))?;
    let participant_service =
        InstalledParticipantService::new(Arc::new(UnavailableParticipantHandler), store, u64::MAX)
            .map_err(|error| ServerError::ConfigValidation {
                message: format!("invalid participant test wire-frame limit: {error:?}"),
            })?;
    let connection_services = connection_services.with_participant_service(participant_service);
    Ok(Arc::new(connection_services))
}

#[derive(Debug)]
struct UnavailableParticipantHandler;

impl ParticipantSemanticHandler for UnavailableParticipantHandler {
    fn handle(
        &self,
        _context: ParticipantConnectionContext,
        _conversations: &mut ParticipantConnectionConversations,
        _request: ClientRequest,
    ) -> Result<ServerValue, ParticipantSemanticError> {
        Err(ParticipantSemanticError::Unavailable)
    }
}

fn tcp_pair() -> Result<(TcpStream, TcpStream), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address: SocketAddr = listener.local_addr()?;
    let client = TcpStream::connect(address)?;
    let (server, _) = listener.accept()?;
    Ok((client, server))
}

#[test]
fn supervisor_fsyncs_startup_before_listener_can_bind() -> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    let config = config()?;
    let supervisor = ConnectionSupervisor::with_services(services(&config, Arc::clone(&store))?)?;

    let startup_entries = block_on(store.read_from(IncarnationStream::stream_key(), 0, 8))??;
    assert_eq!(
        startup_entries.len(),
        1,
        "supervisor construction must complete the startup append and flush"
    );

    let listener = ServerListener::bind(&config, supervisor.clone())?;
    assert_eq!(listener.local_addr().ip(), config.listen_address.ip());
    listener.shutdown()?;
    supervisor.shutdown();
    Ok(())
}

#[test]
fn accepted_connections_receive_distinct_durable_incarnations()
-> Result<(), Box<dyn std::error::Error>> {
    let config = config()?;
    let supervisor = ConnectionSupervisor::with_services(services(&config, store()?)?)?;
    let (_client_one, server_one) = tcp_pair()?;
    let (_client_two, server_two) = tcp_pair()?;

    let first = supervisor.spawn_connection(server_one)?;
    let second = supervisor.spawn_connection(server_two)?;
    assert_eq!(
        first.connection_incarnation(),
        Some(ConnectionIncarnation::new(1, 0))
    );
    assert_eq!(
        second.connection_incarnation(),
        Some(ConnectionIncarnation::new(1, 1))
    );
    assert_ne!(
        first.connection_incarnation(),
        second.connection_incarnation()
    );

    supervisor.shutdown();
    Ok(())
}

#[test]
fn ordinary_full_services_do_not_activate_participant_incarnations()
-> Result<(), Box<dyn std::error::Error>> {
    let durable_store = store()?;
    let config = config()?;
    let ordinary: Arc<dyn ConnectionServices> = Arc::new(
        LiminalConnectionServices::from_config_with_store(&config, Arc::clone(&durable_store))?,
    );
    let supervisor = ConnectionSupervisor::with_services(ordinary)?;
    let (_client, server) = tcp_pair()?;

    let handle = supervisor.spawn_connection(server)?;
    assert_eq!(handle.connection_incarnation(), None);
    let startup_entries =
        block_on(durable_store.read_from(IncarnationStream::stream_key(), 0, 8))??;
    assert!(startup_entries.is_empty());

    supervisor.shutdown();
    Ok(())
}

#[test]
fn worker_front_door_does_not_activate_participant_incarnations()
-> Result<(), Box<dyn std::error::Error>> {
    let supervisor = ConnectionSupervisor::with_services(Arc::new(WorkerFrontDoorServices::new()))?;
    let (_client, server) = tcp_pair()?;

    let handle = supervisor.spawn_connection(server)?;
    assert_eq!(handle.connection_incarnation(), None);

    supervisor.shutdown();
    Ok(())
}

#[test]
fn connection_ordinal_exhaustion_is_a_typed_admission_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    let started = block_on(
        IncarnationStream::seeded_for_test(
            store,
            4,
            ConnectionIncarnationAllocatorRestore {
                server_incarnation: 9,
                last_examined_connection_ordinal: Some(u64::MAX),
                connection_ordinal_exhausted: true,
            },
        )?
        .resume_started_for_test(),
    )??;
    let authority = ConnectionIncarnationAuthority::from_started_for_test(started, 4);

    assert!(matches!(
        authority.allocate(&[]),
        Err(ServerError::ConnectionIncarnationExhausted {
            attempted_server_incarnation: 9,
        })
    ));
    Ok(())
}

#[test]
fn production_connection_fate_authority_opens_and_completes_with_signed_bound()
-> Result<(), Box<dyn std::error::Error>> {
    const MAXIMUM_REFERENCES: usize = 4;
    const MAXIMUM_CONVERSATIONS: usize = 3;
    let store = store()?;
    let startup =
        block_on(IncarnationStream::new(Arc::clone(&store), MAXIMUM_REFERENCES).startup())??;
    let crate::server::participant::incarnation_stream::IncarnationStartup::Started(started) =
        startup
    else {
        return Err("fresh stream unexpectedly required recovery".into());
    };
    let authority =
        ConnectionIncarnationAuthority::from_started_for_test(started, MAXIMUM_CONVERSATIONS);
    let connection_incarnation = authority.allocate(&[])?;
    let conversations = vec![13, 21];

    let intent = authority.open_connection_fate(
        connection_incarnation,
        ConnectionFateClass::ConnectionLost,
        &conversations,
    )?;
    authority.complete_connection_fate(intent.open_sequence)?;

    assert_eq!(intent.connection_incarnation, connection_incarnation);
    assert_eq!(intent.conversations, conversations);
    assert_eq!(intent.declared_conversation_bound, MAXIMUM_CONVERSATIONS);
    let entries = block_on(store.read_from(IncarnationStream::stream_key(), 0, 8))??;
    assert_eq!(entries.len(), 4);
    assert_eq!(
        entries[2].payload,
        encode_open_connection_fate_event_fixture(
            connection_incarnation,
            ConnectionFateClass::ConnectionLost,
            MAXIMUM_CONVERSATIONS,
            &intent.conversations,
        )?
    );
    assert_eq!(
        entries[3].payload,
        encode_complete_connection_fate_event_fixture(intent.open_sequence)?
    );
    Ok(())
}

#[derive(Debug, Default)]
struct RecordingFateHandler {
    work: Mutex<Vec<ConnectionFateWorkItem>>,
    restart_repairs: Mutex<Vec<u64>>,
}

impl ParticipantSemanticHandler for RecordingFateHandler {
    fn handle(
        &self,
        context: ParticipantConnectionContext,
        conversations: &mut ParticipantConnectionConversations,
        request: ClientRequest,
    ) -> Result<ServerValue, ParticipantSemanticError> {
        drop((context, conversations, request));
        Err(ParticipantSemanticError::Unavailable)
    }

    fn handle_connection_fate(
        &self,
        work_item: ConnectionFateWorkItem,
    ) -> Result<(), ParticipantSemanticError> {
        self.work
            .lock()
            .map_err(|error| ParticipantSemanticError::Internal {
                message: error.to_string(),
            })?
            .push(work_item);
        Ok(())
    }

    fn repair_unclean_server_restart(
        &self,
        current_server_incarnation: u64,
    ) -> Result<(), ParticipantSemanticError> {
        self.restart_repairs
            .lock()
            .map_err(|error| ParticipantSemanticError::Internal {
                message: error.to_string(),
            })?
            .push(current_server_incarnation);
        Ok(())
    }

    fn publication_conversation_limit(&self) -> u64 {
        3
    }
}

#[test]
fn startup_completes_historical_opens_before_returning_authority()
-> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    let connection_incarnation = ConnectionIncarnation::new(1, 0);
    let conversations = vec![5, 8];
    let payloads = [
        encode_startup_event_fixture()?,
        encode_allocate_event_fixture(4, &[])?,
        encode_open_connection_fate_event_fixture(
            connection_incarnation,
            ConnectionFateClass::ConnectionLost,
            3,
            &conversations,
        )?,
    ];
    for (sequence, payload) in payloads.into_iter().enumerate() {
        let sequence = u64::try_from(sequence)?;
        let assigned = block_on(store.append(IncarnationStream::stream_key(), payload, sequence))??;
        assert_eq!(assigned, sequence);
    }
    block_on(store.flush())??;
    let handler = RecordingFateHandler::default();

    let authority = ConnectionIncarnationAuthority::startup(
        Arc::clone(&store),
        4,
        handler.publication_conversation_limit(),
        &handler,
    )?;

    let observed = handler
        .work
        .lock()
        .map_err(|error| std::io::Error::other(error.to_string()))?
        .clone();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].open_sequence, 2);
    assert_eq!(observed[0].connection_incarnation, connection_incarnation);
    assert_eq!(observed[0].tracked_conversations, conversations);
    let restart_repairs = handler
        .restart_repairs
        .lock()
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    assert_eq!(restart_repairs.as_slice(), &[2]);
    drop(restart_repairs);
    assert_eq!(authority.allocate(&[])?, ConnectionIncarnation::new(2, 0));
    let entries = block_on(store.read_from(IncarnationStream::stream_key(), 0, 8))??;
    assert_eq!(entries.len(), 6);
    assert_eq!(
        entries[3].payload,
        encode_complete_connection_fate_event_fixture(2)?
    );
    assert_eq!(entries[4].payload, encode_startup_event_fixture()?);
    Ok(())
}

#[derive(Debug)]
struct FailNthFlush {
    inner: Arc<dyn DurableStore>,
    flush_count: AtomicUsize,
    fail_at: usize,
}

impl FailNthFlush {
    fn new(inner: Arc<dyn DurableStore>, fail_at: usize) -> Self {
        Self {
            inner,
            flush_count: AtomicUsize::new(0),
            fail_at,
        }
    }
}

#[async_trait::async_trait]
impl DurableStore for FailNthFlush {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        self.inner.append(stream_key, payload, expected_seq).await
    }

    async fn read_from(
        &self,
        stream_key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, DurabilityError> {
        self.inner.read_from(stream_key, offset, limit).await
    }

    async fn cas(&self, key: &str, old_value: u64, new_value: u64) -> Result<(), DurabilityError> {
        self.inner.cas(key, old_value, new_value).await
    }

    async fn read_value(&self, key: &str) -> Result<Option<u64>, DurabilityError> {
        self.inner.read_value(key).await
    }

    async fn scan(&self, prefix: &str) -> Result<Vec<StoredEntry>, DurabilityError> {
        self.inner.scan(prefix).await
    }

    async fn flush(&self) -> Result<(), DurabilityError> {
        let flush = self.flush_count.fetch_add(1, Ordering::SeqCst) + 1;
        if flush == self.fail_at {
            return Err(DurabilityError::ConfigError(format!(
                "injected incarnation flush failure {flush}"
            )));
        }
        self.inner.flush().await
    }
}

#[test]
fn allocation_flush_failure_refuses_connection_before_process_spawn()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = store()?;
    let failing: Arc<dyn DurableStore> = Arc::new(FailNthFlush::new(Arc::clone(&inner), 2));
    let config = config()?;
    let supervisor = ConnectionSupervisor::with_services(services(&config, failing)?)?;
    let (_client, server) = tcp_pair()?;

    assert!(matches!(
        supervisor.spawn_connection(server),
        Err(ServerError::ParticipantIncarnation {
            phase: "connection allocation persistence",
            ..
        })
    ));
    assert_eq!(supervisor.active_connection_count(), 0);

    let (_second_client, second_server) = tcp_pair()?;
    assert!(matches!(
        supervisor.spawn_connection(second_server),
        Err(ServerError::ParticipantIncarnation {
            phase: "connection allocation unavailable",
            ..
        })
    ));
    let entries = block_on(inner.read_from(IncarnationStream::stream_key(), 0, 8))??;
    assert_eq!(
        entries.len(),
        2,
        "an ambiguous append must poison the live allocator instead of retrying"
    );
    supervisor.shutdown();
    Ok(())
}

#[test]
fn startup_flush_failure_prevents_supervisor_construction() -> Result<(), Box<dyn std::error::Error>>
{
    let inner = store()?;
    let failing: Arc<dyn DurableStore> = Arc::new(FailNthFlush::new(inner, 1));
    let config = config()?;

    assert!(matches!(
        ConnectionSupervisor::with_services(services(&config, failing)?),
        Err(ServerError::ParticipantIncarnation {
            phase: "server startup persistence",
            ..
        })
    ));
    Ok(())
}
