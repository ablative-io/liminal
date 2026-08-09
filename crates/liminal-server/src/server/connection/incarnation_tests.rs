use std::io::Read as _;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use liminal::durability::{
    DurabilityError, DurableStore, StoredEntry, bridge::block_on, open_ephemeral,
};
use liminal::protocol::{Frame, decode};
use liminal_protocol::wire::{ClientRequest, ServerValue};
use liminal_protocol::{
    lifecycle::ConnectionIncarnationAllocatorRestore, wire::ConnectionIncarnation,
};

use super::ConnectionSupervisor;
use super::incarnation::{AMBIGUOUS_DURABLE_WRITE_PHASE, ConnectionIncarnationAuthority};
use super::services::{ConnectionServices, LiminalConnectionServices};
use super::worker_front_door::WorkerFrontDoorServices;
use crate::ServerError;
use crate::config::types::{LimitsConfig, ServerConfig, ServicesConfig};
use crate::server::listener::ServerListener;
use crate::server::participant::incarnation_stream::{
    ConnectionFateClass, IncarnationStartup, IncarnationStream, encode_allocate_event_fixture,
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

/// Builds a live authority over `store` with no injected failure.
fn started_authority(
    store: &Arc<dyn DurableStore>,
    maximum_references: usize,
    maximum_conversations: usize,
) -> Result<ConnectionIncarnationAuthority, Box<dyn std::error::Error>> {
    let startup =
        block_on(IncarnationStream::new(Arc::clone(store), maximum_references).startup())??;
    let IncarnationStartup::Started(started) = startup else {
        return Err("fresh stream unexpectedly required recovery or exhaustion".into());
    };
    Ok(ConnectionIncarnationAuthority::from_started_for_test(
        started,
        maximum_conversations,
    ))
}

/// P0 #56 pin 1: a failure that CANNOT have written durable bytes must not
/// disarm admission for the rest of the process.
///
/// `complete_connection_fate` validates the named Open against the unmatched set
/// BEFORE it encodes or appends anything
/// (`participant/incarnation_stream.rs::complete_connection_fate`), so a Complete
/// for an absent Open is a pure pre-write refusal: the durable stream is
/// byte-identical afterwards and nothing about it is ambiguous. Before this fix
/// that refusal still left the shared authority `Failed`, and because the
/// teardown path and the admission path share one authority, one bad Complete
/// refused every subsequent connection for the process lifetime.
#[test]
fn a_pre_durable_write_failure_leaves_admission_working()
-> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    let authority = started_authority(&store, 4, 3)?;
    let before = block_on(store.read_from(IncarnationStream::stream_key(), 0, 64))??;

    // No Open has ever been appended, so this Complete cannot match one.
    let refused = authority.complete_connection_fate(9_999);
    assert!(
        refused.is_err(),
        "a Complete for an absent Open must be refused"
    );

    // The pre-write refusal wrote nothing: the durable stream is unchanged.
    let after = block_on(store.read_from(IncarnationStream::stream_key(), 0, 64))??;
    assert_eq!(
        after.len(),
        before.len(),
        "a pre-write refusal must not append to the durable stream"
    );

    // And the next connection is admitted normally.
    let admitted = authority.allocate(&[])?;
    assert_eq!(admitted, ConnectionIncarnation::new(1, 0));
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

/// A handler that refuses every fate work item, standing in for a participant
/// that cannot absorb its torn predecessor's unmatched Open.
#[derive(Debug, Default)]
struct RefusingFateHandler;

impl ParticipantSemanticHandler for RefusingFateHandler {
    fn handle(
        &self,
        _context: ParticipantConnectionContext,
        _conversations: &mut ParticipantConnectionConversations,
        _request: ClientRequest,
    ) -> Result<ServerValue, ParticipantSemanticError> {
        Err(ParticipantSemanticError::Unavailable)
    }

    fn handle_connection_fate(
        &self,
        _work_item: ConnectionFateWorkItem,
    ) -> Result<(), ParticipantSemanticError> {
        Err(ParticipantSemanticError::Internal {
            message: "injected recovery refusal".to_owned(),
        })
    }

    fn publication_conversation_limit(&self) -> u64 {
        3
    }
}

/// P0 #56, coordinator question: can boot-time recovery of an unclean
/// predecessor arm the admission hold?
///
/// CHARACTERIZATION, not red-first — this passes at f7efcc4 too. It is here to
/// make a STRUCTURAL fact mechanically checked rather than argued from a
/// reading, because the answer decides how much the rest of this lane matters.
///
/// The authority has exactly one production constructor,
/// `ConnectionIncarnationAuthority::startup`, and the only place it builds a
/// `Self` is `finish_startup`, whose state is `Ready` by construction. Every
/// failure before that point — including every step of unclean-predecessor
/// recovery: the handler fold, the Complete append, the post-recovery Startup —
/// is a `return Err(ServerError)` out of `startup`, which `SupervisorInner::new`
/// propagates with `?`. So a boot that cannot recover its predecessor FAILS TO
/// BUILD A SERVER. It cannot produce a held authority, because it produces no
/// authority at all.
///
/// That matters for reading the field evidence: the latched boots were serving
/// refusals with their ports open and their health probe green, and a process
/// that armed the hold during recovery would instead have exited during
/// construction with no listener ever bound. Arming is runtime-only. On a
/// deployment where every restart is unclean, "recovering from a torn
/// predecessor" stays an ordinary handled path — pinned in the neighbouring
/// `startup_completes_historical_opens_before_returning_authority`, which
/// recovers a torn Open and then allocates normally.
#[test]
fn a_failed_recovery_of_a_torn_predecessor_fails_construction_rather_than_holding_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let store = store()?;
    // A torn predecessor: Startup, an Allocate, and an Open with no Complete.
    let payloads = [
        encode_startup_event_fixture()?,
        encode_allocate_event_fixture(4, &[])?,
        encode_open_connection_fate_event_fixture(
            ConnectionIncarnation::new(1, 0),
            ConnectionFateClass::ConnectionLost,
            3,
            &[5, 8],
        )?,
    ];
    for (sequence, payload) in payloads.into_iter().enumerate() {
        let sequence = u64::try_from(sequence)?;
        let assigned = block_on(store.append(IncarnationStream::stream_key(), payload, sequence))??;
        assert_eq!(assigned, sequence);
    }
    block_on(store.flush())??;
    let handler = RefusingFateHandler;

    let outcome = ConnectionIncarnationAuthority::startup(
        Arc::clone(&store),
        4,
        handler.publication_conversation_limit(),
        &handler,
    );

    assert!(
        matches!(
            outcome,
            Err(ServerError::ParticipantIncarnation {
                phase: "connection-fate handler recovery",
                ..
            })
        ),
        "a refused recovery must fail construction outright, never yield an authority"
    );
    // No Complete was appended: the refusal happened before the durable fold.
    let entries = block_on(store.read_from(IncarnationStream::stream_key(), 0, 8))??;
    assert_eq!(entries.len(), 3);
    Ok(())
}

#[derive(Debug)]
struct FailNthFlush {
    inner: Arc<dyn DurableStore>,
    flush_count: AtomicUsize,
    fail_at: usize,
    /// While set, `read_from` fails too — a store that is down for reads as
    /// well as writes, so a resume replay cannot succeed either.
    reads_down: AtomicBool,
}

impl FailNthFlush {
    fn new(inner: Arc<dyn DurableStore>, fail_at: usize) -> Self {
        Self {
            inner,
            flush_count: AtomicUsize::new(0),
            fail_at,
            reads_down: AtomicBool::new(false),
        }
    }

    fn take_reads_down(&self) {
        self.reads_down.store(true, Ordering::SeqCst);
    }

    fn bring_reads_up(&self) {
        self.reads_down.store(false, Ordering::SeqCst);
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
        if self.reads_down.load(Ordering::SeqCst) {
            return Err(DurabilityError::ConfigError(
                "injected incarnation read failure".to_owned(),
            ));
        }
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

/// P0 #56 pin 3, end to end: a connection refused by the real accept loop is
/// TOLD, on the wire, before the socket closes.
///
/// The unit-level flush proofs live in `refusal_tests`. This one exists because
/// they cannot see the thing that actually broke in the field: the accept loop
/// consumed the socket into `spawn_connection`, which dropped it on every
/// failure path, so the refusal never had anything to be written on. A client
/// saw a completed TCP connection and then an immediate FIN with zero bytes —
/// indistinguishable from a crashed server.
///
/// A real listener, a real client socket, and `read_to_end` — so the assertion
/// is over bytes the kernel delivered, not over anything the server merely
/// intended.
#[test]
fn a_refused_connection_is_told_why_before_the_socket_closes()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = store()?;
    let failing = Arc::new(FailNthFlush::new(Arc::clone(&inner), 2));
    let config = config()?;
    let supervisor =
        ConnectionSupervisor::with_services(services(&config, Arc::clone(&failing) as _)?)?;
    // Reads down as well as writes, so the resume replay cannot rescue this
    // connection and the refusal is deterministic rather than a race.
    failing.take_reads_down();
    let listener = ServerListener::bind(&config, supervisor)?;
    let address = listener.local_addr();

    let mut client = TcpStream::connect(address)?;
    let mut received = Vec::new();
    client.read_to_end(&mut received)?;

    assert!(
        !received.is_empty(),
        "a refused client must be told why, not handed a bare FIN"
    );
    let (frame, consumed) = decode(&received)?;
    assert_eq!(consumed, received.len(), "the refusal is exactly one frame");
    let Frame::ConnectError { message, .. } = frame else {
        return Err(format!("expected ConnectError, got {frame:?}").into());
    };
    let message = message.ok_or("ConnectError carried no message")?;
    assert!(
        message.contains("admission refused"),
        "the refusal must name its class, got: {message}"
    );
    listener.shutdown()?;
    Ok(())
}

/// P0 #56 pin 2: an ambiguous durable write HOLDS admission, and the hold is
/// released by re-reading the store rather than by restarting the process.
///
/// A failed fsync after a successful append is the one genuinely ambiguous
/// outcome: the bytes may or may not be durable, so this process no longer knows
/// the stream's head and must not append through that handle again. Holding is
/// correct. Holding FOREVER is not — "process recovery is required" names a
/// trigger nothing inside the process owns, and on the field estate that turned
/// one bad fsync into 82,166 consecutive refusals on a server whose ports stayed
/// open and whose health probe stayed green.
///
/// Both halves are asserted here against a store that is down for reads as well
/// as writes, so the hold is observable before the recovery is:
///   1. the fsync fails and the connection is refused (nothing spawned);
///   2. while the store is still down, admission stays refused AND the resume
///      replay cannot cheat — the refusal names the ambiguity;
///   3. once the store is healthy, admission comes back on its own, within a
///      bounded number of attempts, with no restart and no operator action.
#[test]
fn an_ambiguous_durable_write_holds_admission_and_then_recovers_by_reading()
-> Result<(), Box<dyn std::error::Error>> {
    /// Attempts allowed after the store recovers. Larger than the backoff
    /// window the authority can reach from a single failed resume, so the pin
    /// asserts "recovers within a bound" rather than a schedule it would have
    /// to be edited alongside.
    const RECOVERY_ATTEMPT_BUDGET: usize = 8;

    let inner = store()?;
    let failing = Arc::new(FailNthFlush::new(Arc::clone(&inner), 2));
    let config = config()?;
    // Startup replays the stream, so the store has to be readable to build the
    // supervisor at all. It goes down immediately afterwards.
    let supervisor =
        ConnectionSupervisor::with_services(services(&config, Arc::clone(&failing) as _)?)?;
    failing.take_reads_down();

    // 1. The ambiguous fsync refuses this connection before anything is spawned.
    let (_client, server) = tcp_pair()?;
    assert!(matches!(
        supervisor.spawn_connection(server),
        Err(ServerError::ParticipantIncarnation {
            phase: "connection allocation persistence",
            ..
        })
    ));
    assert_eq!(supervisor.active_connection_count(), 0);

    // 2. The hold is real: with the store still down the resume cannot succeed,
    //    and the refusal says so rather than pretending the stream is fine.
    let (_second_client, second_server) = tcp_pair()?;
    assert!(matches!(
        supervisor.spawn_connection(second_server),
        Err(ServerError::ParticipantIncarnation {
            phase: AMBIGUOUS_DURABLE_WRITE_PHASE,
            ..
        })
    ));
    assert_eq!(supervisor.active_connection_count(), 0);

    // 3. The store comes back. Nothing restarts; nothing is told. The next
    //    connections simply arrive, and one of them is admitted.
    failing.bring_reads_up();
    let mut sockets = Vec::new();
    let mut admitted = false;
    for _ in 0..RECOVERY_ATTEMPT_BUDGET {
        let (client, server) = tcp_pair()?;
        sockets.push(client);
        if supervisor.spawn_connection(server).is_ok() {
            admitted = true;
            break;
        }
    }
    assert!(
        admitted,
        "admission must recover by re-reading the store, without a process restart"
    );
    assert_eq!(supervisor.active_connection_count(), 1);

    // The resumed stream picked up the durable truth rather than this process's
    // stale count: the ambiguous Allocate DID land (append succeeded, only the
    // fsync failed), so the recovered allocation appends after it.
    let entries = block_on(inner.read_from(IncarnationStream::stream_key(), 0, 8))??;
    assert_eq!(
        entries.len(),
        3,
        "startup, the ambiguous allocate that landed, and the resumed allocate"
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
