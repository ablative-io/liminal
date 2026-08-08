//! SDK-over-loopback acceptance pins: design step 4's client side, driven
//! through the real public surfaces
//! (`docs/design/IN-PROCESS-TRANSPORT.md` §2, §4, §5, §9 ruling 4).
//!
//! Every client here enters through [`RemoteConfig::connect_loopback_with_auth`]
//! and [`RemoteParticipantHandle`] — the same two types an application uses over
//! a socket — against a real [`EmbeddedServer`] whose services are assembled by
//! [`build_connection_services`], the same function the standalone server's own
//! boot calls. The production participant handler, the real on-disk haematite
//! store, the channel cluster and the conversation supervisor are all genuinely
//! there; only the listener is not. No pin constructs a frame by hand, reaches
//! into the supervisor, or applies participant state directly: what these pins
//! can observe is exactly what an embedder can observe, which is the point of a
//! mount whose whole claim is that it reaches the record through the same door.
//!
//! **These pins live in an integration test on purpose.** `liminal-server`
//! dev-depends on `liminal-sdk` with the `embedded` feature, and `liminal-sdk`
//! optionally depends back on `liminal-server` — a cycle Cargo permits because
//! it closes through a dev-dependency. In the crate's own `#[cfg(test)]` lib
//! build that produces TWO `liminal_server` compilations (the lib under test,
//! and the plain lib the SDK links), so `crate::…::EmbeddedServer` and the
//! SDK's `EmbeddedServer` are different types and will not unify. An
//! integration test links the ordinary lib, exactly as the SDK does, so there
//! is one `EmbeddedServer` and the pins compile against the same public surface
//! an embedder compiles against.
//!
//! The parity these pins protect is the RECORD PATH (hardened face-substrate
//! draft r2 §5), not the mount. An in-process caller is trusted code and
//! nothing here asserts otherwise; what is asserted is that admission, the
//! token compare, capacity, the participant gate and teardown do not know which
//! mount knocked.

use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use liminal_protocol::wire::{
    ClientRequest, EnrollBound, EnrollmentRequest, EnrollmentToken, Generation,
    ParticipantDelivery, ParticipantRecord, RecordAdmission, RecordAdmissionAttemptToken,
    ServerPush, ServerValue,
};
use liminal_sdk::{
    ConnectionPoolConfig, ParticipantResumeStore, RemoteConfig, RemoteOperationRecordOutcome,
    RemoteParticipantHandle, RemoteParticipantInbound, RemoteParticipantSendOutcome, SdkError,
};
use liminal_server::config::types::ParticipantConfig;
use liminal_server::config::{LimitsConfig, ServerConfig, ServicesConfig};
use liminal_server::server::connection::{
    ConnectionServices, ConnectionSupervisor, build_connection_services,
};
use liminal_server::server::embedded::EmbeddedServer;
use liminal_server::server::listener::ServerListener;

/// The conversation every participant pin in this file drives.
const LOOPBACK_CONVERSATION: u64 = 0x50_01;
/// The token a gated fixture is configured with.
const CONFIGURED_TOKEN: &[u8] = b"the-configured-token";
/// A token that is not [`CONFIGURED_TOKEN`], and the same length, so the
/// compare is not decided by a length difference alone.
const WRONG_TOKEN: &[u8] = b"the-configured-tokeX";
/// Bound on how long a pin waits for the server to notice a client hangup.
///
/// Teardown is asynchronous by construction on BOTH mounts — a socket close and
/// a dropped duplex end both merely make the connection's next read answer end
/// of file — so a pin that read the registry immediately would be asserting
/// scheduler timing rather than deregistration. The wait is bounded so a
/// genuinely stuck teardown still fails.
const TEARDOWN_DEADLINE: Duration = Duration::from_secs(10);

/// Deployment-shaped participant configuration. Every field is a deployment
/// owner's decision — the type carries no defaults on purpose — so a fixture
/// has to state all of them, exactly as a real `[participant]` section does.
const fn participant_config() -> ParticipantConfig {
    ParticipantConfig {
        wire_frame_limit: 65_536,
        attach_receipt_ttl_ms: 60_000,
        receipt_provenance_ttl_ms: 600_000,
        max_live_attach_receipts_server: 1_024,
        max_live_attach_receipts_per_participant: 8,
        max_receipt_provenance_server: 4_096,
        max_receipt_provenance_per_conversation: 256,
        max_receipt_provenance_per_participant: 64,
        max_retired_identity_slots_server: 1_024,
        identity_slots: 4,
        observer_recovery_max_entries: 64,
        max_semantic_conversations_per_connection: 32,
        max_ordinary_record_entries: 1,
        max_ordinary_record_bytes: 131_072,
        max_generated_marker_entries: 1,
        max_generated_marker_bytes: 4_096,
        mandatory_transaction_bound_entries: 4,
        mandatory_transaction_bound_bytes: 16_384,
        full_recovery_claim_entries: 4,
        full_recovery_claim_bytes: 16_384,
        retained_capacity_entries: 2_048,
        retained_capacity_bytes: 16_777_216,
        max_retained_record_rows: 1_024,
        closure_episode_churn_limit: 1_024,
    }
}

/// The resume store the SDK participant checkpoints into.
#[derive(Debug, Default)]
struct MemoryResumeStore {
    canonical: Vec<u8>,
}

impl ParticipantResumeStore for MemoryResumeStore {
    fn persist(&mut self, canonical_lpcr: &[u8]) -> Result<(), SdkError> {
        self.canonical.clear();
        self.canonical.extend_from_slice(canonical_lpcr);
        Ok(())
    }
}

type SdkParticipant = RemoteParticipantHandle<MemoryResumeStore>;

/// A deployment-shaped config with the participant protocol activated and
/// durable state rooted under `store_dir`.
///
/// `listen_address` is filled in because the type requires one; an
/// [`EmbeddedServer`] never reads it, which is the whole point — nothing on the
/// in-process path reads a socket fact.
fn server_config(store_dir: &Path, limits: LimitsConfig) -> Result<ServerConfig, Box<dyn Error>> {
    Ok(ServerConfig {
        listen_address: "127.0.0.1:0".parse()?,
        health_listen_address: "127.0.0.1:0".parse()?,
        drain_timeout_ms: 30_000,
        channels: Vec::new(),
        routing_rules: Vec::new(),
        persistence_path: Some(store_dir.to_path_buf()),
        cluster: None,
        auth: None,
        services: ServicesConfig::default(),
        limits,
        websocket: None,
        participant: Some(participant_config()),
    })
}

/// Builds the full production service stack for `store_dir` — the same call the
/// standalone server's boot makes.
fn production_services(store_dir: &Path) -> Result<Arc<dyn ConnectionServices>, Box<dyn Error>> {
    // The haematite engine creates its directory exactly one level below a
    // pre-existing parent it can fence, and never `create_dir_all`s — so the
    // fixture creates the parent, as a deployment's operator would.
    std::fs::create_dir_all(store_dir)?;
    let config = server_config(store_dir, LimitsConfig::default())?;
    Ok(build_connection_services(&config)?)
}

/// Starts a listenerless production server.
fn start_embedded(
    store_dir: &Path,
    auth_token: Option<Vec<u8>>,
    limits: LimitsConfig,
) -> Result<Arc<EmbeddedServer>, Box<dyn Error>> {
    let services = production_services(store_dir)?;
    Ok(Arc::new(EmbeddedServer::with_services_auth_and_limits(
        services, auth_token, limits,
    )?))
}

/// The client half of a loopback mount, built exactly as an embedder builds it.
fn loopback_config(server: Arc<EmbeddedServer>, token: &[u8]) -> Result<RemoteConfig, SdkError> {
    RemoteConfig::new(
        "in-process",
        "participant-acceptance",
        LOOPBACK_CONVERSATION.to_string(),
        ConnectionPoolConfig::new(1, 1, 8),
    )?
    .connect_loopback_with_auth(server, token)
}

/// A participant handle riding an in-process connection.
fn connect_participant(server: Arc<EmbeddedServer>) -> Result<SdkParticipant, Box<dyn Error>> {
    let config = loopback_config(server, &[])?;
    Ok(RemoteParticipantHandle::new(
        &config,
        MemoryResumeStore::default(),
    )?)
}

fn send_operation(
    participant: &SdkParticipant,
    request: ClientRequest,
) -> Result<(), Box<dyn Error>> {
    let operation = match participant.record_operation(request)? {
        RemoteOperationRecordOutcome::Recorded(operation)
        | RemoteOperationRecordOutcome::Continuous(operation) => operation,
        RemoteOperationRecordOutcome::Refused { request, reason } => {
            return Err(format!("SDK refused outbound request {request:?}: {reason:?}").into());
        }
    };
    match participant.send_operation(operation)? {
        RemoteParticipantSendOutcome::Sent { .. } => Ok(()),
        RemoteParticipantSendOutcome::TransportLost { error, .. } => {
            Err(format!("SDK transport lost while sending participant operation: {error}").into())
        }
    }
}

fn exchange(
    participant: &SdkParticipant,
    request: ClientRequest,
) -> Result<RemoteParticipantInbound, Box<dyn Error>> {
    send_operation(participant, request)?;
    participant.receive().map_err(Into::into)
}

fn expect_applied(inbound: RemoteParticipantInbound) -> Result<ServerValue, Box<dyn Error>> {
    match inbound {
        RemoteParticipantInbound::Applied { value, .. } => Ok(value),
        other => Err(format!("expected SDK-applied server value, got {other:?}").into()),
    }
}

fn expect_push(participant: &SdkParticipant) -> Result<ServerPush, Box<dyn Error>> {
    match participant.receive()? {
        RemoteParticipantInbound::Push { value, .. } => Ok(value),
        other => Err(format!("expected exact SDK Push inbound, got {other:?}").into()),
    }
}

/// Enrolls one participant and returns its binding.
fn enroll(participant: &SdkParticipant, token: [u8; 16]) -> Result<EnrollBound, Box<dyn Error>> {
    let value = expect_applied(exchange(
        participant,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: LOOPBACK_CONVERSATION,
            enrollment_token: EnrollmentToken::new(token),
        }),
    )?)?;
    let ServerValue::EnrollBound(bound) = value else {
        return Err(format!("enrollment did not bind: {value:?}").into());
    };
    Ok(bound)
}

/// Waits, under a bound, for `probe` to answer true.
fn wait_until(deadline: Duration, mut probe: impl FnMut() -> bool) -> bool {
    let expiry = Instant::now() + deadline;
    loop {
        if probe() {
            return true;
        }
        if Instant::now() >= expiry {
            return false;
        }
        thread::sleep(Duration::from_millis(10));
    }
}

/// Drives one wrong-token connect over a REAL socket against a supervisor
/// configured with the same token, returning the SDK's refusal.
///
/// This exists so the loopback's refusal can be compared against the socket's
/// as measured, rather than against a remembered shape.
fn tcp_wrong_token_refusal(store_dir: &Path) -> Result<SdkError, Box<dyn Error>> {
    let services = production_services(store_dir)?;
    let supervisor =
        ConnectionSupervisor::with_services_and_auth(services, Some(CONFIGURED_TOKEN.to_vec()))?;
    let config = server_config(store_dir, LimitsConfig::default())?;
    let listener = ServerListener::bind(&config, supervisor.clone())?;
    let address = listener.local_addr();
    let refusal = RemoteConfig::new(
        address.to_string(),
        "participant-acceptance",
        LOOPBACK_CONVERSATION.to_string(),
        ConnectionPoolConfig::new(1, 1, 8),
    )?
    .connect_tcp_with_auth(WRONG_TOKEN)
    .err()
    .ok_or("the socket mount admitted a wrong token")?;
    listener.shutdown()?;
    supervisor.shutdown();
    Ok(refusal)
}

#[test]
fn an_sdk_client_handshakes_over_the_loopback_against_a_real_embedded_server()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let server = start_embedded(&home.path().join("open"), None, LimitsConfig::default())?;

    // `connect_loopback_with_auth` returns only after the server's own
    // `connect_response` answered `ConnectAck` over the duplex, so a config in
    // hand IS a completed handshake — the same thing `connect_tcp` returning
    // means.
    let config = loopback_config(Arc::clone(&server), &[])?;

    // A handshake that merely returned would prove very little, so the pin
    // spends the connection: a participant enrollment is a gated participant
    // frame, which only reaches the production handler if the connection was
    // admitted, negotiated a version, and was granted the participant
    // capability bit against a real durable incarnation.
    let participant = RemoteParticipantHandle::new(&config, MemoryResumeStore::default())?;
    let bound = enroll(&participant, [0x50; 16])?;
    assert_eq!(bound.capability_generation(), Generation::ONE);
    Ok(())
}

#[test]
fn a_wrong_token_is_refused_on_its_own_loopback_exactly_as_on_a_socket()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let server = start_embedded(
        &home.path().join("gated"),
        Some(CONFIGURED_TOKEN.to_vec()),
        LimitsConfig::default(),
    )?;

    // Admission is admission: the configured token is what opens the door.
    let accepted = loopback_config(Arc::clone(&server), CONFIGURED_TOKEN)?;
    drop(accepted);

    let loopback_refusal = loopback_config(Arc::clone(&server), WRONG_TOKEN)
        .err()
        .ok_or("the loopback mount admitted a wrong token")?;
    let socket_refusal = tcp_wrong_token_refusal(&home.path().join("gated-socket"))?;

    // Same class AND same words. A refusal that merely shared a variant would
    // leave room for the two mounts to disagree about what happened.
    assert!(
        matches!(loopback_refusal, SdkError::Connection { .. }),
        "loopback refusal was not a connection error: {loopback_refusal:?}"
    );
    assert_eq!(
        format!("{loopback_refusal:?}"),
        format!("{socket_refusal:?}"),
        "the two mounts described the same refusal differently"
    );
    Ok(())
}

#[test]
fn a_participant_enrolls_and_commits_a_record_over_the_loopback() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let server = start_embedded(
        &home.path().join("participant"),
        None,
        LimitsConfig::default(),
    )?;

    let sender = connect_participant(Arc::clone(&server))?;
    let sender_bound = enroll(&sender, [0x51; 16])?;
    let peer = connect_participant(Arc::clone(&server))?;
    let peer_bound = enroll(&peer, [0x52; 16])?;

    // The sender learns about the peer's attach the same way a socket sender
    // does: a server push over its own connection.
    assert_eq!(
        expect_push(&sender)?,
        ServerPush::ParticipantDelivery(ParticipantDelivery {
            conversation_id: LOOPBACK_CONVERSATION,
            delivery_seq: 2,
            record: ParticipantRecord::Attached {
                affected_participant_id: peer_bound.participant_id(),
                binding_epoch: peer_bound.origin_binding_epoch(),
            },
        })
    );

    let record_token = RecordAdmissionAttemptToken::new([0x53; 16]);
    let payload = vec![0x00, 0xFF, 0x50, 0x01, 0xA5];
    let committed = expect_applied(exchange(
        &sender,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: LOOPBACK_CONVERSATION,
            participant_id: sender_bound.participant_id(),
            capability_generation: Generation::ONE,
            record_admission_attempt_token: record_token,
            payload: payload.clone(),
        }),
    )?)?;
    let ServerValue::RecordCommitted(committed) = committed else {
        return Err(format!("the record did not commit over the loopback: {committed:?}").into());
    };
    assert_eq!(
        committed.request().record_admission_attempt_token,
        record_token
    );

    // The commit is a real durable admission, so the peer receives it.
    assert_eq!(
        expect_push(&peer)?,
        ServerPush::ParticipantDelivery(ParticipantDelivery {
            conversation_id: LOOPBACK_CONVERSATION,
            delivery_seq: committed.delivery_seq(),
            record: ParticipantRecord::OrdinaryRecord {
                sender_participant_id: sender_bound.participant_id(),
                payload,
            },
        })
    );
    Ok(())
}

#[test]
fn an_embedded_server_at_admission_capacity_refuses_a_loopback_connect()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let server = start_embedded(
        &home.path().join("capacity"),
        None,
        LimitsConfig {
            max_connections: 1,
            ..LimitsConfig::default()
        },
    )?;

    let first = loopback_config(Arc::clone(&server), &[])?;
    // The in-process connect consumes the same slot pool a socket connect
    // consumes, so the second one has nowhere to go — and says so, cleanly,
    // rather than panicking or handing back a half-built duplex.
    let refusal = loopback_config(Arc::clone(&server), &[])
        .err()
        .ok_or("an embedded server at capacity admitted a second connection")?;
    let SdkError::Connection { description } = &refusal else {
        return Err(format!("capacity refusal was not a connection error: {refusal:?}").into());
    };
    assert!(
        description.contains("max_connections"),
        "the capacity refusal did not name the bound it hit: {description}"
    );
    drop(first);
    Ok(())
}

#[test]
fn dropping_the_loopback_transport_tears_the_server_connection_down() -> Result<(), Box<dyn Error>>
{
    let home = tempfile::tempdir()?;
    let server = start_embedded(
        &home.path().join("teardown"),
        None,
        LimitsConfig {
            max_connections: 1,
            ..LimitsConfig::default()
        },
    )?;

    let config = loopback_config(Arc::clone(&server), &[])?;

    // The positive control for the assertion below: while the connection is
    // held, the one slot is genuinely taken, so a second connect is refused.
    // Without this half, "a connect succeeded after the drop" would be
    // consistent with a cap that never bound at all.
    assert!(
        loopback_config(Arc::clone(&server), &[]).is_err(),
        "the held connection was not occupying its admission slot, so the \
         release below would prove nothing"
    );

    drop(config);

    // The outcome IS the deregistration. At a cap of one, a fresh connect can
    // only succeed if the server noticed the dropped client end, removed its
    // registry record, and released its admission slot — which is the whole of
    // what a socket hangup does, reached by the same path.
    assert!(
        wait_until(TEARDOWN_DEADLINE, || loopback_config(
            Arc::clone(&server),
            &[]
        )
        .is_ok()),
        "the server never released the connection whose client end was dropped"
    );
    Ok(())
}
