//! Per-record round-trip probe across both consumption profiles (board #68).
//!
//! # What is measured
//!
//! One clock, started immediately before the client hands a `RecordAdmission`
//! to the transport and stopped the instant the correlated `RecordCommitted`
//! comes back. That interval spans, in order: the SDK's outbound recording and
//! encode, the write to the transport, the server's read, participant gate,
//! semantic apply, the DURABLE APPEND AND FLUSH, the response encode, the write
//! back, and the SDK's inbound decode and correlation. It is a per-record
//! commit latency, not a ping.
//!
//! # The durability that is in force
//!
//! `RecordCommitted` is not answered before the record's transition-input row is
//! durable. The barrier is `crates/liminal-server/src/server/participant/
//! production/log.rs:228-250` at rev 339e81a — `OperationLog::append` appends
//! the row at the exact optimistic head and then calls `store.flush().await`,
//! under the comment "The flush is the durability barrier the caller's pending
//! shell commit waits behind: nothing is published until these bytes are
//! durable." Both profiles run with `persistence_path` set to a real on-disk
//! store, so both pay that flush. A number taken with an in-memory store would
//! be a different measurement and is not what this reports.
//!
//! # The two profiles
//!
//! Both are driven through the SAME client stack — `RemoteParticipantHandle`
//! from `liminal-sdk` — so the difference between them is the transport and
//! nothing else:
//!
//! - **tcp**: a real `ServerListener` bound on `127.0.0.1:0`, reached over a
//!   real socket by the SDK's TCP transport.
//! - **loopback**: an `EmbeddedServer` in this process, reached over the
//!   in-process duplex by the SDK's loopback transport. The same framed image
//!   crosses it (proven byte-for-byte in `tests/loopback_parity_e2e.rs`); what
//!   it does not cross is the kernel.
//!
//! # The control, and why the raw number needs one
//!
//! A per-record figure on its own cannot say WHERE the time went, and the first
//! thing anybody will ask of a transport comparison is how much of it is the
//! transport. So every profile also measures a second exchange interleaved with
//! the first, one-for-one: a `ParticipantAck` the server answers `AckNoOp`.
//!
//! A lone participant is excluded from its own records, so it carries no
//! delivery obligation, so an acknowledgement over an empty debt is a no-op that
//! WRITES NO DURABLE ROW. That exchange crosses the identical client stack,
//! transport, participant gate and semantic dispatch, and differs in exactly one
//! term: no append, no flush. The difference between the two medians is
//! therefore a measurement of the durability barrier, not a story about it — and
//! [`timed_ack_noop`] refuses to return unless the answer really was `AckNoOp`,
//! so the control cannot silently stop being a control.
//!
//! # Honesty about the machine
//!
//! This is a laptop under shared load, not an isolated bench. The teed log
//! carries the wall-clock start and end and the load average at both ends, so a
//! reader can see for themselves how contested the box was; the runs banked
//! under `gate-logs/p0-69/` were taken with a load average around 20 on 10
//! logical cores, which is heavy. Single client, steady state, no concurrency.
//!
//! Treat the median as the signal. The p99 and max are upper bounds
//! contaminated by scheduler noise on a loaded laptop and are NOT tail-latency
//! SLOs; the honest use of them here is as evidence about the spread, and the
//! control series is what lets a reader separate that noise from the commit
//! path, because contention lands on both series alike.
//!
//! # Running it
//!
//! ```text
//! cargo test --release -p liminal-server --test participant_record_rtt_probe \
//!   -- --ignored --nocapture --test-threads=1
//! ```
//!
//! The full probe is `#[ignore]` because it is a measurement rather than a pin
//! and commits 2,200 durable records. [`the_rtt_probe_harness_runs`] is the same
//! code over a short run and DOES execute in the ordinary battery, so the probe
//! cannot rot unnoticed.

use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use liminal_protocol::wire::{
    ClientRequest, EnrollmentRequest, EnrollmentToken, Generation, ParticipantAck, ParticipantId,
    RecordAdmission, RecordAdmissionAttemptToken, ServerValue,
};
use liminal_sdk::{
    ConnectionPoolConfig, ParticipantResumeStore, RemoteConfig, RemoteOperationRecordOutcome,
    RemoteParticipantHandle, RemoteParticipantInbound, RemoteParticipantSendOutcome, SdkError,
};
use liminal_server::config::types::ParticipantConfig;
use liminal_server::config::{LimitsConfig, ServerConfig, ServicesConfig};
use liminal_server::server::connection::{
    ConnectionServices, ConnectionSupervisor, LiminalConnectionServices,
};
use liminal_server::server::embedded::EmbeddedServer;
use liminal_server::server::listener::ServerListener;

const PROBE_CONVERSATION: u64 = 0x0000_0000_0000_0068;

/// Records committed and discarded before the clock is trusted.
const WARMUP: usize = 100;

/// Records whose round-trip is kept.
const MEASURED: usize = 1_000;

/// The short run the ordinary battery executes, purely to keep the probe alive.
const SMOKE_WARMUP: usize = 5;
const SMOKE_MEASURED: usize = 20;

/// A fixed 64-byte payload. Small enough that the number is dominated by the
/// commit path rather than by copying, large enough not to be degenerate.
const PAYLOAD: [u8; 64] = [0x68; 64];

const MAX_DEMUX_FRAMES: usize = 64;

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

/// The participant limits the probe runs under.
///
/// Two entries are deliberately raised above the values
/// `tests/loopback_parity_e2e.rs` uses, because that scenario commits two
/// records and this one commits eleven hundred: `max_retained_record_rows` and
/// `retained_capacity_entries`. Nothing else moves, and neither of the two sits
/// on the commit path — they bound retention, not the append.
const fn participant_config() -> ParticipantConfig {
    ParticipantConfig {
        wire_frame_limit: 65_536,
        attach_receipt_ttl_ms: 60_000,
        receipt_provenance_ttl_ms: 600_000,
        live_receipt_server_report_threshold: 1_024,
        max_live_attach_receipts_per_participant: 8,
        receipt_provenance_server_report_threshold: 4_096,
        receipt_provenance_per_conversation_report_threshold: 256,
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
        retained_capacity_entries: 65_536,
        retained_capacity_bytes: 67_108_864,
        max_retained_record_rows: 65_536,
        closure_episode_churn_limit: 1_024,
    }
}

fn server_config(store_dir: &Path) -> Result<ServerConfig, Box<dyn Error>> {
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
        limits: LimitsConfig::default(),
        websocket: None,
        participant: Some(participant_config()),
    })
}

const fn pool() -> ConnectionPoolConfig {
    ConnectionPoolConfig::new(1, 1, 8)
}

// ---------------------------------------------------------------------------
// the measured exchange
// ---------------------------------------------------------------------------

fn send(participant: &SdkParticipant, request: ClientRequest) -> Result<(), Box<dyn Error>> {
    let operation = match participant.record_operation(request)? {
        RemoteOperationRecordOutcome::Recorded(operation)
        | RemoteOperationRecordOutcome::Continuous(operation) => operation,
        RemoteOperationRecordOutcome::Refused { request, reason } => {
            return Err(format!("the SDK refused {request:?}: {reason:?}").into());
        }
    };
    match participant.send_operation(operation)? {
        RemoteParticipantSendOutcome::Sent { .. } => Ok(()),
        RemoteParticipantSendOutcome::TransportLost { error, .. } => {
            Err(format!("transport lost while sending: {error}").into())
        }
    }
}

fn await_value(participant: &SdkParticipant) -> Result<ServerValue, Box<dyn Error>> {
    for _ in 0..MAX_DEMUX_FRAMES {
        match participant.receive()? {
            RemoteParticipantInbound::Applied { value, .. } => return Ok(value),
            RemoteParticipantInbound::Push { .. } => {}
            refused @ RemoteParticipantInbound::Refused { .. } => {
                return Err(format!("the server refused: {refused:?}").into());
            }
        }
    }
    Err("no server value arrived".into())
}

fn roundtrip(
    participant: &SdkParticipant,
    request: ClientRequest,
) -> Result<ServerValue, Box<dyn Error>> {
    send(participant, request)?;
    await_value(participant)
}

/// Commits one record and returns the interval from handing it to the transport
/// to receiving its `RecordCommitted`.
///
/// The token is unique per record on purpose: record-admission dedup keys on
/// (token, payload fingerprint, verified participant), so reusing one token
/// would make every call after the first a DEDUP HIT answered from the commit
/// cache — a number that looks wonderful and measures nothing.
/// (`crates/liminal-protocol/src/wire/request.rs:91-132` @ 339e81a.)
fn timed_record(
    participant: &SdkParticipant,
    participant_id: ParticipantId,
    generation: Generation,
    ordinal: u64,
) -> Result<Duration, Box<dyn Error>> {
    let mut token = [0_u8; 16];
    token[0] = b'R';
    if let Some(slot) = token.get_mut(8..16) {
        slot.copy_from_slice(&ordinal.to_be_bytes());
    }
    let request = ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: PROBE_CONVERSATION,
        participant_id,
        capability_generation: generation,
        record_admission_attempt_token: RecordAdmissionAttemptToken::new(token),
        payload: PAYLOAD.to_vec(),
    });

    let started = Instant::now();
    send(participant, request)?;
    let value = await_value(participant)?;
    let elapsed = started.elapsed();

    if !matches!(value, ServerValue::RecordCommitted(_)) {
        return Err(format!("record {ordinal} did not commit: {value:?}").into());
    }
    Ok(elapsed)
}

/// Times one acknowledgement that the server answers `AckNoOp`.
///
/// # Why this is here
///
/// It is the CONTROL for the record measurement, and it needs no production
/// change to obtain. A lone participant is excluded from its own records, so it
/// holds no delivery obligation; an acknowledgement over an empty debt is
/// answered `AckNoOp` and WRITES NO DURABLE ROW (the same fact
/// `tests/loopback_parity_e2e.rs` states in `run_scenario`'s doc comment, which
/// is why that scenario needs a peer to make its ack real).
///
/// So this exchange traverses the identical client stack, the identical
/// transport, the identical participant gate and the identical semantic
/// dispatch as a record admission — and differs in exactly one term: it does
/// not append and does not flush. Subtracting it from the record figure is
/// therefore a measurement of the durability barrier rather than a story about
/// it.
fn timed_ack_noop(
    participant: &SdkParticipant,
    participant_id: ParticipantId,
    generation: Generation,
) -> Result<Duration, Box<dyn Error>> {
    let request = ClientRequest::ParticipantAck(ParticipantAck {
        conversation_id: PROBE_CONVERSATION,
        participant_id,
        capability_generation: generation,
        through_seq: 0,
    });
    let started = Instant::now();
    send(participant, request)?;
    let value = await_value(participant)?;
    let elapsed = started.elapsed();
    if !matches!(value, ServerValue::AckNoOp(_)) {
        return Err(format!(
            "the control ack was not a no-op, so it is not a no-durable-write control: {value:?}"
        )
        .into());
    }
    Ok(elapsed)
}

/// Enrolls and returns the minted identity plus the generation records are
/// admitted under.
fn enroll(participant: &SdkParticipant) -> Result<(ParticipantId, Generation), Box<dyn Error>> {
    let value = roundtrip(
        participant,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: PROBE_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x68; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(bound) = value else {
        return Err(format!("enrollment did not bind: {value:?}").into());
    };
    Ok((
        bound.participant_id(),
        bound.origin_binding_epoch().capability_generation,
    ))
}

/// One profile's two measured series: the durable record commit, and the
/// no-durable-write control taken on the same connection in the same steady
/// state.
struct Series {
    records: Vec<Duration>,
    control: Vec<Duration>,
}

fn drive(
    participant: &SdkParticipant,
    warmup: usize,
    measured: usize,
) -> Result<Series, Box<dyn Error>> {
    let (participant_id, generation) = enroll(participant)?;
    let mut ordinal = 0_u64;
    for _ in 0..warmup {
        ordinal += 1;
        timed_record(participant, participant_id, generation, ordinal)?;
        timed_ack_noop(participant, participant_id, generation)?;
    }
    let mut records = Vec::with_capacity(measured);
    let mut control = Vec::with_capacity(measured);
    for _ in 0..measured {
        ordinal += 1;
        // Interleaved rather than run in two blocks, so any drift in the box's
        // load over the run lands on both series equally instead of on one.
        records.push(timed_record(
            participant,
            participant_id,
            generation,
            ordinal,
        )?);
        control.push(timed_ack_noop(participant, participant_id, generation)?);
    }
    Ok(Series { records, control })
}

// ---------------------------------------------------------------------------
// profiles
// ---------------------------------------------------------------------------

fn probe_tcp(store_dir: &Path, warmup: usize, measured: usize) -> Result<Series, Box<dyn Error>> {
    std::fs::create_dir_all(store_dir)?;
    let config = server_config(store_dir)?;
    let services = Arc::new(LiminalConnectionServices::from_config(&config)?);
    let supervisor = ConnectionSupervisor::with_services(services as Arc<dyn ConnectionServices>)?;
    let listener = ServerListener::bind(&config, supervisor.clone())?;
    let address = listener.local_addr();
    let remote = RemoteConfig::new(
        address.to_string(),
        "rtt-probe-tcp",
        PROBE_CONVERSATION.to_string(),
        pool(),
    )?
    .connect_tcp()?;
    let participant = RemoteParticipantHandle::new(&remote, MemoryResumeStore::default())?;
    let samples = drive(&participant, warmup, measured);
    drop(participant);
    drop(remote);
    listener.shutdown()?;
    supervisor.shutdown();
    samples
}

fn probe_loopback(
    store_dir: &Path,
    warmup: usize,
    measured: usize,
) -> Result<Series, Box<dyn Error>> {
    std::fs::create_dir_all(store_dir)?;
    let config = server_config(store_dir)?;
    let services = Arc::new(LiminalConnectionServices::from_config(&config)?);
    let server = Arc::new(EmbeddedServer::with_services(
        services as Arc<dyn ConnectionServices>,
    )?);
    let remote = RemoteConfig::new(
        "in-process",
        "rtt-probe-loopback",
        PROBE_CONVERSATION.to_string(),
        pool(),
    )?
    .connect_loopback(Arc::clone(&server))?;
    let participant = RemoteParticipantHandle::new(&remote, MemoryResumeStore::default())?;
    let samples = drive(&participant, warmup, measured);
    drop(participant);
    drop(remote);
    samples
}

// ---------------------------------------------------------------------------
// reporting
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Summary {
    count: usize,
    min: Duration,
    median: Duration,
    mean: Duration,
    p99: Duration,
    max: Duration,
}

/// Nearest-rank percentile on the sorted sample: the smallest observation at or
/// above rank `ceil(numerator / denominator * n)`.
///
/// Integer arithmetic throughout, deliberately. A float percentile invites a
/// rounding argument about which observation a figure came from; this way every
/// figure reported IS an observation that happened, and which one is decidable
/// by hand. No interpolation.
fn percentile(sorted: &[Duration], numerator: usize, denominator: usize) -> Duration {
    if sorted.is_empty() || denominator == 0 {
        return Duration::ZERO;
    }
    // Ceiling division without leaving the integers.
    let rank = numerator
        .saturating_mul(sorted.len())
        .saturating_add(denominator - 1)
        / denominator;
    let index = rank.clamp(1, sorted.len()) - 1;
    sorted.get(index).copied().unwrap_or_default()
}

fn summarise(samples: &[Duration]) -> Summary {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let total: Duration = sorted.iter().sum();
    let divisor = u32::try_from(sorted.len()).unwrap_or(u32::MAX).max(1);
    Summary {
        count: sorted.len(),
        min: sorted.first().copied().unwrap_or_default(),
        median: percentile(&sorted, 1, 2),
        mean: total / divisor,
        p99: percentile(&sorted, 99, 100),
        max: sorted.last().copied().unwrap_or_default(),
    }
}

fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1e6
}

fn report(profile: &str, summary: &Summary) {
    println!(
        "RTT {profile:<8} n={:<5} min={:>9.1}us  median={:>9.1}us  mean={:>9.1}us  \
         p99={:>9.1}us  max={:>9.1}us",
        summary.count,
        micros(summary.min),
        micros(summary.median),
        micros(summary.mean),
        micros(summary.p99),
        micros(summary.max),
    );
}

fn print_setup(warmup: usize, measured: usize) {
    println!("--- per-record RTT probe (board #68) ---");
    let epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_secs());
    println!("started        : unix {epoch}");
    println!("platform       : {}", std::env::consts::OS);
    println!("arch           : {}", std::env::consts::ARCH);
    // Read from the compiler, never asserted by hand: a hardcoded "debug" line
    // in a release log is a false claim sitting inside the evidence.
    println!(
        "build profile  : {}",
        if cfg!(debug_assertions) {
            "DEBUG (unoptimized, debug_assertions on)"
        } else {
            "RELEASE (optimized, debug_assertions off)"
        }
    );
    println!("warmup/measured: {warmup} / {measured} per profile");
    println!("payload        : {} bytes", PAYLOAD.len());
    println!("clients        : 1, steady state, no concurrency");
    println!("durability     : on-disk store; ack follows append+flush");
    println!(
        "flush site     : crates/liminal-server/src/server/participant/production/\
         log.rs:228-250 @ 339e81a"
    );
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

/// The full probe. `#[ignore]` — a measurement, not a pin.
#[test]
#[ignore = "measurement: commits 2,200 durable records; run explicitly with --ignored"]
fn per_record_rtt_on_both_consumption_profiles() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    print_setup(WARMUP, MEASURED);

    let tcp = probe_tcp(&home.path().join("tcp"), WARMUP, MEASURED)?;
    let loopback = probe_loopback(&home.path().join("loopback"), WARMUP, MEASURED)?;

    let tcp_record = summarise(&tcp.records);
    let tcp_control = summarise(&tcp.control);
    let loopback_record = summarise(&loopback.records);
    let loopback_control = summarise(&loopback.control);

    println!();
    println!("PROFILE            = the measured per-record commit round trip");
    println!("PROFILE (control)  = same path, AckNoOp, no durable append/flush");
    println!();
    report("tcp", &tcp_record);
    report("tcp/ctl", &tcp_control);
    report("loopback", &loopback_record);
    report("loopback/ctl", &loopback_control);

    println!();
    println!(
        "transport delta (median, record) : tcp - loopback = {:+.1}us",
        micros(tcp_record.median) - micros(loopback_record.median)
    );
    println!(
        "transport delta (median, control): tcp - loopback = {:+.1}us",
        micros(tcp_control.median) - micros(loopback_control.median)
    );
    println!(
        "durability cost (median)         : tcp {:+.1}us   loopback {:+.1}us",
        micros(tcp_record.median) - micros(tcp_control.median),
        micros(loopback_record.median) - micros(loopback_control.median)
    );
    println!(
        "durability share of the median   : tcp {:.1}%   loopback {:.1}%",
        100.0 * (1.0 - micros(tcp_control.median) / micros(tcp_record.median)),
        100.0 * (1.0 - micros(loopback_control.median) / micros(loopback_record.median)),
    );
    println!();
    println!("temp store dir : {} (auto-removed)", home.path().display());

    assert_eq!(tcp_record.count, MEASURED);
    assert_eq!(loopback_record.count, MEASURED);
    assert_eq!(tcp_control.count, MEASURED);
    assert_eq!(loopback_control.count, MEASURED);
    Ok(())
}

/// The same code over a short run, so the probe compiles and works in the
/// ordinary battery instead of rotting behind `#[ignore]`.
#[test]
fn the_rtt_probe_harness_runs() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let tcp = probe_tcp(&home.path().join("tcp"), SMOKE_WARMUP, SMOKE_MEASURED)?;
    let loopback = probe_loopback(&home.path().join("loopback"), SMOKE_WARMUP, SMOKE_MEASURED)?;
    for series in [&tcp, &loopback] {
        assert_eq!(series.records.len(), SMOKE_MEASURED);
        assert_eq!(series.control.len(), SMOKE_MEASURED);
        assert!(
            series
                .records
                .iter()
                .chain(series.control.iter())
                .all(|sample| *sample > Duration::ZERO),
            "a zero-duration round trip means the clock, not the server, was measured"
        );
    }
    Ok(())
}

/// The percentile is nearest-rank, and a reader is entitled to see that proven
/// rather than trust the comment.
#[test]
fn the_percentile_is_nearest_rank() {
    let samples: Vec<Duration> = (1..=100).map(Duration::from_micros).collect();
    assert_eq!(percentile(&samples, 1, 2), Duration::from_micros(50));
    assert_eq!(percentile(&samples, 99, 100), Duration::from_micros(99));
    assert_eq!(percentile(&samples, 1, 1), Duration::from_micros(100));
    assert_eq!(percentile(&[], 1, 2), Duration::ZERO);
    // A sample smaller than the percentile's denominator must still name a real
    // observation rather than index off the end.
    let three: Vec<Duration> = (1..=3).map(Duration::from_micros).collect();
    assert_eq!(percentile(&three, 99, 100), Duration::from_micros(3));
    assert_eq!(percentile(&three, 1, 2), Duration::from_micros(2));
}
