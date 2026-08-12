//! Canonical golden trace of the PARTICIPANT WIRE, captured at the socket seam.
//!
//! # Why this exists
//!
//! There is no independent second implementation of the participant wire. The
//! TypeScript SDK speaks the legacy channel protocol and delegates encoding to
//! the WASM codec, so it re-uses these very bytes rather than reproducing them.
//! A foreign implementer therefore has the spec and nothing else — and a spec
//! that has never been read back by a stranger is a claim, not a contract.
//!
//! This harness produces the missing half: one canonical session, captured as
//! raw bytes at the socket seam, with direction and frame boundaries recorded,
//! and with every run-variable byte range named. The generator is committed
//! alongside the capture on purpose — a frozen capture with no generator is a
//! mystery, not evidence.
//!
//! # Why a REAL socket
//!
//! Wire bytes exist only on a socket transport. The in-process loopback mount
//! carries the same framed image through the same preflight and the same
//! `apply_frame` seam (proven byte-for-byte in `tests/loopback_parity_e2e.rs`),
//! but it never serialises to a file descriptor, so there is no seam at which
//! to hold a byte and say "this is what crosses the network". This harness
//! binds a real `ServerListener` on `127.0.0.1:0` and drives it with a raw
//! `TcpStream` that the test itself owns. Every byte written and every byte
//! read is recorded verbatim by the owning end — nothing is re-encoded, and no
//! decoded value is re-serialised to stand in for what crossed.
//!
//! # The session
//!
//! Two connections, because a DELIVERY needs somewhere to go. A sender is not
//! delivered its own ordinary record — measured here, not assumed — so a
//! single-connection trace would carry no `ParticipantDelivery` at all.
//!
//! | step | connection | frame                                       |
//! |------|------------|---------------------------------------------|
//! | 1    | admitter   | `Connect` / `ConnectAck` (legacy handshake) |
//! | 2    | observer   | `Connect` / `ConnectAck`                    |
//! | 3    | admitter   | `EnrollmentRequest` -> `EnrollBound`        |
//! | 4    | observer   | `EnrollmentRequest` -> `EnrollBound`        |
//! | 5    | admitter   | `CredentialAttachRequest` -> `AttachBound`  |
//! | 6    | admitter   | `RecordAdmission` -> `RecordCommitted`      |
//! | 7    | observer   | drains `ParticipantDelivery` pushes         |
//! | 8    | observer   | `ParticipantAck` -> `AckCommitted`          |
//! | 9    | admitter   | `DetachRequest` -> `DetachCommitted`        |
//!
//! Identity is MINTED, never declared: the `EnrollmentRequest` body carries
//! exactly `{conversation_id, enrollment_token}` and the server answers with the
//! `participant_id` and the `attach_secret` it minted. Every later request
//! quotes that minted identity back.
//!
//! ## The conversation's record stream, as this session produces it
//!
//! | seq | record                                    | delivered to |
//! |-----|-------------------------------------------|--------------|
//! | 1   | `Attached` pid 0 gen 1 (admitter enrolls) | nobody yet   |
//! | 2   | `Attached` pid 1 gen 1 (observer enrolls) | admitter     |
//! | 3   | `Detached` pid 0 gen 1, cause `Superseded`| observer     |
//! | 4   | `Attached` pid 0 gen 2 (credential attach)| observer     |
//! | 5   | `OrdinaryRecord` from pid 0               | observer     |
//!
//! Two facts a foreign implementer will not guess from the request list and
//! which this capture makes visible. First, a credential attach presented on a
//! LIVE binding does not fail — it supersedes, and the supersession is itself a
//! delivered `Detached` record carrying `cause: Superseded` (seq 3) before the
//! new `Attached` at the rotated generation (seq 4). Second, a record is never
//! delivered to the participant that admitted it: seq 5 reaches the observer
//! and never the admitter.
//!
//! # What this harness ASSERTS on every run
//!
//! 1. The exact ordered census of the request/response spine per connection per
//!    direction, by the discriminant read out of the CAPTURED BYTES rather than
//!    out of the decoded value — a census taken through the decoder would agree
//!    with the decoder by construction.
//! 2. The generic header invariants every participant frame must satisfy.
//! 3. That the committed capture in `docs/wire/golden-trace/` still describes
//!    this build — by masking the fresh frames and the committed frames at the
//!    SAME recorded run-variable ranges and requiring the two masked images to
//!    be byte-identical. That comparison is what turns the "structural vs
//!    run-variable" split in the walkthrough into a measured claim rather than
//!    an annotation: if any byte outside a declared variable range moved, this
//!    test goes red.
//! 4. That every push observed is one of the committed push IMAGES — the weaker
//!    standard a push's schedule permits, and the reason for it is in
//!    [`ADMITTER_C2S`]'s doc comment.
//!
//! # Regenerating
//!
//! ```text
//! LIMINAL_GOLDEN_TRACE_OUT=docs/wire/golden-trace \
//!   cargo test -p liminal-server --test golden_trace_participant_wire
//! ```
//!
//! With the variable unset the harness still runs the whole session and still
//! checks the committed capture; it simply writes nothing.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt::Write as _;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use liminal::protocol::{Frame, ProtocolError, ProtocolVersion};
use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, CredentialAttachRequest, DetachAttemptToken, DetachRequest,
    EnrollmentRequest, EnrollmentToken, GENERIC_HEADER_LEN, PARTICIPANT_FRAME_TYPE,
    PARTICIPANT_PREFIX_LEN, ParticipantAck, ParticipantFrame, ParticipantId, ReceiverDirection,
    RecordAdmission, RecordAdmissionAttemptToken, ServerPush, ServerValue,
};
use liminal_server::config::types::ParticipantConfig;
use liminal_server::config::{LimitsConfig, ServerConfig, ServicesConfig};
use liminal_server::server::connection::{
    ConnectionServices, ConnectionSupervisor, LiminalConnectionServices,
};
use liminal_server::server::listener::ServerListener;
use liminal_server::server::participant::PARTICIPANT_CAPABILITY_BIT;

/// The one conversation the canonical session runs in. `0x69` is the lane
/// number (board #69) so a stranger reading the hex can tell the id apart from
/// a length or a sequence at a glance.
const TRACE_CONVERSATION: u64 = 0x0000_0000_0000_0069;

/// Every client-minted token in the session is a 16-byte constant, distinct per
/// token so a reader can point at any one of them in the hex and name it.
const ADMITTER_ENROLLMENT_TOKEN: [u8; 16] = [0x69; 16];
const OBSERVER_ENROLLMENT_TOKEN: [u8; 16] = [0x6E; 16];
const ADMITTER_ATTACH_TOKEN: [u8; 16] = [0x6A; 16];
const ADMITTER_RECORD_TOKEN: [u8; 16] = [0x6B; 16];
const ADMITTER_DETACH_TOKEN: [u8; 16] = [0x6C; 16];

/// The admitted record's payload. ASCII on purpose: it is the one place in the
/// trace where a reader can confirm by eye that the length prefix and the body
/// agree.
const RECORD_PAYLOAD: &[u8] = b"golden-trace-p0-69";

/// Bound on the observer's push drain. A drain that reaches it has failed.
const MAX_DRAIN_FRAMES: usize = 64;

/// Socket deadline for every read in the session.
const IO_TIMEOUT: Duration = Duration::from_secs(10);

/// Committed capture directory, relative to the repository root.
const CAPTURE_DIR: &str = "docs/wire/golden-trace";

// ---------------------------------------------------------------------------
// capture model
// ---------------------------------------------------------------------------

/// Direction of one captured byte range at the socket seam.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Direction {
    ClientToServer,
    ServerToClient,
}

impl Direction {
    const fn marker(self) -> &'static str {
        match self {
            Self::ClientToServer => "C->S",
            Self::ServerToClient => "S->C",
        }
    }

    const fn suffix(self) -> &'static str {
        match self {
            Self::ClientToServer => "c2s",
            Self::ServerToClient => "s2c",
        }
    }
}

/// One byte range inside a captured frame whose value is decided per run.
#[derive(Clone, Debug, PartialEq, Eq)]
struct VariableRange {
    /// Offset from the start of the FRAME (not of the stream).
    offset: usize,
    len: usize,
    field: String,
}

/// One complete frame as it crossed the socket.
#[derive(Clone, Debug)]
struct CapturedFrame {
    connection: String,
    direction: Direction,
    /// Ordinal within this connection+direction stream.
    ordinal: usize,
    /// Offset of the frame's first byte within its connection+direction stream.
    offset: usize,
    len: usize,
    /// Generic outer frame type byte (`0x1A` for every participant frame).
    outer_type: u8,
    /// Participant discriminant read out of the captured bytes, or `None` for
    /// a legacy generic frame (the `Connect`/`ConnectAck` handshake).
    wire_discriminant: Option<u16>,
    /// Human name of the decoded value.
    label: String,
    /// Debug rendering of the decoded value, for the walkthrough.
    detail: String,
    variable: Vec<VariableRange>,
}

/// One connection's two byte streams plus its frame index.
#[derive(Debug)]
struct Capture {
    name: String,
    c2s: Vec<u8>,
    s2c: Vec<u8>,
    frames: Vec<CapturedFrame>,
}

impl Capture {
    fn stream(&self, direction: Direction) -> &[u8] {
        match direction {
            Direction::ClientToServer => &self.c2s,
            Direction::ServerToClient => &self.s2c,
        }
    }
}

// ---------------------------------------------------------------------------
// the recording client
// ---------------------------------------------------------------------------

/// A raw participant client that owns its socket and records every byte.
///
/// Recording happens at the `write_all`/`read` calls themselves, so the capture
/// is what crossed the file descriptor. Frame boundaries are taken from the
/// generic decoder's own `consumed` count, never guessed.
struct Wire {
    socket: TcpStream,
    capture: Capture,
    /// Bytes of `s2c` already attributed to a decoded frame.
    framed: usize,
    /// Pushes demultiplexed ahead of a response, in arrival order.
    pushes: VecDeque<ServerPush>,
    /// Per-direction frame counters.
    out_ordinal: usize,
    in_ordinal: usize,
}

impl Wire {
    fn connect(name: &str, address: std::net::SocketAddr) -> Result<Self, Box<dyn Error>> {
        let socket = TcpStream::connect(address)?;
        socket.set_nodelay(true)?;
        socket.set_read_timeout(Some(IO_TIMEOUT))?;
        socket.set_write_timeout(Some(IO_TIMEOUT))?;
        Ok(Self {
            socket,
            capture: Capture {
                name: name.to_owned(),
                c2s: Vec::new(),
                s2c: Vec::new(),
                frames: Vec::new(),
            },
            framed: 0,
            pushes: VecDeque::new(),
            out_ordinal: 0,
            in_ordinal: 0,
        })
    }

    /// Writes one complete frame and records it. One frame per `write_all`, so
    /// the outbound frame boundary is the call boundary.
    fn send_frame(
        &mut self,
        bytes: &[u8],
        label: String,
        detail: String,
    ) -> Result<(), Box<dyn Error>> {
        let offset = self.capture.c2s.len();
        self.socket.write_all(bytes)?;
        self.socket.flush()?;
        self.capture.c2s.extend_from_slice(bytes);
        let frame = CapturedFrame {
            connection: self.capture.name.clone(),
            direction: Direction::ClientToServer,
            ordinal: self.out_ordinal,
            offset,
            len: bytes.len(),
            outer_type: bytes.first().copied().unwrap_or(0),
            wire_discriminant: wire_discriminant(bytes),
            label,
            detail,
            variable: Vec::new(),
        };
        self.out_ordinal += 1;
        self.capture.frames.push(frame);
        Ok(())
    }

    /// Reads until one complete frame is available, records it, and returns its
    /// exact bytes.
    fn read_frame(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        let bytes = loop {
            let pending = self.capture.s2c.get(self.framed..).unwrap_or(&[]);
            match liminal::protocol::decode(pending) {
                Ok((_, consumed)) => {
                    let start = self.framed;
                    let end = start.saturating_add(consumed);
                    self.framed = end;
                    break self.capture.s2c.get(start..end).unwrap_or(&[]).to_vec();
                }
                Err(
                    ProtocolError::IncompleteHeader { .. } | ProtocolError::TruncatedPayload { .. },
                ) => {
                    let mut chunk = [0_u8; 4096];
                    let read = self.socket.read(&mut chunk)?;
                    if read == 0 {
                        return Err(format!(
                            "{}: socket closed before a complete frame arrived",
                            self.capture.name
                        )
                        .into());
                    }
                    self.capture
                        .s2c
                        .extend_from_slice(chunk.get(..read).unwrap_or(&[]));
                }
                Err(error) => return Err(Box::new(error)),
            }
        };
        Ok(bytes)
    }

    fn record_inbound(&mut self, bytes: &[u8], label: String, detail: String) {
        let offset = self.framed.saturating_sub(bytes.len());
        let frame = CapturedFrame {
            connection: self.capture.name.clone(),
            direction: Direction::ServerToClient,
            ordinal: self.in_ordinal,
            offset,
            len: bytes.len(),
            outer_type: bytes.first().copied().unwrap_or(0),
            wire_discriminant: wire_discriminant(bytes),
            label,
            detail,
            variable: Vec::new(),
        };
        self.in_ordinal += 1;
        self.capture.frames.push(frame);
    }

    /// Performs the legacy `Connect`/`ConnectAck` handshake that every
    /// participant connection sits on top of, asserting the server advertises
    /// the participant capability bit.
    fn handshake(&mut self) -> Result<(), Box<dyn Error>> {
        let frame = Frame::Connect {
            flags: 0,
            min_version: ProtocolVersion::new(1, 0),
            max_version: ProtocolVersion::new(1, 0),
            auth_token: Vec::new(),
        };
        let bytes = encode_generic(&frame)?;
        self.send_frame(&bytes, "Connect".to_owned(), format!("{frame:?}"))?;
        let ack_bytes = self.read_frame()?;
        let (ack, consumed) = liminal::protocol::decode(&ack_bytes)?;
        if consumed != ack_bytes.len() {
            return Err("ConnectAck did not consume its whole frame".into());
        }
        self.record_inbound(&ack_bytes, "ConnectAck".to_owned(), format!("{ack:?}"));
        match ack {
            Frame::ConnectAck { capabilities, .. }
                if capabilities == PARTICIPANT_CAPABILITY_BIT => {}
            other => {
                return Err(format!("participant capability was not advertised: {other:?}").into());
            }
        }
        Ok(())
    }

    /// Sends one participant request and reads until its `ServerValue` arrives,
    /// stashing any push that interleaves ahead of it.
    fn roundtrip(&mut self, request: &ClientRequest) -> Result<ServerValue, Box<dyn Error>> {
        let label = format!("ClientRequest::{:?}", request.discriminant());
        let detail = format!("{request:?}");
        let bytes = encode_request(request)?;
        self.send_frame(&bytes, label, detail)?;
        for _ in 0..MAX_DRAIN_FRAMES {
            match self.read_participant_frame()? {
                ParticipantFrame::ServerValue(value) => return Ok(value),
                ParticipantFrame::ServerPush(push) => self.pushes.push_back(push),
                ParticipantFrame::ClientRequest(unexpected) => {
                    return Err(
                        format!("client received a ClientRequest frame: {unexpected:?}").into(),
                    );
                }
            }
        }
        Err(format!("no ServerValue arrived within {MAX_DRAIN_FRAMES} frames").into())
    }

    /// Reads one participant frame off the wire, records it, and returns the
    /// decoded value.
    fn read_participant_frame(&mut self) -> Result<ParticipantFrame, Box<dyn Error>> {
        let bytes = self.read_frame()?;
        if bytes.first().copied() != Some(PARTICIPANT_FRAME_TYPE) {
            return Err(format!(
                "expected a participant frame, got outer type {:?}",
                bytes.first()
            )
            .into());
        }
        let frame = liminal_protocol::wire::decode(&bytes, ReceiverDirection::Client)
            .map_err(|error| format!("{error:?}"))?;
        let label = match &frame {
            ParticipantFrame::ServerValue(value) => {
                format!("ServerValue::{:?}", value.discriminant())
            }
            ParticipantFrame::ServerPush(push) => format!("ServerPush::{:?}", push.discriminant()),
            ParticipantFrame::ClientRequest(request) => {
                format!("ClientRequest::{:?}", request.discriminant())
            }
        };
        self.record_inbound(&bytes, label, format!("{frame:?}"));
        Ok(frame)
    }

    /// Drains inbound frames until `wanted` deliveries have been observed.
    fn drain_pushes(&mut self, wanted: usize) -> Result<Vec<ServerPush>, Box<dyn Error>> {
        let mut seen: Vec<ServerPush> = self.pushes.drain(..).collect();
        for _ in 0..MAX_DRAIN_FRAMES {
            if seen.len() >= wanted {
                return Ok(seen);
            }
            match self.read_participant_frame()? {
                ParticipantFrame::ServerPush(push) => seen.push(push),
                other => return Err(format!("expected a push, got {other:?}").into()),
            }
        }
        Err(format!("only {} of {wanted} pushes arrived", seen.len()).into())
    }
}

// ---------------------------------------------------------------------------
// codec helpers
// ---------------------------------------------------------------------------

fn encode_generic(frame: &Frame) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut bytes = vec![0_u8; liminal::protocol::encoded_len(frame)?];
    let written = liminal::protocol::encode(frame, &mut bytes)?;
    bytes.truncate(written);
    Ok(bytes)
}

fn encode_request(request: &ClientRequest) -> Result<Vec<u8>, Box<dyn Error>> {
    let frame = ParticipantFrame::ClientRequest(request.clone());
    let len = liminal_protocol::wire::encoded_len(&frame).map_err(|error| format!("{error:?}"))?;
    let mut bytes = vec![0_u8; len];
    let written =
        liminal_protocol::wire::encode(&frame, &mut bytes).map_err(|error| format!("{error:?}"))?;
    bytes.truncate(written);
    Ok(bytes)
}

/// Reads the participant discriminant out of the captured bytes themselves.
///
/// Deliberately NOT taken from the decoded value: the census below is a check
/// on the wire image, and reading it back through the decoder would make the
/// check agree with the decoder by construction.
fn wire_discriminant(bytes: &[u8]) -> Option<u16> {
    if bytes.first().copied() != Some(PARTICIPANT_FRAME_TYPE) {
        return None;
    }
    let start = GENERIC_HEADER_LEN + PARTICIPANT_PREFIX_LEN - 2;
    let pair = bytes.get(start..start + 2)?;
    let pair: [u8; 2] = pair.try_into().ok()?;
    Some(u16::from_be_bytes(pair))
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

// ---------------------------------------------------------------------------
// server fixture
// ---------------------------------------------------------------------------

/// The participant limits this trace runs under. Spelled out in full rather
/// than defaulted so the capture's meaning does not move when a default does.
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
        retained_capacity_entries: 2_048,
        retained_capacity_bytes: 16_777_216,
        max_retained_record_rows: 1_024,
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

// ---------------------------------------------------------------------------
// the session
// ---------------------------------------------------------------------------

/// Everything one run of the canonical session produced.
struct Session {
    admitter: Capture,
    observer: Capture,
    /// Every server-minted attach secret, in mint order.
    secrets: Vec<[u8; 32]>,
    /// Every server-stamped wall-clock deadline, in the order the responses
    /// carried them.
    deadlines: Vec<u128>,
    admitter_participant: ParticipantId,
    observer_participant: ParticipantId,
    record_delivery_seq: u64,
}

fn run_session(store_dir: &Path) -> Result<Session, Box<dyn Error>> {
    std::fs::create_dir_all(store_dir)?;
    let config = server_config(store_dir)?;
    let services = Arc::new(LiminalConnectionServices::from_config(&config)?);
    let supervisor = ConnectionSupervisor::with_services(services as Arc<dyn ConnectionServices>)?;
    let listener = ServerListener::bind(&config, supervisor.clone())?;
    let address = listener.local_addr();

    let session = drive(address);

    listener.shutdown()?;
    supervisor.shutdown();
    session
}

fn drive(address: std::net::SocketAddr) -> Result<Session, Box<dyn Error>> {
    let mut secrets: Vec<[u8; 32]> = Vec::new();
    let mut deadlines: Vec<u128> = Vec::new();

    let mut admitter = Wire::connect("admitter", address)?;
    admitter.handshake()?;
    let mut observer = Wire::connect("observer", address)?;
    observer.handshake()?;

    // 3. The admitter enrolls. The request body carries exactly the
    //    conversation and the enrollment token; identity comes BACK.
    let value = admitter.roundtrip(&ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: TRACE_CONVERSATION,
        enrollment_token: EnrollmentToken::new(ADMITTER_ENROLLMENT_TOKEN),
    }))?;
    let ServerValue::EnrollBound(admitter_bound) = value else {
        return Err(format!("the admitter enrollment did not bind: {value:?}").into());
    };
    let admitter_participant = admitter_bound.participant_id();
    let enrollment_secret = admitter_bound.attach_secret();
    secrets.push(enrollment_secret.into_bytes());
    deadlines.push(admitter_bound.receipt_expires_at());
    deadlines.push(admitter_bound.provenance_expires_at());

    // 4. The observer enrolls, BEFORE the record exists, so it carries a
    //    genuine delivery obligation for it.
    let value = observer.roundtrip(&ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: TRACE_CONVERSATION,
        enrollment_token: EnrollmentToken::new(OBSERVER_ENROLLMENT_TOKEN),
    }))?;
    let ServerValue::EnrollBound(observer_bound) = value else {
        return Err(format!("the observer enrollment did not bind: {value:?}").into());
    };
    let observer_participant = observer_bound.participant_id();
    secrets.push(observer_bound.attach_secret().into_bytes());
    deadlines.push(observer_bound.receipt_expires_at());
    deadlines.push(observer_bound.provenance_expires_at());

    // 5. The admitter presents its minted credential. A successful attach
    //    ROTATES the capability generation and mints a fresh secret.
    let value = admitter.roundtrip(&ClientRequest::CredentialAttach(CredentialAttachRequest {
        conversation_id: TRACE_CONVERSATION,
        participant_id: admitter_participant,
        capability_generation: admitter_bound.origin_binding_epoch().capability_generation,
        attach_secret: enrollment_secret,
        attach_attempt_token: AttachAttemptToken::new(ADMITTER_ATTACH_TOKEN),
        accept_marker_delivery_seq: None,
    }))?;
    let ServerValue::AttachBound(attach_bound) = value else {
        return Err(format!("the credential attach did not bind: {value:?}").into());
    };
    secrets.push(attach_bound.attach_secret().into_bytes());
    deadlines.push(attach_bound.receipt_expires_at());
    deadlines.push(attach_bound.provenance_expires_at());
    let rotated = attach_bound.origin_binding_epoch().capability_generation;

    // 6. One ordinary record.
    let value = admitter.roundtrip(&ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: TRACE_CONVERSATION,
        participant_id: admitter_participant,
        capability_generation: rotated,
        record_admission_attempt_token: RecordAdmissionAttemptToken::new(ADMITTER_RECORD_TOKEN),
        payload: RECORD_PAYLOAD.to_vec(),
    }))?;
    let ServerValue::RecordCommitted(committed) = value else {
        return Err(format!("the record did not commit: {value:?}").into());
    };
    let record_delivery_seq = committed.delivery_seq();

    // 7. The observer drains its deliveries, up to and including the record.
    let mut highest = 0_u64;
    for _ in 0..MAX_DRAIN_FRAMES {
        let pushes = observer.drain_pushes(1)?;
        for push in pushes {
            if let ServerPush::ParticipantDelivery(delivery) = push {
                highest = highest.max(delivery.delivery_seq);
            }
        }
        if highest >= record_delivery_seq {
            break;
        }
    }
    if highest < record_delivery_seq {
        return Err(format!(
            "the observer never saw delivery {record_delivery_seq} (highest {highest})"
        )
        .into());
    }

    // 8. The observer acknowledges continuously through the record.
    let value = observer.roundtrip(&ClientRequest::ParticipantAck(ParticipantAck {
        conversation_id: TRACE_CONVERSATION,
        participant_id: observer_participant,
        capability_generation: observer_bound.origin_binding_epoch().capability_generation,
        through_seq: record_delivery_seq,
    }))?;
    if !matches!(value, ServerValue::AckCommitted(_)) {
        return Err(format!("the acknowledgement did not commit: {value:?}").into());
    }

    // 9. The admitter detaches.
    let value = admitter.roundtrip(&ClientRequest::Detach(DetachRequest {
        conversation_id: TRACE_CONVERSATION,
        participant_id: admitter_participant,
        capability_generation: rotated,
        detach_attempt_token: DetachAttemptToken::new(ADMITTER_DETACH_TOKEN),
    }))?;
    if !matches!(value, ServerValue::DetachCommitted(_)) {
        return Err(format!("the detach did not commit: {value:?}").into());
    }

    Ok(Session {
        admitter: admitter.capture,
        observer: observer.capture,
        secrets,
        deadlines,
        admitter_participant,
        observer_participant,
        record_delivery_seq,
    })
}

// ---------------------------------------------------------------------------
// run-variable range discovery
// ---------------------------------------------------------------------------

/// Locates every run-variable byte range inside each captured frame.
///
/// Ranges are found by SEARCHING the frame for the exact value the server
/// minted or stamped this run, exactly as `tests/loopback_parity_e2e.rs`
/// locates its exempt fields. Two properties make that sound here:
///
/// - An attach secret is 32 bytes of `/dev/urandom`; a coincidental second
///   occurrence inside a 154-byte frame is not a real possibility, and a
///   collision would show up as an extra range in the committed index.
/// - A deadline is a big-endian `u128` whose top ten bytes are zero for any
///   epoch-millisecond value this century. Searching for the whole sixteen
///   bytes therefore anchors on the six low bytes that actually move; a bare
///   six-byte search would match far too much.
///
/// Nothing is masked that is not on this list, which is the point: the masked
/// comparison below is what proves the list COMPLETE.
fn locate_variables(session: &mut Session) {
    let mut needles: Vec<(Vec<u8>, String)> = Vec::new();
    for (index, secret) in session.secrets.iter().enumerate() {
        needles.push((secret.to_vec(), format!("attach_secret[{index}]")));
    }
    for (index, deadline) in session.deadlines.iter().enumerate() {
        needles.push((
            deadline.to_be_bytes().to_vec(),
            format!("wall_clock_deadline[{index}]"),
        ));
    }

    for capture in [&mut session.admitter, &mut session.observer] {
        let streams = (capture.c2s.clone(), capture.s2c.clone());
        for frame in &mut capture.frames {
            let stream = match frame.direction {
                Direction::ClientToServer => &streams.0,
                Direction::ServerToClient => &streams.1,
            };
            let bytes = stream
                .get(frame.offset..frame.offset.saturating_add(frame.len))
                .unwrap_or(&[]);
            let mut found = Vec::new();
            for (needle, field) in &needles {
                let mut from = 0_usize;
                while let Some(at) = find(bytes, needle, from) {
                    found.push(VariableRange {
                        offset: at,
                        len: needle.len(),
                        field: field.clone(),
                    });
                    from = at + 1;
                }
            }
            found.sort_by_key(|range| (range.offset, range.len));
            frame.variable = found;
        }
    }
}

fn find(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    (from..=haystack.len() - needle.len()).find(|&start| {
        haystack
            .get(start..start + needle.len())
            .is_some_and(|window| window == needle)
    })
}

// ---------------------------------------------------------------------------
// artifact emission
// ---------------------------------------------------------------------------

fn frames_index(session: &Session) -> String {
    let mut out = String::new();
    for capture in [&session.admitter, &session.observer] {
        for frame in &capture.frames {
            let bytes = capture
                .stream(frame.direction)
                .get(frame.offset..frame.offset.saturating_add(frame.len))
                .unwrap_or(&[]);
            let variable: Vec<serde_json::Value> = frame
                .variable
                .iter()
                .map(|range| {
                    serde_json::json!({
                        "offset": range.offset,
                        "len": range.len,
                        "field": range.field,
                    })
                })
                .collect();
            let row = serde_json::json!({
                "connection": frame.connection,
                "direction": frame.direction.marker(),
                "ordinal": frame.ordinal,
                "offset": frame.offset,
                "len": frame.len,
                "outer_frame_type": format!("0x{:02X}", frame.outer_type),
                "participant_discriminant": frame
                    .wire_discriminant
                    .map(|value| format!("0x{value:04X}")),
                "label": frame.label,
                "hex": hex(bytes),
                "run_variable_ranges": variable,
            });
            let _ = writeln!(out, "{row}");
        }
    }
    out
}

fn annotated_hex(session: &Session) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "# Canonical participant-wire golden trace -- annotated hexdump"
    );
    let _ = writeln!(
        out,
        "# Direction markers: C->S is client-to-server, S->C is server-to-client."
    );
    let _ = writeln!(
        out,
        "# `offset` is the byte offset of the frame within its connection+direction stream."
    );
    let _ = writeln!(
        out,
        "# `~` marks a run-variable byte; `.` marks a structural byte a foreign"
    );
    let _ = writeln!(out, "# implementation must reproduce verbatim.");
    for capture in [&session.admitter, &session.observer] {
        let _ = writeln!(out, "\n===== connection: {} =====", capture.name);
        for frame in &capture.frames {
            let bytes = capture
                .stream(frame.direction)
                .get(frame.offset..frame.offset.saturating_add(frame.len))
                .unwrap_or(&[]);
            let discriminant = frame
                .wire_discriminant
                .map_or_else(|| "--".to_owned(), |value| format!("0x{value:04X}"));
            let _ = writeln!(
                out,
                "\n[{} #{:02} {} offset={} len={} outer=0x{:02X} discriminant={}]",
                frame.direction.marker(),
                frame.ordinal,
                frame.label,
                frame.offset,
                frame.len,
                frame.outer_type,
                discriminant
            );
            let _ = writeln!(out, "  decoded: {}", frame.detail);
            for range in &frame.variable {
                let _ = writeln!(
                    out,
                    "  run-variable: [{}..{}) {}",
                    range.offset,
                    range.offset + range.len,
                    range.field
                );
            }
            let mut variable_map = vec![false; bytes.len()];
            for range in &frame.variable {
                for index in range.offset..range.offset.saturating_add(range.len) {
                    if let Some(slot) = variable_map.get_mut(index) {
                        *slot = true;
                    }
                }
            }
            for (row, chunk) in bytes.chunks(16).enumerate() {
                let base = row * 16;
                let mut hex_part = String::new();
                let mut mark_part = String::new();
                for (index, byte) in chunk.iter().enumerate() {
                    let _ = write!(hex_part, "{byte:02x} ");
                    mark_part.push_str(
                        if variable_map.get(base + index).copied().unwrap_or(false) {
                            "~~ "
                        } else {
                            ".. "
                        },
                    );
                }
                let _ = writeln!(out, "  {base:04x}  {hex_part:<48}");
                let _ = writeln!(out, "        {mark_part:<48}");
            }
        }
    }
    out
}

fn write_artifacts(session: &Session, out_dir: &Path) -> Result<(), Box<dyn Error>> {
    std::fs::create_dir_all(out_dir)?;
    for capture in [&session.admitter, &session.observer] {
        for direction in [Direction::ClientToServer, Direction::ServerToClient] {
            let path = out_dir.join(format!("{}.{}.bin", capture.name, direction.suffix()));
            std::fs::write(path, capture.stream(direction))?;
        }
    }
    std::fs::write(out_dir.join("frames.jsonl"), frames_index(session))?;
    std::fs::write(out_dir.join("session.hex"), annotated_hex(session))?;
    Ok(())
}

/// Reads one non-negative JSON integer as an index. A missing or malformed
/// entry becomes 0, which cannot pass silently: the index it lands in is
/// compared field by field against the fresh run's.
fn as_index(value: &serde_json::Value) -> usize {
    value
        .as_u64()
        .and_then(|raw| usize::try_from(raw).ok())
        .unwrap_or(0)
}

/// Rebuilds the frame index from a committed `frames.jsonl` so a fresh run can
/// be masked at the SAME recorded ranges as the committed capture.
fn read_committed_index(path: &Path) -> Result<Vec<CapturedFrame>, Box<dyn Error>> {
    let text = std::fs::read_to_string(path)?;
    let mut frames = Vec::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let row: serde_json::Value = serde_json::from_str(line)?;
        let direction = match row["direction"].as_str() {
            Some("C->S") => Direction::ClientToServer,
            Some("S->C") => Direction::ServerToClient,
            other => return Err(format!("unknown direction marker {other:?}").into()),
        };
        let variable = row["run_variable_ranges"]
            .as_array()
            .map(|ranges| {
                ranges
                    .iter()
                    .map(|range| VariableRange {
                        offset: as_index(&range["offset"]),
                        len: as_index(&range["len"]),
                        field: range["field"].as_str().unwrap_or_default().to_owned(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        frames.push(CapturedFrame {
            connection: row["connection"].as_str().unwrap_or_default().to_owned(),
            direction,
            ordinal: as_index(&row["ordinal"]),
            offset: as_index(&row["offset"]),
            len: as_index(&row["len"]),
            outer_type: 0,
            wire_discriminant: row["participant_discriminant"]
                .as_str()
                .and_then(|text| u16::from_str_radix(text.trim_start_matches("0x"), 16).ok()),
            label: row["label"].as_str().unwrap_or_default().to_owned(),
            detail: String::new(),
            variable,
        });
    }
    Ok(frames)
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from("."))
}

// ---------------------------------------------------------------------------
// the census the harness asserts on every run
// ---------------------------------------------------------------------------

/// The exact ordered frame census the canonical session must produce, by
/// on-the-wire discriminant. `None` is the legacy generic handshake frame.
///
/// Written as literals rather than derived from the run, so a change in what
/// the wire carries fails here and is read as a CHANGE, not absorbed.
///
/// # Why the server-to-client censuses exclude pushes
///
/// A `ServerPush` is not a reply. Its position in the inbound stream, and how
/// many times an unacked obligation is re-offered, are decided by the server's
/// publication pump — not by the request sequence. This harness MEASURED that
/// rather than assuming it: five consecutive runs of this identical scenario at
/// rev 339e81a produced the admitter's first delivery BEFORE `AttachBound` in
/// two runs and AFTER it in three. So the ordered census below covers the
/// request/response spine, which is deterministic, and pushes are held to a
/// separate and weaker standard ([`assert_push_shapes`]).
const ADMITTER_C2S: &[(Option<u16>, &str)] = &[
    (None, "Connect"),
    (Some(0x0001), "ClientRequest::EnrollmentRequest"),
    (Some(0x0002), "ClientRequest::CredentialAttachRequest"),
    (Some(0x0007), "ClientRequest::RecordAdmission"),
    (Some(0x0003), "ClientRequest::DetachRequest"),
];

const ADMITTER_S2C_RESPONSES: &[(Option<u16>, &str)] = &[
    (None, "ConnectAck"),
    (Some(0x010A), "ServerValue::EnrollBound"),
    (Some(0x0111), "ServerValue::AttachBound"),
    (Some(0x011F), "ServerValue::RecordCommitted"),
    (Some(0x0117), "ServerValue::DetachCommitted"),
];

const OBSERVER_C2S: &[(Option<u16>, &str)] = &[
    (None, "Connect"),
    (Some(0x0001), "ClientRequest::EnrollmentRequest"),
    (Some(0x0004), "ClientRequest::ParticipantAck"),
];

const OBSERVER_S2C_RESPONSES: &[(Option<u16>, &str)] = &[
    (None, "ConnectAck"),
    (Some(0x010A), "ServerValue::EnrollBound"),
    (Some(0x0119), "ServerValue::AckCommitted"),
];

const fn is_push(frame: &CapturedFrame) -> bool {
    matches!(frame.wire_discriminant, Some(0x0200 | 0x0201))
}

fn census(capture: &Capture, direction: Direction) -> Vec<(Option<u16>, String)> {
    capture
        .frames
        .iter()
        .filter(|frame| frame.direction == direction)
        .map(|frame| (frame.wire_discriminant, frame.label.clone()))
        .collect()
}

/// The request/response spine of one direction, with pushes removed.
fn response_census(capture: &Capture, direction: Direction) -> Vec<(Option<u16>, String)> {
    capture
        .frames
        .iter()
        .filter(|frame| frame.direction == direction && !is_push(frame))
        .map(|frame| (frame.wire_discriminant, frame.label.clone()))
        .collect()
}

/// Masked bytes of the selected frames, concatenated in arrival order.
fn masked_selection(
    capture: &Capture,
    direction: Direction,
    select: impl Fn(&CapturedFrame) -> bool,
) -> Vec<Vec<u8>> {
    capture
        .frames
        .iter()
        .filter(|frame| frame.direction == direction && select(frame))
        .map(|frame| {
            let bytes = capture
                .stream(direction)
                .get(frame.offset..frame.offset.saturating_add(frame.len))
                .unwrap_or(&[]);
            let mut masked = bytes.to_vec();
            for range in &frame.variable {
                if let Some(slice) = masked.get_mut(range.offset..range.offset + range.len) {
                    slice.fill(0xEE);
                }
            }
            masked
        })
        .collect()
}

fn assert_census(actual: &[(Option<u16>, String)], expected: &[(Option<u16>, &str)], what: &str) {
    let rendered: Vec<(Option<u16>, &str)> = actual
        .iter()
        .map(|(discriminant, label)| (*discriminant, label.as_str()))
        .collect();
    assert_eq!(rendered, expected, "{what} frame census changed");
}

/// Every participant frame's generic header, checked against the codec's own
/// stated invariants (`crates/liminal-protocol/src/wire/codec.rs`, `encode`).
fn assert_header_invariants(capture: &Capture) {
    for frame in &capture.frames {
        if frame.outer_type != PARTICIPANT_FRAME_TYPE {
            continue;
        }
        let bytes = capture
            .stream(frame.direction)
            .get(frame.offset..frame.offset.saturating_add(frame.len))
            .unwrap_or(&[]);
        assert_eq!(bytes.len(), frame.len, "frame length index disagrees");
        assert_eq!(bytes.get(1).copied(), Some(0), "flags must be zero");
        assert_eq!(
            bytes.get(2..6),
            Some([0, 0, 0, 0].as_slice()),
            "stream_id must be zero on participant traffic"
        );
        let declared = bytes
            .get(6..10)
            .and_then(|slice| <[u8; 4]>::try_from(slice).ok())
            .map(u32::from_be_bytes)
            .unwrap_or_default() as usize;
        assert_eq!(
            declared + GENERIC_HEADER_LEN,
            frame.len,
            "declared payload length does not close the frame"
        );
        assert_eq!(
            bytes.get(10..14),
            Some([0x00, 0x01, 0x00, 0x00].as_slice()),
            "participant version prefix must be v1.0"
        );
    }
}

/// Everything the session must be true of ITSELF, before the committed capture
/// is consulted at all. Split out so a failure here reads as "the session
/// changed" rather than "the capture is stale" — they are different findings
/// and want different responses.
fn assert_session_shape(session: &Session) {
    for capture in [&session.admitter, &session.observer] {
        for direction in [Direction::ClientToServer, Direction::ServerToClient] {
            println!(
                "CENSUS {} {}: {:?}",
                capture.name,
                direction.marker(),
                census(capture, direction)
            );
        }
    }

    assert_census(
        &census(&session.admitter, Direction::ClientToServer),
        ADMITTER_C2S,
        "admitter C->S",
    );
    assert_census(
        &response_census(&session.admitter, Direction::ServerToClient),
        ADMITTER_S2C_RESPONSES,
        "admitter S->C responses",
    );
    assert_census(
        &census(&session.observer, Direction::ClientToServer),
        OBSERVER_C2S,
        "observer C->S",
    );
    assert_census(
        &response_census(&session.observer, Direction::ServerToClient),
        OBSERVER_S2C_RESPONSES,
        "observer S->C responses",
    );
    assert_header_invariants(&session.admitter);
    assert_header_invariants(&session.observer);

    // A sender is NOT delivered its own ordinary record: the whole reason this
    // trace needs a second connection. Asserted, not assumed — the admitter DOES
    // receive deliveries (the peer's lifecycle records), so the claim is
    // specifically about the record it admitted.
    assert!(
        !delivery_sequences(&session.admitter).contains(&session.record_delivery_seq),
        "the admitter was delivered its own record at {}; the session's premise has changed",
        session.record_delivery_seq
    );
    assert!(
        delivery_sequences(&session.observer).contains(&session.record_delivery_seq),
        "the observer never saw the record at delivery_seq {}",
        session.record_delivery_seq
    );
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[test]
fn the_canonical_participant_session_matches_the_committed_golden_trace()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let mut session = run_session(&home.path().join("store"))?;
    locate_variables(&mut session);

    // The scenario's own shape, so a capture that drifted is not silently
    // re-blessed by regenerating it.
    assert_eq!(
        session.admitter_participant, 0,
        "the first minted identity in a fresh store is slot 0"
    );
    assert_eq!(
        session.observer_participant, 1,
        "the second minted identity in a fresh store is slot 1"
    );
    assert_eq!(
        session.secrets.len(),
        3,
        "two enrollments and one attach rotation mint three secrets"
    );
    assert_eq!(
        session.deadlines.len(),
        6,
        "each bound response stamps a receipt and a provenance deadline"
    );

    assert_session_shape(&session);

    if let Ok(out_dir) = std::env::var("LIMINAL_GOLDEN_TRACE_OUT") {
        let path = if Path::new(&out_dir).is_absolute() {
            PathBuf::from(&out_dir)
        } else {
            repo_root().join(&out_dir)
        };
        write_artifacts(&session, &path)?;
        println!("golden trace written to {}", path.display());
    }

    let committed_dir = repo_root().join(CAPTURE_DIR);
    let index_path = committed_dir.join("frames.jsonl");
    if !index_path.exists() {
        println!(
            "no committed capture at {} yet; skipping the masked comparison",
            index_path.display()
        );
        return Ok(());
    }
    let committed = read_committed_capture(&committed_dir)?;

    // 1. The request/response spine, masked at its declared run-variable ranges,
    //    must be byte-identical to the committed capture. This is the strong
    //    claim: given these requests, a conforming server emits exactly these
    //    bytes, and the only bytes a foreign implementation may not predict are
    //    the ones named in `frames.jsonl`.
    for (fresh, old) in [
        (&session.admitter, &committed.admitter),
        (&session.observer, &committed.observer),
    ] {
        for direction in [Direction::ClientToServer, Direction::ServerToClient] {
            let fresh_spine = masked_selection(fresh, direction, |frame| !is_push(frame));
            let old_spine = masked_selection(old, direction, |frame| !is_push(frame));
            assert_eq!(
                fresh_spine.iter().map(|f| hex(f)).collect::<Vec<_>>(),
                old_spine.iter().map(|f| hex(f)).collect::<Vec<_>>(),
                "{} {}: a request/response byte outside every declared run-variable \
                 range changed",
                fresh.name,
                direction.marker()
            );
        }
    }

    // 2. Pushes are held to the weaker standard their schedule permits: every
    //    push this run observed must be one of the committed push IMAGES. A run
    //    that observes fewer pushes (the pump had not fired yet) is not a
    //    failure; a push whose BYTES differ is.
    //
    //    The admitter's pushes are opportunistic — it never drains deliberately,
    //    so it observes one only when the pump happens to fire ahead of a
    //    response — and so its count is not required to be non-zero. The
    //    observer drains on purpose, so its is.
    assert_push_shapes(&session.admitter, &committed.admitter, false);
    assert_push_shapes(&session.observer, &committed.observer, true);

    println!(
        "canonical session: record committed at delivery_seq {}",
        session.record_delivery_seq
    );
    Ok(())
}

/// Delivery sequences observed in one connection's pushes.
fn delivery_sequences(capture: &Capture) -> Vec<u64> {
    capture
        .frames
        .iter()
        .filter(|frame| is_push(frame))
        .filter_map(|frame| {
            let bytes = capture
                .stream(frame.direction)
                .get(frame.offset..frame.offset.saturating_add(frame.len))?;
            match liminal_protocol::wire::decode(bytes, ReceiverDirection::Client).ok()? {
                ParticipantFrame::ServerPush(ServerPush::ParticipantDelivery(delivery)) => {
                    Some(delivery.delivery_seq)
                }
                _ => None,
            }
        })
        .collect()
}

fn assert_push_shapes(fresh: &Capture, committed: &Capture, require_any: bool) {
    let known: Vec<String> = masked_selection(committed, Direction::ServerToClient, is_push)
        .iter()
        .map(|bytes| hex(bytes))
        .collect();
    let seen = masked_selection(fresh, Direction::ServerToClient, is_push);
    for image in &seen {
        let rendered = hex(image);
        assert!(
            known.contains(&rendered),
            "{}: an unrecognised push image crossed the wire: {rendered}\nknown: {known:#?}",
            fresh.name
        );
    }
    assert!(
        !require_any || !seen.is_empty(),
        "{}: no push arrived at all; the delivery half of the trace is untested",
        fresh.name
    );
}

/// The committed capture, rebuilt from its raw streams plus its frame index.
struct CommittedCapture {
    admitter: Capture,
    observer: Capture,
}

fn read_committed_capture(dir: &Path) -> Result<CommittedCapture, Box<dyn Error>> {
    let frames = read_committed_index(&dir.join("frames.jsonl"))?;
    let build = |name: &str| -> Result<Capture, Box<dyn Error>> {
        Ok(Capture {
            name: name.to_owned(),
            c2s: std::fs::read(dir.join(format!("{name}.c2s.bin")))?,
            s2c: std::fs::read(dir.join(format!("{name}.s2c.bin")))?,
            frames: frames
                .iter()
                .filter(|frame| frame.connection == name)
                .cloned()
                .collect(),
        })
    };
    Ok(CommittedCapture {
        admitter: build("admitter")?,
        observer: build("observer")?,
    })
}
