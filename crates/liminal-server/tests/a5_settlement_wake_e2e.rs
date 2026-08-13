//! Amendment A5 (participant contract §0.16) — the settlement wake, END TO END
//! over a real socket, and the drain-then-retry round trip it exists to enable.
//!
//! # What Leg 2's unit pins could not reach
//!
//! `production/tests_a5_settlement.rs` measures the refusal at the semantic
//! seam and `connection/participant_delivery_tests.rs` measures the push at the
//! delivery pump. Neither of them proves the two halves are joined: that a
//! client refused `MarkerSettlementBackpressure` on a real connection later
//! reads a real `0x0202 MarkerSettled` frame off that same socket, carrying the
//! epoch its own refusal named.
//!
//! # ⛔ The negative half is the point (§0.16 build obligation 3)
//!
//! "The lazy implementation pushes `MarkerSettled` to every connection on the
//! conversation; that is a settlement-timing side channel to uninvolved parties
//! and is OUTLAWED." A green that only proves the refused client HEARD the wake
//! is consistent with a broadcast. So a second live participant sits on the same
//! conversation for the whole run — it is the one that DRIVES the clearing write
//! — and it must read zero settlement wakes.
//!
//! An absence is a measurement of the instrument until proven otherwise, so the
//! negative is never asserted on its own: [`settlement_wakes`] is ONE function,
//! applied to both inboxes in the same test, and the refused client's inbox is
//! asserted to make it return a hit. A predicate that could not see a
//! `MarkerSettled` would fail on the positive control before it could report a
//! false negative on the uninvolved client.
//!
//! Two further holes are closed by construction rather than by hope:
//!
//! * every inbound frame either client reads all run long is ACCUMULATED
//!   ([`Client::pushes`]), including the ones read while demultiplexing a
//!   reply. The other participant e2e harnesses discard pushes inside their
//!   `exchange` loop; doing that here would let a `MarkerSettled` delivered to
//!   the uninvolved client vanish into the same `_ => {}` arm the negative is
//!   measured over.
//! * the uninvolved client is drained LAST, after the refused client's wake has
//!   already been read off the wire. It therefore had strictly more wall-clock
//!   opportunity to receive one than the client that did.
//!
//! # What Leg 2's `AckGap { current_cursor: 0 }` actually was
//!
//! Leg 2 could not close the retry round trip. The wall is measured here and
//! stated so nobody re-derives it: the ordinary-admission selector checks the
//! observer floor against the conversation's retained floor BEFORE it will
//! consider the `DrainFirst` arm, so on a conversation where nobody
//! acknowledges, the record admission that ought to be the clearing write is
//! refused `ObserverBackpressure` and the settlement can never clear. Leg 2's
//! unit fixture
//! (`production/tests_a5_settlement.rs::settlement_window`) opens a real window
//! and every pin over it is sound, but it never drives a clearing write, so that
//! state's inability to clear was invisible from inside it.
//!
//! The fix is not a wait: it is that both members READ AND ACKNOWLEDGE what the
//! pump offered them after every commit ([`Client::pump_and_ack`]), with the
//! acknowledged sequence DERIVED from the delivery sequences the client was
//! actually offered rather than guessed — which is exactly what a `current_cursor
//! = 0` `AckGap` is telling a caller that guessed.
//!
//! This lives in an integration test for the reason `tests/loopback_sdk_e2e.rs`
//! documents: `liminal-server` dev-depends on `liminal-sdk` and the SDK depends
//! back, so only an integration test sees ONE `EmbeddedServer` type.

use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use liminal_protocol::wire::{
    AttachAttemptToken, AttachSecret, ClientRequest, ConversationId, CredentialAttachRequest,
    EnrollmentRequest, EnrollmentToken, Generation, MarkerSettlementBackpressure, ParticipantAck,
    ParticipantId, RecordAdmission, RecordAdmissionAttemptToken, ServerPush, ServerValue,
};
use liminal_sdk::{
    ConnectionPoolConfig, PARTICIPANT_PUMP_WINDOW, ParticipantResumeStore, RemoteConfig,
    RemoteOperationRecordOutcome, RemoteParticipantHandle, RemoteParticipantInbound,
    RemoteParticipantSendOutcome, SdkError,
};
use liminal_server::config::types::ParticipantConfig;
use liminal_server::config::{LimitsConfig, ServerConfig, ServicesConfig};
use liminal_server::server::connection::{
    ConnectionServices, ConnectionSupervisor, LiminalConnectionServices,
};
use liminal_server::server::listener::ServerListener;

/// The one conversation both clients live in.
const SETTLEMENT_CONVERSATION: ConversationId = 0xA516;

/// Frame-count bound on a reply-owed demultiplex. A step that reaches it failed.
const MAX_DEMUX_FRAMES: usize = 128;

/// Frame-count bound on one push drain. A drain that reaches it failed.
const MAX_DRAIN_FRAMES: usize = 128;

/// Records the producer will commit while hunting for the settlement window.
///
/// The unit fixture (`production/tests_a5_settlement.rs::settlement_window`)
/// finds its candidate inside twelve commits against the same tightened
/// configuration; this is a margin over that, and running out is a hard failure
/// rather than a skip — see [`open_settlement_window`].
const MAX_WINDOW_HUNT_COMMITS: u8 = 24;

/// Quiet window the fixture's per-round drains use.
///
/// Short on purpose: those drains run dozens of times and are plumbing, not
/// measurement. Every drain whose result is ASSERTED uses the SDK's own
/// `PARTICIPANT_PUMP_WINDOW` instead.
const FIXTURE_QUIET: Duration = Duration::from_millis(250);

/// Deployment-shaped participant configuration, tightened exactly as the unit
/// fixture's `marker_fixture_config` tightens it.
///
/// The four changed fields are the whole reason a marker candidate can ever be
/// reached in bounded time: retained capacity has to be small enough that an
/// unacknowledged member's abandoned prefix mints an immutable-sequence
/// candidate within a handful of ordinary commits. Every other field is the
/// deployment-shaped value the other participant e2e harnesses use.
const fn settlement_participant_config() -> ParticipantConfig {
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
        // ⛔ The four marker-fixture tightenings.
        max_ordinary_record_bytes: 58,
        max_generated_marker_entries: 1,
        max_generated_marker_bytes: 4_096,
        mandatory_transaction_bound_entries: 4,
        mandatory_transaction_bound_bytes: 16_384,
        full_recovery_claim_entries: 4,
        full_recovery_claim_bytes: 16_384,
        retained_capacity_entries: 14,
        retained_capacity_bytes: 65_536,
        max_retained_record_rows: 16,
        closure_episode_churn_limit: 1_024,
    }
}

/// The resume store the SDK participant checkpoints into.
#[derive(Debug, Default, Clone)]
struct SettlementStore {
    canonical: Arc<Mutex<Vec<u8>>>,
}

impl ParticipantResumeStore for SettlementStore {
    fn persist(&mut self, canonical_lpcr: &[u8]) -> Result<(), SdkError> {
        let mut guard = self.canonical.lock().map_err(|_| SdkError::Store {
            description: "the settlement resume store lock was poisoned".to_owned(),
        })?;
        guard.clear();
        guard.extend_from_slice(canonical_lpcr);
        drop(guard);
        Ok(())
    }
}

type SdkParticipant = RemoteParticipantHandle<SettlementStore>;

fn server_config() -> Result<ServerConfig, Box<dyn Error>> {
    Ok(ServerConfig {
        listen_address: "127.0.0.1:0".parse()?,
        health_listen_address: "127.0.0.1:0".parse()?,
        drain_timeout_ms: 30_000,
        channels: Vec::new(),
        routing_rules: Vec::new(),
        // ⛔ EPHEMERAL BY MEASUREMENT, NOT BY CONVENIENCE, and named here because
        // a reader will otherwise assume it was a shortcut. Point this at a
        // `Some(dir)` haematite database instead and the clearing write dies
        // inside its durable append with `haematite store error: storage error:
        // shard operation failed: shard WAL error: wal tree error: invalid tree
        // node`; the connection process crashes and the client reads EOF. That
        // failure is in the store layer under the marker-drain row, not in the
        // settlement mechanics these pins measure, and it is recorded in the
        // lane evidence
        // (`gate-logs/breaking-window/leg3-c-e2e-disk-store-marker-drain-failure.log`)
        // rather than worked around silently. `None` selects the config's own
        // self-owning ephemeral store — a real deployment configuration, and the
        // one `push_flush_e2e.rs` runs against.
        persistence_path: None,
        cluster: None,
        auth: None,
        services: ServicesConfig::default(),
        limits: LimitsConfig::default(),
        websocket: None,
        participant: Some(settlement_participant_config()),
    })
}

const fn pool() -> ConnectionPoolConfig {
    ConnectionPoolConfig::new(1, 1, 8)
}

/// A bound TCP server with the participant protocol live.
struct SettlementServer {
    listener: Option<ServerListener>,
    supervisor: ConnectionSupervisor,
    address: String,
}

impl SettlementServer {
    fn start() -> Result<Self, Box<dyn Error>> {
        let config = server_config()?;
        let services = Arc::new(LiminalConnectionServices::from_config(&config)?);
        let supervisor =
            ConnectionSupervisor::with_services(services as Arc<dyn ConnectionServices>)?;
        let listener = ServerListener::bind(&config, supervisor.clone())?;
        let address = listener.local_addr().to_string();
        Ok(Self {
            listener: Some(listener),
            supervisor,
            address,
        })
    }

    fn shutdown(mut self) -> Result<(), Box<dyn Error>> {
        if let Some(listener) = self.listener.take() {
            listener.shutdown()?;
        }
        self.supervisor.shutdown();
        Ok(())
    }
}

/// One participant on its own real socket, with EVERY push it has ever read.
///
/// The accumulation is the negative half's instrument. A harness that discarded
/// pushes inside its reply demultiplex — which is what every other participant
/// e2e harness does, correctly, for its own purposes — would measure "the
/// uninvolved client received no wake" over an inbox it had already been
/// throwing frames away from.
struct Client {
    label: &'static str,
    handle: SdkParticipant,
    /// Owned so the connection outlives the handle's use of it.
    _config: RemoteConfig,
    pushes: Vec<ServerPush>,
    participant_id: ParticipantId,
    generation: Generation,
    attach_secret: AttachSecret,
    /// Greatest delivery sequence this client has already acknowledged.
    acked_through: u64,
}

impl Client {
    /// Dials a fresh socket and enrolls, leaving the participant bound.
    fn enroll(
        server: &SettlementServer,
        label: &'static str,
        enrollment_token: [u8; 16],
    ) -> Result<Self, Box<dyn Error>> {
        let config = RemoteConfig::new(
            server.address.clone(),
            label,
            SETTLEMENT_CONVERSATION.to_string(),
            pool(),
        )?
        .connect_tcp()?;
        let handle = RemoteParticipantHandle::new(&config, SettlementStore::default())?;
        let mut client = Self {
            label,
            handle,
            _config: config,
            pushes: Vec::new(),
            participant_id: 0,
            generation: Generation::ONE,
            attach_secret: AttachSecret::new([0; 32]),
            acked_through: 0,
        };
        let enrolled = client.exchange(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: SETTLEMENT_CONVERSATION,
            enrollment_token: EnrollmentToken::new(enrollment_token),
        }))?;
        let ServerValue::EnrollBound(bound) = enrolled else {
            return Err(format!("[{label}] enrollment did not bind: {enrolled:?}").into());
        };
        client.participant_id = bound.participant_id();
        client.generation = bound.capability_generation();
        client.attach_secret = bound.attach_secret();
        Ok(client)
    }

    /// Sends one request and reads until its correlated response arrives,
    /// KEEPING every push it passes on the way.
    fn exchange(&mut self, request: ClientRequest) -> Result<ServerValue, Box<dyn Error>> {
        let recorded = self
            .handle
            .record_operation(request)
            .map_err(|error| format!("[{}] record_operation failed: {error:?}", self.label))?;
        let operation = match recorded {
            RemoteOperationRecordOutcome::Recorded(operation)
            | RemoteOperationRecordOutcome::Continuous(operation) => operation,
            RemoteOperationRecordOutcome::Refused { request, reason } => {
                return Err(format!(
                    "[{}] SDK refused outbound request {request:?}: {reason:?}",
                    self.label
                )
                .into());
            }
        };
        let sent = self
            .handle
            .send_operation(operation)
            .map_err(|error| format!("[{}] send_operation failed: {error:?}", self.label))?;
        match sent {
            RemoteParticipantSendOutcome::Sent { .. } => {}
            RemoteParticipantSendOutcome::TransportLost { error, .. } => {
                return Err(
                    format!("[{}] SDK transport lost while sending: {error}", self.label).into(),
                );
            }
        }
        for _ in 0..MAX_DEMUX_FRAMES {
            let inbound = self
                .handle
                .receive()
                .map_err(|error| format!("[{}] receive failed: {error:?}", self.label))?;
            match inbound {
                RemoteParticipantInbound::Applied { value, .. }
                | RemoteParticipantInbound::Refused { value, .. } => return Ok(value),
                RemoteParticipantInbound::Push { value, .. } => self.pushes.push(value),
            }
        }
        Err(format!(
            "[{}] no response arrived within {MAX_DEMUX_FRAMES} inbound frames",
            self.label
        )
        .into())
    }

    /// Reads pushes until the connection reports silence, keeping all of them.
    ///
    /// Returns how many this drain collected, so a caller can report what it
    /// actually saw rather than asserting a push-timing number.
    fn drain(&mut self) -> Result<usize, Box<dyn Error>> {
        self.drain_within(PARTICIPANT_PUMP_WINDOW)
    }

    /// [`Self::drain`] with a caller-named quiet window.
    ///
    /// The fixture's per-round drains use a short one because they run dozens of
    /// times and a healthy loopback push arrives in microseconds; every drain
    /// whose RESULT is asserted uses the SDK's own
    /// [`PARTICIPANT_PUMP_WINDOW`], so no assertion in this file is measured
    /// through a window the fixture shortened for its own convenience.
    fn drain_within(&mut self, quiet: Duration) -> Result<usize, Box<dyn Error>> {
        let mut drained = 0_usize;
        while drained < MAX_DRAIN_FRAMES {
            let inbound = self
                .handle
                .receive_within(quiet)
                .map_err(|error| format!("[{}] the push drain failed: {error:?}", self.label))?;
            match inbound {
                None => return Ok(drained),
                Some(RemoteParticipantInbound::Push { value, .. }) => {
                    self.pushes.push(value);
                    drained += 1;
                }
                Some(other) => {
                    return Err(format!(
                        "[{}] the drain read a correlated frame it never asked for: {other:?}",
                        self.label
                    )
                    .into());
                }
            }
        }
        Err(format!(
            "[{}] the drain never reached silence within {MAX_DRAIN_FRAMES} frames",
            self.label
        )
        .into())
    }

    /// One ordinary record admission under this client's current credential.
    fn commit_record(&mut self, nonce: u8) -> Result<ServerValue, Box<dyn Error>> {
        self.exchange(ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: SETTLEMENT_CONVERSATION,
            participant_id: self.participant_id,
            capability_generation: self.generation,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([nonce; 16]),
            payload: vec![nonce],
        }))
    }

    /// One credential attach under this client's current credential.
    fn attach(&mut self, attempt_token: [u8; 16]) -> Result<ServerValue, Box<dyn Error>> {
        self.exchange(ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: SETTLEMENT_CONVERSATION,
            participant_id: self.participant_id,
            capability_generation: self.generation,
            attach_secret: self.attach_secret,
            attach_attempt_token: AttachAttemptToken::new(attempt_token),
            accept_marker_delivery_seq: None,
        }))
    }

    /// Greatest delivery sequence this client has been offered so far.
    fn delivered_through(&self) -> u64 {
        self.pushes
            .iter()
            .filter_map(|push| match push {
                ServerPush::ParticipantDelivery(delivery)
                    if delivery.conversation_id == SETTLEMENT_CONVERSATION =>
                {
                    Some(delivery.delivery_seq)
                }
                ServerPush::ParticipantDelivery(_)
                | ServerPush::ObserverProgressed { .. }
                | ServerPush::MarkerSettled { .. } => None,
            })
            .max()
            .unwrap_or(0)
    }

    /// Collects whatever the pump has for this client and acknowledges it.
    ///
    /// ⛔ This is what makes a `DrainFirst` clearing write reachable at all. The
    /// ordinary-admission selector checks the observer floor against the
    /// conversation's retained floor before it will consider draining a marker:
    /// with nobody acknowledging, the admission that should have taken the
    /// `DrainFirst` arm is refused `ObserverBackpressure` instead, and the
    /// settlement never clears. Leg 2 hit exactly this wall from the other side
    /// (`AckGap { current_cursor: 0 }`), which is why the acknowledgement is
    /// DERIVED from the delivery sequences this client was actually offered
    /// rather than guessed.
    fn pump_and_ack(&mut self, quiet: Duration) -> Result<(), Box<dyn Error>> {
        self.drain_within(quiet)?;
        let through_seq = self.delivered_through();
        if through_seq <= self.acked_through {
            return Ok(());
        }
        let acked = self.exchange(ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: SETTLEMENT_CONVERSATION,
            participant_id: self.participant_id,
            capability_generation: self.generation,
            through_seq,
        }))?;
        match acked {
            ServerValue::AckCommitted(_) | ServerValue::AckNoOp(_) => {
                self.acked_through = through_seq;
                Ok(())
            }
            other => Err(format!(
                "[{}] acknowledging delivery {through_seq} did not commit: {other:?}",
                self.label
            )
            .into()),
        }
    }

    /// One credential attach that MUST bind, adopting the rotated credential.
    fn attach_and_adopt(&mut self, attempt_token: [u8; 16]) -> Result<(), Box<dyn Error>> {
        let bound = self.attach(attempt_token)?;
        let ServerValue::AttachBound(bound) = bound else {
            return Err(format!("[{}] attach did not bind: {bound:?}", self.label).into());
        };
        self.generation = bound.capability_generation();
        self.attach_secret = bound.attach_secret();
        Ok(())
    }
}

/// ⛔ THE ONE PREDICATE. Applied to the refused client's inbox as a positive
/// control and to the uninvolved client's inbox as the obligation-3 negative,
/// in the same test, so the negative can never be a measurement of a broken
/// instrument.
fn settlement_wakes(pushes: &[ServerPush]) -> Vec<(ConversationId, u64)> {
    pushes
        .iter()
        .filter_map(|push| match *push {
            ServerPush::MarkerSettled {
                conversation_id,
                refused_epoch,
            } => Some((conversation_id, refused_epoch)),
            ServerPush::ObserverProgressed { .. } | ServerPush::ParticipantDelivery(_) => None,
        })
        .collect()
}

/// Both clients, with a settlement window open and the refused client's waiter
/// already installed by its own refusal.
struct SettlementWindow {
    server: SettlementServer,
    /// The client whose attach was refused: the ONLY connection §0.16 allows a
    /// wake to reach.
    refused: Client,
    /// A live participant on the same conversation that was never refused. It
    /// also drives the clearing write, which is what makes its silence
    /// load-bearing rather than incidental.
    uninvolved: Client,
    /// Epoch the refusal named.
    refused_epoch: u64,
    /// The attempt token the refused attach carried, so the retry can present
    /// the SAME attempt (stage-11 discipline: retry once, not a new attempt).
    refused_attempt_token: [u8; 16],
    /// Ordinary commits it took to mint the marker candidate, reported rather
    /// than asserted.
    commits: u8,
}

/// Drives real traffic until an attach on the refused client hears
/// `MarkerSettlementBackpressure`.
///
/// The refusal IS the window detector, which is what makes this fixture
/// non-vacuous over a socket where no test can read the seam's classifier: the
/// window is not claimed to exist, it is heard. Running out of commits is a hard
/// failure — a settlement pin measured over a conversation that never blocked is
/// exactly the vacuous predicate this fixture exists to prevent.
fn open_settlement_window() -> Result<SettlementWindow, Box<dyn Error>> {
    let server = SettlementServer::start()?;

    // The producer, on its own socket. Its commits generate the marker debt and
    // its attach is the one that will be refused.
    let mut refused = Client::enroll(&server, "a5-refused", [0xA5; 16])?;
    // A SECOND member that never acknowledges. Marker debt is produced by an
    // overtaken participant's abandoned prefix, so a conversation whose only
    // member is also its only producer never mints a candidate and this fixture
    // would hunt forever.
    let mut uninvolved = Client::enroll(&server, "a5-uninvolved", [0xB5; 16])?;

    // Both members attach before any record is admitted, in the unit fixture's
    // own order. This is not decoration: an enrolled-but-unattached second
    // member leaves the conversation's closure accounting non-`Clear` with an
    // empty immutable-candidate lane, which is the ordinary admission selector's
    // `ObserverBackpressure` arm — the FIRST record would be refused and no
    // marker debt could ever accumulate.
    uninvolved.attach_and_adopt([0xB0; 16])?;
    refused.attach_and_adopt([0xA0; 16])?;

    for nonce in 1..=MAX_WINDOW_HUNT_COMMITS {
        let committed = refused.commit_record(nonce)?;
        if !matches!(committed, ServerValue::RecordCommitted(_)) {
            return Err(format!(
                "record {nonce} must commit to generate marker debt, got {committed:?}"
            )
            .into());
        }
        // Both members read and acknowledge what the pump offered them. See
        // `Client::pump_and_ack`: without this the retained floor never moves
        // and the clearing write is refused `ObserverBackpressure` instead of
        // taking the `DrainFirst` arm.
        refused.pump_and_ack(FIXTURE_QUIET)?;
        uninvolved.pump_and_ack(FIXTURE_QUIET)?;
        let attempt_token = [0xC0 | (nonce & 0x0F); 16];
        match refused.attach(attempt_token)? {
            ServerValue::MarkerSettlementBackpressure(
                MarkerSettlementBackpressure::CredentialAttach {
                    conversation_id,
                    refused_epoch,
                },
            ) => {
                assert_eq!(conversation_id, SETTLEMENT_CONVERSATION);
                return Ok(SettlementWindow {
                    server,
                    refused,
                    uninvolved,
                    refused_epoch,
                    refused_attempt_token: attempt_token,
                    commits: nonce,
                });
            }
            ServerValue::AttachBound(bound) => {
                // The window was not open yet; the probe rotated the credential
                // and the fixture carries the new one forward.
                refused.generation = bound.capability_generation();
                refused.attach_secret = bound.attach_secret();
            }
            other => {
                return Err(format!(
                    "the window probe must either bind or hear the settlement row, got {other:?}"
                )
                .into());
            }
        }
    }
    Err(format!(
        "no marker-settlement window opened within {MAX_WINDOW_HUNT_COMMITS} commits"
    )
    .into())
}

/// §0.16 condition 2 END TO END, plus build obligation 3.
///
/// The refused client reads a real `0x0202 MarkerSettled` frame off its own
/// socket carrying the conversation and the epoch ITS OWN refusal named; the
/// uninvolved participant — which drove the very clearing write that fired the
/// wake — reads none.
///
/// RED-PROVEN (positive half) against this same test with the clearing write not
/// driven: `settlement_wakes` then returns `[]` where `[(42262, 8)]` is required,
/// over an inbox the same drain filled with six other pushes — so the assertion
/// is not passing on a frame that was already there, and the drain is not
/// silently broken. Evidence:
/// `gate-logs/breaking-window/leg3-c-e2e-red1-no-clearing-write.log`.
///
/// CONTROL-PROVEN (negative half): the broadcast implementation this outlaws
/// lives in `dispatch.rs`, which this lane may not edit, so the negative is
/// proven non-vacuous instead of mutation-proven — `settlement_wakes` is
/// asserted to RETURN A HIT over the refused client's inbox in this same test,
/// two statements earlier, so the empty result over the uninvolved client's
/// inbox is a measurement rather than a broken filter.
#[test]
fn the_settlement_wake_reaches_the_refused_connection_and_no_other() -> Result<(), Box<dyn Error>> {
    let SettlementWindow {
        server,
        mut refused,
        mut uninvolved,
        refused_epoch,
        commits,
        ..
    } = open_settlement_window()?;
    println!("settlement window opened after {commits} commits, refused_epoch = {refused_epoch}");

    // Nothing has been settled yet, so neither inbox may hold a wake. This is
    // the before-image that stops a later hit being read as pre-existing noise.
    println!(
        "before the clearing write: refused drained {}, uninvolved drained {}",
        refused.drain()?,
        uninvolved.drain()?
    );
    assert!(
        settlement_wakes(&refused.pushes).is_empty(),
        "a wake reached the refused client before anything cleared: {:?}",
        refused.pushes
    );
    assert!(
        settlement_wakes(&uninvolved.pushes).is_empty(),
        "a wake reached the uninvolved client before anything cleared: {:?}",
        uninvolved.pushes
    );

    // ⛔ THE CLEARING WRITE: an ordinary record admission by the OTHER
    // participant, which takes the `RecordAdmissionDecision::DrainFirst` arm and
    // reaches `impact.record_marker_settled` inside `persist_next_marker`.
    let cleared = uninvolved.commit_record(0xE1)?;
    assert!(
        matches!(cleared, ServerValue::RecordCommitted(_)),
        "the clearing write must commit through the DrainFirst arm, got {cleared:?}"
    );

    // The refused client reads its wake off the wire.
    let drained = refused.drain()?;
    println!("refused client drained {drained} pushes after the clearing write");
    assert_eq!(
        settlement_wakes(&refused.pushes),
        vec![(SETTLEMENT_CONVERSATION, refused_epoch)],
        "the refused connection must receive exactly its own wake, carrying the conversation \
         and the epoch its refusal named -- refused_epoch is load-bearing, it is what the \
         stage-11 retry discipline matches on. Its inbox held: {:?}",
        refused.pushes
    );

    // THE NEGATIVE, drained LAST so this client had strictly more opportunity to
    // receive a wake than the one that did.
    let drained = uninvolved.drain()?;
    println!("uninvolved client drained {drained} pushes after the clearing write");
    assert_eq!(
        settlement_wakes(&uninvolved.pushes),
        Vec::new(),
        "§0.16 build obligation 3: an uninvolved live participant on the same conversation -- \
         even the one that DROVE the clearing write -- must receive no settlement wake. Its \
         inbox held: {:?}",
        uninvolved.pushes
    );

    drop(refused);
    drop(uninvolved);
    server.shutdown()?;
    Ok(())
}

/// §0.16 condition 2's RETRY DISCIPLINE, end to end: "persist the waiting state,
/// retry once after matching `MarkerSettled`".
///
/// The refusal is not a lawful answer if the retry it prescribes cannot land, so
/// this closes the round trip the row exists for: the same attach attempt —
/// SAME attempt token, not a fresh attempt — presented again after the wake
/// whose epoch matches the refusal's, and it binds.
///
/// The epoch match is asserted before the retry rather than assumed. A wake
/// carrying some other candidate's epoch would not discharge this refusal's
/// wait, and a retry fired on it would be a retry on a coincidence.
///
/// RED-PROVEN against this same test with the clearing write not driven AND the
/// epoch assertion removed, so that what fails is the RETRY itself: it hears
/// `MarkerSettlementBackpressure { refused_epoch: 8 }` a second time instead of
/// binding (`gate-logs/breaking-window/leg3-c-e2e-red2-retry-without-drain.log`).
/// The green is therefore the drain's doing and not the passage of time. The
/// epoch assertion above is the SAME `settlement_wakes` comparison red-proven in
/// `leg3-c-e2e-red1-no-clearing-write.log`.
#[test]
fn the_refused_attach_binds_when_retried_after_its_matching_wake() -> Result<(), Box<dyn Error>> {
    let SettlementWindow {
        server,
        mut refused,
        mut uninvolved,
        refused_epoch,
        refused_attempt_token,
        ..
    } = open_settlement_window()?;

    // ⛔ THE CLEARING WRITE, again by the other participant.
    let cleared = uninvolved.commit_record(0xE2)?;
    assert!(
        matches!(cleared, ServerValue::RecordCommitted(_)),
        "the clearing write must commit through the DrainFirst arm, got {cleared:?}"
    );

    refused.drain()?;
    assert_eq!(
        settlement_wakes(&refused.pushes),
        vec![(SETTLEMENT_CONVERSATION, refused_epoch)],
        "the retry below is only lawful once a MarkerSettled carrying THIS refusal's epoch has \
         arrived; without that match it would be a retry on a coincidence. Inbox: {:?}",
        refused.pushes
    );

    // The SAME attempt, retried once. Not a new attach.
    let retried = refused.attach(refused_attempt_token)?;
    match retried {
        ServerValue::AttachBound(bound) => {
            assert_eq!(bound.conversation_id(), SETTLEMENT_CONVERSATION);
            assert_eq!(bound.participant_id(), refused.participant_id);
        }
        other => {
            return Err(format!(
                "the retry after a matching MarkerSettled must bind, got {other:?}"
            )
            .into());
        }
    }

    drop(refused);
    drop(uninvolved);
    server.shutdown()?;
    Ok(())
}
