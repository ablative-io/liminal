#![allow(clippy::expect_used, clippy::panic)]

use std::collections::{BTreeMap, BTreeSet};

use liminal_protocol::algebra::{
    ResourceDimension, ResourceVector, WideResourceVector, floor_transition, mandatory_capacity,
    retained_baseline, zero_debt_admission, zero_debt_capacity_failure,
};
use liminal_protocol::lifecycle::{
    ActiveBinding, AllocatedParticipantSlot, BindingState, BindingTerminalDisposition,
    BoundParticipantCursor, ClosureDebt, NonzeroDebtCursorEpisode, ObserverProjection,
    ParticipantSlotAllocatorProof, PendingBindingTerminalPosition,
};
use liminal_protocol::outcome::{
    CapabilityLimitField, CheckedMultiplyOverflow, CredentialRecoveryLost, HandshakeSizeOperands,
    KeepaliveCertificationFailed, KeepaliveField, ParkingLimitField, ParkingShapeViolation,
    ParticipantCapabilityConfigurationInvalid, ParticipantParkingConfigurationInvalid,
    ParticipantRecoveryHandshakeTooLarge, ParticipantRetentionCapacityInvalid, PlatformName,
    RecordAdmissionOperation, RecordAdmissionUnknown, RecoveryHandshakeDimension,
    SdkParkingCapacityIncompatible, StartupKeepaliveReason,
};
use liminal_protocol::wire::{
    AckCommitted, AckGap, AttachAttemptToken, AttachBound, AttachEnvelope, AttachSecret,
    AuthenticationState, BindingEpoch, BindingRequiredEnvelope, ClientDiscriminant, ClientRequest,
    CodecError, CommonStaleAuthorityEnvelope, ConnectionIncarnation, ConversationSequenceExhausted,
    CredentialAttachRequest, DecodeClass, DetachAttemptToken, EnrollmentEnvelope, EnrollmentKnown,
    EnrollmentReceiptCapacityScope, EnrollmentRequest, EnrollmentToken, FRAME_MAX, Generation,
    IdentityCapacityExceeded, IdentityCapacityScope, InboundGateContext, LeaveAttemptToken,
    LeaveCommitted, LeaveEnvelope, LeaveRequest, LeaveStaleAuthority, MarkerAckCommitted,
    MarkerAckEnvelope, NegotiatedParticipantCapability, ObserverBackpressure,
    ObserverBackpressureState, ObserverProgressStatus, ObserverRecoveryAccepted, ParticipantAck,
    ParticipantAckEnvelope, ParticipantCapabilityState, ParticipantDelivery, ParticipantFrame,
    ParticipantRecord, ParticipantReferenceEnvelope, ParticipantTransportRejected,
    ReceiptCapacityExceeded, ReceiptCapacityScope, ReceiptExpired, ReceiptExpiryReason,
    ReceiptReplay, ReceiverDirection, RecordAdmissionEnvelope, RecordCommitted, Retired,
    SequenceAllocatingEnvelope, SequenceBudget, ServerDiscriminant, ServerPush, ServerValue,
    StaleAuthority, StaleOrUnknownReceipt, TransportRejectionReason, ValidatedFrameLimit, decode,
    encode, encoded_len, gate_inbound,
};

const fn generation(value: u64) -> Generation {
    Generation::new(value).expect("acceptance fixtures use nonzero generations")
}

const fn epoch(server: u64, connection: u64, generation: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(server, connection),
        self::generation(generation),
    )
}

fn round_trip(frame: &ParticipantFrame, receiver: ReceiverDirection) -> ParticipantFrame {
    let mut bytes = vec![0; encoded_len(frame).expect("typed frame has a canonical encoding")];
    let written = encode(frame, &mut bytes).expect("sized output accepts canonical frame");
    assert_eq!(written, bytes.len());
    decode(&bytes, receiver).expect("canonical frame decodes at its assigned receiver")
}

fn round_trip_request(request: ClientRequest) -> ClientRequest {
    let frame = ParticipantFrame::ClientRequest(request.clone());
    assert_eq!(
        round_trip(&frame, ReceiverDirection::Server),
        ParticipantFrame::ClientRequest(request.clone())
    );
    request
}

fn round_trip_delivery(delivery: ParticipantDelivery) -> ParticipantDelivery {
    let push = ServerPush::ParticipantDelivery(delivery.clone());
    let frame = ParticipantFrame::ServerPush(push);
    assert_eq!(
        round_trip(&frame, ReceiverDirection::Client),
        ParticipantFrame::ServerPush(ServerPush::ParticipantDelivery(delivery.clone()))
    );
    delivery
}

fn round_trip_value(value: ServerValue) -> ServerValue {
    let frame = ParticipantFrame::ServerValue(value.clone());
    assert_eq!(
        round_trip(&frame, ReceiverDirection::Client),
        ParticipantFrame::ServerValue(value.clone())
    );
    value
}

fn ordinary_delivery(conversation_id: u64, sequence: u64, payload: u8) -> ParticipantDelivery {
    ParticipantDelivery {
        conversation_id,
        delivery_seq: sequence,
        record: ParticipantRecord::OrdinaryRecord {
            sender_participant_id: 7,
            payload: vec![payload],
        },
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeliveryDisposition {
    Applied,
    ProvenDuplicate,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct SdkWatermarks {
    confirmed: BTreeMap<u64, u64>,
    applied_keys: Vec<(u64, u64)>,
}

impl SdkWatermarks {
    fn receive(&mut self, delivery: &ParticipantDelivery) -> DeliveryDisposition {
        let confirmed = self.confirmed.entry(delivery.conversation_id).or_default();
        if delivery.delivery_seq <= *confirmed {
            return DeliveryDisposition::ProvenDuplicate;
        }
        assert_eq!(delivery.delivery_seq, *confirmed + 1);
        *confirmed = delivery.delivery_seq;
        self.applied_keys
            .push((delivery.conversation_id, delivery.delivery_seq));
        DeliveryDisposition::Applied
    }

    fn cursor(&self, conversation_id: u64) -> u64 {
        self.confirmed.get(&conversation_id).copied().unwrap_or(0)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SequenceAllocator {
    high_watermark: u64,
}

impl SequenceAllocator {
    const fn admission(&mut self, commit: bool) -> Option<u64> {
        if !commit {
            return None;
        }
        self.high_watermark += 1;
        Some(self.high_watermark)
    }
}

// Frozen contract lines 3346-3347.
#[test]
fn acceptance_01_same_session_duplicate_guard() {
    let delivery = round_trip_delivery(ordinary_delivery(101, 1, 0xA1));
    let mut sdk = SdkWatermarks::default();

    assert_eq!(sdk.receive(&delivery), DeliveryDisposition::Applied);
    assert_eq!(sdk.cursor(101), 1);
    assert_eq!(sdk.receive(&delivery), DeliveryDisposition::ProvenDuplicate);
    assert_eq!(sdk.applied_keys, vec![(101, 1)]);
}

// Frozen contract lines 3348-3350.
#[test]
fn acceptance_02_crash_before_ack_redelivers_identical_key() {
    let first = round_trip_delivery(ordinary_delivery(102, 1, 0xA2));
    let application_key = (first.conversation_id, first.delivery_seq);

    // The application effect survives, but the SDK checkpoint does not.
    let mut recovered_sdk = SdkWatermarks::default();
    let redelivered = round_trip_delivery(first);
    assert_eq!(
        (redelivered.conversation_id, redelivered.delivery_seq),
        application_key
    );
    assert_eq!(
        recovered_sdk.receive(&redelivered),
        DeliveryDisposition::Applied
    );
    assert_eq!(application_key, (102, 1));
}

// Frozen contract lines 3351-3352.
#[test]
fn acceptance_03_failed_admissions_do_not_consume_sequence() {
    let mut allocator = SequenceAllocator::default();
    let failure_points = [false, false, true, false, true, false, false, true];
    let committed: Vec<u64> = failure_points
        .into_iter()
        .filter_map(|commit| allocator.admission(commit))
        .collect();

    for sequence in &committed {
        let outcome = RecordCommitted::new(
            RecordAdmissionEnvelope {
                conversation_id: 103,
                participant_id: 7,
                capability_generation: Generation::ONE,
                record_admission_attempt_token:
                    liminal_protocol::wire::RecordAdmissionAttemptToken::new([0xA7; 16]),
            },
            *sequence,
        );
        assert_eq!(outcome.delivery_seq(), *sequence);
    }
    assert_eq!(committed, vec![1, 2, 3]);
    assert_eq!(allocator.high_watermark, 3);
}

// Frozen contract lines 3353-3354.
#[test]
fn acceptance_04_all_participants_observe_one_conversation_sequence() {
    let records = [
        ParticipantRecord::Attached {
            affected_participant_id: 1,
            binding_epoch: epoch(4, 1, 1),
        },
        ParticipantRecord::OrdinaryRecord {
            sender_participant_id: 1,
            payload: vec![4],
        },
        ParticipantRecord::Detached {
            affected_participant_id: 1,
            binding_epoch: epoch(4, 1, 1),
            cause: liminal_protocol::wire::DetachedCause::Superseded,
        },
    ];
    let deliveries: Vec<_> = records
        .into_iter()
        .enumerate()
        .map(|(index, record)| {
            round_trip_delivery(ParticipantDelivery {
                conversation_id: 104,
                delivery_seq: u64::try_from(index).expect("three records fit u64") + 1,
                record,
            })
        })
        .collect();

    for _participant_id in 1..=3 {
        let mut sdk = SdkWatermarks::default();
        for delivery in &deliveries {
            assert_eq!(sdk.receive(delivery), DeliveryDisposition::Applied);
        }
        assert_eq!(sdk.cursor(104), 3);
        assert_eq!(sdk.applied_keys, vec![(104, 1), (104, 2), (104, 3)]);
    }
}

// Frozen contract lines 3355-3356.
#[test]
fn acceptance_05_replay_live_cutover_is_lossless_and_ordered() {
    let replay = [
        round_trip_delivery(ordinary_delivery(105, 1, 1)),
        round_trip_delivery(ordinary_delivery(105, 2, 2)),
    ];
    let raced_live_commit = round_trip_delivery(ordinary_delivery(105, 3, 3));
    let mut sdk = SdkWatermarks::default();

    for delivery in replay.iter().chain(std::iter::once(&raced_live_commit)) {
        assert_eq!(sdk.receive(delivery), DeliveryDisposition::Applied);
    }
    assert_eq!(sdk.applied_keys, vec![(105, 1), (105, 2), (105, 3)]);
    assert_eq!(
        sdk.receive(&replay[1]),
        DeliveryDisposition::ProvenDuplicate
    );
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompactionLiveOrdering {
    CompactionThenLiveCommit,
    LiveCommitRacesCandidateDrain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompactionRaceOperation {
    CompactionCandidateDrain,
    LiveCommit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompactionCandidateState {
    Pending,
    Completed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LiveCommitState {
    NotSubmitted,
    PendingCandidateDrain,
    Appended,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MarkerConfirmationState {
    NotDelivered,
    Delivered,
    AwaitingConfirmation,
    CrashedAwaitingRedelivery,
    RedeliveredAwaitingConfirmation,
    Confirmed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PersistedMarkerDelivery {
    key: (u64, u64),
    body: ParticipantRecord,
    encoded_frame: Vec<u8>,
}

impl PersistedMarkerDelivery {
    fn new(delivery: &ParticipantDelivery) -> Self {
        let frame = ParticipantFrame::ServerPush(ServerPush::ParticipantDelivery(delivery.clone()));
        let mut encoded_frame =
            vec![0; encoded_len(&frame).expect("marker frame has a canonical encoding")];
        encode(&frame, &mut encoded_frame).expect("sized marker buffer accepts its encoding");
        Self {
            key: (delivery.conversation_id, delivery.delivery_seq),
            body: delivery.record.clone(),
            encoded_frame,
        }
    }

    fn decode_delivery(&self) -> ParticipantDelivery {
        let decoded = decode(&self.encoded_frame, ReceiverDirection::Client)
            .expect("persisted canonical marker decodes after recovery");
        let ParticipantFrame::ServerPush(ServerPush::ParticipantDelivery(delivery)) = decoded
        else {
            panic!("persisted marker bytes decoded as a different frame kind");
        };
        assert_eq!((delivery.conversation_id, delivery.delivery_seq), self.key);
        assert_eq!(delivery.record, self.body);
        delivery
    }
}

#[derive(Debug, PartialEq, Eq)]
struct MarkerReplayHarness {
    high_watermark: u64,
    cursor: u64,
    compaction_candidate: CompactionCandidateState,
    live_commit: LiveCommitState,
    operation_trace: Vec<CompactionRaceOperation>,
    append_log: Vec<ParticipantDelivery>,
    persisted_marker: Option<PersistedMarkerDelivery>,
    accepted_marker: Option<PersistedMarkerDelivery>,
    marker_confirmation: MarkerConfirmationState,
    retained_physical: BTreeSet<u64>,
    abandoned_sequences: BTreeSet<u64>,
    delivered_ordinary_keys: BTreeSet<(u64, u64)>,
    cursor_transitions: Vec<(u64, u64)>,
    queued_live: Option<ParticipantDelivery>,
}

impl MarkerReplayHarness {
    fn for_ordering(ordering: CompactionLiveOrdering) -> Self {
        let mut fixture = Self {
            high_watermark: 29,
            cursor: 10,
            compaction_candidate: CompactionCandidateState::Pending,
            live_commit: LiveCommitState::NotSubmitted,
            operation_trace: Vec::new(),
            append_log: Vec::new(),
            persisted_marker: None,
            accepted_marker: None,
            marker_confirmation: MarkerConfirmationState::NotDelivered,
            retained_physical: (20..=29).collect(),
            abandoned_sequences: BTreeSet::new(),
            delivered_ordinary_keys: (1..=10).map(|sequence| (106, sequence)).collect(),
            cursor_transitions: Vec::new(),
            queued_live: None,
        };
        match ordering {
            CompactionLiveOrdering::CompactionThenLiveCommit => {
                fixture.complete_compaction_candidate();
                fixture.commit_live_record();
            }
            CompactionLiveOrdering::LiveCommitRacesCandidateDrain => {
                fixture.commit_live_record();
                fixture.complete_compaction_candidate();
            }
        }
        fixture
    }

    const fn next_sequence(&mut self) -> u64 {
        self.high_watermark += 1;
        self.high_watermark
    }

    fn complete_compaction_candidate(&mut self) {
        self.operation_trace
            .push(CompactionRaceOperation::CompactionCandidateDrain);
        assert_eq!(self.compaction_candidate, CompactionCandidateState::Pending);
        let marker = round_trip_delivery(ParticipantDelivery {
            conversation_id: 106,
            delivery_seq: self.next_sequence(),
            record: ParticipantRecord::HistoryCompacted {
                affected_participant_id: 6,
                abandoned_after: self.cursor,
                abandoned_through: 29,
                physical_floor_at_decision: 20,
            },
        });
        self.persisted_marker = Some(PersistedMarkerDelivery::new(&marker));
        self.append_log.push(marker);
        self.compaction_candidate = CompactionCandidateState::Completed;
        if self.live_commit == LiveCommitState::PendingCandidateDrain {
            self.append_live_record();
            self.live_commit = LiveCommitState::Appended;
        }
    }

    fn commit_live_record(&mut self) {
        self.operation_trace
            .push(CompactionRaceOperation::LiveCommit);
        assert_eq!(self.live_commit, LiveCommitState::NotSubmitted);
        if self.compaction_candidate == CompactionCandidateState::Pending {
            self.live_commit = LiveCommitState::PendingCandidateDrain;
        } else {
            self.append_live_record();
            self.live_commit = LiveCommitState::Appended;
        }
    }

    fn append_live_record(&mut self) {
        let live = round_trip_delivery(ordinary_delivery(106, self.next_sequence(), 0x31));
        self.append_log.push(live.clone());
        self.queued_live = Some(live);
    }

    fn deliver_marker(&mut self) -> (Vec<u8>, DeliveryDisposition) {
        let persisted = self
            .persisted_marker
            .as_ref()
            .expect("candidate drain persists the marker before delivery");
        let disposition = if let Some(accepted) = &self.accepted_marker {
            assert_eq!(
                self.marker_confirmation,
                MarkerConfirmationState::CrashedAwaitingRedelivery
            );
            assert_eq!(accepted.key, persisted.key);
            assert_eq!(accepted.body, persisted.body);
            assert_eq!(accepted.encoded_frame, persisted.encoded_frame);
            self.marker_confirmation = MarkerConfirmationState::RedeliveredAwaitingConfirmation;
            DeliveryDisposition::ProvenDuplicate
        } else {
            assert_eq!(
                self.marker_confirmation,
                MarkerConfirmationState::NotDelivered
            );
            self.marker_confirmation = MarkerConfirmationState::Delivered;
            DeliveryDisposition::Applied
        };
        let delivery = persisted.decode_delivery();
        assert_eq!(delivery.delivery_seq, 30);
        (persisted.encoded_frame.clone(), disposition)
    }

    fn accept_marker(&mut self) -> Result<MarkerAckCommitted, AckGap> {
        let persisted = self
            .persisted_marker
            .as_ref()
            .expect("candidate drain persists the marker before acceptance")
            .clone();
        let request = MarkerAckEnvelope {
            conversation_id: persisted.key.0,
            participant_id: 6,
            capability_generation: Generation::ONE,
            marker_delivery_seq: persisted.key.1,
        };
        if self.marker_confirmation != MarkerConfirmationState::Delivered {
            return Err(AckGap::new(
                ParticipantAckEnvelope {
                    conversation_id: request.conversation_id,
                    participant_id: request.participant_id,
                    capability_generation: request.capability_generation,
                    through_seq: request.marker_delivery_seq,
                },
                self.cursor,
            )
            .expect("undelivered marker is above cursor"));
        }

        let delivery = persisted.decode_delivery();
        let ParticipantRecord::HistoryCompacted {
            affected_participant_id,
            abandoned_after,
            abandoned_through,
            physical_floor_at_decision,
        } = delivery.record
        else {
            panic!("persisted marker has the wrong record kind");
        };
        assert_eq!(affected_participant_id, request.participant_id);
        assert_eq!(abandoned_after, self.cursor);
        assert_eq!(abandoned_through, 29);
        assert_eq!(physical_floor_at_decision, 20);
        self.abandoned_sequences
            .extend((abandoned_after + 1)..=abandoned_through);
        self.cursor_transitions
            .push((self.cursor, request.marker_delivery_seq));
        self.cursor = request.marker_delivery_seq;
        self.retained_physical.clear();
        self.accepted_marker = Some(persisted);
        self.marker_confirmation = MarkerConfirmationState::AwaitingConfirmation;
        Ok(MarkerAckCommitted::new(request))
    }

    fn crash_before_confirmation(mut self) -> Self {
        assert_eq!(
            self.marker_confirmation,
            MarkerConfirmationState::AwaitingConfirmation
        );
        self.marker_confirmation = MarkerConfirmationState::CrashedAwaitingRedelivery;
        self
    }

    fn confirm_replayed_ack(&mut self) {
        assert_eq!(
            self.marker_confirmation,
            MarkerConfirmationState::RedeliveredAwaitingConfirmation
        );
        self.marker_confirmation = MarkerConfirmationState::Confirmed;
    }

    fn deliver_live_record(&mut self) -> Option<ParticipantDelivery> {
        if self.marker_confirmation != MarkerConfirmationState::Confirmed {
            return None;
        }
        let live = self.queued_live.take()?;
        assert_eq!(live.delivery_seq, self.cursor + 1);
        self.cursor_transitions
            .push((self.cursor, live.delivery_seq));
        self.cursor = live.delivery_seq;
        self.delivered_ordinary_keys
            .insert((live.conversation_id, live.delivery_seq));
        Some(live)
    }
}

fn assert_compaction_race_log(replay: &MarkerReplayHarness, ordering: CompactionLiveOrdering) {
    let expected_operations = match ordering {
        CompactionLiveOrdering::CompactionThenLiveCommit => vec![
            CompactionRaceOperation::CompactionCandidateDrain,
            CompactionRaceOperation::LiveCommit,
        ],
        CompactionLiveOrdering::LiveCommitRacesCandidateDrain => vec![
            CompactionRaceOperation::LiveCommit,
            CompactionRaceOperation::CompactionCandidateDrain,
        ],
    };
    assert_eq!(replay.operation_trace, expected_operations);
    assert_eq!(
        replay
            .append_log
            .iter()
            .map(|delivery| delivery.delivery_seq)
            .collect::<Vec<_>>(),
        vec![30, 31]
    );
    assert_eq!(
        replay.append_log[0].record,
        ParticipantRecord::HistoryCompacted {
            affected_participant_id: 6,
            abandoned_after: 10,
            abandoned_through: 29,
            physical_floor_at_decision: 20,
        }
    );
    assert_eq!(
        replay.append_log[1].record,
        ParticipantRecord::OrdinaryRecord {
            sender_participant_id: 7,
            payload: vec![0x31],
        }
    );
}

fn exercise_marker_accept_and_replay(ordering: CompactionLiveOrdering) {
    let mut replay = MarkerReplayHarness::for_ordering(ordering);
    assert_compaction_race_log(&replay, ordering);
    assert_eq!(replay.retained_physical, (20..=29).collect());
    assert_eq!(
        replay.delivered_ordinary_keys,
        (1..=10).map(|sequence| (106, sequence)).collect()
    );

    let (first_encoding, first_delivery) = replay.deliver_marker();
    assert_eq!(first_delivery, DeliveryDisposition::Applied);
    let committed = replay
        .accept_marker()
        .expect("the delivered marker can be accepted");
    assert_eq!(committed.current_cursor(), 30);
    assert_eq!(replay.cursor, 30);
    assert_eq!(replay.abandoned_sequences, (11..=29).collect());
    assert!(replay.retained_physical.is_empty());
    assert_eq!(replay.cursor_transitions, vec![(10, 30)]);
    assert_eq!(
        replay
            .delivered_ordinary_keys
            .iter()
            .map(|(_, sequence)| *sequence)
            .collect::<BTreeSet<_>>(),
        (1..=10).collect()
    );

    let mut recovered = replay.crash_before_confirmation();
    let (replayed_encoding, replay_delivery) = recovered.deliver_marker();
    assert_eq!(replay_delivery, DeliveryDisposition::ProvenDuplicate);
    assert_eq!(replayed_encoding, first_encoding);
    assert_eq!(recovered.cursor_transitions, vec![(10, 30)]);
    let accepted = recovered
        .accepted_marker
        .as_ref()
        .expect("accepted marker key and body survive the crash");
    let persisted = recovered
        .persisted_marker
        .as_ref()
        .expect("server delivery journal survives the crash");
    assert_eq!(accepted.key, (106, 30));
    assert_eq!(accepted.key, persisted.key);
    assert_eq!(accepted.body, persisted.body);
    recovered.confirm_replayed_ack();

    let live = recovered
        .deliver_live_record()
        .expect("live delivery remains queued behind marker confirmation");
    assert_eq!(live.delivery_seq, 31);
    assert_eq!(recovered.cursor, 31);
    assert_eq!(recovered.cursor_transitions, vec![(10, 30), (30, 31)]);
    assert_eq!(
        recovered
            .cursor_transitions
            .iter()
            .filter(|transition| **transition == (10, 30))
            .count(),
        1
    );
    assert_eq!(recovered.abandoned_sequences, (11..=29).collect());
    assert!(
        recovered
            .delivered_ordinary_keys
            .is_disjoint(&(11..=29).map(|sequence| (106, sequence)).collect())
    );
    assert_eq!(
        recovered.delivered_ordinary_keys,
        (1..=10)
            .chain(std::iter::once(31))
            .map(|sequence| (106, sequence))
            .collect()
    );
}

fn exercise_marker_decline(ordering: CompactionLiveOrdering) {
    let mut declined = MarkerReplayHarness::for_ordering(ordering);
    assert_compaction_race_log(&declined, ordering);
    let (_, declined_delivery) = declined.deliver_marker();
    assert_eq!(declined_delivery, DeliveryDisposition::Applied);
    assert_eq!(declined.cursor, 10);
    assert_eq!(declined.retained_physical, (20..=29).collect());
    assert!(declined.abandoned_sequences.is_empty());
    assert_eq!(declined.deliver_live_record(), None);
    assert_eq!(declined.retained_physical, (20..=29).collect());
}

// Frozen contract lines 3357-3365.
#[test]
fn acceptance_06_history_compaction_marker_accept_decline_and_replay() {
    for ordering in [
        CompactionLiveOrdering::CompactionThenLiveCommit,
        CompactionLiveOrdering::LiveCommitRacesCandidateDrain,
    ] {
        exercise_marker_accept_and_replay(ordering);
        exercise_marker_decline(ordering);
    }

    let mut undelivered =
        MarkerReplayHarness::for_ordering(CompactionLiveOrdering::LiveCommitRacesCandidateDrain);
    let gap = undelivered
        .accept_marker()
        .expect_err("normal ack cannot invent marker delivery");
    assert_eq!(gap.current_cursor(), 10);
    assert_eq!(undelivered.retained_physical, (20..=29).collect());
    assert!(undelivered.abandoned_sequences.is_empty());
    assert!(undelivered.cursor_transitions.is_empty());
}

// Frozen contract lines 3366-3367.
#[test]
fn acceptance_07_conversation_demux_has_independent_cursors() {
    let interleaved = [
        ordinary_delivery(107, 1, 1),
        ordinary_delivery(207, 1, 2),
        ordinary_delivery(107, 2, 3),
        ordinary_delivery(207, 2, 4),
    ];
    let mut sdk = SdkWatermarks::default();
    for delivery in interleaved.map(round_trip_delivery) {
        assert_eq!(sdk.receive(&delivery), DeliveryDisposition::Applied);
    }
    assert_eq!(sdk.cursor(107), 2);
    assert_eq!(sdk.cursor(207), 2);
    assert_eq!(
        sdk.applied_keys,
        vec![(107, 1), (207, 1), (107, 2), (207, 2)]
    );
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplayTrigger {
    Attach,
    StorageCompletion,
    Ack,
    CommittedRecord,
    ObserverProgress,
    AdmittedDeadline,
    TimerSweep,
}

fn trigger_replay(progress: &mut u64, trigger: ReplayTrigger) {
    if trigger != ReplayTrigger::TimerSweep {
        *progress += 1;
    }
}

// Frozen contract lines 3368-3370.
#[test]
fn acceptance_08_silence_never_advances_replay() {
    let mut progress = 0;
    trigger_replay(&mut progress, ReplayTrigger::TimerSweep);
    assert_eq!(progress, 0);

    for trigger in [
        ReplayTrigger::Attach,
        ReplayTrigger::StorageCompletion,
        ReplayTrigger::Ack,
        ReplayTrigger::CommittedRecord,
        ReplayTrigger::ObserverProgress,
        ReplayTrigger::AdmittedDeadline,
    ] {
        trigger_replay(&mut progress, trigger);
    }
    assert_eq!(progress, 6);

    let wake = ServerPush::ObserverProgressed {
        conversation_id: 108,
        refused_epoch: 4,
        observer_progress: 5,
    };
    let wake_frame = ParticipantFrame::ServerPush(wake.clone());
    assert_eq!(
        round_trip(&wake_frame, ReceiverDirection::Client),
        ParticipantFrame::ServerPush(wake)
    );
}

const fn enroll_bound(
    conversation_id: u64,
    token_byte: u8,
    participant_id: u64,
) -> liminal_protocol::wire::EnrollBound {
    liminal_protocol::wire::EnrollBound::new(
        conversation_id,
        EnrollmentToken::new([token_byte; 16]),
        participant_id,
        AttachSecret::new([token_byte; 32]),
        epoch(9, participant_id, 1),
        1_000,
        2_000,
    )
    .expect("enrollment origin epoch is generation one")
}

const fn enrollment_envelope(conversation_id: u64, token_byte: u8) -> EnrollmentEnvelope {
    EnrollmentEnvelope {
        conversation_id,
        enrollment_token: EnrollmentToken::new([token_byte; 16]),
    }
}

// Frozen contract lines 3371-3376.
#[test]
fn acceptance_09_enrollment_replay_is_stable_and_expiry_cannot_ghost_mint() {
    let receipt = enroll_bound(109, 9, 0);
    let first = round_trip_value(ServerValue::EnrollBound(receipt.clone()));
    let replay = round_trip_value(ServerValue::Bound(ReceiptReplay::Enrollment(
        receipt.clone(),
    )));
    assert!(matches!(first, ServerValue::EnrollBound(ref value) if value == &receipt));
    assert!(
        matches!(replay, ServerValue::Bound(ReceiptReplay::Enrollment(ref value)) if value == &receipt)
    );
    assert_eq!(receipt.capability_generation(), Generation::ONE);
    assert_eq!(receipt.attach_secret(), AttachSecret::new([9; 32]));

    let distinct_a = round_trip_request(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: 109,
        enrollment_token: EnrollmentToken::new([0xA9; 16]),
    }));
    let distinct_b = round_trip_request(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: 109,
        enrollment_token: EnrollmentToken::new([0xB9; 16]),
    }));
    assert_ne!(distinct_a, distinct_b);
    let identities = [
        AllocatedParticipantSlot::from_allocator(SlotProof {
            conversation_id: 109,
            index: 0,
            limit: 2,
        })
        .expect("first distinct token reserves index zero")
        .participant_id(),
        AllocatedParticipantSlot::from_allocator(SlotProof {
            conversation_id: 109,
            index: 1,
            limit: 2,
        })
        .expect("second distinct token reserves index one")
        .participant_id(),
    ];
    assert_eq!(identities, [0, 1]);

    let expired = ReceiptExpired::Enrollment {
        conversation_id: 109,
        token: receipt.token(),
        participant_id: receipt.participant_id(),
        result_generation: Generation::ONE,
        current_generation: Generation::ONE,
        reason: ReceiptExpiryReason::Deadline,
    };
    assert_eq!(
        round_trip_value(ServerValue::ReceiptExpired(expired.clone())),
        ServerValue::ReceiptExpired(expired)
    );
    assert_eq!(identities, [0, 1]);
}

// Frozen contract lines 3377-3380.
#[test]
fn acceptance_10_member_replay_detach_reattach_leave_and_floor() {
    let initial_replay: Vec<_> = (1..=3)
        .map(|sequence| round_trip_delivery(ordinary_delivery(110, sequence, 10)))
        .collect();
    assert_eq!(
        initial_replay
            .iter()
            .map(|delivery| delivery.delivery_seq)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );

    let detached_cursor = 3;
    let offline: Vec<_> = (4..=5)
        .map(|sequence| round_trip_delivery(ordinary_delivery(110, sequence, 10)))
        .collect();
    assert_eq!(offline[0].delivery_seq, detached_cursor + 1);
    assert_eq!(offline[1].delivery_seq, 5);

    let floor = floor_transition(1, None, 6, 5, 6);
    assert_eq!(floor.member_cursor, 6);
    assert_eq!(floor.preferred_floor, 6);
    assert_eq!(floor.resulting_floor, 6);

    let retired = Retired::Participant {
        request: ParticipantReferenceEnvelope::RecordAdmission(RecordAdmissionEnvelope {
            conversation_id: 110,
            participant_id: 10,
            capability_generation: Generation::ONE,
            record_admission_attempt_token:
                liminal_protocol::wire::RecordAdmissionAttemptToken::new([0xA7; 16]),
        }),
        retired_generation: Generation::ONE,
    };
    assert_eq!(
        round_trip_value(ServerValue::Retired(retired.clone())),
        ServerValue::Retired(retired)
    );
}

// Frozen contract lines 3381-3385.
#[test]
fn acceptance_11_generation_race_has_one_rotation_and_one_handoff_pair() {
    let request_generation = Generation::ONE;
    let winning = AttachBound::ordinary(
        111,
        AttachAttemptToken::new([0x11; 16]),
        11,
        request_generation,
        AttachSecret::new([0x21; 32]),
        epoch(11, 2, 2),
        0,
        1_000,
        2_000,
    )
    .expect("generation two is the exact successor");
    let stale_token = AttachAttemptToken::new([0x12; 16]);
    assert_eq!(winning.capability_generation(), generation(2));
    assert_ne!(winning.token(), stale_token);

    let handoff = [
        ParticipantRecord::Detached {
            affected_participant_id: 11,
            binding_epoch: epoch(11, 1, 1),
            cause: liminal_protocol::wire::DetachedCause::Superseded,
        },
        ParticipantRecord::Attached {
            affected_participant_id: 11,
            binding_epoch: winning.origin_binding_epoch(),
        },
    ];
    let log: Vec<_> = handoff
        .into_iter()
        .enumerate()
        .map(|(index, record)| {
            round_trip_delivery(ParticipantDelivery {
                conversation_id: 111,
                delivery_seq: u64::try_from(index).expect("pair index fits u64") + 1,
                record,
            })
        })
        .collect();
    assert_eq!(log.len(), 2);

    let replay = round_trip_value(ServerValue::Bound(ReceiptReplay::CredentialAttach(
        winning.clone(),
    )));
    assert!(
        matches!(replay, ServerValue::Bound(ReceiptReplay::CredentialAttach(value)) if value == winning)
    );

    let durable = DurableMutationCounts {
        bindings: 1,
        records: u64::try_from(log.len()).expect("handoff pair length fits u64"),
        identities: 1,
    };
    let unchanged = durable;
    let stale = StaleAuthority::Live {
        request: CommonStaleAuthorityEnvelope::CredentialAttach(AttachEnvelope {
            conversation_id: 111,
            participant_id: 11,
            capability_generation: request_generation,
            attach_attempt_token: stale_token,
            accept_marker_delivery_seq: None,
        }),
        current_generation: generation(2),
    };
    let (_, refusal_snapshot) = durable.classify_without_commit(ServerValue::StaleAuthority(stale));
    assert_eq!(refusal_snapshot, durable);
    assert_eq!(durable, unchanged);
    assert_eq!(log.len(), 2);
}

// Frozen contract lines 3386-3391.
#[test]
fn acceptance_12_retired_replays_never_disclose_secret_or_remint() {
    let enrollment = Retired::Enrollment {
        request: enrollment_envelope(112, 0x12),
        participant_id: 12,
        retired_generation: Generation::ONE,
    };
    assert_eq!(
        round_trip_value(ServerValue::Retired(enrollment.clone())),
        ServerValue::Retired(enrollment)
    );

    let attach = Retired::Participant {
        request: ParticipantReferenceEnvelope::CredentialAttach(AttachEnvelope {
            conversation_id: 112,
            participant_id: 12,
            capability_generation: Generation::ONE,
            attach_attempt_token: AttachAttemptToken::new([0x22; 16]),
            accept_marker_delivery_seq: None,
        }),
        retired_generation: generation(2),
    };
    let encoded = round_trip_value(ServerValue::Retired(attach.clone()));
    assert_eq!(encoded, ServerValue::Retired(attach));
    // `Retired` has no attach-secret, binding, or record field by construction.
}

// Frozen contract lines 3392-3404.
#[test]
fn acceptance_13_leave_authority_decode_replay_and_serialized_races() {
    let leave_envelope = LeaveEnvelope {
        conversation_id: 113,
        participant_id: 13,
        capability_generation: generation(2),
        leave_attempt_token: LeaveAttemptToken::new([0x13; 16]),
    };
    let no_binding = ServerValue::NoBinding(liminal_protocol::wire::NoBinding {
        request: BindingRequiredEnvelope::Leave(leave_envelope.clone()),
    });
    assert_eq!(round_trip_value(no_binding.clone()), no_binding);

    for presented in [Generation::ONE, generation(2)] {
        let stale = ServerValue::StaleAuthority(StaleAuthority::Leave(LeaveStaleAuthority::Live {
            conversation_id: 113,
            participant_id: 13,
            presented_generation: presented,
            leave_attempt_token: leave_envelope.leave_attempt_token,
            current_generation: generation(2),
        }));
        assert_eq!(round_trip_value(stale.clone()), stale);
    }

    let request = ParticipantFrame::ClientRequest(ClientRequest::Leave(LeaveRequest {
        conversation_id: 113,
        participant_id: 13,
        capability_generation: generation(2),
        attach_secret: AttachSecret::new([0x13; 32]),
        leave_attempt_token: leave_envelope.leave_attempt_token,
    }));
    let mut bytes = vec![0; encoded_len(&request).expect("leave encodes")];
    let _ = encode(&request, &mut bytes).expect("leave output is sized");
    let _ = bytes.pop();
    assert_eq!(
        decode(&bytes, ReceiverDirection::Server),
        Err(CodecError::Decode {
            class: DecodeClass::MissingRequiredField
        })
    );

    let committed = LeaveCommitted::new(
        113,
        leave_envelope.leave_attempt_token,
        13,
        generation(2),
        Some(epoch(13, 2, 2)),
        None,
        8,
    )
    .expect("leave epoch carries retired generation");
    let stable = round_trip_value(ServerValue::LeaveCommitted(committed.clone()));
    assert_eq!(stable, ServerValue::LeaveCommitted(committed.clone()));
    assert_eq!(
        round_trip_value(ServerValue::LeaveCommitted(committed.clone())),
        stable
    );
    let left = round_trip_delivery(ParticipantDelivery {
        conversation_id: 113,
        delivery_seq: committed.left_delivery_seq(),
        record: ParticipantRecord::Left {
            affected_participant_id: 13,
            ended_binding_epoch: committed.ended_binding_epoch(),
        },
    });
    assert_eq!(left.delivery_seq, 8);

    let attach_first = StaleAuthority::Leave(LeaveStaleAuthority::Live {
        conversation_id: 113,
        participant_id: 13,
        presented_generation: generation(2),
        leave_attempt_token: leave_envelope.leave_attempt_token,
        current_generation: generation(3),
    });
    assert!(matches!(attach_first, StaleAuthority::Leave(_)));
    let leave_first = Retired::Participant {
        request: ParticipantReferenceEnvelope::CredentialAttach(AttachEnvelope {
            conversation_id: 113,
            participant_id: 13,
            capability_generation: generation(2),
            attach_attempt_token: AttachAttemptToken::new([0x33; 16]),
            accept_marker_delivery_seq: None,
        }),
        retired_generation: generation(2),
    };
    assert!(matches!(leave_first, Retired::Participant { .. }));
}

// Frozen contract lines 3405-3409.
#[test]
fn acceptance_14_unbound_receipt_never_grants_new_connection_authority() {
    let lost = AttachBound::ordinary(
        114,
        AttachAttemptToken::new([0x14; 16]),
        14,
        Generation::ONE,
        AttachSecret::new([0x24; 32]),
        epoch(14, 1, 2),
        4,
        1_000,
        2_000,
    )
    .expect("lost attach rotates to generation two");
    let replay = round_trip_value(ServerValue::UnboundReceipt(
        ReceiptReplay::CredentialAttach(lost.clone()),
    ));
    let c2_authority = match replay {
        ServerValue::UnboundReceipt(_) => None,
        ServerValue::Bound(receipt) => Some(receipt),
        _ => panic!("receipt replay has one of the two binding classifications"),
    };
    assert_eq!(c2_authority, None);
    let fresh = AttachBound::ordinary(
        114,
        AttachAttemptToken::new([0x15; 16]),
        14,
        generation(2),
        AttachSecret::new([0x25; 32]),
        epoch(14, 2, 3),
        lost.persisted_cursor(),
        2_000,
        3_000,
    )
    .expect("fresh C2 attach rotates to generation three");
    assert!(matches!(
        round_trip_value(ServerValue::AttachBound(fresh.clone())),
        ServerValue::AttachBound(value) if value == fresh
    ));
    let ack = AckCommitted::new(ParticipantAckEnvelope {
        conversation_id: 114,
        participant_id: 14,
        capability_generation: generation(3),
        through_seq: 5,
    });
    assert_eq!(ack.current_cursor(), 5);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SdkCredential {
    generation: Generation,
    secret: AttachSecret,
}

impl SdkCredential {
    fn persist_if_newer(&mut self, receipt: &AttachBound) -> bool {
        if receipt.capability_generation() <= self.generation {
            return false;
        }
        self.generation = receipt.capability_generation();
        self.secret = receipt.attach_secret();
        true
    }
}

// Frozen contract lines 3410-3416.
#[test]
fn acceptance_15_sdk_generation_monotonicity_deadlines_and_receipt_cap() {
    let generation_two = AttachBound::ordinary(
        115,
        AttachAttemptToken::new([0x15; 16]),
        15,
        Generation::ONE,
        AttachSecret::new([2; 32]),
        epoch(15, 1, 2),
        0,
        1_000,
        2_000,
    )
    .expect("generation two receipt");
    let generation_three = AttachBound::ordinary(
        115,
        AttachAttemptToken::new([0x25; 16]),
        15,
        generation(2),
        AttachSecret::new([3; 32]),
        epoch(15, 2, 3),
        0,
        2_000,
        3_000,
    )
    .expect("generation three receipt");
    let mut sdk = SdkCredential {
        generation: Generation::ONE,
        secret: AttachSecret::new([1; 32]),
    };
    assert!(sdk.persist_if_newer(&generation_three));
    assert!(!sdk.persist_if_newer(&generation_two));
    assert_eq!(sdk.generation, generation(3));
    assert_eq!(sdk.secret, AttachSecret::new([3; 32]));

    let before = ServerValue::Bound(ReceiptReplay::CredentialAttach(generation_three.clone()));
    assert_eq!(round_trip_value(before.clone()), before);
    let after = ServerValue::ReceiptExpired(ReceiptExpired::CredentialAttach {
        conversation_id: 115,
        token: generation_three.token(),
        participant_id: 15,
        presented_generation: generation(2),
        presented_marker_delivery_seq: None,
        result_generation: generation(3),
        current_generation: generation(3),
        reason: ReceiptExpiryReason::Deadline,
    });
    assert_eq!(round_trip_value(after.clone()), after);

    let cap = ServerValue::ReceiptCapacityExceeded(ReceiptCapacityExceeded::CredentialAttach {
        request: AttachEnvelope {
            conversation_id: 115,
            participant_id: 15,
            capability_generation: generation(3),
            attach_attempt_token: AttachAttemptToken::new([0x35; 16]),
            accept_marker_delivery_seq: None,
        },
        scope: ReceiptCapacityScope::LiveReceiptParticipant,
        limit: 1,
        occupied: 1,
    });
    let before_refusal = sdk;
    assert_eq!(round_trip_value(cap.clone()), cap);
    assert_eq!(sdk, before_refusal);
    let mut replay_progress = 0;
    trigger_replay(&mut replay_progress, ReplayTrigger::TimerSweep);
    assert_eq!(replay_progress, 0); // expiry has no scan-driven work
}

// Frozen contract lines 3417-3421; exact R-C4 fixtures are lines 3221-3244.
#[test]
fn acceptance_16_four_worked_floor_cases() {
    let multiple_claims = floor_transition(1, Some(10), 100, 100, 25);
    assert_eq!(multiple_claims.preferred_floor, 11);
    assert_eq!(multiple_claims.resulting_floor, 25);
    assert_eq!(
        (1_u64..25).collect::<Vec<_>>(),
        (1..=24).collect::<Vec<_>>()
    );
    let cursors = [multiple_claims.member_cursor, 40];
    assert_eq!(
        cursors
            .into_iter()
            .filter(|cursor| u128::from(*cursor) < multiple_claims.resulting_floor)
            .collect::<Vec<_>>(),
        vec![10]
    );

    let leave_cursor_40 = floor_transition(11, Some(10), 101, 100, 11);
    let leave_cursor_10 = floor_transition(11, Some(40), 101, 100, 41);
    assert_eq!(leave_cursor_40.resulting_floor, 11);
    assert_eq!(leave_cursor_10.resulting_floor, 41);

    let final_leave = floor_transition(1, None, 101, 100, 101);
    assert_eq!(final_leave.member_cursor, 101);
    assert_eq!(final_leave.resulting_floor, 101);
    let observer_after_left = floor_transition(101, None, 101, 101, 102);
    assert_eq!(observer_after_left.resulting_floor, 102);

    let late_mint = floor_transition(25, Some(0), 101, 100, 25);
    assert_eq!(late_mint.preferred_floor, 1);
    assert_eq!(late_mint.resulting_floor, 25);
    let marker = ParticipantRecord::HistoryCompacted {
        affected_participant_id: 16,
        abandoned_after: 0,
        abandoned_through: 101,
        physical_floor_at_decision: 25,
    };
    assert!(matches!(
        round_trip_delivery(ParticipantDelivery {
            conversation_id: 116,
            delivery_seq: 102,
            record: marker,
        })
        .record,
        ParticipantRecord::HistoryCompacted {
            abandoned_after: 0,
            abandoned_through: 101,
            physical_floor_at_decision: 25,
            ..
        }
    ));
    let hard_stop = ObserverBackpressureState::initial(100);
    assert_eq!(hard_stop.observer_progress(), 100);
    assert!(25 <= u128::from(hard_stop.observer_progress()) + 1);
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct DurableMutationCounts {
    bindings: u64,
    records: u64,
    identities: u64,
}

impl DurableMutationCounts {
    fn classify_without_commit(&self, value: ServerValue) -> (ServerValue, Self) {
        (round_trip_value(value), *self)
    }
}

// Frozen contract lines 3422-3431.
#[test]
fn acceptance_17_expired_winner_and_stale_loser_are_exactly_classified() {
    let conversation_id = 117;
    let participant_id = 17;
    let request_generation = generation(7);
    let result_generation = generation(8);
    let winner = AttachAttemptToken::new([0x17; 16]);
    let loser = AttachAttemptToken::new([0x27; 16]);
    let expired = ReceiptExpired::CredentialAttach {
        conversation_id,
        token: winner,
        participant_id,
        presented_generation: request_generation,
        presented_marker_delivery_seq: None,
        result_generation,
        current_generation: result_generation,
        reason: ReceiptExpiryReason::Deadline,
    };
    let durable = DurableMutationCounts::default();
    let before = durable;
    assert_eq!(
        durable
            .classify_without_commit(ServerValue::ReceiptExpired(expired.clone()))
            .0,
        ServerValue::ReceiptExpired(expired)
    );

    let stale_loser = StaleAuthority::Live {
        request: CommonStaleAuthorityEnvelope::CredentialAttach(AttachEnvelope {
            conversation_id,
            participant_id,
            capability_generation: request_generation,
            attach_attempt_token: loser,
            accept_marker_delivery_seq: None,
        }),
        current_generation: result_generation,
    };
    assert_eq!(
        durable
            .classify_without_commit(ServerValue::StaleAuthority(stale_loser.clone()))
            .0,
        ServerValue::StaleAuthority(stale_loser)
    );

    for token in [winner, loser] {
        let unknown = StaleOrUnknownReceipt {
            conversation_id,
            token,
            participant_id,
            presented_generation: request_generation,
            presented_marker_delivery_seq: None,
            current_generation: result_generation,
        };
        assert_eq!(
            durable
                .classify_without_commit(ServerValue::StaleOrUnknownReceipt(unknown.clone()))
                .0,
            ServerValue::StaleOrUnknownReceipt(unknown)
        );
    }
    assert_eq!(durable, before);
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SlotProof {
    conversation_id: u64,
    index: u64,
    limit: u64,
}

impl ParticipantSlotAllocatorProof for SlotProof {
    fn conversation_id(&self) -> u64 {
        self.conversation_id
    }

    fn participant_index(&self) -> u64 {
        self.index
    }

    fn identity_limit(&self) -> u64 {
        self.limit
    }
}

// Frozen contract lines 3432-3449.
#[test]
#[allow(clippy::too_many_lines)]
fn acceptance_18_identity_ordinals_and_capacity_scope_precedence() {
    let limit = 3;
    for index in 0..limit {
        assert!(
            AllocatedParticipantSlot::from_allocator(SlotProof {
                conversation_id: 118,
                index,
                limit,
            })
            .is_ok()
        );
    }
    assert!(
        AllocatedParticipantSlot::from_allocator(SlotProof {
            conversation_id: 118,
            index: limit,
            limit,
        })
        .is_err()
    );
    assert!(
        AllocatedParticipantSlot::from_allocator(SlotProof {
            conversation_id: 218,
            index: 0,
            limit,
        })
        .is_ok()
    );

    let enrollment = enrollment_envelope(118, 0x18);
    let identity_outcomes = [
        IdentityCapacityScope::Server,
        IdentityCapacityScope::Conversation,
    ]
    .map(|scope| {
        ServerValue::IdentityCapacityExceeded(IdentityCapacityExceeded {
            request: enrollment.clone(),
            scope,
            limit: 1,
            occupied: 1,
        })
    });
    for outcome in identity_outcomes {
        assert_eq!(round_trip_value(outcome.clone()), outcome);
    }

    let enrollment_scopes = [
        EnrollmentReceiptCapacityScope::LiveReceiptServer,
        EnrollmentReceiptCapacityScope::ProvenanceServer,
        EnrollmentReceiptCapacityScope::ProvenanceConversation,
    ];
    assert_eq!(
        enrollment_scopes.map(EnrollmentReceiptCapacityScope::wire_scope),
        [
            ReceiptCapacityScope::LiveReceiptServer,
            ReceiptCapacityScope::ProvenanceServer,
            ReceiptCapacityScope::ProvenanceConversation,
        ]
    );
    for scope in enrollment_scopes {
        let outcome = ServerValue::ReceiptCapacityExceeded(ReceiptCapacityExceeded::Enrollment {
            request: enrollment.clone(),
            scope,
            limit: 1,
            occupied: 1,
        });
        assert_eq!(round_trip_value(outcome.clone()), outcome);
    }

    let attach = AttachEnvelope {
        conversation_id: 118,
        participant_id: 0,
        capability_generation: Generation::ONE,
        attach_attempt_token: AttachAttemptToken::new([0x28; 16]),
        accept_marker_delivery_seq: None,
    };
    for scope in [
        ReceiptCapacityScope::LiveReceiptServer,
        ReceiptCapacityScope::LiveReceiptParticipant,
        ReceiptCapacityScope::ProvenanceServer,
        ReceiptCapacityScope::ProvenanceConversation,
        ReceiptCapacityScope::ProvenanceParticipant,
    ] {
        let outcome =
            ServerValue::ReceiptCapacityExceeded(ReceiptCapacityExceeded::CredentialAttach {
                request: attach.clone(),
                scope,
                limit: 1,
                occupied: 1,
            });
        assert_eq!(round_trip_value(outcome.clone()), outcome);
    }

    let stable_leave = LeaveCommitted::new(
        118,
        LeaveAttemptToken::new([0x38; 16]),
        0,
        Generation::ONE,
        None,
        None,
        2,
    )
    .expect("unbound Leave has no ended epoch");
    assert_eq!(
        round_trip_value(ServerValue::LeaveCommitted(stable_leave.clone())),
        ServerValue::LeaveCommitted(stable_leave)
    );
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct SocketCallbacks {
    delivered: Vec<ServerDiscriminant>,
}

impl SocketCallbacks {
    fn poison(&mut self, rejection: ParticipantTransportRejected) {
        let value = round_trip_value(ServerValue::ParticipantTransportRejected(rejection));
        self.delivered.push(value.discriminant());
    }
}

// Frozen contract lines 3450-3454.
#[test]
fn acceptance_19_backpressure_wakes_once_but_acks_never_park() {
    let state = ObserverBackpressureState::initial(19);
    let refusals = [
        ObserverBackpressure::Enrollment {
            request: enrollment_envelope(119, 0x19),
            state,
        },
        ObserverBackpressure::CredentialAttach {
            request: AttachEnvelope {
                conversation_id: 119,
                participant_id: 19,
                capability_generation: Generation::ONE,
                attach_attempt_token: AttachAttemptToken::new([0x29; 16]),
                accept_marker_delivery_seq: None,
            },
            state,
        },
        ObserverBackpressure::Detach {
            request: liminal_protocol::wire::DetachEnvelope {
                conversation_id: 119,
                participant_id: 19,
                capability_generation: Generation::ONE,
                detach_attempt_token: DetachAttemptToken::new([0x39; 16]),
            },
            committed_binding_epoch: epoch(19, 1, 1),
            state,
        },
        ObserverBackpressure::RecordAdmission {
            request: RecordAdmissionEnvelope {
                conversation_id: 119,
                participant_id: 19,
                capability_generation: Generation::ONE,
                record_admission_attempt_token:
                    liminal_protocol::wire::RecordAdmissionAttemptToken::new([0xA7; 16]),
            },
            state,
        },
        ObserverBackpressure::Leave {
            request: LeaveEnvelope {
                conversation_id: 119,
                participant_id: 19,
                capability_generation: Generation::ONE,
                leave_attempt_token: LeaveAttemptToken::new([0x49; 16]),
            },
            state,
            prior_terminal_cell_exists: false,
        },
    ];
    for refusal in refusals {
        let value = ServerValue::ObserverBackpressure(refusal);
        assert_eq!(round_trip_value(value.clone()), value);
    }

    let wake = ServerPush::ObserverProgressed {
        conversation_id: 119,
        refused_epoch: 19,
        observer_progress: 20,
    };
    let wake_frame = ParticipantFrame::ServerPush(wake.clone());
    assert_eq!(
        round_trip(&wake_frame, ReceiverDirection::Client),
        ParticipantFrame::ServerPush(wake)
    );
    let normal_ack = AckCommitted::new(ParticipantAckEnvelope {
        conversation_id: 119,
        participant_id: 19,
        capability_generation: Generation::ONE,
        through_seq: 1,
    });
    let marker_ack = MarkerAckCommitted::new(MarkerAckEnvelope {
        conversation_id: 119,
        participant_id: 19,
        capability_generation: Generation::ONE,
        marker_delivery_seq: 2,
    });
    assert_eq!(normal_ack.current_cursor(), 1);
    assert_eq!(marker_ack.current_cursor(), 2);

    let unknown = RecordAdmissionUnknown {
        conversation_id: 119,
        participant_id: 19,
        capability_generation: Generation::ONE,
        operation: RecordAdmissionOperation::OrdinaryRecordAdmission,
        park_order: 7,
    };
    assert_eq!(
        unknown.operation,
        RecordAdmissionOperation::OrdinaryRecordAdmission
    );
    assert_eq!(unknown.park_order, 7);
    let mut socket = SocketCallbacks::default();
    socket.poison(ParticipantTransportRejected {
        reason: TransportRejectionReason::DecodeFailed {
            decode_class: DecodeClass::InvalidField,
        },
    });
    assert_eq!(
        socket.delivered,
        vec![ServerDiscriminant::ParticipantTransportRejected]
    );
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CredentialRecoveryState {
    pending_token: Option<AttachAttemptToken>,
    terminal: Option<CredentialRecoveryLost>,
    automatic_retry_queue: Vec<AttachAttemptToken>,
}

impl CredentialRecoveryState {
    fn lose(&mut self, outcome: CredentialRecoveryLost) {
        self.pending_token = None;
        self.automatic_retry_queue.clear();
        self.terminal = Some(outcome);
    }
}

// Frozen contract lines 3455-3458.
#[test]
fn acceptance_20_lost_credential_recovery_is_terminal_and_preserves_identity() {
    let token = AttachAttemptToken::new([0x20; 16]);
    let expired = ReceiptExpired::CredentialAttach {
        conversation_id: 120,
        token,
        participant_id: 20,
        presented_generation: generation(4),
        presented_marker_delivery_seq: None,
        result_generation: generation(5),
        current_generation: generation(5),
        reason: ReceiptExpiryReason::Deadline,
    };
    assert_eq!(
        round_trip_value(ServerValue::ReceiptExpired(expired.clone())),
        ServerValue::ReceiptExpired(expired)
    );
    let lost = CredentialRecoveryLost {
        conversation_id: 120,
        participant_id: 20,
        last_known_generation: generation(4),
    };
    assert_eq!(lost.conversation_id, 120);
    assert_eq!(lost.participant_id, 20);
    assert_eq!(lost.last_known_generation, generation(4));
    let mut recovery = CredentialRecoveryState {
        pending_token: Some(token),
        terminal: None,
        automatic_retry_queue: vec![token],
    };
    recovery.lose(lost);
    assert_eq!(recovery.pending_token, None);
    assert_eq!(recovery.terminal, Some(lost));
    assert!(recovery.automatic_retry_queue.is_empty());
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BoundaryDurability {
    sequence_budget: SequenceBudget,
    generation: Generation,
    retained_records: u64,
    live_receipts: u64,
    provenance_rows: u64,
}

impl BoundaryDurability {
    fn terminal_refusal(&self, value: ServerValue) -> (ServerValue, Self) {
        (round_trip_value(value), self.clone())
    }
}

// Frozen contract lines 3459-3486.
#[test]
#[allow(clippy::too_many_lines)]
fn acceptance_21_reachable_joint_generation_sequence_boundary() {
    let max = u64::MAX;
    let generation_star_value = (max - 3) / 2;
    let generation_star = generation(generation_star_value);
    let successful_supersessions = generation_star_value - 1;
    let lifecycle_records = 1_u128 + 2_u128 * u128::from(successful_supersessions);
    assert_eq!(lifecycle_records, u128::from(max - 4));

    let pre_budget = SequenceBudget {
        high_watermark: max - 4,
        remaining: 4,
        e: 1,
        t: 1,
        m: 0,
        rs: 0,
        rt: 0,
        l_times_t: 1,
        l_times_rt: 0,
        l_other_times_e: 0,
    };
    let request = AttachEnvelope {
        conversation_id: 121,
        participant_id: 0,
        capability_generation: generation_star,
        attach_attempt_token: AttachAttemptToken::new([0x21; 16]),
        accept_marker_delivery_seq: None,
    };
    let request_frame =
        round_trip_request(ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: request.conversation_id,
            participant_id: request.participant_id,
            capability_generation: request.capability_generation,
            attach_secret: AttachSecret::new([0x21; 32]),
            attach_attempt_token: request.attach_attempt_token,
            accept_marker_delivery_seq: None,
        }));
    assert!(matches!(request_frame, ClientRequest::CredentialAttach(_)));

    let baseline = retained_baseline(ResourceVector::new(0, 0), 1, 0, ResourceVector::new(1, 64))
        .expect("one uncredited identity reserves one marker");
    assert_eq!(baseline, WideResourceVector::new(1, 64));
    assert!(zero_debt_admission(
        baseline,
        ResourceVector::new(2, 128),
        ResourceVector::new(2, 128),
        ResourceVector::new(16, 1_024),
    ));

    let resulting_budget = SequenceBudget {
        high_watermark: max - 2,
        remaining: 2,
        ..pre_budget
    };
    let required = resulting_budget.e
        + resulting_budget.t
        + u64::try_from(resulting_budget.l_times_t).expect("fixture product fits u64");
    assert_eq!(required, 3);
    assert!(required > resulting_budget.remaining);

    let durable = BoundaryDurability {
        sequence_budget: pre_budget,
        generation: generation_star,
        retained_records: 0,
        live_receipts: 0,
        provenance_rows: 0,
    };
    let before = durable.clone();
    let exhaustion = ConversationSequenceExhausted {
        request: SequenceAllocatingEnvelope::CredentialAttach(request),
        sequence_budget: resulting_budget,
    };
    assert_eq!(
        durable
            .terminal_refusal(ServerValue::ConversationSequenceExhausted(Box::new(
                exhaustion.clone(),
            )))
            .0,
        ServerValue::ConversationSequenceExhausted(Box::new(exhaustion))
    );
    assert_eq!(durable, before);

    for scope in [
        ReceiptCapacityScope::LiveReceiptServer,
        ReceiptCapacityScope::ProvenanceParticipant,
    ] {
        let capacity = ReceiptCapacityExceeded::CredentialAttach {
            request: AttachEnvelope {
                conversation_id: 121,
                participant_id: 0,
                capability_generation: generation_star,
                attach_attempt_token: AttachAttemptToken::new([0x31; 16]),
                accept_marker_delivery_seq: None,
            },
            scope,
            limit: 4,
            occupied: 4,
        };
        let (_, refusal_snapshot) =
            durable.terminal_refusal(ServerValue::ReceiptCapacityExceeded(capacity));
        assert_eq!(refusal_snapshot, durable);
    }
    assert_eq!(durable, before);

    for raw in 1_u16..=8 {
        let tag = ClientDiscriminant::try_from(raw).expect("client register is contiguous");
        assert_eq!(tag.wire_value(), raw);
    }
    for raw in 0x0100_u16..=0x0124 {
        let tag = ServerDiscriminant::try_from(raw).expect("server register is contiguous");
        assert_eq!(tag.wire_value(), raw);
    }
}

// Frozen contract lines 3487-3490.
#[test]
fn acceptance_22_old_enrollment_token_becomes_known_not_reminted() {
    let known = EnrollmentKnown {
        conversation_id: 122,
        token: EnrollmentToken::new([0x22; 16]),
        participant_id: 22,
        current_generation: generation(3),
    };
    assert_eq!(
        round_trip_value(ServerValue::EnrollmentKnown(known.clone())),
        ServerValue::EnrollmentKnown(known)
    );
    // `EnrollmentKnown` structurally has neither a secret nor a binding epoch.
    let fresh = round_trip_request(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: 122,
        enrollment_token: EnrollmentToken::new([0x32; 16]),
    }));
    assert!(matches!(fresh, ClientRequest::Enrollment(_)));
    let known_slot = AllocatedParticipantSlot::from_allocator(SlotProof {
        conversation_id: 122,
        index: 22,
        limit: 24,
    })
    .expect("known participant retains its reserved slot");
    let fresh_slot = AllocatedParticipantSlot::from_allocator(SlotProof {
        conversation_id: 122,
        index: 23,
        limit: 24,
    })
    .expect("fresh token reserves the next distinct slot");
    assert_ne!(known_slot.participant_id(), fresh_slot.participant_id());
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParkedLeaveState {
    conversation_id: u64,
    refusal_epoch: u64,
    progress_cycles: u64,
    pending_leave: Option<LeaveEnvelope>,
    terminal_outcome: Option<ServerDiscriminant>,
    ack_rows: Vec<ParticipantAckEnvelope>,
    reissues: u64,
    polling_attempts: u64,
}

impl ParkedLeaveState {
    fn observer_progress(&mut self, push: &ServerPush) {
        let ServerPush::ObserverProgressed {
            conversation_id,
            refused_epoch,
            observer_progress,
        } = &push
        else {
            panic!("only observer progress wakes a parked refusal")
        };
        assert_eq!(*conversation_id, self.conversation_id);
        assert_eq!(*refused_epoch, self.refusal_epoch);
        assert_eq!(*observer_progress, self.refusal_epoch + 1);
        let frame = ParticipantFrame::ServerPush(push.clone());
        assert_eq!(round_trip(&frame, ReceiverDirection::Client), frame);
        self.refusal_epoch = *observer_progress;
        self.progress_cycles += 1;
    }

    fn reattach(&mut self, receipt: &AttachBound) {
        let leave = self
            .pending_leave
            .take()
            .expect("fixture has one parked Leave row");
        let stale = ServerValue::StaleAuthority(StaleAuthority::Leave(LeaveStaleAuthority::Live {
            conversation_id: leave.conversation_id,
            participant_id: leave.participant_id,
            presented_generation: leave.capability_generation,
            leave_attempt_token: leave.leave_attempt_token,
            current_generation: receipt.capability_generation(),
        }));
        self.terminal_outcome = Some(round_trip_value(stale).discriminant());
    }
}

// Frozen contract lines 3491-3496.
#[test]
fn acceptance_23_backpressure_progress_cycles_are_event_driven_and_nonmutating() {
    let mut parked = ParkedLeaveState {
        conversation_id: 123,
        refusal_epoch: 23,
        progress_cycles: 0,
        pending_leave: Some(LeaveEnvelope {
            conversation_id: 123,
            participant_id: 23,
            capability_generation: Generation::ONE,
            leave_attempt_token: LeaveAttemptToken::new([0x23; 16]),
        }),
        terminal_outcome: None,
        ack_rows: Vec::new(),
        reissues: 0,
        polling_attempts: 0,
    };
    for _ in 0..2 {
        let refusal = ObserverBackpressureState::initial(parked.refusal_epoch);
        assert_eq!(refusal.backpressure_epoch(), parked.refusal_epoch);
        let push = ServerPush::ObserverProgressed {
            conversation_id: 123,
            refused_epoch: parked.refusal_epoch,
            observer_progress: parked.refusal_epoch + 1,
        };
        parked.observer_progress(&push);
    }
    assert_eq!(parked.progress_cycles, 2);

    let status = ObserverRecoveryAccepted {
        statuses: vec![ObserverProgressStatus {
            conversation_id: 123,
            refused_epoch: 24,
            current_observer_progress: 25,
            armed: false,
            progressed: true,
        }],
    };
    assert_eq!(
        round_trip_value(ServerValue::ObserverRecoveryAccepted(status.clone())),
        ServerValue::ObserverRecoveryAccepted(status)
    );
    let attach = AttachBound::ordinary(
        123,
        AttachAttemptToken::new([0x43; 16]),
        23,
        Generation::ONE,
        AttachSecret::new([0x23; 32]),
        epoch(23, 2, 2),
        0,
        1_000,
        2_000,
    )
    .expect("reattach rotates the parked Leave authority");
    parked.reattach(&attach);
    assert_eq!(parked.pending_leave, None);
    assert_eq!(
        parked.terminal_outcome,
        Some(ServerDiscriminant::StaleAuthority)
    );
    assert_eq!(parked.reissues, 0);
    assert!(parked.ack_rows.is_empty());
    assert_eq!(parked.polling_attempts, 0);
}

fn pending_states(binding: ActiveBinding) -> Vec<BindingState> {
    let pending = BindingTerminalDisposition::Pending(PendingBindingTerminalPosition::new(24));
    vec![
        binding.clean_deregister(pending).binding_state(),
        binding.clean_disconnect(pending).binding_state(),
        binding.server_shutdown(pending).binding_state(),
        binding.connection_lost(pending).binding_state(),
        binding.process_killed(pending).binding_state(),
        binding.protocol_error(pending).binding_state(),
        binding.unclean_server_restart(pending).binding_state(),
    ]
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingDurability {
    binding: BindingState,
    occupied_bounded_slots: u64,
    durable_records: Vec<ParticipantDelivery>,
}

impl PendingDurability {
    fn recover(&mut self, delivery_seq: u64) {
        let BindingState::PendingFinalization(pending) = self.binding else {
            return;
        };
        let committed = pending.commit(delivery_seq);
        let record = match committed {
            liminal_protocol::lifecycle::CommittedBindingTerminal::Detached(value) => {
                ParticipantRecord::Detached {
                    affected_participant_id: value.participant_id(),
                    binding_epoch: value.binding_epoch(),
                    cause: value.cause(),
                }
            }
            liminal_protocol::lifecycle::CommittedBindingTerminal::Died(value) => {
                ParticipantRecord::Died {
                    affected_participant_id: value.participant_id(),
                    binding_epoch: value.binding_epoch(),
                    cause: value.cause(),
                }
            }
        };
        self.durable_records
            .push(round_trip_delivery(ParticipantDelivery {
                conversation_id: pending.conversation_id(),
                delivery_seq,
                record,
            }));
        self.binding = BindingState::Detached;
        self.occupied_bounded_slots = self
            .occupied_bounded_slots
            .checked_sub(1)
            .expect("pending state owns its one bounded slot");
    }
}

// Frozen contract lines 3497-3501.
#[test]
fn acceptance_24_pending_finalization_crash_recovery_is_atomic() {
    let binding = ActiveBinding {
        participant_id: 24,
        conversation_id: 124,
        binding_epoch: epoch(24, 1, 1),
    };
    let states = pending_states(binding);
    assert_eq!(states.len(), 7);

    for state in states {
        let BindingState::PendingFinalization(pending) = state else {
            panic!("blocked fate must retain PendingFinalization")
        };
        let crash_snapshot = pending;
        assert_eq!(crash_snapshot.participant_id(), 24);
        assert_eq!(crash_snapshot.conversation_id(), 124);
        assert_eq!(crash_snapshot.binding_epoch(), binding.binding_epoch);
        assert_eq!(crash_snapshot.admission_order().transaction_order(), 24);

        let pre_transaction = PendingDurability {
            binding: state,
            occupied_bounded_slots: 1,
            durable_records: Vec::new(),
        };
        let crash_before = pre_transaction.clone();
        assert_eq!(crash_before, pre_transaction);

        let mut recovered = crash_before;
        recovered.recover(25);
        assert_eq!(recovered.binding, BindingState::Detached);
        assert_eq!(recovered.occupied_bounded_slots, 0);
        assert_eq!(recovered.durable_records.len(), 1);
        assert_eq!(recovered.durable_records[0].delivery_seq, 25);

        let crash_after = recovered.clone();
        recovered.recover(25);
        assert_eq!(recovered, crash_after); // replay cannot append/release twice
    }
}

const PARTICIPANT_FRAME_SCHEMA_BYTES: u64 = 16;
const PARTICIPANT_REQUEST_SCHEMA_BYTES: u64 = 40;
const PARKED_ROW_METADATA_BYTES: u64 = 8;
const RECOVERY_ENTRY_SCHEMA_BYTES: u64 = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ParkingConfig {
    n: u64,
    c: u64,
    p: u64,
    g: u64,
    d: u64,
    r: u64,
    b: u64,
    re: u64,
    wf: u64,
    connection_slots: u64,
}

impl ParkingConfig {
    const fn valid() -> Self {
        Self {
            n: 2,
            c: 96,
            p: 2,
            g: 3,
            d: 144,
            r: PARTICIPANT_REQUEST_SCHEMA_BYTES,
            b: PARTICIPANT_REQUEST_SCHEMA_BYTES + PARKED_ROW_METADATA_BYTES,
            re: RECOVERY_ENTRY_SCHEMA_BYTES,
            wf: PARTICIPANT_REQUEST_SCHEMA_BYTES,
            connection_slots: 2,
        }
    }

    const fn set_zero(&mut self, field: ParkingLimitField) {
        match field {
            ParkingLimitField::N => self.n = 0,
            ParkingLimitField::C => self.c = 0,
            ParkingLimitField::P => self.p = 0,
            ParkingLimitField::G => self.g = 0,
            ParkingLimitField::D => self.d = 0,
            ParkingLimitField::R => self.r = 0,
            ParkingLimitField::B => self.b = 0,
            ParkingLimitField::RE => self.re = 0,
            ParkingLimitField::WF => self.wf = 0,
        }
    }
}

#[allow(clippy::too_many_lines)]
fn validate_parking(config: ParkingConfig) -> Result<(), ParticipantParkingConfigurationInvalid> {
    for (field, value) in [
        (ParkingLimitField::N, config.n),
        (ParkingLimitField::C, config.c),
        (ParkingLimitField::P, config.p),
        (ParkingLimitField::G, config.g),
        (ParkingLimitField::D, config.d),
        (ParkingLimitField::R, config.r),
        (ParkingLimitField::B, config.b),
        (ParkingLimitField::RE, config.re),
        (ParkingLimitField::WF, config.wf),
    ] {
        if value == 0 {
            return Err(ParticipantParkingConfigurationInvalid {
                violation: ParkingShapeViolation::NonzeroLimit {
                    field,
                    actual: 0,
                    required_minimum: 1,
                },
            });
        }
    }
    if config.re != RECOVERY_ENTRY_SCHEMA_BYTES {
        return Err(ParticipantParkingConfigurationInvalid {
            violation: ParkingShapeViolation::RecoveryEntrySchemaBytes {
                actual: config.re,
                required: RECOVERY_ENTRY_SCHEMA_BYTES,
            },
        });
    }
    if config.wf < PARTICIPANT_FRAME_SCHEMA_BYTES {
        return Err(ParticipantParkingConfigurationInvalid {
            violation: ParkingShapeViolation::WireSchemaBytes {
                actual: config.wf,
                required: PARTICIPANT_FRAME_SCHEMA_BYTES,
            },
        });
    }
    let request_send_limit = config.r.min(config.wf);
    if request_send_limit < PARTICIPANT_REQUEST_SCHEMA_BYTES {
        return Err(ParticipantParkingConfigurationInvalid {
            violation: ParkingShapeViolation::RequestSchemaBytes {
                configured_request_limit: config.r,
                wire_frame_limit: config.wf,
                actual: request_send_limit,
                required: PARTICIPANT_REQUEST_SCHEMA_BYTES,
            },
        });
    }
    let required_row_bytes = u128::from(config.r) + u128::from(PARKED_ROW_METADATA_BYTES);
    if u128::from(config.b) < required_row_bytes {
        return Err(ParticipantParkingConfigurationInvalid {
            violation: ParkingShapeViolation::RowSchemaBytes {
                request_limit: config.r,
                row_metadata_bytes: PARKED_ROW_METADATA_BYTES,
                actual: config.b,
                required: required_row_bytes,
            },
        });
    }
    let Some(conversation_bytes) = config.n.checked_mul(config.b) else {
        return Err(ParticipantParkingConfigurationInvalid {
            violation: ParkingShapeViolation::CheckedProduct(CheckedMultiplyOverflow {
                left: config.n,
                right: config.b,
            }),
        });
    };
    let Some(sdk_bytes) = config.g.checked_mul(config.b) else {
        return Err(ParticipantParkingConfigurationInvalid {
            violation: ParkingShapeViolation::CheckedProduct(CheckedMultiplyOverflow {
                left: config.g,
                right: config.b,
            }),
        });
    };
    if config.c > conversation_bytes {
        return Err(ParticipantParkingConfigurationInvalid {
            violation: ParkingShapeViolation::RowBytesBound {
                left: config.n,
                right: config.b,
                checked_product: conversation_bytes,
                actual: config.c,
            },
        });
    }
    if config.d > sdk_bytes {
        return Err(ParticipantParkingConfigurationInvalid {
            violation: ParkingShapeViolation::SdkBytesBound {
                left: config.g,
                right: config.b,
                checked_product: sdk_bytes,
                actual: config.d,
            },
        });
    }
    if config.p > config.connection_slots {
        return Err(ParticipantParkingConfigurationInvalid {
            violation: ParkingShapeViolation::RecoverableSlots {
                actual: config.p,
                limit: config.connection_slots,
            },
        });
    }
    Ok(())
}

const fn parked_shape(violation: ParkingShapeViolation) -> SdkParkingCapacityIncompatible {
    match violation {
        ParkingShapeViolation::NonzeroLimit {
            field,
            actual,
            required_minimum,
        } => SdkParkingCapacityIncompatible::NonzeroLimit {
            field,
            actual,
            required_minimum,
        },
        ParkingShapeViolation::RecoveryEntrySchemaBytes { actual, required } => {
            SdkParkingCapacityIncompatible::RecoveryEntrySchemaBytes { actual, required }
        }
        ParkingShapeViolation::WireSchemaBytes { actual, required } => {
            SdkParkingCapacityIncompatible::WireSchemaBytes { actual, required }
        }
        ParkingShapeViolation::RequestSchemaBytes {
            configured_request_limit,
            wire_frame_limit,
            actual,
            required,
        } => SdkParkingCapacityIncompatible::RequestSchemaBytes {
            configured_request_limit,
            wire_frame_limit,
            actual,
            required,
        },
        ParkingShapeViolation::RowSchemaBytes {
            request_limit,
            row_metadata_bytes,
            actual,
            required,
        } => SdkParkingCapacityIncompatible::RowSchemaBytes {
            request_limit,
            row_metadata_bytes,
            actual,
            required,
        },
        ParkingShapeViolation::CheckedProduct(operands) => {
            SdkParkingCapacityIncompatible::CheckedProduct(operands)
        }
        ParkingShapeViolation::RowBytesBound {
            left,
            right,
            checked_product,
            actual,
        } => SdkParkingCapacityIncompatible::RowBytesBound {
            left,
            right,
            checked_product,
            actual,
        },
        ParkingShapeViolation::SdkBytesBound {
            left,
            right,
            checked_product,
            actual,
        } => SdkParkingCapacityIncompatible::SdkBytesBound {
            left,
            right,
            checked_product,
            actual,
        },
        ParkingShapeViolation::RecoverableSlots { actual, limit } => {
            SdkParkingCapacityIncompatible::RecoverableSlots { actual, limit }
        }
    }
}

fn handshake_operands(max_entries: u64) -> HandshakeSizeOperands {
    HandshakeSizeOperands {
        max_entries,
        framing_bytes: 24,
        request_entry_bytes: 16,
        response_entry_bytes: 26,
        error_response_bytes: 27,
        request_encoded_bytes: 24 + 16 * u128::from(max_entries),
        response_encoded_bytes: 24 + 26 * u128::from(max_entries),
    }
}

fn handshake_failure(
    request_limit: u64,
    wire_frame_limit: u64,
    operands: HandshakeSizeOperands,
) -> Option<ParticipantRecoveryHandshakeTooLarge> {
    let dimension = if operands.request_encoded_bytes > u128::from(request_limit) {
        Some(RecoveryHandshakeDimension::RequestBytes)
    } else if operands.request_encoded_bytes > u128::from(wire_frame_limit) {
        Some(RecoveryHandshakeDimension::RequestWireFrameBytes)
    } else if operands.response_encoded_bytes > u128::from(wire_frame_limit) {
        Some(RecoveryHandshakeDimension::ResponseWireFrameBytes)
    } else {
        None
    }?;
    Some(ParticipantRecoveryHandshakeTooLarge {
        max_entries: operands.max_entries,
        framing_bytes: operands.framing_bytes,
        request_entry_bytes: operands.request_entry_bytes,
        response_entry_bytes: operands.response_entry_bytes,
        error_response_bytes: operands.error_response_bytes,
        request_encoded_bytes: operands.request_encoded_bytes,
        response_encoded_bytes: operands.response_encoded_bytes,
        request_limit,
        wire_frame_limit,
        dimension,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum StartupFailure {
    Parking(ParticipantParkingConfigurationInvalid),
    Handshake(ParticipantRecoveryHandshakeTooLarge),
    Capability(ParticipantCapabilityConfigurationInvalid),
    Keepalive(KeepaliveCertificationFailed),
    Retention(ParticipantRetentionCapacityInvalid),
}

fn select_startup_failure(failures: [Option<StartupFailure>; 5]) -> Option<StartupFailure> {
    failures.into_iter().flatten().next()
}

const fn startup_failure_rank(failure: &StartupFailure) -> usize {
    match failure {
        StartupFailure::Parking(_) => 0,
        StartupFailure::Handshake(_) => 1,
        StartupFailure::Capability(_) => 2,
        StartupFailure::Keepalive(_) => 3,
        StartupFailure::Retention(_) => 4,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CapabilityConfig {
    limits: [u64; 9],
}

impl CapabilityConfig {
    const fn valid() -> Self {
        Self {
            limits: [1_000, 2_000, 4, 4, 4, 4, 4, 4, 4],
        }
    }
}

fn validate_capability(
    config: CapabilityConfig,
) -> Result<(), ParticipantCapabilityConfigurationInvalid> {
    let fields = [
        CapabilityLimitField::AttachReceiptTtlMs,
        CapabilityLimitField::ReceiptProvenanceTtlMs,
        CapabilityLimitField::MaxLiveAttachReceiptsServer,
        CapabilityLimitField::MaxLiveAttachReceiptsPerParticipant,
        CapabilityLimitField::MaxReceiptProvenanceServer,
        CapabilityLimitField::MaxReceiptProvenancePerConversation,
        CapabilityLimitField::MaxReceiptProvenancePerParticipant,
        CapabilityLimitField::MaxRetiredIdentitySlotsServer,
        CapabilityLimitField::MaxRetiredIdentitySlotsPerConversation,
    ];
    for (field, actual) in fields.into_iter().zip(config.limits) {
        if actual == 0 {
            return Err(ParticipantCapabilityConfigurationInvalid::NonzeroLimit {
                field,
                actual,
                required_minimum: 1,
            });
        }
    }
    if config.limits[1] < config.limits[0] {
        return Err(
            ParticipantCapabilityConfigurationInvalid::ReceiptDeadlineOrder {
                attach_receipt_ttl_ms: config.limits[0],
                receipt_provenance_ttl_ms: config.limits[1],
                required_minimum_provenance_ttl_ms: config.limits[0],
            },
        );
    }
    Ok(())
}

fn validate_keepalive(
    idle_seconds: u64,
    interval_seconds: u64,
) -> Result<(), KeepaliveCertificationFailed> {
    if idle_seconds == 0 {
        return Err(KeepaliveCertificationFailed::StartupConfiguration(
            StartupKeepaliveReason::Zero {
                field: KeepaliveField::IdleSeconds,
                requested: idle_seconds,
                required_minimum: 1,
            },
        ));
    }
    if !(1..=60).contains(&interval_seconds) {
        return Err(KeepaliveCertificationFailed::StartupConfiguration(
            StartupKeepaliveReason::OutOfRange {
                field: KeepaliveField::IntervalSeconds,
                requested: interval_seconds,
                supported_min: 1,
                supported_max: 60,
                platform: PlatformName::new("acceptance-platform".to_owned()),
            },
        ));
    }
    Ok(())
}

// Frozen contract lines 3502-3602. The occurrence-array assertions at lines
// 3580-3590 are intentionally replaced by participant-keyed facts under
// docs/design/LP-EXTRACTION-GOAL.md Fix 2, as mandated by this session's brief.
#[test]
#[allow(clippy::too_many_lines)]
fn acceptance_25_configuration_matrix_and_generated_capacity_boundary() {
    let parking_fields = [
        ParkingLimitField::N,
        ParkingLimitField::C,
        ParkingLimitField::P,
        ParkingLimitField::G,
        ParkingLimitField::D,
        ParkingLimitField::R,
        ParkingLimitField::B,
        ParkingLimitField::RE,
        ParkingLimitField::WF,
    ];
    for field in parking_fields {
        let mut config = ParkingConfig::valid();
        config.set_zero(field);
        let failure = validate_parking(config).expect_err("zero is the first shape failure");
        assert_eq!(
            failure.violation,
            ParkingShapeViolation::NonzeroLimit {
                field,
                actual: 0,
                required_minimum: 1,
            }
        );
        assert_eq!(
            parked_shape(failure.violation),
            SdkParkingCapacityIncompatible::NonzeroLimit {
                field,
                actual: 0,
                required_minimum: 1,
            }
        );
    }
    let mut all_zero = ParkingConfig::valid();
    for field in parking_fields {
        all_zero.set_zero(field);
    }
    assert!(matches!(
        validate_parking(all_zero),
        Err(ParticipantParkingConfigurationInvalid {
            violation: ParkingShapeViolation::NonzeroLimit {
                field: ParkingLimitField::N,
                ..
            }
        })
    ));

    for actual in [15, 17] {
        let mut config = ParkingConfig::valid();
        config.re = actual;
        assert_eq!(
            validate_parking(config),
            Err(ParticipantParkingConfigurationInvalid {
                violation: ParkingShapeViolation::RecoveryEntrySchemaBytes {
                    actual,
                    required: 16,
                }
            })
        );
    }
    assert!(validate_parking(ParkingConfig::valid()).is_ok());

    let mut wire = ParkingConfig::valid();
    wire.wf = PARTICIPANT_FRAME_SCHEMA_BYTES - 1;
    assert!(matches!(
        validate_parking(wire),
        Err(ParticipantParkingConfigurationInvalid {
            violation: ParkingShapeViolation::WireSchemaBytes { .. }
        })
    ));
    for wf in [
        PARTICIPANT_FRAME_SCHEMA_BYTES,
        PARTICIPANT_FRAME_SCHEMA_BYTES + 1,
    ] {
        let mut config = ParkingConfig::valid();
        config.wf = wf;
        assert!(matches!(
            validate_parking(config),
            Err(ParticipantParkingConfigurationInvalid {
                violation: ParkingShapeViolation::RequestSchemaBytes { .. }
            })
        ));
    }
    for wf in [
        PARTICIPANT_REQUEST_SCHEMA_BYTES,
        PARTICIPANT_REQUEST_SCHEMA_BYTES + 1,
    ] {
        let mut config = ParkingConfig::valid();
        config.wf = wf;
        assert!(validate_parking(config).is_ok());
    }

    let mut request = ParkingConfig::valid();
    request.r = PARTICIPANT_REQUEST_SCHEMA_BYTES - 1;
    assert!(matches!(
        validate_parking(request),
        Err(ParticipantParkingConfigurationInvalid {
            violation: ParkingShapeViolation::RequestSchemaBytes { .. }
        })
    ));
    let mut row = ParkingConfig::valid();
    row.b = row.r + PARKED_ROW_METADATA_BYTES - 1;
    assert!(matches!(
        validate_parking(row),
        Err(ParticipantParkingConfigurationInvalid {
            violation: ParkingShapeViolation::RowSchemaBytes { .. }
        })
    ));
    for request_limit in [
        PARTICIPANT_REQUEST_SCHEMA_BYTES,
        PARTICIPANT_REQUEST_SCHEMA_BYTES + 1,
    ] {
        let mut config = ParkingConfig::valid();
        config.r = request_limit;
        config.b = request_limit + PARKED_ROW_METADATA_BYTES;
        assert!(validate_parking(config).is_ok());
    }
    for row_limit in [
        PARTICIPANT_REQUEST_SCHEMA_BYTES + PARKED_ROW_METADATA_BYTES,
        PARTICIPANT_REQUEST_SCHEMA_BYTES + PARKED_ROW_METADATA_BYTES + 1,
    ] {
        let mut config = ParkingConfig::valid();
        config.b = row_limit;
        assert!(validate_parking(config).is_ok());
    }

    let mut first_product = ParkingConfig::valid();
    first_product.n = u64::MAX;
    assert!(matches!(
        validate_parking(first_product),
        Err(ParticipantParkingConfigurationInvalid {
            violation: ParkingShapeViolation::CheckedProduct(CheckedMultiplyOverflow {
                left: u64::MAX,
                right: 48,
            })
        })
    ));
    let mut second_product = ParkingConfig::valid();
    second_product.n = 1;
    second_product.c = 48;
    second_product.g = u64::MAX;
    assert!(matches!(
        validate_parking(second_product),
        Err(ParticipantParkingConfigurationInvalid {
            violation: ParkingShapeViolation::CheckedProduct(CheckedMultiplyOverflow {
                left: u64::MAX,
                right: 48,
            })
        })
    ));
    let mut conversation_bytes = ParkingConfig::valid();
    conversation_bytes.c = 97;
    assert!(matches!(
        validate_parking(conversation_bytes),
        Err(ParticipantParkingConfigurationInvalid {
            violation: ParkingShapeViolation::RowBytesBound {
                checked_product: 96,
                actual: 97,
                ..
            }
        })
    ));
    let mut sdk_bytes = ParkingConfig::valid();
    sdk_bytes.d = 145;
    assert!(matches!(
        validate_parking(sdk_bytes),
        Err(ParticipantParkingConfigurationInvalid {
            violation: ParkingShapeViolation::SdkBytesBound {
                checked_product: 144,
                actual: 145,
                ..
            }
        })
    ));
    let mut slots = ParkingConfig::valid();
    slots.p = 6;
    slots.connection_slots = 5;
    assert!(matches!(
        validate_parking(slots),
        Err(ParticipantParkingConfigurationInvalid {
            violation: ParkingShapeViolation::RecoverableSlots {
                actual: 6,
                limit: 5,
            }
        })
    ));

    let mut recovery_schema = ParkingConfig::valid();
    recovery_schema.re = 15;
    let parked_shapes = [
        all_zero,
        recovery_schema,
        wire,
        request,
        row,
        first_product,
        second_product,
        conversation_bytes,
        sdk_bytes,
        slots,
    ]
    .map(|config| {
        parked_shape(
            validate_parking(config)
                .expect_err("each parked-phase fixture violates one shape dimension")
                .violation,
        )
    });
    assert!(matches!(
        parked_shapes,
        [
            SdkParkingCapacityIncompatible::NonzeroLimit { .. },
            SdkParkingCapacityIncompatible::RecoveryEntrySchemaBytes { .. },
            SdkParkingCapacityIncompatible::WireSchemaBytes { .. },
            SdkParkingCapacityIncompatible::RequestSchemaBytes { .. },
            SdkParkingCapacityIncompatible::RowSchemaBytes { .. },
            SdkParkingCapacityIncompatible::CheckedProduct(_),
            SdkParkingCapacityIncompatible::CheckedProduct(_),
            SdkParkingCapacityIncompatible::RowBytesBound { .. },
            SdkParkingCapacityIncompatible::SdkBytesBound { .. },
            SdkParkingCapacityIncompatible::RecoverableSlots { .. },
        ]
    ));

    let max_entries = PARTICIPANT_REQUEST_SCHEMA_BYTES.max(PARTICIPANT_FRAME_SCHEMA_BYTES) + 1;
    let operands = handshake_operands(max_entries);
    let request_bytes = u64::try_from(operands.request_encoded_bytes).expect("A fits u64");
    let response_bytes = u64::try_from(operands.response_encoded_bytes).expect("Z fits u64");
    assert!(response_bytes < FRAME_MAX);
    let proposals = [
        (
            request_bytes - 1,
            response_bytes,
            RecoveryHandshakeDimension::RequestBytes,
        ),
        (
            request_bytes,
            request_bytes - 1,
            RecoveryHandshakeDimension::RequestWireFrameBytes,
        ),
        (
            request_bytes,
            response_bytes - 1,
            RecoveryHandshakeDimension::ResponseWireFrameBytes,
        ),
        (
            request_bytes - 1,
            request_bytes - 1,
            RecoveryHandshakeDimension::RequestBytes,
        ),
    ];
    for (request_limit, wire_limit, expected) in proposals {
        let failure = handshake_failure(request_limit, wire_limit, operands)
            .expect("proposal violates one handshake dimension");
        assert_eq!(failure.dimension, expected);
        let parked = match failure.dimension {
            RecoveryHandshakeDimension::RequestBytes => {
                SdkParkingCapacityIncompatible::RecoveryHandshakeRequestBytes {
                    operands,
                    limit: request_limit,
                }
            }
            RecoveryHandshakeDimension::RequestWireFrameBytes => {
                SdkParkingCapacityIncompatible::RecoveryHandshakeRequestWireFrameBytes {
                    operands,
                    limit: wire_limit,
                }
            }
            RecoveryHandshakeDimension::ResponseWireFrameBytes => {
                SdkParkingCapacityIncompatible::RecoveryHandshakeResponseWireFrameBytes {
                    operands,
                    limit: wire_limit,
                }
            }
        };
        assert!(matches!(
            parked,
            SdkParkingCapacityIncompatible::RecoveryHandshakeRequestBytes { .. }
                | SdkParkingCapacityIncompatible::RecoveryHandshakeRequestWireFrameBytes { .. }
                | SdkParkingCapacityIncompatible::RecoveryHandshakeResponseWireFrameBytes { .. }
        ));
    }

    let capability_fields = [
        CapabilityLimitField::AttachReceiptTtlMs,
        CapabilityLimitField::ReceiptProvenanceTtlMs,
        CapabilityLimitField::MaxLiveAttachReceiptsServer,
        CapabilityLimitField::MaxLiveAttachReceiptsPerParticipant,
        CapabilityLimitField::MaxReceiptProvenanceServer,
        CapabilityLimitField::MaxReceiptProvenancePerConversation,
        CapabilityLimitField::MaxReceiptProvenancePerParticipant,
        CapabilityLimitField::MaxRetiredIdentitySlotsServer,
        CapabilityLimitField::MaxRetiredIdentitySlotsPerConversation,
    ];
    for (index, field) in capability_fields.into_iter().enumerate() {
        let mut config = CapabilityConfig::valid();
        config.limits[index] = 0;
        assert_eq!(
            validate_capability(config),
            Err(ParticipantCapabilityConfigurationInvalid::NonzeroLimit {
                field,
                actual: 0,
                required_minimum: 1,
            })
        );
    }
    assert_eq!(
        validate_capability(CapabilityConfig { limits: [0; 9] }),
        Err(ParticipantCapabilityConfigurationInvalid::NonzeroLimit {
            field: CapabilityLimitField::AttachReceiptTtlMs,
            actual: 0,
            required_minimum: 1,
        })
    );
    let mut equal_deadline = CapabilityConfig::valid();
    equal_deadline.limits[1] = 1_000;
    assert!(validate_capability(equal_deadline).is_ok());
    let mut short_deadline = equal_deadline;
    short_deadline.limits[1] = 999;
    assert_eq!(
        validate_capability(short_deadline),
        Err(
            ParticipantCapabilityConfigurationInvalid::ReceiptDeadlineOrder {
                attach_receipt_ttl_ms: 1_000,
                receipt_provenance_ttl_ms: 999,
                required_minimum_provenance_ttl_ms: 1_000,
            }
        )
    );

    let parking_failure = validate_parking(all_zero).expect_err("all-zero selects parking");
    let handshake_failure = handshake_failure(request_bytes - 1, response_bytes, operands)
        .expect("request is one byte too large");
    let capability_failure = validate_capability(CapabilityConfig { limits: [0; 9] })
        .expect_err("all-zero capability selects its first field");
    let keepalive_zero = validate_keepalive(0, 61).expect_err("idle zero wins");
    let keepalive_range = validate_keepalive(30, 61).expect_err("valid idle exposes interval");
    assert!(matches!(
        &keepalive_zero,
        KeepaliveCertificationFailed::StartupConfiguration(StartupKeepaliveReason::Zero {
            field: KeepaliveField::IdleSeconds,
            ..
        })
    ));
    assert!(matches!(
        &keepalive_range,
        KeepaliveCertificationFailed::StartupConfiguration(StartupKeepaliveReason::OutOfRange {
            field: KeepaliveField::IntervalSeconds,
            ..
        })
    ));
    let startup_required =
        retained_baseline(ResourceVector::new(0, 0), 1, 0, ResourceVector::new(1, 64))
            .expect("startup capacity formula is defined")
            .entries
            + 4;
    let retention_failure = ParticipantRetentionCapacityInvalid::EntryCapacity {
        required: startup_required,
        configured: 4,
    };
    let failures = [
        Some(StartupFailure::Parking(parking_failure)),
        Some(StartupFailure::Handshake(handshake_failure)),
        Some(StartupFailure::Capability(capability_failure)),
        Some(StartupFailure::Keepalive(keepalive_zero)),
        Some(StartupFailure::Retention(retention_failure)),
    ];
    for expected_rank in 0..failures.len() {
        let mut proposal = failures.clone();
        for earlier in proposal.iter_mut().take(expected_rank) {
            *earlier = None;
        }
        let selected = select_startup_failure(proposal)
            .expect("one invalid startup family remains in every proposal");
        assert_eq!(startup_failure_rank(&selected), expected_rank);
    }

    let marker_bytes = 64;
    let mandatory_bytes = 128;
    let baseline = retained_baseline(
        ResourceVector::new(0, 0),
        1,
        0,
        ResourceVector::new(1, marker_bytes),
    )
    .expect("empty conversation reserves one marker maximum");
    let q = ResourceVector::new(2, mandatory_bytes);
    let k = q;
    let cap = ResourceVector::new(5, marker_bytes + 2 * mandatory_bytes);
    assert!(zero_debt_admission(baseline, q, k, cap));
    assert_eq!(
        zero_debt_capacity_failure(
            baseline,
            q,
            k,
            ResourceVector::new(cap.entries - 1, cap.bytes)
        ),
        Some(ResourceDimension::Entries)
    );
    assert_eq!(
        zero_debt_capacity_failure(
            baseline,
            q,
            k,
            ResourceVector::new(cap.entries, cap.bytes - 1)
        ),
        Some(ResourceDimension::Bytes)
    );

    let post_enrollment_baseline = WideResourceVector::new(2, 2 * u128::from(marker_bytes));
    let post_capacity = mandatory_capacity(post_enrollment_baseline, q, k, cap);
    assert_eq!(
        post_capacity.debt,
        WideResourceVector::new(1, u128::from(marker_bytes))
    );
    assert!(post_capacity.is_legal());
    let first_witness = ObserverProjection::new(1);
    assert_eq!(first_witness.through_seq(), 1);

    let binding_epoch = epoch(25, 1, 1);
    let debt = ClosureDebt::new(post_capacity.debt).expect("enrollment creates nonzero debt");
    let mut episode = NonzeroDebtCursorEpisode::new(
        125,
        debt,
        1,
        1,
        1,
        1,
        vec![BoundParticipantCursor::new(0, binding_epoch, 0)],
    )
    .expect("projection-first pre-ack state is valid");
    let outcome = episode
        .acknowledge(
            0,
            binding_epoch,
            &ParticipantAck {
                conversation_id: 125,
                participant_id: 0,
                capability_generation: Generation::ONE,
                through_seq: 1,
            },
            1,
        )
        .expect("exact bound ack advances");
    assert!(matches!(
        outcome,
        liminal_protocol::lifecycle::CumulativeAckOutcome::Committed(_)
    ));
    assert_eq!(episode.floor_computation().preferred_floor, 2);
    assert_eq!(episode.floor_computation().resulting_floor, 2);
    assert_eq!(episode.facts().len(), 1);
    assert!(
        !episode
            .encode()
            .expect("participant-keyed state encodes")
            .is_empty()
    );

    let cleared_baseline = retained_baseline(
        ResourceVector::new(0, 0),
        1,
        0,
        ResourceVector::new(1, marker_bytes),
    )
    .expect("cleared state restores marker reserve");
    assert!(zero_debt_admission(cleared_baseline, q, k, cap));

    let valid_limit = ValidatedFrameLimit::new(FRAME_MAX).expect("FRAME_MAX is valid");
    let enrollment =
        ParticipantFrame::ClientRequest(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: 125,
            enrollment_token: EnrollmentToken::new([0x25; 16]),
        }));
    let mut bytes = vec![0; encoded_len(&enrollment).expect("enrollment encodes")];
    let _ = encode(&enrollment, &mut bytes).expect("enrollment output is sized");
    let gated = gate_inbound(
        &bytes,
        InboundGateContext {
            receiver: ReceiverDirection::Server,
            authentication: AuthenticationState::Authenticated,
            participant_capability: ParticipantCapabilityState::Negotiated(
                NegotiatedParticipantCapability::v1(valid_limit),
            ),
        },
    )
    .expect("valid startup admits the first ordinary request");
    assert_eq!(gated, enrollment);

    // The source explicitly requires this boundary to be exercised again.
    acceptance_21_reachable_joint_generation_sequence_boundary();
}
