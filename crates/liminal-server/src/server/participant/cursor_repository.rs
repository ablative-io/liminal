use std::sync::Arc;

use liminal::durability::{DurabilityError, DurableStore};
use liminal_protocol::algebra::WideResourceVector;
use liminal_protocol::lifecycle::{
    BoundParticipantCursor, ClosureDebt, CumulativeAckAuthorizationError, CumulativeAckOutcome,
    CursorEpisodeBuildError, NonzeroDebtCursorEpisode,
};
use liminal_protocol::wire::{
    BindingEpoch, ConnectionIncarnation, ConversationId, DeliverySeq, Generation, ParticipantAck,
    ParticipantIndex,
};

const EVENT_MAGIC: [u8; 4] = *b"LCRE";
const EVENT_VERSION: u8 = 1;
const START_TAG: u8 = 1;
const ACK_TAG: u8 = 2;
const READ_BATCH_SIZE: usize = 256;

/// Exact inputs from which `liminal-protocol` constructs a nonzero-debt cursor episode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CursorEpisodeStart {
    /// Conversation owning the retained suffix and participant cursors.
    pub conversation_id: ConversationId,
    /// Exact nonzero closure debt proving that the episode is active.
    pub debt: ClosureDebt,
    /// Durable hard-observer progress `o`.
    pub observer_progress: DeliverySeq,
    /// Candidate high watermark `H'` and inclusive retained-suffix end.
    pub candidate_high_watermark: DeliverySeq,
    /// Durable first-retained floor `F` before the episode begins.
    pub current_floor: u128,
    /// Mandatory-class capacity floor for the first append-free ack.
    pub cap_floor: u128,
    /// Bound participant cursors supplied to the shared episode constructor.
    pub participants: Vec<BoundParticipantCursor>,
}

impl CursorEpisodeStart {
    fn build(&self) -> Result<NonzeroDebtCursorEpisode, CursorRepositoryError> {
        NonzeroDebtCursorEpisode::new(
            self.conversation_id,
            self.debt,
            self.observer_progress,
            self.candidate_high_watermark,
            self.current_floor,
            self.cap_floor,
            self.participants.clone(),
        )
        .map_err(CursorRepositoryError::EpisodeBuild)
    }
}

/// One authority-bearing cumulative-ack command persisted for deterministic replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CursorAckCommand {
    /// Permanent participant index selecting the bound cursor.
    pub participant_index: ParticipantIndex,
    /// Binding epoch of the connection that received the request.
    pub receiving_binding_epoch: BindingEpoch,
    /// Exact participant acknowledgement decoded by the shared wire crate.
    pub request: ParticipantAck,
    /// Greatest sequence offered contiguously to this exact binding epoch.
    pub contiguously_available_through: DeliverySeq,
}

/// Failure to create, append to, or replay a participant cursor episode.
#[derive(Debug, thiserror::Error)]
pub enum CursorRepositoryError {
    /// The durable store rejected a read, append, or flush operation.
    #[error(transparent)]
    Durability(#[from] DurabilityError),
    /// The shared protocol rejected persisted episode-start inputs.
    #[error("shared cursor episode constructor rejected inputs: {0:?}")]
    EpisodeBuild(CursorEpisodeBuildError),
    /// The shared protocol rejected a persisted ack's participant authority.
    #[error("shared cumulative-ack transition rejected authority: {0:?}")]
    AckAuthorization(CumulativeAckAuthorizationError),
    /// A durable cursor event used an unsupported or malformed representation.
    #[error("corrupt cursor episode log: {0}")]
    CorruptLog(&'static str),
    /// The durable stream was not contiguous at the expected event sequence.
    #[error("cursor episode stream discontinuity: expected {expected}, found {actual}")]
    SequenceDiscontinuity {
        /// Next sequence required by replay.
        expected: u64,
        /// Sequence carried by the durable entry.
        actual: u64,
    },
    /// The durable stream sequence domain was exhausted.
    #[error("cursor episode stream sequence exhausted")]
    SequenceExhausted,
    /// The participant vector cannot fit the repository event format.
    #[error("cursor episode participant count exceeds u32")]
    TooManyParticipants,
}

/// Server-owned append-only repository for one participant cursor episode.
///
/// Durable records contain only the original construction inputs and ack
/// commands. Recovery never decodes or fabricates protocol state: it calls the
/// shared constructor once and the shared cumulative-ack transition for every
/// later record, preserving the crate as the sole owner of lifecycle rules.
#[derive(Debug)]
pub struct CursorEpisodeRepository {
    store: Arc<dyn DurableStore>,
    stream_key: String,
    episode: NonzeroDebtCursorEpisode,
    next_expected_sequence: u64,
}

impl CursorEpisodeRepository {
    /// Creates and durably starts a new episode stream.
    ///
    /// The shared constructor validates the start inputs before the event is
    /// appended at expected sequence zero.
    ///
    /// # Errors
    ///
    /// Returns [`CursorRepositoryError`] when shared construction, event
    /// encoding, or the optimistic append fails.
    pub async fn create(
        stream_key: impl Into<String>,
        store: Arc<dyn DurableStore>,
        start: CursorEpisodeStart,
    ) -> Result<Self, CursorRepositoryError> {
        let episode = start.build()?;
        let payload = CursorEvent::Start(start).encode()?;
        let stream_key = stream_key.into();
        let assigned_sequence = store.append(&stream_key, payload, 0).await?;
        if assigned_sequence != 0 {
            return Err(CursorRepositoryError::SequenceDiscontinuity {
                expected: 0,
                actual: assigned_sequence,
            });
        }
        Ok(Self {
            store,
            stream_key,
            episode,
            next_expected_sequence: 1,
        })
    }

    /// Reopens an episode by replaying its complete append-only command log.
    ///
    /// Returns `None` when the selected stream has no records.
    ///
    /// # Errors
    ///
    /// Returns [`CursorRepositoryError`] for store failures, malformed or
    /// discontinuous records, repeated/missing start events, or any command the
    /// shared lifecycle transition refuses during replay.
    pub async fn recover(
        stream_key: impl Into<String>,
        store: Arc<dyn DurableStore>,
    ) -> Result<Option<Self>, CursorRepositoryError> {
        let stream_key = stream_key.into();
        let mut next_expected_sequence = 0_u64;
        let mut episode = None;

        loop {
            let entries = store
                .read_from(&stream_key, next_expected_sequence, READ_BATCH_SIZE)
                .await?;
            let batch_len = entries.len();
            if batch_len == 0 {
                break;
            }

            for stored in entries {
                if stored.sequence != next_expected_sequence {
                    return Err(CursorRepositoryError::SequenceDiscontinuity {
                        expected: next_expected_sequence,
                        actual: stored.sequence,
                    });
                }
                match CursorEvent::decode(&stored.payload)? {
                    CursorEvent::Start(start) => {
                        if episode.is_some() || next_expected_sequence != 0 {
                            return Err(CursorRepositoryError::CorruptLog(
                                "episode start must be the first and only start event",
                            ));
                        }
                        episode = Some(start.build()?);
                    }
                    CursorEvent::Ack(command) => {
                        let Some(current) = episode.as_mut() else {
                            return Err(CursorRepositoryError::CorruptLog(
                                "ack event precedes episode start",
                            ));
                        };
                        current
                            .acknowledge(
                                command.participant_index,
                                command.receiving_binding_epoch,
                                &command.request,
                                command.contiguously_available_through,
                            )
                            .map_err(CursorRepositoryError::AckAuthorization)?;
                    }
                }
                next_expected_sequence = next_expected_sequence
                    .checked_add(1)
                    .ok_or(CursorRepositoryError::SequenceExhausted)?;
            }

            if batch_len < READ_BATCH_SIZE {
                break;
            }
        }

        let Some(episode) = episode else {
            return Ok(None);
        };
        Ok(Some(Self {
            store,
            stream_key,
            episode,
            next_expected_sequence,
        }))
    }

    /// Applies one shared cumulative-ack transition and appends its exact command
    /// only when the protocol reports a committed event.
    ///
    /// `NoOp`, `Gap`, and `Regression` consume no event and return without a
    /// durable append. Committed mutation becomes visible from this repository
    /// only after the optimistic append succeeds. Consequently an append conflict
    /// leaves the in-memory episode at its durable pre-command state.
    ///
    /// # Errors
    ///
    /// Returns [`CursorRepositoryError`] when the shared transition rejects the
    /// authority, event encoding fails, or the durable append conflicts/fails.
    pub async fn acknowledge(
        &mut self,
        command: CursorAckCommand,
    ) -> Result<CumulativeAckOutcome, CursorRepositoryError> {
        let mut candidate = self.episode.clone();
        let outcome = candidate
            .acknowledge(
                command.participant_index,
                command.receiving_binding_epoch,
                &command.request,
                command.contiguously_available_through,
            )
            .map_err(CursorRepositoryError::AckAuthorization)?;
        if !matches!(&outcome, CumulativeAckOutcome::Committed(_)) {
            return Ok(outcome);
        }
        let payload = CursorEvent::Ack(command).encode()?;
        let assigned_sequence = self
            .store
            .append(&self.stream_key, payload, self.next_expected_sequence)
            .await?;
        if assigned_sequence != self.next_expected_sequence {
            return Err(CursorRepositoryError::SequenceDiscontinuity {
                expected: self.next_expected_sequence,
                actual: assigned_sequence,
            });
        }
        self.next_expected_sequence = self
            .next_expected_sequence
            .checked_add(1)
            .ok_or(CursorRepositoryError::SequenceExhausted)?;
        self.episode = candidate;
        Ok(outcome)
    }

    /// Flushes the underlying durable store.
    ///
    /// # Errors
    ///
    /// Returns the store's durability failure if buffered records cannot be
    /// made durable.
    pub async fn flush(&self) -> Result<(), CursorRepositoryError> {
        self.store
            .flush()
            .await
            .map_err(CursorRepositoryError::from)
    }

    /// Returns the state produced exclusively by shared lifecycle transitions.
    #[must_use]
    pub const fn episode(&self) -> &NonzeroDebtCursorEpisode {
        &self.episode
    }

    /// Returns the next optimistic stream sequence.
    #[must_use]
    pub const fn next_expected_sequence(&self) -> u64 {
        self.next_expected_sequence
    }

    /// Returns the repository's durable stream key.
    #[must_use]
    pub fn stream_key(&self) -> &str {
        &self.stream_key
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CursorEvent {
    Start(CursorEpisodeStart),
    Ack(CursorAckCommand),
}

impl CursorEvent {
    fn encode(&self) -> Result<Vec<u8>, CursorRepositoryError> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&EVENT_MAGIC);
        bytes.push(EVENT_VERSION);
        match self {
            Self::Start(start) => {
                bytes.push(START_TAG);
                encode_start(start, &mut bytes)?;
            }
            Self::Ack(command) => {
                bytes.push(ACK_TAG);
                encode_ack(command, &mut bytes);
            }
        }
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self, CursorRepositoryError> {
        let mut reader = EventReader::new(bytes);
        if reader.read_array::<4>()? != EVENT_MAGIC {
            return Err(CursorRepositoryError::CorruptLog(
                "cursor event magic mismatch",
            ));
        }
        if reader.read_u8()? != EVENT_VERSION {
            return Err(CursorRepositoryError::CorruptLog(
                "unsupported cursor event version",
            ));
        }
        let event = match reader.read_u8()? {
            START_TAG => Self::Start(decode_start(&mut reader)?),
            ACK_TAG => Self::Ack(decode_ack(&mut reader)?),
            _ => {
                return Err(CursorRepositoryError::CorruptLog(
                    "unknown cursor event tag",
                ));
            }
        };
        reader.finish()?;
        Ok(event)
    }
}

fn encode_start(
    start: &CursorEpisodeStart,
    bytes: &mut Vec<u8>,
) -> Result<(), CursorRepositoryError> {
    bytes.extend_from_slice(&start.conversation_id.to_be_bytes());
    let debt = start.debt.value();
    bytes.extend_from_slice(&debt.entries.to_be_bytes());
    bytes.extend_from_slice(&debt.bytes.to_be_bytes());
    bytes.extend_from_slice(&start.observer_progress.to_be_bytes());
    bytes.extend_from_slice(&start.candidate_high_watermark.to_be_bytes());
    bytes.extend_from_slice(&start.current_floor.to_be_bytes());
    bytes.extend_from_slice(&start.cap_floor.to_be_bytes());
    let participant_count = u32::try_from(start.participants.len())
        .map_err(|_| CursorRepositoryError::TooManyParticipants)?;
    bytes.extend_from_slice(&participant_count.to_be_bytes());
    for participant in &start.participants {
        bytes.extend_from_slice(&participant.participant_id().to_be_bytes());
        encode_binding_epoch(participant.active_binding_epoch(), bytes);
        bytes.extend_from_slice(&participant.cursor().to_be_bytes());
    }
    Ok(())
}

fn decode_start(reader: &mut EventReader<'_>) -> Result<CursorEpisodeStart, CursorRepositoryError> {
    let conversation_id = reader.read_u64()?;
    let debt = ClosureDebt::new(WideResourceVector::new(
        reader.read_u128()?,
        reader.read_u128()?,
    ))
    .ok_or(CursorRepositoryError::CorruptLog(
        "episode start carries zero closure debt",
    ))?;
    let observer_progress = reader.read_u64()?;
    let candidate_high_watermark = reader.read_u64()?;
    let current_floor = reader.read_u128()?;
    let cap_floor = reader.read_u128()?;
    let participant_count = reader.read_u32()?;
    let participant_capacity = usize::try_from(participant_count).map_err(|_| {
        CursorRepositoryError::CorruptLog("participant count cannot fit this platform")
    })?;
    let mut participants = Vec::with_capacity(participant_capacity);
    for _ in 0..participant_count {
        let participant_id = reader.read_u64()?;
        let binding_epoch = decode_binding_epoch(reader)?;
        let cursor = reader.read_u64()?;
        participants.push(BoundParticipantCursor::new(
            participant_id,
            binding_epoch,
            cursor,
        ));
    }
    Ok(CursorEpisodeStart {
        conversation_id,
        debt,
        observer_progress,
        candidate_high_watermark,
        current_floor,
        cap_floor,
        participants,
    })
}

fn encode_ack(command: &CursorAckCommand, bytes: &mut Vec<u8>) {
    bytes.extend_from_slice(&command.participant_index.to_be_bytes());
    encode_binding_epoch(command.receiving_binding_epoch, bytes);
    bytes.extend_from_slice(&command.request.conversation_id.to_be_bytes());
    bytes.extend_from_slice(&command.request.participant_id.to_be_bytes());
    bytes.extend_from_slice(&command.request.capability_generation.get().to_be_bytes());
    bytes.extend_from_slice(&command.request.through_seq.to_be_bytes());
    bytes.extend_from_slice(&command.contiguously_available_through.to_be_bytes());
}

fn decode_ack(reader: &mut EventReader<'_>) -> Result<CursorAckCommand, CursorRepositoryError> {
    let participant_index = reader.read_u64()?;
    let receiving_binding_epoch = decode_binding_epoch(reader)?;
    let conversation_id = reader.read_u64()?;
    let participant_id = reader.read_u64()?;
    let capability_generation = decode_generation(reader)?;
    let through_seq = reader.read_u64()?;
    let contiguously_available_through = reader.read_u64()?;
    Ok(CursorAckCommand {
        participant_index,
        receiving_binding_epoch,
        request: ParticipantAck {
            conversation_id,
            participant_id,
            capability_generation,
            through_seq,
        },
        contiguously_available_through,
    })
}

fn encode_binding_epoch(epoch: BindingEpoch, bytes: &mut Vec<u8>) {
    bytes.extend_from_slice(
        &epoch
            .connection_incarnation
            .server_incarnation
            .to_be_bytes(),
    );
    bytes.extend_from_slice(
        &epoch
            .connection_incarnation
            .connection_ordinal
            .to_be_bytes(),
    );
    bytes.extend_from_slice(&epoch.capability_generation.get().to_be_bytes());
}

fn decode_binding_epoch(
    reader: &mut EventReader<'_>,
) -> Result<BindingEpoch, CursorRepositoryError> {
    let server_incarnation = reader.read_u64()?;
    let connection_ordinal = reader.read_u64()?;
    let capability_generation = decode_generation(reader)?;
    Ok(BindingEpoch::new(
        ConnectionIncarnation::new(server_incarnation, connection_ordinal),
        capability_generation,
    ))
}

fn decode_generation(reader: &mut EventReader<'_>) -> Result<Generation, CursorRepositoryError> {
    Generation::new(reader.read_u64()?).ok_or(CursorRepositoryError::CorruptLog(
        "cursor event carries zero generation",
    ))
}

struct EventReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> EventReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], CursorRepositoryError> {
        let end = self
            .offset
            .checked_add(N)
            .ok_or(CursorRepositoryError::CorruptLog(
                "cursor event offset overflow",
            ))?;
        let slice = self
            .bytes
            .get(self.offset..end)
            .ok_or(CursorRepositoryError::CorruptLog("truncated cursor event"))?;
        let array = <[u8; N]>::try_from(slice)
            .map_err(|_| CursorRepositoryError::CorruptLog("cursor event field width mismatch"))?;
        self.offset = end;
        Ok(array)
    }

    fn read_u8(&mut self) -> Result<u8, CursorRepositoryError> {
        Ok(u8::from_be_bytes(self.read_array()?))
    }

    fn read_u32(&mut self) -> Result<u32, CursorRepositoryError> {
        Ok(u32::from_be_bytes(self.read_array()?))
    }

    fn read_u64(&mut self) -> Result<u64, CursorRepositoryError> {
        Ok(u64::from_be_bytes(self.read_array()?))
    }

    fn read_u128(&mut self) -> Result<u128, CursorRepositoryError> {
        Ok(u128::from_be_bytes(self.read_array()?))
    }

    const fn finish(self) -> Result<(), CursorRepositoryError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(CursorRepositoryError::CorruptLog(
                "cursor event has trailing bytes",
            ))
        }
    }
}
