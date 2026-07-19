//! Canonical schema-v1 Unit 2 extension stream.
//!
//! This stream is keyed separately from the literal schema-v2 participant
//! operation stream. Its codec is deliberately singular and binary: payload
//! bytes are stored once without JSON/base64 expansion, every enum is explicitly
//! tagged, every length is checked, and trailing or drifted bytes are refused.

mod codec;

use std::{collections::VecDeque, sync::Arc};

use liminal::durability::{DurabilityError, DurableStore};
use liminal_protocol::wire::{
    BindingEpoch, MarkerAck, ParticipantDelivery, ParticipantId, ParticipantRecord,
};

pub(super) use codec::{decode_row, encode_row};

/// Stream-key prefix for Unit 2 participant extension rows.
pub(super) const OUTBOX_STREAM_PREFIX: &str = "liminal:participant-production-unit2:";
/// Durable page size signed for Unit 2 restore.
pub(super) const UNIT2_OUTBOX_RESTORE_BATCH_ROWS: usize = 64;
/// Exact Unit 2 extension schema version.
pub(super) const OUTBOX_SCHEMA_VERSION: u8 = 1;

/// Failure to encode, decode, append, or read one Unit 2 extension row.
#[derive(Debug, thiserror::Error)]
pub(super) enum OutboxLogError {
    /// The underlying durable store rejected an operation.
    #[error(transparent)]
    Durability(#[from] DurabilityError),
    /// A row contains no leading schema-version byte.
    #[error("Unit 2 extension row is missing its schema version")]
    MissingSchemaVersion,
    /// A row uses an unsupported schema version.
    #[error("unsupported Unit 2 extension schema version {0}")]
    SchemaVersion(u8),
    /// One stream contains more than one schema version.
    #[error("mixed Unit 2 extension schema versions: expected {expected}, found {actual}")]
    MixedSchemaVersions {
        /// Version established by the first physical row.
        expected: u8,
        /// Different version read later in the same stream.
        actual: u8,
    },
    /// A row ended before its selected schema was complete.
    #[error("Unit 2 extension row ended before {field}")]
    UnexpectedEnd {
        /// Field being decoded when bytes ended.
        field: &'static str,
    },
    /// A numeric tag does not name a schema-v1 value.
    #[error("unknown Unit 2 extension {domain} tag {value}")]
    UnknownTag {
        /// Tagged schema domain.
        domain: &'static str,
        /// Unsupported numeric selector.
        value: u8,
    },
    /// A boolean or optional-value selector was neither zero nor one.
    #[error("invalid Unit 2 extension {field} selector {value}")]
    InvalidSelector {
        /// Selected field.
        field: &'static str,
        /// Invalid selector.
        value: u8,
    },
    /// A persisted capability generation was zero.
    #[error("Unit 2 extension row carries zero capability generation")]
    ZeroGeneration,
    /// A variable-length field exceeds the canonical u32 framing domain.
    #[error("Unit 2 extension {field} length {length} exceeds u32")]
    LengthOverflow {
        /// Variable-length field.
        field: &'static str,
        /// Rejected host length.
        length: usize,
    },
    /// A canonical participant push cannot be measured.
    #[error("Unit 2 extension participant push is not canonically encodable: {0:?}")]
    PushCodec(liminal_protocol::wire::CodecError),
    /// Bytes remain after the one selected schema-v1 row.
    #[error("Unit 2 extension row has {remaining} trailing bytes")]
    TrailingBytes {
        /// Unconsumed bytes.
        remaining: usize,
    },
    /// The durable stream was not contiguous at the expected sequence.
    #[error("Unit 2 extension stream expected sequence {expected}, found {actual}")]
    Sequence {
        /// Next physical sequence required by restore.
        expected: u64,
        /// Sequence supplied by durable storage.
        actual: u64,
    },
    /// The store assigned a different sequence than the optimistic head.
    #[error("Unit 2 extension append expected {expected}, got {actual}")]
    AssignedSequence {
        /// Optimistic-concurrency sequence supplied to the store.
        expected: u64,
        /// Sequence assigned by the store.
        actual: u64,
    },
}

/// Exhaustive v2 source kinds that produce a participant delivery batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ProducedSourceKind {
    Enrolled,
    Attached,
    Detached,
    MarkerDrained,
    RecordAdmission,
    Left,
}

/// One validated record inside a source batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ProjectedRecord {
    pub(super) delivery_seq: u64,
    pub(super) body: ParticipantRecord,
    pub(super) recipients: Vec<ParticipantId>,
    pub(super) sender: Option<ParticipantId>,
    pub(super) encoded_push_bytes: u64,
}

impl ProjectedRecord {
    /// Builds a projection and records its canonical complete push charge.
    pub(super) fn try_new(
        conversation_id: u64,
        delivery_seq: u64,
        body: ParticipantRecord,
        recipients: Vec<ParticipantId>,
        sender: Option<ParticipantId>,
    ) -> Result<Self, OutboxLogError> {
        let encoded_push_bytes = codec::canonical_push_bytes(conversation_id, delivery_seq, &body)?;
        Ok(Self {
            delivery_seq,
            body,
            recipients,
            sender,
            encoded_push_bytes,
        })
    }

    pub(super) const fn delivery_seq(&self) -> u64 {
        self.delivery_seq
    }

    pub(super) const fn body(&self) -> &ParticipantRecord {
        &self.body
    }

    pub(super) fn recipients(&self) -> &[ParticipantId] {
        &self.recipients
    }

    pub(super) const fn sender(&self) -> Option<ParticipantId> {
        self.sender
    }

    pub(super) const fn encoded_push_bytes(&self) -> u64 {
        self.encoded_push_bytes
    }

    pub(super) fn into_delivery(self, conversation_id: u64) -> ParticipantDelivery {
        ParticipantDelivery {
            conversation_id,
            delivery_seq: self.delivery_seq,
            record: self.body,
        }
    }
}

/// One nonempty, one-or-two-record projection of a committed v2 source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ProducedBatch {
    pub(super) source_log_sequence: u64,
    pub(super) source_kind: ProducedSourceKind,
    pub(super) ordered_records: Vec<ProjectedRecord>,
}

impl ProducedBatch {
    pub(super) const fn source_log_sequence(&self) -> u64 {
        self.source_log_sequence
    }

    pub(super) const fn source_kind(&self) -> ProducedSourceKind {
        self.source_kind
    }

    pub(super) fn ordered_records(&self) -> &[ProjectedRecord] {
        &self.ordered_records
    }

    pub(super) const fn new(
        source_log_sequence: u64,
        source_kind: ProducedSourceKind,
        ordered_records: Vec<ProjectedRecord>,
    ) -> Self {
        Self {
            source_log_sequence,
            source_kind,
            ordered_records,
        }
    }
}

/// Complete persistence census for one committed `MarkerAck`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct StoredMarkerAckCommitted {
    pub(super) request: MarkerAck,
    pub(super) receiving_binding_epoch: BindingEpoch,
    pub(super) offered_marker_delivery_seq: u64,
    pub(super) delivered_binding_epoch: BindingEpoch,
    pub(super) from_cursor: u64,
    pub(super) resulting_cursor: u64,
    pub(super) base_log_head: u64,
    pub(super) extension_sequence: u64,
}

/// Exhaustive schema-v1 Unit 2 extension row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum OutboxRow {
    /// One complete source-batch projection.
    Produced(ProducedBatch),
    /// Exact cumulative acknowledgement committed in the v2 stream.
    AckAdvanced {
        source_log_sequence: u64,
        participant_id: ParticipantId,
        through_seq: u64,
    },
    /// Frontier-affecting marker acknowledgement and complete audit.
    MarkerAckCommitted(StoredMarkerAckCommitted),
}

impl OutboxRow {
    /// Returns the row's merge boundary in the literal v2 stream.
    pub(super) const fn base_log_head(&self) -> Option<u64> {
        match self {
            Self::Produced(batch) => batch.source_log_sequence.checked_add(1),
            Self::AckAdvanced {
                source_log_sequence,
                ..
            } => source_log_sequence.checked_add(1),
            Self::MarkerAckCommitted(row) => Some(row.base_log_head),
        }
    }
}

/// Move-only, one-page cursor over one conversation's Unit 2 extension stream.
///
/// A short nonempty store read is never EOF. The cursor advances to the checked
/// successor and confirms EOF only when that offset returns an empty read.
pub(super) struct OutboxRestoreCursor<'a> {
    log: &'a OutboxLog,
    next_expected_sequence: u64,
    established_version: Option<u8>,
    eof: bool,
    page: VecDeque<(u64, OutboxRow)>,
}

impl<'a> OutboxRestoreCursor<'a> {
    fn new(log: &'a OutboxLog) -> Self {
        Self {
            log,
            next_expected_sequence: 0,
            established_version: None,
            eof: false,
            page: VecDeque::new(),
        }
    }

    async fn load_page(&mut self) -> Result<(), OutboxLogError> {
        if self.eof || !self.page.is_empty() {
            return Ok(());
        }
        let entries = self
            .log
            .store
            .read_from(
                &self.log.stream_key,
                self.next_expected_sequence,
                UNIT2_OUTBOX_RESTORE_BATCH_ROWS,
            )
            .await?;
        if entries.is_empty() {
            self.eof = true;
            return Ok(());
        }

        let mut decoded = VecDeque::with_capacity(entries.len());
        for entry in entries {
            if entry.sequence != self.next_expected_sequence {
                return Err(OutboxLogError::Sequence {
                    expected: self.next_expected_sequence,
                    actual: entry.sequence,
                });
            }
            let version = entry
                .payload
                .first()
                .copied()
                .ok_or(OutboxLogError::MissingSchemaVersion)?;
            if let Some(expected) = self.established_version {
                if version != expected {
                    return Err(OutboxLogError::MixedSchemaVersions {
                        expected,
                        actual: version,
                    });
                }
            } else {
                self.established_version = Some(version);
            }
            if version != OUTBOX_SCHEMA_VERSION {
                return Err(OutboxLogError::SchemaVersion(version));
            }
            decoded.push_back((self.next_expected_sequence, decode_row(&entry.payload)?));
            self.next_expected_sequence =
                self.next_expected_sequence
                    .checked_add(1)
                    .ok_or(OutboxLogError::Sequence {
                        expected: u64::MAX,
                        actual: entry.sequence,
                    })?;
        }
        self.page = decoded;
        Ok(())
    }

    /// Borrows the current physical head, loading exactly one page when needed.
    pub(super) async fn front(&mut self) -> Result<Option<&(u64, OutboxRow)>, OutboxLogError> {
        self.load_page().await?;
        Ok(self.page.front())
    }

    /// Consumes the current physical head after a successful [`Self::front`].
    pub(super) fn pop_front(&mut self) -> Option<(u64, OutboxRow)> {
        self.page.pop_front()
    }

    /// Streams and discards every decoded row through explicit empty-read EOF.
    pub(super) async fn validate_all(mut self) -> Result<(), OutboxLogError> {
        while self.front().await?.is_some() {
            let _ = self.pop_front();
        }
        Ok(())
    }

    /// Returns the checked successor offset after explicit EOF confirmation.
    pub(super) const fn confirmed_head(&self) -> Option<u64> {
        if self.eof {
            Some(self.next_expected_sequence)
        } else {
            None
        }
    }
}

/// Append-only handle over one conversation's Unit 2 extension stream.
#[derive(Debug)]
pub(super) struct OutboxLog {
    store: Arc<dyn DurableStore>,
    stream_key: String,
}

impl OutboxLog {
    pub(super) fn new(store: Arc<dyn DurableStore>, conversation_id: u64) -> Self {
        Self {
            store,
            stream_key: format!("{OUTBOX_STREAM_PREFIX}{conversation_id}"),
        }
    }

    /// Starts a fresh bounded traversal at physical sequence zero.
    pub(super) fn restore_cursor(&self) -> OutboxRestoreCursor<'_> {
        OutboxRestoreCursor::new(self)
    }

    /// Appends one canonical row at the exact optimistic head, then flushes.
    pub(super) async fn append(
        &self,
        row: &OutboxRow,
        expected_sequence: u64,
    ) -> Result<(), OutboxLogError> {
        let payload = encode_row(row)?;
        let assigned = self
            .store
            .append(&self.stream_key, payload, expected_sequence)
            .await?;
        if assigned != expected_sequence {
            return Err(OutboxLogError::AssignedSequence {
                expected: expected_sequence,
                actual: assigned,
            });
        }
        self.store.flush().await?;
        Ok(())
    }

    /// Frozen aggregate reader retained only as the pre-W3 test reference.
    ///
    /// This intentionally preserves the old short-page termination and owns
    /// its independent decode/validation loop. Production restore has no
    /// selector or fallback to this implementation.
    #[cfg(test)]
    pub(super) async fn read_all(&self) -> Result<Vec<(u64, OutboxRow)>, OutboxLogError> {
        let mut rows = Vec::new();
        let mut sequence = 0_u64;
        let mut established_version = None;
        loop {
            let entries = self
                .store
                .read_from(&self.stream_key, sequence, UNIT2_OUTBOX_RESTORE_BATCH_ROWS)
                .await?;
            if entries.is_empty() {
                break;
            }
            let page_len = entries.len();
            for entry in entries {
                if entry.sequence != sequence {
                    return Err(OutboxLogError::Sequence {
                        expected: sequence,
                        actual: entry.sequence,
                    });
                }
                let version = entry
                    .payload
                    .first()
                    .copied()
                    .ok_or(OutboxLogError::MissingSchemaVersion)?;
                if let Some(expected) = established_version {
                    if version != expected {
                        return Err(OutboxLogError::MixedSchemaVersions {
                            expected,
                            actual: version,
                        });
                    }
                } else {
                    established_version = Some(version);
                }
                if version != OUTBOX_SCHEMA_VERSION {
                    return Err(OutboxLogError::SchemaVersion(version));
                }
                rows.push((sequence, decode_row(&entry.payload)?));
                sequence = sequence.checked_add(1).ok_or(OutboxLogError::Sequence {
                    expected: u64::MAX,
                    actual: entry.sequence,
                })?;
            }
            if page_len < UNIT2_OUTBOX_RESTORE_BATCH_ROWS {
                break;
            }
        }
        Ok(rows)
    }
}
