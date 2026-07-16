//! Opaque event-sourced participant-conversation shell.
//!
//! The shell owns event ordering and durable replay without serializing any
//! lifecycle authority directly. Protocol operations will add private event
//! bodies over time; storage bindings persist only the canonical event bytes
//! and may use resulting state only after consuming the corresponding commit.

use alloc::vec::Vec;

use crate::wire::ConversationId;

const EVENT_MAGIC: [u8; 4] = *b"LPCE";
const EVENT_CODEC_MAJOR: u16 = 1;
const EVENT_CODEC_MINOR: u16 = 0;
const GENESIS_VALIDATED_TAG: u16 = 1;
const EVENT_HEADER_LEN: usize = 30;

/// Immutable genesis configuration for one participant conversation.
///
/// Fields are private so later configuration additions cannot be bypassed by a
/// storage binding constructing a partial genesis value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConversationGenesis {
    conversation_id: ConversationId,
}

impl ConversationGenesis {
    /// Creates the protocol-owned genesis configuration for a conversation.
    #[must_use]
    pub const fn new(conversation_id: ConversationId) -> Self {
        Self { conversation_id }
    }

    /// Returns the stable conversation identifier.
    #[must_use]
    pub const fn conversation_id(self) -> ConversationId {
        self.conversation_id
    }
}

/// Event-sourced participant conversation.
///
/// This aggregate is intentionally not `Clone`: at most one owner may prepare
/// the next event ordinal. Its lifecycle state remains private and has no raw
/// `into_parts` escape hatch.
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::ParticipantConversation;
///
/// fn require_clone<T: Clone>() {}
/// require_clone::<ParticipantConversation>();
/// ```
#[derive(Debug, PartialEq, Eq)]
pub struct ParticipantConversation {
    genesis: ConversationGenesis,
    next_event_ordinal: u64,
    genesis_validated: bool,
}

impl ParticipantConversation {
    /// Starts an empty aggregate from immutable genesis configuration.
    #[must_use]
    pub const fn from_genesis(genesis: ConversationGenesis) -> Self {
        Self {
            genesis,
            next_event_ordinal: 0,
            genesis_validated: false,
        }
    }

    /// Returns this aggregate's stable conversation identifier.
    #[must_use]
    pub const fn conversation_id(&self) -> ConversationId {
        self.genesis.conversation_id
    }

    /// Returns the exact ordinal required of the next durable event.
    #[must_use]
    pub const fn next_event_ordinal(&self) -> u64 {
        self.next_event_ordinal
    }

    /// Reports whether the one-shot genesis-validation event has committed.
    #[must_use]
    pub const fn genesis_validated(&self) -> bool {
        self.genesis_validated
    }

    /// Consumes the aggregate into a durability decision for genesis validation.
    ///
    /// A successful decision owns the pre-state inside [`ConversationCommit`],
    /// preventing another event from being prepared at the same ordinal while
    /// durable append is pending.
    #[must_use]
    pub const fn decide_genesis_validation(self) -> ConversationDecision {
        if self.genesis_validated {
            return ConversationDecision::Refused(ConversationRefusal {
                conversation: self,
                reason: ConversationRefusalReason::GenesisAlreadyValidated,
            });
        }
        let Some(resulting_event_ordinal) = self.next_event_ordinal.checked_add(1) else {
            let ordinal = self.next_event_ordinal;
            return ConversationDecision::Refused(ConversationRefusal {
                conversation: self,
                reason: ConversationRefusalReason::EventOrdinalExhausted { ordinal },
            });
        };
        let event = ConversationEvent {
            header: ConversationEventHeader {
                conversation_id: self.genesis.conversation_id,
                ordinal: self.next_event_ordinal,
            },
            body: ConversationEventBody::GenesisValidated,
        };
        ConversationDecision::Commit(ConversationCommit {
            conversation: self,
            event,
            resulting_event_ordinal,
        })
    }

    /// Consumes one decoded durable event into the next replay state.
    ///
    /// # Errors
    ///
    /// Returns [`ConversationReplayFailure`] retaining the unchanged pre-state
    /// unless conversation identifier, ordinal, body precondition, and the
    /// resulting ordinal all validate before mutation.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "replay consumes the non-Clone decoded occurrence so its private body cannot be reused as live transition authority"
    )]
    pub fn replay(mut self, event: ConversationEvent) -> Result<Self, ConversationReplayFailure> {
        let resulting_event_ordinal = match self.validate_event(&event) {
            Ok(resulting_event_ordinal) => resulting_event_ordinal,
            Err(reason) => {
                return Err(ConversationReplayFailure {
                    conversation: self,
                    reason,
                });
            }
        };
        self.apply_validated_event(&event, resulting_event_ordinal);
        Ok(self)
    }

    fn validate_event(&self, event: &ConversationEvent) -> Result<u64, ConversationReplayError> {
        if event.header.conversation_id != self.genesis.conversation_id {
            return Err(ConversationReplayError::ConversationMismatch {
                expected: self.genesis.conversation_id,
                actual: event.header.conversation_id,
            });
        }
        if event.header.ordinal != self.next_event_ordinal {
            return Err(ConversationReplayError::OrdinalMismatch {
                expected: self.next_event_ordinal,
                actual: event.header.ordinal,
            });
        }
        match event.body {
            ConversationEventBody::GenesisValidated if self.genesis_validated => {
                Err(ConversationReplayError::GenesisAlreadyValidated)
            }
            ConversationEventBody::GenesisValidated => event.header.ordinal.checked_add(1).ok_or(
                ConversationReplayError::EventOrdinalExhausted {
                    ordinal: event.header.ordinal,
                },
            ),
        }
    }

    const fn apply_validated_event(
        &mut self,
        event: &ConversationEvent,
        resulting_event_ordinal: u64,
    ) {
        match &event.body {
            ConversationEventBody::GenesisValidated => {
                self.genesis_validated = true;
                self.next_event_ordinal = resulting_event_ordinal;
            }
        }
    }

    #[cfg(test)]
    pub(super) const fn from_test_state(
        genesis: ConversationGenesis,
        next_event_ordinal: u64,
        genesis_validated: bool,
    ) -> Self {
        Self {
            genesis,
            next_event_ordinal,
            genesis_validated,
        }
    }
}

/// Header of every durable participant-conversation event.
#[derive(Debug, PartialEq, Eq)]
struct ConversationEventHeader {
    conversation_id: ConversationId,
    ordinal: u64,
}

/// Private typed body of a durable participant-conversation event.
#[derive(Debug, PartialEq, Eq)]
enum ConversationEventBody {
    GenesisValidated,
}

/// Opaque durable event emitted only by a protocol decision or canonical decode.
///
/// Neither header nor body fields are public, so a binding cannot manufacture a
/// typed event from raw lifecycle values.
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::ConversationEvent;
///
/// fn fabricate() {
///     let _ = ConversationEvent {
///         conversation_id: 7,
///         ordinal: 0,
///     };
/// }
/// ```
#[derive(Debug, PartialEq, Eq)]
pub struct ConversationEvent {
    header: ConversationEventHeader,
    body: ConversationEventBody,
}

impl ConversationEvent {
    /// Returns the event's owning conversation.
    #[must_use]
    pub const fn conversation_id(&self) -> ConversationId {
        self.header.conversation_id
    }

    /// Returns the event's contiguous log ordinal.
    #[must_use]
    pub const fn ordinal(&self) -> u64 {
        self.header.ordinal
    }

    /// Returns the exact canonical byte length of this event.
    #[must_use]
    pub const fn encoded_len(&self) -> usize {
        EVENT_HEADER_LEN + self.body.encoded_len()
    }

    /// Encodes the stable v1 event envelope in network byte order.
    #[must_use]
    pub fn encode_canonical(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(EVENT_HEADER_LEN);
        encoded.extend_from_slice(&EVENT_MAGIC);
        encoded.extend_from_slice(&EVENT_CODEC_MAJOR.to_be_bytes());
        encoded.extend_from_slice(&EVENT_CODEC_MINOR.to_be_bytes());
        encoded.extend_from_slice(&self.header.conversation_id.to_be_bytes());
        encoded.extend_from_slice(&self.header.ordinal.to_be_bytes());
        encoded.extend_from_slice(&GENESIS_VALIDATED_TAG.to_be_bytes());
        encoded.extend_from_slice(&0_u32.to_be_bytes());
        encoded
    }

    /// Decodes one exact canonical v1 event envelope.
    ///
    /// # Errors
    ///
    /// Returns [`ConversationEventDecodeError`] for truncation, an invalid
    /// magic prefix, an unsupported codec version or body tag, or any declared
    /// body length that differs from the complete supplied frame.
    pub fn decode_canonical(input: &[u8]) -> Result<Self, ConversationEventDecodeError> {
        if input.len() < EVENT_HEADER_LEN {
            return Err(ConversationEventDecodeError::Truncated {
                required: EVENT_HEADER_LEN,
                available: input.len(),
            });
        }
        if input.get(0..4) != Some(EVENT_MAGIC.as_slice()) {
            return Err(ConversationEventDecodeError::InvalidMagic);
        }
        let codec_major = take_u16(input, 4)?;
        let codec_minor = take_u16(input, 6)?;
        if (codec_major, codec_minor) != (EVENT_CODEC_MAJOR, EVENT_CODEC_MINOR) {
            return Err(ConversationEventDecodeError::UnsupportedCodec {
                major: codec_major,
                minor: codec_minor,
            });
        }
        let conversation_id = take_u64(input, 8)?;
        let ordinal = take_u64(input, 16)?;
        let event_tag = take_u16(input, 24)?;
        let declared_body_len = take_u32(input, 26)?;
        let declared_body_len_usize = usize::try_from(declared_body_len)
            .map_err(|_| ConversationEventDecodeError::LengthOverflow)?;
        let actual_body_len = input.len() - EVENT_HEADER_LEN;
        if declared_body_len_usize != actual_body_len {
            return Err(ConversationEventDecodeError::NonCanonicalLength {
                declared_body_len,
                actual_body_len,
            });
        }
        let body = match event_tag {
            GENESIS_VALIDATED_TAG if declared_body_len == 0 => {
                ConversationEventBody::GenesisValidated
            }
            GENESIS_VALIDATED_TAG => {
                return Err(ConversationEventDecodeError::NonCanonicalLength {
                    declared_body_len,
                    actual_body_len,
                });
            }
            unknown => {
                return Err(ConversationEventDecodeError::UnknownEventKind { tag: unknown });
            }
        };
        Ok(Self {
            header: ConversationEventHeader {
                conversation_id,
                ordinal,
            },
            body,
        })
    }
}

impl ConversationEventBody {
    const fn encoded_len(&self) -> usize {
        match self {
            Self::GenesisValidated => 0,
        }
    }
}

fn take_u16(input: &[u8], start: usize) -> Result<u16, ConversationEventDecodeError> {
    let end = start
        .checked_add(2)
        .ok_or(ConversationEventDecodeError::LengthOverflow)?;
    let bytes: [u8; 2] = input
        .get(start..end)
        .ok_or(ConversationEventDecodeError::Truncated {
            required: end,
            available: input.len(),
        })?
        .try_into()
        .map_err(|_| ConversationEventDecodeError::LengthOverflow)?;
    Ok(u16::from_be_bytes(bytes))
}

fn take_u32(input: &[u8], start: usize) -> Result<u32, ConversationEventDecodeError> {
    let end = start
        .checked_add(4)
        .ok_or(ConversationEventDecodeError::LengthOverflow)?;
    let bytes: [u8; 4] = input
        .get(start..end)
        .ok_or(ConversationEventDecodeError::Truncated {
            required: end,
            available: input.len(),
        })?
        .try_into()
        .map_err(|_| ConversationEventDecodeError::LengthOverflow)?;
    Ok(u32::from_be_bytes(bytes))
}

fn take_u64(input: &[u8], start: usize) -> Result<u64, ConversationEventDecodeError> {
    let end = start
        .checked_add(8)
        .ok_or(ConversationEventDecodeError::LengthOverflow)?;
    let bytes: [u8; 8] = input
        .get(start..end)
        .ok_or(ConversationEventDecodeError::Truncated {
            required: end,
            available: input.len(),
        })?
        .try_into()
        .map_err(|_| ConversationEventDecodeError::LengthOverflow)?;
    Ok(u64::from_be_bytes(bytes))
}

/// Stable canonical event-decode failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConversationEventDecodeError {
    /// Fewer bytes than the fixed envelope or selected field requires.
    Truncated {
        /// Minimum bytes required at the detected boundary.
        required: usize,
        /// Bytes supplied by the caller.
        available: usize,
    },
    /// The event does not begin with the assigned `LPCE` magic prefix.
    InvalidMagic,
    /// The event codec version is not supported by this crate.
    UnsupportedCodec {
        /// Presented codec major version.
        major: u16,
        /// Presented codec minor version.
        minor: u16,
    },
    /// The private body tag is unassigned.
    UnknownEventKind {
        /// Unrecognized body tag.
        tag: u16,
    },
    /// Declared and supplied body lengths differ or the selected body is nonempty.
    NonCanonicalLength {
        /// Length declared in the stable envelope.
        declared_body_len: u32,
        /// Complete bytes supplied after the fixed header.
        actual_body_len: usize,
    },
    /// A platform length conversion or offset addition overflowed.
    LengthOverflow,
}

/// Semantic durable-replay failure for a structurally decoded event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConversationReplayError {
    /// Event belongs to another conversation stream.
    ConversationMismatch {
        /// Conversation required by the aggregate.
        expected: ConversationId,
        /// Conversation carried by the event.
        actual: ConversationId,
    },
    /// Event is not the exact next contiguous ordinal.
    OrdinalMismatch {
        /// Next ordinal required by the aggregate.
        expected: u64,
        /// Ordinal carried by the event.
        actual: u64,
    },
    /// The one-shot genesis validation already committed.
    GenesisAlreadyValidated,
    /// No later event ordinal is representable.
    EventOrdinalExhausted {
        /// Last representable event ordinal.
        ordinal: u64,
    },
}

/// Failed replay retaining the byte-for-byte unchanged pre-state.
#[derive(Debug, PartialEq, Eq)]
pub struct ConversationReplayFailure {
    conversation: ParticipantConversation,
    reason: ConversationReplayError,
}

impl ConversationReplayFailure {
    /// Returns the exact semantic replay failure.
    #[must_use]
    pub const fn reason(&self) -> ConversationReplayError {
        self.reason
    }

    /// Recovers the unchanged replay pre-state.
    #[must_use]
    pub const fn into_conversation(self) -> ParticipantConversation {
        self.conversation
    }
}

/// Reason a protocol decision emitted no durable event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConversationRefusalReason {
    /// The one-shot genesis validation already committed.
    GenesisAlreadyValidated,
    /// No later event ordinal is representable.
    EventOrdinalExhausted {
        /// Last representable event ordinal.
        ordinal: u64,
    },
}

/// Refused decision retaining the unchanged aggregate for continued use.
#[derive(Debug, PartialEq, Eq)]
pub struct ConversationRefusal {
    conversation: ParticipantConversation,
    reason: ConversationRefusalReason,
}

impl ConversationRefusal {
    /// Returns the stable refusal reason.
    #[must_use]
    pub const fn reason(&self) -> ConversationRefusalReason {
        self.reason
    }

    /// Recovers the unchanged pre-state.
    #[must_use]
    pub const fn into_conversation(self) -> ParticipantConversation {
        self.conversation
    }
}

/// Protocol decision that either owns one pending durable commit or refuses it.
#[derive(Debug, PartialEq, Eq)]
pub enum ConversationDecision {
    /// Append this event before consuming the commit into usable state.
    Commit(ConversationCommit),
    /// No event was selected; the unchanged aggregate is recoverable.
    Refused(ConversationRefusal),
}

/// Ownership barrier between event selection and durable append.
///
/// The aggregate is private while the event is speculative. It becomes usable
/// only through consuming [`ConversationCommit::commit`], or returns unchanged
/// through consuming [`ConversationCommit::abort`] if append fails.
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::ConversationCommit;
///
/// fn leak(commit: ConversationCommit) {
///     let _ = commit.conversation;
/// }
/// ```
#[derive(Debug, PartialEq, Eq)]
pub struct ConversationCommit {
    conversation: ParticipantConversation,
    event: ConversationEvent,
    resulting_event_ordinal: u64,
}

impl ConversationCommit {
    /// Borrows the exact event that must be durably appended.
    #[must_use]
    pub const fn event(&self) -> &ConversationEvent {
        &self.event
    }

    /// Consumes the durability barrier and advances to the committed state.
    #[must_use]
    pub const fn commit(mut self) -> ParticipantConversation {
        self.conversation
            .apply_validated_event(&self.event, self.resulting_event_ordinal);
        self.conversation
    }

    /// Cancels a failed append and recovers the byte-for-byte unchanged pre-state.
    #[must_use]
    pub const fn abort(self) -> ParticipantConversation {
        self.conversation
    }
}
