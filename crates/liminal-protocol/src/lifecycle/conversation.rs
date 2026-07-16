//! Opaque event-sourced participant-conversation shell.
//!
//! The shell owns event ordering and durable replay without serializing any
//! lifecycle authority directly. Protocol operations will add private event
//! bodies over time; storage bindings persist only the canonical event bytes
//! and may use resulting state only after consuming the corresponding commit.

use alloc::vec::Vec;

use crate::wire::{
    ConversationId, DetachAttemptToken, Generation, LeaveAttemptToken, LeaveCommitted,
};

use super::operation_event::{
    AttachedOperation, BindingFateOperation, ConversationOperation, DetachedOperation,
    EnrolledOperation, LeftOperation, NonzeroDebtAckOperation,
};

const EVENT_MAGIC: [u8; 4] = *b"LPCE";
const EVENT_CODEC_MAJOR: u16 = 1;
const EVENT_CODEC_MINOR: u16 = 0;
const GENESIS_VALIDATED_TAG: u16 = 1;
const PARTICIPANT_ENROLLED_TAG: u16 = 2;
const PARTICIPANT_ATTACHED_TAG: u16 = 3;
const PARTICIPANT_DETACHED_TAG: u16 = 4;
const PARTICIPANT_LEFT_TAG: u16 = 5;
const BINDING_FATE_RECORDED_TAG: u16 = 6;
const NONZERO_DEBT_ACK_RECORDED_TAG: u16 = 7;
const EVENT_HEADER_LEN: usize = 30;
const BINDING_EPOCH_LEN: u32 = 24;
const ATTACH_SHAPED_BODY_LEN: u32 = 48;
const DETACHED_BODY_LEN: u32 = 64;
const LEFT_BODY_BASE_LEN: u32 = 49;
const BINDING_FATE_BODY_LEN: u32 = 40;
const NONZERO_DEBT_ACK_BODY_LEN: u32 = 24;
const LEFT_ENDED_EPOCH_FLAG: u8 = 0b01;
const LEFT_PRIOR_TERMINAL_FLAG: u8 = 0b10;
const LEFT_FLAG_MASK: u8 = LEFT_ENDED_EPOCH_FLAG | LEFT_PRIOR_TERMINAL_FLAG;

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

    /// Consumes the aggregate into a durability decision for one lifecycle
    /// operation.
    ///
    /// Every operation payload's public producer consumes one of the crate's
    /// own sealed commit values (each payload type documents its exact sealed
    /// input), so the shell records only operations that actually committed;
    /// validated cold restore is the one raw promotion path into those
    /// inputs. Every arm carries the
    /// conversation named by its producing commit, and a mismatch against
    /// this shell's conversation is refused before any event is selected. A
    /// successful decision owns the pre-state inside [`ConversationCommit`]
    /// exactly as genesis validation does: no state advances until the
    /// durable append is confirmed by consuming
    /// [`ConversationCommit::commit`].
    #[must_use]
    pub const fn decide_operation(self, operation: ConversationOperation) -> ConversationDecision {
        if !self.genesis_validated {
            return ConversationDecision::Refused(ConversationRefusal {
                conversation: self,
                reason: ConversationRefusalReason::GenesisNotValidated,
            });
        }
        let actual = operation.conversation_id();
        if actual != self.genesis.conversation_id {
            let expected = self.genesis.conversation_id;
            return ConversationDecision::Refused(ConversationRefusal {
                conversation: self,
                reason: ConversationRefusalReason::OperationConversationMismatch {
                    expected,
                    actual,
                },
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
            body: ConversationEventBody::Operation(operation),
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
            ConversationEventBody::Operation(_) if !self.genesis_validated => {
                Err(ConversationReplayError::GenesisNotValidated)
            }
            ConversationEventBody::GenesisValidated | ConversationEventBody::Operation(_) => {
                event.header.ordinal.checked_add(1).ok_or(
                    ConversationReplayError::EventOrdinalExhausted {
                        ordinal: event.header.ordinal,
                    },
                )
            }
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
            ConversationEventBody::Operation(_) => {
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
    Operation(ConversationOperation),
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
        EVENT_HEADER_LEN + self.body.encoded_body_len() as usize
    }

    /// Encodes the stable v1 event envelope in network byte order.
    #[must_use]
    pub fn encode_canonical(&self) -> Vec<u8> {
        let body_len = self.body.encoded_body_len();
        let mut encoded = Vec::with_capacity(EVENT_HEADER_LEN + body_len as usize);
        encoded.extend_from_slice(&EVENT_MAGIC);
        encoded.extend_from_slice(&EVENT_CODEC_MAJOR.to_be_bytes());
        encoded.extend_from_slice(&EVENT_CODEC_MINOR.to_be_bytes());
        encoded.extend_from_slice(&self.header.conversation_id.to_be_bytes());
        encoded.extend_from_slice(&self.header.ordinal.to_be_bytes());
        encoded.extend_from_slice(&self.body.tag().to_be_bytes());
        encoded.extend_from_slice(&body_len.to_be_bytes());
        self.body.encode_body_into(&mut encoded);
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
            PARTICIPANT_ENROLLED_TAG => {
                require_body_len(ATTACH_SHAPED_BODY_LEN, declared_body_len, actual_body_len)?;
                decode_enrolled(conversation_id, input)?
            }
            PARTICIPANT_ATTACHED_TAG => {
                require_body_len(ATTACH_SHAPED_BODY_LEN, declared_body_len, actual_body_len)?;
                decode_attached(conversation_id, input)?
            }
            PARTICIPANT_DETACHED_TAG => {
                require_body_len(DETACHED_BODY_LEN, declared_body_len, actual_body_len)?;
                decode_detached(conversation_id, input)?
            }
            PARTICIPANT_LEFT_TAG => {
                decode_left(conversation_id, input, declared_body_len, actual_body_len)?
            }
            BINDING_FATE_RECORDED_TAG => {
                require_body_len(BINDING_FATE_BODY_LEN, declared_body_len, actual_body_len)?;
                decode_binding_fate(conversation_id, input)?
            }
            NONZERO_DEBT_ACK_RECORDED_TAG => {
                require_body_len(
                    NONZERO_DEBT_ACK_BODY_LEN,
                    declared_body_len,
                    actual_body_len,
                )?;
                decode_nonzero_debt_ack(conversation_id, input)?
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
    const fn tag(&self) -> u16 {
        match self {
            Self::GenesisValidated => GENESIS_VALIDATED_TAG,
            Self::Operation(operation) => match operation {
                ConversationOperation::Enrolled(_) => PARTICIPANT_ENROLLED_TAG,
                ConversationOperation::Attached(_) => PARTICIPANT_ATTACHED_TAG,
                ConversationOperation::Detached(_) => PARTICIPANT_DETACHED_TAG,
                ConversationOperation::Left(_) => PARTICIPANT_LEFT_TAG,
                ConversationOperation::BindingFate(_) => BINDING_FATE_RECORDED_TAG,
                ConversationOperation::NonzeroDebtAck(_) => NONZERO_DEBT_ACK_RECORDED_TAG,
            },
        }
    }

    const fn encoded_body_len(&self) -> u32 {
        match self {
            Self::GenesisValidated => 0,
            Self::Operation(operation) => match operation {
                ConversationOperation::Enrolled(_) | ConversationOperation::Attached(_) => {
                    ATTACH_SHAPED_BODY_LEN
                }
                ConversationOperation::Detached(_) => DETACHED_BODY_LEN,
                ConversationOperation::Left(operation) => {
                    let committed = operation.committed();
                    let mut length = LEFT_BODY_BASE_LEN;
                    if committed.ended_binding_epoch().is_some() {
                        length += BINDING_EPOCH_LEN;
                    }
                    if committed.prior_terminal_delivery_seq().is_some() {
                        length += 8;
                    }
                    length
                }
                ConversationOperation::BindingFate(_) => BINDING_FATE_BODY_LEN,
                ConversationOperation::NonzeroDebtAck(_) => NONZERO_DEBT_ACK_BODY_LEN,
            },
        }
    }

    fn encode_body_into(&self, encoded: &mut Vec<u8>) {
        match self {
            Self::GenesisValidated => {}
            Self::Operation(ConversationOperation::Enrolled(operation)) => {
                encoded.extend_from_slice(&operation.participant_id().to_be_bytes());
                encode_epoch_into(encoded, operation.binding_epoch());
                encoded.extend_from_slice(&operation.attached_transaction_order().to_be_bytes());
                encoded.extend_from_slice(&operation.attached_delivery_seq().to_be_bytes());
            }
            Self::Operation(ConversationOperation::Attached(operation)) => {
                encoded.extend_from_slice(&operation.participant_id().to_be_bytes());
                encode_epoch_into(encoded, operation.binding_epoch());
                encoded.extend_from_slice(&operation.attached_transaction_order().to_be_bytes());
                encoded.extend_from_slice(&operation.attached_delivery_seq().to_be_bytes());
            }
            Self::Operation(ConversationOperation::Detached(operation)) => {
                encoded.extend_from_slice(operation.detach_attempt_token().as_bytes());
                encoded.extend_from_slice(&operation.participant_id().to_be_bytes());
                encode_epoch_into(encoded, operation.committed_binding_epoch());
                encoded.extend_from_slice(&operation.detached_transaction_order().to_be_bytes());
                encoded.extend_from_slice(&operation.detached_delivery_seq().to_be_bytes());
            }
            Self::Operation(ConversationOperation::Left(operation)) => {
                let committed = operation.committed();
                encoded.extend_from_slice(committed.leave_attempt_token().as_bytes());
                encoded.extend_from_slice(&committed.participant_id().to_be_bytes());
                encoded.extend_from_slice(&committed.retired_generation().get().to_be_bytes());
                let mut flags = 0_u8;
                if committed.ended_binding_epoch().is_some() {
                    flags |= LEFT_ENDED_EPOCH_FLAG;
                }
                if committed.prior_terminal_delivery_seq().is_some() {
                    flags |= LEFT_PRIOR_TERMINAL_FLAG;
                }
                encoded.push(flags);
                if let Some(epoch) = committed.ended_binding_epoch() {
                    encode_epoch_into(encoded, epoch);
                }
                if let Some(prior) = committed.prior_terminal_delivery_seq() {
                    encoded.extend_from_slice(&prior.to_be_bytes());
                }
                encoded.extend_from_slice(&operation.left_transaction_order().to_be_bytes());
                encoded.extend_from_slice(&committed.left_delivery_seq().to_be_bytes());
            }
            Self::Operation(ConversationOperation::BindingFate(operation)) => {
                encoded.extend_from_slice(&operation.participant_id().to_be_bytes());
                encode_epoch_into(encoded, operation.last_dead_binding_epoch());
                encoded.extend_from_slice(&operation.resulting_floor().to_be_bytes());
            }
            Self::Operation(ConversationOperation::NonzeroDebtAck(operation)) => {
                encoded.extend_from_slice(&operation.participant_id().to_be_bytes());
                encoded.extend_from_slice(&operation.capability_generation().get().to_be_bytes());
                encoded.extend_from_slice(&operation.through_seq().to_be_bytes());
            }
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

fn take_u8(input: &[u8], start: usize) -> Result<u8, ConversationEventDecodeError> {
    input
        .get(start)
        .copied()
        .ok_or_else(|| ConversationEventDecodeError::Truncated {
            required: start.saturating_add(1),
            available: input.len(),
        })
}

fn take_bytes16(input: &[u8], start: usize) -> Result<[u8; 16], ConversationEventDecodeError> {
    let end = start
        .checked_add(16)
        .ok_or(ConversationEventDecodeError::LengthOverflow)?;
    input
        .get(start..end)
        .ok_or(ConversationEventDecodeError::Truncated {
            required: end,
            available: input.len(),
        })?
        .try_into()
        .map_err(|_| ConversationEventDecodeError::LengthOverflow)
}

fn take_epoch(
    input: &[u8],
    start: usize,
    tag: u16,
) -> Result<crate::wire::BindingEpoch, ConversationEventDecodeError> {
    let server_incarnation = take_u64(input, start)?;
    let connection_ordinal = take_u64(input, start.saturating_add(8))?;
    let raw_generation = take_u64(input, start.saturating_add(16))?;
    let capability_generation = Generation::new(raw_generation)
        .ok_or(ConversationEventDecodeError::NonCanonicalBody { tag })?;
    Ok(crate::wire::BindingEpoch::new(
        crate::wire::ConnectionIncarnation::new(server_incarnation, connection_ordinal),
        capability_generation,
    ))
}

fn take_generation(
    input: &[u8],
    start: usize,
    tag: u16,
) -> Result<Generation, ConversationEventDecodeError> {
    Generation::new(take_u64(input, start)?)
        .ok_or(ConversationEventDecodeError::NonCanonicalBody { tag })
}

fn encode_epoch_into(encoded: &mut Vec<u8>, epoch: crate::wire::BindingEpoch) {
    encoded.extend_from_slice(
        &epoch
            .connection_incarnation
            .server_incarnation
            .to_be_bytes(),
    );
    encoded.extend_from_slice(
        &epoch
            .connection_incarnation
            .connection_ordinal
            .to_be_bytes(),
    );
    encoded.extend_from_slice(&epoch.capability_generation.get().to_be_bytes());
}

const fn require_body_len(
    expected: u32,
    declared_body_len: u32,
    actual_body_len: usize,
) -> Result<(), ConversationEventDecodeError> {
    if declared_body_len == expected {
        Ok(())
    } else {
        Err(ConversationEventDecodeError::NonCanonicalLength {
            declared_body_len,
            actual_body_len,
        })
    }
}

fn decode_enrolled(
    conversation_id: ConversationId,
    input: &[u8],
) -> Result<ConversationEventBody, ConversationEventDecodeError> {
    let participant_id = take_u64(input, EVENT_HEADER_LEN)?;
    let binding_epoch = take_epoch(input, EVENT_HEADER_LEN + 8, PARTICIPANT_ENROLLED_TAG)?;
    let attached_transaction_order = take_u64(input, EVENT_HEADER_LEN + 32)?;
    let attached_delivery_seq = take_u64(input, EVENT_HEADER_LEN + 40)?;
    let operation = EnrolledOperation::from_decoded(
        conversation_id,
        participant_id,
        binding_epoch,
        attached_transaction_order,
        attached_delivery_seq,
    )
    .ok_or(ConversationEventDecodeError::NonCanonicalBody {
        tag: PARTICIPANT_ENROLLED_TAG,
    })?;
    Ok(ConversationEventBody::Operation(
        ConversationOperation::Enrolled(operation),
    ))
}

fn decode_attached(
    conversation_id: ConversationId,
    input: &[u8],
) -> Result<ConversationEventBody, ConversationEventDecodeError> {
    let participant_id = take_u64(input, EVENT_HEADER_LEN)?;
    let binding_epoch = take_epoch(input, EVENT_HEADER_LEN + 8, PARTICIPANT_ATTACHED_TAG)?;
    let attached_transaction_order = take_u64(input, EVENT_HEADER_LEN + 32)?;
    let attached_delivery_seq = take_u64(input, EVENT_HEADER_LEN + 40)?;
    Ok(ConversationEventBody::Operation(
        ConversationOperation::Attached(AttachedOperation::from_decoded(
            conversation_id,
            participant_id,
            binding_epoch,
            attached_transaction_order,
            attached_delivery_seq,
        )),
    ))
}

fn decode_detached(
    conversation_id: ConversationId,
    input: &[u8],
) -> Result<ConversationEventBody, ConversationEventDecodeError> {
    let detach_attempt_token = DetachAttemptToken::new(take_bytes16(input, EVENT_HEADER_LEN)?);
    let participant_id = take_u64(input, EVENT_HEADER_LEN + 16)?;
    let committed_binding_epoch =
        take_epoch(input, EVENT_HEADER_LEN + 24, PARTICIPANT_DETACHED_TAG)?;
    let detached_transaction_order = take_u64(input, EVENT_HEADER_LEN + 48)?;
    let detached_delivery_seq = take_u64(input, EVENT_HEADER_LEN + 56)?;
    Ok(ConversationEventBody::Operation(
        ConversationOperation::Detached(DetachedOperation::from_decoded(
            detach_attempt_token,
            conversation_id,
            participant_id,
            committed_binding_epoch,
            detached_transaction_order,
            detached_delivery_seq,
        )),
    ))
}

fn decode_left(
    conversation_id: ConversationId,
    input: &[u8],
    declared_body_len: u32,
    actual_body_len: usize,
) -> Result<ConversationEventBody, ConversationEventDecodeError> {
    if declared_body_len < LEFT_BODY_BASE_LEN {
        return Err(ConversationEventDecodeError::NonCanonicalLength {
            declared_body_len,
            actual_body_len,
        });
    }
    let leave_attempt_token = LeaveAttemptToken::new(take_bytes16(input, EVENT_HEADER_LEN)?);
    let participant_id = take_u64(input, EVENT_HEADER_LEN + 16)?;
    let retired_generation = take_generation(input, EVENT_HEADER_LEN + 24, PARTICIPANT_LEFT_TAG)?;
    let flags = take_u8(input, EVENT_HEADER_LEN + 32)?;
    if flags & !LEFT_FLAG_MASK != 0 {
        return Err(ConversationEventDecodeError::NonCanonicalBody {
            tag: PARTICIPANT_LEFT_TAG,
        });
    }
    let mut expected_body_len = LEFT_BODY_BASE_LEN;
    if flags & LEFT_ENDED_EPOCH_FLAG != 0 {
        expected_body_len += BINDING_EPOCH_LEN;
    }
    if flags & LEFT_PRIOR_TERMINAL_FLAG != 0 {
        expected_body_len += 8;
    }
    if declared_body_len != expected_body_len {
        return Err(ConversationEventDecodeError::NonCanonicalLength {
            declared_body_len,
            actual_body_len,
        });
    }
    let mut cursor = EVENT_HEADER_LEN + 33;
    let ended_binding_epoch = if flags & LEFT_ENDED_EPOCH_FLAG == 0 {
        None
    } else {
        let epoch = take_epoch(input, cursor, PARTICIPANT_LEFT_TAG)?;
        cursor += BINDING_EPOCH_LEN as usize;
        Some(epoch)
    };
    let prior_terminal_delivery_seq = if flags & LEFT_PRIOR_TERMINAL_FLAG == 0 {
        None
    } else {
        let prior = take_u64(input, cursor)?;
        cursor += 8;
        Some(prior)
    };
    let left_transaction_order = take_u64(input, cursor)?;
    let left_delivery_seq = take_u64(input, cursor + 8)?;
    let committed = LeaveCommitted::new(
        conversation_id,
        leave_attempt_token,
        participant_id,
        retired_generation,
        ended_binding_epoch,
        prior_terminal_delivery_seq,
        left_delivery_seq,
    )
    .ok_or(ConversationEventDecodeError::NonCanonicalBody {
        tag: PARTICIPANT_LEFT_TAG,
    })?;
    Ok(ConversationEventBody::Operation(
        ConversationOperation::Left(LeftOperation::from_decoded(
            committed,
            left_transaction_order,
        )),
    ))
}

fn decode_binding_fate(
    conversation_id: ConversationId,
    input: &[u8],
) -> Result<ConversationEventBody, ConversationEventDecodeError> {
    let participant_id = take_u64(input, EVENT_HEADER_LEN)?;
    let last_dead_binding_epoch =
        take_epoch(input, EVENT_HEADER_LEN + 8, BINDING_FATE_RECORDED_TAG)?;
    let resulting_floor = take_u64(input, EVENT_HEADER_LEN + 32)?;
    Ok(ConversationEventBody::Operation(
        ConversationOperation::BindingFate(BindingFateOperation::from_decoded(
            conversation_id,
            participant_id,
            last_dead_binding_epoch,
            resulting_floor,
        )),
    ))
}

fn decode_nonzero_debt_ack(
    conversation_id: ConversationId,
    input: &[u8],
) -> Result<ConversationEventBody, ConversationEventDecodeError> {
    let participant_id = take_u64(input, EVENT_HEADER_LEN)?;
    let capability_generation =
        take_generation(input, EVENT_HEADER_LEN + 8, NONZERO_DEBT_ACK_RECORDED_TAG)?;
    let through_seq = take_u64(input, EVENT_HEADER_LEN + 16)?;
    Ok(ConversationEventBody::Operation(
        ConversationOperation::NonzeroDebtAck(NonzeroDebtAckOperation::from_decoded(
            conversation_id,
            participant_id,
            capability_generation,
            through_seq,
        )),
    ))
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
    /// Declared and supplied body lengths differ, or the declared length is
    /// not the canonical length for the selected body kind.
    NonCanonicalLength {
        /// Length declared in the stable envelope.
        declared_body_len: u32,
        /// Complete bytes supplied after the fixed header.
        actual_body_len: usize,
    },
    /// A structurally complete body violates a canonical field invariant, such
    /// as a zero generation, a non-generation-one enrollment epoch, an
    /// unassigned Leave flag bit, or an invalid permanent Leave result.
    NonCanonicalBody {
        /// Body tag whose field invariants failed.
        tag: u16,
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
    /// A lifecycle operation event preceded the genesis-validation event.
    GenesisNotValidated,
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
    /// A lifecycle operation was decided before genesis validation committed.
    GenesisNotValidated,
    /// The operation's provenance names another conversation.
    OperationConversationMismatch {
        /// Conversation required by the aggregate.
        expected: ConversationId,
        /// Conversation named by the operation's provenance.
        actual: ConversationId,
    },
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
