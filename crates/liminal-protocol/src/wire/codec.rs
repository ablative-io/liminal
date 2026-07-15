use alloc::vec::Vec;

use super::{
    AttachAttemptToken, AttachSecret, BindingEpoch, ClientDiscriminant, ClientRequest,
    CloseCauseTag, ConnectionIncarnation, CredentialAttachRequest, DecodeClass, DetachAttemptToken,
    DetachRequest, DetachedCause, DiedCause, EnrollmentRequest, EnrollmentToken, Generation,
    LeaveAttemptToken, LeaveRequest, MarkerAck, ObserverRecoveryHandshake, ObserverRefusal,
    ParticipantAck, ParticipantDelivery, ParticipantRecord, ProtocolVersion, PushDiscriminant,
    RecordAdmission, RecordKind, ServerPush, ServerValue,
};

/// Stable generic frame type assigned to participant traffic.
pub const PARTICIPANT_FRAME_TYPE: u8 = 0x1A;

/// Bytes in the existing generic frame header.
pub const GENERIC_HEADER_LEN: usize = 10;

/// Bytes in the version/discriminant prefix of every participant payload.
pub const PARTICIPANT_PREFIX_LEN: usize = 6;

/// Fixed complete-frame overhead before a selected participant body.
pub const PARTICIPANT_FRAME_OVERHEAD: usize = GENERIC_HEADER_LEN + PARTICIPANT_PREFIX_LEN;

/// Largest complete frame representable by the generic `u32` payload length.
pub const FRAME_MAX: u64 = 4_294_967_305;

/// Allocation ceiling used before participant capability is stored.
pub const PRECAP_PARTICIPANT_FRAME_MAX: u64 = 1_048_576;

/// Receiver whose direction registry is used during decoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReceiverDirection {
    /// Server receiver; only client requests are assigned.
    Server,
    /// SDK receiver; server values and pushes are assigned.
    Client,
}

/// One complete typed participant frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParticipantFrame {
    /// Client-to-server request.
    ClientRequest(ClientRequest),
    /// Server-to-client semantic response.
    ServerValue(ServerValue),
    /// Server-to-client pushed value.
    ServerPush(ServerPush),
}

/// Deterministic participant codec failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CodecError {
    /// A structural body or framing failure with the canonical decode class.
    Decode {
        /// Exact failure class.
        class: DecodeClass,
    },
    /// The concrete participant version differs from v1.0.
    UnsupportedVersion {
        /// Version read from the structural prefix.
        presented: ProtocolVersion,
        /// Version supported by this codec.
        supported: ProtocolVersion,
    },
    /// Caller-provided encoding storage is too short.
    OutputTooSmall {
        /// Complete bytes required.
        required: usize,
        /// Bytes supplied by the caller.
        available: usize,
    },
    /// A body or complete frame cannot fit its canonical `u32` length field.
    LengthOverflow,
    /// A typed value violates a field-domain invariant.
    InvalidValue,
    /// Server semantic bodies are reserved for the next codec increment.
    ServerValuePending,
}

/// Computes complete bytes from a generic-header payload length.
#[must_use]
pub fn complete_frame_bytes(payload_length: u32) -> u64 {
    10 + u64::from(payload_length)
}

/// Returns the exact complete-frame encoded length.
///
/// # Errors
///
/// Returns [`CodecError::LengthOverflow`] when a selected variable body cannot
/// fit the generic frame's `u32` payload length. Server semantic values return
/// [`CodecError::ServerValuePending`] until their body codec is added.
pub fn encoded_len(frame: &ParticipantFrame) -> Result<usize, CodecError> {
    let mut size = SizeSink::default();
    encode_body(frame, &mut size)?;
    let payload = PARTICIPANT_PREFIX_LEN
        .checked_add(size.len)
        .ok_or(CodecError::LengthOverflow)?;
    let _: u32 = payload.try_into().map_err(|_| CodecError::LengthOverflow)?;
    GENERIC_HEADER_LEN
        .checked_add(payload)
        .ok_or(CodecError::LengthOverflow)
}

/// Encodes one complete participant frame into caller-provided storage.
///
/// The generic header, v1.0 structural prefix, and selected body are emitted in
/// network byte order. The function performs no allocation.
///
/// # Errors
///
/// Returns [`CodecError::OutputTooSmall`] when `output` is shorter than
/// [`encoded_len`], or propagates a selected body's length/domain error.
pub fn encode(frame: &ParticipantFrame, output: &mut [u8]) -> Result<usize, CodecError> {
    let required = encoded_len(frame)?;
    if output.len() < required {
        return Err(CodecError::OutputTooSmall {
            required,
            available: output.len(),
        });
    }

    let payload_len = required
        .checked_sub(GENERIC_HEADER_LEN)
        .ok_or(CodecError::LengthOverflow)?;
    let payload_len: u32 = payload_len
        .try_into()
        .map_err(|_| CodecError::LengthOverflow)?;
    let discriminant = frame_discriminant(frame);

    let mut writer = Writer::new(output);
    writer.put_u8(PARTICIPANT_FRAME_TYPE)?;
    writer.put_u8(0)?;
    writer.put_u32(0)?;
    writer.put_u32(payload_len)?;
    writer.put_u16(ProtocolVersion::V1.major)?;
    writer.put_u16(ProtocolVersion::V1.minor)?;
    writer.put_u16(discriminant)?;
    encode_body(frame, &mut writer)?;
    Ok(required)
}

/// Decodes one exact complete participant frame for `receiver`.
///
/// Counts are proven against the already bounded remaining frame before any
/// list allocation. Extra bytes inside or outside the declared complete frame
/// are rejected as non-canonical by this exact-frame API.
///
/// # Errors
///
/// Returns the contract's canonical structural class, or
/// [`CodecError::UnsupportedVersion`] before discriminant/body decoding. Server
/// semantic bodies currently return [`CodecError::ServerValuePending`].
pub fn decode(input: &[u8], receiver: ReceiverDirection) -> Result<ParticipantFrame, CodecError> {
    if input.len() < GENERIC_HEADER_LEN {
        return Err(decode_error(DecodeClass::Framing));
    }

    let mut header = Reader::new(input);
    let frame_type = header.take_u8()?;
    let flags = header.take_u8()?;
    let stream_id = header.take_u32()?;
    let payload_len = header.take_u32()?;
    if frame_type != PARTICIPANT_FRAME_TYPE || flags != 0 || stream_id != 0 || payload_len < 6 {
        return Err(decode_error(DecodeClass::Framing));
    }

    let complete: usize = complete_frame_bytes(payload_len)
        .try_into()
        .map_err(|_| decode_error(DecodeClass::Framing))?;
    if input.len() < complete {
        return Err(decode_error(DecodeClass::MissingRequiredField));
    }
    if input.len() > complete {
        return Err(decode_error(DecodeClass::CanonicalEncoding));
    }

    let payload = input
        .get(GENERIC_HEADER_LEN..complete)
        .ok_or_else(|| decode_error(DecodeClass::MissingRequiredField))?;
    let mut prefix = Reader::new(payload);
    let version = ProtocolVersion::new(prefix.take_u16()?, prefix.take_u16()?);
    if version != ProtocolVersion::V1 {
        return Err(CodecError::UnsupportedVersion {
            presented: version,
            supported: ProtocolVersion::V1,
        });
    }
    let discriminant = prefix.take_u16()?;
    let body = prefix.remaining_bytes();

    match receiver {
        ReceiverDirection::Server => {
            let tag = ClientDiscriminant::try_from(discriminant)
                .map_err(|_| decode_error(DecodeClass::UnknownDiscriminant))?;
            decode_client_request(tag, body).map(ParticipantFrame::ClientRequest)
        }
        ReceiverDirection::Client => {
            if let Ok(tag) = PushDiscriminant::try_from(discriminant) {
                return decode_server_push(tag, body).map(ParticipantFrame::ServerPush);
            }
            if (0x0100..=0x0124).contains(&discriminant) {
                return Err(CodecError::ServerValuePending);
            }
            Err(decode_error(DecodeClass::UnknownDiscriminant))
        }
    }
}

const fn frame_discriminant(frame: &ParticipantFrame) -> u16 {
    match frame {
        ParticipantFrame::ClientRequest(value) => value.discriminant().wire_value(),
        ParticipantFrame::ServerValue(value) => value.discriminant().wire_value(),
        ParticipantFrame::ServerPush(value) => value.discriminant().wire_value(),
    }
}

fn encode_body<S: Sink>(frame: &ParticipantFrame, sink: &mut S) -> Result<(), CodecError> {
    match frame {
        ParticipantFrame::ClientRequest(value) => encode_client_request(value, sink),
        ParticipantFrame::ServerPush(value) => encode_server_push(value, sink),
        ParticipantFrame::ServerValue(_) => Err(CodecError::ServerValuePending),
    }
}

fn encode_client_request<S: Sink>(value: &ClientRequest, sink: &mut S) -> Result<(), CodecError> {
    match value {
        ClientRequest::Enrollment(value) => {
            sink.put_u64(value.conversation_id)?;
            sink.put_fixed(value.enrollment_token.as_bytes())
        }
        ClientRequest::CredentialAttach(value) => {
            sink.put_u64(value.conversation_id)?;
            sink.put_u64(value.participant_id)?;
            put_generation(sink, value.capability_generation)?;
            sink.put_fixed(value.attach_secret.as_bytes())?;
            sink.put_fixed(value.attach_attempt_token.as_bytes())?;
            put_option_u64(sink, value.accept_marker_delivery_seq)
        }
        ClientRequest::Detach(value) => {
            sink.put_u64(value.conversation_id)?;
            sink.put_u64(value.participant_id)?;
            put_generation(sink, value.capability_generation)?;
            sink.put_fixed(value.detach_attempt_token.as_bytes())
        }
        ClientRequest::ParticipantAck(value) => {
            sink.put_u64(value.conversation_id)?;
            sink.put_u64(value.participant_id)?;
            put_generation(sink, value.capability_generation)?;
            sink.put_u64(value.through_seq)
        }
        ClientRequest::Leave(value) => {
            sink.put_u64(value.conversation_id)?;
            sink.put_u64(value.participant_id)?;
            put_generation(sink, value.capability_generation)?;
            sink.put_fixed(value.attach_secret.as_bytes())?;
            sink.put_fixed(value.leave_attempt_token.as_bytes())
        }
        ClientRequest::MarkerAck(value) => {
            sink.put_u64(value.conversation_id)?;
            sink.put_u64(value.participant_id)?;
            put_generation(sink, value.capability_generation)?;
            sink.put_u64(value.marker_delivery_seq)
        }
        ClientRequest::RecordAdmission(value) => {
            sink.put_u64(value.conversation_id)?;
            sink.put_u64(value.participant_id)?;
            put_generation(sink, value.capability_generation)?;
            sink.put_bytes(&value.payload)
        }
        ClientRequest::ObserverRecovery(value) => {
            let count: u64 = value
                .observer_refusals
                .len()
                .try_into()
                .map_err(|_| CodecError::LengthOverflow)?;
            sink.put_u64(count)?;
            for refusal in &value.observer_refusals {
                sink.put_u64(refusal.conversation_id)?;
                sink.put_u64(refusal.refused_epoch)?;
            }
            Ok(())
        }
    }
}

fn decode_client_request(
    tag: ClientDiscriminant,
    body: &[u8],
) -> Result<ClientRequest, CodecError> {
    let mut reader = Reader::new(body);
    let value = match tag {
        ClientDiscriminant::EnrollmentRequest => ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: reader.take_u64()?,
            enrollment_token: EnrollmentToken::new(reader.take_fixed()?),
        }),
        ClientDiscriminant::CredentialAttachRequest => {
            ClientRequest::CredentialAttach(CredentialAttachRequest {
                conversation_id: reader.take_u64()?,
                participant_id: reader.take_u64()?,
                capability_generation: reader.take_generation()?,
                attach_secret: AttachSecret::new(reader.take_fixed()?),
                attach_attempt_token: AttachAttemptToken::new(reader.take_fixed()?),
                accept_marker_delivery_seq: reader.take_option_u64()?,
            })
        }
        ClientDiscriminant::DetachRequest => ClientRequest::Detach(DetachRequest {
            conversation_id: reader.take_u64()?,
            participant_id: reader.take_u64()?,
            capability_generation: reader.take_generation()?,
            detach_attempt_token: DetachAttemptToken::new(reader.take_fixed()?),
        }),
        ClientDiscriminant::ParticipantAck => ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: reader.take_u64()?,
            participant_id: reader.take_u64()?,
            capability_generation: reader.take_generation()?,
            through_seq: reader.take_u64()?,
        }),
        ClientDiscriminant::LeaveRequest => ClientRequest::Leave(LeaveRequest {
            conversation_id: reader.take_u64()?,
            participant_id: reader.take_u64()?,
            capability_generation: reader.take_generation()?,
            attach_secret: AttachSecret::new(reader.take_fixed()?),
            leave_attempt_token: LeaveAttemptToken::new(reader.take_fixed()?),
        }),
        ClientDiscriminant::MarkerAck => ClientRequest::MarkerAck(MarkerAck {
            conversation_id: reader.take_u64()?,
            participant_id: reader.take_u64()?,
            capability_generation: reader.take_generation()?,
            marker_delivery_seq: reader.take_u64()?,
        }),
        ClientDiscriminant::RecordAdmission => ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: reader.take_u64()?,
            participant_id: reader.take_u64()?,
            capability_generation: reader.take_generation()?,
            payload: reader.take_bytes()?,
        }),
        ClientDiscriminant::ObserverRecoveryHandshake => {
            let count = reader.take_u64()?;
            reader.require_fixed_list(count, 16)?;
            let mut observer_refusals = Vec::new();
            for _ in 0..count {
                observer_refusals.push(ObserverRefusal {
                    conversation_id: reader.take_u64()?,
                    refused_epoch: reader.take_u64()?,
                });
            }
            ClientRequest::ObserverRecovery(ObserverRecoveryHandshake { observer_refusals })
        }
    };
    reader.finish()?;
    Ok(value)
}

fn encode_server_push<S: Sink>(value: &ServerPush, sink: &mut S) -> Result<(), CodecError> {
    match value {
        ServerPush::ObserverProgressed {
            conversation_id,
            refused_epoch,
            observer_progress,
        } => {
            sink.put_u64(*conversation_id)?;
            sink.put_u64(*refused_epoch)?;
            sink.put_u64(*observer_progress)
        }
        ServerPush::ParticipantDelivery(value) => encode_participant_delivery(value, sink),
    }
}

fn encode_participant_delivery<S: Sink>(
    value: &ParticipantDelivery,
    sink: &mut S,
) -> Result<(), CodecError> {
    sink.put_u64(value.conversation_id)?;
    sink.put_u64(value.delivery_seq)?;
    sink.put_u16(value.record.record_kind().wire_value())?;
    match &value.record {
        ParticipantRecord::OrdinaryRecord {
            sender_participant_id,
            payload,
        } => {
            sink.put_u64(*sender_participant_id)?;
            sink.put_bytes(payload)
        }
        ParticipantRecord::Attached {
            affected_participant_id,
            binding_epoch,
        } => {
            sink.put_u64(*affected_participant_id)?;
            put_binding_epoch(sink, *binding_epoch)
        }
        ParticipantRecord::Detached {
            affected_participant_id,
            binding_epoch,
            cause,
        } => {
            sink.put_u64(*affected_participant_id)?;
            put_binding_epoch(sink, *binding_epoch)?;
            sink.put_u16(cause.close_cause().tag().wire_value())
        }
        ParticipantRecord::Died {
            affected_participant_id,
            binding_epoch,
            cause,
        } => {
            sink.put_u64(*affected_participant_id)?;
            put_binding_epoch(sink, *binding_epoch)?;
            sink.put_u16(cause.close_cause().tag().wire_value())?;
            if let DiedCause::UncleanServerRestart {
                prior_server_incarnation,
            } = cause
            {
                sink.put_u64(*prior_server_incarnation)?;
            }
            Ok(())
        }
        ParticipantRecord::Left {
            affected_participant_id,
            ended_binding_epoch,
        } => {
            sink.put_u64(*affected_participant_id)?;
            put_option_binding_epoch(sink, *ended_binding_epoch)
        }
        ParticipantRecord::HistoryCompacted {
            affected_participant_id,
            abandoned_after,
            abandoned_through,
            physical_floor_at_decision,
        } => {
            sink.put_u64(*affected_participant_id)?;
            sink.put_u64(*abandoned_after)?;
            sink.put_u64(*abandoned_through)?;
            sink.put_u64(*physical_floor_at_decision)
        }
    }
}

fn decode_server_push(tag: PushDiscriminant, body: &[u8]) -> Result<ServerPush, CodecError> {
    let mut reader = Reader::new(body);
    let value = match tag {
        PushDiscriminant::ObserverProgressed => ServerPush::ObserverProgressed {
            conversation_id: reader.take_u64()?,
            refused_epoch: reader.take_u64()?,
            observer_progress: reader.take_u64()?,
        },
        PushDiscriminant::ParticipantDelivery => {
            let conversation_id = reader.take_u64()?;
            let delivery_seq = reader.take_u64()?;
            let kind = RecordKind::try_from(reader.take_u16()?)
                .map_err(|_| decode_error(DecodeClass::InvalidField))?;
            let record = decode_participant_record(kind, &mut reader)?;
            ServerPush::ParticipantDelivery(ParticipantDelivery {
                conversation_id,
                delivery_seq,
                record,
            })
        }
    };
    reader.finish()?;
    Ok(value)
}

fn decode_participant_record(
    kind: RecordKind,
    reader: &mut Reader<'_>,
) -> Result<ParticipantRecord, CodecError> {
    match kind {
        RecordKind::OrdinaryRecord => Ok(ParticipantRecord::OrdinaryRecord {
            sender_participant_id: reader.take_u64()?,
            payload: reader.take_bytes()?,
        }),
        RecordKind::Attached => Ok(ParticipantRecord::Attached {
            affected_participant_id: reader.take_u64()?,
            binding_epoch: reader.take_binding_epoch()?,
        }),
        RecordKind::Detached => {
            let affected_participant_id = reader.take_u64()?;
            let binding_epoch = reader.take_binding_epoch()?;
            let tag = CloseCauseTag::try_from(reader.take_u16()?)
                .map_err(|_| decode_error(DecodeClass::InvalidField))?;
            let cause = match tag {
                CloseCauseTag::CleanDeregister => DetachedCause::CleanDeregister,
                CloseCauseTag::Superseded => DetachedCause::Superseded,
                CloseCauseTag::ServerShutdown => DetachedCause::ServerShutdown,
                _ => return Err(decode_error(DecodeClass::InvalidField)),
            };
            Ok(ParticipantRecord::Detached {
                affected_participant_id,
                binding_epoch,
                cause,
            })
        }
        RecordKind::Died => {
            let affected_participant_id = reader.take_u64()?;
            let binding_epoch = reader.take_binding_epoch()?;
            let tag = CloseCauseTag::try_from(reader.take_u16()?)
                .map_err(|_| decode_error(DecodeClass::InvalidField))?;
            let cause = match tag {
                CloseCauseTag::ConnectionLost => DiedCause::ConnectionLost,
                CloseCauseTag::ProcessKilled => DiedCause::ProcessKilled,
                CloseCauseTag::ProtocolError => DiedCause::ProtocolError,
                CloseCauseTag::UncleanServerRestart => DiedCause::UncleanServerRestart {
                    prior_server_incarnation: reader.take_u64()?,
                },
                _ => return Err(decode_error(DecodeClass::InvalidField)),
            };
            Ok(ParticipantRecord::Died {
                affected_participant_id,
                binding_epoch,
                cause,
            })
        }
        RecordKind::Left => Ok(ParticipantRecord::Left {
            affected_participant_id: reader.take_u64()?,
            ended_binding_epoch: reader.take_option_binding_epoch()?,
        }),
        RecordKind::HistoryCompacted => Ok(ParticipantRecord::HistoryCompacted {
            affected_participant_id: reader.take_u64()?,
            abandoned_after: reader.take_u64()?,
            abandoned_through: reader.take_u64()?,
            physical_floor_at_decision: reader.take_u64()?,
        }),
    }
}

fn put_generation<S: Sink>(sink: &mut S, generation: Generation) -> Result<(), CodecError> {
    sink.put_u64(generation.get())
}

fn put_connection_incarnation<S: Sink>(
    sink: &mut S,
    value: ConnectionIncarnation,
) -> Result<(), CodecError> {
    sink.put_u64(value.server_incarnation)?;
    sink.put_u64(value.connection_ordinal)
}

fn put_binding_epoch<S: Sink>(sink: &mut S, value: BindingEpoch) -> Result<(), CodecError> {
    put_connection_incarnation(sink, value.connection_incarnation)?;
    put_generation(sink, value.capability_generation)
}

fn put_option_u64<S: Sink>(sink: &mut S, value: Option<u64>) -> Result<(), CodecError> {
    match value {
        None => sink.put_u8(0),
        Some(value) => {
            sink.put_u8(1)?;
            sink.put_u64(value)
        }
    }
}

fn put_option_binding_epoch<S: Sink>(
    sink: &mut S,
    value: Option<BindingEpoch>,
) -> Result<(), CodecError> {
    match value {
        None => sink.put_u8(0),
        Some(value) => {
            sink.put_u8(1)?;
            put_binding_epoch(sink, value)
        }
    }
}

const fn decode_error(class: DecodeClass) -> CodecError {
    CodecError::Decode { class }
}

trait Sink {
    fn put(&mut self, bytes: &[u8]) -> Result<(), CodecError>;

    fn put_u8(&mut self, value: u8) -> Result<(), CodecError> {
        self.put(&[value])
    }

    fn put_u16(&mut self, value: u16) -> Result<(), CodecError> {
        self.put(&value.to_be_bytes())
    }

    fn put_u32(&mut self, value: u32) -> Result<(), CodecError> {
        self.put(&value.to_be_bytes())
    }

    fn put_u64(&mut self, value: u64) -> Result<(), CodecError> {
        self.put(&value.to_be_bytes())
    }

    fn put_fixed<const N: usize>(&mut self, value: &[u8; N]) -> Result<(), CodecError> {
        self.put(value)
    }

    fn put_bytes(&mut self, value: &[u8]) -> Result<(), CodecError> {
        let length: u32 = value
            .len()
            .try_into()
            .map_err(|_| CodecError::LengthOverflow)?;
        self.put_u32(length)?;
        self.put(value)
    }
}

#[derive(Default)]
struct SizeSink {
    len: usize,
}

impl Sink for SizeSink {
    fn put(&mut self, bytes: &[u8]) -> Result<(), CodecError> {
        self.len = self
            .len
            .checked_add(bytes.len())
            .ok_or(CodecError::LengthOverflow)?;
        Ok(())
    }
}

struct Writer<'a> {
    output: &'a mut [u8],
    position: usize,
}

impl<'a> Writer<'a> {
    const fn new(output: &'a mut [u8]) -> Self {
        Self {
            output,
            position: 0,
        }
    }
}

impl Sink for Writer<'_> {
    fn put(&mut self, bytes: &[u8]) -> Result<(), CodecError> {
        let end = self
            .position
            .checked_add(bytes.len())
            .ok_or(CodecError::LengthOverflow)?;
        let available = self.output.len();
        let Some(destination) = self.output.get_mut(self.position..end) else {
            return Err(CodecError::OutputTooSmall {
                required: end,
                available,
            });
        };
        destination.copy_from_slice(bytes);
        self.position = end;
        Ok(())
    }
}

struct Reader<'a> {
    input: &'a [u8],
    position: usize,
    invalid_field: bool,
}

impl<'a> Reader<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self {
            input,
            position: 0,
            invalid_field: false,
        }
    }

    const fn remaining(&self) -> usize {
        self.input.len().saturating_sub(self.position)
    }

    fn remaining_bytes(&self) -> &'a [u8] {
        match self.input.get(self.position..) {
            Some(bytes) => bytes,
            None => &[],
        }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], CodecError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| decode_error(DecodeClass::MissingRequiredField))?;
        let Some(bytes) = self.input.get(self.position..end) else {
            return Err(decode_error(DecodeClass::MissingRequiredField));
        };
        self.position = end;
        Ok(bytes)
    }

    fn take_fixed<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        let mut output = [0; N];
        output.copy_from_slice(self.take(N)?);
        Ok(output)
    }

    fn take_u8(&mut self) -> Result<u8, CodecError> {
        Ok(u8::from_be_bytes(self.take_fixed()?))
    }

    fn take_u16(&mut self) -> Result<u16, CodecError> {
        Ok(u16::from_be_bytes(self.take_fixed()?))
    }

    fn take_u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_be_bytes(self.take_fixed()?))
    }

    fn take_u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_be_bytes(self.take_fixed()?))
    }

    fn take_generation(&mut self) -> Result<Generation, CodecError> {
        let raw = self.take_u64()?;
        if let Some(value) = Generation::new(raw) {
            return Ok(value);
        }
        self.invalid_field = true;
        Generation::new(1).ok_or(decode_error(DecodeClass::InvalidField))
    }

    fn take_binding_epoch(&mut self) -> Result<BindingEpoch, CodecError> {
        Ok(BindingEpoch::new(
            ConnectionIncarnation::new(self.take_u64()?, self.take_u64()?),
            self.take_generation()?,
        ))
    }

    fn take_option_u64(&mut self) -> Result<Option<u64>, CodecError> {
        match self.take_u8()? {
            0 => Ok(None),
            1 => self.take_u64().map(Some),
            _ => Err(decode_error(DecodeClass::InvalidField)),
        }
    }

    fn take_option_binding_epoch(&mut self) -> Result<Option<BindingEpoch>, CodecError> {
        match self.take_u8()? {
            0 => Ok(None),
            1 => self.take_binding_epoch().map(Some),
            _ => Err(decode_error(DecodeClass::InvalidField)),
        }
    }

    fn take_bytes(&mut self) -> Result<Vec<u8>, CodecError> {
        let length: usize = self
            .take_u32()?
            .try_into()
            .map_err(|_| decode_error(DecodeClass::MissingRequiredField))?;
        Ok(self.take(length)?.to_vec())
    }

    fn require_fixed_list(&self, count: u64, width: usize) -> Result<(), CodecError> {
        let width =
            u128::try_from(width).map_err(|_| decode_error(DecodeClass::MissingRequiredField))?;
        let remaining = u128::try_from(self.remaining())
            .map_err(|_| decode_error(DecodeClass::MissingRequiredField))?;
        let required = u128::from(count)
            .checked_mul(width)
            .ok_or_else(|| decode_error(DecodeClass::MissingRequiredField))?;
        if required > remaining {
            return Err(decode_error(DecodeClass::MissingRequiredField));
        }
        Ok(())
    }

    const fn finish(self) -> Result<(), CodecError> {
        if self.position != self.input.len() {
            return Err(decode_error(DecodeClass::CanonicalEncoding));
        }
        if self.invalid_field {
            return Err(decode_error(DecodeClass::InvalidField));
        }
        Ok(())
    }
}
