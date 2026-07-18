//! Deterministic body codec for the contiguous server-value registry.
//!
//! The complete-frame codec delegates only the semantic body to this module.
//! In particular, `originating_request` and observer-recovery `status_count`
//! are routing selectors: invalid routes fail before an unread suffix is
//! interpreted.

use alloc::{string::String, vec::Vec};

use crate::algebra::{ResourceDimension, ResourceVector, WideResourceVector};

use super::codec::CodecError;
use super::{
    AttachAttemptToken, AttachSecret, BindingEpoch, ConnectionIncarnation, DetachAttemptToken,
    EnrollmentToken, Generation, LeaveAttemptToken, ProtocolVersion, RecordAdmissionAttemptToken,
    closure as c, envelope as e, response as r, tags as t,
};

const TOKEN_LEN: usize = 16;
const SECRET_LEN: usize = 32;
const RECOVERY_STATUS_LEN: u128 = 26;

/// Unforgeable outside this module; authorizes construction from authenticated
/// wire bytes without weakening the server-side terminalized-state signature.
pub(super) struct TerminalizedWireDecodeAuthority(());

const TERMINALIZED_WIRE_DECODE_AUTHORITY: TerminalizedWireDecodeAuthority =
    TerminalizedWireDecodeAuthority(());

/// Encodes a semantic server value's exact v1 body.
///
/// The returned discriminant belongs with the returned body and cannot drift
/// from the selected Rust variant.
///
/// # Errors
///
/// Returns [`CodecError::UnsupportedVersion`] for a version other than v1.0,
/// [`CodecError::LengthOverflow`] for a variable field that cannot fit its
/// canonical length prefix, or [`CodecError::InvalidValue`] when a typed value
/// violates a fixed wire invariant.
pub fn encode_server_value_body(
    value: &r::ServerValue,
    version: ProtocolVersion,
) -> Result<(t::ServerDiscriminant, Vec<u8>), CodecError> {
    require_v1(version)?;
    let discriminant = value.discriminant();
    let mut encoder = Encoder::default();

    if (0x0101..=0x0120).contains(&discriminant.wire_value()) {
        let originating_request = value
            .originating_request()
            .ok_or(CodecError::InvalidValue)?;
        encoder.put_u16(originating_request.wire_value());
    }

    encode_server_suffix(value, &mut encoder)?;
    Ok((discriminant, encoder.bytes))
}

/// Decodes one exact semantic server-value body under v1.0.
///
/// The concrete protocol version is returned beside the decoded value so a
/// complete-frame caller can preserve its already-validated prefix value.
///
/// # Errors
///
/// Returns the canonical structural decode class, or
/// [`CodecError::UnsupportedVersion`] when `version` is not v1.0.
pub fn decode_server_value_body(
    discriminant: t::ServerDiscriminant,
    version: ProtocolVersion,
    body: &[u8],
) -> Result<(r::ServerValue, ProtocolVersion), CodecError> {
    require_v1(version)?;
    let mut decoder = Decoder::new(body);
    let originating_request = if (0x0101..=0x0120).contains(&discriminant.wire_value()) {
        let raw = decoder.take_u16()?;
        let request = t::ClientDiscriminant::try_from(raw)
            .map_err(|_| decode_error(t::DecodeClass::InvalidField))?;
        if !origin_is_valid(discriminant, request) {
            return Err(decode_error(t::DecodeClass::InvalidField));
        }
        Some(request)
    } else {
        None
    };

    let value = decode_server_suffix(discriminant, originating_request, &mut decoder)?;
    decoder.finish()?;
    Ok((value, version))
}

fn require_v1(version: ProtocolVersion) -> Result<(), CodecError> {
    if version == ProtocolVersion::V1 {
        Ok(())
    } else {
        Err(CodecError::UnsupportedVersion {
            presented: version,
            supported: ProtocolVersion::V1,
        })
    }
}

const fn decode_error(class: t::DecodeClass) -> CodecError {
    CodecError::Decode { class }
}

#[derive(Default)]
struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    fn put_u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn put_u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn put_u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn put_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn put_u128(&mut self, value: u128) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn put_fixed(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    fn put_bool(&mut self, value: bool) {
        self.put_u8(u8::from(value));
    }

    fn put_option_u64(&mut self, value: Option<u64>) {
        match value {
            Some(value) => {
                self.put_u8(1);
                self.put_u64(value);
            }
            None => self.put_u8(0),
        }
    }

    fn put_option_generation(&mut self, value: Option<Generation>) {
        match value {
            Some(value) => {
                self.put_u8(1);
                self.put_generation(value);
            }
            None => self.put_u8(0),
        }
    }

    fn put_option_binding_epoch(&mut self, value: Option<BindingEpoch>) {
        match value {
            Some(value) => {
                self.put_u8(1);
                self.put_binding_epoch(value);
            }
            None => self.put_u8(0),
        }
    }

    fn put_generation(&mut self, value: Generation) {
        self.put_u64(value.get());
    }

    fn put_protocol_version(&mut self, value: ProtocolVersion) {
        self.put_u16(value.major);
        self.put_u16(value.minor);
    }

    fn put_binding_epoch(&mut self, value: BindingEpoch) {
        self.put_u64(value.connection_incarnation.server_incarnation);
        self.put_u64(value.connection_incarnation.connection_ordinal);
        self.put_generation(value.capability_generation);
    }

    fn put_resource_vector(&mut self, value: ResourceVector) {
        self.put_u64(value.entries);
        self.put_u64(value.bytes);
    }

    fn put_wide_resource_vector(&mut self, value: WideResourceVector) {
        self.put_u128(value.entries);
        self.put_u128(value.bytes);
    }

    fn put_string(&mut self, value: &str) -> Result<(), CodecError> {
        let length: u32 = value
            .len()
            .try_into()
            .map_err(|_| CodecError::LengthOverflow)?;
        self.put_u32(length);
        self.put_fixed(value.as_bytes());
        Ok(())
    }
}

struct Decoder<'a> {
    input: &'a [u8],
    position: usize,
    invalid_field: bool,
}

impl<'a> Decoder<'a> {
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

    fn take(&mut self, length: usize) -> Result<&'a [u8], CodecError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| decode_error(t::DecodeClass::MissingRequiredField))?;
        let value = self
            .input
            .get(self.position..end)
            .ok_or_else(|| decode_error(t::DecodeClass::MissingRequiredField))?;
        self.position = end;
        Ok(value)
    }

    fn take_u8(&mut self) -> Result<u8, CodecError> {
        let bytes = self.take(1)?;
        bytes
            .first()
            .copied()
            .ok_or_else(|| decode_error(t::DecodeClass::MissingRequiredField))
    }

    fn take_u16(&mut self) -> Result<u16, CodecError> {
        let bytes: [u8; 2] = self
            .take(2)?
            .try_into()
            .map_err(|_| decode_error(t::DecodeClass::MissingRequiredField))?;
        Ok(u16::from_be_bytes(bytes))
    }

    fn take_u32(&mut self) -> Result<u32, CodecError> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| decode_error(t::DecodeClass::MissingRequiredField))?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn take_u64(&mut self) -> Result<u64, CodecError> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| decode_error(t::DecodeClass::MissingRequiredField))?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn take_u128(&mut self) -> Result<u128, CodecError> {
        let bytes: [u8; 16] = self
            .take(16)?
            .try_into()
            .map_err(|_| decode_error(t::DecodeClass::MissingRequiredField))?;
        Ok(u128::from_be_bytes(bytes))
    }

    fn take_fixed<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        self.take(N)?
            .try_into()
            .map_err(|_| decode_error(t::DecodeClass::MissingRequiredField))
    }

    fn take_bool(&mut self) -> Result<bool, CodecError> {
        match self.take_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(decode_error(t::DecodeClass::InvalidField)),
        }
    }

    fn take_option_u64(&mut self) -> Result<Option<u64>, CodecError> {
        match self.take_u8()? {
            0 => Ok(None),
            1 => self.take_u64().map(Some),
            _ => Err(decode_error(t::DecodeClass::InvalidField)),
        }
    }

    fn take_generation(&mut self) -> Result<Generation, CodecError> {
        let raw = self.take_u64()?;
        if let Some(value) = Generation::new(raw) {
            Ok(value)
        } else {
            self.invalid_field = true;
            generation_one()
        }
    }

    fn take_option_generation(&mut self) -> Result<Option<Generation>, CodecError> {
        match self.take_u8()? {
            0 => Ok(None),
            1 => self.take_generation().map(Some),
            _ => Err(decode_error(t::DecodeClass::InvalidField)),
        }
    }

    fn take_binding_epoch(&mut self) -> Result<BindingEpoch, CodecError> {
        Ok(BindingEpoch::new(
            ConnectionIncarnation::new(self.take_u64()?, self.take_u64()?),
            self.take_generation()?,
        ))
    }

    fn take_option_binding_epoch(&mut self) -> Result<Option<BindingEpoch>, CodecError> {
        match self.take_u8()? {
            0 => Ok(None),
            1 => self.take_binding_epoch().map(Some),
            _ => Err(decode_error(t::DecodeClass::InvalidField)),
        }
    }

    fn take_protocol_version(&mut self) -> Result<ProtocolVersion, CodecError> {
        Ok(ProtocolVersion::new(self.take_u16()?, self.take_u16()?))
    }

    fn take_resource_vector(&mut self) -> Result<ResourceVector, CodecError> {
        Ok(ResourceVector::new(self.take_u64()?, self.take_u64()?))
    }

    fn take_wide_resource_vector(&mut self) -> Result<WideResourceVector, CodecError> {
        Ok(WideResourceVector::new(
            self.take_u128()?,
            self.take_u128()?,
        ))
    }

    fn take_string(&mut self) -> Result<String, CodecError> {
        let length: usize = self
            .take_u32()?
            .try_into()
            .map_err(|_| decode_error(t::DecodeClass::MissingRequiredField))?;
        let bytes = self.take(length)?;
        if let Ok(value) = core::str::from_utf8(bytes) {
            Ok(String::from(value))
        } else {
            self.invalid_field = true;
            Ok(String::new())
        }
    }

    const fn invalidate(&mut self) {
        self.invalid_field = true;
    }

    const fn finish(self) -> Result<(), CodecError> {
        if self.remaining() != 0 {
            Err(decode_error(t::DecodeClass::CanonicalEncoding))
        } else if self.invalid_field {
            Err(decode_error(t::DecodeClass::InvalidField))
        } else {
            Ok(())
        }
    }
}

fn generation_one() -> Result<Generation, CodecError> {
    Generation::new(1).ok_or(CodecError::InvalidValue)
}

fn put_enrollment(value: &e::EnrollmentEnvelope, encoder: &mut Encoder) {
    encoder.put_u64(value.conversation_id);
    encoder.put_fixed(value.enrollment_token.as_bytes());
}

fn put_attach(value: &e::AttachEnvelope, encoder: &mut Encoder) {
    encoder.put_u64(value.conversation_id);
    encoder.put_u64(value.participant_id);
    encoder.put_generation(value.capability_generation);
    encoder.put_fixed(value.attach_attempt_token.as_bytes());
    encoder.put_option_u64(value.accept_marker_delivery_seq);
}

fn put_detach(value: &e::DetachEnvelope, encoder: &mut Encoder) {
    encoder.put_u64(value.conversation_id);
    encoder.put_u64(value.participant_id);
    encoder.put_generation(value.capability_generation);
    encoder.put_fixed(value.detach_attempt_token.as_bytes());
}

fn put_participant_ack(value: &e::ParticipantAckEnvelope, encoder: &mut Encoder) {
    encoder.put_u64(value.conversation_id);
    encoder.put_u64(value.participant_id);
    encoder.put_generation(value.capability_generation);
    encoder.put_u64(value.through_seq);
}

fn put_leave(value: &e::LeaveEnvelope, encoder: &mut Encoder) {
    encoder.put_u64(value.conversation_id);
    encoder.put_u64(value.participant_id);
    encoder.put_generation(value.capability_generation);
    encoder.put_fixed(value.leave_attempt_token.as_bytes());
}

fn put_marker_ack(value: &e::MarkerAckEnvelope, encoder: &mut Encoder) {
    encoder.put_u64(value.conversation_id);
    encoder.put_u64(value.participant_id);
    encoder.put_generation(value.capability_generation);
    encoder.put_u64(value.marker_delivery_seq);
}

fn put_record_admission(value: &e::RecordAdmissionEnvelope, encoder: &mut Encoder) {
    encoder.put_u64(value.conversation_id);
    encoder.put_u64(value.participant_id);
    encoder.put_generation(value.capability_generation);
    encoder.put_fixed(value.record_admission_attempt_token.as_bytes());
}

fn put_response_envelope(value: &e::ResponseEnvelope, encoder: &mut Encoder) {
    match value {
        e::ResponseEnvelope::Enrollment(value) => put_enrollment(value, encoder),
        e::ResponseEnvelope::CredentialAttach(value) => put_attach(value, encoder),
        e::ResponseEnvelope::Detach(value) => put_detach(value, encoder),
        e::ResponseEnvelope::ParticipantAck(value) => put_participant_ack(value, encoder),
        e::ResponseEnvelope::Leave(value) => put_leave(value, encoder),
        e::ResponseEnvelope::MarkerAck(value) => put_marker_ack(value, encoder),
        e::ResponseEnvelope::RecordAdmission(value) => put_record_admission(value, encoder),
    }
}

fn put_participant_reference(value: &r::ParticipantReferenceEnvelope, encoder: &mut Encoder) {
    match value {
        r::ParticipantReferenceEnvelope::CredentialAttach(value) => put_attach(value, encoder),
        r::ParticipantReferenceEnvelope::Detach(value) => put_detach(value, encoder),
        r::ParticipantReferenceEnvelope::ParticipantAck(value) => {
            put_participant_ack(value, encoder);
        }
        r::ParticipantReferenceEnvelope::Leave(value) => put_leave(value, encoder),
        r::ParticipantReferenceEnvelope::MarkerAck(value) => put_marker_ack(value, encoder),
        r::ParticipantReferenceEnvelope::RecordAdmission(value) => {
            put_record_admission(value, encoder);
        }
    }
}

fn put_binding_required(value: &r::BindingRequiredEnvelope, encoder: &mut Encoder) {
    match value {
        r::BindingRequiredEnvelope::Detach(value) => put_detach(value, encoder),
        r::BindingRequiredEnvelope::ParticipantAck(value) => put_participant_ack(value, encoder),
        r::BindingRequiredEnvelope::Leave(value) => put_leave(value, encoder),
        r::BindingRequiredEnvelope::MarkerAck(value) => put_marker_ack(value, encoder),
        r::BindingRequiredEnvelope::RecordAdmission(value) => put_record_admission(value, encoder),
    }
}

fn put_order_allocating(value: &r::OrderAllocatingEnvelope, encoder: &mut Encoder) {
    match value {
        r::OrderAllocatingEnvelope::Enrollment(value) => put_enrollment(value, encoder),
        r::OrderAllocatingEnvelope::CredentialAttach(value) => put_attach(value, encoder),
        r::OrderAllocatingEnvelope::RecordAdmission(value) => put_record_admission(value, encoder),
    }
}

fn put_closure_checked(value: &c::ClosureCheckedEnvelope, encoder: &mut Encoder) {
    match value {
        c::ClosureCheckedEnvelope::Enrollment(value) => put_enrollment(value, encoder),
        c::ClosureCheckedEnvelope::CredentialAttach(value) => put_attach(value, encoder),
        c::ClosureCheckedEnvelope::Leave(value) => put_leave(value, encoder),
        c::ClosureCheckedEnvelope::RecordAdmission(value) => put_record_admission(value, encoder),
    }
}

fn put_sequence_allocating(value: &r::SequenceAllocatingEnvelope, encoder: &mut Encoder) {
    match value {
        r::SequenceAllocatingEnvelope::Enrollment(value) => put_enrollment(value, encoder),
        r::SequenceAllocatingEnvelope::CredentialAttach(value) => put_attach(value, encoder),
        r::SequenceAllocatingEnvelope::RecordAdmission(value) => {
            put_record_admission(value, encoder);
        }
    }
}

fn put_marker_proof(value: &r::MarkerProofRequest, encoder: &mut Encoder) {
    match value {
        r::MarkerProofRequest::CredentialAttach(value) => {
            encoder.put_u64(value.conversation_id);
            encoder.put_fixed(value.token.as_bytes());
            encoder.put_u64(value.participant_id);
            encoder.put_generation(value.capability_generation);
            encoder.put_u64(value.requested_marker_delivery_seq);
        }
        r::MarkerProofRequest::MarkerAck(value) => {
            encoder.put_u64(value.conversation_id);
            encoder.put_u64(value.participant_id);
            encoder.put_generation(value.capability_generation);
            encoder.put_u64(value.requested_marker_delivery_seq);
        }
    }
}

fn put_repayment_edge(value: c::RepaymentEdge, encoder: &mut Encoder) {
    encoder.put_u16(value.tag().wire_value());
    match value {
        c::RepaymentEdge::None => {}
        c::RepaymentEdge::ObserverProjection { through_seq } => encoder.put_u64(through_seq),
        c::RepaymentEdge::PhysicalCompaction {
            from_floor,
            through_seq,
        } => {
            encoder.put_u64(from_floor);
            encoder.put_u64(through_seq);
        }
        c::RepaymentEdge::MarkerDelivery {
            participant_id,
            binding_epoch,
            marker_delivery_seq,
        } => {
            encoder.put_u64(participant_id);
            encoder.put_binding_epoch(binding_epoch);
            encoder.put_u64(marker_delivery_seq);
        }
        c::RepaymentEdge::ParticipantCursorProgress(value) => {
            encoder.put_u64(value.participant_id);
            encoder.put_binding_epoch(value.binding_epoch);
            encoder.put_u64(value.through_seq);
            encoder.put_option_u64(value.marker_delivery_seq);
        }
        c::RepaymentEdge::DetachedCredentialRecovery {
            participant_id,
            marker_delivery_seq,
            prior_binding_epoch,
        } => {
            encoder.put_u64(participant_id);
            encoder.put_u64(marker_delivery_seq);
            encoder.put_binding_epoch(prior_binding_epoch);
        }
        c::RepaymentEdge::DetachedMarkerRelease {
            participant_id,
            marker_delivery_seq,
            last_dead_binding_epoch,
        } => {
            encoder.put_u64(participant_id);
            encoder.put_u64(marker_delivery_seq);
            encoder.put_binding_epoch(last_dead_binding_epoch);
        }
        c::RepaymentEdge::DetachedCursorRelease {
            participant_id,
            last_dead_binding_epoch,
        } => {
            encoder.put_u64(participant_id);
            encoder.put_binding_epoch(last_dead_binding_epoch);
        }
    }
}

fn put_closure_snapshot(value: c::ClosureSnapshot, encoder: &mut Encoder) {
    encoder.put_u64(value.marker_capacity_credits);
    encoder.put_u64(value.marker_anchors);
    encoder.put_u64(value.entry_debt);
    encoder.put_u64(value.byte_debt);
    put_repayment_edge(value.repayment_edge, encoder);
    encoder.put_u64(value.edge_sequence_claims);
    encoder.put_u64(value.edge_order_position_claims);
    encoder.put_resource_vector(value.edge_k_remaining);
    encoder.put_wide_resource_vector(value.k_headroom);
    encoder.put_u64(value.episode_churn_used);
    encoder.put_u64(value.delta_cycles);
    encoder.put_u64(value.episode_churn_limit);
}

fn put_sequence_budget(value: super::SequenceBudget, encoder: &mut Encoder) {
    encoder.put_u64(value.high_watermark);
    encoder.put_u64(value.remaining);
    encoder.put_u64(value.e);
    encoder.put_u64(value.t);
    encoder.put_u64(value.m);
    encoder.put_u64(value.rs);
    encoder.put_u64(value.rt);
    encoder.put_u128(value.l_times_t);
    encoder.put_u128(value.l_times_rt);
    encoder.put_u128(value.l_other_times_e);
}

#[allow(clippy::too_many_lines)]
fn encode_server_suffix(value: &r::ServerValue, encoder: &mut Encoder) -> Result<(), CodecError> {
    match value {
        r::ServerValue::ParticipantTransportRejected(value) => match &value.reason {
            r::TransportRejectionReason::FrameTooLarge {
                complete_frame_bytes,
                max_frame_bytes,
            } => {
                encoder.put_u16(t::TransportReasonTag::FrameTooLarge.wire_value());
                encoder.put_u64(*complete_frame_bytes);
                encoder.put_u64(*max_frame_bytes);
            }
            r::TransportRejectionReason::DecodeFailed { decode_class } => {
                encoder.put_u16(t::TransportReasonTag::DecodeFailed.wire_value());
                encoder.put_u16(decode_class.wire_value());
            }
            r::TransportRejectionReason::UnsupportedVersion {
                presented_version,
                supported_version,
            } => {
                encoder.put_u16(t::TransportReasonTag::UnsupportedVersion.wire_value());
                encoder.put_protocol_version(*presented_version);
                encoder.put_protocol_version(*supported_version);
            }
            r::TransportRejectionReason::AuthenticationFailed => {
                encoder.put_u16(t::TransportReasonTag::AuthenticationFailed.wire_value());
            }
            r::TransportRejectionReason::ParticipantCapabilityRequired => {
                encoder.put_u16(t::TransportReasonTag::ParticipantCapabilityRequired.wire_value());
                encoder.put_string(r::PARTICIPANT_CAPABILITY)?;
            }
        },
        r::ServerValue::AttemptTokenBodyConflict(value) => match value {
            r::AttemptTokenBodyConflict::CredentialAttach {
                token,
                conversation_id,
                presented_participant_id,
                presented_generation,
                presented_marker_delivery_seq,
                conflict,
            } => {
                encoder.put_fixed(token.as_bytes());
                encoder.put_u16(t::AttemptOperation::CredentialAttachRequest.wire_value());
                encoder.put_u64(*conversation_id);
                encoder.put_u64(*presented_participant_id);
                encoder.put_generation(*presented_generation);
                encoder.put_option_u64(*presented_marker_delivery_seq);
                encoder.put_u16(conflict.wire_value());
            }
            r::AttemptTokenBodyConflict::Leave {
                token,
                conversation_id,
                presented_participant_id,
                presented_generation,
            } => {
                encoder.put_fixed(token.as_bytes());
                encoder.put_u16(t::AttemptOperation::LeaveRequest.wire_value());
                encoder.put_u64(*conversation_id);
                encoder.put_u64(*presented_participant_id);
                encoder.put_generation(*presented_generation);
                encoder.put_u16(t::AttemptConflict::Generation.wire_value());
            }
        },
        r::ServerValue::ConnectionConversationCapacityExceeded(value) => match value {
            r::ConnectionConversationCapacityExceeded::SemanticRequest { request, limit } => {
                put_response_envelope(request, encoder);
                encoder.put_u64(*limit);
            }
            r::ConnectionConversationCapacityExceeded::ObserverRecovery {
                conversation_id,
                limit,
            } => {
                encoder.put_u64(0);
                encoder.put_u64(*conversation_id);
                encoder.put_u64(*limit);
            }
        },
        r::ServerValue::ConnectionConversationBindingOccupied(value) => match value {
            r::ConnectionConversationBindingOccupied::Enrollment {
                conversation_id,
                enrollment_token,
            } => {
                encoder.put_u64(*conversation_id);
                encoder.put_fixed(enrollment_token.as_bytes());
                encoder.put_option_u64(None);
            }
            r::ConnectionConversationBindingOccupied::CredentialAttach {
                conversation_id,
                participant_id,
                capability_generation,
                attach_attempt_token,
                accept_marker_delivery_seq,
            } => {
                encoder.put_u64(*conversation_id);
                encoder.put_u64(*participant_id);
                encoder.put_generation(*capability_generation);
                encoder.put_fixed(attach_attempt_token.as_bytes());
                encoder.put_option_u64(*accept_marker_delivery_seq);
                encoder.put_option_u64(Some(*participant_id));
            }
        },
        r::ServerValue::ConversationOrderExhausted(value) => {
            put_order_allocating(value.request(), encoder);
            encoder.put_u16(value.counter().wire_value());
            encoder.put_u64(value.high());
            encoder.put_option_u64(value.next_value());
            encoder.put_u128(value.order_remaining());
            encoder.put_u128(value.reserved_claims());
            encoder.put_u64(r::ConversationOrderExhausted::REQUIRED_MAJORS);
            encoder.put_u128(value.resulting_order_remaining());
            encoder.put_u128(value.resulting_reserved_claims());
        }
        r::ServerValue::ParticipantUnknown(value) => {
            put_participant_reference(&value.request, encoder);
        }
        r::ServerValue::NoBinding(value) => put_binding_required(&value.request, encoder),
        r::ServerValue::StaleAuthority(value) => put_stale_authority(value, encoder),
        r::ServerValue::Retired(value) => match value {
            r::Retired::Enrollment {
                request,
                participant_id,
                retired_generation,
            } => {
                put_enrollment(request, encoder);
                encoder.put_u64(*participant_id);
                encoder.put_generation(*retired_generation);
            }
            r::Retired::Participant {
                request,
                retired_generation,
            } => {
                put_participant_reference(request, encoder);
                encoder.put_generation(*retired_generation);
            }
        },
        r::ServerValue::MarkerClosureCapacityExceeded(value) => {
            put_closure_checked(&value.request, encoder);
            let scope = match value.reason {
                c::ClosureRefusalReason::Capacity(_) => t::ClosureScope::Capacity,
                c::ClosureRefusalReason::RecoveryFence => t::ClosureScope::RecoveryFence,
                c::ClosureRefusalReason::DeliveredMarkerAwaitingAck => {
                    t::ClosureScope::DeliveredMarkerAwaitingAck
                }
                c::ClosureRefusalReason::EpisodeChurnLimit => t::ClosureScope::EpisodeChurnLimit,
            };
            encoder.put_u16(scope.wire_value());
            put_closure_snapshot(value.snapshot, encoder);
            if let c::ClosureRefusalReason::Capacity(reason) = value.reason {
                encoder.put_u16(t::ResourceDimensionTag::from(reason.dimension).wire_value());
                encoder.put_u128(reason.required);
                encoder.put_u128(reason.limit);
            }
        }
        r::ServerValue::EnrollBound(value) => {
            encoder.put_u64(value.conversation_id());
            encoder.put_fixed(value.token().as_bytes());
            encoder.put_u64(value.participant_id());
            encoder.put_option_generation(value.request_generation());
            encoder.put_generation(value.capability_generation());
            encoder.put_fixed(value.attach_secret().as_bytes());
            encoder.put_binding_epoch(value.origin_binding_epoch());
            encoder.put_u64(value.persisted_cursor());
            encoder.put_option_u64(value.accepted_marker_delivery_seq());
            encoder.put_u128(value.receipt_expires_at());
            encoder.put_u128(value.provenance_expires_at());
        }
        r::ServerValue::EnrollmentKnown(value) => {
            encoder.put_u64(value.conversation_id);
            encoder.put_fixed(value.token.as_bytes());
            encoder.put_u64(value.participant_id);
            encoder.put_generation(value.current_generation);
        }
        r::ServerValue::ReceiptExpired(value) => put_receipt_expired(value, encoder),
        r::ServerValue::ReceiptCapacityExceeded(value) => {
            put_receipt_capacity(value, encoder);
        }
        r::ServerValue::IdentityCapacityExceeded(value) => {
            put_enrollment(&value.request, encoder);
            encoder.put_u16(value.scope.wire_value());
            encoder.put_u64(value.limit);
            encoder.put_u64(value.occupied);
            encoder.put_u64(r::IdentityCapacityExceeded::REQUESTED);
        }
        r::ServerValue::ObserverBackpressure(value) => put_observer_backpressure(value, encoder),
        r::ServerValue::ConversationSequenceExhausted(value) => {
            put_sequence_allocating(&value.request, encoder);
            put_sequence_budget(value.sequence_budget, encoder);
        }
        r::ServerValue::AttachBound(value) => {
            encoder.put_u64(value.conversation_id());
            encoder.put_fixed(value.token().as_bytes());
            encoder.put_u64(value.participant_id());
            encoder.put_option_generation(Some(value.request_generation()));
            encoder.put_generation(value.capability_generation());
            encoder.put_fixed(value.attach_secret().as_bytes());
            encoder.put_binding_epoch(value.origin_binding_epoch());
            encoder.put_u64(value.persisted_cursor());
            encoder.put_option_u64(value.accepted_marker_delivery_seq());
            encoder.put_u128(value.receipt_expires_at());
            encoder.put_u128(value.provenance_expires_at());
        }
        r::ServerValue::StaleOrUnknownReceipt(value) => {
            encoder.put_u64(value.conversation_id);
            encoder.put_fixed(value.token.as_bytes());
            encoder.put_u64(value.participant_id);
            encoder.put_generation(value.presented_generation);
            encoder.put_option_u64(value.presented_marker_delivery_seq);
            encoder.put_generation(value.current_generation);
        }
        r::ServerValue::MarkerNotDelivered(value) => {
            put_marker_proof(&value.request, encoder);
            encoder.put_u16(value.reason.wire_value());
            encoder.put_u64(value.expected_marker_delivery_seq);
        }
        r::ServerValue::MarkerMismatch(value) => {
            put_marker_proof(&value.request, encoder);
            encoder.put_u16(value.mismatch.reason().wire_value());
            match value.mismatch {
                r::MarkerMismatchBody::BelowCursor { current_cursor } => {
                    encoder.put_u64(current_cursor);
                }
                r::MarkerMismatchBody::NoMarkerExpected => {}
                r::MarkerMismatchBody::ExpectedDifferentMarker {
                    expected_marker_delivery_seq,
                } => encoder.put_u64(expected_marker_delivery_seq),
            }
        }
        r::ServerValue::Bound(value) | r::ServerValue::UnboundReceipt(value) => {
            put_receipt_replay(value, encoder);
        }
        r::ServerValue::DetachCommitted(value) => {
            encoder.put_u64(value.conversation_id());
            encoder.put_u64(value.participant_id());
            encoder.put_generation(value.capability_generation());
            encoder.put_fixed(value.detach_attempt_token().as_bytes());
            encoder.put_binding_epoch(value.committed_binding_epoch());
            encoder.put_u64(value.detached_delivery_seq());
        }
        r::ServerValue::DetachInProgress(value) => {
            encoder.put_u64(value.conversation_id);
            encoder.put_u64(value.participant_id);
            encoder.put_fixed(value.presented_token.as_bytes());
            encoder.put_generation(value.presented_generation);
            encoder.put_binding_epoch(value.committed_binding_epoch);
        }
        r::ServerValue::AckCommitted(value) => {
            put_participant_ack(value.request(), encoder);
            encoder.put_u64(value.current_cursor());
        }
        r::ServerValue::AckNoOp(value) => match value {
            r::AckNoOp::ParticipantAck(request) => {
                put_participant_ack(request, encoder);
                encoder.put_u64(value.current_cursor());
            }
            r::AckNoOp::MarkerAck(request) => {
                put_marker_ack(request, encoder);
                encoder.put_u64(value.current_cursor());
            }
        },
        r::ServerValue::AckGap(value) => {
            put_participant_ack(value.request(), encoder);
            encoder.put_u64(value.current_cursor());
            encoder.put_u16(value.reason().wire_value());
        }
        r::ServerValue::AckRegression(value) => {
            put_participant_ack(value.request(), encoder);
            encoder.put_u64(value.current_cursor());
            encoder.put_u16(value.reason().wire_value());
        }
        r::ServerValue::LeaveCommitted(value) => {
            encoder.put_u64(value.conversation_id());
            encoder.put_fixed(value.leave_attempt_token().as_bytes());
            encoder.put_u64(value.participant_id());
            encoder.put_generation(value.presented_generation());
            encoder.put_generation(value.retired_generation());
            encoder.put_option_binding_epoch(value.ended_binding_epoch());
            encoder.put_option_u64(value.prior_terminal_delivery_seq());
            encoder.put_u64(value.left_delivery_seq());
        }
        r::ServerValue::MarkerAckCommitted(value) => {
            put_marker_ack(value.request(), encoder);
            encoder.put_u64(value.current_cursor());
        }
        r::ServerValue::RecordCommitted(value) => {
            put_record_admission(value.request(), encoder);
            encoder.put_u64(value.sender_participant_id());
            encoder.put_u64(value.delivery_seq());
        }
        r::ServerValue::RecordTooLarge(value) => {
            put_record_admission(&value.request, encoder);
            encoder.put_u16(t::ResourceDimensionTag::from(value.dimension).wire_value());
            encoder.put_resource_vector(value.encoded_record_charge);
            encoder.put_resource_vector(value.max_ordinary_record_charge);
        }
        r::ServerValue::ObserverRecoveryAccepted(value) => {
            let count: u64 = value
                .statuses
                .len()
                .try_into()
                .map_err(|_| CodecError::LengthOverflow)?;
            encoder.put_u64(count);
            for status in &value.statuses {
                encoder.put_u64(status.conversation_id);
                encoder.put_u64(status.refused_epoch);
                encoder.put_u64(status.current_observer_progress);
                encoder.put_bool(status.armed);
                encoder.put_bool(status.progressed);
            }
        }
        r::ServerValue::InvalidObserverEpoch(value) => {
            encoder.put_u64(0);
            encoder.put_u16(value.reason().wire_value());
            match value {
                r::InvalidObserverEpoch::ConversationUnknown {
                    conversation_id,
                    presented_epoch,
                } => {
                    encoder.put_u64(*conversation_id);
                    encoder.put_u64(*presented_epoch);
                    encoder.put_option_u64(None);
                }
                r::InvalidObserverEpoch::EpochAhead {
                    conversation_id,
                    presented_epoch,
                    current_observer_progress,
                } => {
                    encoder.put_u64(*conversation_id);
                    encoder.put_u64(*presented_epoch);
                    encoder.put_option_u64(Some(*current_observer_progress));
                }
            }
        }
        r::ServerValue::InvalidObserverEpochList(value) => {
            encoder.put_u64(0);
            encoder.put_u16(value.reason().wire_value());
            match value {
                r::InvalidObserverEpochList::TooManyEntries {
                    presented_entries,
                    max_entries,
                } => {
                    encoder.put_u64(*presented_entries);
                    encoder.put_u64(*max_entries);
                }
                r::InvalidObserverEpochList::DuplicateConversation {
                    conversation_id,
                    first_index,
                    duplicate_index,
                } => {
                    encoder.put_u64(*conversation_id);
                    encoder.put_u64(*first_index);
                    encoder.put_u64(*duplicate_index);
                }
            }
        }
    }
    Ok(())
}

fn put_stale_authority(value: &r::StaleAuthority, encoder: &mut Encoder) {
    match value {
        r::StaleAuthority::Live {
            request,
            current_generation,
        } => {
            match request {
                r::CommonStaleAuthorityEnvelope::CredentialAttach(value) => {
                    put_attach(value, encoder);
                }
                r::CommonStaleAuthorityEnvelope::ParticipantAck(value) => {
                    put_participant_ack(value, encoder);
                }
                r::CommonStaleAuthorityEnvelope::MarkerAck(value) => {
                    put_marker_ack(value, encoder);
                }
                r::CommonStaleAuthorityEnvelope::RecordAdmission(value) => {
                    put_record_admission(value, encoder);
                }
            }
            encoder.put_generation(*current_generation);
        }
        r::StaleAuthority::Detach(value) => {
            encoder.put_u16(value.authority_state_tag().wire_value());
            match value {
                r::DetachStaleAuthority::Live {
                    conversation_id,
                    participant_id,
                    capability_generation,
                    detach_attempt_token,
                    current_generation,
                } => {
                    encoder.put_u64(*conversation_id);
                    encoder.put_u64(*participant_id);
                    encoder.put_generation(*capability_generation);
                    encoder.put_fixed(detach_attempt_token.as_bytes());
                    encoder.put_generation(*current_generation);
                }
                r::DetachStaleAuthority::TerminalizedDetachCell(value) => {
                    encoder.put_u64(value.conversation_id());
                    encoder.put_u64(value.participant_id());
                    encoder.put_generation(value.capability_generation());
                    encoder.put_fixed(value.detach_attempt_token().as_bytes());
                    encoder.put_generation(value.current_generation());
                    encoder.put_binding_epoch(value.committed_binding_epoch());
                    encoder.put_u16(value.binding_state().tag().wire_value());
                    if let r::BindingStateView::Bound {
                        current_binding_epoch,
                    } = value.binding_state()
                    {
                        encoder.put_binding_epoch(current_binding_epoch);
                    }
                }
            }
        }
        r::StaleAuthority::Leave(value) => {
            encoder.put_u16(value.authority_state_tag().wire_value());
            match value {
                r::LeaveStaleAuthority::Live {
                    conversation_id,
                    participant_id,
                    presented_generation,
                    leave_attempt_token,
                    current_generation,
                } => {
                    encoder.put_u64(*conversation_id);
                    encoder.put_u64(*participant_id);
                    encoder.put_generation(*presented_generation);
                    encoder.put_fixed(leave_attempt_token.as_bytes());
                    encoder.put_generation(*current_generation);
                }
                r::LeaveStaleAuthority::CommittedLeaveTombstone {
                    conversation_id,
                    participant_id,
                    presented_generation,
                    leave_attempt_token,
                    retired_generation,
                } => {
                    encoder.put_u64(*conversation_id);
                    encoder.put_u64(*participant_id);
                    encoder.put_generation(*presented_generation);
                    encoder.put_fixed(leave_attempt_token.as_bytes());
                    encoder.put_generation(*retired_generation);
                }
            }
        }
    }
}

fn put_receipt_expired(value: &r::ReceiptExpired, encoder: &mut Encoder) {
    match value {
        r::ReceiptExpired::Enrollment {
            conversation_id,
            token,
            participant_id,
            result_generation,
            current_generation,
            reason,
        } => {
            encoder.put_u64(*conversation_id);
            encoder.put_fixed(token.as_bytes());
            encoder.put_u64(*participant_id);
            encoder.put_option_generation(None);
            encoder.put_generation(*result_generation);
            encoder.put_generation(*current_generation);
            encoder.put_u16(reason.wire_value());
        }
        r::ReceiptExpired::CredentialAttach {
            conversation_id,
            token,
            participant_id,
            presented_generation,
            presented_marker_delivery_seq,
            result_generation,
            current_generation,
            reason,
        } => {
            encoder.put_u64(*conversation_id);
            encoder.put_fixed(token.as_bytes());
            encoder.put_u64(*participant_id);
            encoder.put_option_generation(Some(*presented_generation));
            encoder.put_option_u64(*presented_marker_delivery_seq);
            encoder.put_generation(*result_generation);
            encoder.put_generation(*current_generation);
            encoder.put_u16(reason.wire_value());
        }
    }
}

fn put_receipt_capacity(value: &r::ReceiptCapacityExceeded, encoder: &mut Encoder) {
    match value {
        r::ReceiptCapacityExceeded::Enrollment {
            request,
            scope,
            limit,
            occupied,
        } => {
            put_enrollment(request, encoder);
            encoder.put_u16(scope.wire_scope().wire_value());
            encoder.put_u64(*limit);
            encoder.put_u64(*occupied);
            encoder.put_u64(r::ReceiptCapacityExceeded::REQUESTED);
        }
        r::ReceiptCapacityExceeded::CredentialAttach {
            request,
            scope,
            limit,
            occupied,
        } => {
            put_attach(request, encoder);
            encoder.put_u16(scope.wire_value());
            encoder.put_u64(*limit);
            encoder.put_u64(*occupied);
            encoder.put_u64(r::ReceiptCapacityExceeded::REQUESTED);
        }
    }
}

fn put_observer_backpressure(value: &r::ObserverBackpressure, encoder: &mut Encoder) {
    match value {
        r::ObserverBackpressure::Enrollment { request, state } => {
            put_enrollment(request, encoder);
            put_backpressure_state(*state, encoder);
        }
        r::ObserverBackpressure::CredentialAttach { request, state } => {
            put_attach(request, encoder);
            put_backpressure_state(*state, encoder);
        }
        r::ObserverBackpressure::Detach {
            request,
            committed_binding_epoch,
            state,
        } => {
            put_detach(request, encoder);
            encoder.put_binding_epoch(*committed_binding_epoch);
            put_backpressure_state(*state, encoder);
        }
        r::ObserverBackpressure::Leave {
            request,
            state,
            prior_terminal_cell_exists,
        } => {
            put_leave(request, encoder);
            put_backpressure_state(*state, encoder);
            encoder.put_bool(*prior_terminal_cell_exists);
        }
        r::ObserverBackpressure::RecordAdmission { request, state } => {
            put_record_admission(request, encoder);
            put_backpressure_state(*state, encoder);
        }
    }
}

fn put_backpressure_state(value: r::ObserverBackpressureState, encoder: &mut Encoder) {
    encoder.put_u64(value.backpressure_epoch());
    encoder.put_u64(value.observer_progress());
}

fn put_receipt_replay(value: &r::ReceiptReplay, encoder: &mut Encoder) {
    match value {
        r::ReceiptReplay::Enrollment(value) => {
            encoder.put_u64(value.conversation_id());
            encoder.put_fixed(value.token().as_bytes());
            encoder.put_u64(value.participant_id());
            encoder.put_option_generation(None);
            encoder.put_generation(value.capability_generation());
            encoder.put_fixed(value.attach_secret().as_bytes());
            encoder.put_binding_epoch(value.origin_binding_epoch());
            encoder.put_u64(value.persisted_cursor());
            encoder.put_option_u64(None);
            encoder.put_u128(value.receipt_expires_at());
            encoder.put_u128(value.provenance_expires_at());
        }
        r::ReceiptReplay::CredentialAttach(value) => {
            encoder.put_u64(value.conversation_id());
            encoder.put_fixed(value.token().as_bytes());
            encoder.put_u64(value.participant_id());
            encoder.put_option_generation(Some(value.request_generation()));
            encoder.put_generation(value.capability_generation());
            encoder.put_fixed(value.attach_secret().as_bytes());
            encoder.put_binding_epoch(value.origin_binding_epoch());
            encoder.put_u64(value.persisted_cursor());
            encoder.put_option_u64(value.accepted_marker_delivery_seq());
            encoder.put_u128(value.receipt_expires_at());
            encoder.put_u128(value.provenance_expires_at());
        }
    }
}

const fn origin_is_valid(
    discriminant: t::ServerDiscriminant,
    origin: t::ClientDiscriminant,
) -> bool {
    use t::ClientDiscriminant as O;
    use t::ServerDiscriminant as D;

    match discriminant {
        D::AttemptTokenBodyConflict => {
            matches!(origin, O::CredentialAttachRequest | O::LeaveRequest)
        }
        D::ConnectionConversationCapacityExceeded => {
            !matches!(origin, O::ObserverRecoveryHandshake)
        }
        D::ConnectionConversationBindingOccupied => {
            matches!(origin, O::EnrollmentRequest | O::CredentialAttachRequest)
        }
        D::ConversationOrderExhausted => matches!(
            origin,
            O::EnrollmentRequest | O::CredentialAttachRequest | O::RecordAdmission
        ),
        D::ParticipantUnknown => {
            !matches!(origin, O::EnrollmentRequest | O::ObserverRecoveryHandshake)
        }
        D::NoBinding => !matches!(
            origin,
            O::EnrollmentRequest | O::CredentialAttachRequest | O::ObserverRecoveryHandshake
        ),
        D::StaleAuthority => !matches!(origin, O::EnrollmentRequest | O::ObserverRecoveryHandshake),
        D::Retired => !matches!(origin, O::ObserverRecoveryHandshake),
        D::MarkerClosureCapacityExceeded => matches!(
            origin,
            O::EnrollmentRequest
                | O::CredentialAttachRequest
                | O::LeaveRequest
                | O::RecordAdmission
        ),
        D::EnrollBound | D::EnrollmentKnown | D::IdentityCapacityExceeded => {
            matches!(origin, O::EnrollmentRequest)
        }
        D::ReceiptExpired | D::ReceiptCapacityExceeded | D::Bound | D::UnboundReceipt => {
            matches!(origin, O::EnrollmentRequest | O::CredentialAttachRequest)
        }
        D::ObserverBackpressure => matches!(
            origin,
            O::EnrollmentRequest
                | O::CredentialAttachRequest
                | O::DetachRequest
                | O::LeaveRequest
                | O::RecordAdmission
        ),
        D::ConversationSequenceExhausted => matches!(
            origin,
            O::EnrollmentRequest | O::CredentialAttachRequest | O::RecordAdmission
        ),
        D::AttachBound | D::StaleOrUnknownReceipt => {
            matches!(origin, O::CredentialAttachRequest)
        }
        D::MarkerNotDelivered | D::MarkerMismatch => {
            matches!(origin, O::CredentialAttachRequest | O::MarkerAck)
        }
        D::DetachCommitted | D::DetachInProgress => matches!(origin, O::DetachRequest),
        D::AckCommitted | D::AckGap | D::AckRegression => matches!(origin, O::ParticipantAck),
        D::AckNoOp => matches!(origin, O::ParticipantAck | O::MarkerAck),
        D::LeaveCommitted => matches!(origin, O::LeaveRequest),
        D::MarkerAckCommitted => matches!(origin, O::MarkerAck),
        D::RecordCommitted | D::RecordTooLarge => matches!(origin, O::RecordAdmission),
        D::ParticipantTransportRejected
        | D::ObserverRecoveryAccepted
        | D::InvalidObserverEpoch
        | D::InvalidObserverEpochList
        | D::ObserverRecoveryConnectionCapacityExceeded => false,
    }
}

// Decoding is kept below encoding so every selected schema can be compared
// field-for-field in one source file.

fn take_enrollment(decoder: &mut Decoder<'_>) -> Result<e::EnrollmentEnvelope, CodecError> {
    Ok(e::EnrollmentEnvelope {
        conversation_id: decoder.take_u64()?,
        enrollment_token: EnrollmentToken::new(decoder.take_fixed::<TOKEN_LEN>()?),
    })
}

fn take_attach(decoder: &mut Decoder<'_>) -> Result<e::AttachEnvelope, CodecError> {
    Ok(e::AttachEnvelope {
        conversation_id: decoder.take_u64()?,
        participant_id: decoder.take_u64()?,
        capability_generation: decoder.take_generation()?,
        attach_attempt_token: AttachAttemptToken::new(decoder.take_fixed::<TOKEN_LEN>()?),
        accept_marker_delivery_seq: decoder.take_option_u64()?,
    })
}

fn take_detach(decoder: &mut Decoder<'_>) -> Result<e::DetachEnvelope, CodecError> {
    Ok(e::DetachEnvelope {
        conversation_id: decoder.take_u64()?,
        participant_id: decoder.take_u64()?,
        capability_generation: decoder.take_generation()?,
        detach_attempt_token: DetachAttemptToken::new(decoder.take_fixed::<TOKEN_LEN>()?),
    })
}

fn take_participant_ack(
    decoder: &mut Decoder<'_>,
) -> Result<e::ParticipantAckEnvelope, CodecError> {
    Ok(e::ParticipantAckEnvelope {
        conversation_id: decoder.take_u64()?,
        participant_id: decoder.take_u64()?,
        capability_generation: decoder.take_generation()?,
        through_seq: decoder.take_u64()?,
    })
}

fn take_leave(decoder: &mut Decoder<'_>) -> Result<e::LeaveEnvelope, CodecError> {
    Ok(e::LeaveEnvelope {
        conversation_id: decoder.take_u64()?,
        participant_id: decoder.take_u64()?,
        capability_generation: decoder.take_generation()?,
        leave_attempt_token: LeaveAttemptToken::new(decoder.take_fixed::<TOKEN_LEN>()?),
    })
}

fn take_marker_ack(decoder: &mut Decoder<'_>) -> Result<e::MarkerAckEnvelope, CodecError> {
    Ok(e::MarkerAckEnvelope {
        conversation_id: decoder.take_u64()?,
        participant_id: decoder.take_u64()?,
        capability_generation: decoder.take_generation()?,
        marker_delivery_seq: decoder.take_u64()?,
    })
}

fn take_record_admission(
    decoder: &mut Decoder<'_>,
) -> Result<e::RecordAdmissionEnvelope, CodecError> {
    Ok(e::RecordAdmissionEnvelope {
        conversation_id: decoder.take_u64()?,
        participant_id: decoder.take_u64()?,
        capability_generation: decoder.take_generation()?,
        record_admission_attempt_token: RecordAdmissionAttemptToken::new(
            decoder.take_fixed::<TOKEN_LEN>()?,
        ),
    })
}

fn take_response_envelope(
    origin: t::ClientDiscriminant,
    decoder: &mut Decoder<'_>,
) -> Result<e::ResponseEnvelope, CodecError> {
    match origin {
        t::ClientDiscriminant::EnrollmentRequest => {
            take_enrollment(decoder).map(e::ResponseEnvelope::Enrollment)
        }
        t::ClientDiscriminant::CredentialAttachRequest => {
            take_attach(decoder).map(e::ResponseEnvelope::CredentialAttach)
        }
        t::ClientDiscriminant::DetachRequest => {
            take_detach(decoder).map(e::ResponseEnvelope::Detach)
        }
        t::ClientDiscriminant::ParticipantAck => {
            take_participant_ack(decoder).map(e::ResponseEnvelope::ParticipantAck)
        }
        t::ClientDiscriminant::LeaveRequest => take_leave(decoder).map(e::ResponseEnvelope::Leave),
        t::ClientDiscriminant::MarkerAck => {
            take_marker_ack(decoder).map(e::ResponseEnvelope::MarkerAck)
        }
        t::ClientDiscriminant::RecordAdmission => {
            take_record_admission(decoder).map(e::ResponseEnvelope::RecordAdmission)
        }
        t::ClientDiscriminant::ObserverRecoveryHandshake => {
            Err(decode_error(t::DecodeClass::InvalidField))
        }
    }
}

fn take_participant_reference(
    origin: t::ClientDiscriminant,
    decoder: &mut Decoder<'_>,
) -> Result<r::ParticipantReferenceEnvelope, CodecError> {
    match origin {
        t::ClientDiscriminant::CredentialAttachRequest => {
            take_attach(decoder).map(r::ParticipantReferenceEnvelope::CredentialAttach)
        }
        t::ClientDiscriminant::DetachRequest => {
            take_detach(decoder).map(r::ParticipantReferenceEnvelope::Detach)
        }
        t::ClientDiscriminant::ParticipantAck => {
            take_participant_ack(decoder).map(r::ParticipantReferenceEnvelope::ParticipantAck)
        }
        t::ClientDiscriminant::LeaveRequest => {
            take_leave(decoder).map(r::ParticipantReferenceEnvelope::Leave)
        }
        t::ClientDiscriminant::MarkerAck => {
            take_marker_ack(decoder).map(r::ParticipantReferenceEnvelope::MarkerAck)
        }
        t::ClientDiscriminant::RecordAdmission => {
            take_record_admission(decoder).map(r::ParticipantReferenceEnvelope::RecordAdmission)
        }
        t::ClientDiscriminant::EnrollmentRequest
        | t::ClientDiscriminant::ObserverRecoveryHandshake => {
            Err(decode_error(t::DecodeClass::InvalidField))
        }
    }
}

fn take_binding_required(
    origin: t::ClientDiscriminant,
    decoder: &mut Decoder<'_>,
) -> Result<r::BindingRequiredEnvelope, CodecError> {
    match origin {
        t::ClientDiscriminant::DetachRequest => {
            take_detach(decoder).map(r::BindingRequiredEnvelope::Detach)
        }
        t::ClientDiscriminant::ParticipantAck => {
            take_participant_ack(decoder).map(r::BindingRequiredEnvelope::ParticipantAck)
        }
        t::ClientDiscriminant::LeaveRequest => {
            take_leave(decoder).map(r::BindingRequiredEnvelope::Leave)
        }
        t::ClientDiscriminant::MarkerAck => {
            take_marker_ack(decoder).map(r::BindingRequiredEnvelope::MarkerAck)
        }
        t::ClientDiscriminant::RecordAdmission => {
            take_record_admission(decoder).map(r::BindingRequiredEnvelope::RecordAdmission)
        }
        t::ClientDiscriminant::EnrollmentRequest
        | t::ClientDiscriminant::CredentialAttachRequest
        | t::ClientDiscriminant::ObserverRecoveryHandshake => {
            Err(decode_error(t::DecodeClass::InvalidField))
        }
    }
}

fn take_order_allocating(
    origin: t::ClientDiscriminant,
    decoder: &mut Decoder<'_>,
) -> Result<r::OrderAllocatingEnvelope, CodecError> {
    match origin {
        t::ClientDiscriminant::EnrollmentRequest => {
            take_enrollment(decoder).map(r::OrderAllocatingEnvelope::Enrollment)
        }
        t::ClientDiscriminant::CredentialAttachRequest => {
            take_attach(decoder).map(r::OrderAllocatingEnvelope::CredentialAttach)
        }
        t::ClientDiscriminant::RecordAdmission => {
            take_record_admission(decoder).map(r::OrderAllocatingEnvelope::RecordAdmission)
        }
        _ => Err(decode_error(t::DecodeClass::InvalidField)),
    }
}

fn take_closure_checked(
    origin: t::ClientDiscriminant,
    decoder: &mut Decoder<'_>,
) -> Result<c::ClosureCheckedEnvelope, CodecError> {
    match origin {
        t::ClientDiscriminant::EnrollmentRequest => {
            take_enrollment(decoder).map(c::ClosureCheckedEnvelope::Enrollment)
        }
        t::ClientDiscriminant::CredentialAttachRequest => {
            take_attach(decoder).map(c::ClosureCheckedEnvelope::CredentialAttach)
        }
        t::ClientDiscriminant::LeaveRequest => {
            take_leave(decoder).map(c::ClosureCheckedEnvelope::Leave)
        }
        t::ClientDiscriminant::RecordAdmission => {
            take_record_admission(decoder).map(c::ClosureCheckedEnvelope::RecordAdmission)
        }
        _ => Err(decode_error(t::DecodeClass::InvalidField)),
    }
}

fn take_sequence_allocating(
    origin: t::ClientDiscriminant,
    decoder: &mut Decoder<'_>,
) -> Result<r::SequenceAllocatingEnvelope, CodecError> {
    match origin {
        t::ClientDiscriminant::EnrollmentRequest => {
            take_enrollment(decoder).map(r::SequenceAllocatingEnvelope::Enrollment)
        }
        t::ClientDiscriminant::CredentialAttachRequest => {
            take_attach(decoder).map(r::SequenceAllocatingEnvelope::CredentialAttach)
        }
        t::ClientDiscriminant::RecordAdmission => {
            take_record_admission(decoder).map(r::SequenceAllocatingEnvelope::RecordAdmission)
        }
        _ => Err(decode_error(t::DecodeClass::InvalidField)),
    }
}

fn take_marker_proof(
    origin: t::ClientDiscriminant,
    decoder: &mut Decoder<'_>,
) -> Result<r::MarkerProofRequest, CodecError> {
    match origin {
        t::ClientDiscriminant::CredentialAttachRequest => Ok(
            r::MarkerProofRequest::CredentialAttach(r::AttachMarkerProof {
                conversation_id: decoder.take_u64()?,
                token: AttachAttemptToken::new(decoder.take_fixed::<TOKEN_LEN>()?),
                participant_id: decoder.take_u64()?,
                capability_generation: decoder.take_generation()?,
                requested_marker_delivery_seq: decoder.take_u64()?,
            }),
        ),
        t::ClientDiscriminant::MarkerAck => {
            Ok(r::MarkerProofRequest::MarkerAck(r::MarkerAckProof {
                conversation_id: decoder.take_u64()?,
                participant_id: decoder.take_u64()?,
                capability_generation: decoder.take_generation()?,
                requested_marker_delivery_seq: decoder.take_u64()?,
            }))
        }
        _ => Err(decode_error(t::DecodeClass::InvalidField)),
    }
}

fn take_repayment_edge(decoder: &mut Decoder<'_>) -> Result<c::RepaymentEdge, CodecError> {
    let tag = t::RepaymentEdgeTag::try_from(decoder.take_u16()?)
        .map_err(|_| decode_error(t::DecodeClass::InvalidField))?;
    match tag {
        t::RepaymentEdgeTag::None => Ok(c::RepaymentEdge::None),
        t::RepaymentEdgeTag::ObserverProjection => Ok(c::RepaymentEdge::ObserverProjection {
            through_seq: decoder.take_u64()?,
        }),
        t::RepaymentEdgeTag::PhysicalCompaction => Ok(c::RepaymentEdge::PhysicalCompaction {
            from_floor: decoder.take_u64()?,
            through_seq: decoder.take_u64()?,
        }),
        t::RepaymentEdgeTag::MarkerDelivery => Ok(c::RepaymentEdge::MarkerDelivery {
            participant_id: decoder.take_u64()?,
            binding_epoch: decoder.take_binding_epoch()?,
            marker_delivery_seq: decoder.take_u64()?,
        }),
        t::RepaymentEdgeTag::ParticipantCursorProgress => Ok(
            c::RepaymentEdge::ParticipantCursorProgress(c::ParticipantCursorProgressEdge {
                participant_id: decoder.take_u64()?,
                binding_epoch: decoder.take_binding_epoch()?,
                through_seq: decoder.take_u64()?,
                marker_delivery_seq: decoder.take_option_u64()?,
            }),
        ),
        t::RepaymentEdgeTag::DetachedCredentialRecovery => {
            Ok(c::RepaymentEdge::DetachedCredentialRecovery {
                participant_id: decoder.take_u64()?,
                marker_delivery_seq: decoder.take_u64()?,
                prior_binding_epoch: decoder.take_binding_epoch()?,
            })
        }
        t::RepaymentEdgeTag::DetachedMarkerRelease => Ok(c::RepaymentEdge::DetachedMarkerRelease {
            participant_id: decoder.take_u64()?,
            marker_delivery_seq: decoder.take_u64()?,
            last_dead_binding_epoch: decoder.take_binding_epoch()?,
        }),
        t::RepaymentEdgeTag::DetachedCursorRelease => Ok(c::RepaymentEdge::DetachedCursorRelease {
            participant_id: decoder.take_u64()?,
            last_dead_binding_epoch: decoder.take_binding_epoch()?,
        }),
    }
}

fn take_closure_snapshot(decoder: &mut Decoder<'_>) -> Result<c::ClosureSnapshot, CodecError> {
    Ok(c::ClosureSnapshot {
        marker_capacity_credits: decoder.take_u64()?,
        marker_anchors: decoder.take_u64()?,
        entry_debt: decoder.take_u64()?,
        byte_debt: decoder.take_u64()?,
        repayment_edge: take_repayment_edge(decoder)?,
        edge_sequence_claims: decoder.take_u64()?,
        edge_order_position_claims: decoder.take_u64()?,
        edge_k_remaining: decoder.take_resource_vector()?,
        k_headroom: decoder.take_wide_resource_vector()?,
        episode_churn_used: decoder.take_u64()?,
        delta_cycles: decoder.take_u64()?,
        episode_churn_limit: decoder.take_u64()?,
    })
}

fn take_sequence_budget(decoder: &mut Decoder<'_>) -> Result<super::SequenceBudget, CodecError> {
    Ok(super::SequenceBudget {
        high_watermark: decoder.take_u64()?,
        remaining: decoder.take_u64()?,
        e: decoder.take_u64()?,
        t: decoder.take_u64()?,
        m: decoder.take_u64()?,
        rs: decoder.take_u64()?,
        rt: decoder.take_u64()?,
        l_times_t: decoder.take_u128()?,
        l_times_rt: decoder.take_u128()?,
        l_other_times_e: decoder.take_u128()?,
    })
}

fn required_origin(
    origin: Option<t::ClientDiscriminant>,
) -> Result<t::ClientDiscriminant, CodecError> {
    origin.ok_or_else(|| decode_error(t::DecodeClass::InvalidField))
}

#[allow(clippy::too_many_lines)]
fn decode_server_suffix(
    discriminant: t::ServerDiscriminant,
    origin: Option<t::ClientDiscriminant>,
    decoder: &mut Decoder<'_>,
) -> Result<r::ServerValue, CodecError> {
    use t::ServerDiscriminant as D;

    match discriminant {
        D::ParticipantTransportRejected => {
            let reason = match take_tag::<t::TransportReasonTag>(decoder)? {
                t::TransportReasonTag::FrameTooLarge => {
                    r::TransportRejectionReason::FrameTooLarge {
                        complete_frame_bytes: decoder.take_u64()?,
                        max_frame_bytes: decoder.take_u64()?,
                    }
                }
                t::TransportReasonTag::DecodeFailed => {
                    let decode_class = take_tag::<t::DecodeClass>(decoder)?;
                    r::TransportRejectionReason::DecodeFailed { decode_class }
                }
                t::TransportReasonTag::UnsupportedVersion => {
                    r::TransportRejectionReason::UnsupportedVersion {
                        presented_version: decoder.take_protocol_version()?,
                        supported_version: decoder.take_protocol_version()?,
                    }
                }
                t::TransportReasonTag::AuthenticationFailed => {
                    r::TransportRejectionReason::AuthenticationFailed
                }
                t::TransportReasonTag::ParticipantCapabilityRequired => {
                    let capability = decoder.take_string()?;
                    if capability != r::PARTICIPANT_CAPABILITY {
                        decoder.invalidate();
                    }
                    r::TransportRejectionReason::ParticipantCapabilityRequired
                }
            };
            Ok(r::ServerValue::ParticipantTransportRejected(
                r::ParticipantTransportRejected { reason },
            ))
        }
        D::AttemptTokenBodyConflict => {
            let origin = required_origin(origin)?;
            let token = decoder.take_fixed::<TOKEN_LEN>()?;
            let operation = take_tag::<t::AttemptOperation>(decoder)?;
            match origin {
                t::ClientDiscriminant::CredentialAttachRequest => {
                    if operation != t::AttemptOperation::CredentialAttachRequest {
                        return Err(decode_error(t::DecodeClass::InvalidField));
                    }
                    Ok(r::ServerValue::AttemptTokenBodyConflict(
                        r::AttemptTokenBodyConflict::CredentialAttach {
                            token: AttachAttemptToken::new(token),
                            conversation_id: decoder.take_u64()?,
                            presented_participant_id: decoder.take_u64()?,
                            presented_generation: decoder.take_generation()?,
                            presented_marker_delivery_seq: decoder.take_option_u64()?,
                            conflict: take_tag::<t::AttemptConflict>(decoder)?,
                        },
                    ))
                }
                t::ClientDiscriminant::LeaveRequest => {
                    if operation != t::AttemptOperation::LeaveRequest {
                        return Err(decode_error(t::DecodeClass::InvalidField));
                    }
                    let value = r::AttemptTokenBodyConflict::Leave {
                        token: LeaveAttemptToken::new(token),
                        conversation_id: decoder.take_u64()?,
                        presented_participant_id: decoder.take_u64()?,
                        presented_generation: decoder.take_generation()?,
                    };
                    if take_tag::<t::AttemptConflict>(decoder)? != t::AttemptConflict::Generation {
                        decoder.invalidate();
                    }
                    Ok(r::ServerValue::AttemptTokenBodyConflict(value))
                }
                _ => Err(decode_error(t::DecodeClass::InvalidField)),
            }
        }
        D::ConnectionConversationCapacityExceeded => {
            let origin = required_origin(origin)?;
            Ok(r::ServerValue::ConnectionConversationCapacityExceeded(
                r::ConnectionConversationCapacityExceeded::SemanticRequest {
                    request: take_response_envelope(origin, decoder)?,
                    limit: decoder.take_u64()?,
                },
            ))
        }
        D::ConnectionConversationBindingOccupied => {
            let origin = required_origin(origin)?;
            let value = match origin {
                t::ClientDiscriminant::EnrollmentRequest => {
                    let conversation_id = decoder.take_u64()?;
                    let enrollment_token = EnrollmentToken::new(decoder.take_fixed::<TOKEN_LEN>()?);
                    if decoder.take_option_u64()?.is_some() {
                        decoder.invalidate();
                    }
                    r::ConnectionConversationBindingOccupied::Enrollment {
                        conversation_id,
                        enrollment_token,
                    }
                }
                t::ClientDiscriminant::CredentialAttachRequest => {
                    let conversation_id = decoder.take_u64()?;
                    let participant_id = decoder.take_u64()?;
                    let capability_generation = decoder.take_generation()?;
                    let attach_attempt_token =
                        AttachAttemptToken::new(decoder.take_fixed::<TOKEN_LEN>()?);
                    let accept_marker_delivery_seq = decoder.take_option_u64()?;
                    if decoder.take_option_u64()? != Some(participant_id) {
                        decoder.invalidate();
                    }
                    r::ConnectionConversationBindingOccupied::CredentialAttach {
                        conversation_id,
                        participant_id,
                        capability_generation,
                        attach_attempt_token,
                        accept_marker_delivery_seq,
                    }
                }
                _ => return Err(decode_error(t::DecodeClass::InvalidField)),
            };
            Ok(r::ServerValue::ConnectionConversationBindingOccupied(value))
        }
        D::ConversationOrderExhausted => {
            let origin = required_origin(origin)?;
            let request = take_order_allocating(origin, decoder)?;
            let _counter = take_tag::<t::Counter>(decoder)?;
            let high = decoder.take_u64()?;
            let next_value = decoder.take_option_u64()?;
            let order_remaining = decoder.take_u128()?;
            let reserved_claims = decoder.take_u128()?;
            if decoder.take_u64()? != r::ConversationOrderExhausted::REQUIRED_MAJORS {
                decoder.invalidate();
            }
            let resulting_order_remaining = decoder.take_u128()?;
            let resulting_reserved_claims = decoder.take_u128()?;
            let value = r::ConversationOrderExhausted::new(
                request,
                high,
                order_remaining,
                reserved_claims,
                resulting_order_remaining,
                resulting_reserved_claims,
            );
            if next_value != value.next_value() {
                decoder.invalidate();
            }
            Ok(r::ServerValue::ConversationOrderExhausted(
                alloc::boxed::Box::new(value),
            ))
        }
        D::ParticipantUnknown => {
            let origin = required_origin(origin)?;
            Ok(r::ServerValue::ParticipantUnknown(r::ParticipantUnknown {
                request: take_participant_reference(origin, decoder)?,
            }))
        }
        D::NoBinding => {
            let origin = required_origin(origin)?;
            Ok(r::ServerValue::NoBinding(r::NoBinding {
                request: take_binding_required(origin, decoder)?,
            }))
        }
        D::StaleAuthority => take_stale_authority(required_origin(origin)?, decoder)
            .map(r::ServerValue::StaleAuthority),
        D::Retired => {
            let origin = required_origin(origin)?;
            let value = if origin == t::ClientDiscriminant::EnrollmentRequest {
                r::Retired::Enrollment {
                    request: take_enrollment(decoder)?,
                    participant_id: decoder.take_u64()?,
                    retired_generation: decoder.take_generation()?,
                }
            } else {
                r::Retired::Participant {
                    request: take_participant_reference(origin, decoder)?,
                    retired_generation: decoder.take_generation()?,
                }
            };
            Ok(r::ServerValue::Retired(value))
        }
        D::MarkerClosureCapacityExceeded => {
            let origin = required_origin(origin)?;
            let request = take_closure_checked(origin, decoder)?;
            let scope = take_tag::<t::ClosureScope>(decoder)?;
            let snapshot = take_closure_snapshot(decoder)?;
            let reason = match scope {
                t::ClosureScope::Capacity => {
                    let dimension =
                        ResourceDimension::from(take_tag::<t::ResourceDimensionTag>(decoder)?);
                    c::ClosureRefusalReason::Capacity(c::ClosureCapacityReason {
                        dimension,
                        required: decoder.take_u128()?,
                        limit: decoder.take_u128()?,
                    })
                }
                t::ClosureScope::RecoveryFence => c::ClosureRefusalReason::RecoveryFence,
                t::ClosureScope::DeliveredMarkerAwaitingAck => {
                    c::ClosureRefusalReason::DeliveredMarkerAwaitingAck
                }
                t::ClosureScope::EpisodeChurnLimit => c::ClosureRefusalReason::EpisodeChurnLimit,
            };
            Ok(r::ServerValue::MarkerClosureCapacityExceeded(
                alloc::boxed::Box::new(c::MarkerClosureCapacityExceeded {
                    request,
                    snapshot,
                    reason,
                }),
            ))
        }
        D::EnrollBound => take_enroll_bound(decoder).map(r::ServerValue::EnrollBound),
        D::EnrollmentKnown => Ok(r::ServerValue::EnrollmentKnown(r::EnrollmentKnown {
            conversation_id: decoder.take_u64()?,
            token: EnrollmentToken::new(decoder.take_fixed::<TOKEN_LEN>()?),
            participant_id: decoder.take_u64()?,
            current_generation: decoder.take_generation()?,
        })),
        D::ReceiptExpired => take_receipt_expired(required_origin(origin)?, decoder)
            .map(r::ServerValue::ReceiptExpired),
        D::ReceiptCapacityExceeded => take_receipt_capacity(required_origin(origin)?, decoder)
            .map(r::ServerValue::ReceiptCapacityExceeded),
        D::IdentityCapacityExceeded => {
            let request = take_enrollment(decoder)?;
            let scope = take_tag::<t::IdentityCapacityScope>(decoder)?;
            let limit = decoder.take_u64()?;
            let occupied = decoder.take_u64()?;
            if decoder.take_u64()? != r::IdentityCapacityExceeded::REQUESTED {
                decoder.invalidate();
            }
            Ok(r::ServerValue::IdentityCapacityExceeded(
                r::IdentityCapacityExceeded {
                    request,
                    scope,
                    limit,
                    occupied,
                },
            ))
        }
        D::ObserverBackpressure => take_observer_backpressure(required_origin(origin)?, decoder)
            .map(r::ServerValue::ObserverBackpressure),
        D::ConversationSequenceExhausted => {
            let request = take_sequence_allocating(required_origin(origin)?, decoder)?;
            Ok(r::ServerValue::ConversationSequenceExhausted(
                alloc::boxed::Box::new(r::ConversationSequenceExhausted {
                    request,
                    sequence_budget: take_sequence_budget(decoder)?,
                }),
            ))
        }
        D::AttachBound => take_attach_bound(decoder).map(r::ServerValue::AttachBound),
        D::StaleOrUnknownReceipt => Ok(r::ServerValue::StaleOrUnknownReceipt(
            r::StaleOrUnknownReceipt {
                conversation_id: decoder.take_u64()?,
                token: AttachAttemptToken::new(decoder.take_fixed::<TOKEN_LEN>()?),
                participant_id: decoder.take_u64()?,
                presented_generation: decoder.take_generation()?,
                presented_marker_delivery_seq: decoder.take_option_u64()?,
                current_generation: decoder.take_generation()?,
            },
        )),
        D::MarkerNotDelivered => {
            let request = take_marker_proof(required_origin(origin)?, decoder)?;
            let reason = take_tag::<t::MarkerNotDeliveredReason>(decoder)?;
            Ok(r::ServerValue::MarkerNotDelivered(r::MarkerNotDelivered {
                request,
                reason,
                expected_marker_delivery_seq: decoder.take_u64()?,
            }))
        }
        D::MarkerMismatch => {
            let request = take_marker_proof(required_origin(origin)?, decoder)?;
            let reason = take_tag::<t::MarkerMismatchReason>(decoder)?;
            let mismatch = match reason {
                t::MarkerMismatchReason::BelowCursor => r::MarkerMismatchBody::BelowCursor {
                    current_cursor: decoder.take_u64()?,
                },
                t::MarkerMismatchReason::NoMarkerExpected => {
                    r::MarkerMismatchBody::NoMarkerExpected
                }
                t::MarkerMismatchReason::ExpectedDifferentMarker => {
                    r::MarkerMismatchBody::ExpectedDifferentMarker {
                        expected_marker_delivery_seq: decoder.take_u64()?,
                    }
                }
            };
            Ok(r::ServerValue::MarkerMismatch(r::MarkerMismatch {
                request,
                mismatch,
            }))
        }
        D::Bound => {
            take_receipt_replay(required_origin(origin)?, decoder).map(r::ServerValue::Bound)
        }
        D::UnboundReceipt => take_receipt_replay(required_origin(origin)?, decoder)
            .map(r::ServerValue::UnboundReceipt),
        D::DetachCommitted => {
            let conversation_id = decoder.take_u64()?;
            let participant_id = decoder.take_u64()?;
            let capability_generation = decoder.take_generation()?;
            let detach_attempt_token = DetachAttemptToken::new(decoder.take_fixed::<TOKEN_LEN>()?);
            let committed_binding_epoch = decoder.take_binding_epoch()?;
            let detached_delivery_seq = decoder.take_u64()?;
            if capability_generation != committed_binding_epoch.capability_generation {
                decoder.invalidate();
            }
            Ok(r::ServerValue::DetachCommitted(r::DetachCommitted::new(
                conversation_id,
                participant_id,
                detach_attempt_token,
                committed_binding_epoch,
                detached_delivery_seq,
            )))
        }
        D::DetachInProgress => Ok(r::ServerValue::DetachInProgress(r::DetachInProgress {
            conversation_id: decoder.take_u64()?,
            participant_id: decoder.take_u64()?,
            presented_token: DetachAttemptToken::new(decoder.take_fixed::<TOKEN_LEN>()?),
            presented_generation: decoder.take_generation()?,
            committed_binding_epoch: decoder.take_binding_epoch()?,
        })),
        D::AckCommitted => {
            let request = take_participant_ack(decoder)?;
            if decoder.take_u64()? != request.through_seq {
                decoder.invalidate();
            }
            Ok(r::ServerValue::AckCommitted(r::AckCommitted::new(request)))
        }
        D::AckNoOp => {
            take_ack_no_op(required_origin(origin)?, decoder).map(r::ServerValue::AckNoOp)
        }
        D::AckGap => {
            let request = take_participant_ack(decoder)?;
            let current_cursor = decoder.take_u64()?;
            let _reason = take_tag::<t::AckGapReason>(decoder)?;
            r::AckGap::new(request, current_cursor)
                .map(r::ServerValue::AckGap)
                .ok_or_else(|| decode_error(t::DecodeClass::InvalidField))
        }
        D::AckRegression => {
            let request = take_participant_ack(decoder)?;
            let current_cursor = decoder.take_u64()?;
            let _reason = take_tag::<t::AckRegressionReason>(decoder)?;
            r::AckRegression::new(request, current_cursor)
                .map(r::ServerValue::AckRegression)
                .ok_or_else(|| decode_error(t::DecodeClass::InvalidField))
        }
        D::LeaveCommitted => {
            let conversation_id = decoder.take_u64()?;
            let leave_attempt_token = LeaveAttemptToken::new(decoder.take_fixed::<TOKEN_LEN>()?);
            let participant_id = decoder.take_u64()?;
            let presented_generation = decoder.take_generation()?;
            let retired_generation = decoder.take_generation()?;
            let ended_binding_epoch = decoder.take_option_binding_epoch()?;
            let prior_terminal_delivery_seq = decoder.take_option_u64()?;
            let left_delivery_seq = decoder.take_u64()?;
            if presented_generation != retired_generation {
                return Err(decode_error(t::DecodeClass::InvalidField));
            }
            r::LeaveCommitted::new(
                conversation_id,
                leave_attempt_token,
                participant_id,
                retired_generation,
                ended_binding_epoch,
                prior_terminal_delivery_seq,
                left_delivery_seq,
            )
            .map(r::ServerValue::LeaveCommitted)
            .ok_or_else(|| decode_error(t::DecodeClass::InvalidField))
        }
        D::MarkerAckCommitted => {
            let request = take_marker_ack(decoder)?;
            if decoder.take_u64()? != request.marker_delivery_seq {
                decoder.invalidate();
            }
            Ok(r::ServerValue::MarkerAckCommitted(
                r::MarkerAckCommitted::new(request),
            ))
        }
        D::RecordCommitted => {
            let request = take_record_admission(decoder)?;
            if decoder.take_u64()? != request.participant_id {
                decoder.invalidate();
            }
            let delivery_seq = decoder.take_u64()?;
            Ok(r::ServerValue::RecordCommitted(r::RecordCommitted::new(
                request,
                delivery_seq,
            )))
        }
        D::RecordTooLarge => Ok(r::ServerValue::RecordTooLarge(r::RecordTooLarge {
            request: take_record_admission(decoder)?,
            dimension: ResourceDimension::from(take_tag::<t::ResourceDimensionTag>(decoder)?),
            encoded_record_charge: decoder.take_resource_vector()?,
            max_ordinary_record_charge: decoder.take_resource_vector()?,
        })),
        D::ObserverRecoveryAccepted => {
            take_recovery_accepted(decoder).map(r::ServerValue::ObserverRecoveryAccepted)
        }
        D::InvalidObserverEpoch => {
            require_zero_recovery_count(decoder)?;
            take_invalid_observer_epoch(decoder).map(r::ServerValue::InvalidObserverEpoch)
        }
        D::InvalidObserverEpochList => {
            require_zero_recovery_count(decoder)?;
            take_invalid_observer_epoch_list(decoder).map(r::ServerValue::InvalidObserverEpochList)
        }
        D::ObserverRecoveryConnectionCapacityExceeded => {
            require_zero_recovery_count(decoder)?;
            Ok(r::ServerValue::ConnectionConversationCapacityExceeded(
                r::ConnectionConversationCapacityExceeded::ObserverRecovery {
                    conversation_id: decoder.take_u64()?,
                    limit: decoder.take_u64()?,
                },
            ))
        }
    }
}

trait WireTag: TryFrom<u16> {}

impl WireTag for t::TransportReasonTag {}
impl WireTag for t::DecodeClass {}
impl WireTag for t::AttemptOperation {}
impl WireTag for t::AttemptConflict {}
impl WireTag for t::Counter {}
impl WireTag for t::ClosureScope {}
impl WireTag for t::ResourceDimensionTag {}
impl WireTag for t::IdentityCapacityScope {}
impl WireTag for t::MarkerNotDeliveredReason {}
impl WireTag for t::MarkerMismatchReason {}
impl WireTag for t::AckGapReason {}
impl WireTag for t::AckRegressionReason {}
impl WireTag for t::InvalidObserverEpochReason {}
impl WireTag for t::InvalidObserverEpochListReason {}

fn take_tag<T>(decoder: &mut Decoder<'_>) -> Result<T, CodecError>
where
    T: WireTag,
{
    T::try_from(decoder.take_u16()?).map_err(|_| decode_error(t::DecodeClass::InvalidField))
}

fn take_stale_authority(
    origin: t::ClientDiscriminant,
    decoder: &mut Decoder<'_>,
) -> Result<r::StaleAuthority, CodecError> {
    match origin {
        t::ClientDiscriminant::CredentialAttachRequest => Ok(r::StaleAuthority::Live {
            request: r::CommonStaleAuthorityEnvelope::CredentialAttach(take_attach(decoder)?),
            current_generation: decoder.take_generation()?,
        }),
        t::ClientDiscriminant::ParticipantAck => Ok(r::StaleAuthority::Live {
            request: r::CommonStaleAuthorityEnvelope::ParticipantAck(take_participant_ack(
                decoder,
            )?),
            current_generation: decoder.take_generation()?,
        }),
        t::ClientDiscriminant::MarkerAck => Ok(r::StaleAuthority::Live {
            request: r::CommonStaleAuthorityEnvelope::MarkerAck(take_marker_ack(decoder)?),
            current_generation: decoder.take_generation()?,
        }),
        t::ClientDiscriminant::RecordAdmission => Ok(r::StaleAuthority::Live {
            request: r::CommonStaleAuthorityEnvelope::RecordAdmission(take_record_admission(
                decoder,
            )?),
            current_generation: decoder.take_generation()?,
        }),
        t::ClientDiscriminant::DetachRequest => {
            let authority = t::DetachAuthorityStateTag::try_from(decoder.take_u16()?)
                .map_err(|_| decode_error(t::DecodeClass::InvalidField))?;
            let value = match authority {
                t::DetachAuthorityStateTag::Live => r::DetachStaleAuthority::Live {
                    conversation_id: decoder.take_u64()?,
                    participant_id: decoder.take_u64()?,
                    capability_generation: decoder.take_generation()?,
                    detach_attempt_token: DetachAttemptToken::new(
                        decoder.take_fixed::<TOKEN_LEN>()?,
                    ),
                    current_generation: decoder.take_generation()?,
                },
                t::DetachAuthorityStateTag::TerminalizedDetachCell => {
                    let conversation_id = decoder.take_u64()?;
                    let participant_id = decoder.take_u64()?;
                    let capability_generation = decoder.take_generation()?;
                    let detach_attempt_token =
                        DetachAttemptToken::new(decoder.take_fixed::<TOKEN_LEN>()?);
                    let current_generation = decoder.take_generation()?;
                    let committed_binding_epoch = decoder.take_binding_epoch()?;
                    let binding_state = match t::BindingStateTag::try_from(decoder.take_u16()?)
                        .map_err(|_| decode_error(t::DecodeClass::InvalidField))?
                    {
                        t::BindingStateTag::Bound => r::BindingStateView::Bound {
                            current_binding_epoch: decoder.take_binding_epoch()?,
                        },
                        t::BindingStateTag::Detached => r::BindingStateView::Detached,
                    };
                    r::DetachStaleAuthority::TerminalizedDetachCell(
                        r::TerminalizedDetachCell::from_wire_decode(
                            TERMINALIZED_WIRE_DECODE_AUTHORITY,
                            conversation_id,
                            participant_id,
                            capability_generation,
                            detach_attempt_token,
                            current_generation,
                            committed_binding_epoch,
                            binding_state,
                        ),
                    )
                }
            };
            Ok(r::StaleAuthority::Detach(value))
        }
        t::ClientDiscriminant::LeaveRequest => {
            let authority = t::LeaveAuthorityStateTag::try_from(decoder.take_u16()?)
                .map_err(|_| decode_error(t::DecodeClass::InvalidField))?;
            let conversation_id = decoder.take_u64()?;
            let participant_id = decoder.take_u64()?;
            let presented_generation = decoder.take_generation()?;
            let leave_attempt_token = LeaveAttemptToken::new(decoder.take_fixed::<TOKEN_LEN>()?);
            let value = match authority {
                t::LeaveAuthorityStateTag::Live => r::LeaveStaleAuthority::Live {
                    conversation_id,
                    participant_id,
                    presented_generation,
                    leave_attempt_token,
                    current_generation: decoder.take_generation()?,
                },
                t::LeaveAuthorityStateTag::CommittedLeaveTombstone => {
                    r::LeaveStaleAuthority::CommittedLeaveTombstone {
                        conversation_id,
                        participant_id,
                        presented_generation,
                        leave_attempt_token,
                        retired_generation: decoder.take_generation()?,
                    }
                }
            };
            Ok(r::StaleAuthority::Leave(value))
        }
        t::ClientDiscriminant::EnrollmentRequest
        | t::ClientDiscriminant::ObserverRecoveryHandshake => {
            Err(decode_error(t::DecodeClass::InvalidField))
        }
    }
}

fn take_enroll_bound(decoder: &mut Decoder<'_>) -> Result<r::EnrollBound, CodecError> {
    let conversation_id = decoder.take_u64()?;
    let token = EnrollmentToken::new(decoder.take_fixed::<TOKEN_LEN>()?);
    let participant_id = decoder.take_u64()?;
    if decoder.take_option_generation()?.is_some() {
        decoder.invalidate();
    }
    let capability_generation = decoder.take_generation()?;
    if capability_generation.get() != 1 {
        decoder.invalidate();
    }
    let attach_secret = AttachSecret::new(decoder.take_fixed::<SECRET_LEN>()?);
    let origin_binding_epoch = decoder.take_binding_epoch()?;
    if origin_binding_epoch.capability_generation.get() != 1 {
        decoder.invalidate();
    }
    if decoder.take_u64()? != 0 {
        decoder.invalidate();
    }
    if decoder.take_option_u64()?.is_some() {
        decoder.invalidate();
    }
    let receipt_expires_at = decoder.take_u128()?;
    let provenance_expires_at = decoder.take_u128()?;

    let valid_epoch = if origin_binding_epoch.capability_generation.get() == 1 {
        origin_binding_epoch
    } else {
        BindingEpoch::new(
            origin_binding_epoch.connection_incarnation,
            generation_one()?,
        )
    };
    r::EnrollBound::new(
        conversation_id,
        token,
        participant_id,
        attach_secret,
        valid_epoch,
        receipt_expires_at,
        provenance_expires_at,
    )
    .ok_or(CodecError::InvalidValue)
}

fn take_receipt_expired(
    origin: t::ClientDiscriminant,
    decoder: &mut Decoder<'_>,
) -> Result<r::ReceiptExpired, CodecError> {
    let conversation_id = decoder.take_u64()?;
    let token = decoder.take_fixed::<TOKEN_LEN>()?;
    let participant_id = decoder.take_u64()?;
    match origin {
        t::ClientDiscriminant::EnrollmentRequest => {
            if decoder.take_option_generation()?.is_some() {
                decoder.invalidate();
            }
            Ok(r::ReceiptExpired::Enrollment {
                conversation_id,
                token: EnrollmentToken::new(token),
                participant_id,
                result_generation: decoder.take_generation()?,
                current_generation: decoder.take_generation()?,
                reason: take_tag::<t::ReceiptExpiryReason>(decoder)?,
            })
        }
        t::ClientDiscriminant::CredentialAttachRequest => {
            let presented_generation = required_generation_option(decoder)?;
            Ok(r::ReceiptExpired::CredentialAttach {
                conversation_id,
                token: AttachAttemptToken::new(token),
                participant_id,
                presented_generation,
                presented_marker_delivery_seq: decoder.take_option_u64()?,
                result_generation: decoder.take_generation()?,
                current_generation: decoder.take_generation()?,
                reason: take_tag::<t::ReceiptExpiryReason>(decoder)?,
            })
        }
        _ => Err(decode_error(t::DecodeClass::InvalidField)),
    }
}

impl WireTag for t::ReceiptExpiryReason {}
impl WireTag for t::ReceiptCapacityScope {}

fn take_receipt_capacity(
    origin: t::ClientDiscriminant,
    decoder: &mut Decoder<'_>,
) -> Result<r::ReceiptCapacityExceeded, CodecError> {
    match origin {
        t::ClientDiscriminant::EnrollmentRequest => {
            let request = take_enrollment(decoder)?;
            let scope = match take_tag::<t::ReceiptCapacityScope>(decoder)? {
                t::ReceiptCapacityScope::LiveReceiptServer => {
                    r::EnrollmentReceiptCapacityScope::LiveReceiptServer
                }
                t::ReceiptCapacityScope::ProvenanceServer => {
                    r::EnrollmentReceiptCapacityScope::ProvenanceServer
                }
                t::ReceiptCapacityScope::ProvenanceConversation => {
                    r::EnrollmentReceiptCapacityScope::ProvenanceConversation
                }
                t::ReceiptCapacityScope::LiveReceiptParticipant
                | t::ReceiptCapacityScope::ProvenanceParticipant => {
                    return Err(decode_error(t::DecodeClass::InvalidField));
                }
            };
            let limit = decoder.take_u64()?;
            let occupied = decoder.take_u64()?;
            require_one(decoder, r::ReceiptCapacityExceeded::REQUESTED)?;
            Ok(r::ReceiptCapacityExceeded::Enrollment {
                request,
                scope,
                limit,
                occupied,
            })
        }
        t::ClientDiscriminant::CredentialAttachRequest => {
            let request = take_attach(decoder)?;
            let scope = take_tag::<t::ReceiptCapacityScope>(decoder)?;
            let limit = decoder.take_u64()?;
            let occupied = decoder.take_u64()?;
            require_one(decoder, r::ReceiptCapacityExceeded::REQUESTED)?;
            Ok(r::ReceiptCapacityExceeded::CredentialAttach {
                request,
                scope,
                limit,
                occupied,
            })
        }
        _ => Err(decode_error(t::DecodeClass::InvalidField)),
    }
}

fn require_one(decoder: &mut Decoder<'_>, expected: u64) -> Result<(), CodecError> {
    if decoder.take_u64()? != expected {
        decoder.invalidate();
    }
    Ok(())
}

fn take_observer_backpressure(
    origin: t::ClientDiscriminant,
    decoder: &mut Decoder<'_>,
) -> Result<r::ObserverBackpressure, CodecError> {
    match origin {
        t::ClientDiscriminant::EnrollmentRequest => Ok(r::ObserverBackpressure::Enrollment {
            request: take_enrollment(decoder)?,
            state: take_backpressure_state(decoder)?,
        }),
        t::ClientDiscriminant::CredentialAttachRequest => {
            Ok(r::ObserverBackpressure::CredentialAttach {
                request: take_attach(decoder)?,
                state: take_backpressure_state(decoder)?,
            })
        }
        t::ClientDiscriminant::DetachRequest => Ok(r::ObserverBackpressure::Detach {
            request: take_detach(decoder)?,
            committed_binding_epoch: decoder.take_binding_epoch()?,
            state: take_backpressure_state(decoder)?,
        }),
        t::ClientDiscriminant::LeaveRequest => Ok(r::ObserverBackpressure::Leave {
            request: take_leave(decoder)?,
            state: take_backpressure_state(decoder)?,
            prior_terminal_cell_exists: decoder.take_bool()?,
        }),
        t::ClientDiscriminant::RecordAdmission => Ok(r::ObserverBackpressure::RecordAdmission {
            request: take_record_admission(decoder)?,
            state: take_backpressure_state(decoder)?,
        }),
        _ => Err(decode_error(t::DecodeClass::InvalidField)),
    }
}

fn take_backpressure_state(
    decoder: &mut Decoder<'_>,
) -> Result<r::ObserverBackpressureState, CodecError> {
    let backpressure_epoch = decoder.take_u64()?;
    let observer_progress = decoder.take_u64()?;
    r::ObserverBackpressureState::replay(backpressure_epoch, observer_progress)
        .ok_or_else(|| decode_error(t::DecodeClass::InvalidField))
}

fn required_generation_option(decoder: &mut Decoder<'_>) -> Result<Generation, CodecError> {
    decoder.take_option_generation()?.map_or_else(
        || {
            decoder.invalidate();
            generation_one()
        },
        Ok,
    )
}

fn take_attach_bound(decoder: &mut Decoder<'_>) -> Result<r::AttachBound, CodecError> {
    let conversation_id = decoder.take_u64()?;
    let token = AttachAttemptToken::new(decoder.take_fixed::<TOKEN_LEN>()?);
    let participant_id = decoder.take_u64()?;
    let request_generation = required_generation_option(decoder)?;
    let capability_generation = decoder.take_generation()?;
    let attach_secret = AttachSecret::new(decoder.take_fixed::<SECRET_LEN>()?);
    let origin_binding_epoch = decoder.take_binding_epoch()?;
    let persisted_cursor = decoder.take_u64()?;
    let accepted_marker_delivery_seq = decoder.take_option_u64()?;
    let receipt_expires_at = decoder.take_u128()?;
    let provenance_expires_at = decoder.take_u128()?;

    let value = match accepted_marker_delivery_seq {
        Some(marker) if persisted_cursor == marker => r::AttachBound::fenced(
            conversation_id,
            token,
            participant_id,
            request_generation,
            attach_secret,
            origin_binding_epoch,
            marker,
            receipt_expires_at,
            provenance_expires_at,
        ),
        Some(_) => None,
        None => r::AttachBound::ordinary(
            conversation_id,
            token,
            participant_id,
            request_generation,
            attach_secret,
            origin_binding_epoch,
            persisted_cursor,
            receipt_expires_at,
            provenance_expires_at,
        ),
    }
    .ok_or_else(|| decode_error(t::DecodeClass::InvalidField))?;
    if value.capability_generation() == capability_generation {
        Ok(value)
    } else {
        Err(decode_error(t::DecodeClass::InvalidField))
    }
}

fn take_receipt_replay(
    origin: t::ClientDiscriminant,
    decoder: &mut Decoder<'_>,
) -> Result<r::ReceiptReplay, CodecError> {
    let conversation_id = decoder.take_u64()?;
    let token = decoder.take_fixed::<TOKEN_LEN>()?;
    let participant_id = decoder.take_u64()?;
    match origin {
        t::ClientDiscriminant::EnrollmentRequest => {
            let request_generation = decoder.take_option_generation()?;
            let capability_generation = decoder.take_generation()?;
            let attach_secret = AttachSecret::new(decoder.take_fixed::<SECRET_LEN>()?);
            let origin_binding_epoch = decoder.take_binding_epoch()?;
            let persisted_cursor = decoder.take_u64()?;
            let accepted_marker_delivery_seq = decoder.take_option_u64()?;
            let receipt_expires_at = decoder.take_u128()?;
            let provenance_expires_at = decoder.take_u128()?;
            let value = r::EnrollBound::new(
                conversation_id,
                EnrollmentToken::new(token),
                participant_id,
                attach_secret,
                origin_binding_epoch,
                receipt_expires_at,
                provenance_expires_at,
            )
            .ok_or_else(|| decode_error(t::DecodeClass::InvalidField))?;
            if request_generation.is_none()
                && capability_generation == Generation::ONE
                && persisted_cursor == 0
                && accepted_marker_delivery_seq.is_none()
            {
                Ok(r::ReceiptReplay::Enrollment(value))
            } else {
                Err(decode_error(t::DecodeClass::InvalidField))
            }
        }
        t::ClientDiscriminant::CredentialAttachRequest => {
            let request_generation = required_generation_option(decoder)?;
            let capability_generation = decoder.take_generation()?;
            let attach_secret = AttachSecret::new(decoder.take_fixed::<SECRET_LEN>()?);
            let origin_binding_epoch = decoder.take_binding_epoch()?;
            let persisted_cursor = decoder.take_u64()?;
            let accepted_marker_delivery_seq = decoder.take_option_u64()?;
            let receipt_expires_at = decoder.take_u128()?;
            let provenance_expires_at = decoder.take_u128()?;
            let attach_token = AttachAttemptToken::new(token);
            let value = match accepted_marker_delivery_seq {
                Some(marker) if persisted_cursor == marker => r::AttachBound::fenced(
                    conversation_id,
                    attach_token,
                    participant_id,
                    request_generation,
                    attach_secret,
                    origin_binding_epoch,
                    marker,
                    receipt_expires_at,
                    provenance_expires_at,
                ),
                Some(_) => None,
                None => r::AttachBound::ordinary(
                    conversation_id,
                    attach_token,
                    participant_id,
                    request_generation,
                    attach_secret,
                    origin_binding_epoch,
                    persisted_cursor,
                    receipt_expires_at,
                    provenance_expires_at,
                ),
            }
            .ok_or_else(|| decode_error(t::DecodeClass::InvalidField))?;
            if value.capability_generation() == capability_generation {
                Ok(r::ReceiptReplay::CredentialAttach(value))
            } else {
                Err(decode_error(t::DecodeClass::InvalidField))
            }
        }
        _ => Err(decode_error(t::DecodeClass::InvalidField)),
    }
}

fn take_ack_no_op(
    origin: t::ClientDiscriminant,
    decoder: &mut Decoder<'_>,
) -> Result<r::AckNoOp, CodecError> {
    match origin {
        t::ClientDiscriminant::ParticipantAck => {
            let request = take_participant_ack(decoder)?;
            if decoder.take_u64()? != request.through_seq {
                decoder.invalidate();
            }
            Ok(r::AckNoOp::participant_ack(request))
        }
        t::ClientDiscriminant::MarkerAck => {
            let request = take_marker_ack(decoder)?;
            if decoder.take_u64()? != request.marker_delivery_seq {
                decoder.invalidate();
            }
            Ok(r::AckNoOp::marker_ack(request))
        }
        _ => Err(decode_error(t::DecodeClass::InvalidField)),
    }
}

fn take_recovery_accepted(
    decoder: &mut Decoder<'_>,
) -> Result<r::ObserverRecoveryAccepted, CodecError> {
    let count = decoder.take_u64()?;
    let required = u128::from(count) * RECOVERY_STATUS_LEN;
    let remaining = u64::try_from(decoder.remaining())
        .map_err(|_| decode_error(t::DecodeClass::MissingRequiredField))?;
    if required > u128::from(remaining) {
        return Err(decode_error(t::DecodeClass::MissingRequiredField));
    }
    let count_usize: usize = count
        .try_into()
        .map_err(|_| decode_error(t::DecodeClass::MissingRequiredField))?;

    let mut statuses = Vec::new();
    for _ in 0..count_usize {
        statuses.push(r::ObserverProgressStatus {
            conversation_id: decoder.take_u64()?,
            refused_epoch: decoder.take_u64()?,
            current_observer_progress: decoder.take_u64()?,
            armed: decoder.take_bool()?,
            progressed: decoder.take_bool()?,
        });
    }
    Ok(r::ObserverRecoveryAccepted { statuses })
}

fn require_zero_recovery_count(decoder: &mut Decoder<'_>) -> Result<(), CodecError> {
    if decoder.take_u64()? == 0 {
        Ok(())
    } else {
        Err(decode_error(t::DecodeClass::InvalidField))
    }
}

fn take_invalid_observer_epoch(
    decoder: &mut Decoder<'_>,
) -> Result<r::InvalidObserverEpoch, CodecError> {
    let reason = take_tag::<t::InvalidObserverEpochReason>(decoder)?;
    let conversation_id = decoder.take_u64()?;
    let presented_epoch = decoder.take_u64()?;
    let current = decoder.take_option_u64()?;
    match reason {
        t::InvalidObserverEpochReason::ConversationUnknown => {
            if current.is_some() {
                decoder.invalidate();
            }
            Ok(r::InvalidObserverEpoch::ConversationUnknown {
                conversation_id,
                presented_epoch,
            })
        }
        t::InvalidObserverEpochReason::EpochAhead => {
            let current_observer_progress = current.map_or_else(
                || {
                    decoder.invalidate();
                    0
                },
                core::convert::identity,
            );
            Ok(r::InvalidObserverEpoch::EpochAhead {
                conversation_id,
                presented_epoch,
                current_observer_progress,
            })
        }
    }
}

fn take_invalid_observer_epoch_list(
    decoder: &mut Decoder<'_>,
) -> Result<r::InvalidObserverEpochList, CodecError> {
    match take_tag::<t::InvalidObserverEpochListReason>(decoder)? {
        t::InvalidObserverEpochListReason::TooManyEntries => {
            Ok(r::InvalidObserverEpochList::TooManyEntries {
                presented_entries: decoder.take_u64()?,
                max_entries: decoder.take_u64()?,
            })
        }
        t::InvalidObserverEpochListReason::DuplicateConversation => {
            Ok(r::InvalidObserverEpochList::DuplicateConversation {
                conversation_id: decoder.take_u64()?,
                first_index: decoder.take_u64()?,
                duplicate_index: decoder.take_u64()?,
            })
        }
    }
}
