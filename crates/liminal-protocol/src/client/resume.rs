use alloc::vec::Vec;

use super::{
    ClientBindingState, ClientParticipantAggregate, DetachReplayStatus, DetachReplayTerminal,
    ExpectedOperationState, ReconnectAggregate, SdkDetachReplayAggregate,
    reconnect::ReconnectMachineState, replay::DetachReplayState,
};
use super::{resume_decode::decode_facts, resume_encode::encode_aggregate};
use crate::wire::{ClientRequest, CodecError};

pub(super) const MAGIC: [u8; 4] = *b"LPCR";
pub(super) const SPECULATIVE_MAGIC: [u8; 4] = *b"LPCP";
pub(super) const VERSION: u16 = 1;
pub(super) const HEADER_LEN: usize = 14;

/// Section whose tag or nested canonical frame was invalid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientResumeRecordSection {
    /// Client binding state.
    Binding,
    /// Outstanding expected operation.
    ExpectedOperation,
    /// Detach replay state.
    DetachReplay,
    /// Reconnect permit or attempt state.
    Reconnect,
}

/// Failure while creating a canonical record from live client state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientResumeRecordEncodeError {
    /// A nested request or terminal value cannot use the canonical wire codec.
    NestedCodec {
        /// Failing section.
        section: ClientResumeRecordSection,
        /// Exact wire codec error.
        source: CodecError,
    },
    /// The own-codec payload cannot fit its u64 length field.
    LengthOverflow,
}

/// Typed canonical client-record decode failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientResumeRecordDecodeError {
    /// Input ended before the requested number of bytes.
    Truncated {
        /// Bytes required at the failure point.
        needed: usize,
        /// Bytes remaining at the failure point.
        remaining: usize,
    },
    /// The four-byte client-record magic was not `LPCR`.
    InvalidMagic {
        /// Presented magic bytes.
        presented: [u8; 4],
    },
    /// The client-record envelope version is unsupported.
    UnsupportedVersion {
        /// Presented version.
        presented: u16,
    },
    /// Declared payload length differs from the exact remaining bytes.
    LengthMismatch {
        /// Declared payload bytes.
        declared: u64,
        /// Actual payload bytes.
        actual: usize,
    },
    /// A closed section tag was unknown.
    InvalidTag {
        /// Section containing the tag.
        section: ClientResumeRecordSection,
        /// Unknown tag.
        tag: u8,
    },
    /// A nested canonical participant frame was invalid or had the wrong direction.
    NestedCodec {
        /// Failing section.
        section: ClientResumeRecordSection,
        /// Exact wire codec error when structural decode failed.
        source: Option<CodecError>,
    },
    /// Extra bytes followed the four exact sections.
    TrailingBytes {
        /// Number of unexpected bytes.
        remaining: usize,
    },
}

/// Validated cold-restore invariant failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientResumeRestoreError {
    /// A bound credential generation differs from its binding epoch.
    BindingGenerationMismatch,
    /// A continuous acknowledgement illegally occupied the write-ahead slot.
    ContinuousAckOutstanding,
    /// Replay terminal payload does not match its retained exact detach request.
    ReplayTerminalMismatch,
    /// Expected operation authorization is zero or exceeds its durable counter.
    InvalidOperationAuthorization,
    /// Expected operation is illegal for the restored binding state or identity.
    ExpectedBindingMismatch,
    /// Reconnect authorization is zero or exceeds its durable counter.
    InvalidReconnectAuthorization,
    /// Private canonical bytes no longer decode; this is unreachable through public construction.
    CorruptRecord(ClientResumeRecordDecodeError),
}

/// Private-field, inert, canonical client persistence record.
///
/// This type proves only that bytes are a canonical committed-record envelope;
/// the storage owner remains responsible for admitting bytes from exactly one
/// cold process epoch. Preventing two processes from restoring the same bytes
/// is intentionally outside this `no_std` protocol crate, matching the server
/// precedent in `lifecycle/storage.rs`, whose joint validation likewise does
/// not prevent storage-owner double restore. Issuance flags are nevertheless
/// preserved exactly, so a record that testifies an authority was already
/// issued never silently re-mints that authority.
#[derive(Debug, PartialEq, Eq)]
pub struct ClientResumeRecord {
    canonical: Vec<u8>,
}

impl ClientResumeRecord {
    /// Encodes this already-validated record in canonical v1 form.
    #[must_use]
    pub fn encode_canonical(&self) -> Vec<u8> {
        self.canonical.clone()
    }

    /// Decodes one exact canonical v1 client record without minting authority.
    ///
    /// # Errors
    ///
    /// Returns typed truncation, magic, version, tag, length, nested-codec, or
    /// trailing-byte errors.
    pub fn decode_canonical(input: &[u8]) -> Result<Self, ClientResumeRecordDecodeError> {
        let _ = decode_facts(input)?;
        Ok(Self {
            canonical: input.to_vec(),
        })
    }

    /// Validates cross-fact invariants and cold-restores executable client state.
    ///
    /// # Errors
    ///
    /// Returns a typed invariant error before any aggregate or reconnect
    /// authority escapes.
    pub fn restore(self) -> Result<ClientParticipantAggregate, ClientResumeRestoreError> {
        let facts =
            decode_facts(&self.canonical).map_err(ClientResumeRestoreError::CorruptRecord)?;
        validate_facts(&facts)?;
        Ok(ClientParticipantAggregate {
            binding: facts.binding,
            expected: facts.expected,
            next_operation_authorization: facts.next_operation_authorization,
            detach_replay: SdkDetachReplayAggregate {
                state: facts.replay,
            },
            reconnect: ReconnectAggregate {
                state: facts.reconnect_state,
                next_authorization: facts.next_authorization,
            },
        })
    }
}

impl ClientParticipantAggregate {
    /// Captures every durable client fact in an inert resume record.
    ///
    /// # Errors
    ///
    /// Returns a typed nested-codec or length error if a live typed value cannot
    /// be represented canonically.
    pub fn resume_record(&self) -> Result<ClientResumeRecord, ClientResumeRecordEncodeError> {
        Ok(ClientResumeRecord {
            canonical: encode_aggregate(self, MAGIC)?,
        })
    }
}

impl super::ClientPendingOperationRecord {
    /// Encodes the speculative successor under the non-restorable `LPCP` magic.
    ///
    /// No aggregate, request, executable operation, or publicly restorable
    /// `LPCR` envelope escapes through this method. This explicit speculative
    /// shape is the brief-required distinction between write-ahead persistence
    /// and cold restore of a committed fact.
    ///
    /// # Errors
    ///
    /// Returns a typed nested-codec or length error; callers can then abort the
    /// pending decision unchanged.
    pub fn encode_resume_record(&self) -> Result<Vec<u8>, ClientResumeRecordEncodeError> {
        encode_aggregate(&self.successor, SPECULATIVE_MAGIC)
    }
}

impl super::ClientOperationCommit {
    /// Captures the committed successor as a publicly cold-restorable record.
    ///
    /// The caller persists these `LPCR` bytes before releasing the aggregate and
    /// operation with [`Self::into_parts`]. Pending `LPCP` bytes cannot be
    /// decoded or promoted through this API.
    ///
    /// # Errors
    ///
    /// Returns a typed nested-codec or length error before authority release.
    pub fn resume_record(&self) -> Result<ClientResumeRecord, ClientResumeRecordEncodeError> {
        Ok(ClientResumeRecord {
            canonical: encode_aggregate(&self.aggregate, MAGIC)?,
        })
    }
}

pub(super) struct DecodedFacts {
    pub(super) binding: ClientBindingState,
    pub(super) next_operation_authorization: u64,
    pub(super) expected: Option<ExpectedOperationState>,
    pub(super) replay: DetachReplayState,
    pub(super) reconnect_state: ReconnectMachineState,
    pub(super) next_authorization: u64,
}

fn validate_facts(facts: &DecodedFacts) -> Result<(), ClientResumeRestoreError> {
    if let ClientBindingState::Bound {
        generation,
        binding_epoch,
        ..
    } = facts.binding
        && generation != binding_epoch.capability_generation
    {
        return Err(ClientResumeRestoreError::BindingGenerationMismatch);
    }
    if matches!(
        facts.expected,
        Some(ExpectedOperationState {
            request: ClientRequest::ParticipantAck(_),
            ..
        })
    ) {
        return Err(ClientResumeRestoreError::ContinuousAckOutstanding);
    }
    if facts.expected.as_ref().is_some_and(|expected| {
        expected.authorization == 0 || expected.authorization > facts.next_operation_authorization
    }) {
        return Err(ClientResumeRestoreError::InvalidOperationAuthorization);
    }
    if facts
        .expected
        .as_ref()
        .is_some_and(|expected| !facts.binding.accepts_request(&expected.request))
    {
        return Err(ClientResumeRestoreError::ExpectedBindingMismatch);
    }
    if let DetachReplayState::Recorded {
        request,
        status: DetachReplayStatus::Terminal(terminal),
    } = &facts.replay
        && !terminal_matches(request, terminal)
    {
        return Err(ClientResumeRestoreError::ReplayTerminalMismatch);
    }
    let authorization = match facts.reconnect_state {
        ReconnectMachineState::Permit { authorization, .. }
        | ReconnectMachineState::Attempt { authorization, .. } => Some(authorization),
        ReconnectMachineState::Parked | ReconnectMachineState::Online => None,
    };
    if authorization.is_some_and(|value| value == 0 || value > facts.next_authorization) {
        return Err(ClientResumeRestoreError::InvalidReconnectAuthorization);
    }
    Ok(())
}

fn terminal_matches(
    request: &crate::wire::DetachEnvelope,
    terminal: &DetachReplayTerminal,
) -> bool {
    match terminal {
        DetachReplayTerminal::DetachCommitted(value) => {
            value.conversation_id() == request.conversation_id
                && value.participant_id() == request.participant_id
                && value.capability_generation() == request.capability_generation
                && value.detach_attempt_token() == request.detach_attempt_token
        }
        DetachReplayTerminal::DetachInProgress(value) => {
            let expected_generation = request.capability_generation;
            let presented_generation = value.presented_generation;
            let expected_token = request.detach_attempt_token;
            let presented_token = value.presented_token;
            value.conversation_id == request.conversation_id
                && value.participant_id == request.participant_id
                && presented_generation == expected_generation
                && presented_token == expected_token
        }
        DetachReplayTerminal::TerminalizedDetachCell(value) => {
            value.conversation_id() == request.conversation_id
                && value.participant_id() == request.participant_id
                && value.capability_generation() == request.capability_generation
                && value.detach_attempt_token() == request.detach_attempt_token
        }
    }
}
