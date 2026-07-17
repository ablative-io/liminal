//! Remote participant state, durable client records, and typed transport outcomes.
//!
//! This module owns process mechanics only. Every participant lifecycle and
//! correlation decision is delegated to `liminal-protocol`; the SDK stores the
//! crate aggregate and its sealed one-use authorities without mirroring their
//! rules.

mod recovery;
mod replay_apply;

pub use recovery::{
    RemoteDetachReplayOutcome, RemoteExpectedOperationRecovery, RemoteLostOperationResolution,
    RemoteLostReconnectResolution, RemoteReconnectAttemptOutcome, RemoteReconnectPermitRecovery,
    RemoteReplayApplyOutcome, RemoteTransportLossOutcome,
};

use alloc::sync::Arc;
use core::fmt;

use liminal_protocol::client::{
    ClientCorrelatedInboundDecision, ClientInboundDecision, ClientInboundRefusalReason,
    ClientOperationRecordDecision, ClientOperationRecordRefusalReason, ClientParticipantAggregate,
    ClientResponseCorrelation, ClientResumeRecord, ClientResumeRecordDecodeError,
    ClientResumeRecordEncodeError, ClientResumeRestoreError, ExpectedOperationFateRefusalReason,
    ExpectedOperationTransportFate, ExpectedParticipantOperation, ReconnectPermitDecision,
    decide_correlated_inbound, decide_inbound, record_expected_operation_fate,
    record_transport_fate,
};
use liminal_protocol::outcome::ReconnectDelayResult;
use liminal_protocol::wire::{ClientRequest, ParticipantFrame, ServerPush, ServerValue};
use spin::Mutex;

use crate::SdkError;

use super::protocol::{ParticipantTransportFrame, RemoteTransport};
use super::{RemoteConfig, ServerAddress};

/// Storage boundary for canonical `LPCR` client resume bytes.
///
/// Implementations must replace the previously committed bytes durably before
/// returning `Ok(())`. The SDK calls this boundary after the protocol crate's
/// commit seal and before releasing executable operation authority.
pub trait ParticipantResumeStore: Send {
    /// Durably replaces the stored canonical client resume record.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Store`] when the bytes were not durably committed.
    fn persist(&mut self, canonical_lpcr: &[u8]) -> Result<(), SdkError>;
}

/// Transport-layer testimony identifying the connection attempt that delivered a frame.
///
/// This is the sealed transport context anticipated by rationale 15 in
/// `LP-CLIENT-GOAL`. It does not alter the wire format or relax the protocol
/// crate's conservative `RecordAdmission` ambiguity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParticipantResponseProvenance {
    connection_id: u64,
    attempt_id: u64,
}

impl ParticipantResponseProvenance {
    #[cfg(feature = "std")]
    pub(super) const fn new(connection_id: u64, attempt_id: u64) -> Self {
        Self {
            connection_id,
            attempt_id,
        }
    }

    /// Returns the local identity of the established socket.
    #[must_use]
    pub const fn connection_id(self) -> u64 {
        self.connection_id
    }

    /// Returns the local identity of the real connection attempt.
    #[must_use]
    pub const fn attempt_id(self) -> u64 {
        self.attempt_id
    }
}

/// Failure at the SDK participant state, codec, storage, or transport boundary.
#[derive(Debug, thiserror::Error)]
pub enum RemoteParticipantError {
    /// A prior commit could not be persisted, so no aggregate authority remains reachable.
    #[error("participant state is unavailable after an unreleased durability failure")]
    StateUnavailable,
    /// The protocol crate could not encode the current aggregate as canonical LPCR.
    #[error("client resume record encode failed: {0:?}")]
    ResumeEncode(ClientResumeRecordEncodeError),
    /// Persisted bytes were not a canonical LPCR record.
    #[error("client resume record decode failed: {0:?}")]
    ResumeDecode(ClientResumeRecordDecodeError),
    /// Canonical facts violated a protocol restore invariant.
    #[error("client resume record restore failed: {0:?}")]
    ResumeRestore(ClientResumeRestoreError),
    /// The caller-owned durable store rejected a canonical record.
    #[error("client resume record persistence failed: {0}")]
    Storage(SdkError),
    /// The real transport failed outside a typed fate-reporting operation.
    #[error("participant transport failed: {0}")]
    Transport(SdkError),
    /// A client-to-server request appeared on the SDK receive side.
    #[error("participant transport decoded a request in the client receive direction")]
    InvalidInboundDirection,
    /// No live response correlation exists for a replay-specific input.
    #[error("no live participant response authority is held")]
    ResponseAuthorityUnavailable,
}

/// Opaque one-use operation released only after the SDK persisted sealed LPCR bytes.
#[derive(Debug)]
pub struct RemoteParticipantOperation {
    operation: ExpectedParticipantOperation,
    durability: OperationDurability,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OperationDurability {
    WriteAhead,
    Continuous,
}

/// Result of admitting an outbound request through the crate write-ahead barrier.
#[derive(Debug)]
pub enum RemoteOperationRecordOutcome {
    /// Canonical LPCR bytes were persisted and one operation may now be sent.
    Recorded(RemoteParticipantOperation),
    /// A continuous acknowledgement bypassed the write-ahead slot by crate rule.
    Continuous(RemoteParticipantOperation),
    /// The crate refused the exact request without changing aggregate state.
    Refused {
        /// Exact refused request.
        request: ClientRequest,
        /// Closed protocol refusal reason.
        reason: ClientOperationRecordRefusalReason,
    },
}

/// Typed operation-domain consequence of an established transport loss.
#[derive(Debug, PartialEq, Eq)]
pub enum RemoteOperationTransportFate {
    /// A non-detach operation's response became unavailable.
    Recorded {
        /// Exact terminalized request.
        request: ClientRequest,
    },
    /// The exact detach was returned to parked replay.
    DetachParked,
    /// The crate retained the live correlation unchanged.
    Refused {
        /// Closed refusal reason from the generic operation-fate gate.
        reason: ExpectedOperationFateRefusalReason,
    },
    /// No response authority was outstanding.
    NotOutstanding,
}

/// Typed reconnect permit returned by a crate-authorized fresh event.
#[derive(Debug)]
pub struct RemoteReconnectPermit {
    pub(super) permit: liminal_protocol::client::ReconnectAttemptPermit,
}

/// Event-driven reconnect permit decision; there is no delay or timer arm.
#[derive(Debug)]
pub enum RemoteReconnectPermitOutcome {
    /// The crate minted one one-use permit.
    Permitted {
        /// Opaque permit for one real connection attempt.
        permit: RemoteReconnectPermit,
        /// Legacy-named crate result whose value is event-only.
        result: ReconnectDelayResult,
    },
    /// Existing authority was retained.
    Refused {
        /// Closed crate refusal reason.
        reason: liminal_protocol::client::ReconnectPermitRefusalReason,
        /// Event-only crate result.
        result: ReconnectDelayResult,
    },
}

/// Result of sending an operation on the participant transport.
#[derive(Debug)]
pub enum RemoteParticipantSendOutcome {
    /// The request bytes were written on this connection attempt.
    Sent {
        /// Sealed transport context for a later response.
        provenance: ParticipantResponseProvenance,
    },
    /// The write failed and both operation and reconnect fates were delegated.
    TransportLost {
        /// Concrete socket failure.
        error: SdkError,
        /// Crate-owned operation-fate result.
        operation_fate: RemoteOperationTransportFate,
        /// Crate-owned reconnect permit result.
        reconnect: RemoteReconnectPermitOutcome,
    },
}

/// Typed result of one decoded participant frame on the real receive path.
#[derive(Debug)]
pub enum RemoteParticipantInbound {
    /// The protocol crate correlated and applied a semantic response.
    Applied {
        /// Exact applied server value.
        value: ServerValue,
        /// Connection/attempt that delivered it.
        provenance: ParticipantResponseProvenance,
    },
    /// The protocol crate retained the response and aggregate unchanged.
    Refused {
        /// Exact refused server value.
        value: ServerValue,
        /// Closed crate refusal reason, including conservative ambiguity.
        reason: ClientInboundRefusalReason,
        /// Connection/attempt that delivered it.
        provenance: ParticipantResponseProvenance,
    },
    /// Server push decoded in the client direction; no correlation rule applies.
    Push {
        /// Exact pushed value.
        value: ServerPush,
        /// Connection/attempt that delivered it.
        provenance: ParticipantResponseProvenance,
    },
}

pub(super) struct RemoteParticipantState<S> {
    pub(super) aggregate: Option<ClientParticipantAggregate>,
    pub(super) correlation: Option<ClientResponseCorrelation>,
    pub(super) reconnect_attempt: Option<liminal_protocol::client::ReconnectInProgressAttempt>,
    pub(super) store: S,
}

/// Remote participant entrypoint backed by protocol-crate state and canonical LPCR storage.
///
/// Records are deliberately not promised as generally successful: the reduced-B1
/// server surface fails fully authorized `RecordAdmission` and `Leave` closed until
/// live claim-frontier acquisition lands (`docs/design/LP-GAP-CLOSURE-GOAL.md:145`).
pub struct RemoteParticipantHandle<S> {
    pub(super) server_address: ServerAddress,
    pub(super) transport: Arc<dyn RemoteTransport>,
    pub(super) state: Mutex<RemoteParticipantState<S>>,
}

impl<S> fmt::Debug for RemoteParticipantHandle<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteParticipantHandle")
            .field("server_address", &self.server_address)
            .finish_non_exhaustive()
    }
}

impl<S: ParticipantResumeStore> RemoteParticipantHandle<S> {
    /// Creates and durably checkpoints a fresh unbound participant.
    ///
    /// # Errors
    ///
    /// Returns a typed encode or storage error before the handle is exposed.
    pub fn new(config: &RemoteConfig, store: S) -> Result<Self, RemoteParticipantError> {
        Self::from_aggregate(config, store, ClientParticipantAggregate::new())
    }

    /// Decodes, validates, restores, and durably records crash testimony before exposure.
    ///
    /// # Errors
    ///
    /// Returns typed LPCR decode/restore, encode, or storage errors.
    pub fn restore(
        config: &RemoteConfig,
        store: S,
        canonical_lpcr: &[u8],
    ) -> Result<Self, RemoteParticipantError> {
        let record = ClientResumeRecord::decode_canonical(canonical_lpcr)
            .map_err(RemoteParticipantError::ResumeDecode)?;
        let aggregate = record
            .restore()
            .map_err(RemoteParticipantError::ResumeRestore)?;
        Self::from_aggregate(config, store, aggregate)
    }

    fn from_aggregate(
        config: &RemoteConfig,
        mut store: S,
        aggregate: ClientParticipantAggregate,
    ) -> Result<Self, RemoteParticipantError> {
        persist(&mut store, &aggregate)?;
        Ok(Self {
            server_address: config.server_address.clone(),
            transport: Arc::clone(&config.transport),
            state: Mutex::new(RemoteParticipantState {
                aggregate: Some(aggregate),
                correlation: None,
                reconnect_attempt: None,
                store,
            }),
        })
    }

    /// Runs `record_operation -> commit -> LPCR persist -> into_parts` exactly.
    ///
    /// # Errors
    ///
    /// Returns typed resume encoding or storage failures. A failed post-commit
    /// persistence leaves the handle unavailable and releases no authority.
    pub fn record_operation(
        &self,
        request: ClientRequest,
    ) -> Result<RemoteOperationRecordOutcome, RemoteParticipantError> {
        let mut state = self.state.lock();
        let aggregate = take_aggregate(&mut state)?;
        match liminal_protocol::client::record_operation(aggregate, request) {
            ClientOperationRecordDecision::Pending(pending) => {
                let commit = pending.commit();
                let record = commit
                    .resume_record()
                    .map_err(RemoteParticipantError::ResumeEncode)?;
                state
                    .store
                    .persist(&record.encode_canonical())
                    .map_err(RemoteParticipantError::Storage)?;
                let (aggregate, operation) = commit.into_parts();
                state.aggregate = Some(aggregate);
                Ok(RemoteOperationRecordOutcome::Recorded(
                    RemoteParticipantOperation {
                        operation,
                        durability: OperationDurability::WriteAhead,
                    },
                ))
            }
            ClientOperationRecordDecision::Continuous(continuous) => {
                let (aggregate, operation) = continuous.into_parts();
                state.aggregate = Some(aggregate);
                Ok(RemoteOperationRecordOutcome::Continuous(
                    RemoteParticipantOperation {
                        operation,
                        durability: OperationDurability::Continuous,
                    },
                ))
            }
            ClientOperationRecordDecision::Refused(refusal) => {
                let reason = refusal.reason();
                let (aggregate, request) = refusal.into_parts();
                state.aggregate = Some(aggregate);
                Ok(RemoteOperationRecordOutcome::Refused { request, reason })
            }
        }
    }

    /// Persists issued state, writes the exact operation, and retains correlation.
    ///
    /// # Errors
    ///
    /// Returns typed state, LPCR, or storage failures. Transport failures are
    /// returned as a typed outcome after crate fate delegation.
    pub fn send_operation(
        &self,
        operation: RemoteParticipantOperation,
    ) -> Result<RemoteParticipantSendOutcome, RemoteParticipantError> {
        let mut state = self.state.lock();
        let aggregate = take_aggregate(&mut state)?;
        if operation.durability == OperationDurability::WriteAhead {
            persist(&mut state.store, &aggregate)?;
        }
        let (request, correlation) = operation.operation.into_request();
        match self
            .transport
            .send_participant(&self.server_address, &request)
        {
            Ok(provenance) => {
                if operation.durability == OperationDurability::WriteAhead {
                    state.correlation = Some(correlation);
                }
                state.aggregate = Some(aggregate);
                Ok(RemoteParticipantSendOutcome::Sent { provenance })
            }
            Err(error) => {
                let operation_fate = if operation.durability == OperationDurability::WriteAhead {
                    record_operation_transport_fate(&mut state, aggregate, correlation)
                } else {
                    state.aggregate = Some(aggregate);
                    RemoteOperationTransportFate::NotOutstanding
                };
                let reconnect = record_connection_fate(&mut state)?;
                Ok(RemoteParticipantSendOutcome::TransportLost {
                    error,
                    operation_fate,
                    reconnect,
                })
            }
        }
    }

    /// Receives one real participant frame and delegates every `ServerValue` to the crate.
    ///
    /// # Errors
    ///
    /// Returns transport, direction, LPCR encoding, or storage failures.
    pub fn receive(&self) -> Result<RemoteParticipantInbound, RemoteParticipantError> {
        let ParticipantTransportFrame { frame, provenance } = self
            .transport
            .receive_participant(&self.server_address)
            .map_err(RemoteParticipantError::Transport)?;
        match frame {
            ParticipantFrame::ServerPush(value) => {
                Ok(RemoteParticipantInbound::Push { value, provenance })
            }
            ParticipantFrame::ClientRequest(_) => {
                Err(RemoteParticipantError::InvalidInboundDirection)
            }
            ParticipantFrame::ServerValue(value) => self.apply_inbound(value, provenance),
        }
    }

    fn apply_inbound(
        &self,
        value: ServerValue,
        provenance: ParticipantResponseProvenance,
    ) -> Result<RemoteParticipantInbound, RemoteParticipantError> {
        let mut state = self.state.lock();
        let aggregate = take_aggregate(&mut state)?;
        if let Some(correlation) = state.correlation.take() {
            match decide_correlated_inbound(aggregate, value, correlation) {
                ClientCorrelatedInboundDecision::Applied(applied) => {
                    let (aggregate, value) = applied.into_parts();
                    persist(&mut state.store, &aggregate)?;
                    state.aggregate = Some(aggregate);
                    Ok(RemoteParticipantInbound::Applied { value, provenance })
                }
                ClientCorrelatedInboundDecision::Refused(refusal) => {
                    let reason = refusal.reason();
                    let (aggregate, value, correlation) = refusal.into_parts();
                    state.aggregate = Some(aggregate);
                    state.correlation = Some(correlation);
                    Ok(RemoteParticipantInbound::Refused {
                        value,
                        reason,
                        provenance,
                    })
                }
            }
        } else {
            match decide_inbound(aggregate, value) {
                ClientInboundDecision::Applied(applied) => {
                    let (aggregate, value) = applied.into_parts();
                    persist(&mut state.store, &aggregate)?;
                    state.aggregate = Some(aggregate);
                    Ok(RemoteParticipantInbound::Applied { value, provenance })
                }
                ClientInboundDecision::Refused(refusal) => {
                    let reason = refusal.reason();
                    let (aggregate, value) = refusal.into_parts();
                    state.aggregate = Some(aggregate);
                    Ok(RemoteParticipantInbound::Refused {
                        value,
                        reason,
                        provenance,
                    })
                }
            }
        }
    }
}

pub(super) fn take_aggregate<S>(
    state: &mut RemoteParticipantState<S>,
) -> Result<ClientParticipantAggregate, RemoteParticipantError> {
    state
        .aggregate
        .take()
        .ok_or(RemoteParticipantError::StateUnavailable)
}

pub(super) fn persist<S: ParticipantResumeStore>(
    store: &mut S,
    aggregate: &ClientParticipantAggregate,
) -> Result<(), RemoteParticipantError> {
    let record = aggregate
        .resume_record()
        .map_err(RemoteParticipantError::ResumeEncode)?;
    store
        .persist(&record.encode_canonical())
        .map_err(RemoteParticipantError::Storage)
}

fn record_operation_transport_fate<S: ParticipantResumeStore>(
    state: &mut RemoteParticipantState<S>,
    aggregate: ClientParticipantAggregate,
    correlation: ClientResponseCorrelation,
) -> RemoteOperationTransportFate {
    match record_expected_operation_fate(
        aggregate,
        correlation,
        ExpectedOperationTransportFate::ResponseUnavailable,
    ) {
        liminal_protocol::client::ExpectedOperationFateDecision::Recorded {
            aggregate,
            request,
            ..
        } => {
            state.aggregate = Some(aggregate);
            RemoteOperationTransportFate::Recorded { request }
        }
        liminal_protocol::client::ExpectedOperationFateDecision::Refused {
            aggregate,
            correlation,
            reason: ExpectedOperationFateRefusalReason::DetachUsesReplayFate,
            ..
        } => match liminal_protocol::client::transport_fate(
            aggregate,
            correlation,
            liminal_protocol::client::DetachTransportFate::ResponseUnavailable,
        ) {
            liminal_protocol::client::DetachTransportFateDecision::Parked(applied) => {
                state.aggregate = Some(applied.into_aggregate());
                RemoteOperationTransportFate::DetachParked
            }
            liminal_protocol::client::DetachTransportFateDecision::Refused(refusal) => {
                let (aggregate, (correlation, _)) = refusal.into_parts();
                state.aggregate = Some(aggregate);
                state.correlation = Some(correlation);
                RemoteOperationTransportFate::Refused {
                    reason: ExpectedOperationFateRefusalReason::DetachUsesReplayFate,
                }
            }
        },
        liminal_protocol::client::ExpectedOperationFateDecision::Refused {
            aggregate,
            correlation,
            reason,
            ..
        } => {
            state.aggregate = Some(aggregate);
            state.correlation = Some(correlation);
            RemoteOperationTransportFate::Refused { reason }
        }
    }
}

pub(super) fn record_connection_fate<S: ParticipantResumeStore>(
    state: &mut RemoteParticipantState<S>,
) -> Result<RemoteReconnectPermitOutcome, RemoteParticipantError> {
    let aggregate = take_aggregate(state)?;
    let (aggregate, outcome) = match record_transport_fate(
        aggregate,
        liminal_protocol::client::EstablishedConnectionTransportFate::Lost,
    ) {
        ReconnectPermitDecision::Permitted {
            aggregate,
            permit,
            result,
        } => (
            aggregate,
            RemoteReconnectPermitOutcome::Permitted {
                permit: RemoteReconnectPermit { permit },
                result,
            },
        ),
        ReconnectPermitDecision::Refused(refusal) => {
            let reason = refusal.reason();
            let result = refusal.result();
            let (aggregate, _) = refusal.into_parts();
            (
                aggregate,
                RemoteReconnectPermitOutcome::Refused { reason, result },
            )
        }
    };
    persist(&mut state.store, &aggregate)?;
    state.aggregate = Some(aggregate);
    Ok(outcome)
}

#[cfg(test)]
mod tests;
