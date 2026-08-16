use liminal_protocol::client::{
    DetachReplayRefusalReason, DetachTransportAttemptDecision, DetachTransportFate,
    DetachTransportFateDecision, ExplicitReconnectAction, LostAuthorityKind,
    LostOperationAuthorityDecision, LostReconnectAuthorityDecision, ProvedOnlineTransition,
    ReconnectAttemptDecision, ReconnectAttemptFate, ReconnectAttemptFateDecision,
    ReconnectAttemptFateRefusalReason, ReconnectAttemptRefusalReason, ReconnectPermitDecision,
    RecoveredExpectedOperationDecision, RecoveredReconnectPermitDecision, record_attempt_fate,
    record_explicit_reconnect, record_online_transition, recover_expected_operation,
    recover_reconnect_permit, redeem_attempt, resolve_lost_operation_authority,
    resolve_lost_reconnect_authority, transport_attempt_started, transport_fate,
};
use liminal_protocol::outcome::ReconnectState;
use liminal_protocol::wire::{ClientRequest, DetachRequest, Generation, ServerValue};

use super::{
    OperationDurability, ParticipantResumeStore, RemoteOperationRecordOutcome,
    RemoteOperationTransportFate, RemoteParticipantError, RemoteParticipantHandle,
    RemoteParticipantInbound, RemoteParticipantOperation, RemoteParticipantSendOutcome,
    RemoteReconnectPermit, RemoteReconnectPermitOutcome, persist_retaining,
    record_connection_fate, record_operation_transport_fate, take_aggregate,
};

/// Result of releasing a committed cold-restored operation.
#[derive(Debug)]
pub enum RemoteExpectedOperationRecovery {
    /// One unissued operation authority was recovered.
    Recovered(RemoteParticipantOperation),
    /// No recoverable operation exists.
    NotAvailable {
        /// Whether the retained operation had already been issued.
        already_issued: bool,
    },
}

/// Result of consuming operation-domain crash testimony.
#[derive(Debug, PartialEq, Eq)]
pub enum RemoteLostOperationResolution {
    /// A non-detach operation was terminalized by serialized testimony.
    Recorded {
        /// Exact request whose authority was destroyed.
        request: ClientRequest,
        /// Closed testimony kind consumed by the crate.
        testimony: LostAuthorityKind,
    },
    /// A detach was returned to parked replay by serialized testimony.
    DetachParked {
        /// Exact detach request retained for replay.
        request: ClientRequest,
        /// Closed testimony kind consumed by the crate.
        testimony: LostAuthorityKind,
    },
    /// No operation-domain testimony was pending.
    Refused {
        /// Closed crate refusal reason.
        reason: liminal_protocol::client::LostAuthorityResolutionRefusalReason,
    },
}

/// Result of consuming reconnect-domain crash testimony.
#[derive(Debug, PartialEq, Eq)]
pub enum RemoteLostReconnectResolution {
    /// Testimony parked reconnect state without minting replacement authority.
    Recorded {
        /// Closed testimony kind consumed by the crate.
        testimony: LostAuthorityKind,
    },
    /// No reconnect-domain testimony was pending.
    Refused {
        /// Closed crate refusal reason.
        reason: liminal_protocol::client::LostAuthorityResolutionRefusalReason,
    },
}

/// Result of releasing a committed cold-restored reconnect permit.
#[derive(Debug)]
pub enum RemoteReconnectPermitRecovery {
    /// One unissued permit was recovered.
    Recovered(RemoteReconnectPermit),
    /// No recoverable permit exists.
    NotAvailable {
        /// Current crate reconnect state.
        state: ReconnectState,
    },
}

/// Result of a real connection attempt redeemed from one crate permit.
#[derive(Debug)]
pub enum RemoteReconnectAttemptOutcome {
    /// The real attempt connected and the crate recorded online state.
    Connected {
        /// Provenance assigned to the new established socket.
        provenance: super::ParticipantResponseProvenance,
    },
    /// The real attempt failed and the crate parked without timer authority.
    Failed {
        /// Concrete socket failure.
        error: crate::SdkError,
    },
    /// The crate refused permit redemption and returned it unchanged.
    Refused {
        /// Reusable unchanged permit.
        permit: RemoteReconnectPermit,
        /// Closed crate refusal reason.
        reason: ReconnectAttemptRefusalReason,
    },
    /// The transport ran, but the crate retained the in-progress fate authority.
    FateRefused {
        /// Closed crate refusal reason.
        reason: ReconnectAttemptFateRefusalReason,
        /// Socket failure when the attempted fate was `Failed`.
        error: Option<crate::SdkError>,
        /// New socket provenance when the attempted fate was `Connected`.
        provenance: Option<super::ParticipantResponseProvenance>,
    },
}

/// Result of starting and sending a parked detach replay.
#[derive(Debug)]
pub enum RemoteDetachReplayOutcome {
    /// A real transport send was attempted.
    Send(RemoteParticipantSendOutcome),
    /// The crate refused replay start without changing state.
    Refused {
        /// Closed crate replay refusal reason.
        reason: DetachReplayRefusalReason,
    },
}

/// Result of an explicit replay apply seam delegated to the crate.
#[derive(Debug, PartialEq, Eq)]
pub enum RemoteReplayApplyOutcome<T> {
    /// The crate applied the transition.
    Applied,
    /// The crate retained state, correlation, and exact input.
    Refused {
        /// Exact refused input.
        input: T,
        /// Closed crate refusal reason.
        reason: DetachReplayRefusalReason,
    },
}

/// Why a lost credential attach could not be driven from retained testimony.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LostCredentialAttachRefusalReason {
    /// No operation-domain lost-authority testimony is pending at all.
    NoPendingTestimony,
    /// Testimony is pending, but the operation it testifies is not an issued
    /// credential attach.
    ///
    /// The driver leaves it strictly alone rather than resolving it: a detach
    /// keeps its own replay machinery and a tokenless operation keeps its typed
    /// abandonment, and both of those paths need the take-once testimony this
    /// driver would otherwise have spent to discover it did not own the case.
    NotAnIssuedCredentialAttach,
}

/// Why a driven credential attach ended in an honest re-issue terminal.
///
/// This is an SDK-side classification of two wire answers, not a new wire
/// value: both arms are read off responses the server already sends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CredentialAttachReissueReason {
    /// The receipt window closed while provenance still explains the commit, so
    /// the server can still state WHICH generation the lost commit produced.
    ReceiptExpired(liminal_protocol::wire::ReceiptExpiryReason),
    /// Provenance expired too. The server deliberately claims no commit proof,
    /// so exact-old and unknown tokens are indistinguishable from here.
    StaleOrUnknownReceipt,
}

/// Outcome of driving one lost issued credential attach to a server answer.
///
/// The three healing-or-terminal arms are the exhaustive server answers to a
/// same-token re-presentation, and the remainder are pass-throughs that hand
/// back exactly what the crate or the transport reported.
#[derive(Debug)]
pub enum RemoteCredentialAttachRecovery {
    /// The server replayed its committed receipt and the crate applied it: the
    /// ROTATED credential is now held, and the orphan is over.
    ///
    /// This is the designed healing window being spent. The value is the exact
    /// replay the server sent — `Bound` when the receipt still names its origin
    /// binding, `UnboundReceipt` when the tear killed the connection that held
    /// it. Both carry the successor generation and the newly minted secret; the
    /// difference is only whether the crate lands in `Bound` or `Detached`.
    HealedFromReceipt {
        /// Exact applied replay value.
        value: ServerValue,
        /// Connection/attempt that delivered it.
        provenance: super::ParticipantResponseProvenance,
    },
    /// The attach had never committed, so the re-presentation committed it now.
    ///
    /// The kill landed in the window between the client's send and the server's
    /// commit. Nothing was lost and nothing needed replaying.
    CommittedFresh {
        /// Exact applied `AttachBound` value.
        value: ServerValue,
        /// Connection/attempt that delivered it.
        provenance: super::ParticipantResponseProvenance,
    },
    /// The committed outcome is permanently unanswerable; operator re-issue is
    /// the cure.
    ///
    /// THE LOAD-BEARING TERMINAL. It is reached when the client was dead longer
    /// than the receipt window the server could hold open, which is policy
    /// (config-owned since #39) rather than failure. It is deliberately a state
    /// of its own rather than a generic refusal, because it is the exact point
    /// at which an embedder should dispose and re-enroll instead of retrying —
    /// and no amount of retrying will ever change it.
    ReissueRequired {
        /// The generation the lost commit produced, when the server can still
        /// prove it. `None` for `StaleOrUnknownReceipt`, which makes no commit
        /// claim at all — the absence is the server's honesty, not a gap here.
        result_generation: Option<Generation>,
        /// The generation the identity is live at now.
        current_generation: Generation,
        /// Which of the two unanswerable classes this is.
        reason: CredentialAttachReissueReason,
        /// Exact applied server value.
        value: ServerValue,
        /// Connection/attempt that delivered it.
        provenance: super::ParticipantResponseProvenance,
    },
    /// The crate applied some other correlated answer, carried verbatim.
    ///
    /// `StaleAuthority`, `ParticipantUnknown`, `Retired` and their kin arrive
    /// here. The driver relabels nothing: an answer it does not classify is
    /// handed over as the server sent it.
    Answered {
        /// Exact applied server value.
        value: ServerValue,
        /// Connection/attempt that delivered it.
        provenance: super::ParticipantResponseProvenance,
    },
    /// The crate refused the answer and retained its correlation unchanged.
    AnswerRefused {
        /// Exact refused server value.
        value: ServerValue,
        /// Closed crate refusal reason.
        reason: liminal_protocol::client::ClientInboundRefusalReason,
        /// Connection/attempt that delivered it.
        provenance: super::ParticipantResponseProvenance,
    },
    /// A push arrived where the correlated answer was owed.
    ///
    /// The delivery is handed back rather than dropped, and the live response
    /// correlation is still held, so a caller may simply keep receiving: the
    /// crate applies the answer whenever it does arrive.
    PushedBeforeAnswer {
        /// Exact pushed value.
        value: liminal_protocol::wire::ServerPush,
        /// Connection/attempt that delivered it.
        provenance: super::ParticipantResponseProvenance,
    },
    /// No issued credential-attach testimony was pending; nothing was consumed.
    NotPending {
        /// Closed refusal reason.
        reason: LostCredentialAttachRefusalReason,
    },
    /// The crate refused to re-record the retained envelope.
    RerecordRefused {
        /// Exact refused request.
        request: ClientRequest,
        /// Closed crate refusal reason.
        reason: liminal_protocol::client::ClientOperationRecordRefusalReason,
    },
    /// The probe could not be written; both fates were delegated to the crate.
    TransportLost {
        /// Concrete socket failure.
        error: crate::SdkError,
        /// Crate-owned operation-fate result.
        operation_fate: RemoteOperationTransportFate,
        /// Crate-owned reconnect permit result.
        reconnect: RemoteReconnectPermitOutcome,
    },
}

/// Combined typed consequence of an established connection loss.
#[derive(Debug)]
pub struct RemoteTransportLossOutcome {
    /// Operation-domain fate selected by crate rules.
    pub operation_fate: RemoteOperationTransportFate,
    /// Event-driven reconnect decision selected by crate rules.
    pub reconnect: RemoteReconnectPermitOutcome,
}

impl<S: ParticipantResumeStore> RemoteParticipantHandle<S> {
    /// Releases one unissued operation from committed cold-restored state.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteParticipantError::StateUnavailable`] after a prior fatal
    /// durability failure.
    pub fn recover_expected_operation(
        &self,
    ) -> Result<RemoteExpectedOperationRecovery, RemoteParticipantError> {
        let mut state = self.state.lock();
        let aggregate = take_aggregate(&mut state)?;
        match recover_expected_operation(aggregate) {
            RecoveredExpectedOperationDecision::Recovered {
                aggregate,
                operation,
            } => {
                state.aggregate = Some(aggregate);
                Ok(RemoteExpectedOperationRecovery::Recovered(
                    RemoteParticipantOperation {
                        operation,
                        durability: OperationDurability::WriteAhead,
                    },
                ))
            }
            RecoveredExpectedOperationDecision::NotAvailable {
                aggregate,
                already_issued,
            } => {
                state.aggregate = Some(aggregate);
                Ok(RemoteExpectedOperationRecovery::NotAvailable { already_issued })
            }
        }
    }

    /// Consumes operation-domain lost-authority testimony exactly once.
    ///
    /// # Errors
    ///
    /// Returns LPCR encode or storage failures while checkpointing the decision.
    pub fn resolve_lost_operation_authority(
        &self,
    ) -> Result<RemoteLostOperationResolution, RemoteParticipantError> {
        let mut state = self.state.lock();
        let aggregate = take_aggregate(&mut state)?;
        let outcome = match resolve_lost_operation_authority(aggregate) {
            LostOperationAuthorityDecision::Recorded {
                aggregate,
                request,
                testimony,
            } => {
                state.aggregate = Some(aggregate);
                RemoteLostOperationResolution::Recorded {
                    request,
                    testimony: testimony.kind(),
                }
            }
            LostOperationAuthorityDecision::DetachParked {
                aggregate,
                request,
                testimony,
            } => {
                state.aggregate = Some(aggregate);
                RemoteLostOperationResolution::DetachParked {
                    request,
                    testimony: testimony.kind(),
                }
            }
            LostOperationAuthorityDecision::Refused { aggregate, reason } => {
                state.aggregate = Some(aggregate);
                RemoteLostOperationResolution::Refused { reason }
            }
        };
        checkpoint_state(&mut state)?;
        Ok(outcome)
    }

    /// Drives one lost issued credential attach back to a server answer.
    ///
    /// # The act this performs, and why it is lawful
    ///
    /// A client killed between issuing a `CredentialAttach` and consuming its
    /// answer has lost the ONLY carrier of the rotated credential, because the
    /// commit mints a fresh secret and advances the generation and the
    /// `AttachBound` response is the sole place either value appears. The
    /// server, however, holds the committed outcome inside a receipt window and
    /// will replay it — including the rotation — to a re-presentation of the
    /// SAME attempt token, verified against the receipt's own committed
    /// presented secret, which is the invalidated OLD one. That is deliberate.
    ///
    /// So the healing act is to re-present the EXACT retained envelope: same
    /// attach attempt token, same generation, same old secret. Nothing is
    /// forged and nothing new is minted — this method re-records the envelope
    /// the restore handed back, unchanged, and the crate admits it because the
    /// retained binding still matches it. Token dedup makes it at-most-once, so
    /// a re-presentation of a never-committed attach commits exactly once.
    ///
    /// # FIRST-ACT, by construction
    ///
    /// The probe is sent on THIS call, with no backoff, no timer, and no retry
    /// loop in front of it. That is a hard requirement rather than a
    /// performance preference: the receipt window is the healing window, it is
    /// fixed at commit and never re-opens, and any delay this driver introduced
    /// would be spent out of the window it exists to spend. A retry discipline
    /// must never EXTEND the orphan (§0.16 A5 condition 2).
    ///
    /// # What it will not touch
    ///
    /// Testimony belonging to any other operation class is left entirely alone,
    /// including its take-once atom — see
    /// [`LostCredentialAttachRefusalReason::NotAnIssuedCredentialAttach`].
    ///
    /// # Errors
    ///
    /// Returns typed LPCR encode, storage, or state failures. Every socket and
    /// server outcome is a typed arm of [`RemoteCredentialAttachRecovery`]
    /// rather than an error.
    pub fn recover_lost_credential_attach(
        &self,
    ) -> Result<RemoteCredentialAttachRecovery, RemoteParticipantError> {
        // 1. Look before consuming. The take-once atom must survive a driver
        //    that turns out not to own this case.
        match self.lost_credential_attach_pending()? {
            PendingTestimonyVerdict::IssuedCredentialAttach => {}
            PendingTestimonyVerdict::Nothing => {
                return Ok(RemoteCredentialAttachRecovery::NotPending {
                    reason: LostCredentialAttachRefusalReason::NoPendingTestimony,
                });
            }
            PendingTestimonyVerdict::OtherOperation => {
                return Ok(RemoteCredentialAttachRecovery::NotPending {
                    reason: LostCredentialAttachRefusalReason::NotAnIssuedCredentialAttach,
                });
            }
        }

        // 2. Consume the testimony. The peek above proved this is an issued
        //    credential attach, so `Recorded` is the only reachable arm; the
        //    others fall through to a typed refusal rather than a panic.
        let request = match self.resolve_lost_operation_authority()? {
            RemoteLostOperationResolution::Recorded { request, .. } => request,
            RemoteLostOperationResolution::DetachParked { .. }
            | RemoteLostOperationResolution::Refused { .. } => {
                return Ok(RemoteCredentialAttachRecovery::NotPending {
                    reason: LostCredentialAttachRefusalReason::NotAnIssuedCredentialAttach,
                });
            }
        };

        // 3. Re-record the EXACT retained envelope. Nothing is minted and
        //    nothing is advanced: same attempt token, same generation, same old
        //    secret. The crate admits it because the retained binding still
        //    matches it, and server-side token dedup keeps it at-most-once.
        let operation = match self.record_operation(request)? {
            RemoteOperationRecordOutcome::Recorded(operation)
            | RemoteOperationRecordOutcome::Continuous(operation) => operation,
            RemoteOperationRecordOutcome::Refused { request, reason } => {
                return Ok(RemoteCredentialAttachRecovery::RerecordRefused { request, reason });
            }
        };

        // 4. THE PROBE, on this call. No backoff, no timer, no retry loop: the
        //    receipt window is what this is spending, and a delay here would be
        //    spent out of it.
        match self.send_operation(operation)? {
            RemoteParticipantSendOutcome::Sent { .. } => {}
            RemoteParticipantSendOutcome::TransportLost {
                error,
                operation_fate,
                reconnect,
            } => {
                return Ok(RemoteCredentialAttachRecovery::TransportLost {
                    error,
                    operation_fate,
                    reconnect,
                });
            }
        }

        // 5. Apply the answer through the crate's ordinary inbound path, then
        //    classify what it applied. The application is the crate's; the
        //    classification below reads the applied value and invents nothing.
        Ok(classify_recovery_answer(self.receive()?))
    }

    /// Borrows a copy of the retained credential-attach envelope this handle
    /// would re-present, without consuming anything.
    ///
    /// `Some` means [`Self::recover_lost_credential_attach`] has work to do and
    /// names exactly the envelope it will send. It is the honest way for an
    /// embedder to ask "am I an orphan, and what is owed" before deciding to
    /// drive, and for a test to prove the driver re-presents the retained bytes
    /// rather than something it minted.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteParticipantError::StateUnavailable`] after a prior fatal
    /// durability failure.
    pub fn peek_lost_credential_attach(
        &self,
    ) -> Result<Option<liminal_protocol::wire::CredentialAttachRequest>, RemoteParticipantError>
    {
        let mut state = self.state.lock();
        let aggregate = take_aggregate(&mut state)?;
        let retained = aggregate.lost_credential_attach().cloned();
        state.aggregate = Some(aggregate);
        Ok(retained)
    }

    /// Classifies the pending operation-domain testimony WITHOUT consuming it.
    fn lost_credential_attach_pending(
        &self,
    ) -> Result<PendingTestimonyVerdict, RemoteParticipantError> {
        let mut state = self.state.lock();
        let aggregate = take_aggregate(&mut state)?;
        let verdict = if aggregate.lost_credential_attach().is_some() {
            PendingTestimonyVerdict::IssuedCredentialAttach
        } else if aggregate.lost_operation_testimony().is_some() {
            PendingTestimonyVerdict::OtherOperation
        } else {
            PendingTestimonyVerdict::Nothing
        };
        state.aggregate = Some(aggregate);
        Ok(verdict)
    }

    /// Takes a durable tokenless abandonment so its exact request can be re-recorded.
    ///
    /// # Errors
    ///
    /// Returns LPCR encode or storage failures while durably recording the take.
    pub fn take_restored_operation_abandonment(
        &self,
    ) -> Result<
        Option<liminal_protocol::client::RestoredExpectedOperationAbandonment>,
        RemoteParticipantError,
    > {
        let mut state = self.state.lock();
        let mut aggregate = take_aggregate(&mut state)?;
        let abandonment = aggregate.take_restored_operation_abandonment();
        if abandonment.is_some() {
            aggregate = persist_retaining(&mut state, aggregate)?;
        }
        state.aggregate = Some(aggregate);
        Ok(abandonment)
    }

    /// Records established-connection fate and returns at most one reconnect permit.
    ///
    /// # Errors
    ///
    /// Returns LPCR encode or storage failures while checkpointing the event.
    pub fn record_transport_fate(
        &self,
    ) -> Result<RemoteReconnectPermitOutcome, RemoteParticipantError> {
        let mut state = self.state.lock();
        record_connection_fate(&mut state)
    }

    /// Records a proved online transition as a crate fresh event.
    ///
    /// # Errors
    ///
    /// Returns LPCR encode or storage failures while checkpointing issued authority.
    pub fn record_online_transition(
        &self,
    ) -> Result<RemoteReconnectPermitOutcome, RemoteParticipantError> {
        self.record_fresh_reconnect(|aggregate| {
            record_online_transition(aggregate, ProvedOnlineTransition::ProvedOnline)
        })
    }

    /// Records explicit caller action as a crate fresh event, with no timer arm.
    ///
    /// # Errors
    ///
    /// Returns LPCR encode or storage failures while checkpointing issued authority.
    pub fn record_explicit_reconnect(
        &self,
    ) -> Result<RemoteReconnectPermitOutcome, RemoteParticipantError> {
        self.record_fresh_reconnect(|aggregate| {
            record_explicit_reconnect(aggregate, ExplicitReconnectAction::ReconnectNow)
        })
    }

    fn record_fresh_reconnect(
        &self,
        decide: impl FnOnce(
            liminal_protocol::client::ClientParticipantAggregate,
        ) -> ReconnectPermitDecision,
    ) -> Result<RemoteReconnectPermitOutcome, RemoteParticipantError> {
        let mut state = self.state.lock();
        let aggregate = take_aggregate(&mut state)?;
        let outcome = match decide(aggregate) {
            ReconnectPermitDecision::Permitted {
                aggregate,
                permit,
                result,
            } => {
                state.aggregate = Some(aggregate);
                RemoteReconnectPermitOutcome::Permitted {
                    permit: RemoteReconnectPermit { permit },
                    result,
                }
            }
            ReconnectPermitDecision::Refused(refusal) => {
                let reason = refusal.reason();
                let result = refusal.result();
                let (aggregate, _) = refusal.into_parts();
                state.aggregate = Some(aggregate);
                RemoteReconnectPermitOutcome::Refused { reason, result }
            }
        };
        checkpoint_state(&mut state)?;
        Ok(outcome)
    }

    /// Releases one unissued reconnect permit from committed cold-restored state.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteParticipantError::StateUnavailable`] after a prior fatal failure.
    pub fn recover_reconnect_permit(
        &self,
    ) -> Result<RemoteReconnectPermitRecovery, RemoteParticipantError> {
        let mut state = self.state.lock();
        let aggregate = take_aggregate(&mut state)?;
        match recover_reconnect_permit(aggregate) {
            RecoveredReconnectPermitDecision::Recovered { aggregate, permit } => {
                state.aggregate = Some(aggregate);
                Ok(RemoteReconnectPermitRecovery::Recovered(
                    RemoteReconnectPermit { permit },
                ))
            }
            RecoveredReconnectPermitDecision::NotAvailable {
                aggregate,
                state: value,
            } => {
                state.aggregate = Some(aggregate);
                Ok(RemoteReconnectPermitRecovery::NotAvailable { state: value })
            }
        }
    }

    /// Consumes reconnect-domain lost-authority testimony exactly once.
    ///
    /// # Errors
    ///
    /// Returns LPCR encode or storage failures while checkpointing the resolution.
    pub fn resolve_lost_reconnect_authority(
        &self,
    ) -> Result<RemoteLostReconnectResolution, RemoteParticipantError> {
        let mut state = self.state.lock();
        let aggregate = take_aggregate(&mut state)?;
        let outcome = match resolve_lost_reconnect_authority(aggregate) {
            LostReconnectAuthorityDecision::Recorded {
                aggregate,
                testimony,
            } => {
                state.aggregate = Some(aggregate);
                RemoteLostReconnectResolution::Recorded {
                    testimony: testimony.kind(),
                }
            }
            LostReconnectAuthorityDecision::Refused { aggregate, reason } => {
                state.aggregate = Some(aggregate);
                RemoteLostReconnectResolution::Refused { reason }
            }
        };
        checkpoint_state(&mut state)?;
        Ok(outcome)
    }

    /// Redeems one permit before opening one real transport connection.
    ///
    /// # Errors
    ///
    /// Returns LPCR encode or storage failures before or after the real attempt.
    pub fn reconnect(
        &self,
        permit: RemoteReconnectPermit,
    ) -> Result<RemoteReconnectAttemptOutcome, RemoteParticipantError> {
        let mut state = self.state.lock();
        let aggregate = take_aggregate(&mut state)?;
        let (aggregate, attempt) = match redeem_attempt(aggregate, permit.permit) {
            ReconnectAttemptDecision::Started { aggregate, attempt } => (aggregate, attempt),
            ReconnectAttemptDecision::Refused {
                aggregate,
                permit,
                reason,
            } => {
                state.aggregate = Some(aggregate);
                return Ok(RemoteReconnectAttemptOutcome::Refused {
                    permit: RemoteReconnectPermit { permit },
                    reason,
                });
            }
        };
        let aggregate = persist_retaining(&mut state, aggregate)?;
        state.aggregate = Some(aggregate);
        drop(state);

        let transport_result = self.transport.reconnect_participant(&self.server_address);
        let fate = if transport_result.is_ok() {
            ReconnectAttemptFate::Connected
        } else {
            ReconnectAttemptFate::Failed
        };

        let mut state = self.state.lock();
        let aggregate = take_aggregate(&mut state)?;
        match record_attempt_fate(aggregate, attempt, fate) {
            ReconnectAttemptFateDecision::Recorded(aggregate) => {
                let aggregate = persist_retaining(&mut state, aggregate)?;
                state.aggregate = Some(aggregate);
                match transport_result {
                    Ok(provenance) => Ok(RemoteReconnectAttemptOutcome::Connected { provenance }),
                    Err(error) => Ok(RemoteReconnectAttemptOutcome::Failed { error }),
                }
            }
            ReconnectAttemptFateDecision::Refused {
                aggregate,
                attempt,
                reason,
                ..
            } => {
                state.aggregate = Some(aggregate);
                state.reconnect_attempt = Some(attempt);
                let (provenance, error) = match transport_result {
                    Ok(value) => (Some(value), None),
                    Err(value) => (None, Some(value)),
                };
                Ok(RemoteReconnectAttemptOutcome::FateRefused {
                    reason,
                    error,
                    provenance,
                })
            }
        }
    }

    /// Records response and connection fates after an established transport loss.
    ///
    /// # Errors
    ///
    /// Returns LPCR encode or storage failures while checkpointing both decisions.
    pub fn record_established_transport_loss(
        &self,
    ) -> Result<RemoteTransportLossOutcome, RemoteParticipantError> {
        let mut state = self.state.lock();
        let operation_fate = if let Some(correlation) = state.correlation.take() {
            let aggregate = take_aggregate(&mut state)?;
            record_operation_transport_fate(&mut state, aggregate, correlation)
        } else {
            RemoteOperationTransportFate::NotOutstanding
        };
        let reconnect = record_connection_fate(&mut state)?;
        Ok(RemoteTransportLossOutcome {
            operation_fate,
            reconnect,
        })
    }

    /// Starts and sends the exact parked detach replay selected by the crate.
    ///
    /// # Errors
    ///
    /// Returns LPCR, storage, or state failures. Socket failure is a typed send outcome.
    pub fn replay_detach(&self) -> Result<RemoteDetachReplayOutcome, RemoteParticipantError> {
        let mut state = self.state.lock();
        let aggregate = take_aggregate(&mut state)?;
        let (aggregate, attempt) = match transport_attempt_started(aggregate) {
            DetachTransportAttemptDecision::Started { aggregate, attempt } => (aggregate, attempt),
            DetachTransportAttemptDecision::Refused(refusal) => {
                let reason = refusal.reason();
                let (aggregate, ()) = refusal.into_parts();
                state.aggregate = Some(aggregate);
                return Ok(RemoteDetachReplayOutcome::Refused { reason });
            }
        };
        let aggregate = persist_retaining(&mut state, aggregate)?;
        let (request, correlation) = attempt.into_request();
        let request = ClientRequest::Detach(DetachRequest {
            conversation_id: request.conversation_id,
            participant_id: request.participant_id,
            capability_generation: request.capability_generation,
            detach_attempt_token: request.detach_attempt_token,
        });
        match self
            .transport
            .send_participant(&self.server_address, &request)
        {
            Ok(provenance) => {
                state.aggregate = Some(aggregate);
                state.correlation = Some(correlation);
                Ok(RemoteDetachReplayOutcome::Send(
                    RemoteParticipantSendOutcome::Sent { provenance },
                ))
            }
            Err(error) => {
                let operation_fate = match transport_fate(
                    aggregate,
                    correlation,
                    DetachTransportFate::ResponseUnavailable,
                ) {
                    DetachTransportFateDecision::Parked(applied) => {
                        state.aggregate = Some(applied.into_aggregate());
                        RemoteOperationTransportFate::DetachParked
                    }
                    DetachTransportFateDecision::Refused(refusal) => {
                        let (aggregate, (correlation, _)) = refusal.into_parts();
                        state.aggregate = Some(aggregate);
                        state.correlation = Some(correlation);
                        RemoteOperationTransportFate::Refused {
                            reason: liminal_protocol::client::ExpectedOperationFateRefusalReason::DetachUsesReplayFate,
                        }
                    }
                };
                let reconnect = record_connection_fate(&mut state)?;
                Ok(RemoteDetachReplayOutcome::Send(
                    RemoteParticipantSendOutcome::TransportLost {
                        error,
                        operation_fate,
                        reconnect,
                    },
                ))
            }
        }
    }
}

/// What the pending operation-domain testimony is, read without consuming it.
///
/// Three cases, kept distinct because the driver must act differently on each:
/// drive it, decline it while leaving the atom for the path that owns it, or
/// report that nothing is owed at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PendingTestimonyVerdict {
    /// No operation-domain testimony is pending.
    Nothing,
    /// An issued credential attach — this driver's case.
    IssuedCredentialAttach,
    /// Testimony for an operation class this driver does not own.
    OtherOperation,
}

/// Classifies the answer to one recovery probe.
///
/// Every arm reads a value the crate has ALREADY applied (or refused). Nothing
/// here re-derives a decision the crate made, and nothing is relabelled: the
/// four classified arms are the four exhaustive server answers to a same-token
/// re-presentation, and everything else is handed back verbatim.
fn classify_recovery_answer(inbound: RemoteParticipantInbound) -> RemoteCredentialAttachRecovery {
    let (value, provenance) = match inbound {
        RemoteParticipantInbound::Applied { value, provenance } => (value, provenance),
        RemoteParticipantInbound::Refused {
            value,
            reason,
            provenance,
        } => {
            return RemoteCredentialAttachRecovery::AnswerRefused {
                value,
                reason,
                provenance,
            };
        }
        RemoteParticipantInbound::Push { value, provenance } => {
            return RemoteCredentialAttachRecovery::PushedBeforeAnswer { value, provenance };
        }
    };
    match &value {
        // The committed outcome, replayed. `Bound` and `UnboundReceipt` differ
        // only in whether the receipt still names a live origin binding; both
        // carry the rotation, and the crate has already adopted it.
        ServerValue::Bound(liminal_protocol::wire::ReceiptReplay::CredentialAttach(_))
        | ServerValue::UnboundReceipt(liminal_protocol::wire::ReceiptReplay::CredentialAttach(
            _,
        )) => RemoteCredentialAttachRecovery::HealedFromReceipt { value, provenance },
        // Never committed before, committed now.
        ServerValue::AttachBound(_) => {
            RemoteCredentialAttachRecovery::CommittedFresh { value, provenance }
        }
        // Past the receipt window, inside provenance: the server can still name
        // the generation the lost commit produced.
        ServerValue::ReceiptExpired(liminal_protocol::wire::ReceiptExpired::CredentialAttach {
            result_generation,
            current_generation,
            reason,
            ..
        }) => {
            let (result_generation, current_generation, reason) = (
                Some(*result_generation),
                *current_generation,
                CredentialAttachReissueReason::ReceiptExpired(*reason),
            );
            RemoteCredentialAttachRecovery::ReissueRequired {
                result_generation,
                current_generation,
                reason,
                value,
                provenance,
            }
        }
        // Past provenance: the server claims no commit proof, so no result
        // generation is reported rather than one being inferred.
        ServerValue::StaleOrUnknownReceipt(stale) => {
            let current_generation = stale.current_generation;
            RemoteCredentialAttachRecovery::ReissueRequired {
                result_generation: None,
                current_generation,
                reason: CredentialAttachReissueReason::StaleOrUnknownReceipt,
                value,
                provenance,
            }
        }
        _ => RemoteCredentialAttachRecovery::Answered { value, provenance },
    }
}

fn checkpoint_state<S: ParticipantResumeStore>(
    state: &mut super::RemoteParticipantState<S>,
) -> Result<(), RemoteParticipantError> {
    let aggregate = take_aggregate(state)?;
    let aggregate = persist_retaining(state, aggregate)?;
    state.aggregate = Some(aggregate);
    Ok(())
}
