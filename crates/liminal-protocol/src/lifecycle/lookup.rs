use crate::wire::{
    AttachBound, AttachEnvelope, AttemptConflict, AttemptTokenBodyConflict, BindingEpoch,
    BindingRequiredEnvelope, BindingStateView, CommonStaleAuthorityEnvelope,
    CredentialAttachRequest, DeliverySeq, DetachCommitted, DetachEnvelope, DetachInProgress,
    DetachRequest, DetachStaleAuthority, EnrollBound, EnrollmentEnvelope, EnrollmentKnown,
    EnrollmentRequest, Generation, LeaveCommitted, LeaveEnvelope, LeaveRequest,
    LeaveStaleAuthority, MarkerAck, MarkerAckEnvelope, NoBinding, ParticipantAck,
    ParticipantAckEnvelope, ParticipantReferenceEnvelope, ParticipantUnknown, ReceiptExpired,
    ReceiptExpiryReason, ReceiptReplay, RecordAdmission, RecordAdmissionEnvelope, Retired,
    StaleAuthority, StaleOrUnknownReceipt,
};

use super::{
    ActiveBinding, BindingState, DetachCell, IdentityState, LiveMember, PendingReplayRequest,
    RetiredIdentity,
};

/// Result of resolving the presented participant key before operation-specific
/// authority checks.
#[derive(Debug)]
pub enum PresentedIdentity<'a, EF, V, LF> {
    /// No live identity or tombstone exists for the presented key.
    Absent,
    /// The presented key resolves to live membership.
    Live(&'a LiveMember<EF>),
    /// The presented key resolves to a permanent tombstone.
    Retired(&'a RetiredIdentity<EF, V, LF>),
}

impl<EF, V, LF> Copy for PresentedIdentity<'_, EF, V, LF> {}

impl<EF, V, LF> Clone for PresentedIdentity<'_, EF, V, LF> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, EF, V, LF> From<Option<&'a IdentityState<EF, V, LF>>>
    for PresentedIdentity<'a, EF, V, LF>
{
    fn from(value: Option<&'a IdentityState<EF, V, LF>>) -> Self {
        match value {
            None => Self::Absent,
            Some(IdentityState::Live(member)) => Self::Live(member),
            Some(IdentityState::Retired(tombstone)) => Self::Retired(tombstone),
        }
    }
}

/// Identity reached through an exact token index before the presented-key
/// identity lookup runs.
///
/// Absence is represented by the operation-specific token phase. A resolved
/// token always names either a live identity or its permanent tombstone.
#[derive(Debug)]
pub enum ResolvedIdentity<'a, EF, V, LF> {
    /// The token index resolves to live membership.
    Live(&'a LiveMember<EF>),
    /// The token index resolves to a permanent tombstone.
    Retired(&'a RetiredIdentity<EF, V, LF>),
}

impl<EF, V, LF> Copy for ResolvedIdentity<'_, EF, V, LF> {}

impl<EF, V, LF> Clone for ResolvedIdentity<'_, EF, V, LF> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, EF, V, LF> From<&'a IdentityState<EF, V, LF>> for ResolvedIdentity<'a, EF, V, LF> {
    fn from(value: &'a IdentityState<EF, V, LF>) -> Self {
        match value {
            IdentityState::Live(member) => Self::Live(member),
            IdentityState::Retired(tombstone) => Self::Retired(tombstone),
        }
    }
}

/// Secret-verifier result supplied by the consuming cryptographic layer for
/// credential attach lookup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachSecretProof {
    /// The constant-time verifier rejected the presented secret.
    Mismatch,
    /// The constant-time verifier accepted the presented secret.
    Verified,
}

/// Stored secret-bearing enrollment receipt while its receipt deadline is
/// live.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnrollmentLiveReceipt {
    committed: EnrollBound,
}

impl EnrollmentLiveReceipt {
    /// Captures the exact committed enrollment payload as a live receipt.
    #[must_use]
    pub const fn from_commit(committed: EnrollBound) -> Self {
        Self { committed }
    }
}

/// Non-secret enrollment provenance retained after the live receipt is
/// deleted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnrollmentProvenance {
    result_generation: Generation,
    reason: ReceiptExpiryReason,
}

impl EnrollmentProvenance {
    /// Creates the exact-reason enrollment provenance row.
    #[must_use]
    pub const fn new(result_generation: Generation, reason: ReceiptExpiryReason) -> Self {
        Self {
            result_generation,
            reason,
        }
    }
}

/// Phase-specific enrollment-token lookup state after the lifetime token index
/// has run.
#[derive(Debug)]
pub enum EnrollmentTokenPhase<'a, EF, V, LF> {
    /// No lifetime mapping exists; enrollment may run its remaining checks.
    Unmapped,
    /// The exact secret-bearing receipt remains live.
    LiveReceipt {
        /// Participant resolved by the enrollment token.
        identity: ResolvedIdentity<'a, EF, V, LF>,
        /// Stored canonical commit payload.
        receipt: &'a EnrollmentLiveReceipt,
    },
    /// Only exact-reason non-secret provenance remains.
    Provenance {
        /// Participant resolved by the enrollment token.
        identity: ResolvedIdentity<'a, EF, V, LF>,
        /// Stored provenance fields.
        provenance: EnrollmentProvenance,
    },
    /// The permanent lifetime mapping remains after receipt provenance.
    LifetimeMapping {
        /// Participant resolved by the enrollment token.
        identity: ResolvedIdentity<'a, EF, V, LF>,
    },
}

impl<EF, V, LF> Copy for EnrollmentTokenPhase<'_, EF, V, LF> {}

impl<EF, V, LF> Clone for EnrollmentTokenPhase<'_, EF, V, LF> {
    fn clone(&self) -> Self {
        *self
    }
}

/// Total enrollment-token lookup result through phase 0c.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnrollmentLookupResult {
    /// A token mapping resolved to a tombstone before any live token result.
    Retired(Retired),
    /// An exact live receipt still names its origin binding.
    Bound(ReceiptReplay),
    /// An exact live receipt no longer names its origin binding.
    UnboundReceipt(ReceiptReplay),
    /// Exact non-secret provenance remains.
    ReceiptExpired(ReceiptExpired),
    /// Only the permanent live enrollment mapping remains.
    EnrollmentKnown(EnrollmentKnown),
    /// No token mapping exists; fresh enrollment may continue.
    AuthorizedNew,
}

/// Applies token-to-identity tombstone precedence and enrollment's three live
/// token phases.
#[must_use]
pub fn lookup_enrollment<EF, V, LF>(
    phase: EnrollmentTokenPhase<'_, EF, V, LF>,
    binding: &BindingState,
    request: &EnrollmentRequest,
) -> EnrollmentLookupResult {
    match phase {
        EnrollmentTokenPhase::Unmapped => EnrollmentLookupResult::AuthorizedNew,
        EnrollmentTokenPhase::LiveReceipt { identity, receipt } => match identity {
            ResolvedIdentity::Retired(tombstone) => {
                EnrollmentLookupResult::Retired(retired_enrollment(tombstone, request))
            }
            ResolvedIdentity::Live(member) => {
                let committed = &receipt.committed;
                let replay = ReceiptReplay::Enrollment(committed.clone());
                if receipt_binding_is_current(
                    binding,
                    request.conversation_id,
                    member.participant_id(),
                    committed.origin_binding_epoch(),
                ) {
                    EnrollmentLookupResult::Bound(replay)
                } else {
                    EnrollmentLookupResult::UnboundReceipt(replay)
                }
            }
        },
        EnrollmentTokenPhase::Provenance {
            identity,
            provenance,
        } => match identity {
            ResolvedIdentity::Retired(tombstone) => {
                EnrollmentLookupResult::Retired(retired_enrollment(tombstone, request))
            }
            ResolvedIdentity::Live(member) => {
                EnrollmentLookupResult::ReceiptExpired(ReceiptExpired::Enrollment {
                    conversation_id: request.conversation_id,
                    token: request.enrollment_token,
                    participant_id: member.participant_id(),
                    result_generation: provenance.result_generation,
                    current_generation: member.generation(),
                    reason: provenance.reason,
                })
            }
        },
        EnrollmentTokenPhase::LifetimeMapping { identity } => match identity {
            ResolvedIdentity::Retired(tombstone) => {
                EnrollmentLookupResult::Retired(retired_enrollment(tombstone, request))
            }
            ResolvedIdentity::Live(member) => {
                EnrollmentLookupResult::EnrollmentKnown(EnrollmentKnown {
                    conversation_id: request.conversation_id,
                    token: request.enrollment_token,
                    participant_id: member.participant_id(),
                    current_generation: member.generation(),
                })
            }
        },
    }
}

/// Stored canonical credential-attach result while its secret-bearing receipt
/// remains live.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CredentialAttachLiveReceipt {
    committed: AttachBound,
}

impl CredentialAttachLiveReceipt {
    /// Captures the exact committed attach payload as a live receipt.
    #[must_use]
    pub const fn from_commit(committed: AttachBound) -> Self {
        Self { committed }
    }
}

/// Non-secret credential-attach provenance retained after deleting the live
/// verifier and secret body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CredentialAttachProvenance {
    result_generation: Generation,
    reason: ReceiptExpiryReason,
}

impl CredentialAttachProvenance {
    /// Creates one exact-reason credential-attach provenance row.
    #[must_use]
    pub const fn new(result_generation: Generation, reason: ReceiptExpiryReason) -> Self {
        Self {
            result_generation,
            reason,
        }
    }
}

/// Phase selected by credential-attach token lookup.
#[derive(Debug)]
pub enum CredentialAttachTokenPhase<'a, EF, V, LF> {
    /// No receipt or provenance key matched.
    NoMatch,
    /// The exact secret-bearing receipt remains live.
    LiveReceipt {
        /// Participant resolved by the exact token key.
        identity: ResolvedIdentity<'a, EF, V, LF>,
        /// Stored canonical result and original non-secret request fields.
        receipt: &'a CredentialAttachLiveReceipt,
    },
    /// The exact token key reached non-secret provenance.
    Provenance {
        /// Participant resolved by the exact token key.
        identity: ResolvedIdentity<'a, EF, V, LF>,
        /// Exact terminal reason and result generation.
        provenance: CredentialAttachProvenance,
    },
    /// Receipt provenance is gone, so exact-old and unknown-old status is
    /// intentionally ambiguous.
    AfterProvenance,
}

impl<EF, V, LF> Copy for CredentialAttachTokenPhase<'_, EF, V, LF> {}

impl<EF, V, LF> Clone for CredentialAttachTokenPhase<'_, EF, V, LF> {
    fn clone(&self) -> Self {
        *self
    }
}

/// Total credential-attach lookup result through authority phase 3.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CredentialAttachLookupResult<'a, EF> {
    /// An exact token or the presented key resolved to a tombstone.
    Retired(Retired),
    /// Neither a live identity nor tombstone exists.
    ParticipantUnknown(ParticipantUnknown),
    /// Secret or current-generation authority is stale.
    StaleAuthority(StaleAuthority),
    /// Verified exact live receipt changed a canonical non-secret field.
    AttemptTokenBodyConflict(AttemptTokenBodyConflict),
    /// Exact live receipt still names its origin binding.
    Bound(ReceiptReplay),
    /// Exact live receipt no longer names its origin binding.
    UnboundReceipt(ReceiptReplay),
    /// Exact non-secret provenance remains.
    ReceiptExpired(ReceiptExpired),
    /// No verifier remains after provenance.
    StaleOrUnknownReceipt(StaleOrUnknownReceipt),
    /// Fresh live authority passed and attach may run remaining checks.
    AuthorizedFresh {
        /// Verified live member; attach itself does not require a binding.
        member: &'a LiveMember<EF>,
    },
}

/// Applies credential-attach token lookup, tombstone precedence, verifier
/// order, and ordinary live-authority checks.
///
/// `secret_proof` is computed against the live receipt verifier when the phase
/// is [`CredentialAttachTokenPhase::LiveReceipt`], and against the current live
/// member for [`CredentialAttachTokenPhase::NoMatch`]. Provenance phases have no
/// verifier and ignore it by construction of their result path.
#[must_use]
pub fn lookup_credential_attach<'a, EF, V, LF>(
    token_phase: CredentialAttachTokenPhase<'a, EF, V, LF>,
    presented_identity: PresentedIdentity<'a, EF, V, LF>,
    binding: &BindingState,
    request: &CredentialAttachRequest,
    secret_proof: AttachSecretProof,
) -> CredentialAttachLookupResult<'a, EF> {
    match token_phase {
        CredentialAttachTokenPhase::LiveReceipt { identity, receipt } => {
            return match identity {
                ResolvedIdentity::Retired(tombstone) => {
                    CredentialAttachLookupResult::Retired(retired_attach(tombstone, request))
                }
                ResolvedIdentity::Live(member) => {
                    lookup_live_attach_receipt(member, binding, request, secret_proof, receipt)
                }
            };
        }
        CredentialAttachTokenPhase::Provenance {
            identity,
            provenance,
        } => {
            return match identity {
                ResolvedIdentity::Retired(tombstone) => {
                    CredentialAttachLookupResult::Retired(retired_attach(tombstone, request))
                }
                ResolvedIdentity::Live(member) => {
                    CredentialAttachLookupResult::ReceiptExpired(ReceiptExpired::CredentialAttach {
                        conversation_id: request.conversation_id,
                        token: request.attach_attempt_token,
                        participant_id: request.participant_id,
                        presented_generation: request.capability_generation,
                        presented_marker_delivery_seq: request.accept_marker_delivery_seq,
                        result_generation: provenance.result_generation,
                        current_generation: member.generation(),
                        reason: provenance.reason,
                    })
                }
            };
        }
        CredentialAttachTokenPhase::NoMatch | CredentialAttachTokenPhase::AfterProvenance => {}
    }

    let member = match presented_identity {
        PresentedIdentity::Retired(tombstone) => {
            return CredentialAttachLookupResult::Retired(retired_attach(tombstone, request));
        }
        PresentedIdentity::Absent => {
            return CredentialAttachLookupResult::ParticipantUnknown(unknown_attach(request));
        }
        PresentedIdentity::Live(member) => {
            if member.conversation_id() != request.conversation_id
                || member.participant_id() != request.participant_id
            {
                return CredentialAttachLookupResult::ParticipantUnknown(unknown_attach(request));
            }
            member
        }
    };

    if matches!(token_phase, CredentialAttachTokenPhase::AfterProvenance) {
        return CredentialAttachLookupResult::StaleOrUnknownReceipt(StaleOrUnknownReceipt {
            conversation_id: request.conversation_id,
            token: request.attach_attempt_token,
            participant_id: request.participant_id,
            presented_generation: request.capability_generation,
            presented_marker_delivery_seq: request.accept_marker_delivery_seq,
            current_generation: member.generation(),
        });
    }

    if request.capability_generation != member.generation()
        || secret_proof == AttachSecretProof::Mismatch
    {
        return CredentialAttachLookupResult::StaleAuthority(stale_attach(
            request,
            member.generation(),
        ));
    }

    CredentialAttachLookupResult::AuthorizedFresh { member }
}

fn lookup_live_attach_receipt<'a, EF>(
    member: &'a LiveMember<EF>,
    binding: &BindingState,
    request: &CredentialAttachRequest,
    secret_proof: AttachSecretProof,
    receipt: &CredentialAttachLiveReceipt,
) -> CredentialAttachLookupResult<'a, EF> {
    if secret_proof == AttachSecretProof::Mismatch {
        return CredentialAttachLookupResult::StaleAuthority(stale_attach(
            request,
            member.generation(),
        ));
    }

    let committed = &receipt.committed;
    let conflict = if request.capability_generation != committed.request_generation() {
        Some(AttemptConflict::Generation)
    } else if request.accept_marker_delivery_seq != committed.accepted_marker_delivery_seq() {
        Some(AttemptConflict::MarkerDeliverySequence)
    } else {
        None
    };
    if let Some(conflict) = conflict {
        return CredentialAttachLookupResult::AttemptTokenBodyConflict(
            AttemptTokenBodyConflict::CredentialAttach {
                token: request.attach_attempt_token,
                conversation_id: request.conversation_id,
                presented_participant_id: request.participant_id,
                presented_generation: request.capability_generation,
                presented_marker_delivery_seq: request.accept_marker_delivery_seq,
                conflict,
            },
        );
    }

    let replay = ReceiptReplay::CredentialAttach(committed.clone());
    if receipt_binding_is_current(
        binding,
        request.conversation_id,
        request.participant_id,
        committed.origin_binding_epoch(),
    ) {
        CredentialAttachLookupResult::Bound(replay)
    } else {
        CredentialAttachLookupResult::UnboundReceipt(replay)
    }
}

/// Binding-required participant request families that share lookup phases
/// 1 through 5.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParticipantBindingRequest {
    /// Continuous cumulative acknowledgement.
    ParticipantAck(ParticipantAck),
    /// Explicit retained-marker acknowledgement.
    MarkerAck(MarkerAck),
    /// Ordinary record admission.
    RecordAdmission(RecordAdmission),
}

/// Total shared lookup result for ack, marker-ack, and ordinary admission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BindingRequiredLookupResult<'a, EF> {
    /// Presented participant is permanently retired.
    Retired(Retired),
    /// Presented participant is unknown.
    ParticipantUnknown(ParticipantUnknown),
    /// Presented generation is stale.
    StaleAuthority(StaleAuthority),
    /// Exact receiving binding epoch is absent or different.
    NoBinding(NoBinding),
    /// Phases 1 through 4 passed and operation-specific checks may run.
    Authorized {
        /// Verified live member.
        member: &'a LiveMember<EF>,
        /// Exact current authoritative binding.
        binding: ActiveBinding,
    },
}

/// Applies the common tombstone, unknown, generation, and exact-binding order
/// for participant ack, marker ack, and ordinary admission.
#[must_use]
pub fn lookup_binding_required<'a, EF, V, LF>(
    identity: PresentedIdentity<'a, EF, V, LF>,
    binding: &BindingState,
    receiving_binding_epoch: Option<BindingEpoch>,
    request: &ParticipantBindingRequest,
) -> BindingRequiredLookupResult<'a, EF> {
    let member = match identity {
        PresentedIdentity::Retired(tombstone) => {
            return BindingRequiredLookupResult::Retired(Retired::Participant {
                request: participant_reference(request),
                retired_generation: tombstone.retired_generation(),
            });
        }
        PresentedIdentity::Absent => {
            return BindingRequiredLookupResult::ParticipantUnknown(ParticipantUnknown {
                request: participant_reference(request),
            });
        }
        PresentedIdentity::Live(member) => {
            if member.conversation_id() != binding_request_conversation(request)
                || member.participant_id() != binding_request_participant(request)
            {
                return BindingRequiredLookupResult::ParticipantUnknown(ParticipantUnknown {
                    request: participant_reference(request),
                });
            }
            member
        }
    };

    if binding_request_generation(request) != member.generation() {
        return BindingRequiredLookupResult::StaleAuthority(StaleAuthority::Live {
            request: common_stale_envelope(request),
            current_generation: member.generation(),
        });
    }

    match binding {
        BindingState::Bound(active)
            if active.conversation_id == member.conversation_id()
                && active.participant_id == member.participant_id()
                && active.binding_epoch.capability_generation == member.generation()
                && receiving_binding_epoch == Some(active.binding_epoch) =>
        {
            BindingRequiredLookupResult::Authorized {
                member,
                binding: *active,
            }
        }
        BindingState::Detached | BindingState::PendingFinalization(_) | BindingState::Bound(_) => {
            BindingRequiredLookupResult::NoBinding(NoBinding {
                request: binding_required_envelope(request),
            })
        }
    }
}

/// Exact-token resolution state for the identity-slot detach cell.
#[derive(Debug)]
pub enum DetachTokenResolution<'a, EF, V, LF> {
    /// The presented token does not equal the stored cell token.
    NoExactMatch,
    /// The exact token index resolved its owning identity.
    Exact(ResolvedIdentity<'a, EF, V, LF>),
}

impl<EF, V, LF> Copy for DetachTokenResolution<'_, EF, V, LF> {}

impl<EF, V, LF> Clone for DetachTokenResolution<'_, EF, V, LF> {
    fn clone(&self) -> Self {
        *self
    }
}

/// Complete serialized context for exhaustive detach reference lookup.
///
/// Grouping these values prevents a caller from accidentally omitting the
/// exact receiving binding epoch while still keeping token-resolved and
/// presented-key identities distinct.
pub struct DetachLookupContext<'a, EF, V, LF, D> {
    /// Exact-token index result selected before presented-key lookup.
    pub token_resolution: DetachTokenResolution<'a, EF, V, LF>,
    /// Identity found under the request's presented participant key.
    pub presented_identity: PresentedIdentity<'a, EF, V, LF>,
    /// Current four-variant identity-slot detach cell.
    pub cell: &'a DetachCell<D>,
    /// Current binding or pending-finalization state.
    pub binding: &'a BindingState,
    /// Binding epoch carried by the receiving connection's serialized context.
    pub receiving_binding_epoch: Option<BindingEpoch>,
    /// Exact decoded detach request.
    pub request: &'a DetachRequest,
    /// Canonical non-secret verifier computed by the consuming server.
    pub request_verifier: D,
    /// Observer progress used only to prepare exact pending replay.
    pub observer_progress: DeliverySeq,
}

/// Total result of detach participant/token/binding lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DetachLookupResult<'a, EF, D> {
    /// A tombstone won before every live detach-cell result.
    Retired(Retired),
    /// No participant identity or tombstone exists.
    ParticipantUnknown(ParticipantUnknown),
    /// Exact-token or ordinary live authority is stale.
    StaleAuthority(DetachStaleAuthority),
    /// Authority is live but this request has no exact current binding.
    NoBinding(NoBinding),
    /// An exact pending token requires the equality/drain/rewrite transition.
    PendingReplayRequired(PendingReplayRequest<D>),
    /// A different token encountered a pending detach cell.
    DetachInProgress(DetachInProgress),
    /// An exact committed token replays the stable detach result.
    DetachCommitted(DetachCommitted),
    /// All lookup and binding checks passed; remaining detach checks may run.
    Authorized {
        /// Verified live membership.
        member: &'a LiveMember<EF>,
        /// Exact current binding authority.
        binding: ActiveBinding,
    },
}

/// Applies the total participant lookup and detach-cell precedence.
///
/// Tombstone classification precedes all live cell results. For a live
/// identity, exact cell replay precedes generation and binding checks; a
/// different token against `Pending` precedes those checks as
/// `DetachInProgress`. A different token against `Committed` or `Terminalized`
/// continues through ordinary authority lookup.
#[must_use]
pub fn lookup_detach<'a, EF, V, LF, D>(
    context: &DetachLookupContext<'a, EF, V, LF, D>,
) -> DetachLookupResult<'a, EF, D>
where
    D: Copy + Eq,
{
    let token_resolution = context.token_resolution;
    let presented_identity = context.presented_identity;
    let cell = context.cell;
    let binding = context.binding;
    let receiving_binding_epoch = context.receiving_binding_epoch;
    let request = context.request;
    let request_verifier = context.request_verifier;
    let observer_progress = context.observer_progress;
    let exact_member = match token_resolution {
        DetachTokenResolution::Exact(ResolvedIdentity::Retired(tombstone)) => {
            return DetachLookupResult::Retired(retired_detach(tombstone, request));
        }
        DetachTokenResolution::Exact(ResolvedIdentity::Live(member)) => Some(member),
        DetachTokenResolution::NoExactMatch => None,
    };

    if let Some(member) = exact_member
        && let Some(result) = lookup_exact_detach_cell(
            member,
            cell,
            binding,
            request,
            request_verifier,
            observer_progress,
        )
    {
        return result;
    }

    let member = match presented_identity {
        PresentedIdentity::Retired(tombstone) => {
            return DetachLookupResult::Retired(retired_detach(tombstone, request));
        }
        PresentedIdentity::Absent => {
            return DetachLookupResult::ParticipantUnknown(unknown_detach(request));
        }
        PresentedIdentity::Live(member) => {
            if member.conversation_id() != request.conversation_id
                || member.participant_id() != request.participant_id
            {
                return DetachLookupResult::ParticipantUnknown(unknown_detach(request));
            }
            member
        }
    };

    if matches!(token_resolution, DetachTokenResolution::NoExactMatch)
        && let DetachCell::Pending(pending) = cell
    {
        return DetachLookupResult::DetachInProgress(pending.competing_attempt(
            request.conversation_id,
            request.detach_attempt_token,
            request.capability_generation,
        ));
    }

    if request.capability_generation != member.generation() {
        return stale_detach(request, member.generation());
    }

    match binding {
        BindingState::Bound(active)
            if active.conversation_id == request.conversation_id
                && active.participant_id == request.participant_id
                && active.binding_epoch.capability_generation == request.capability_generation
                && receiving_binding_epoch == Some(active.binding_epoch) =>
        {
            DetachLookupResult::Authorized {
                member,
                binding: *active,
            }
        }
        BindingState::Detached | BindingState::PendingFinalization(_) | BindingState::Bound(_) => {
            DetachLookupResult::NoBinding(no_binding_detach(request))
        }
    }
}

fn lookup_exact_detach_cell<'a, EF, D>(
    member: &'a LiveMember<EF>,
    cell: &DetachCell<D>,
    binding: &BindingState,
    request: &DetachRequest,
    request_verifier: D,
    observer_progress: DeliverySeq,
) -> Option<DetachLookupResult<'a, EF, D>>
where
    D: Copy + Eq,
{
    // A detach cell deliberately does not duplicate the conversation id.  The
    // resolved durable member therefore supplies that part of the authority;
    // never reflect a caller-selected conversation into an exact-token reply.
    if request.conversation_id != member.conversation_id() {
        return None;
    }

    match cell {
        DetachCell::Empty(_) => None,
        DetachCell::Pending(pending) => {
            Some(pending.verify_exact(request, request_verifier).map_or_else(
                |_| stale_detach(request, member.generation()),
                |verified| {
                    DetachLookupResult::PendingReplayRequired(verified.prepare_replay(
                        request.conversation_id,
                        *binding,
                        observer_progress,
                    ))
                },
            ))
        }
        DetachCell::Committed(committed) => Some(
            committed
                .verify_exact(request, request_verifier)
                .map_or_else(
                    |_| stale_detach(request, member.generation()),
                    |verified| {
                        DetachLookupResult::DetachCommitted(
                            verified.outcome(request.conversation_id),
                        )
                    },
                ),
        ),
        DetachCell::Terminalized(terminalized) => Some(
            terminalized
                .verify_exact(request, request_verifier)
                .map_or_else(
                    |_| stale_detach(request, member.generation()),
                    |verified| {
                        let outcome = verified.outcome(
                            request.conversation_id,
                            member.generation(),
                            binding_view(member, binding),
                        );
                        DetachLookupResult::StaleAuthority(
                            DetachStaleAuthority::TerminalizedDetachCell(outcome),
                        )
                    },
                ),
        ),
    }
}

/// Result of the permanent Leave-token verifier supplied by the consuming
/// cryptographic layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaveSecretProof {
    /// The presented secret does not match the tombstone/live credential.
    Mismatch,
    /// The presented secret proof matches.
    Verified,
}

/// Total result of Leave participant/token/binding lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaveLookupResult<'a, EF> {
    /// Exact committed Leave token replayed its permanent result.
    LeaveCommitted(LeaveCommitted),
    /// Exact committed token had verified secret but changed generation.
    AttemptTokenBodyConflict(AttemptTokenBodyConflict),
    /// Live or committed-tombstone authority was stale.
    StaleAuthority(LeaveStaleAuthority),
    /// A different token resolved to a tombstone.
    Retired(Retired),
    /// No participant identity or tombstone exists.
    ParticipantUnknown(ParticipantUnknown),
    /// A live bound member was addressed from a different binding epoch.
    NoBinding(NoBinding),
    /// A bound Leave may execute its remaining checks.
    AuthorizedBound {
        /// Verified live membership.
        member: &'a LiveMember<EF>,
        /// Exact current binding authority.
        binding: ActiveBinding,
    },
    /// A detached Leave may execute its remaining checks without acquiring a
    /// binding or cursor authority.
    AuthorizedDetached {
        /// Verified detached live membership.
        member: &'a LiveMember<EF>,
    },
}

/// Applies the committed-Leave exception, tombstone precedence, and live Leave
/// authority/binding order.
///
/// `request_binding_epoch` is the binding epoch carried by the receiving
/// connection's serialized context. It is absent for the explicitly permitted
/// detached Leave path.
#[must_use]
pub fn lookup_leave<'a, EF, V, LF>(
    identity: PresentedIdentity<'a, EF, V, LF>,
    binding: &BindingState,
    request_binding_epoch: Option<crate::wire::BindingEpoch>,
    request: &LeaveRequest,
    secret_proof: LeaveSecretProof,
) -> LeaveLookupResult<'a, EF> {
    match identity {
        PresentedIdentity::Retired(tombstone) => {
            lookup_retired_leave(tombstone, request, secret_proof)
        }
        PresentedIdentity::Absent => LeaveLookupResult::ParticipantUnknown(unknown_leave(request)),
        PresentedIdentity::Live(member) => {
            if member.conversation_id() != request.conversation_id
                || member.participant_id() != request.participant_id
            {
                return LeaveLookupResult::ParticipantUnknown(unknown_leave(request));
            }

            let generation_mismatch = request.capability_generation != member.generation();
            let secret_mismatch = secret_proof == LeaveSecretProof::Mismatch;
            if generation_mismatch || secret_mismatch {
                return LeaveLookupResult::StaleAuthority(live_leave_stale(
                    request,
                    member.generation(),
                ));
            }

            match binding {
                BindingState::Bound(active)
                    if active.conversation_id == request.conversation_id
                        && active.participant_id == request.participant_id
                        && request_binding_epoch == Some(active.binding_epoch) =>
                {
                    LeaveLookupResult::AuthorizedBound {
                        member,
                        binding: *active,
                    }
                }
                BindingState::Bound(_) => LeaveLookupResult::NoBinding(no_binding_leave(request)),
                BindingState::Detached | BindingState::PendingFinalization(_) => {
                    LeaveLookupResult::AuthorizedDetached { member }
                }
            }
        }
    }
}

fn lookup_retired_leave<'a, EF, V, LF>(
    tombstone: &RetiredIdentity<EF, V, LF>,
    request: &LeaveRequest,
    secret_proof: LeaveSecretProof,
) -> LeaveLookupResult<'a, EF> {
    if request.conversation_id != tombstone.conversation_id()
        || request.participant_id != tombstone.participant_id()
    {
        return LeaveLookupResult::ParticipantUnknown(unknown_leave(request));
    }

    if request.leave_attempt_token != tombstone.leave_attempt_token() {
        return LeaveLookupResult::Retired(retired_leave(tombstone, request));
    }

    if secret_proof == LeaveSecretProof::Mismatch {
        return LeaveLookupResult::StaleAuthority(LeaveStaleAuthority::CommittedLeaveTombstone {
            conversation_id: request.conversation_id,
            participant_id: request.participant_id,
            presented_generation: request.capability_generation,
            leave_attempt_token: request.leave_attempt_token,
            retired_generation: tombstone.retired_generation(),
        });
    }

    if request.capability_generation != tombstone.committed_result().presented_generation() {
        return LeaveLookupResult::AttemptTokenBodyConflict(AttemptTokenBodyConflict::Leave {
            token: request.leave_attempt_token,
            conversation_id: request.conversation_id,
            presented_participant_id: request.participant_id,
            presented_generation: request.capability_generation,
        });
    }

    LeaveLookupResult::LeaveCommitted(tombstone.committed_result().clone())
}

fn receipt_binding_is_current(
    binding: &BindingState,
    conversation_id: crate::wire::ConversationId,
    participant_id: crate::wire::ParticipantId,
    origin_binding_epoch: BindingEpoch,
) -> bool {
    match binding {
        BindingState::Bound(active) => {
            active.conversation_id == conversation_id
                && active.participant_id == participant_id
                && active.binding_epoch == origin_binding_epoch
        }
        BindingState::Detached | BindingState::PendingFinalization(_) => false,
    }
}

const fn enrollment_envelope(request: &EnrollmentRequest) -> EnrollmentEnvelope {
    EnrollmentEnvelope {
        conversation_id: request.conversation_id,
        enrollment_token: request.enrollment_token,
    }
}

const fn attach_envelope(request: &CredentialAttachRequest) -> AttachEnvelope {
    AttachEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        attach_attempt_token: request.attach_attempt_token,
        accept_marker_delivery_seq: request.accept_marker_delivery_seq,
    }
}

const fn retired_enrollment<EF, V, LF>(
    tombstone: &RetiredIdentity<EF, V, LF>,
    request: &EnrollmentRequest,
) -> Retired {
    Retired::Enrollment {
        request: enrollment_envelope(request),
        participant_id: tombstone.participant_id(),
        retired_generation: tombstone.retired_generation(),
    }
}

const fn retired_attach<EF, V, LF>(
    tombstone: &RetiredIdentity<EF, V, LF>,
    request: &CredentialAttachRequest,
) -> Retired {
    Retired::Participant {
        request: ParticipantReferenceEnvelope::CredentialAttach(attach_envelope(request)),
        retired_generation: tombstone.retired_generation(),
    }
}

const fn unknown_attach(request: &CredentialAttachRequest) -> ParticipantUnknown {
    ParticipantUnknown {
        request: ParticipantReferenceEnvelope::CredentialAttach(attach_envelope(request)),
    }
}

const fn stale_attach(
    request: &CredentialAttachRequest,
    current_generation: Generation,
) -> StaleAuthority {
    StaleAuthority::Live {
        request: CommonStaleAuthorityEnvelope::CredentialAttach(attach_envelope(request)),
        current_generation,
    }
}

const fn binding_request_conversation(
    request: &ParticipantBindingRequest,
) -> crate::wire::ConversationId {
    match request {
        ParticipantBindingRequest::ParticipantAck(request) => request.conversation_id,
        ParticipantBindingRequest::MarkerAck(request) => request.conversation_id,
        ParticipantBindingRequest::RecordAdmission(request) => request.conversation_id,
    }
}

const fn binding_request_participant(
    request: &ParticipantBindingRequest,
) -> crate::wire::ParticipantId {
    match request {
        ParticipantBindingRequest::ParticipantAck(request) => request.participant_id,
        ParticipantBindingRequest::MarkerAck(request) => request.participant_id,
        ParticipantBindingRequest::RecordAdmission(request) => request.participant_id,
    }
}

const fn binding_request_generation(request: &ParticipantBindingRequest) -> Generation {
    match request {
        ParticipantBindingRequest::ParticipantAck(request) => request.capability_generation,
        ParticipantBindingRequest::MarkerAck(request) => request.capability_generation,
        ParticipantBindingRequest::RecordAdmission(request) => request.capability_generation,
    }
}

const fn participant_ack_envelope(request: &ParticipantAck) -> ParticipantAckEnvelope {
    ParticipantAckEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        through_seq: request.through_seq,
    }
}

const fn marker_ack_envelope(request: &MarkerAck) -> MarkerAckEnvelope {
    MarkerAckEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        marker_delivery_seq: request.marker_delivery_seq,
    }
}

const fn record_admission_envelope(request: &RecordAdmission) -> RecordAdmissionEnvelope {
    RecordAdmissionEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
    }
}

const fn participant_reference(
    request: &ParticipantBindingRequest,
) -> ParticipantReferenceEnvelope {
    match request {
        ParticipantBindingRequest::ParticipantAck(request) => {
            ParticipantReferenceEnvelope::ParticipantAck(participant_ack_envelope(request))
        }
        ParticipantBindingRequest::MarkerAck(request) => {
            ParticipantReferenceEnvelope::MarkerAck(marker_ack_envelope(request))
        }
        ParticipantBindingRequest::RecordAdmission(request) => {
            ParticipantReferenceEnvelope::RecordAdmission(record_admission_envelope(request))
        }
    }
}

const fn binding_required_envelope(request: &ParticipantBindingRequest) -> BindingRequiredEnvelope {
    match request {
        ParticipantBindingRequest::ParticipantAck(request) => {
            BindingRequiredEnvelope::ParticipantAck(participant_ack_envelope(request))
        }
        ParticipantBindingRequest::MarkerAck(request) => {
            BindingRequiredEnvelope::MarkerAck(marker_ack_envelope(request))
        }
        ParticipantBindingRequest::RecordAdmission(request) => {
            BindingRequiredEnvelope::RecordAdmission(record_admission_envelope(request))
        }
    }
}

const fn common_stale_envelope(
    request: &ParticipantBindingRequest,
) -> CommonStaleAuthorityEnvelope {
    match request {
        ParticipantBindingRequest::ParticipantAck(request) => {
            CommonStaleAuthorityEnvelope::ParticipantAck(participant_ack_envelope(request))
        }
        ParticipantBindingRequest::MarkerAck(request) => {
            CommonStaleAuthorityEnvelope::MarkerAck(marker_ack_envelope(request))
        }
        ParticipantBindingRequest::RecordAdmission(request) => {
            CommonStaleAuthorityEnvelope::RecordAdmission(record_admission_envelope(request))
        }
    }
}

fn binding_view<EF>(member: &LiveMember<EF>, binding: &BindingState) -> BindingStateView {
    match binding {
        BindingState::Bound(active)
            if active.participant_id == member.participant_id()
                && active.conversation_id == member.conversation_id()
                && active.binding_epoch.capability_generation == member.generation() =>
        {
            BindingStateView::Bound {
                current_binding_epoch: active.binding_epoch,
            }
        }
        BindingState::Detached | BindingState::PendingFinalization(_) | BindingState::Bound(_) => {
            BindingStateView::Detached
        }
    }
}

const fn detach_envelope(request: &DetachRequest) -> DetachEnvelope {
    DetachEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        detach_attempt_token: request.detach_attempt_token,
    }
}

const fn leave_envelope(request: &LeaveRequest) -> LeaveEnvelope {
    LeaveEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        leave_attempt_token: request.leave_attempt_token,
    }
}

const fn retired_detach<EF, V, LF>(
    tombstone: &RetiredIdentity<EF, V, LF>,
    request: &DetachRequest,
) -> Retired {
    Retired::Participant {
        request: ParticipantReferenceEnvelope::Detach(detach_envelope(request)),
        retired_generation: tombstone.retired_generation(),
    }
}

const fn retired_leave<EF, V, LF>(
    tombstone: &RetiredIdentity<EF, V, LF>,
    request: &LeaveRequest,
) -> Retired {
    Retired::Participant {
        request: ParticipantReferenceEnvelope::Leave(leave_envelope(request)),
        retired_generation: tombstone.retired_generation(),
    }
}

const fn unknown_detach(request: &DetachRequest) -> ParticipantUnknown {
    ParticipantUnknown {
        request: ParticipantReferenceEnvelope::Detach(detach_envelope(request)),
    }
}

const fn unknown_leave(request: &LeaveRequest) -> ParticipantUnknown {
    ParticipantUnknown {
        request: ParticipantReferenceEnvelope::Leave(leave_envelope(request)),
    }
}

const fn no_binding_detach(request: &DetachRequest) -> NoBinding {
    NoBinding {
        request: BindingRequiredEnvelope::Detach(detach_envelope(request)),
    }
}

const fn no_binding_leave(request: &LeaveRequest) -> NoBinding {
    NoBinding {
        request: BindingRequiredEnvelope::Leave(leave_envelope(request)),
    }
}

const fn stale_detach<'a, EF, D>(
    request: &DetachRequest,
    current_generation: Generation,
) -> DetachLookupResult<'a, EF, D> {
    DetachLookupResult::StaleAuthority(DetachStaleAuthority::Live {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        detach_attempt_token: request.detach_attempt_token,
        current_generation,
    })
}

const fn live_leave_stale(
    request: &LeaveRequest,
    current_generation: Generation,
) -> LeaveStaleAuthority {
    LeaveStaleAuthority::Live {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        presented_generation: request.capability_generation,
        leave_attempt_token: request.leave_attempt_token,
        current_generation,
    }
}
