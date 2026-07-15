use crate::wire::{
    AttemptTokenBodyConflict, BindingRequiredEnvelope, BindingStateView, DeliverySeq,
    DetachCommitted, DetachEnvelope, DetachInProgress, DetachRequest, DetachStaleAuthority,
    Generation, LeaveCommitted, LeaveEnvelope, LeaveRequest, LeaveStaleAuthority, NoBinding,
    ObserverBackpressure, ParticipantReferenceEnvelope, ParticipantUnknown, Retired,
};

use super::{
    ActiveBinding, BindingState, DetachCell, DetachReplayError, IdentityState, LiveMember,
    RetiredIdentity,
};

/// Result of resolving the presented participant key before operation-specific
/// authority checks.
#[derive(Debug)]
pub enum PresentedIdentity<'a, V> {
    /// No live identity or tombstone exists for the presented key.
    Absent,
    /// The presented key resolves to live membership.
    Live(&'a LiveMember),
    /// The presented key resolves to a permanent tombstone.
    Retired(&'a RetiredIdentity<V>),
}

impl<V> Copy for PresentedIdentity<'_, V> {}

impl<V> Clone for PresentedIdentity<'_, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, V> From<Option<&'a IdentityState<V>>> for PresentedIdentity<'a, V> {
    fn from(value: Option<&'a IdentityState<V>>) -> Self {
        match value {
            None => Self::Absent,
            Some(IdentityState::Live(member)) => Self::Live(member),
            Some(IdentityState::Retired(tombstone)) => Self::Retired(tombstone),
        }
    }
}

/// Total result of detach participant/token/binding lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DetachLookupResult {
    /// A tombstone won before every live detach-cell result.
    Retired(Retired),
    /// No participant identity or tombstone exists.
    ParticipantUnknown(ParticipantUnknown),
    /// Exact-token or ordinary live authority is stale.
    StaleAuthority(DetachStaleAuthority),
    /// Authority is live but this request has no exact current binding.
    NoBinding(NoBinding),
    /// An exact pending token replays its current observer refusal.
    Pending(ObserverBackpressure),
    /// A different token encountered a pending detach cell.
    DetachInProgress(DetachInProgress),
    /// An exact committed token replays the stable detach result.
    DetachCommitted(DetachCommitted),
    /// All lookup and binding checks passed; remaining detach checks may run.
    Authorized {
        /// Verified live membership.
        member: LiveMember,
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
pub fn lookup_detach<V: Copy + Eq>(
    identity: PresentedIdentity<'_, V>,
    cell: &DetachCell<V>,
    binding: &BindingState,
    request: &DetachRequest,
    request_verifier: V,
    observer_progress: DeliverySeq,
) -> DetachLookupResult {
    let member = match identity {
        PresentedIdentity::Retired(tombstone) => {
            return DetachLookupResult::Retired(retired_detach(tombstone, request));
        }
        PresentedIdentity::Absent => {
            return DetachLookupResult::ParticipantUnknown(unknown_detach(request));
        }
        PresentedIdentity::Live(member) => {
            if member.conversation_id != request.conversation_id
                || member.participant_id != request.participant_id
            {
                return DetachLookupResult::ParticipantUnknown(unknown_detach(request));
            }
            *member
        }
    };

    match cell {
        DetachCell::Empty(_) => {}
        DetachCell::Pending(pending) => match pending.verify_exact(request, request_verifier) {
            Ok(verified) => {
                return DetachLookupResult::Pending(
                    verified.outcome(request.conversation_id, observer_progress),
                );
            }
            Err(DetachReplayError::Token) => {
                return DetachLookupResult::DetachInProgress(pending.competing_attempt(
                    request.conversation_id,
                    request.detach_attempt_token,
                    request.capability_generation,
                ));
            }
            Err(_) => return stale_detach(request, member.generation),
        },
        DetachCell::Committed(committed) => {
            match committed.verify_exact(request, request_verifier) {
                Ok(verified) => {
                    return DetachLookupResult::DetachCommitted(
                        verified.outcome(request.conversation_id),
                    );
                }
                Err(DetachReplayError::Token) => {}
                Err(_) => return stale_detach(request, member.generation),
            }
        }
        DetachCell::Terminalized(terminalized) => {
            match terminalized.verify_exact(request, request_verifier) {
                Ok(verified) => {
                    let terminalized = verified.outcome(
                        request.conversation_id,
                        member.generation,
                        binding_view(binding),
                    );
                    return DetachLookupResult::StaleAuthority(
                        DetachStaleAuthority::TerminalizedDetachCell(terminalized),
                    );
                }
                Err(DetachReplayError::Token) => {}
                Err(_) => return stale_detach(request, member.generation),
            }
        }
    }

    if request.capability_generation != member.generation {
        return stale_detach(request, member.generation);
    }

    match binding {
        BindingState::Bound(active)
            if active.conversation_id == request.conversation_id
                && active.participant_id == request.participant_id
                && active.binding_epoch.capability_generation == request.capability_generation =>
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
pub enum LeaveLookupResult {
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
        member: LiveMember,
        /// Exact current binding authority.
        binding: ActiveBinding,
    },
    /// A detached Leave may execute its remaining checks without acquiring a
    /// binding or cursor authority.
    AuthorizedDetached {
        /// Verified detached live membership.
        member: LiveMember,
    },
}

/// Applies the committed-Leave exception, tombstone precedence, and live Leave
/// authority/binding order.
///
/// `request_binding_epoch` is the binding epoch carried by the receiving
/// connection's serialized context. It is absent for the explicitly permitted
/// detached Leave path.
#[must_use]
pub fn lookup_leave<V>(
    identity: PresentedIdentity<'_, V>,
    binding: &BindingState,
    request_binding_epoch: Option<crate::wire::BindingEpoch>,
    request: &LeaveRequest,
    secret_proof: LeaveSecretProof,
) -> LeaveLookupResult {
    match identity {
        PresentedIdentity::Retired(tombstone) => {
            lookup_retired_leave(tombstone, request, secret_proof)
        }
        PresentedIdentity::Absent => LeaveLookupResult::ParticipantUnknown(unknown_leave(request)),
        PresentedIdentity::Live(member) => {
            if member.conversation_id != request.conversation_id
                || member.participant_id != request.participant_id
            {
                return LeaveLookupResult::ParticipantUnknown(unknown_leave(request));
            }

            let generation_mismatch = request.capability_generation != member.generation;
            let secret_mismatch = secret_proof == LeaveSecretProof::Mismatch;
            if generation_mismatch || secret_mismatch {
                return LeaveLookupResult::StaleAuthority(live_leave_stale(
                    request,
                    member.generation,
                ));
            }

            match binding {
                BindingState::Bound(active)
                    if active.conversation_id == request.conversation_id
                        && active.participant_id == request.participant_id
                        && request_binding_epoch == Some(active.binding_epoch) =>
                {
                    LeaveLookupResult::AuthorizedBound {
                        member: *member,
                        binding: *active,
                    }
                }
                BindingState::Bound(_) => LeaveLookupResult::NoBinding(no_binding_leave(request)),
                BindingState::Detached | BindingState::PendingFinalization(_) => {
                    LeaveLookupResult::AuthorizedDetached { member: *member }
                }
            }
        }
    }
}

fn lookup_retired_leave<V>(
    tombstone: &RetiredIdentity<V>,
    request: &LeaveRequest,
    secret_proof: LeaveSecretProof,
) -> LeaveLookupResult {
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

    if request.capability_generation != tombstone.committed_result().presented_generation {
        return LeaveLookupResult::AttemptTokenBodyConflict(AttemptTokenBodyConflict::Leave {
            token: request.leave_attempt_token,
            conversation_id: request.conversation_id,
            presented_participant_id: request.participant_id,
            presented_generation: request.capability_generation,
        });
    }

    LeaveLookupResult::LeaveCommitted(tombstone.committed_result().clone())
}

const fn binding_view(binding: &BindingState) -> BindingStateView {
    match binding {
        BindingState::Bound(active) => BindingStateView::Bound {
            current_binding_epoch: active.binding_epoch,
        },
        BindingState::Detached | BindingState::PendingFinalization(_) => BindingStateView::Detached,
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

const fn retired_detach<V>(tombstone: &RetiredIdentity<V>, request: &DetachRequest) -> Retired {
    Retired::Participant {
        request: ParticipantReferenceEnvelope::Detach(detach_envelope(request)),
        retired_generation: tombstone.retired_generation(),
    }
}

const fn retired_leave<V>(tombstone: &RetiredIdentity<V>, request: &LeaveRequest) -> Retired {
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

const fn stale_detach(
    request: &DetachRequest,
    current_generation: Generation,
) -> DetachLookupResult {
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
