//! Move-only executable ownership for live lifecycle frontier transitions.
//!
//! Storage supplies canonical encoded row charges, but every participant,
//! binding, cursor, retained row, and aggregate-claim transition is derived
//! inside the protocol from an existing sealed operation commit.

use alloc::{boxed::Box, vec, vec::Vec};

use super::super::{
    AttachCommit, AttachTransition, ClaimFrontiers, ClosureAccounting, CommittedDetachTransition,
    FrontierBinding, FrontierParticipant, InitialEnrollmentFrontierCommit, MarkerAckCommit,
    NonzeroParticipantAckCommit, OrderLedger, ParticipantAckCommit, RetainedCausalRecord,
    RetainedCausalRecordKind, SequenceLedger, claim_frontier::LiveFrontierTransitionError,
};
use super::{InitialEnrollmentOperationCommit, RetainedRecordCharge};

mod ledger;
mod state;
use ledger::{
    detach_order, detach_sequence, detached_attach_order, detached_attach_sequence,
    enrollment_order, enrollment_sequence, superseding_attach_order, superseding_attach_sequence,
};
use state::{
    accounting_after_fenced_attach, accounting_after_rows, retained_attached, retained_terminal,
};

/// Complete executable frontier, closure-accounting, and keyed-retention owner.
///
/// The owner is intentionally move-only. It is the only live mutation input and
/// never exposes a constructor from independent frontier/accounting components.
/// Frontier, closure, retained charges, and participant history therefore cannot
/// be cloned or recombined from different owners:
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::LiveFrontierOwner;
///
/// fn clone_frontier(owner: &LiveFrontierOwner) -> LiveFrontierOwner {
///     owner.clone()
/// }
/// ```
///
/// ```compile_fail
/// use liminal_protocol::lifecycle::LiveFrontierOwner;
///
/// fn splice(left: &mut LiveFrontierOwner, right: LiveFrontierOwner) {
///     left.frontiers = right.frontiers;
///     left.closure_accounting = right.closure_accounting;
///     left.retained_charges = right.retained_charges;
/// }
/// ```
#[derive(Debug, PartialEq, Eq)]
pub struct LiveFrontierOwner {
    frontiers: ClaimFrontiers,
    closure_accounting: ClosureAccounting,
    retained_charges: Vec<RetainedRecordCharge>,
    retained_record_limit: u64,
}

impl LiveFrontierOwner {
    /// Acquires live ownership from the protocol's atomic initial-enrollment result.
    #[must_use]
    pub fn from_initial_enrollment<F>(
        initial: InitialEnrollmentFrontierCommit<F>,
        retained_record_limit: u64,
    ) -> (InitialEnrollmentOperationCommit<F>, Self) {
        let (operation, frontiers, closure_accounting, attached_charge) =
            initial.into_conversation_parts();
        let attached = operation.enrollment().attached;
        let retained_charges = vec![RetainedRecordCharge::new(
            attached.delivery_seq(),
            attached.admission_order(),
            attached_charge,
        )];
        (
            operation,
            Self {
                frontiers,
                closure_accounting,
                retained_charges,
                retained_record_limit,
            },
        )
    }

    #[cfg(test)]
    pub(in crate::lifecycle) const fn from_test_parts(
        frontiers: ClaimFrontiers,
        closure_accounting: ClosureAccounting,
        retained_charges: Vec<RetainedRecordCharge>,
        retained_record_limit: u64,
    ) -> Self {
        Self {
            frontiers,
            closure_accounting,
            retained_charges,
            retained_record_limit,
        }
    }

    /// Borrows the coupled claim frontiers.
    #[must_use]
    pub const fn frontiers(&self) -> &ClaimFrontiers {
        &self.frontiers
    }

    /// Returns complete current closure accounting.
    #[must_use]
    pub const fn closure_accounting(&self) -> ClosureAccounting {
        self.closure_accounting
    }

    /// Borrows canonical keyed charges for the retained suffix.
    #[must_use]
    pub fn retained_charges(&self) -> &[RetainedRecordCharge] {
        &self.retained_charges
    }

    /// Returns the signed retained causal-row cap.
    #[must_use]
    pub const fn retained_record_limit(&self) -> u64 {
        self.retained_record_limit
    }

    /// Consumes the complete owner for `RecordAdmission`, Leave, or persistence.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ClaimFrontiers,
        ClosureAccounting,
        Vec<RetainedRecordCharge>,
        u64,
    ) {
        (
            self.frontiers,
            self.closure_accounting,
            self.retained_charges,
            self.retained_record_limit,
        )
    }
}

/// Exact charges for a credential attach's one or two retained rows.
#[derive(Debug, PartialEq, Eq)]
pub struct AttachFrontierCharges {
    terminal: Option<RetainedRecordCharge>,
    attached: RetainedRecordCharge,
    seal: LiveTransitionInputSeal,
}

#[derive(Debug, PartialEq, Eq)]
enum LiveTransitionInputSeal {
    Validated,
}

impl AttachFrontierCharges {
    /// Couples the canonical `Attached` charge with an optional terminal charge.
    #[must_use]
    pub const fn new(
        terminal: Option<RetainedRecordCharge>,
        attached: RetainedRecordCharge,
    ) -> Self {
        Self {
            terminal,
            attached,
            seal: LiveTransitionInputSeal::Validated,
        }
    }

    const fn into_parts(self) -> (Option<RetainedRecordCharge>, RetainedRecordCharge) {
        let Self {
            terminal,
            attached,
            seal,
        } = self;
        match seal {
            LiveTransitionInputSeal::Validated => (terminal, attached),
        }
    }
}

/// A typed lifecycle commit paired with its complete post-transition owner.
#[derive(Debug, PartialEq, Eq)]
pub struct LiveFrontierCommit<T> {
    operation: T,
    owner: LiveFrontierOwner,
}

impl<T> LiveFrontierCommit<T> {
    /// Borrows the exact typed lifecycle commit.
    #[must_use]
    pub const fn operation(&self) -> &T {
        &self.operation
    }

    /// Borrows the complete post-transition owner.
    #[must_use]
    pub const fn owner(&self) -> &LiveFrontierOwner {
        &self.owner
    }

    /// Consumes the atomic transition for durability publication.
    #[must_use]
    pub fn into_parts(self) -> (T, LiveFrontierOwner) {
        (self.operation, self.owner)
    }
}

/// Failed live transition retaining the unchanged complete owner and operation.
#[derive(Debug, PartialEq, Eq)]
pub struct LiveFrontierFailure<T> {
    error: LiveFrontierError,
    operation: T,
    owner: LiveFrontierOwner,
}

impl<T> LiveFrontierFailure<T> {
    /// Returns the exact typed transition failure.
    #[must_use]
    pub const fn error(&self) -> LiveFrontierError {
        self.error
    }

    /// Recovers the unchanged owner and intact operation commit.
    #[must_use]
    pub fn into_parts(self) -> (T, LiveFrontierOwner) {
        (self.operation, self.owner)
    }
}

/// Failure selected while coupling a sealed lifecycle commit to live ownership.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LiveFrontierError {
    /// Commit and live owner name different authority.
    Authority,
    /// A mandatory immutable/recovery transition has precedence.
    Precedence,
    /// Canonical keyed row charges differ from the commit-derived retained rows.
    RetainedCharge,
    /// The retained causal-row cap would be exceeded.
    RetainedRecordLimit,
    /// Aggregate claim arithmetic or exact owner reconstruction failed.
    Frontier,
    /// Resulting closure accounting is invalid or outside its signed capacity.
    ClosureAccounting,
}

/// Result of coupling any typed lifecycle commit to live frontier ownership.
pub type LiveFrontierResult<T> = Result<LiveFrontierCommit<T>, Box<LiveFrontierFailure<T>>>;

/// Applies a subsequent enrollment to the complete live owner.
///
/// # Errors
///
/// Returns a failure retaining the unchanged owner and intact enrollment commit.
pub fn apply_enrollment_frontier<F>(
    owner: LiveFrontierOwner,
    operation: super::super::EnrollmentCommit<F>,
    attached_charge: RetainedRecordCharge,
) -> LiveFrontierResult<super::super::EnrollmentCommit<F>> {
    let attached = operation.attached;
    if attached.conversation_id() != owner.frontiers.conversation_id() {
        return failure(owner, operation, LiveFrontierError::Authority);
    }
    let participant_id = attached.participant_id();
    let mut active = owner.frontiers.active_identities().participants().to_vec();
    if active
        .iter()
        .any(|participant| participant.participant_index() == participant_id)
    {
        return failure(owner, operation, LiveFrontierError::Authority);
    }
    active.push(FrontierParticipant::new(
        participant_id,
        operation.member.cursor(),
        FrontierBinding::Bound(attached.binding_epoch()),
    ));
    active.sort_unstable_by_key(|participant| participant.participant_index());
    let rows = [retained_attached(attached)];
    let Some(sequence) =
        enrollment_sequence(owner.frontiers.sequence().ledger(), attached.delivery_seq())
    else {
        return failure(owner, operation, LiveFrontierError::Frontier);
    };
    let Some(order) = enrollment_order(
        owner.frontiers.order().ledger(),
        attached.admission_order().transaction_order(),
    ) else {
        return failure(owner, operation, LiveFrontierError::Frontier);
    };
    transition(
        owner,
        operation,
        active,
        &rows,
        vec![attached_charge],
        sequence,
        order,
    )
}

/// Applies credential attach to the complete live owner.
///
/// # Errors
///
/// Returns a failure retaining the unchanged owner, intact attach commit, and
/// exact reason the commit could not enter the frontier.
pub fn apply_attach_frontier<F, V>(
    owner: LiveFrontierOwner,
    operation: AttachCommit<F, V>,
    charges: AttachFrontierCharges,
) -> LiveFrontierResult<AttachCommit<F, V>> {
    let (terminal_charge, attached_charge) = charges.into_parts();
    let attached = operation.attached;
    if attached.conversation_id() != owner.frontiers.conversation_id() {
        return failure(owner, operation, LiveFrontierError::Authority);
    }
    let mut active = owner.frontiers.active_identities().participants().to_vec();
    let Some(participant) = active
        .iter_mut()
        .find(|participant| participant.participant_index() == attached.participant_id())
    else {
        return failure(owner, operation, LiveFrontierError::Authority);
    };
    *participant = FrontierParticipant::new(
        participant.participant_index(),
        operation.member.cursor(),
        FrontierBinding::Bound(attached.binding_epoch()),
    );
    let current_sequence = owner.frontiers.sequence().ledger();
    let current_order = owner.frontiers.order().ledger();
    let (rows, keyed_charges, sequence, order) = match operation.transition {
        AttachTransition::Detached => {
            if terminal_charge.is_some() {
                return failure(owner, operation, LiveFrontierError::RetainedCharge);
            }
            let Some(sequence) =
                detached_attach_sequence(current_sequence, attached.delivery_seq())
            else {
                return failure(owner, operation, LiveFrontierError::Frontier);
            };
            let Some(order) = detached_attach_order(
                current_order,
                attached.admission_order().transaction_order(),
            ) else {
                return failure(owner, operation, LiveFrontierError::Frontier);
            };
            (
                vec![retained_attached(attached)],
                vec![attached_charge],
                sequence,
                order,
            )
        }
        AttachTransition::Superseded { terminal } => {
            let Some(terminal_charge) = terminal_charge else {
                return failure(owner, operation, LiveFrontierError::RetainedCharge);
            };
            let rows = vec![
                retained_terminal(terminal.into()),
                retained_attached(attached),
            ];
            let Some(sequence) = superseding_attach_sequence(current_sequence, &rows) else {
                return failure(owner, operation, LiveFrontierError::Frontier);
            };
            let Some(order) = superseding_attach_order(
                current_order,
                attached.admission_order().transaction_order(),
            ) else {
                return failure(owner, operation, LiveFrontierError::Frontier);
            };
            (
                rows,
                vec![terminal_charge, attached_charge],
                sequence,
                order,
            )
        }
        AttachTransition::FencedRecovery {
            prior_binding_epoch,
            composed_terminal,
            next_closure_state,
        } => {
            return apply_fenced_attach_frontier(
                owner,
                operation,
                terminal_charge,
                attached_charge,
                prior_binding_epoch,
                composed_terminal,
                next_closure_state,
            );
        }
    };
    transition(
        owner,
        operation,
        active,
        &rows,
        keyed_charges,
        sequence,
        order,
    )
}

/// Applies a committed detach terminal to the complete live owner.
///
/// # Errors
///
/// Returns a failure retaining the unchanged owner and intact detach commit.
pub fn apply_detach_frontier<EF, V>(
    owner: LiveFrontierOwner,
    operation: CommittedDetachTransition<EF, V>,
    terminal_charge: RetainedRecordCharge,
) -> LiveFrontierResult<CommittedDetachTransition<EF, V>> {
    let terminal = operation.terminal();
    if terminal.conversation_id() != owner.frontiers.conversation_id() {
        return failure(owner, operation, LiveFrontierError::Authority);
    }
    let mut active = owner.frontiers.active_identities().participants().to_vec();
    let Some(participant) = active
        .iter_mut()
        .find(|participant| participant.participant_index() == terminal.participant_id())
    else {
        return failure(owner, operation, LiveFrontierError::Authority);
    };
    *participant = FrontierParticipant::new(
        participant.participant_index(),
        operation.member().cursor(),
        FrontierBinding::Detached(terminal.binding_epoch()),
    );
    let row = retained_terminal(terminal.into());
    let Some(sequence) = detach_sequence(owner.frontiers.sequence().ledger(), row.delivery_seq)
    else {
        return failure(owner, operation, LiveFrontierError::Frontier);
    };
    let Some(order) = detach_order(
        owner.frontiers.order().ledger(),
        row.admission_order.transaction_order(),
    ) else {
        return failure(owner, operation, LiveFrontierError::Frontier);
    };
    transition(
        owner,
        operation,
        active,
        &[row],
        vec![terminal_charge],
        sequence,
        order,
    )
}

/// Applies a zero-debt participant acknowledgement cursor transition.
///
/// # Errors
///
/// Returns a failure retaining the unchanged owner and intact ack commit.
pub fn apply_participant_ack_frontier(
    mut owner: LiveFrontierOwner,
    operation: ParticipantAckCommit,
) -> LiveFrontierResult<ParticipantAckCommit> {
    let request = operation.outcome().request();
    let Some(current) = owner
        .frontiers
        .active_identities()
        .participants()
        .iter()
        .find(|participant| participant.participant_index() == request.participant_id)
        .copied()
    else {
        return failure(owner, operation, LiveFrontierError::Authority);
    };
    let participant = FrontierParticipant::new(
        request.participant_id,
        request.through_seq,
        current.binding(),
    );
    owner.frontiers = match owner.frontiers.apply_live_identity(participant) {
        Ok(frontiers) => frontiers,
        Err(frontier_failure) => {
            let (frontiers, error) = *frontier_failure;
            owner.frontiers = frontiers;
            return failure(owner, operation, map_frontier_error(error));
        }
    };
    Ok(LiveFrontierCommit { operation, owner })
}

/// Applies a nonzero-debt participant acknowledgement cursor transition.
///
/// The episode and member remain owned by the sealed aggregate commit; this
/// transition consumes the same exact acknowledged cursor into the coupled
/// claim-frontier participant rank.
///
/// # Errors
///
/// Returns a failure retaining the unchanged owner and intact aggregate commit.
pub fn apply_nonzero_participant_ack_frontier(
    mut owner: LiveFrontierOwner,
    operation: NonzeroParticipantAckCommit,
) -> LiveFrontierResult<NonzeroParticipantAckCommit> {
    let request = operation.outcome().request();
    let Some(current) = owner
        .frontiers
        .active_identities()
        .participants()
        .iter()
        .find(|participant| participant.participant_index() == request.participant_id)
        .copied()
    else {
        return failure(owner, operation, LiveFrontierError::Authority);
    };
    let participant = FrontierParticipant::new(
        request.participant_id,
        request.through_seq,
        current.binding(),
    );
    owner.frontiers = match owner.frontiers.apply_live_identity(participant) {
        Ok(frontiers) => frontiers,
        Err(frontier_failure) => {
            let (frontiers, error) = *frontier_failure;
            owner.frontiers = frontiers;
            return failure(owner, operation, map_frontier_error(error));
        }
    };
    Ok(LiveFrontierCommit { operation, owner })
}

/// Applies a zero-debt marker acknowledgement cursor transition.
///
/// # Errors
///
/// Returns a failure retaining the unchanged owner and intact marker-ack commit.
pub fn apply_marker_ack_frontier(
    mut owner: LiveFrontierOwner,
    operation: MarkerAckCommit,
) -> LiveFrontierResult<MarkerAckCommit> {
    let request = operation.outcome().request();
    if !owner
        .frontiers
        .retained_marker_records()
        .iter()
        .any(|record| {
            record.delivery_seq == request.marker_delivery_seq
                && matches!(
                    record.kind,
                    RetainedCausalRecordKind::CompactionMarker { participant_index, .. }
                        if participant_index == request.participant_id
                )
        })
    {
        return failure(owner, operation, LiveFrontierError::Authority);
    }
    let Some(current) = owner
        .frontiers
        .active_identities()
        .participants()
        .iter()
        .find(|participant| participant.participant_index() == request.participant_id)
        .copied()
    else {
        return failure(owner, operation, LiveFrontierError::Authority);
    };
    let participant = FrontierParticipant::new(
        request.participant_id,
        request.marker_delivery_seq,
        current.binding(),
    );
    owner.frontiers = match owner.frontiers.apply_live_identity(participant) {
        Ok(frontiers) => frontiers,
        Err(frontier_failure) => {
            let (frontiers, error) = *frontier_failure;
            owner.frontiers = frontiers;
            return failure(owner, operation, map_frontier_error(error));
        }
    };
    Ok(LiveFrontierCommit { operation, owner })
}

fn apply_fenced_attach_frontier<F, V>(
    owner: LiveFrontierOwner,
    operation: AttachCommit<F, V>,
    terminal_charge: Option<RetainedRecordCharge>,
    attached_charge: RetainedRecordCharge,
    prior_binding_epoch: crate::wire::BindingEpoch,
    composed_terminal: Option<super::super::CommittedBindingTerminal>,
    next_closure_state: super::super::ClosureState,
) -> LiveFrontierResult<AttachCommit<F, V>> {
    let attached = operation.attached;
    let (rows, charges) = match (composed_terminal, terminal_charge) {
        (None, None) => (vec![retained_attached(attached)], vec![attached_charge]),
        (Some(terminal), Some(terminal_charge)) => (
            vec![retained_terminal(terminal), retained_attached(attached)],
            vec![terminal_charge, attached_charge],
        ),
        (None, Some(_)) | (Some(_), None) => {
            return failure(owner, operation, LiveFrontierError::RetainedCharge);
        }
    };
    let participant = FrontierParticipant::new(
        attached.participant_id(),
        operation.member.cursor(),
        FrontierBinding::Bound(attached.binding_epoch()),
    );
    fenced_attach_transition(
        owner,
        operation,
        participant,
        prior_binding_epoch,
        next_closure_state,
        &rows,
        charges,
    )
}

fn fenced_attach_transition<T>(
    mut owner: LiveFrontierOwner,
    operation: T,
    participant: FrontierParticipant,
    prior_binding_epoch: crate::wire::BindingEpoch,
    next_closure_state: super::super::ClosureState,
    rows: &[RetainedCausalRecord],
    charges: Vec<RetainedRecordCharge>,
) -> LiveFrontierResult<T> {
    if rows.len() != charges.len()
        || rows.iter().zip(&charges).any(|(row, charge)| {
            row.delivery_seq != charge.delivery_seq()
                || row.admission_order != charge.admission_order()
                || charge.encoded_charge().entries != 1
        })
    {
        return failure(owner, operation, LiveFrontierError::RetainedCharge);
    }
    let resulting_len = owner
        .frontiers
        .retained_records()
        .len()
        .checked_add(rows.len());
    if resulting_len
        .and_then(|len| u64::try_from(len).ok())
        .is_none_or(|len| len > owner.retained_record_limit)
    {
        return failure(owner, operation, LiveFrontierError::RetainedRecordLimit);
    }
    let Some(accounting) =
        accounting_after_fenced_attach(owner.closure_accounting, &charges, next_closure_state)
    else {
        return failure(owner, operation, LiveFrontierError::ClosureAccounting);
    };
    owner.frontiers =
        match owner
            .frontiers
            .apply_live_fenced_attach(participant, prior_binding_epoch, rows)
        {
            Ok(frontiers) => frontiers,
            Err(frontier_failure) => {
                let (frontiers, error) = *frontier_failure;
                owner.frontiers = frontiers;
                return failure(owner, operation, map_frontier_error(error));
            }
        };
    owner.retained_charges.extend(charges);
    owner
        .retained_charges
        .sort_unstable_by_key(|charge| charge.delivery_seq());
    owner.closure_accounting = accounting;
    Ok(LiveFrontierCommit { operation, owner })
}

fn transition<T>(
    mut owner: LiveFrontierOwner,
    operation: T,
    active: Vec<FrontierParticipant>,
    rows: &[RetainedCausalRecord],
    charges: Vec<RetainedRecordCharge>,
    sequence: SequenceLedger,
    order: OrderLedger,
) -> LiveFrontierResult<T> {
    if rows.len() != charges.len()
        || rows.iter().zip(&charges).any(|(row, charge)| {
            row.delivery_seq != charge.delivery_seq()
                || row.admission_order != charge.admission_order()
                || charge.encoded_charge().entries != 1
        })
    {
        return failure(owner, operation, LiveFrontierError::RetainedCharge);
    }
    let resulting_len = owner
        .frontiers
        .retained_records()
        .len()
        .checked_add(rows.len());
    if resulting_len
        .and_then(|len| u64::try_from(len).ok())
        .is_none_or(|len| len > owner.retained_record_limit)
    {
        return failure(owner, operation, LiveFrontierError::RetainedRecordLimit);
    }
    let Some(accounting) = accounting_after_rows(owner.closure_accounting, &charges) else {
        return failure(owner, operation, LiveFrontierError::ClosureAccounting);
    };
    owner.frontiers = match owner
        .frontiers
        .apply_live_transition(active, rows, sequence, order)
    {
        Ok(frontiers) => frontiers,
        Err(frontier_failure) => {
            let (frontiers, error) = *frontier_failure;
            owner.frontiers = frontiers;
            return failure(owner, operation, map_frontier_error(error));
        }
    };
    owner.retained_charges.extend(charges);
    owner.closure_accounting = accounting;
    Ok(LiveFrontierCommit { operation, owner })
}

const fn map_frontier_error(error: LiveFrontierTransitionError) -> LiveFrontierError {
    match error {
        LiveFrontierTransitionError::Authority => LiveFrontierError::Authority,
        LiveFrontierTransitionError::Precedence => LiveFrontierError::Precedence,
        LiveFrontierTransitionError::RecordPosition
        | LiveFrontierTransitionError::Exhausted
        | LiveFrontierTransitionError::ResultingFrontier => LiveFrontierError::Frontier,
    }
}

fn failure<T, U>(
    owner: LiveFrontierOwner,
    operation: T,
    error: LiveFrontierError,
) -> Result<U, Box<LiveFrontierFailure<T>>> {
    Err(Box::new(LiveFrontierFailure {
        error,
        operation,
        owner,
    }))
}
