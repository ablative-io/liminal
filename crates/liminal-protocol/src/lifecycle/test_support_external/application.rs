use alloc::{
    format,
    string::{String, ToString},
};

use crate::{
    algebra::{ResourceVector, WideResourceVector},
    outcome::CandidatePhase,
};

use super::{
    AdmissionOrder, AttachCommit, AttachFrontierCharges, ClosureAccounting, ClosureState,
    DcrClaims, DcrContext, DcrFrontiers, DetachedCredentialRecovery, LiveFrontierOwner,
    OrderDirectOwner, RetainedCausalRecord, RetainedCausalRecordKind, RetainedRecordCharge,
    SequenceDirectOwner, StoredEdge, apply_attach_frontier,
};

pub(super) struct AppliedDcrAttach {
    pub(super) committed: AttachCommit<[u8; 32], [u8; 32]>,
    pub(super) owner: LiveFrontierOwner,
    pub(super) next_terminal_sequence: u64,
    pub(super) next_terminal_order: u64,
}

pub(super) fn frontier_owner(
    context: DcrContext,
    recovery: DetachedCredentialRecovery,
    fixture: DcrFrontiers,
) -> Result<LiveFrontierOwner, String> {
    let accounting = ClosureAccounting::try_new(
        ClosureState::Owed {
            debt: context.debt,
            edge: StoredEdge::DetachedCredentialRecovery(recovery),
        },
        1,
        1,
        2,
        2,
        ResourceVector::new(2, 8),
        WideResourceVector::new(2, 8),
        ResourceVector::new(16, 64),
        1,
        2,
    )
    .map_err(|error| format!("DCR closure accounting failed: {error:?}"))?;
    let retained_charges = fixture
        .restore
        .retained_records
        .iter()
        .map(|row| {
            RetainedRecordCharge::new(
                row.delivery_seq,
                row.admission_order,
                ResourceVector::new(1, 4),
            )
        })
        .collect();
    Ok(LiveFrontierOwner::from_test_parts(
        fixture.frontiers,
        accounting,
        retained_charges,
        3,
    ))
}

fn terminal_claims(owner: &LiveFrontierOwner, context: DcrContext) -> Result<(u64, u64), String> {
    let sequence = owner
        .frontiers()
        .sequence()
        .movable_claims()
        .iter()
        .find_map(|claim| match claim.owner {
            SequenceDirectOwner::BindingTerminal(terminal)
                if terminal.participant_index == context.participant_id
                    && terminal.binding_epoch == context.recovered_binding_epoch =>
            {
                Some(claim.delivery_seq)
            }
            _ => None,
        })
        .ok_or_else(|| "DCR attach omitted its replacement-terminal sequence claim".to_string())?;
    let order = owner
        .frontiers()
        .order()
        .movable_claims()
        .iter()
        .find_map(|claim| match claim.owner {
            OrderDirectOwner::ActiveBindingTerminal(terminal)
                if terminal.participant_index == context.participant_id
                    && terminal.binding_epoch == context.recovered_binding_epoch =>
            {
                Some(claim.transaction_order)
            }
            _ => None,
        })
        .ok_or_else(|| "DCR attach omitted its replacement-terminal order claim".to_string())?;
    Ok((sequence, order))
}

pub(super) fn apply_dcr_attach(
    context: DcrContext,
    claims: DcrClaims,
    recovery: DetachedCredentialRecovery,
    fixture: DcrFrontiers,
    committed: AttachCommit<[u8; 32], [u8; 32]>,
) -> Result<AppliedDcrAttach, String> {
    let owner = frontier_owner(context, recovery, fixture)?;
    let attached = RetainedCausalRecord {
        delivery_seq: claims.recovery_attach_seq,
        admission_order: AdmissionOrder::new(
            claims.recovery_operation_order,
            CandidatePhase::AttachLifecycle,
            context.participant_id,
        ),
        kind: RetainedCausalRecordKind::AttachLifecycle {
            participant_index: context.participant_id,
            binding_epoch: context.recovered_binding_epoch,
        },
    };
    let applied = apply_attach_frontier(
        owner,
        committed,
        AttachFrontierCharges::new(
            None,
            RetainedRecordCharge::new(
                attached.delivery_seq,
                attached.admission_order,
                ResourceVector::new(1, 4),
            ),
        ),
    )
    .map_err(|failure| {
        format!(
            "DCR attach frontier application failed: {:?}",
            failure.error()
        )
    })?;
    let (committed, owner) = applied.into_parts();
    let (next_terminal_sequence, next_terminal_order) = terminal_claims(&owner, context)?;
    Ok(AppliedDcrAttach {
        committed,
        owner,
        next_terminal_sequence,
        next_terminal_order,
    })
}
