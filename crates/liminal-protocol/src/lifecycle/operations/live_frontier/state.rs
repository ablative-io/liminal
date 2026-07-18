use crate::{
    algebra::WideResourceVector,
    lifecycle::{
        AttachedLifecycleRecord, BindingTerminalOwner, ClosureAccounting, CommittedBindingTerminal,
        RetainedCausalRecord, RetainedCausalRecordKind, RetainedRecordCharge,
    },
};

pub(super) fn accounting_after_rows(
    accounting: ClosureAccounting,
    charges: &[RetainedRecordCharge],
) -> Option<ClosureAccounting> {
    accounting_after_rows_with_state(
        accounting,
        charges,
        accounting.state(),
        accounting.marker_anchors(),
        accounting.edge_sequence_claims(),
        accounting.edge_order_position_claims(),
        accounting.edge_k_remaining(),
    )
}

pub(super) fn accounting_after_fenced_attach(
    accounting: ClosureAccounting,
    charges: &[RetainedRecordCharge],
    next_state: crate::lifecycle::ClosureState,
) -> Option<ClosureAccounting> {
    accounting_after_rows_with_state(
        accounting,
        charges,
        next_state,
        accounting.marker_anchors().checked_sub(1)?,
        0,
        0,
        crate::algebra::ResourceVector::default(),
    )
}

fn accounting_after_rows_with_state(
    accounting: ClosureAccounting,
    charges: &[RetainedRecordCharge],
    state: crate::lifecycle::ClosureState,
    marker_anchors: u64,
    edge_sequence_claims: u64,
    edge_order_position_claims: u64,
    edge_k_remaining: crate::algebra::ResourceVector,
) -> Option<ClosureAccounting> {
    let baseline = charges
        .iter()
        .try_fold(accounting.baseline(), |current, charge| {
            let charge = charge.encoded_charge();
            Some(WideResourceVector::new(
                current.entries.checked_add(u128::from(charge.entries))?,
                current.bytes.checked_add(u128::from(charge.bytes))?,
            ))
        })?;
    ClosureAccounting::try_new(
        state,
        accounting.marker_capacity_credits(),
        marker_anchors,
        edge_sequence_claims,
        edge_order_position_claims,
        edge_k_remaining,
        baseline,
        accounting.configured_cap(),
        accounting.episode_churn_used(),
        accounting.episode_churn_limit(),
    )
    .ok()
}

pub(super) const fn retained_attached(attached: AttachedLifecycleRecord) -> RetainedCausalRecord {
    RetainedCausalRecord {
        delivery_seq: attached.delivery_seq(),
        admission_order: attached.admission_order(),
        kind: RetainedCausalRecordKind::AttachLifecycle {
            participant_index: attached.participant_id(),
            binding_epoch: attached.binding_epoch(),
        },
    }
}

pub(super) const fn retained_terminal(terminal: CommittedBindingTerminal) -> RetainedCausalRecord {
    RetainedCausalRecord {
        delivery_seq: terminal.delivery_seq(),
        admission_order: terminal.admission_order(),
        kind: RetainedCausalRecordKind::BindingTerminal(BindingTerminalOwner {
            participant_index: terminal.participant_id(),
            binding_epoch: terminal.binding_epoch(),
        }),
    }
}
