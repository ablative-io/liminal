use crate::{
    algebra::WideResourceVector,
    lifecycle::{
        AttachedLifecycleRecord, BindingTerminalOwner, ClosureAccounting, CommittedBindingTerminal,
        RetainedCausalRecord, RetainedCausalRecordKind, RetainedRecordCharge,
    },
};

#[derive(Clone, Copy)]
struct MarkerAccounting {
    credits: u64,
    anchors: u64,
}

pub(super) fn accounting_after_rows(
    accounting: ClosureAccounting,
    charges: &[RetainedRecordCharge],
) -> Option<ClosureAccounting> {
    accounting_after_rows_with_state(
        accounting,
        charges,
        accounting.state(),
        MarkerAccounting {
            credits: accounting.marker_capacity_credits(),
            anchors: accounting.marker_anchors(),
        },
        accounting.edge_sequence_claims(),
        accounting.edge_order_position_claims(),
        accounting.edge_k_remaining(),
    )
}

pub(super) fn accounting_after_marker_ack(
    accounting: ClosureAccounting,
) -> Option<ClosureAccounting> {
    accounting_after_rows_with_state(
        accounting,
        &[],
        accounting.state(),
        MarkerAccounting {
            credits: accounting.marker_capacity_credits(),
            anchors: accounting.marker_anchors().checked_sub(1)?,
        },
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
        MarkerAccounting {
            credits: accounting.marker_capacity_credits(),
            anchors: accounting.marker_anchors().checked_sub(1)?,
        },
        0,
        0,
        crate::algebra::ResourceVector::default(),
    )
}

pub(super) fn accounting_after_leave(
    accounting: ClosureAccounting,
    charges: &[RetainedRecordCharge],
    retired_marker_charge: Option<RetainedRecordCharge>,
) -> Option<ClosureAccounting> {
    let marker_released = u64::from(retired_marker_charge.is_some());
    let mut charges = charges.to_vec();
    if let Some(charge) = retired_marker_charge {
        charges.push(charge);
    }
    accounting_after_rows_with_state(
        accounting,
        &charges,
        accounting.state(),
        MarkerAccounting {
            credits: accounting
                .marker_capacity_credits()
                .checked_sub(marker_released)?,
            anchors: accounting.marker_anchors().checked_sub(marker_released)?,
        },
        accounting.edge_sequence_claims(),
        accounting.edge_order_position_claims(),
        accounting.edge_k_remaining(),
    )
}

fn accounting_after_rows_with_state(
    accounting: ClosureAccounting,
    charges: &[RetainedRecordCharge],
    state: crate::lifecycle::ClosureState,
    marker: MarkerAccounting,
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
        marker.credits,
        marker.anchors,
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
