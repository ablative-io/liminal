#![allow(clippy::expect_used, clippy::panic)]

use crate::algebra::{ResourceDimension, ResourceVector, WideResourceVector};
use crate::wire::{
    ClosureCapacityReason, ClosureCheckedEnvelope, ClosureRefusalReason, EnrollmentEnvelope,
    EnrollmentToken, RepaymentEdge,
};

use super::closure_accounting::{
    ClosureAccounting, ClosureAccountingError, RecoveryFenceDecision, RemainingClosureDecision,
    RequiredCapacityPlan, RequiredCapacityPlanError, check_recovery_fence, check_remaining_closure,
};
use super::{ClosureDebt, ClosureState, ObserverProjection, StoredEdge};

fn request() -> ClosureCheckedEnvelope {
    ClosureCheckedEnvelope::Enrollment(EnrollmentEnvelope {
        conversation_id: 91,
        enrollment_token: EnrollmentToken::new([0x91; 16]),
    })
}

fn accounting(state: ClosureState) -> ClosureAccounting {
    let owed = matches!(state, ClosureState::Owed { .. });
    ClosureAccounting::try_new(
        state,
        u64::from(owed),
        u64::from(owed),
        u64::from(owed) * 4,
        u64::from(owed) * 3,
        if owed {
            ResourceVector::new(2, 20)
        } else {
            ResourceVector::default()
        },
        WideResourceVector::new(5, 50),
        ResourceVector::new(10, 100),
        1,
        3,
    )
    .expect("fixture accounting is valid")
}

fn owed() -> ClosureState {
    ClosureState::Owed {
        debt: ClosureDebt::new(WideResourceVector::new(1, 7)).expect("fixture debt is nonzero"),
        edge: StoredEdge::ObserverProjection(ObserverProjection::new(12)),
    }
}

#[test]
fn restore_rejects_mixed_clear_state_and_unencodable_or_impossible_values() {
    let mixed = ClosureAccounting::try_new(
        ClosureState::Clear,
        0,
        0,
        1,
        0,
        ResourceVector::default(),
        WideResourceVector::new(0, 0),
        ResourceVector::new(1, 1),
        0,
        1,
    );
    assert_eq!(
        mixed,
        Err(ClosureAccountingError::ClearStateOwnsEdgeResources)
    );

    let anchors = ClosureAccounting::try_new(
        ClosureState::Clear,
        1,
        2,
        0,
        0,
        ResourceVector::default(),
        WideResourceVector::new(0, 0),
        ResourceVector::new(1, 1),
        0,
        1,
    );
    assert_eq!(
        anchors,
        Err(ClosureAccountingError::MarkerAnchorsExceedCredits {
            anchors: 2,
            credits: 1,
        })
    );

    let huge_debt = ClosureState::Owed {
        debt: ClosureDebt::new(WideResourceVector::new(u128::from(u64::MAX) + 1, 0))
            .expect("entry debt is nonzero"),
        edge: StoredEdge::ObserverProjection(ObserverProjection::new(1)),
    };
    let result = ClosureAccounting::try_new(
        huge_debt,
        0,
        0,
        1,
        1,
        ResourceVector::default(),
        WideResourceVector::new(0, 0),
        ResourceVector::new(1, 1),
        0,
        1,
    );
    assert_eq!(
        result,
        Err(ClosureAccountingError::DebtOutsideWireDomain {
            dimension: ResourceDimension::Entries,
        })
    );
}

#[test]
fn recovery_fence_uses_unchanged_state_and_zero_delta_before_numeric_checks() {
    let current = accounting(owed());
    let decision = check_recovery_fence(&request(), current, true);
    let RecoveryFenceDecision::Respond(refusal) = decision else {
        panic!("recovery fence must refuse");
    };
    assert_eq!(refusal.reason, ClosureRefusalReason::RecoveryFence);
    assert_eq!(refusal.snapshot.entry_debt, 1);
    assert_eq!(refusal.snapshot.byte_debt, 7);
    assert_eq!(refusal.snapshot.delta_cycles, 0);
    assert_eq!(
        refusal.snapshot.repayment_edge,
        RepaymentEdge::ObserverProjection { through_seq: 12 }
    );
    assert_eq!(refusal.snapshot.k_headroom, WideResourceVector::new(5, 50));
    assert_eq!(current, accounting(owed()));
}

#[test]
fn remaining_precedence_is_delivered_then_churn_then_entries_then_bytes() {
    let current = accounting(ClosureState::Clear);
    let required = RequiredCapacityPlan::from_successors(&[
        WideResourceVector::new(11, 101),
        WideResourceVector::new(12, 102),
    ])
    .expect("two successors are nonempty");

    let delivered = check_remaining_closure(&request(), current, true, 9, required);
    let RemainingClosureDecision::Respond(delivered) = delivered else {
        panic!("delivered marker wins");
    };
    assert_eq!(
        delivered.reason,
        ClosureRefusalReason::DeliveredMarkerAwaitingAck
    );
    assert_eq!(delivered.snapshot.delta_cycles, 0);

    let churn = check_remaining_closure(&request(), current, false, 3, required);
    let RemainingClosureDecision::Respond(churn) = churn else {
        panic!("churn wins before capacity");
    };
    assert_eq!(churn.reason, ClosureRefusalReason::EpisodeChurnLimit);
    assert_eq!(churn.snapshot.delta_cycles, 3);

    let entries = check_remaining_closure(&request(), current, false, 2, required);
    let RemainingClosureDecision::Respond(entries) = entries else {
        panic!("entries fail first");
    };
    assert_eq!(
        entries.reason,
        ClosureRefusalReason::Capacity(ClosureCapacityReason {
            dimension: ResourceDimension::Entries,
            required: 12,
            limit: 10,
        })
    );

    let bytes_only = RequiredCapacityPlan::from_successors(&[WideResourceVector::new(10, 101)])
        .expect("one successor is nonempty");
    let bytes = check_remaining_closure(&request(), current, false, 2, bytes_only);
    let RemainingClosureDecision::Respond(bytes) = bytes else {
        panic!("bytes fail after entries pass");
    };
    assert_eq!(
        bytes.reason,
        ClosureRefusalReason::Capacity(ClosureCapacityReason {
            dimension: ResourceDimension::Bytes,
            required: 101,
            limit: 100,
        })
    );
}

#[test]
fn equality_passes_with_componentwise_successor_maximum_and_exact_cycle_charge() {
    let current = accounting(ClosureState::Clear);
    let plan = RequiredCapacityPlan::from_successors(&[
        WideResourceVector::new(10, 90),
        WideResourceVector::new(8, 100),
    ])
    .expect("two successors are nonempty");
    assert_eq!(plan.maximum(), WideResourceVector::new(10, 100));

    let decision = check_remaining_closure(&request(), current, false, 2, plan);
    let RemainingClosureDecision::Eligible(permit) = decision else {
        panic!("capacity and churn equality pass");
    };
    assert_eq!(permit.accounting(), current);
    assert_eq!(permit.required_capacity(), plan);
    assert_eq!(permit.delta_cycles(), 2);
}

#[test]
fn ordinary_required_capacity_uses_canonical_widened_addition_order() {
    let plan = RequiredCapacityPlan::ordinary(
        WideResourceVector::new(4, 40),
        ResourceVector::new(2, 20),
        ResourceVector::new(1, 10),
    )
    .expect("fixture additions fit");
    assert_eq!(plan.maximum(), WideResourceVector::new(7, 70));
    assert_eq!(
        RequiredCapacityPlan::from_successors(&[]),
        Err(RequiredCapacityPlanError::EmptySuccessorSet)
    );
    assert_eq!(
        RequiredCapacityPlan::ordinary(
            WideResourceVector::new(u128::MAX, 0),
            ResourceVector::new(1, 0),
            ResourceVector::default(),
        ),
        Err(RequiredCapacityPlanError::ArithmeticOverflow {
            dimension: ResourceDimension::Entries,
        })
    );
}
