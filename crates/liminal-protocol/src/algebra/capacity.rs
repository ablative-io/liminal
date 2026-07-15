use super::types::{
    BaselineError, MandatoryCapacity, RecoveryTransfer, RecoveryTransferError, ResourceDimension,
    ResourceVector, WideResourceVector, widen_u64,
};

/// Computes `B = S + ((I - C) × marker_max)` componentwise.
///
/// Every operand is widened to `u128` before subtraction, multiplication, or
/// addition. This keeps the printed suboperation order without importing the
/// occurrence-array machinery excluded by `docs/design/LP-EXTRACTION-GOAL.md`.
///
/// # Errors
///
/// Returns [`BaselineError`] when `C > I`.
pub const fn retained_baseline(
    retained_charge: ResourceVector,
    identity_slots: u64,
    marker_credits: u64,
    marker_max: ResourceVector,
) -> Result<WideResourceVector, BaselineError> {
    if marker_credits > identity_slots {
        return Err(BaselineError::MarkerCreditsExceedIdentitySlots {
            identity_slots,
            marker_credits,
        });
    }

    let uncredited_slots = widen_u64(identity_slots) - widen_u64(marker_credits);
    let marker_entries = uncredited_slots * widen_u64(marker_max.entries);
    let marker_bytes = uncredited_slots * widen_u64(marker_max.bytes);
    Ok(WideResourceVector::new(
        widen_u64(retained_charge.entries) + marker_entries,
        widen_u64(retained_charge.bytes) + marker_bytes,
    ))
}

/// Returns the first failed component of `B + Q + K <= cap`.
///
/// Entries are checked before bytes, matching the contract's refusal
/// precedence. An arithmetic overflow is a failure of that component.
#[must_use]
pub const fn zero_debt_capacity_failure(
    baseline: WideResourceVector,
    mandatory_bound: ResourceVector,
    recovery_claim: ResourceVector,
    configured_cap: ResourceVector,
) -> Option<ResourceDimension> {
    if !component_fits(
        baseline.entries,
        mandatory_bound.entries,
        recovery_claim.entries,
        configured_cap.entries,
    ) {
        return Some(ResourceDimension::Entries);
    }
    if !component_fits(
        baseline.bytes,
        mandatory_bound.bytes,
        recovery_claim.bytes,
        configured_cap.bytes,
    ) {
        return Some(ResourceDimension::Bytes);
    }
    None
}

/// Checks the zero-debt ordinary-admission invariant `B + Q + K <= cap`.
#[must_use]
pub const fn zero_debt_admission(
    baseline: WideResourceVector,
    mandatory_bound: ResourceVector,
    recovery_claim: ResourceVector,
    configured_cap: ResourceVector,
) -> bool {
    zero_debt_capacity_failure(baseline, mandatory_bound, recovery_claim, configured_cap).is_none()
}

/// Computes the mandatory-class debt and its two required checks.
///
/// The result contains
/// `d' = max(0, B' + Q + K_remaining' - cap)`, the absolute-fit check
/// `B' + K_remaining' <= cap`, and the debt bound `d' <= Q`.
#[must_use]
pub const fn mandatory_capacity(
    resulting_baseline: ResourceVector,
    mandatory_bound: ResourceVector,
    remaining_recovery_claim: ResourceVector,
    configured_cap: ResourceVector,
) -> MandatoryCapacity {
    let entries = mandatory_component(
        resulting_baseline.entries,
        mandatory_bound.entries,
        remaining_recovery_claim.entries,
        configured_cap.entries,
    );
    let bytes = mandatory_component(
        resulting_baseline.bytes,
        mandatory_bound.bytes,
        remaining_recovery_claim.bytes,
        configured_cap.bytes,
    );

    MandatoryCapacity {
        debt: WideResourceVector::new(entries.debt, bytes.debt),
        absolute_fit: entries.absolute_fit && bytes.absolute_fit,
        debt_within_mandatory_bound: entries.debt_within_bound && bytes.debt_within_bound,
    }
}

/// Transfers an exact recovery record charge from `K_remaining` into `B`.
///
/// The candidate is counted once: `B' = B_removed + r` and
/// `K_remaining' = K_remaining - r`.
///
/// # Errors
///
/// Returns [`RecoveryTransferError`] for the first component in which `r`
/// exceeds `K_remaining`.
pub const fn recovery_transfer(
    baseline_after_removals: ResourceVector,
    remaining_recovery_claim: ResourceVector,
    charge: ResourceVector,
) -> Result<RecoveryTransfer, RecoveryTransferError> {
    if charge.entries > remaining_recovery_claim.entries {
        return Err(RecoveryTransferError {
            dimension: ResourceDimension::Entries,
        });
    }
    if charge.bytes > remaining_recovery_claim.bytes {
        return Err(RecoveryTransferError {
            dimension: ResourceDimension::Bytes,
        });
    }

    Ok(RecoveryTransfer {
        baseline: WideResourceVector::new(
            widen_u64(baseline_after_removals.entries) + widen_u64(charge.entries),
            widen_u64(baseline_after_removals.bytes) + widen_u64(charge.bytes),
        ),
        remaining_recovery_claim: ResourceVector::new(
            remaining_recovery_claim.entries - charge.entries,
            remaining_recovery_claim.bytes - charge.bytes,
        ),
    })
}

/// Checks the only legal no-edge state: zero debt plus full-K fit.
#[must_use]
pub const fn no_edge_legal(
    debt: WideResourceVector,
    baseline: WideResourceVector,
    mandatory_bound: ResourceVector,
    full_recovery_claim: ResourceVector,
    configured_cap: ResourceVector,
) -> bool {
    debt.is_zero()
        && zero_debt_admission(
            baseline,
            mandatory_bound,
            full_recovery_claim,
            configured_cap,
        )
}

const fn component_fits(baseline: u128, q: u64, k: u64, cap: u64) -> bool {
    let Some(with_q) = baseline.checked_add(widen_u64(q)) else {
        return false;
    };
    let Some(required) = with_q.checked_add(widen_u64(k)) else {
        return false;
    };
    required <= widen_u64(cap)
}

struct MandatoryComponent {
    debt: u128,
    absolute_fit: bool,
    debt_within_bound: bool,
}

const fn mandatory_component(baseline: u64, q: u64, k: u64, cap: u64) -> MandatoryComponent {
    let baseline = widen_u64(baseline);
    let q = widen_u64(q);
    let k = widen_u64(k);
    let cap = widen_u64(cap);
    let debt = (baseline + q + k).saturating_sub(cap);
    MandatoryComponent {
        debt,
        absolute_fit: baseline + k <= cap,
        debt_within_bound: debt <= q,
    }
}
