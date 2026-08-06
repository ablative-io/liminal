use super::types::{FloorComputation, widen_u64};

/// What a binding-fate floor measured earlier is allowed to install NOW.
///
/// A floor is measured at one moment and installed at another. Between them the
/// frontier moves, so the measured value is a PROPOSAL and this is the verdict
/// on it. Returned by [`admissible_installed_floor`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissibleFloor {
    /// Install exactly this floor. It is guaranteed to sit inside the interval
    /// the installing enforcer accepts, so installing it cannot be refused.
    Install(u128),
    /// The current retained floor is already at or past what this measurement
    /// could legally install, so the measured floor is SUBSUMED and installing
    /// it would mean nothing. An explicit success with no effect — never a
    /// refusal, and never an install that drives the floor backwards.
    Subsumed,
}

/// Lowers a computed floor so it cannot cross the lowest retained marker.
///
/// ⚠ THIS LOWERS. [`floor_transition`]'s `cap_floor` argument RAISES: it is
/// `max(base_result, cap_floor)`, a floor-**raiser** despite the name. Passing
/// the lowest marker through `cap_floor` yields `max(base, marker)`, which
/// still sits above the marker in exactly the poisoning case and would raise
/// floors in cases that work today. The two are opposites; do not substitute
/// one for the other.
///
/// The clamp target is the marker ITSELF, never `marker - 1`. The enforcers
/// refuse on `record.delivery_seq < resulting_floor` — strictly below — so
/// `resulting_floor == marker` is admissible, and it stays consistent
/// downstream because floor installation retains markers `>= resulting_floor`,
/// so a marker sitting exactly at the floor survives its own pin. Clamping to
/// just below the marker is the defensive reflex and it silently destroys legal
/// floor advances.
///
/// An empty marker set is the majority case and is answered explicitly rather
/// than left to a guess about the minimum of an empty set: nothing pins the
/// floor, so the computed floor passes through byte-identical.
#[must_use]
pub const fn marker_clamped_floor(
    computed_floor: u128,
    lowest_retained_marker_seq: Option<u64>,
) -> u128 {
    match lowest_retained_marker_seq {
        Some(marker) => {
            let marker = widen_u64(marker);
            if computed_floor < marker {
                computed_floor
            } else {
                marker
            }
        }
        None => computed_floor,
    }
}

/// Decides what an earlier-measured binding-fate floor may install against the
/// frontier as it stands now.
///
/// The installing enforcer refuses on TWO conditions, and clamping downward
/// against markers only bounds one of them: a floor clamped down can land BELOW
/// the current retained floor and be refused for that instead — the same
/// permanent refusal under a different name. So the admissible interval is
///
/// > `[retained_floor, min(lowest_retained_marker_seq, high_watermark + 1)]`
///
/// with **both ends read now, not at measurement time**: the upper end moves
/// too, because it derives from the current high watermark.
///
/// Monotonicity is safe rather than assumed. Floor installation retains only
/// markers `>= resulting_floor`, so the lowest retained marker is never below
/// the retained floor, and the marker clamp can therefore never on its own
/// drive a floor backwards. When the interval is nonetheless empty — which
/// requires a frontier that has already broken that invariant — the answer is
/// [`AdmissibleFloor::Subsumed`], i.e. install nothing, because installing
/// anything would prune rows the frontier still owes.
#[must_use]
pub const fn admissible_installed_floor(
    measured_floor: u128,
    retained_floor: u128,
    lowest_retained_marker_seq: Option<u64>,
    high_watermark: u64,
) -> AdmissibleFloor {
    let retained_end = widen_u64(high_watermark) + 1;
    let upper = marker_clamped_floor(retained_end, lowest_retained_marker_seq);
    let target = if measured_floor < upper {
        measured_floor
    } else {
        upper
    };
    if target < retained_floor {
        AdmissibleFloor::Subsumed
    } else {
        AdmissibleFloor::Install(target)
    }
}

/// Computes the participant physical-floor rule.
///
/// `minimum_member_cursor` is evaluated after membership changes. When it is
/// `None`, the rule substitutes the candidate high watermark `H'` for `m`.
/// Floors use `u128` so checked one-past-`u64::MAX` remains representable.
#[must_use]
pub const fn floor_transition(
    current_floor: u128,
    minimum_member_cursor: Option<u64>,
    candidate_high_watermark: u64,
    observer_progress: u64,
    cap_floor: u128,
) -> FloorComputation {
    let member_cursor = match minimum_member_cursor {
        Some(cursor) => cursor,
        None => candidate_high_watermark,
    };
    let preferred_floor = if member_cursor < observer_progress {
        widen_u64(member_cursor) + 1
    } else {
        widen_u64(observer_progress) + 1
    };
    let base_result = if current_floor > preferred_floor {
        current_floor
    } else {
        preferred_floor
    };
    let resulting_floor = if base_result > cap_floor {
        base_result
    } else {
        cap_floor
    };

    FloorComputation {
        member_cursor,
        preferred_floor,
        resulting_floor,
    }
}
