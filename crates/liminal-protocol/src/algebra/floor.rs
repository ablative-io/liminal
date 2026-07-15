use super::types::{FloorComputation, widen_u64};

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
