//! Checked allocator advancement for one connection-fate source row.

use super::state::{ConversationAuthority, StateError};

pub(super) struct FateAllocations {
    pub(super) source_sequence: u64,
    pub(super) next_order: u64,
    pub(super) next_sequence: u64,
    pub(super) next_log_sequence: u64,
}

pub(super) fn checked_fate_allocations(
    authority: &ConversationAuthority,
) -> Result<FateAllocations, StateError> {
    let next_order =
        authority
            .next_order
            .checked_add(1)
            .ok_or(StateError::AllocationExhausted {
                domain: "transaction order",
            })?;
    let next_sequence =
        authority
            .next_seq
            .checked_add(1)
            .ok_or(StateError::AllocationExhausted {
                domain: "delivery sequence",
            })?;
    let next_log_sequence =
        authority
            .next_log_sequence
            .checked_add(1)
            .ok_or(StateError::AllocationExhausted {
                domain: "log sequence",
            })?;
    Ok(FateAllocations {
        source_sequence: authority.next_log_sequence,
        next_order,
        next_sequence,
        next_log_sequence,
    })
}
