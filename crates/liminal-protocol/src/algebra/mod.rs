//! Pure bounded-retention and floor-transition algebra.

mod capacity;
#[cfg(test)]
mod capacity_tests;
mod floor;
#[cfg(test)]
mod floor_tests;
mod types;

pub use capacity::{
    mandatory_capacity, no_edge_legal, recovery_transfer, retained_baseline, zero_debt_admission,
    zero_debt_capacity_failure,
};
pub use floor::{
    AdmissibleFloor, admissible_installed_floor, floor_transition, marker_clamped_floor,
};
pub use types::{
    BaselineError, FloorComputation, MandatoryCapacity, RecoveryTransfer, RecoveryTransferError,
    ResourceDimension, ResourceVector, WideResourceVector,
};
