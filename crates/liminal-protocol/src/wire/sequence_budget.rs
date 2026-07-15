/// Canonical ten-field conversation sequence budget.
///
/// The first seven fields are `u64`; the three checked-product fields are
/// `u128`. No alternate or optional exhaustion field exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SequenceBudget {
    /// Greatest allocated delivery sequence.
    pub high_watermark: u64,
    /// Remaining allocatable delivery values.
    pub remaining: u64,
    /// Exit (`E`) claims.
    pub e: u64,
    /// Binding terminal (`T`) claims.
    pub t: u64,
    /// Marker (`M`) claims.
    pub m: u64,
    /// Recovery attach sequence (`RS`) claims.
    pub rs: u64,
    /// Replacement terminal (`RT`) claims.
    pub rt: u64,
    /// Checked `L × T` product.
    pub l_times_t: u128,
    /// Checked `L × RT` product.
    pub l_times_rt: u128,
    /// Checked `L_other × E` product.
    pub l_other_times_e: u128,
}
