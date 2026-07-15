//! Typed participant lifecycle state and transitions.
//!
//! The deviations here are mandated by `docs/design/LP-EXTRACTION-GOAL.md`:
//! detach cells retain a terminalized token after attach so the old binding
//! epoch remains producible, and cursor-progress facts are keyed per
//! participant rather than stored in the contract's fixed occurrence array.
//! The array cannot represent two participants advancing over the same retained
//! suffix, so completion ordering is instead enforced by typed transitions and
//! tests.
