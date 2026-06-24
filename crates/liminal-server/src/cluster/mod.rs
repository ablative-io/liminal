//! SRV-005: clustering via beamr distribution with seed-node discovery.
//!
//! Multiple liminal-server instances form a single logical message bus by
//! joining a beamr distribution cluster: nodes discover each other from
//! configured seed addresses ([`discovery`]), track membership by polling the
//! connection table ([`membership`]), and propagate channel subscriptions plus
//! published messages across nodes through process groups ([`sync`]). All
//! cluster behaviour delegates to beamr distribution primitives — there is no
//! custom consensus, gossip, or failure detector here.

pub mod discovery;
pub mod membership;
pub mod sync;

pub use discovery::{ClusterResolver, SeedConnectOutcome};
pub use membership::{ClusterHandle, Membership, MembershipDelta, start};
pub use sync::ClusterSync;
