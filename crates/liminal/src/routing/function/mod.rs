//! Routing function loading and supervised execution (ADR-003).
//!
//! This module is the boundary that wraps the beamr runtime for the flexible
//! tier of the two-tier routing model. Routing functions are content-addressed
//! modules: they are loaded by content hash (so re-loading identical bytecode
//! reuses the already-loaded module) and executed in supervised, isolated
//! processes. A panic or runaway loop in one routing function is contained by
//! the supervisor and surfaced to the caller as an error, never propagated to
//! the evaluation pipeline of other channels.
//!
//! Beamr types are intentionally not exposed here; callers interact only with
//! the wrapper types defined in this module. The [`loader`] submodule wraps
//! content-hash module loading and hot deployment; the [`execute`] submodule
//! wraps supervised, isolated execution.

pub mod execute;
pub mod loader;

pub use execute::{
    ConsumerId, ConsumerStateView, FunctionError, RoutingDecision, RoutingMessage,
    SupervisedExecutor,
};
pub use loader::{ContentHash, ModuleLoader, RoutingFunction, RoutingModule, RoutingSlot};
