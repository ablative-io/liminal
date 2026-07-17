#![no_std]
#![deny(missing_docs)]

//! Shared wire, algebra, lifecycle, and outcome rules for conversation
//! participants.

extern crate alloc;

#[cfg(test)]
extern crate std;

pub mod algebra;
pub mod client;
pub mod lifecycle;
pub mod outcome;
pub mod wire;
