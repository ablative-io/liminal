#![no_std]
#![deny(missing_docs)]

//! Shared wire, algebra, lifecycle, and outcome rules for conversation
//! participants.

extern crate alloc;

pub mod algebra;
pub mod lifecycle;
pub mod outcome;
pub mod wire;
