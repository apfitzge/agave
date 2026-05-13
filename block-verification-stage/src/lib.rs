#![cfg(feature = "agave-unstable-api")]
#![allow(clippy::arithmetic_side_effects)]

//! Block verification stage.

mod entry_hash_verifier;

pub mod scheduler;
pub mod setup;
