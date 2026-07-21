//! Wire-only crate: stable frame encoding, version negotiation constants and
//! generated Prost control messages.

pub mod error;
pub mod frame;
pub mod protocol;

pub use error::{ProtocolError, Result};
