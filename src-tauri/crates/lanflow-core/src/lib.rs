//! Tauri-independent application core. UI adapters provide progress callbacks;
//! file data stays in native Rust code.

pub mod auth;
pub mod client;
pub mod db;
pub mod discovery;
pub mod error;
pub mod fileops;
pub mod models;
pub mod server;
pub mod tasks;

pub use error::{LanFlowError, Result};
