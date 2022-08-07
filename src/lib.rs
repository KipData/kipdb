pub mod core;
pub mod error;
pub mod config;
pub mod net;
pub mod cmd;

pub use crate::core::kv::KvStore;
pub use error::{KvsError};

pub const DEFAULT_PORT: u16 = 6379;