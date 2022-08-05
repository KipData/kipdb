pub mod core;
pub mod error;
pub mod config;
pub mod net;
pub mod cmd;

use std::error::Error;
pub use crate::core::kv::KvStore;
pub use error::{KvsError};

type Result<T> = std::result::Result<T, dyn Error>;