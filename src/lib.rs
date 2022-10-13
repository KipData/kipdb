pub mod kernel;
pub mod error;
pub mod config;
pub mod net;
pub mod cmd;

pub use crate::kernel::hash_kv::HashStore;
pub use error::{KvsError};

pub const DEFAULT_PORT: u16 = 6333;

pub const LOCAL_IP: &str = "127.0.0.1";