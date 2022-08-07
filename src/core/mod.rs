 use crate::KvsError;

pub mod kv;

/// Result type for kvs
pub type Result<T> = std::result::Result<T, KvsError>;