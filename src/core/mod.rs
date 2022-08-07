 use crate::KvsError;

pub mod kv;

pub type Result<T> = std::result::Result<T, KvsError>;