use std::path::PathBuf;
use crate::KvsError;

pub mod hash_kv;

pub type Result<T> = std::result::Result<T, KvsError>;

pub trait KVStore {
    fn open(path: impl Into<PathBuf>) -> Result<Self> where Self:Sized ;

    fn set(&mut self, key: String, value: String) -> Result<()>;

    fn get(&self, key: String) -> Result<Option<String>>;

    fn remove(&mut self, key: String) -> Result<()>;
}