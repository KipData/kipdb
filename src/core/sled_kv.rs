use std::path::PathBuf;
use sled::Db;
use crate::core::KVStore;
use crate::KvsError;

pub struct SledStore {
    data_base: Db
}

impl KVStore for SledStore {
    fn name() -> &'static str where Self: Sized {
        "Sled made in spacejam"
    }

    fn open(path: impl Into<PathBuf>) -> crate::core::Result<Self> where Self: Sized {
        let db = sled::open(&path.into())?;

        Ok(SledStore {
            data_base: db
        })
    }

    fn flush(&mut self) -> crate::core::Result<()> {
        self.data_base.flush()?;
        Ok(())
    }

    fn set(&mut self, key: String, value: String) -> crate::core::Result<()> {
        self.data_base.insert(key.as_str(), rmp_serde::encode::to_vec(&value)?)?;
        Ok(())
    }

    fn get(&self, key: String) -> crate::core::Result<Option<String>> {
        match self.data_base.get(key)? {
            None => { Ok(None) }
            Some(vec) => {
                Ok(Some(rmp_serde::decode::from_slice::<String>(&*vec)?))
            }
        }
    }

    fn remove(&mut self, key: String) -> crate::core::Result<()> {
        match self.data_base.remove(key) {
            Ok(Some(_)) => { Ok(()) }
            Ok(None) => { Err(KvsError::KeyNotFound) }
            Err(e) => { Err(KvsError::Sled(e)) }
        }
    }

    fn shut_down(&mut self) -> crate::core::Result<()> {
        self.data_base.flush()?;
        Ok(())
    }
}