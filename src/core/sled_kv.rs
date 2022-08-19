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

    fn set(&mut self, key: &Vec<u8>, value: Vec<u8>) -> crate::core::Result<()> {
        self.data_base.insert(key, value)?;
        Ok(())
    }

    fn get(&self, key: &Vec<u8>) -> crate::core::Result<Option<Vec<u8>>> {
        match self.data_base.get(key)? {
            None => { Ok(None) }
            Some(i_vec) => {
                Ok(Some(i_vec.to_vec()))
            }
        }
    }

    fn remove(&mut self, key: &Vec<u8>) -> crate::core::Result<()> {
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