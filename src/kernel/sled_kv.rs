use std::path::PathBuf;
use std::sync::Arc;
use sled::Db;
use async_trait::async_trait;
use crate::kernel::KVStore;
use crate::KvsError;

pub struct SledStore {
    data_base: Arc<Db>
}

#[async_trait]
impl KVStore for SledStore {

    fn name() -> &'static str where Self: Sized {
        "Sled made in spacejam"
    }

    async fn open(path: impl Into<PathBuf> + Send) -> crate::kernel::Result<Self> {
        let db = Arc::new(sled::open(&path.into())?);

        Ok(SledStore {
            data_base: db
        })
    }

    async fn flush(&self) -> crate::kernel::Result<()> {
        self.data_base.flush()?;
        Ok(())
    }

    async fn set(&self, key: &Vec<u8>, value: Vec<u8>) -> crate::kernel::Result<()> {
        self.data_base.insert(key, value)?;
        Ok(())
    }

    async fn get(&self, key: &Vec<u8>) -> crate::kernel::Result<Option<Vec<u8>>> {
        match self.data_base.get(key)? {
            None => { Ok(None) }
            Some(i_vec) => {
                Ok(Some(i_vec.to_vec()))
            }
        }
    }

    async fn remove(&self, key: &Vec<u8>) -> crate::kernel::Result<()> {
        match self.data_base.remove(key) {
            Ok(Some(_)) => { Ok(()) }
            Ok(None) => { Err(KvsError::KeyNotFound) }
            Err(e) => { Err(KvsError::Sled(e)) }
        }
    }

    async fn size_of_disk(&self) -> crate::kernel::Result<u64> {
        Ok(self.data_base.size_on_disk()?)
    }

    async fn len(&self) -> crate::kernel::Result<usize> {
        Ok(self.data_base.len())
    }
}