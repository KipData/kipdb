use std::path::PathBuf;
use std::sync::Arc;
use sled::Db;
use async_trait::async_trait;
use crate::kernel::KVStore;
use crate::KernelError;

#[derive(Debug)]
pub struct SledStore {
    data_base: Arc<Db>
}

#[async_trait]
impl KVStore for SledStore {

    #[inline]
    fn name() -> &'static str where Self: Sized {
        "Sled made in spacejam"
    }

    #[inline]
    async fn open(path: impl Into<PathBuf> + Send) -> crate::kernel::Result<Self> {
        let db = Arc::new(sled::open(path.into())?);

        Ok(SledStore {
            data_base: db
        })
    }

    #[inline]
    async fn flush(&self) -> crate::kernel::Result<()> {
        let _ignore = self.data_base.flush()?;
        Ok(())
    }

    #[inline]
    async fn set(&self, key: &[u8], value: Vec<u8>) -> crate::kernel::Result<()> {
        let _ignore = self.data_base.insert(key, value)?;
        Ok(())
    }

    #[inline]
    async fn get(&self, key: &[u8]) -> crate::kernel::Result<Option<Vec<u8>>> {
        match self.data_base.get(key)? {
            None => { Ok(None) }
            Some(i_vec) => {
                Ok(Some(i_vec.to_vec()))
            }
        }
    }

    #[inline]
    async fn remove(&self, key: &[u8]) -> crate::kernel::Result<()> {
        match self.data_base.remove(key) {
            Ok(Some(_)) => { Ok(()) }
            Ok(None) => { Err(KernelError::KeyNotFound) }
            Err(e) => { Err(KernelError::Sled(e)) }
        }
    }

    #[inline]
    async fn size_of_disk(&self) -> crate::kernel::Result<u64> {
        Ok(self.data_base.size_on_disk()?)
    }

    #[inline]
    async fn len(&self) -> crate::kernel::Result<usize> {
        Ok(self.data_base.len())
    }

    #[inline]
    async fn is_empty(&self) -> bool {
        self.data_base.is_empty()
    }
}