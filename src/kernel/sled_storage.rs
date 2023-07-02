use crate::kernel::Storage;
use crate::KernelError;
use async_trait::async_trait;
use bytes::Bytes;
use sled::Db;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug)]
pub struct SledStorage {
    data_base: Arc<Db>,
}

#[async_trait]
impl Storage for SledStorage {
    #[inline]
    fn name() -> &'static str
    where
        Self: Sized,
    {
        "Sled made in spacejam"
    }

    #[inline]
    async fn open(path: impl Into<PathBuf> + Send) -> crate::kernel::Result<Self> {
        let db = Arc::new(sled::open(path.into())?);

        Ok(SledStorage { data_base: db })
    }

    #[inline]
    async fn flush(&self) -> crate::kernel::Result<()> {
        let _ignore = self.data_base.flush()?;
        Ok(())
    }

    #[inline]
    async fn set(&self, key: &[u8], value: Bytes) -> crate::kernel::Result<()> {
        let _ignore = self.data_base.insert(key, value.to_vec())?;
        Ok(())
    }

    #[inline]
    async fn get(&self, key: &[u8]) -> crate::kernel::Result<Option<Bytes>> {
        match self.data_base.get(key)? {
            None => Ok(None),
            Some(i_vec) => Ok(Some(Bytes::from(i_vec.to_vec()))),
        }
    }

    #[inline]
    async fn remove(&self, key: &[u8]) -> crate::kernel::Result<()> {
        match self.data_base.remove(key) {
            Ok(Some(_)) => Ok(()),
            Ok(None) => Err(KernelError::KeyNotFound),
            Err(e) => Err(KernelError::SledErr(e)),
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
