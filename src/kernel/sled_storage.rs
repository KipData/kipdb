use crate::kernel::Storage;
use crate::KernelError;
use async_trait::async_trait;
use bytes::Bytes;
use core::slice::SlicePattern;
use sled::Db;
use std::path::PathBuf;

#[derive(Debug)]
pub struct SledStorage {
    data_base: Db,
}

#[async_trait]
impl Storage for SledStorage {
    #[inline]
    fn name() -> &'static str
    where
        Self: Sized,
    {
        "Sled"
    }

    #[inline]
    async fn open(path: impl Into<PathBuf> + Send) -> crate::kernel::KernelResult<Self> {
        let db = sled::open(path.into())?;

        Ok(SledStorage { data_base: db })
    }

    #[inline]
    async fn flush(&self) -> crate::kernel::KernelResult<()> {
        let _ignore = self.data_base.flush_async().await?;
        Ok(())
    }

    #[inline]
    async fn set(&self, key: Bytes, value: Bytes) -> crate::kernel::KernelResult<()> {
        let _ignore = self.data_base.insert(key.as_slice(), value.to_vec())?;
        Ok(())
    }

    #[inline]
    async fn get(&self, key: &[u8]) -> crate::kernel::KernelResult<Option<Bytes>> {
        match self.data_base.get(key)? {
            None => Ok(None),
            Some(i_vec) => Ok(Some(Bytes::from(i_vec.to_vec()))),
        }
    }

    #[inline]
    async fn remove(&self, key: &[u8]) -> crate::kernel::KernelResult<()> {
        match self.data_base.remove(key) {
            Ok(Some(_)) => Ok(()),
            Ok(None) => Err(KernelError::KeyNotFound),
            Err(e) => Err(KernelError::SledErr(e)),
        }
    }

    #[inline]
    async fn size_of_disk(&self) -> crate::kernel::KernelResult<u64> {
        Ok(self.data_base.size_on_disk()?)
    }

    #[inline]
    async fn len(&self) -> crate::kernel::KernelResult<usize> {
        Ok(self.data_base.len())
    }

    #[inline]
    async fn is_empty(&self) -> bool {
        self.data_base.is_empty()
    }
}
