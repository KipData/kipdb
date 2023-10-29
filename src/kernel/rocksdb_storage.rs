use crate::kernel::Storage;
use crate::KernelError;
use async_trait::async_trait;
use bytes::Bytes;
use core::slice::SlicePattern;
use std::path::PathBuf;

#[derive(Debug)]
pub struct RocksdbStorage {
    data_base: rocksdb::DB,
}

#[async_trait]
impl Storage for RocksdbStorage {
    #[inline]
    fn name() -> &'static str
    where
        Self: Sized,
    {
        "Rocksdb"
    }

    #[inline]
    async fn open(path: impl Into<PathBuf> + Send) -> crate::kernel::KernelResult<Self> {
        let db = rocksdb::DB::open_default(path.into())?;

        Ok(RocksdbStorage { data_base: db })
    }

    #[inline]
    async fn flush(&self) -> crate::kernel::KernelResult<()> {
        let _ignore = self.data_base.flush()?;
        Ok(())
    }

    #[inline]
    async fn set(&self, key: Bytes, value: Bytes) -> crate::kernel::KernelResult<()> {
        let _ignore = self.data_base.put(key.as_slice(), &value)?;
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
        match self.data_base.delete(key) {
            Ok(_) => Ok(()),
            Err(e) => Err(KernelError::RocksdbErr(e)),
        }
    }

    #[inline]
    async fn size_of_disk(&self) -> crate::kernel::KernelResult<u64> {
        Err(KernelError::NotSupport(
            "Rocksdb does not support size_of_disk()",
        ))
    }

    #[inline]
    async fn len(&self) -> crate::kernel::KernelResult<usize> {
        Err(KernelError::NotSupport("Rocksdb does not support len()"))
    }

    #[inline]
    async fn is_empty(&self) -> bool {
        unimplemented!("Rocksdb does not support is_empty()")
    }
}
