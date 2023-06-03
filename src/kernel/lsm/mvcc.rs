use std::collections::Bound;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use bytes::Bytes;
use itertools::Itertools;
use skiplist::SkipMap;
use tokio::sync::mpsc::UnboundedSender;
use crate::kernel::lsm::compactor::CompactTask;
use crate::kernel::lsm::is_exceeded_then_minor;
use crate::kernel::lsm::iterator::version_iter::VersionIter;
use crate::kernel::Result;
use crate::kernel::lsm::lsm_kv::{Config, Sequence, StoreInner};
use crate::kernel::lsm::mem_table::{KeyValue, MemTable};
use crate::kernel::lsm::version::Version;
use crate::KernelError;

pub struct Transaction {
    pub(crate) store_inner: Arc<StoreInner>,
    pub(crate) compactor_tx: UnboundedSender<CompactTask>,

    pub(crate) version: Arc<Version>,
    pub(crate) writer_buf: SkipMap<Bytes, Option<Bytes>>,
    pub(crate) seq_id: i64,
}

impl Transaction {

    /// 通过Key获取对应的Value
    ///
    /// 此处不需要等待压缩，因为在Transaction存活时不会触发Compaction
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(value) = self.writer_buf.get(key).and_then(Option::clone) {
            return Ok(Some(value));
        }

        if let Some(value) = self.mem_table().find_with_sequence_id(key, self.seq_id) {
            return Ok(Some(value));
        }

        if let Some(value) = self.version.find_data_for_ss_tables(key)? {
            return Ok(Some(value));
        }

        Ok(None)
    }

    pub fn set(&mut self, key: &[u8], value: Bytes) {
        let _ignore = self.writer_buf.insert(
            Bytes::copy_from_slice(key), Some(value)
        );
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        let _ = self.get(key)?
            .ok_or(KernelError::KeyNotFound)?;

        let _ignore = self.writer_buf
            .insert(Bytes::copy_from_slice(key), None);

        Ok(())
    }

    pub fn commit(self) -> Result<()> {
        let batch_data = self.writer_buf.iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect_vec();

        let mem_table = self.mem_table();
        let data_len = mem_table.insert_batch_data(batch_data, Sequence::create())?;

        let _ = mem_table.tx_count
            .fetch_sub(1, Ordering::Release);

        is_exceeded_then_minor(data_len, &self.compactor_tx, self.config())?;

        Ok(())
    }

    pub fn mem_range(&self, min: Bound<&[u8]>, max: Bound<&[u8]>) -> Vec<KeyValue> {
        self.mem_table().range_scan(min, max, Some(self.seq_id))
    }

    pub fn disk_iter(&self) -> Result<VersionIter> {
        VersionIter::new(&self.version)
    }

    fn mem_table(&self) -> &MemTable {
        &self.store_inner.mem_table
    }

    fn config(&self) -> &Config {
        &self.store_inner.config
    }
}

/// TODO: 更多的Test Case
#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use itertools::Itertools;
    use tempfile::TempDir;
    use crate::kernel::lsm::lsm_kv::{Config, LsmStore};
    use crate::kernel::{KVStore, Result};

    #[test]
    fn test_transaction() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        tokio_test::block_on(async move {
            let times = 5000;

            let value = b"Stray birds of summer come to my window to sing and fly away.
            And yellow leaves of autumn, which have no songs, flutter and fall
            there with a sign.";

            let config = Config::new(temp_dir.into_path())
                .minor_threshold_with_len(1000)
                .major_threshold_with_sst_size(4);
            let kv_store = LsmStore::open_with_config(config).await?;

            let mut transaction = kv_store.new_transaction().await;

            let mut vec_kv = Vec::new();

            for i in 0..times {
                let vec_u8 = bincode::serialize(&i)?;
                vec_kv.push((
                    Bytes::from(vec_u8.clone()),
                    Bytes::from(vec_u8.into_iter()
                        .chain(value.to_vec())
                        .collect_vec())
                ));
            }

            for i in 0..times {
                transaction.set(&vec_kv[i].0, vec_kv[i].1.clone());
            }

            transaction.remove(&vec_kv[times - 1].0)?;

            for i in 0..times - 1 {
                assert_eq!(transaction.get(&vec_kv[i].0)?, Some(vec_kv[i].1.clone()));
            }

            assert_eq!(transaction.get(&vec_kv[times - 1].0)?, None);

            // 提交前不应该读取到数据
            for i in 0..times {
                assert_eq!(kv_store.get(&vec_kv[i].0).await?, None);
            }

            transaction.commit()?;

            for i in 0..times - 1 {
                assert_eq!(kv_store.get(&vec_kv[i].0).await?, Some(vec_kv[i].1.clone()));
            }

            Ok(())
        })
    }
}