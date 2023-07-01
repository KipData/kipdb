use crate::kernel::lsm::compactor::CompactTask;
use crate::kernel::lsm::iterator::{Iter, Seek};
use crate::kernel::lsm::mem_table::{KeyValue, MemTable};
use crate::kernel::lsm::storage::{Sequence, StoreInner};
use crate::kernel::lsm::version::iter::VersionIter;
use crate::kernel::lsm::version::Version;
use crate::kernel::Result;
use crate::KernelError;
use bytes::Bytes;
use itertools::Itertools;
use skiplist::SkipMap;
use std::collections::Bound;
use std::iter::Map;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;

type MapIter<'a> = Map<
    skiplist::skipmap::Iter<'a, Bytes, Option<Bytes>>,
    fn((&Bytes, &Option<Bytes>)) -> (Bytes, Option<Bytes>),
>;

pub struct Transaction {
    pub(crate) store_inner: Arc<StoreInner>,
    pub(crate) compactor_tx: Sender<CompactTask>,

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

        if let Some(value) = self.version.query(key)? {
            return Ok(Some(value));
        }

        Ok(None)
    }

    pub fn set(&mut self, key: &[u8], value: Bytes) {
        let _ignore = self
            .writer_buf
            .insert(Bytes::copy_from_slice(key), Some(value));
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        let _ = self.get(key)?.ok_or(KernelError::KeyNotFound)?;

        let _ignore = self.writer_buf.insert(Bytes::copy_from_slice(key), None);

        Ok(())
    }

    pub fn range_scan(&self, min: Bound<&[u8]>, max: Bound<&[u8]>) -> Result<Vec<KeyValue>> {
        let version_range = self.version_range(min, max)?;
        let mem_table_range = self.mem_table().range_scan(min, max, Some(self.seq_id));

        Ok(self
            ._mem_range(min, max)
            .chain(mem_table_range)
            .chain(version_range)
            .unique_by(|(key, _)| key.clone())
            .sorted_by_key(|(key, _)| key.clone())
            .collect_vec())
    }

    fn version_range(&self, min: Bound<&[u8]>, max: Bound<&[u8]>) -> Result<Vec<KeyValue>> {
        let mut version_range = Vec::new();
        let mut iter = VersionIter::new(&self.version)?;

        match min {
            Bound::Included(key) => {
                if let Some(included_item) = iter.seek(Seek::Backward(key))? {
                    if included_item.0 == key {
                        version_range.push(included_item)
                    }
                }
            }
            Bound::Excluded(key) => {
                let _ = iter.seek(Seek::Backward(key))?;
            }
            _ => (),
        }

        while let Some(item) = iter.next_err()? {
            if match max {
                Bound::Included(key) => item.0 <= key,
                Bound::Excluded(key) => item.0 < key,
                _ => true,
            } {
                version_range.push(item);
            } else {
                break;
            }
        }
        Ok(version_range)
    }

    pub async fn commit(self) -> Result<()> {
        let batch_data = self
            .writer_buf
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect_vec();

        let mem_table = self.mem_table();
        if mem_table.insert_batch_data(batch_data, Sequence::create())? {
            if let Err(TrySendError::Closed(_)) =
                self.compactor_tx.try_send(CompactTask::Flush(None))
            {
                return Err(KernelError::ChannelClose);
            }
        }

        let _ = mem_table.tx_count.fetch_sub(1, Ordering::Release);

        Ok(())
    }

    pub fn mem_range(&self, min: Bound<&[u8]>, max: Bound<&[u8]>) -> Vec<KeyValue> {
        let mem_table_range = self.mem_table().range_scan(min, max, Some(self.seq_id));

        self._mem_range(min, max)
            .chain(mem_table_range)
            .unique_by(|(key, _)| key.clone())
            .sorted_by_key(|(key, _)| key.clone())
            .collect_vec()
    }

    fn _mem_range(&self, min: Bound<&[u8]>, max: Bound<&[u8]>) -> MapIter {
        self.writer_buf
            .range(
                min.map(Bytes::copy_from_slice).as_ref(),
                max.map(Bytes::copy_from_slice).as_ref(),
            )
            .map(|(key, value)| (key.clone(), value.clone()))
    }

    pub fn disk_iter(&self) -> Result<VersionIter> {
        VersionIter::new(&self.version)
    }

    fn mem_table(&self) -> &MemTable {
        &self.store_inner.mem_table
    }
}

/// TODO: 更多的Test Case
#[cfg(test)]
mod tests {
    use crate::kernel::lsm::storage::{Config, LsmStore};
    use crate::kernel::{Result, Storage};
    use bincode::Options;
    use bytes::Bytes;
    use itertools::Itertools;
    use std::collections::Bound;
    use tempfile::TempDir;

    #[test]
    fn test_transaction() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        tokio_test::block_on(async move {
            let times = 5000;

            let value = b"Stray birds of summer come to my window to sing and fly away.
            And yellow leaves of autumn, which have no songs, flutter and fall
            there with a sign.";

            let config = Config::new(temp_dir.into_path()).major_threshold_with_sst_size(4);
            let kv_store = LsmStore::open_with_config(config).await?;

            let mut vec_kv = Vec::new();

            for i in 0..times {
                let vec_u8 = bincode::options().with_big_endian().serialize(&i)?;
                vec_kv.push((
                    Bytes::from(vec_u8.clone()),
                    Bytes::from(vec_u8.into_iter().chain(value.to_vec()).collect_vec()),
                ));
            }

            // 模拟数据分布在MemTable以及SSTable中
            for i in 0..50 {
                kv_store.set(&vec_kv[i].0, vec_kv[i].1.clone()).await?;
            }

            kv_store.flush().await?;

            for i in 50..100 {
                kv_store.set(&vec_kv[i].0, vec_kv[i].1.clone()).await?;
            }

            let mut tx_1 = kv_store.new_transaction().await;

            for i in 100..times {
                tx_1.set(&vec_kv[i].0, vec_kv[i].1.clone());
            }

            tx_1.remove(&vec_kv[times - 1].0)?;

            // 事务在提交前事务可以读取到自身以及Store已写入的数据
            for i in 0..times - 1 {
                assert_eq!(tx_1.get(&vec_kv[i].0)?, Some(vec_kv[i].1.clone()));
            }

            assert_eq!(tx_1.get(&vec_kv[times - 1].0)?, None);

            // 事务在提交前Store不应该读取到事务中的数据
            for i in 100..times {
                assert_eq!(kv_store.get(&vec_kv[i].0).await?, None);
            }

            let vec_test = vec_kv[25..]
                .iter()
                .cloned()
                .map(|(key, value)| (key, Some(value)))
                .collect_vec();

            let vec_range = tx_1.range_scan(Bound::Included(&vec_kv[25].0), Bound::Unbounded)?;

            // -1是因为最后一个元素在之前tx中删除了，因此为None
            for i in 0..vec_range.len() - 1 {
                // 元素太多，因此这里就单个对比，否则会导致报错时日志过多
                assert_eq!(vec_range[i], vec_test[i]);
            }

            tx_1.commit().await?;

            for i in 0..times - 1 {
                assert_eq!(kv_store.get(&vec_kv[i].0).await?, Some(vec_kv[i].1.clone()));
            }

            Ok(())
        })
    }
}
