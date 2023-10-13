use crate::kernel::lsm::compactor::CompactTask;
use crate::kernel::lsm::iterator::merging_iter::MergingIter;
use crate::kernel::lsm::iterator::{Iter, Seek};
use crate::kernel::lsm::mem_table::{KeyValue, MemTable};
use crate::kernel::lsm::query_and_compaction;
use crate::kernel::lsm::storage::{Sequence, StoreInner};
use crate::kernel::lsm::version::iter::VersionIter;
use crate::kernel::lsm::version::Version;
use crate::kernel::KernelResult;
use crate::KernelError;
use bytes::Bytes;
use itertools::Itertools;
use skiplist::SkipMap;
use std::collections::Bound;
use std::iter::Map;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;

type MapIter<'a> = Map<
    skiplist::skipmap::Iter<'a, Bytes, Option<Bytes>>,
    fn((&Bytes, &Option<Bytes>)) -> KeyValue,
>;

unsafe impl Send for BufPtr {}
unsafe impl Sync for BufPtr {}

struct BufPtr(NonNull<Vec<KeyValue>>);

pub struct Transaction {
    pub(crate) store_inner: Arc<StoreInner>,
    pub(crate) compactor_tx: Sender<CompactTask>,

    pub(crate) version: Arc<Version>,
    pub(crate) write_buf: Option<SkipMap<Bytes, Option<Bytes>>>,
    pub(crate) seq_id: i64,
}

impl Transaction {
    fn write_buf_or_init(&mut self) -> &mut SkipMap<Bytes, Option<Bytes>> {
        self.write_buf.get_or_insert_with(|| SkipMap::new())
    }

    /// 通过Key获取对应的Value
    ///
    /// 此处不需要等待压缩，因为在Transaction存活时不会触发Compaction
    #[inline]
    pub fn get(&self, key: &[u8]) -> KernelResult<Option<Bytes>> {
        if let Some(value) = self.write_buf.as_ref().and_then(|buf| buf.get(key)) {
            return Ok(value.clone());
        }

        if let Some((_, value)) = self.mem_table().find_with_sequence_id(key, self.seq_id) {
            return Ok(value);
        }

        if let Some((_, value)) = query_and_compaction(key, &self.version, &self.compactor_tx)? {
            return Ok(value);
        }

        Ok(None)
    }

    #[inline]
    pub fn set(&mut self, key: Bytes, value: Bytes) {
        let _ignore = self.write_buf_or_init().insert(key, Some(value));
    }

    #[inline]
    pub fn remove(&mut self, key: &[u8]) -> KernelResult<()> {
        let _ = self.get(key)?.ok_or(KernelError::KeyNotFound)?;
        let bytes = Bytes::copy_from_slice(key);
        let _ignore = self.write_buf_or_init().insert(bytes, None);

        Ok(())
    }

    #[inline]
    pub async fn commit(mut self) -> KernelResult<()> {
        if let Some(buf) = self.write_buf.take() {
            let batch_data = buf
                .into_iter()
                .map(|(key, value)| (key, value))
                .collect_vec();

            let is_exceeds = self
                .store_inner
                .mem_table
                .insert_batch_data(batch_data, Sequence::create())?;

            if is_exceeds {
                if let Err(TrySendError::Closed(_)) =
                    self.compactor_tx.try_send(CompactTask::Flush(None))
                {
                    return Err(KernelError::ChannelClose);
                }
            }
        }

        Ok(())
    }

    #[inline]
    pub fn mem_range(&self, min: Bound<&[u8]>, max: Bound<&[u8]>) -> Vec<KeyValue> {
        let mem_table_range = self.mem_table().range_scan(min, max, Some(self.seq_id));

        if let Some(buf_iter) = self._mem_range(min, max) {
            buf_iter
                .chain(mem_table_range)
                .unique_by(|(key, _)| key.clone())
                .sorted_by_key(|(key, _)| key.clone())
                .collect_vec()
        } else {
            mem_table_range
        }
    }

    fn _mem_range(&self, min: Bound<&[u8]>, max: Bound<&[u8]>) -> Option<MapIter> {
        if let Some(buf) = &self.write_buf {
            Some(
                buf.range(
                    min.map(Bytes::copy_from_slice).as_ref(),
                    max.map(Bytes::copy_from_slice).as_ref(),
                )
                .map(|(key, value)| (key.clone(), value.clone())),
            )
        } else {
            None
        }
    }

    fn mem_table(&self) -> &MemTable {
        &self.store_inner.mem_table
    }

    #[inline]
    pub fn disk_iter(&self) -> KernelResult<VersionIter> {
        VersionIter::new(&self.version)
    }

    #[inline]
    pub fn iter<'a>(
        &'a self,
        min: Bound<&[u8]>,
        max: Bound<&[u8]>,
    ) -> KernelResult<TransactionIter> {
        let range_buf = self.mem_range(min, max);
        let ptr = BufPtr(Box::leak(Box::new(range_buf)).into());

        let mem_iter = unsafe {
            BufIter {
                inner: ptr.0.as_ref(),
                pos: 0,
            }
        };

        let mut version_iter = VersionIter::new(&self.version)?;
        let mut seek_buf = None;

        match min {
            Bound::Included(key) => {
                let ver_seek_option = version_iter.seek(Seek::Backward(key))?;
                unsafe {
                    let op = |disk_option: Option<&KeyValue>, mem_option: Option<&KeyValue>| match (
                        disk_option,
                        mem_option,
                    ) {
                        (Some(disk), Some(mem)) => disk.0 >= mem.0,
                        _ => false,
                    };

                    if !op(ver_seek_option.as_ref(), ptr.0.as_ref().first()) {
                        seek_buf = ver_seek_option;
                    }
                }
            }
            Bound::Excluded(key) => {
                let _ = version_iter.seek(Seek::Backward(key))?;
            }
            Bound::Unbounded => (),
        }

        let vec_iter: Vec<Box<dyn Iter<'a, Item = KeyValue> + 'a + Send + Sync>> =
            vec![Box::new(mem_iter), Box::new(version_iter)];

        Ok(TransactionIter {
            inner: MergingIter::new(vec_iter)?,
            max: max.map(Bytes::copy_from_slice),
            ptr,
            seek_buf,
        })
    }
}

impl Drop for Transaction {
    #[inline]
    fn drop(&mut self) {
        let _ = self.mem_table().tx_count.fetch_sub(1, Ordering::Release);
    }
}

unsafe impl Sync for TransactionIter<'_> {}

unsafe impl Send for TransactionIter<'_> {}

pub struct TransactionIter<'a> {
    inner: MergingIter<'a>,
    ptr: BufPtr,
    max: Bound<Bytes>,
    seek_buf: Option<KeyValue>,
}

impl<'a> Iter<'a> for TransactionIter<'a> {
    type Item = KeyValue;

    #[inline]
    fn try_next(&mut self) -> KernelResult<Option<Self::Item>> {
        if let Some(item) = self.seek_buf.take() {
            return Ok(Some(item));
        }

        let option = match &self.max {
            Bound::Included(key) => self
                .inner
                .try_next()?
                .and_then(|data| (data.0 <= key).then_some(data)),
            Bound::Excluded(key) => self
                .inner
                .try_next()?
                .and_then(|data| (data.0 < key).then_some(data)),
            Bound::Unbounded => self.inner.try_next()?,
        };

        Ok(option)
    }

    #[inline]
    fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    #[inline]
    fn seek(&mut self, seek: Seek<'_>) -> KernelResult<Option<Self::Item>> {
        self.inner.seek(seek)
    }
}

impl Drop for TransactionIter<'_> {
    #[inline]
    fn drop(&mut self) {
        unsafe { drop(Box::from_raw(self.ptr.0.as_ptr())) }
    }
}

struct BufIter<'a> {
    inner: &'a Vec<KeyValue>,
    pos: usize,
}

impl<'a> Iter<'a> for BufIter<'a> {
    type Item = KeyValue;

    fn try_next(&mut self) -> KernelResult<Option<Self::Item>> {
        Ok(self.is_valid().then(|| {
            let item = self.inner[self.pos].clone();
            self.pos += 1;
            item
        }))
    }

    fn is_valid(&self) -> bool {
        self.pos < self.inner.len()
    }

    fn seek(&mut self, seek: Seek<'_>) -> KernelResult<Option<Self::Item>> {
        match seek {
            Seek::First => self.pos = 0,
            Seek::Last => self.pos = self.inner.len() - 1,
            Seek::Backward(seek_key) => {
                self.pos = self
                    .inner
                    .binary_search_by(|(key, _)| seek_key.cmp(key).reverse())
                    .unwrap_or_else(|i| i);
            }
        };

        self.try_next()
    }
}

/// TODO: 更多的Test Case
#[cfg(test)]
mod tests {
    use crate::kernel::lsm::iterator::Iter;
    use crate::kernel::lsm::storage::{Config, KipStorage};
    use crate::kernel::{KernelResult, Storage};
    use bincode::Options;
    use bytes::Bytes;
    use itertools::Itertools;
    use std::collections::Bound;
    use tempfile::TempDir;

    #[test]
    fn test_transaction() -> KernelResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        tokio_test::block_on(async move {
            let times = 5000;

            let value = b"0";

            let config = Config::new(temp_dir.into_path()).major_threshold_with_sst_size(4);
            let kv_store = KipStorage::open_with_config(config).await?;

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
                kv_store
                    .set(vec_kv[i].0.clone(), vec_kv[i].1.clone())
                    .await?;
            }

            kv_store.flush().await?;

            for i in 50..100 {
                kv_store
                    .set(vec_kv[i].0.clone(), vec_kv[i].1.clone())
                    .await?;
            }

            let mut tx_1 = kv_store.new_transaction().await;

            for i in 100..times {
                tx_1.set(vec_kv[i].0.clone(), vec_kv[i].1.clone());
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

            let mut iter = tx_1.iter(Bound::Included(&vec_kv[25].0), Bound::Unbounded)?;

            // -1是因为最后一个元素在之前tx中删除了，因此为None
            for i in 0..vec_test.len() - 1 {
                // 元素太多，因此这里就单个对比，否则会导致报错时日志过多
                assert_eq!(iter.try_next()?.unwrap(), vec_test[i]);
            }

            drop(iter);

            tx_1.commit().await?;

            for i in 0..times - 1 {
                assert_eq!(kv_store.get(&vec_kv[i].0).await?, Some(vec_kv[i].1.clone()));
            }

            Ok(())
        })
    }
}
