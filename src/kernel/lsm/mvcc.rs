use crate::kernel::lsm::compactor::CompactTask;
use crate::kernel::lsm::iterator::merging_iter::MergingIter;
use crate::kernel::lsm::iterator::{Iter, Seek};
use crate::kernel::lsm::mem_table::{KeyValue, MemTable};
use crate::kernel::lsm::query_and_compaction;
use crate::kernel::lsm::storage::{KipStorage, Sequence, StoreInner};
use crate::kernel::lsm::version::iter::VersionIter;
use crate::kernel::lsm::version::Version;
use crate::kernel::KernelResult;
use crate::KernelError;
use bytes::Bytes;
use core::slice::SlicePattern;
use itertools::Itertools;
use skiplist::SkipMap;
use std::collections::Bound;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;

unsafe impl Send for BufPtr {}
unsafe impl Sync for BufPtr {}

struct BufPtr(NonNull<Vec<KeyValue>>);

pub enum CheckType {
    Optimistic,
}

pub struct Transaction {
    store_inner: Arc<StoreInner>,
    compactor_tx: Sender<CompactTask>,

    version: Arc<Version>,
    seq_id: i64,
    check_type: CheckType,

    write_buf: Option<SkipMap<Bytes, Option<Bytes>>>,
}

impl Transaction {
    pub(crate) async fn new(storage: &KipStorage, check_type: CheckType) -> Self {
        let _ = storage.mem_table().tx_count.fetch_add(1, Ordering::Release);

        Transaction {
            store_inner: Arc::clone(&storage.inner),
            version: storage.current_version().await,
            compactor_tx: storage.compactor_tx.clone(),

            seq_id: Sequence::create(),
            write_buf: None,
            check_type,
        }
    }

    fn write_buf_or_init(&mut self) -> &mut SkipMap<Bytes, Option<Bytes>> {
        self.write_buf.get_or_insert_with(SkipMap::new)
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
            let batch_data = buf.into_iter().collect_vec();

            match self.check_type {
                CheckType::Optimistic => {
                    if self
                        .mem_table()
                        .check_key_conflict(&batch_data, self.seq_id)
                    {
                        return Err(KernelError::RepeatedWrite);
                    }
                }
            }

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
        let mut write_buf_ptr = None;
        let mut vec_iter: Vec<Box<dyn Iter<'a, Item = KeyValue> + 'a + Send + Sync>> =
            Vec::with_capacity(3);

        let option_write_buf = self.write_buf.as_ref().map(|buf| {
            buf.range(
                min.map(Bytes::copy_from_slice).as_ref(),
                max.map(Bytes::copy_from_slice).as_ref(),
            )
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect_vec()
        });
        if let Some(write_buf) = option_write_buf {
            let buf_ptr = BufPtr(Box::leak(Box::new(write_buf)).into());
            let buf_iter = unsafe {
                BufIter {
                    inner: buf_ptr.0.as_ref(),
                    pos: 0,
                }
            };

            write_buf_ptr = Some(buf_ptr);
            vec_iter.push(Box::new(buf_iter));
        }

        let mem_buf = self.mem_table().range_scan(min, max, Some(self.seq_id));
        let mem_buf_ptr = BufPtr(Box::leak(Box::new(mem_buf)).into());

        vec_iter.push(Box::new(unsafe {
            BufIter {
                inner: mem_buf_ptr.0.as_ref(),
                pos: 0,
            }
        }));
        VersionIter::merging_with_version(&self.version, &mut vec_iter)?;

        let mut inner = MergingIter::new(vec_iter)?;
        let mut init_buf = None;

        match &min {
            Bound::Included(key) => {
                init_buf = inner
                    .seek(Seek::Backward(key.as_slice()))?
                    .and_then(|data| (data.0 >= key).then_some(data));
            }
            Bound::Excluded(key) => {
                init_buf = inner
                    .seek(Seek::Backward(key.as_slice()))?
                    .and_then(|data| (data.0 > key).then_some(data));
            }
            Bound::Unbounded => (),
        };

        Ok(TransactionIter {
            inner,
            max: max.map(Bytes::copy_from_slice),

            write_buf_ptr,
            mem_buf_ptr,

            is_overed: false,
            init_buf,
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

    write_buf_ptr: Option<BufPtr>,
    mem_buf_ptr: BufPtr,

    max: Bound<Bytes>,
    init_buf: Option<KeyValue>,
    is_overed: bool,
}

impl<'a> Iter<'a> for TransactionIter<'a> {
    type Item = KeyValue;

    #[inline]
    fn try_next(&mut self) -> KernelResult<Option<Self::Item>> {
        if self.is_overed {
            return Ok(None);
        }
        let mut item = self.init_buf.take();

        if item.is_none() {
            item = self.inner.try_next()?;
        }
        let option = match &self.max {
            Bound::Included(key) => item.and_then(|data| (data.0 <= key).then_some(data)),
            Bound::Excluded(key) => item.and_then(|data| (data.0 < key).then_some(data)),
            Bound::Unbounded => item,
        };
        self.is_overed = option.is_none();

        Ok(option)
    }

    #[inline]
    fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    #[inline]
    fn seek(&mut self, seek: Seek<'_>) -> KernelResult<Option<Self::Item>> {
        self.init_buf = None;

        self.inner.seek(seek)
    }
}

impl Drop for TransactionIter<'_> {
    #[inline]
    fn drop(&mut self) {
        if let Some(buf_prt) = &self.write_buf_ptr {
            unsafe { drop(Box::from_raw(buf_prt.0.as_ptr())) }
        }
        unsafe { drop(Box::from_raw(self.mem_buf_ptr.0.as_ptr())) }
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
    use crate::kernel::lsm::mvcc::CheckType;
    use crate::kernel::lsm::storage::{Config, KipStorage};
    use crate::kernel::{KernelResult, Storage};
    use crate::KernelError;
    use bincode::Options;
    use bytes::Bytes;
    use itertools::Itertools;
    use std::collections::Bound;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_transaction() -> KernelResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

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
        for kv in vec_kv.iter().take(50) {
            kv_store.set(kv.0.clone(), kv.1.clone()).await?;
        }

        kv_store.flush().await?;

        for kv in vec_kv.iter().take(100).skip(50) {
            kv_store.set(kv.0.clone(), kv.1.clone()).await?;
        }

        let mut tx_1 = kv_store.new_transaction(CheckType::Optimistic).await;

        for kv in vec_kv.iter().take(times).skip(100) {
            tx_1.set(kv.0.clone(), kv.1.clone());
        }

        tx_1.remove(&vec_kv[times - 1].0)?;

        // 事务在提交前事务可以读取到自身以及Store已写入的数据
        for kv in vec_kv.iter().take(times - 1) {
            assert_eq!(tx_1.get(&kv.0)?, Some(kv.1.clone()));
        }

        assert_eq!(tx_1.get(&vec_kv[times - 1].0)?, None);

        // 事务在提交前Store不应该读取到事务中的数据
        for kv in vec_kv.iter().take(times).skip(100) {
            assert_eq!(kv_store.get(&kv.0).await?, None);
        }

        let vec_test = vec_kv[25..]
            .iter()
            .cloned()
            .map(|(key, value)| (key, Some(value)))
            .collect_vec();

        let mut iter = tx_1.iter(Bound::Included(&vec_kv[25].0), Bound::Unbounded)?;

        // -1是因为最后一个元素在之前tx中删除了，因此为None
        for kv in vec_test.iter().take(vec_test.len() - 1) {
            // 元素太多，因此这里就单个对比，否则会导致报错时日志过多
            assert_eq!(iter.try_next()?.unwrap(), kv.clone());
        }

        drop(iter);

        tx_1.commit().await?;

        for kv in vec_kv.iter().take(times - 1) {
            assert_eq!(kv_store.get(&kv.0).await?, Some(kv.1.clone()));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_transaction_check_optimistic() -> KernelResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let config = Config::new(temp_dir.into_path()).major_threshold_with_sst_size(4);
        let kv_store = KipStorage::open_with_config(config).await?;

        let mut tx_1 = kv_store.new_transaction(CheckType::Optimistic).await;
        let mut tx_2 = kv_store.new_transaction(CheckType::Optimistic).await;

        tx_1.set(Bytes::from("same_key"), Bytes::new());
        tx_2.set(Bytes::from("same_key"), Bytes::new());

        tx_1.commit().await?;

        assert!(matches!(
            tx_2.commit().await,
            Err(KernelError::RepeatedWrite)
        ));

        Ok(())
    }
}
