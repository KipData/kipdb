use std::cmp::Ordering;
use std::collections::Bound;
use std::mem;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Acquire;
use bytes::Bytes;
use itertools::Itertools;
use parking_lot::Mutex;
use skiplist::SkipMap;
use crate::kernel::Result;
use crate::kernel::lsm::lsm_kv::Sequence;

/// Value为此Key的Records(Key与seq_id)
pub(crate) type MemMap = SkipMap<InternalKey, Option<Bytes>>;

pub(crate) type KeyValue = (Bytes, Option<Bytes>);

/// seq_id的上限值
///
/// 用于默认的key的填充(补充使UserKey为高位，因此默认获取最新的seq_id数据)
const SEQ_MAX: i64 = i64::MAX;

pub(crate) fn key_value_bytes_len(key_value: &KeyValue) -> usize {
    key_value.0.len() + key_value.1.as_ref().map(Bytes::len).unwrap_or(0)
}

#[derive(PartialEq, Eq, Debug)]
pub(crate) struct InternalKey {
    key: Bytes,
    seq_id: i64,
}

impl PartialOrd<Self> for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.key.partial_cmp(&other.key)
            .and_then(|ord| match ord {
                Ordering::Less => Some(Ordering::Less),
                Ordering::Equal => self.seq_id.partial_cmp(&other.seq_id),
                Ordering::Greater => Some(Ordering::Greater),
            })
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
            .then_with(|| self.seq_id.cmp(&other.seq_id))
    }
}

impl InternalKey {
    pub(crate) fn new(key: Bytes) -> Self {
        InternalKey { key, seq_id: Sequence::create() }
    }

    pub(crate) fn new_with_seq(key: Bytes, seq_id: i64) -> Self {
        InternalKey { key, seq_id }
    }

    pub(crate) fn get_key(&self) -> &Bytes {
        &self.key
    }
}

pub(crate) struct MemTable {
    inner: Mutex<TableInner>,
    pub(crate) tx_count: AtomicUsize
}

struct TableInner {
    _mem: MemMap,
    _immut: Option<MemMap>
}

impl MemTable {
    pub(crate) fn new(mem_map: MemMap) -> Self {
        MemTable {
            inner: Mutex::new(TableInner {
                _mem: mem_map, _immut: None
            }),
            tx_count: AtomicUsize::new(0),
        }
    }

    /// 插入并判断是否溢出
    ///
    /// 插入时不会去除重复键值，而是进行追加
    pub(crate) fn insert_data(
        &self,
        data: KeyValue,
    ) -> Result<usize> {
        let (key, value) = data;
        let mut inner = self.inner.lock();

        let _ = inner._mem.insert(InternalKey::new(key), value);

        Ok(inner._mem.len())
    }

    pub(crate) fn insert_batch_data(
        &self,
        vec_data: Vec<KeyValue>,
        seq_id: i64
    ) -> Result<usize> {
        let mut inner = self.inner.lock();

        for (key, value) in vec_data {
            let _ = inner._mem.insert(InternalKey::new_with_seq(key, seq_id), value);
        }

        Ok(inner._mem.len())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.inner.lock()._mem.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.lock()._mem.len()
    }

    /// MemTable将数据弹出并转移到immutable中  (弹出数据为有序的)
    pub(crate) fn swap(&self) -> Option<Vec<KeyValue>> {
        loop {
            if 0 == self.tx_count.load(Acquire) {
                let mut inner = self.inner.lock();
                // 二重检测防止lock时(前)突然出现事务
                // 当lock后，即使出现事务，会因为lock已被Compactor获取而无法读写，
                // 因此不会对读写进行干扰
                // 并且事务即使在lock后出现，所持有的seq为该压缩之前，
                // 也不会丢失该seq的_mem，因为转移到了_immut，可以从_immut得到对应seq的数据
                if 0 != self.tx_count.load(Acquire) {
                    continue
                }
                return (!inner._mem.is_empty())
                    .then(|| {
                        let mut vec_data = inner._mem.iter()
                            .map(|(k, v)| (k.key.clone(), v.clone()))
                            // rev以使用最后(最新)的key
                            .rev()
                            .unique_by(|(k, _)| k.clone())
                            .collect_vec();

                        vec_data.reverse();

                        inner._immut = Some(mem::replace(
                            &mut inner._mem, SkipMap::new()
                        ));

                        vec_data
                    });
            }
            std::hint::spin_loop();
        }
    }

    pub(crate) fn find(&self, key: &[u8]) -> Option<Bytes> {
        // 填充SEQ_MAX使其变为最高位以尽可能获取最新数据
        let internal_key = InternalKey::new_with_seq(Bytes::copy_from_slice(key), SEQ_MAX);
        let inner = self.inner.lock();

        Self::find_(&internal_key, &inner._mem)
            .or_else(|| {
                inner._immut.as_ref()
                    .and_then(|mem_map| Self::find_(&internal_key, mem_map))
            })
    }

    fn find_with_inner(key: &[u8], seq_id: i64, inner: &TableInner) -> Option<Bytes> {
        let internal_key = InternalKey::new_with_seq(Bytes::copy_from_slice(key), seq_id);

        if let Some(value) = MemTable::find_(&internal_key, &inner._mem) {
            Some(value)
        } else if let Some(mem_map) = &inner._immut {
            MemTable::find_(&internal_key, mem_map)
        } else {
            None
        }
    }

    /// 查询时附带seq_id进行历史数据查询
    pub(crate) fn find_with_sequence_id(&self, key: &[u8], seq_id: i64) -> Option<Bytes> {
        Self::find_with_inner(key, seq_id, &self.inner.lock())
    }

    fn find_(internal_key: &InternalKey, mem_map: &MemMap) -> Option<Bytes> {
        mem_map.upper_bound(Bound::Included(internal_key))
            .and_then(|(intern_key, value)| {
                (internal_key.get_key() == &intern_key.key)
                    .then(|| value.clone())
            })
            .flatten()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use crate::kernel::lsm::lsm_kv::Sequence;
    use crate::kernel::Result;
    use crate::kernel::lsm::mem_table::{MemMap, MemTable};

    #[test]
    fn test_mem_table_find() -> Result<()> {
        let mem_table = MemTable::new(MemMap::new());

        let data_1 = (Bytes::from(vec![b'k']), Some(Bytes::from(vec![b'1'])));
        let data_2 = (Bytes::from(vec![b'k']), Some(Bytes::from(vec![b'2'])));

        assert_eq!(mem_table.insert_data(data_1)?, 1);

        let old_seq_id = Sequence::create();

        assert_eq!(mem_table.find(&vec![b'k']), Some(Bytes::from(vec![b'1'])));

        assert_eq!(mem_table.insert_data(data_2)?, 2);

        assert_eq!(mem_table.find(&vec![b'k']), Some(Bytes::from(vec![b'2'])));

        assert_eq!(mem_table.find_with_sequence_id(&vec![b'k'], old_seq_id), Some(Bytes::from(vec![b'1'])));

        let new_seq_id = Sequence::create();

        assert_eq!(mem_table.find_with_sequence_id(&vec![b'k'], new_seq_id), Some(Bytes::from(vec![b'2'])));

        Ok(())
    }

    #[test]
    fn test_mem_table_swap() -> Result<()> {
        let mem_table = MemTable::new(MemMap::new());

        assert_eq!(mem_table.insert_data((Bytes::from(vec![b'k', b'1']), Some(Bytes::from(vec![b'1']))))?, 1);
        assert_eq!(mem_table.insert_data((Bytes::from(vec![b'k', b'1']), Some(Bytes::from(vec![b'2']))))?, 2);
        assert_eq!(mem_table.insert_data((Bytes::from(vec![b'k', b'2']), Some(Bytes::from(vec![b'1']))))?, 3);
        assert_eq!(mem_table.insert_data((Bytes::from(vec![b'k', b'2']), Some(Bytes::from(vec![b'2']))))?, 4);

        let mut vec_unique_sort_with_cmd_key = mem_table.swap().unwrap();

        assert_eq!(vec_unique_sort_with_cmd_key.pop(), Some((Bytes::from(vec![b'k', b'2']), Some(Bytes::from(vec![b'2'])))));
        assert_eq!(vec_unique_sort_with_cmd_key.pop(), Some((Bytes::from(vec![b'k', b'1']), Some(Bytes::from(vec![b'2'])))));

        Ok(())
    }
}