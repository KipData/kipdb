use std::cmp::Ordering;
use std::collections::Bound;
use std::mem;
use crossbeam_skiplist::SkipMap;
use itertools::Itertools;
use optimistic_lock_coupling::{OptimisticLockCoupling, OptimisticLockCouplingReadGuard, OptimisticLockCouplingWriteGuard};
use crate::kernel::Result;
use crate::kernel::lsm::lsm_kv::Sequence;

/// Value为此Key的Records(CommandData与seq_id)
pub(crate) type MemMap = SkipMap<InternalKey, Option<Vec<u8>>>;

pub(crate) type KeyValue = (Vec<u8>, Option<Vec<u8>>);

/// seq_id的上限值
///
/// 用于默认的key的填充(补充使UserKey为高位，因此默认获取最新的seq_id数据)
const SEQ_MAX: i64 = i64::MAX;

pub(crate) fn key_value_bytes_len(key_value: &KeyValue) -> usize {
    key_value.0.len() + key_value.1.as_ref().map(Vec::len).unwrap_or(0)
}

#[derive(PartialEq, Eq, Debug)]
pub(crate) struct InternalKey {
    key: Vec<u8>,
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
    pub(crate) fn new(key: Vec<u8>) -> Self {
        InternalKey { key, seq_id: Sequence::create() }
    }

    pub(crate) fn new_with_seq(key: Vec<u8>, seq_id: i64) -> Self {
        InternalKey { key, seq_id }
    }

    pub(crate) fn get_key(&self) -> &Vec<u8> {
        &self.key
    }
}

pub(crate) struct MemTable {
    pub(crate) inner: OptimisticLockCoupling<TableInner>
}

#[derive(Debug)]
pub(crate) struct TableInner {
    pub(crate) mem_table: MemMap,
    immut_table: Option<MemMap>
}

impl MemTable {
    pub(crate) fn new(mem_map: MemMap) -> Self {
        MemTable {
            inner: OptimisticLockCoupling::new(
                TableInner {
                    mem_table: mem_map,
                    immut_table: None,
                }
            ),
        }
    }

    /// 插入并判断是否溢出
    ///
    /// 插入时不会去除重复键值，而是进行追加
    #[allow(unused_results)]
    pub(crate) fn insert_data(
        &self,
        data: KeyValue,
    ) -> Result<()> {
        self.loop_read(|inner| {
            let (key, value) = data;
            inner.mem_table.insert(InternalKey::new(key), value);
        });

        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.loop_read(|inner| inner.mem_table.is_empty())
    }

    pub(crate) fn len(&self) -> usize {
        self.loop_read(|inner| inner.mem_table.len())
    }

    /// 尝试判断至是否超出阈值并尝试获取写锁进行MemTable数据转移 (弹出数据为有序的)
    pub(crate) fn try_exceeded_then_swap(&self, exceeded: usize) -> Option<Vec<KeyValue>> {
        // 以try_write以判断是否存在事务进行读取而防止发送Minor Compaction而导致MemTable数据异常
        if let Ok(mut inner) = self.inner.write() {
            if inner.mem_table.len() >= exceeded {
                return Self::swap_(&mut inner);
            }
        }
        None
    }

    /// MemTable将数据弹出并转移到immutable中  (弹出数据为有序的)
    pub(crate) fn swap(&self) -> Option<Vec<KeyValue>> {
        loop {
            if let Ok(mut inner) = self.inner.write() {
                return Self::swap_(&mut inner);
            }
            std::hint::spin_loop();
        }
    }

    fn swap_(inner: &mut OptimisticLockCouplingWriteGuard<TableInner>) -> Option<Vec<KeyValue>> {
        (!inner.mem_table.is_empty())
            .then(|| {
                let mut vec_data = inner.mem_table.iter()
                    .map(|entry| {
                        let key = entry.key().get_key().clone();
                        let value = entry.value().clone();
                        (key, value)
                    })
                    // rev以使用最后(最新)的key
                    .rev()
                    .unique_by(|(k, _)| k.clone())
                    .collect_vec();

                vec_data.reverse();

                inner.immut_table = Some(mem::replace(
                    &mut inner.mem_table, SkipMap::new()
                ));

                vec_data
            })
    }

    pub(crate) fn find(&self, key: &[u8]) -> Option<Vec<u8>> {
        // 填充SEQ_MAX使其变为最高位以尽可能获取最新数据
        let internal_key = InternalKey::new_with_seq(key.to_vec(), SEQ_MAX);
        self.loop_read(|inner| {
            Self::find_(&internal_key, &inner.mem_table)
                .or_else(|| {
                    inner.immut_table.as_ref()
                        .and_then(|mem_map| Self::find_(&internal_key, mem_map))
                })
        })
    }

    pub(crate) fn find_with_inner(key: &[u8], seq_id: i64, inner: &TableInner) -> Option<Vec<u8>> {
        let internal_key = InternalKey::new_with_seq(key.to_vec(), seq_id);
        if let Some(value) = MemTable::find_(&internal_key, &inner.mem_table) {
            Some(value)
        } else if let Some(mem_map) = &inner.immut_table {
            MemTable::find_(&internal_key, &mem_map)
        } else {
            None
        }
    }

    /// 查询时附带seq_id进行历史数据查询
    #[allow(dead_code)]
    fn find_with_sequence_id(&self, key: &[u8], seq_id: i64) -> Option<Vec<u8>> {
        self.loop_read(|inner| {
            Self::find_with_inner(key, seq_id, &inner)
        })
    }

    fn find_(internal_key: &InternalKey, mem_map: &MemMap) -> Option<Vec<u8>> {
        mem_map.upper_bound(Bound::Included(internal_key))
            .and_then(|entry| {
                (internal_key.get_key() == entry.key().get_key())
                    .then(|| entry.value().clone())
            })
            .flatten()
    }

    pub(crate) fn loop_read<F, R>(&self, fn_once: F) -> R
        where F: FnOnce(OptimisticLockCouplingReadGuard<TableInner>) -> R
    {
        loop {
            if let Ok(inner) = self.inner.read() {
                return fn_once(inner);
            }
            std::hint::spin_loop();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::kernel::lsm::lsm_kv::Sequence;
    use crate::kernel::Result;
    use crate::kernel::lsm::mem_table::{MemMap, MemTable};

    #[test]
    fn test_mem_table_find() -> Result<()> {
        let mem_table = MemTable::new(MemMap::new());

        let data_1 = (vec![b'k'], Some(vec![b'1']));
        let data_2 = (vec![b'k'], Some(vec![b'2']));

        mem_table.insert_data(data_1)?;

        let old_seq_id = Sequence::create();

        assert_eq!(mem_table.find(&vec![b'k']), Some(vec![b'1']));

        mem_table.insert_data(data_2)?;

        assert_eq!(mem_table.find(&vec![b'k']), Some(vec![b'2']));

        assert_eq!(mem_table.find_with_sequence_id(&vec![b'k'], old_seq_id), Some(vec![b'1']));

        let new_seq_id = Sequence::create();

        assert_eq!(mem_table.find_with_sequence_id(&vec![b'k'], new_seq_id), Some(vec![b'2']));

        Ok(())
    }

    #[test]
    fn test_mem_table_swap() -> Result<()> {
        let mem_table = MemTable::new(MemMap::new());

        mem_table.insert_data((vec![b'k', b'1'], Some(vec![b'1'])))?;
        mem_table.insert_data((vec![b'k', b'1'], Some(vec![b'2'])))?;
        mem_table.insert_data((vec![b'k', b'2'], Some(vec![b'1'])))?;
        mem_table.insert_data((vec![b'k', b'2'], Some(vec![b'2'])))?;

        let mut vec_unique_sort_with_cmd_key = mem_table.swap().unwrap();

        assert_eq!(vec_unique_sort_with_cmd_key.pop(), Some((vec![b'k', b'2'], Some(vec![b'2']))));
        assert_eq!(vec_unique_sort_with_cmd_key.pop(), Some((vec![b'k', b'1'], Some(vec![b'2']))));

        Ok(())
    }
}