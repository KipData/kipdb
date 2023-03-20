use std::collections::Bound;
use std::mem;
use crossbeam_skiplist::SkipMap;
use itertools::Itertools;
use optimistic_lock_coupling::{OptimisticLockCoupling, OptimisticLockCouplingReadGuard, OptimisticLockCouplingWriteGuard};
use crate::kernel::Result;
use crate::kernel::CommandData;
use crate::kernel::lsm::lsm_kv::Sequence;

/// Value为此Key的Records(CommandData与seq_id)
pub(crate) type MemMap = SkipMap<Vec<u8>, CommandData>;

/// seq_id的上限值
///
/// 用于默认的key的填充(补充使UserKey为高位，因此默认获取最新的seq_id数据)
const SEQ_MAX: [u8; 8] = [u8::MAX, u8::MAX, u8::MAX, u8::MAX, u8::MAX, u8::MAX, u8::MAX, 127];

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
        cmd: CommandData,
    ) -> Result<()> {
        // 将seq_id作为低位
        let key = key_encode_with_seq(cmd.get_key_clone(), Sequence::create())?;

        self.loop_read(|inner| {
            inner.mem_table.insert(key, cmd);
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
    pub(crate) fn try_exceeded_then_swap(&self, exceeded: usize) -> Option<Vec<CommandData>> {
        // 以try_write以判断是否存在事务进行读取而防止发送Minor Compaction而导致MemTable数据异常
        if let Ok(mut inner) = self.inner.write() {
            if inner.mem_table.len() >= exceeded {
                return Self::swap_(&mut inner);
            }
        }
        None
    }

    /// MemTable将数据弹出并转移到immutable中  (弹出数据为有序的)
    pub(crate) fn swap(&self) -> Option<Vec<CommandData>> {
        loop {
            if let Ok(mut inner) = self.inner.write() {
                return Self::swap_(&mut inner);
            }
            std::hint::spin_loop();
        }
    }

    fn swap_(inner: &mut OptimisticLockCouplingWriteGuard<TableInner>) -> Option<Vec<CommandData>> {
        (!inner.mem_table.is_empty())
            .then(|| {
                let mut vec_data = inner.mem_table.iter()
                    .map(|entry| entry.value().clone())
                    // rev以使用最后(最新)的key
                    .rev()
                    .unique_by(CommandData::get_key_clone)
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
        let mut encode_key = key.to_vec();
        encode_key.append(
            &mut SEQ_MAX.to_vec()
        );
        self.loop_read(|inner| {
            Self::find_(&encode_key, key, &inner.mem_table)
                .or_else(|| {
                    inner.immut_table.as_ref()
                        .and_then(|mem_map| Self::find_(&encode_key, key, mem_map))
                })
        })
    }

    pub(crate) fn find_with_inner(key: &[u8], seq_id: i64, inner: &TableInner) -> Result<Option<Vec<u8>>> {
        return if let Some(value) = find_with_seq_(key, seq_id, &inner.mem_table)? {
            Ok(Some(value))
        } else if let Some(mem_map) = &inner.immut_table {
            find_with_seq_(key, seq_id, mem_map)
        } else {
            Ok(None)
        };

        /// 通过sequence_id对mem_table的数据进行获取
        ///
        /// 获取第一个小于等于sequence_id的数据
        fn find_with_seq_(key: &[u8], sequence_id: i64, mem_map: &MemMap) -> Result<Option<Vec<u8>>> {
            key_encode_with_seq(key.to_vec(), sequence_id)
                .map(|seq_key| MemTable::find_(&seq_key, key, mem_map))

        }
    }

    /// 查询时附带sequence_id进行历史数据查询
    #[allow(dead_code)]
    fn find_with_sequence_id(&self, key: &[u8], sequence_id: i64) -> Result<Option<Vec<u8>>> {
        self.loop_read(|inner| {
            Self::find_with_inner(key, sequence_id, &inner)
        })
    }

    fn find_(seq_key: &[u8], user_key: &[u8], mem_map: &MemMap) -> Option<Vec<u8>> {
        mem_map.upper_bound(Bound::Included(seq_key))
            .and_then(|entry| {
                let data = entry.value();
                (user_key == data.get_key())
                    .then(|| data.get_value_clone())
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

/// 将key与seq_id进行混合编码
///
/// key在高位，seq_id在低位
pub(crate) fn key_encode_with_seq(mut key: Vec<u8>, seq_id: i64) -> Result<Vec<u8>> {
    key.append(
        &mut bincode::serialize(&seq_id)?
    );
    Ok(key)
}

#[cfg(test)]
mod tests {
    use crate::kernel::lsm::lsm_kv::Sequence;
    use crate::kernel::{CommandData, Result};
    use crate::kernel::lsm::mem_table::{MemMap, MemTable};

    #[test]
    fn test_mem_table_find() -> Result<()> {
        let mem_table = MemTable::new(MemMap::new());

        let data_1 = CommandData::set(vec![b'k'], vec![b'1']);
        let data_2 = CommandData::set(vec![b'k'], vec![b'2']);

        mem_table.insert_data(data_1)?;

        let old_seq_id = Sequence::create();

        assert_eq!(mem_table.find(&vec![b'k']), Some(vec![b'1']));

        mem_table.insert_data(data_2)?;

        assert_eq!(mem_table.find(&vec![b'k']), Some(vec![b'2']));

        assert_eq!(mem_table.find_with_sequence_id(&vec![b'k'], old_seq_id)?, Some(vec![b'1']));

        let new_seq_id = Sequence::create();

        assert_eq!(mem_table.find_with_sequence_id(&vec![b'k'], new_seq_id)?, Some(vec![b'2']));

        Ok(())
    }

    #[test]
    fn test_mem_table_swap() -> Result<()> {
        let mem_table = MemTable::new(MemMap::new());

        mem_table.insert_data(CommandData::set(vec![b'k', b'1'], vec![b'1']))?;
        mem_table.insert_data(CommandData::set(vec![b'k', b'1'], vec![b'2']))?;
        mem_table.insert_data(CommandData::set(vec![b'k', b'2'], vec![b'1']))?;
        mem_table.insert_data(CommandData::set(vec![b'k', b'2'], vec![b'2']))?;

        let mut vec_unique_sort_with_cmd_key = mem_table.swap().unwrap();

        assert_eq!(vec_unique_sort_with_cmd_key.pop(), Some(CommandData::set(vec![b'k', b'2'], vec![b'2'])));
        assert_eq!(vec_unique_sort_with_cmd_key.pop(), Some(CommandData::set(vec![b'k', b'1'], vec![b'2'])));

        Ok(())
    }
}