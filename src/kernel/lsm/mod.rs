use std::collections::{Bound, HashMap};
use std::collections::hash_map::{Iter, RandomState, Values};
use std::mem;
use crossbeam_skiplist::SkipMap;
use growable_bloom_filter::GrowableBloom;
use itertools::Itertools;
use parking_lot::lock_api::RwLockWriteGuard;
use parking_lot::{RawRwLock, RwLock};
use serde::{Deserialize, Serialize};
use crate::kernel::{CommandData, Result};
use crate::kernel::io::{IoFactory, IoReader, IoType};
use crate::kernel::lsm::compactor::MergeShardingVec;
use crate::kernel::lsm::lsm_kv::Config;
use crate::kernel::lsm::ss_table::{Scope, SSTable};
use crate::kernel::utils::lru_cache::ShardingLruCache;

mod ss_table;
pub mod lsm_kv;
mod compactor;
mod version;
mod log;
mod mvcc;
mod block;

/// Value为此Key的Records(CommandData与seq_id)
pub(crate) type MemMap = SkipMap<Vec<u8>, (CommandData, i64)>;

/// seq_id的上限值
///
/// 用于默认的key的填充(补充使UserKey为高位，因此默认获取最新的seq_id数据)
const SEQ_MAX: [u8; 8] = [u8::MAX, u8::MAX, u8::MAX, u8::MAX, u8::MAX, u8::MAX, u8::MAX, 127];

/// Footer序列化长度定长
/// 注意Footer序列化时，需要使用类似BinCode这样的定长序列化框架，否则若类似Rmp的话会导致Footer在不同数据时，长度不一致
pub(crate) const TABLE_FOOTER_SIZE: usize = 21;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
struct Footer {
    level: u8,
    index_offset: u32,
    index_len: u32,
    meta_offset: u32,
    meta_len: u32,
    size_of_disk: u32,
}

#[derive(Serialize, Deserialize, Debug)]
struct MetaBlock {
    scope: Scope,
    filter: GrowableBloom,
    len: usize,
    index_restart_interval: usize,
    data_restart_interval: usize,
}

#[derive(Debug)]
pub(crate) struct MemTable {
    // 此读写锁锁仅用于切换mem_table和immut_table
    // 在大部分情况下应该算作为无锁(读锁并行)
    inner: RwLock<TableInner>
}

#[derive(Debug)]
pub(crate) struct TableInner {
    mem_table: MemMap,
    immut_table: Option<MemMap>
}

impl MemTable {
    pub(crate) fn new(mem_map: MemMap) -> Self {
        MemTable {
            inner: RwLock::new(
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
        config: &Config,
    ) -> Result<()> {
        // 将seq_id作为低位
        let seq_id = config.create_gen();
        let key = key_encode_with_seq(cmd.get_key_clone(), seq_id)?;

        self.inner.read()
            .mem_table.insert(key, (cmd, seq_id));

        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.inner.read()
            .mem_table.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.read()
            .mem_table.len()
    }

    /// 尝试判断至是否超出阈值并尝试获取写锁进行MemTable数据转移 (弹出数据为有序的)
    fn try_exceeded_then_swap(&self, exceeded: usize) -> Option<(Vec<CommandData>, i64)> {
        // 以try_write以判断是否存在事务进行读取而防止发送Minor Compaction而导致MemTable数据异常
        self.inner.try_write()
            .and_then(|mut inner| {
                if inner.mem_table.len() >= exceeded {
                    Self::swap_(&mut inner)
                } else { None }
            })
    }

    /// MemTable将数据弹出并转移到immutable中  (弹出数据为有序的)
    fn swap(&self) -> Option<(Vec<CommandData>, i64)> {
        let mut inner = self.inner.write();
        Self::swap_(&mut inner)
    }

    fn swap_(inner: &mut RwLockWriteGuard<RawRwLock, TableInner>) -> Option<(Vec<CommandData>, i64)> {
        (!inner.mem_table.is_empty())
            .then(|| {
                let mut last_seq_id = 0;
                let mut vec_data = inner.mem_table.iter()
                    .map(|entry| {
                        let (cmd, seq_id) = entry.value().clone();
                        if seq_id > last_seq_id {
                            last_seq_id = seq_id
                        }
                        cmd
                    })
                    // rev以使用最后(最新)的key
                    .rev()
                    .unique_by(CommandData::get_key_clone)
                    .collect_vec();

                vec_data.reverse();

                inner.immut_table = Some(mem::replace(
                    &mut inner.mem_table, SkipMap::new()
                ));
                (vec_data, last_seq_id)
            })
    }

    fn find(&self, key: &[u8]) -> Option<Vec<u8>> {
        // 填充SEQ_MAX使其变为最高位以尽可能获取最新数据
        let mut encode_key = key.to_vec();
        encode_key.append(
            &mut SEQ_MAX.to_vec()
        );
        let inner = self.inner.read();

        Self::find_(&encode_key, key, &inner.mem_table)
            .or_else(|| {
                inner.immut_table.as_ref()
                    .and_then(|mem_map| Self::find_(&encode_key, key, mem_map))
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
        Self::find_with_inner(key, sequence_id, &self.inner.read())
    }

    fn find_(seq_key: &[u8], user_key: &[u8], mem_map: &MemMap) -> Option<Vec<u8>> {
        mem_map.upper_bound(Bound::Included(seq_key))
            .and_then(|entry| {
                let (data, _) = entry.value();
                (user_key == data.get_key())
                    .then(|| data.get_value_clone())
            })
            .flatten()
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

pub(crate) struct SSTableMap {
    inner: HashMap<i64, SSTable>,
    cache: ShardingLruCache<i64, SSTable>
}

impl SSTableMap {
    pub(crate) fn new(config: &Config) -> Result<Self> {
        let cache = ShardingLruCache::new(
            config.table_cache_size,
            16,
            RandomState::default()
        )?;
        Ok(SSTableMap {
            inner: HashMap::new(),
            cache,
        })
    }

    pub(crate) fn insert(&mut self, ss_table: SSTable) -> Option<SSTable> {
        self.inner.insert(ss_table.get_gen(), ss_table)
    }

    pub(crate) fn get(&self, gen: &i64) -> Option<SSTable> {
        self.cache.get(gen)
            .or_else(|| self.inner.get(gen))
            .map(SSTable::clone)
    }

    /// 将指定gen通过MMapHandler进行读取以提高读取性能
    /// 创建MMapHandler成本较高，因此使用单独api控制缓存
    pub(crate) fn caching(&mut self, gen: i64, factory: &IoFactory) -> Result<Option<SSTable>> {
        let reader = factory.reader(gen, IoType::MMap)?;
        Ok(self.cache.put(gen, SSTable::load_from_file(reader)?))
    }

    pub(crate) fn remove(&mut self, gen: &i64) -> Option<SSTable> {
        let _ignore = self.cache.remove(gen);
        self.inner.remove(gen)
    }

    pub(crate) fn values(&self) -> Values<i64, SSTable> {
        self.inner.values()
    }

    pub(crate) fn iter(&self) -> Iter<i64, SSTable> {
        self.inner.iter()
    }

    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl Footer {

    /// 从对应文件的IOHandler中将Footer读取出来
    fn read_to_file(reader: &dyn IoReader) -> Result<Self> {
        let start_pos = reader.file_size()? - TABLE_FOOTER_SIZE as u64;
        Ok(bincode::deserialize(
            &reader.read_with_pos(start_pos, TABLE_FOOTER_SIZE)?
        )?)
    }
}

/// CommandData数据分片，尽可能将数据按给定的分片大小：file_size，填满一片（可能会溢出一些）
/// 保持原有数据的顺序进行分片，所有第一片分片中最后的值肯定会比其他分片开始的值Key排序较前（如果vec_data是以Key从小到大排序的话）
/// TODO: Block对齐封装,替代此方法
fn data_sharding(mut vec_data: Vec<CommandData>, file_size: usize, config: &Config) -> MergeShardingVec {
    // 向上取整计算SSTable数量
    let part_size = (vec_data.iter()
        .map(CommandData::bytes_len)
        .sum::<usize>() + file_size - 1) / file_size;

    vec_data.reverse();
    let mut vec_sharding = vec![(0, Vec::new()); part_size];
    let slice = vec_sharding.as_mut_slice();
    let vec_gen = config.create_gen_with_size(part_size);

    for i in 0 .. part_size {
        // 减小create_gen影响的时间
        slice[i].0 = vec_gen[i];
        let mut data_len = 0;
        while !vec_data.is_empty() {
            if let Some(cmd_data) = vec_data.pop() {
                data_len += cmd_data.bytes_len();
                if data_len >= file_size && i < part_size - 1 {
                    slice[i + 1].1.push(cmd_data);
                    break
                }
                slice[i].1.push(cmd_data);
            } else { break }
        }
    }
    // 过滤掉没有数据的切片
    vec_sharding.retain(|(_, vec)| !vec.is_empty());
    vec_sharding
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use crate::kernel::lsm::{MemMap, MemTable, Footer, TABLE_FOOTER_SIZE};
    use crate::kernel::lsm::lsm_kv::Config;
    use crate::kernel::{CommandData, Result};

    #[test]
    fn test_footer() -> Result<()> {
        let info = Footer {
            level: 0,
            index_offset: 0,
            index_len: 0,
            meta_offset: 0,
            meta_len: 0,
            size_of_disk: 0,
        };


        assert_eq!(bincode::serialize(&info)?.len(), TABLE_FOOTER_SIZE);

        Ok(())
    }

    #[test]
    fn test_mem_table_find() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let config = Config::new(temp_dir.into_path(), 0, 0);

        let mem_table = MemTable::new(MemMap::new());

        let data_1 = CommandData::set(vec![b'k'], vec![b'1']);
        let data_2 = CommandData::set(vec![b'k'], vec![b'2']);

        mem_table.insert_data(data_1.clone(), &config)?;

        let old_seq_id = config.create_gen_lazy();

        assert_eq!(mem_table.find(&vec![b'k']), Some(vec![b'1']));

        mem_table.insert_data(data_2.clone(), &config)?;

        assert_eq!(mem_table.find(&vec![b'k']), Some(vec![b'2']));

        assert_eq!(mem_table.find_with_sequence_id(&vec![b'k'], old_seq_id)?, Some(vec![b'1']));

        let new_seq_id = config.create_gen_lazy();

        assert_eq!(mem_table.find_with_sequence_id(&vec![b'k'], new_seq_id)?, Some(vec![b'2']));

        Ok(())
    }

    #[test]
    fn test_mem_table_swap() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let config = Config::new(temp_dir.into_path(), 0, 0);

        let mem_table = MemTable::new(MemMap::new());

        mem_table.insert_data(CommandData::set(vec![b'k', b'1'], vec![b'1']), &config)?;
        mem_table.insert_data(CommandData::set(vec![b'k', b'1'], vec![b'2']), &config)?;
        mem_table.insert_data(CommandData::set(vec![b'k', b'2'], vec![b'1']), &config)?;
        mem_table.insert_data(CommandData::set(vec![b'k', b'2'], vec![b'2']), &config)?;

        let (mut vec_unique_sort_with_cmd_key, _) = mem_table.swap().unwrap();

        assert_eq!(vec_unique_sort_with_cmd_key.pop(), Some(CommandData::set(vec![b'k', b'2'], vec![b'2'])));
        assert_eq!(vec_unique_sort_with_cmd_key.pop(), Some(CommandData::set(vec![b'k', b'1'], vec![b'2'])));

        Ok(())
    }
}