use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::hash_map::{Iter, RandomState, Values};
use growable_bloom_filter::GrowableBloom;
use serde::{Deserialize, Serialize};
use skiplist::SkipMap;
use tokio::sync::RwLock;
use crate::kernel::{CommandData, Result};
use crate::kernel::io::{IOHandler, IOHandlerFactory, IOType};
use crate::kernel::lsm::compactor::MergeShardingVec;
use crate::kernel::lsm::lsm_kv::Config;
use crate::kernel::lsm::ss_table::{Scope, SSTable};
use crate::kernel::utils::lru_cache::ShardingLruCache;

mod ss_table;
pub mod lsm_kv;
mod compactor;
mod version;
mod log;

pub(crate) type MemMap = SkipMap<Vec<u8>, CommandData>;

/// MetaInfo序列化长度定长
/// 注意MetaInfo序列化时，需要使用类似BinCode这样的定长序列化框架，否则若类似Rmp的话会导致MetaInfo在不同数据时，长度不一致
const TABLE_META_INFO_SIZE: usize = 28;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
struct MetaInfo {
    level: u64,
    data_part_len: u64,
    index_len: u64,
    crc_code: u32,
}

#[derive(Serialize, Deserialize)]
struct MetaBlock {
    vec_index: Vec<(Vec<u8>, Position)>,
    scope: Scope,
    filter: GrowableBloom,
    len: usize,
}

#[derive(Debug)]
struct MemTable {
    // MemTable切片，管理MemTable和ImmutableMemTable
    mem_table_slice: RwLock<[(MemMap, u64); 2]>
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Hash, PartialOrd, Eq)]
pub(crate) struct Position {
    start: u64,
    len: usize
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

    pub(crate) async fn insert(&mut self, ss_table: SSTable) -> Option<SSTable> {
        self.inner.insert(ss_table.get_gen(), ss_table)
    }

    pub(crate) async fn get(&self, gen: &i64) -> Option<SSTable> {
        self.cache.get(gen).await
            .or_else(|| self.inner.get(gen))
            .map(SSTable::clone)
    }

    /// 将指定gen通过MMapHandler进行读取以提高读取性能
    /// 创建MMapHandler成本较高，因此使用单独api控制缓存
    pub(crate) async fn caching(&mut self, gen: i64, factory: &IOHandlerFactory) -> Result<Option<SSTable>> {
        let mmap_handler = factory.create(gen.clone(), IOType::MMapOnlyReader)?;
        Ok(self.cache.put(
            gen,
            SSTable::load_from_file(mmap_handler).await?
        ).await)
    }

    pub(crate) async fn remove(&mut self, gen: &i64) -> Option<SSTable> {
        let _ignore = self.cache.remove(gen).await;
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

impl MemTable {
    pub(crate) fn new(mem_map: MemMap) -> Self {
        let mem_occupied = mem_map.iter()
            .map(|(key, value)| {
                (key.len() + value.bytes_len()) as u64
            })
            .sum();
        MemTable { mem_table_slice: RwLock::new([(mem_map, mem_occupied), (MemMap::new(), 0)]) }
    }

    pub(crate) async fn insert_data(&self, key: Vec<u8>, value: CommandData) {
        let mut mem_table_slice = self.mem_table_slice.write().await;

        mem_table_slice[0].1 += (key.len() + value.bytes_len()) as u64;
        let _ignore = mem_table_slice[0].0.insert(key, value);
    }

    pub(crate) async fn mem_table_is_empty(&self) -> bool {
        let mem_table_slice = self.mem_table_slice.read().await;

        mem_table_slice[0].0.is_empty()
    }

    pub(crate) async fn mem_table_len(&self) -> usize {
        let mem_table_slice = self.mem_table_slice.read().await;

        mem_table_slice[0].0.len()
    }

    async fn is_threshold_exceeded_minor(&self, threshold_size_with_mem_table: u64) -> bool {
        self.mem_table_slice
            .read()
            .await[0].1 > threshold_size_with_mem_table
    }

    /// MemTable交换并分解
    async fn table_swap(&self) -> Vec<CommandData>{
        let mut mem_table_slice = self.mem_table_slice.write().await;

        mem_table_slice.swap(0, 1);
        mem_table_slice[0] = (MemMap::new(), 0);
        // 这里看起来需要clone所有的value看起来开销很大，不过实际上CommandData的Value是使用Arc指针封装
        mem_table_slice[1].0.values()
            .cloned()
            .collect()
    }

    async fn get_cmd_data(&self, key: &[u8]) -> Option<CommandData> {
        let mem_table_slice = self.mem_table_slice.read().await;

        mem_table_slice[0].0.get(key)
            .or_else(|| mem_table_slice[1].0.get(key))
            .map(CommandData::clone)
    }
}

impl MetaInfo {
    /// 将MetaInfo自身写入对应的IOHandler之中
    async fn write_to_file_and_flush(&self, io_handler: &Box<dyn IOHandler>) -> Result<()>{
        let _ignore = io_handler.write(bincode::serialize(&self)?).await?;
        io_handler.flush().await?;
        Ok(())
    }

    /// 从对应文件的IOHandler中将MetaInfo读取出来
    async fn read_to_file(io_handler: &Box<dyn IOHandler>) -> Result<Self> {
        let start_pos = io_handler.file_size()? - TABLE_META_INFO_SIZE as u64;
        let table_meta_info = io_handler.read_with_pos(start_pos, TABLE_META_INFO_SIZE).await?;

        Ok(bincode::deserialize(table_meta_info.as_slice())?)
    }
}

impl Position {
    /// 通过稀疏索引与指定Key进行获取对应Position
    pub(crate) fn from_sparse_index_with_key<'a>(sparse_index: &'a SkipMap<Vec<u8>, Position>, key: &'a [u8]) -> Option<&'a Self> {
        sparse_index.into_iter()
            .rev()
            .find(|(key_item, _)| !key.cmp(key_item).eq(&Ordering::Less))
            .map(|(_, value_item)| value_item)
    }
}

/// CommandData数据分片，尽可能将数据按给定的分片大小：file_size，填满一片（可能会溢出一些）
/// 保持原有数据的顺序进行分片，所有第一片分片中最后的值肯定会比其他分片开始的值Key排序较前（如果vec_data是以Key从小到大排序的话）
async fn data_sharding(mut vec_data: Vec<CommandData>, file_size: usize, config: &Config, with_gen: bool) -> MergeShardingVec {
    // 向上取整计算STable数量
    let part_size = (vec_data.iter()
        .map(CommandData::bytes_len)
        .sum::<usize>() + file_size - 1) / file_size;

    vec_data.reverse();
    let mut vec_sharding = vec![(0, Vec::new()); part_size];
    let slice = vec_sharding.as_mut_slice();
    for i in 0 .. part_size {
        // 减小create_gen影响的时间
        if with_gen {
            slice[i].0 = config.create_gen()
        }
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

#[test]
fn test_meta_info() -> Result<()> {
    let info = MetaInfo {
        level: 0,
        data_part_len: 0,
        index_len: 0,
        crc_code: 0
    };

    let vec_u8 = bincode::serialize(&info)?;

    assert_eq!(vec_u8.len(), TABLE_META_INFO_SIZE);

    Ok(())
}