use std::cmp::Ordering;
use std::collections::{Bound, HashMap};
use std::collections::hash_map::{Iter, RandomState, Values};
use std::mem;
use std::sync::Arc;
use crossbeam_skiplist::SkipMap;
use growable_bloom_filter::GrowableBloom;
use itertools::Itertools;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
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

/// Value为此Key的Records(CommandData与seq_id)
pub(crate) type MemMap = SkipMap<Vec<u8>, (CommandData, i64)>;

/// seq_id的上限值
///
/// 用于默认的key的填充(补充使UserKey为高位，因此默认获取最新的seq_id数据)
const SEQ_MAX: [u8; 8] = [u8::MAX, u8::MAX, u8::MAX, u8::MAX, u8::MAX, u8::MAX, u8::MAX, 127];

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
    // 此读写锁锁仅用于切换mem_table和immut_table
    // 在大部分情况下应该算作为无锁(读锁并行)
    inner: RwLock<TableInner>
}

#[derive(Debug)]
struct TableInner {
    mem_table: MemMap,
    immut_table: Option<Arc<MemMap>>
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
    pub(crate) fn insert_data_and_is_exceeded(
        &self,
        cmd: CommandData,
        config: &Config,
    ) -> bool {
        // 将seq_id作为低位
        let seq_id = config.create_gen();
        let key = Self::key_encode_with_seq(cmd.get_key_clone(), seq_id);

        let inner = self.inner.read();
        let _ignore = inner.mem_table.insert(key, (cmd, seq_id));

        inner.mem_table.len() >= config.minor_threshold_with_len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.inner.read()
            .mem_table.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.read()
            .mem_table.len()
    }

    /// MemTable交换并分解,弹出有序数据
    fn table_swap_and_sort(&self) -> Option<(Vec<CommandData>, i64)> {
        // TODO: 事务开启后需要持有读锁
        // 以try_write以判断是否存在事务进行读取而防止发送Minor Compaction而导致MemTable数据异常
        if let Some(mut inner) = self.inner.try_write() {
            let mut last_seq_id = 0;
            let vec_data = inner.mem_table.iter()
                .map(|entry| {
                    let (cmd, seq_id) = entry.value().clone();
                    if seq_id > last_seq_id {
                        last_seq_id = seq_id
                    }
                    cmd
                })
                .collect_vec();

            inner.immut_table = Some(Arc::new(
                mem::replace(&mut inner.mem_table, SkipMap::new())
            ));

            Some((vec_data, last_seq_id)  )
        } else { None }
    }

    fn find(&self, key: &[u8]) -> Option<Vec<u8>> {
        let inner = self.inner.read();

        // 填充SEQ_MAX使其变为最高位以尽可能获取最新数据
        let mut encode_key = key.to_vec();
        encode_key.append(
            &mut SEQ_MAX.to_vec()
        );

        Self::find_(&encode_key, key, &inner.mem_table)
            .or_else(|| {
                inner.immut_table.as_ref()
                    .map(|mem_map|
                        Self::find_(&encode_key, key, &mem_map))
                    .flatten()
            })
    }

    /// 查询时附带sequence_id进行历史数据查询
    #[allow(dead_code)]
    fn find_with_sequence_id(&self, key: &[u8], sequence_id: i64) -> Option<Vec<u8>> {
        let inner = self.inner.read();

        return find_with_seq_(key, sequence_id, &inner.mem_table)
            .or_else(|| {
                inner.immut_table.as_ref()
                    .map(|mem_map| find_with_seq_(key, sequence_id, &mem_map))
                    .flatten()
            });


        /// 通过sequence_id对mem_table的数据进行获取
        ///
        /// 获取第一个小于等于sequence_id的数据
        fn find_with_seq_(key: &[u8], sequence_id: i64, mem_map: &MemMap) -> Option<Vec<u8>> {
            MemTable::find_(
                &MemTable::key_encode_with_seq(key.to_vec(), sequence_id),
                key,
                mem_map
            )
        }
    }

    fn find_(seq_key: &[u8], user_key: &[u8], mem_map: &MemMap) -> Option<Vec<u8>> {
        mem_map.upper_bound(Bound::Included(seq_key))
            .map(|entry| {
                let (data, _) = entry.value();
                if user_key == data.get_key() {
                    data.get_value_clone()
                } else { None }
            })
            .flatten()
    }

    /// 将key与seq_id进行混合编码
    ///
    /// key在高位，seq_id在低位
    fn key_encode_with_seq(mut key: Vec<u8>, seq_id: i64) -> Vec<u8> {
        key.append(
            &mut bincode::serialize(&seq_id).unwrap()
        );
        key
    }
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
    pub(crate) fn from_sparse_index_with_key(sparse_index: &SkipMap<Vec<u8>, Position>, key: &[u8]) -> Option<Self> {
        sparse_index.into_iter()
            .rev()
            .find(|entry| !key.cmp(entry.key()).eq(&Ordering::Less))
            .map(|entry| entry.value().clone())
    }
}

/// CommandData数据分片，尽可能将数据按给定的分片大小：file_size，填满一片（可能会溢出一些）
/// 保持原有数据的顺序进行分片，所有第一片分片中最后的值肯定会比其他分片开始的值Key排序较前（如果vec_data是以Key从小到大排序的话）
/// TODO: Block对齐封装,替代此方法
fn data_sharding(mut vec_data: Vec<CommandData>, file_size: usize, config: &Config) -> MergeShardingVec {
    // 向上取整计算STable数量
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

#[test]
fn test_mem_table_find() -> Result<()> {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let config = Config::new(temp_dir.into_path(), 0, 0);

    let mem_table = MemTable::new(MemMap::new());

    let data_1 = CommandData::set(vec![b'k'], vec![b'1']);
    let data_2 = CommandData::set(vec![b'k'], vec![b'2']);

    assert!(!mem_table.insert_data_and_is_exceeded(data_1.clone(), &config));

    let old_seq_id = config.create_gen_lazy();

    assert_eq!(mem_table.find(&vec![b'k']), Some(vec![b'1']));

    assert!(!mem_table.insert_data_and_is_exceeded(data_2.clone(), &config));

    assert_eq!(mem_table.find(&vec![b'k']), Some(vec![b'2']));

    assert_eq!(mem_table.find_with_sequence_id(&vec![b'k'], old_seq_id), Some(vec![b'1']));

    let new_seq_id = config.create_gen_lazy();

    assert_eq!(mem_table.find_with_sequence_id(&vec![b'k'], new_seq_id), Some(vec![b'2']));

    Ok(())
}