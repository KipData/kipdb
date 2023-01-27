use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::hash_map::{Iter, RandomState, Values};
use growable_bloom_filter::GrowableBloom;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use skiplist::SkipMap;
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
pub(crate) type MemMap = SkipMap<Vec<u8>, Vec<(CommandData, i64)>>;

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
    mem_table_slice: RwLock<[(MemMap, u64, i64); 2]>
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
    /// 初始化时进行last_seq_id的建立和数据大小统计
    pub(crate) fn new(mem_map: MemMap) -> Self {
        let mut last_seq_id: i64 = 0;
        let mem_occupied = mem_map.iter()
            .map(|(key, vec_record)| {
                vec_record.into_iter()
                    .map(|(record, record_seq_id)| {
                        //找出最新seq_id
                        if &last_seq_id < record_seq_id {
                            last_seq_id = *record_seq_id;
                        }
                        record.bytes_len() as u64
                    })
                    .sum::<u64>() + key.len() as u64
            })
            .sum();
        MemTable {
            mem_table_slice: RwLock::new([
                (mem_map, mem_occupied, last_seq_id),
                (MemMap::new(), 0, 0)
            ])
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
        let mut mem_table_slice = self.mem_table_slice.write();
        let sequence_id = config.create_gen();

        let key = cmd.get_key_clone();

        mem_table_slice[0].2 = sequence_id;
        mem_table_slice[0].1 += (key.len() + cmd.bytes_len()) as u64;

        match mem_table_slice[0].0.get_mut(&key) {
            None => {
                let _ignore = mem_table_slice[0].0.insert(key, vec![(cmd, sequence_id)]);
            },
            Some(vec_record) => {
                vec_record.push((cmd, sequence_id));
            }
        }

        mem_table_slice[0].1 >= config.minor_threshold_with_data_size
    }

    pub(crate) fn mem_table_is_empty(&self) -> bool {
        let mem_table_slice = self.mem_table_slice.read();

        mem_table_slice[0].0.is_empty()
    }

    pub(crate) fn mem_table_len(&self) -> usize {
        let mem_table_slice = self.mem_table_slice.read();

        mem_table_slice[0].0.len()
    }

    #[allow(dead_code)]
    fn is_threshold_exceeded_minor(&self, threshold_size_with_mem_table: u64) -> bool {
        self.mem_table_slice
            .read()[0].1 > threshold_size_with_mem_table
    }

    /// MemTable交换并分解,弹出有序数据
    fn table_swap_and_sort(&self) -> (Vec<CommandData>, i64) {
        let mut mem_table_slice = self.mem_table_slice.write();

        mem_table_slice.swap(0, 1);
        mem_table_slice[0] = (MemMap::new(), 0, 0);
        // 这里看起来需要clone所有的value看起来开销很大，不过实际上CommandData的Value是使用Arc指针封装
        // SkipMap的values自带排序
        (mem_table_slice[1].0.values()
            .filter_map(|vec_record| {
                // 只获取最新的值，其他旧值抛弃
                vec_record.last()
                    .map(|(record, _)| record)
            })
            .cloned()
            .collect(), mem_table_slice[1].2)
    }

    fn find(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mem_table_slice = self.mem_table_slice.read();

        Self::find_(key, &mem_table_slice[0].0)
            .or_else(|| Self::find_(key, &mem_table_slice[1].0))
    }

    /// 查询时附带sequence_id进行历史数据查询
    #[allow(dead_code)]
    fn find_with_sequence_id(&self, key: &[u8], sequence_id: i64) -> Option<Vec<u8>> {
        let mem_table_slice = self.mem_table_slice.read();

        return find_with_seq_(key, sequence_id, &mem_table_slice[0])
            .or_else(|| find_with_seq_(key, sequence_id, &mem_table_slice[1]));


        /// 通过sequence_id对mem_table的数据进行获取
        ///
        /// 获取第一个小于等于sequence_id的数据
        fn find_with_seq_(key: &[u8], sequence_id: i64, mem_table: &(MemMap, u64, i64)) -> Option<Vec<u8>> {
            let (mem_map, _, last_seq_id) = mem_table;
            if &sequence_id < last_seq_id {
                return mem_map.get(key)
                    .unwrap_or(&vec![])
                    .into_iter()
                    .rfind(|(_, record_seq_id)| &sequence_id >= record_seq_id)
                    .map(|(record, _)| record.get_value_clone())
                    .flatten();
            } else {
                MemTable::find_(key, mem_map)
            }
        }
    }

    fn find_(key: &[u8], mem_map: &MemMap) -> Option<Vec<u8>> {
        mem_map.get(key)
            .and_then(|vec_record| {
                vec_record.last()
                    .map(|(record, _)| record.get_value_clone())
                    .flatten()
            })
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