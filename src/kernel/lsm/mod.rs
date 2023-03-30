use std::collections::HashMap;
use std::collections::hash_map::{Iter, RandomState, Values};
use growable_bloom_filter::GrowableBloom;
use serde::{Deserialize, Serialize};
use crate::kernel::Result;
use crate::kernel::io::{IoFactory, IoReader, IoType};
use crate::kernel::lsm::compactor::MergeShardingVec;
use crate::kernel::lsm::lsm_kv::{Config, Gen};
use crate::kernel::lsm::mem_table::{key_value_bytes_len, KeyValue};
use crate::kernel::lsm::ss_table::{Scope, SSTable};
use crate::kernel::utils::lru_cache::ShardingLruCache;

mod ss_table;
pub mod lsm_kv;
mod compactor;
mod version;
mod log;
mod mvcc;
mod block;
mod mem_table;
mod iterator;

/// Footer序列化长度定长
/// 注意Footer序列化时，需要使用类似BinCode这样的定长序列化框架，否则若类似Rmp的话会导致Footer在不同数据时，长度不一致
pub(crate) const TABLE_FOOTER_SIZE: usize = 21;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[repr(C, align(32))]
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

pub(crate) struct SSTableMap {
    inner: HashMap<i64, SSTable>,
    cache: ShardingLruCache<i64, SSTable>
}

impl SSTableMap {
    pub(crate) fn new(config: Config) -> Result<Self> {
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
fn data_sharding(mut vec_data: Vec<KeyValue>, file_size: usize) -> MergeShardingVec {
    // 向上取整计算SSTable数量
    let part_size = (vec_data.iter()
        .map(key_value_bytes_len)
        .sum::<usize>() + file_size - 1) / file_size;

    vec_data.reverse();
    let mut vec_sharding = vec![(0, Vec::new()); part_size];
    let slice = vec_sharding.as_mut_slice();

    for i in 0 .. part_size {
        // 减小create_gen影响的时间
        slice[i].0 = Gen::create();
        let mut data_len = 0;
        while !vec_data.is_empty() {
            if let Some(key_value) = vec_data.pop() {
                data_len += key_value_bytes_len(&key_value);
                if data_len >= file_size && i < part_size - 1 {
                    slice[i + 1].1.push(key_value);
                    break
                }
                slice[i].1.push(key_value);
            } else { break }
        }
    }
    // 过滤掉没有数据的切片
    vec_sharding.retain(|(_, vec)| !vec.is_empty());
    vec_sharding
}

#[cfg(test)]
mod tests {
    use crate::kernel::lsm::{Footer, TABLE_FOOTER_SIZE};
    use crate::kernel::Result;

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
}