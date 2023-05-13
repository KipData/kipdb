use std::collections::hash_map::RandomState;
use std::io::SeekFrom;
use std::sync::Arc;
use growable_bloom_filter::GrowableBloom;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tracing::warn;
use crate::kernel::Result;
use crate::kernel::io::{IoFactory, IoReader, IoType};
use crate::kernel::lsm::compactor::{CompactTask, LEVEL_0, MergeShardingVec};
use crate::kernel::lsm::log::LogLoader;
use crate::kernel::lsm::lsm_kv::{Config, Gen};
use crate::kernel::lsm::mem_table::{key_value_bytes_len, KeyValue, MemTable};
use crate::kernel::lsm::ss_table::SSTable;
use crate::kernel::utils::lru_cache::ShardingLruCache;
use crate::KernelError;

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
    filter: GrowableBloom,
    len: usize,
    index_restart_interval: usize,
    data_restart_interval: usize,
}

pub(crate) struct SSTableLoader {
    inner: ShardingLruCache<i64, SSTable>,
    factory: Arc<IoFactory>,
    config: Config,
    wal: LogLoader
}

impl SSTableLoader {
    pub(crate) fn new(config: Config, factory: Arc<IoFactory>, wal: LogLoader) -> Result<Self> {
        let inner = ShardingLruCache::new(
            config.table_cache_size,
            16,
            RandomState::default()
        )?;
        Ok(SSTableLoader { inner, factory, config, wal })
    }

    pub(crate) fn insert(&mut self, ss_table: SSTable) -> Option<SSTable> {
        self.inner.put(ss_table.get_gen(), ss_table)
    }

    pub(crate) fn get(&self, gen: i64) -> Option<SSTable> {
        self.inner.get_or_insert(gen, |gen| {
            let sst_factory = &self.factory;

            let ss_table = match sst_factory.reader(*gen, IoType::Direct)
                .and_then(SSTable::load_from_file)
            {
                Ok(ss_table) => ss_table,
                Err(err) => {
                    warn!(
                        "[LSMStore][Load SSTable: {}][try to reload with wal]: {:?}",
                        gen, err
                    );
                    let mut reload_data = LogLoader::load(
                        &self.wal,
                        *gen,
                        MemTable::key_value_decode
                    )?.into_iter()
                        .rev()
                        .unique_by(|(key, _)| key.clone())
                        .collect_vec();
                    reload_data.sort_by_key(|(key, _)| key.clone());

                    SSTable::create_for_mem_table(
                        &self.config,
                        *gen,
                        sst_factory,
                        reload_data,
                        LEVEL_0,
                        IoType::Direct
                    )?.0
                }
            };

            Ok(ss_table)
        })
            .map(SSTable::clone)
            .ok()
    }

    pub(crate) fn remove(&mut self, gen: &i64) -> Option<SSTable> {
        self.inner.remove(gen)
    }

    #[allow(dead_code)]
    pub(crate) fn is_emtpy(&self) -> bool {
        self.inner.is_empty()
    }

    pub(crate) fn clean(&self, gen: i64) -> Result<()> {
        self.factory.clean(gen)
    }
}

impl Footer {
    /// 从对应文件的IOHandler中将Footer读取出来
    fn read_to_file(reader: &mut dyn IoReader) -> Result<Self> {
        let mut buf = [0; TABLE_FOOTER_SIZE];

        let _ = reader.seek(SeekFrom::End( -(TABLE_FOOTER_SIZE as i64)))?;
        let _ = reader.read(&mut buf)?;

        Ok(bincode::deserialize(&buf)?)
    }
}

/// KeyValue数据分片，尽可能将数据按给定的分片大小：file_size，填满一片（可能会溢出一些）
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

fn is_exceeded_then_minor(
    data_len: usize,
    tx: &UnboundedSender<CompactTask>,
    config: &Config
) -> Result<()> {
    if data_len >= config.minor_threshold_with_len {
        tx.send(CompactTask::Flush(None))
            .map_err(|_| KernelError::ChannelClose)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;
    use std::sync::Arc;
    use bincode::Options;
    use bytes::Bytes;
    use tempfile::TempDir;
    use crate::kernel::io::{FileExtension, IoFactory, IoType};
    use crate::kernel::lsm::{Footer, SSTableLoader, TABLE_FOOTER_SIZE};
    use crate::kernel::lsm::log::LogLoader;
    use crate::kernel::lsm::lsm_kv::Config;
    use crate::kernel::lsm::mem_table::{data_to_bytes, DEFAULT_WAL_PATH};
    use crate::kernel::lsm::ss_table::SSTable;
    use crate::kernel::lsm::version::DEFAULT_SS_TABLE_PATH;
    use crate::kernel::Result;
    use crate::kernel::utils::lru_cache::ShardingLruCache;

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
    fn test_ss_table_loader() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let value = Bytes::copy_from_slice(b"If you shed tears when you miss the sun, you also miss the stars.");
        let config = Config::new(temp_dir.into_path());
        let sst_factory = Arc::new(IoFactory::new(
            config.dir_path.join(DEFAULT_SS_TABLE_PATH),
            FileExtension::SSTable
        )?);
        let cache = ShardingLruCache::new(
            config.table_cache_size,
            16,
            RandomState::default()
        )?;
        let mut vec_data = Vec::new();
        let times = 2333;
        let (log_loader, _, _) = LogLoader::reload(
            config.path(),
            (DEFAULT_WAL_PATH, Some(1)),
            IoType::Buf,
            |_| Ok(())
        )?;
        let mut log_writer = log_loader.writer(1)?;

        // 填充测试数据
        for i in 0..times {
            let key_value = (Bytes::from(bincode::options().with_big_endian().serialize(&i)?), Some(value.clone()));

            let _ = log_writer.add_record(&data_to_bytes(key_value.clone())?)?;
            vec_data.push(key_value);
        }
        // 测试重复数据是否被正常覆盖
        let repeat_data = (vec_data[0].0.clone(), None);
        let _ = log_writer.add_record(&data_to_bytes(repeat_data.clone())?)?;
        vec_data[0] = repeat_data.clone();

        log_writer.flush()?;

        let (ss_table, _) = SSTable::create_for_mem_table(
            &config,
            1,
            &sst_factory,
            vec_data.clone(),
            0,
            IoType::Direct
        )?;
        let mut sst_loader = SSTableLoader::new(
            config,
            sst_factory.clone(),
            log_loader.clone()
        )?;

        assert!(sst_loader.insert(ss_table).is_none());
        assert!(sst_loader.remove(&1).is_some());
        assert!(sst_loader.is_emtpy());

        let ss_table_loaded = sst_loader.get(1).unwrap();

        assert_eq!(ss_table_loaded.query_with_key(&repeat_data.0, &cache)?, repeat_data.1);
        for i in 1..times {
            assert_eq!(ss_table_loaded.query_with_key(&vec_data[i].0, &cache)?, Some(value.clone()))
        }

        // 模拟SSTable异常而使用Wal进行恢复的情况
        assert!(sst_loader.remove(&1).is_some());
        assert!(sst_loader.is_emtpy());
        sst_loader.clean(1).unwrap();
        assert!(!sst_factory.exists(1).unwrap());

        let ss_table_backup = sst_loader.get(1).unwrap();

        assert_eq!(ss_table_backup.query_with_key(&repeat_data.0, &cache)?, repeat_data.1);
        for i in 1..times {
            assert_eq!(ss_table_backup.query_with_key(&vec_data[i].0, &cache)?, Some(value.clone()))
        }
        Ok(())
    }
}