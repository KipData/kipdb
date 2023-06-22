use std::collections::hash_map::RandomState;
use std::mem;
use std::sync::Arc;
use growable_bloom_filter::GrowableBloom;
use itertools::Itertools;
use parking_lot::Mutex;
use tracing::{info, warn};
use crate::kernel::io::{IoFactory, IoType};
use crate::kernel::lsm::compactor::LEVEL_0;
use crate::kernel::lsm::log::LogLoader;
use crate::kernel::lsm::mem_table::{KeyValue, logs_decode};
use crate::kernel::lsm::ss_table::{Scope, SSTable, SSTableInner};
use crate::kernel::lsm::ss_table::block::{BlockBuilder, BlockOptions, CompressType, MetaBlock, Value};
use crate::kernel::lsm::ss_table::footer::{Footer, TABLE_FOOTER_SIZE};
use crate::kernel::lsm::ss_table::meta::SSTableMeta;
use crate::kernel::lsm::storage::Config;
use crate::kernel::utils::lru_cache::ShardingLruCache;
use crate::kernel::Result;

#[derive(Clone)]
pub(crate) struct SSTableLoader {
    inner: Arc<ShardingLruCache<i64, SSTable>>,
    factory: Arc<IoFactory>,
    config: Config,
    wal: LogLoader
}

impl SSTableLoader {
    pub(crate) fn new(config: Config, factory: Arc<IoFactory>, wal: LogLoader) -> Result<Self> {
        let inner = Arc::new(
            ShardingLruCache::new(
                config.table_cache_size,
                16,
                RandomState::default()
            )?
        );
        Ok(SSTableLoader { inner, factory, config, wal })
    }

    fn _create(
        &self,
        gen: i64,
        vec_data: Vec<KeyValue>,
        level: usize
    ) -> Result<SSTable> {
        fn io_type_with_level(level: usize) -> IoType {
            if LEVEL_0 == level {
                IoType::Mem
            } else {
                IoType::Direct
            }
        }

        let io_type = io_type_with_level(level);
        let io_factory = &self.factory;
        let config = &self.config;
        let len = vec_data.len();
        let data_restart_interval = config.data_restart_interval;
        let index_restart_interval = config.index_restart_interval;
        let mut filter = GrowableBloom::new(config.desired_error_prob, len);

        let mut builder = BlockBuilder::new(
            BlockOptions::from(config)
                .compress_type(CompressType::LZ4)
                .data_restart_interval(data_restart_interval)
                .index_restart_interval(index_restart_interval)
        );
        for data in vec_data {
            let (key, value) = data;
            let _ = filter.insert(&key);
            builder.add((key, Value::from(value)));
        }
        let meta = MetaBlock {
            filter,
            len,
            index_restart_interval,
            data_restart_interval,
        };

        let (data_bytes, index_bytes) = builder.build()?;
        let meta_bytes = bincode::serialize(&meta)?;
        let footer = Footer {
            level: level as u8,
            index_offset: data_bytes.len() as u32,
            index_len: index_bytes.len() as u32,
            meta_offset: (data_bytes.len() + index_bytes.len()) as u32,
            meta_len: meta_bytes.len() as u32,
            size_of_disk: (data_bytes.len() + index_bytes.len() + meta_bytes.len() + TABLE_FOOTER_SIZE) as u32,
        };
        let mut writer = io_factory.writer(gen, io_type)?;
        writer.write_all(
            data_bytes.into_iter()
                .chain(index_bytes)
                .chain(meta_bytes)
                .chain(bincode::serialize(&footer)?)
                .collect_vec()
                .as_mut()
        )?;
        writer.flush()?;
        info!("[SsTable: {}][create][MetaBlock]: {:?}", gen, meta);

        let reader = Mutex::new(io_factory.reader(gen, io_type)?);
        Ok(SSTable {
            inner: Arc::new(
                SSTableInner {
                    footer,
                    reader,
                    gen,
                    meta,
                }
            )
        })
    }

    pub(crate) fn create(
        &self,
        gen: i64,
        vec_data: Vec<KeyValue>,
        level: usize,
    ) -> Result<(Scope, SSTableMeta)> {
        // 获取数据的Key涵盖范围
        let scope = Scope::from_vec_data(gen, &vec_data)?;
        let ss_table = self._create(gen, vec_data, level)?;
        let sst_meta = SSTableMeta::from(&ss_table);
        let _ = self.inner.put(gen, ss_table);

        Ok((scope, sst_meta))
    }

    pub(crate) fn get(&self, gen: i64) -> Option<&SSTable> {
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
                    let reload_data = logs_decode(
                        self.wal.load(*gen, |bytes| Ok(mem::take(bytes)))?
                    )?.collect_vec();

                    self._create(*gen, reload_data, LEVEL_0)?
                }
            };

            Ok(ss_table)
        })
            .ok()
    }

    pub(crate) fn remove(&self, gen: &i64) -> Option<SSTable> {
        self.inner.remove(gen)
    }

    #[allow(dead_code)]
    pub(crate) fn is_emtpy(&self) -> bool {
        self.inner.is_empty()
    }

    pub(crate) fn clean(&self, gen: i64) -> Result<()> {
        let _ = self.remove(&gen);
        self.clean_only_sst(gen)?;
        self.clean_only_wal(gen)?;

        Ok(())
    }

    pub(crate) fn clean_only_wal(&self, gen: i64) -> Result<()> {
        self.wal.clean(gen)
    }

    pub(crate) fn clean_only_sst(&self, gen: i64) -> Result<()> {
        self.factory.clean(gen)
    }

    #[allow(dead_code)]
    pub(crate) fn is_sst_file_exist(&self, gen: i64) -> Result<bool> {
        self.factory.exists(gen)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;
    use std::sync::Arc;
    use bincode::Options;
    use bytes::Bytes;
    use tempfile::TempDir;
    use crate::kernel::io::{FileExtension, IoFactory, IoType};
    use crate::kernel::lsm::log::LogLoader;
    use crate::kernel::lsm::mem_table::{data_to_bytes, DEFAULT_WAL_PATH};
    use crate::kernel::lsm::ss_table::loader::SSTableLoader;
    use crate::kernel::lsm::storage::Config;
    use crate::kernel::lsm::version::DEFAULT_SS_TABLE_PATH;
    use crate::kernel::Result;
    use crate::kernel::utils::lru_cache::ShardingLruCache;

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

        let sst_loader = SSTableLoader::new(
            config,
            sst_factory.clone(),
            log_loader.clone()
        )?;

        let _ = sst_loader.create(1, vec_data.clone(), 0)?;

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
        sst_loader.clean_only_sst(1).unwrap();
        assert!(!sst_factory.exists(1).unwrap());

        let ss_table_backup = sst_loader.get(1).unwrap();

        assert_eq!(ss_table_backup.query_with_key(&repeat_data.0, &cache)?, repeat_data.1);
        for i in 1..times {
            assert_eq!(ss_table_backup.query_with_key(&vec_data[i].0, &cache)?, Some(value.clone()))
        }
        Ok(())
    }
}