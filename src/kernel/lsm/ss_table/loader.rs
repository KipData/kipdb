use std::collections::hash_map::RandomState;
use std::sync::Arc;
use itertools::Itertools;
use tracing::warn;
use crate::kernel::io::{IoFactory, IoType};
use crate::kernel::lsm::compactor::LEVEL_0;
use crate::kernel::lsm::log::LogLoader;
use crate::kernel::lsm::mem_table::logs_decode;
use crate::kernel::lsm::ss_table::SSTable;
use crate::kernel::lsm::storage::Config;
use crate::kernel::utils::lru_cache::ShardingLruCache;
use crate::kernel::Result;

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

    pub(crate) fn insert(&self, ss_table: SSTable) -> Option<SSTable> {
        self.inner.put(ss_table.get_gen(), ss_table)
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
                        self.wal.load(*gen, |bytes| Ok(bytes.clone()))?
                    )?.collect_vec();

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

    #[allow(dead_code)]
    pub(crate) fn clean_only_wal(&self, gen: i64) -> Result<()> {
        self.wal.clean(gen)
    }

    pub(crate) fn clean_only_sst(&self, gen: i64) -> Result<()> {
        self.factory.clean(gen)
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
    use crate::kernel::lsm::ss_table::SSTable;
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

        let (ss_table, _) = SSTable::create_for_mem_table(
            &config,
            1,
            &sst_factory,
            vec_data.clone(),
            0,
            IoType::Direct
        )?;
        let sst_loader = SSTableLoader::new(
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