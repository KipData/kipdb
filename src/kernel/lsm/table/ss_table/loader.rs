use crate::kernel::io::{IoFactory, IoType};
use crate::kernel::lsm::compactor::LEVEL_0;
use crate::kernel::lsm::log::LogLoader;
use crate::kernel::lsm::mem_table::{logs_decode, KeyValue};
use crate::kernel::lsm::storage::Config;
use crate::kernel::lsm::table::meta::TableMeta;
use crate::kernel::lsm::table::scope::Scope;
use crate::kernel::lsm::table::skip_table::SkipTable;
use crate::kernel::lsm::table::ss_table::block::BlockCache;
use crate::kernel::lsm::table::ss_table::SSTable;
use crate::kernel::lsm::table::{BoxTable, Table};
use crate::kernel::utils::lru_cache::ShardingLruCache;
use crate::kernel::Result;
use bytes::Bytes;
use itertools::Itertools;
use std::collections::hash_map::RandomState;
use std::mem;
use std::sync::Arc;
use tracing::warn;

#[derive(Copy, Clone, Debug)]
pub enum TableType {
    SortedString,
    #[allow(dead_code)]
    Skip,
}

#[derive(Clone)]
pub(crate) struct TableLoader {
    inner: Arc<ShardingLruCache<i64, BoxTable>>,
    factory: Arc<IoFactory>,
    config: Config,
    wal: LogLoader,
    cache: Arc<BlockCache>,
}

impl TableLoader {
    pub(crate) fn new(config: Config, factory: Arc<IoFactory>, wal: LogLoader) -> Result<Self> {
        let inner = Arc::new(ShardingLruCache::new(
            config.table_cache_size,
            16,
            RandomState::default(),
        )?);
        let cache = Arc::new(ShardingLruCache::new(
            config.block_cache_size,
            16,
            RandomState::default(),
        )?);
        Ok(TableLoader {
            inner,
            factory,
            config,
            wal,
            cache,
        })
    }

    pub(crate) fn create(
        &self,
        gen: i64,
        vec_data: Vec<KeyValue>,
        level: usize,
        table_type: TableType,
    ) -> Result<(Scope, TableMeta)> {
        // 获取数据的Key涵盖范围
        let scope = Scope::from_vec_data(gen, &vec_data)?;
        let table: Box<dyn Table> = match table_type {
            TableType::SortedString => Box::new(self.create_ss_table(gen, vec_data, level)?),
            TableType::Skip => Box::new(SkipTable::new(level, gen, vec_data)),
        };
        let table_meta = TableMeta::from(table.as_ref());
        let _ = self.inner.put(gen, table);

        Ok((scope, table_meta))
    }

    pub(crate) fn get(&self, gen: i64) -> Option<&dyn Table> {
        self.inner
            .get_or_insert(gen, |gen| {
                let sst_factory = &self.factory;

                let ss_table = match sst_factory
                    .reader(*gen, IoType::Direct)
                    .and_then(|reader| SSTable::load_from_file(reader, Arc::clone(&self.cache)))
                {
                    Ok(ss_table) => ss_table,
                    Err(err) => {
                        // 尝试恢复仅对Level 0的Table有效
                        warn!(
                            "[LSMStore][Load Table: {}][try to reload with wal]: {:?}",
                            gen, err
                        );
                        let reload_data =
                            logs_decode(self.wal.load(*gen, |bytes| Ok(mem::take(bytes)))?)?
                                .collect_vec();

                        self.create_ss_table(*gen, reload_data, LEVEL_0)?
                    }
                };

                Ok(Box::new(ss_table))
            })
            .map(Box::as_ref)
            .ok()
    }

    fn create_ss_table(
        &self,
        gen: i64,
        reload_data: Vec<(Bytes, Option<Bytes>)>,
        level: usize,
    ) -> Result<SSTable> {
        SSTable::new(
            &self.factory,
            &self.config,
            Arc::clone(&self.cache),
            gen,
            reload_data,
            level,
            IoType::Direct,
        )
    }

    pub(crate) fn remove(&self, gen: &i64) -> Option<BoxTable> {
        self.inner.remove(gen)
    }

    #[allow(dead_code)]
    pub(crate) fn is_emtpy(&self) -> bool {
        self.inner.is_empty()
    }

    pub(crate) fn clean(&self, gen: i64) -> Result<()> {
        let _ = self.remove(&gen);
        self.factory.clean(gen)?;
        self.wal.clean(gen)?;

        Ok(())
    }

    // Tips: 仅仅对持久化Table有效，SkipTable类内存Table始终为false
    #[allow(dead_code)]
    pub(crate) fn is_table_file_exist(&self, gen: i64) -> Result<bool> {
        self.factory.exists(gen)
    }
}

#[cfg(test)]
mod tests {
    use crate::kernel::io::{FileExtension, IoFactory, IoType};
    use crate::kernel::lsm::log::LogLoader;
    use crate::kernel::lsm::mem_table::{data_to_bytes, DEFAULT_WAL_PATH};
    use crate::kernel::lsm::storage::Config;
    use crate::kernel::lsm::table::ss_table::loader::{TableLoader, TableType};
    use crate::kernel::lsm::version::DEFAULT_SS_TABLE_PATH;
    use crate::kernel::Result;
    use bincode::Options;
    use bytes::Bytes;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_ss_table_loader() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let value = Bytes::copy_from_slice(
            b"If you shed tears when you miss the sun, you also miss the stars.",
        );
        let config = Config::new(temp_dir.into_path());
        let sst_factory = Arc::new(IoFactory::new(
            config.dir_path.join(DEFAULT_SS_TABLE_PATH),
            FileExtension::SSTable,
        )?);
        let mut vec_data = Vec::new();
        let times = 2333;
        let (log_loader, _, _) = LogLoader::reload(
            config.path(),
            (DEFAULT_WAL_PATH, Some(1)),
            IoType::Buf,
            |_| Ok(()),
        )?;
        let mut log_writer = log_loader.writer(1)?;

        // 填充测试数据
        for i in 0..times {
            let key_value = (
                Bytes::from(bincode::options().with_big_endian().serialize(&i)?),
                Some(value.clone()),
            );

            let _ = log_writer.add_record(&data_to_bytes(key_value.clone())?)?;
            vec_data.push(key_value);
        }
        // 测试重复数据是否被正常覆盖
        let repeat_data = (vec_data[0].0.clone(), None);
        let _ = log_writer.add_record(&data_to_bytes(repeat_data.clone())?)?;
        vec_data[0] = repeat_data.clone();

        log_writer.flush()?;

        let sst_loader = TableLoader::new(config, sst_factory.clone(), log_loader.clone())?;

        let _ = sst_loader.create(1, vec_data.clone(), 0, TableType::SortedString)?;

        assert!(sst_loader.remove(&1).is_some());
        assert!(sst_loader.is_emtpy());

        let ss_table_loaded = sst_loader.get(1).unwrap();

        assert_eq!(ss_table_loaded.query(&repeat_data.0)?, repeat_data.1);
        for i in 1..times {
            assert_eq!(ss_table_loaded.query(&vec_data[i].0)?, Some(value.clone()))
        }

        // 模拟SSTable异常而使用Wal进行恢复的情况
        assert!(sst_loader.remove(&1).is_some());
        assert!(sst_loader.is_emtpy());
        clean_sst(1, &sst_loader).unwrap();
        assert!(!sst_factory.exists(1).unwrap());

        let ss_table_backup = sst_loader.get(1).unwrap();

        assert_eq!(ss_table_backup.query(&repeat_data.0)?, repeat_data.1);
        for i in 1..times {
            assert_eq!(ss_table_backup.query(&vec_data[i].0)?, Some(value.clone()))
        }
        Ok(())
    }

    fn clean_sst(gen: i64, loader: &TableLoader) -> Result<()> {
        loader.factory.clean(gen)?;

        Ok(())
    }
}
