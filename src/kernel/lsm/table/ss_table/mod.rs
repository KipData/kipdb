use crate::kernel::io::IoReader;
use crate::kernel::lsm::version::Version;
use crate::kernel::Result;
use crate::KernelError;
use bytes::Bytes;
use parking_lot::Mutex;
use std::io::SeekFrom;
use std::sync::Arc;
use tracing::info;
use crate::kernel::lsm::table::ss_table::block::{Block, BlockCache, BlockItem, BlockType, CompressType, Index, MetaBlock};
use crate::kernel::lsm::table::ss_table::footer::Footer;
use crate::kernel::lsm::table::Table;

pub(crate) mod block;
pub(crate) mod block_iter;
mod footer;
pub(crate) mod iter;
pub(crate) mod loader;

/// SSTable
///
/// SSTable仅加载MetaBlock与Footer，避免大量冷数据时冗余的SSTable加载的空间占用
pub(crate) struct SSTable {
    // 表索引信息
    footer: Footer,
    // 文件IO操作器
    reader: Mutex<Box<dyn IoReader>>,
    // 该SSTable的唯一编号(时间递增)
    gen: i64,
    // 统计信息存储Block
    meta: MetaBlock,
    // Block缓存(Index/Value)
    cache: Arc<BlockCache>,
}

impl SSTable {
    /// 通过已经存在的文件构建SSTable
    ///
    /// 使用原有的路径与分区大小恢复出一个有内容的SSTable
    pub(crate) fn load_from_file(mut reader: Box<dyn IoReader>, cache: Arc<BlockCache>) -> Result<Self> {
        let gen = reader.get_gen();
        let footer = Footer::read_to_file(reader.as_mut())?;
        let Footer {
            size_of_disk,
            meta_offset,
            meta_len,
            ..
        } = &footer;
        info!(
            "[SsTable: {gen}][load_from_file][MetaBlock]: {footer:?}, Size of Disk: {}, IO Type: {:?}",
            size_of_disk ,
            reader.get_type()
        );

        let mut buf = vec![0; *meta_len as usize];
        let _ = reader.seek(SeekFrom::Start(*meta_offset as u64))?;
        let _ = reader.read(&mut buf)?;

        let meta = bincode::deserialize(&buf)?;
        let reader = Mutex::new(reader);
        Ok(SSTable {
            footer,
            gen,
            reader,
            meta,
            cache,
        })
    }

    pub(crate) fn data_block(&self, index: Index) -> Result<BlockType> {
        Ok(BlockType::Data(Self::loading_block(
            self.reader.lock().as_mut(),
            index.offset(),
            index.len(),
            CompressType::LZ4,
            self.meta.data_restart_interval,
        )?))
    }

    pub(crate) fn index_block(&self) -> Result<&Block<Index>> {
        self.cache
            .get_or_insert((self.gen(), None), |_| {
                let Footer {
                    index_offset,
                    index_len,
                    ..
                } = self.footer;
                Ok(BlockType::Index(Self::loading_block(
                    self.reader.lock().as_mut(),
                    index_offset,
                    index_len as usize,
                    CompressType::None,
                    self.meta.index_restart_interval,
                )?))
            })
            .map(|block_type| match block_type {
                BlockType::Index(data_block) => Some(data_block),
                _ => None,
            })?
            .ok_or(KernelError::DataEmpty)
    }

    fn loading_block<T>(
        reader: &mut dyn IoReader,
        offset: u32,
        len: usize,
        compress_type: CompressType,
        restart_interval: usize,
    ) -> Result<Block<T>>
    where
        T: BlockItem,
    {
        let mut buf = vec![0; len];
        let _ = reader.seek(SeekFrom::Start(offset as u64))?;
        reader.read_exact(&mut buf)?;

        Block::decode(buf, compress_type, restart_interval)
    }

    /// 获取指定SSTable索引位置
    pub(crate) fn find_index_with_level(
        option_first: Option<i64>,
        version: &Version,
        level: usize,
    ) -> usize {
        option_first
            .and_then(|gen| version.get_index(level, gen))
            .unwrap_or(0)
    }
}

impl Table for SSTable {
    fn query(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.meta.filter.contains(key) {
            let index_block = self.index_block()?;

            if let BlockType::Data(data_block) = self.cache.get_or_insert(
                (self.gen(), Some(index_block.find_with_upper(key))),
                |(_, index)| {
                    let index = (*index).ok_or_else(|| KernelError::DataEmpty)?;
                    Ok(Self::data_block(self, index)?)
                },
            )? {
                return Ok(data_block.find(key));
            }
        }

        Ok(None)
    }

    fn len(&self) -> usize {
        self.meta.len
    }

    fn size_of_disk(&self) -> u64 {
        self.footer.size_of_disk as u64
    }

    fn gen(&self) -> i64 {
        self.gen
    }

    fn level(&self) -> usize {
        self.footer.level as usize
    }
}

#[cfg(test)]
mod tests {
    use crate::kernel::io::{FileExtension, IoFactory, IoType};
    use crate::kernel::lsm::log::LogLoader;
    use crate::kernel::lsm::mem_table::DEFAULT_WAL_PATH;
    use crate::kernel::lsm::storage::Config;
    use crate::kernel::lsm::version::DEFAULT_SS_TABLE_PATH;
    use crate::kernel::utils::lru_cache::ShardingLruCache;
    use crate::kernel::Result;
    use bincode::Options;
    use bytes::Bytes;
    use std::collections::hash_map::RandomState;
    use std::sync::Arc;
    use tempfile::TempDir;
    use crate::kernel::lsm::table::ss_table::loader::SSTableLoader;
    use crate::kernel::lsm::table::ss_table::SSTable;
    use crate::kernel::lsm::table::Table;

    #[test]
    fn test_ss_table() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let value = Bytes::copy_from_slice(
            b"If you shed tears when you miss the sun, you also miss the stars.",
        );
        let config = Config::new(temp_dir.into_path());
        let sst_factory = Arc::new(IoFactory::new(
            config.dir_path.join(DEFAULT_SS_TABLE_PATH),
            FileExtension::SSTable,
        )?);
        let (log_loader, _, _) = LogLoader::reload(
            config.path(),
            (DEFAULT_WAL_PATH, Some(1)),
            IoType::Buf,
            |_| Ok(()),
        )?;
        let sst_loader = SSTableLoader::new(config.clone(), sst_factory.clone(), log_loader)?;

        let mut vec_data = Vec::new();
        let times = 2333;

        for i in 0..times {
            vec_data.push((
                Bytes::from(bincode::options().with_big_endian().serialize(&i)?),
                Some(value.clone()),
            ));
        }
        // Tips: 此处Level需要为0以上，因为Level 0默认为Mem类型，容易丢失
        let _ = sst_loader.create(1, vec_data.clone(), 1)?;
        assert!(sst_loader.is_sst_file_exist(1)?);

        let ss_table = sst_loader.get(1).unwrap();

        for i in 0..times {
            assert_eq!(
                ss_table.query(&vec_data[i].0)?,
                Some(value.clone())
            )
        }
        let cache = ShardingLruCache::new(config.table_cache_size, 16, RandomState::default())?;
        let ss_table = SSTable::load_from_file(sst_factory.reader(1, IoType::Direct)?, Arc::new(cache))?;
        for i in 0..times {
            assert_eq!(
                ss_table.query(&vec_data[i].0)?,
                Some(value.clone())
            )
        }

        Ok(())
    }
}
