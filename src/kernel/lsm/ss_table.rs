use std::sync::Arc;
use growable_bloom_filter::GrowableBloom;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tracing::info;
use crate::kernel::CommandData;
use crate::kernel::io::{IoFactory, IoReader, IoType};
use crate::kernel::lsm::{MetaBlock, Footer, TABLE_FOOTER_SIZE};
use crate::kernel::lsm::block::{Block, BlockBuilder, BlockCache, BlockItem, BlockOptions, BlockType, CompressType, Index, Value};
use crate::kernel::lsm::lsm_kv::Config;
use crate::kernel::lsm::version::Version;
use crate::kernel::Result;
use crate::KvsError;

pub(crate) struct SSTable {
    inner: Arc<SSTableInner>
}

impl Clone for SSTable {
    fn clone(&self) -> Self {
        SSTable {
            inner: Arc::clone(&self.inner)
        }
    }
}

/// SSTable
///
/// SSTable仅加载MetaBlock与Footer，避免大量冷数据时冗余的SSTable加载的空间占用
pub(crate) struct SSTableInner {
    // 表索引信息
    footer: Footer,
    // 文件IO操作器
    reader: Box<dyn IoReader>,
    // 该SSTable的唯一编号(时间递增)
    gen: i64,
    // 统计信息存储Block
    meta: MetaBlock,
}

/// 数据范围索引
/// 用于缓存SSTable中所有数据的第一个和最后一个数据的Key
/// 标明数据的范围以做到快速区域定位
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct Scope {
    start: Vec<u8>,
    end: Vec<u8>
}

impl Scope {

    /// 由CommandData组成的Key构成scope
    pub(crate) fn from_cmd_data(first: &CommandData, last: &CommandData) -> Self {
        Scope {
            start: first.get_key_clone(),
            end: last.get_key_clone()
        }
    }

    /// 由Key构成scope
    pub(crate) fn from_key(key: &[u8]) -> Self {
        Scope {
            start: key.to_owned(),
            end: key.to_owned()
        }
    }

    /// 将多个scope重组融合成一个scope
    pub(crate) fn fusion(vec_scope :Vec<&Scope>) -> Result<Self> {
        if !vec_scope.is_empty() {
            let start = vec_scope.iter()
                .map(|scope| &scope.start)
                .min().ok_or(KvsError::DataEmpty)?
                .clone();
            let end = vec_scope.iter()
                .map(|scope| &scope.end)
                .max().ok_or(KvsError::DataEmpty)?
                .clone();

            Ok(Scope { start, end })
        } else {
            Err(KvsError::DataEmpty)
        }
    }

    /// 判断scope之间是否相交
    pub(crate) fn meet(&self, target: &Scope) -> bool {
        (self.start.le(&target.start) && self.end.ge(&target.start)) ||
            (self.start.le(&target.end) && self.end.ge(&target.end))
    }

    /// 由一组Command组成一个scope
    #[allow(clippy::pattern_type_mismatch)]
    pub(crate) fn from_vec_cmd_data(vec_mem_data: &Vec<CommandData>) -> Result<Self> {
        match vec_mem_data.as_slice() {
            [first, .., last] => {
                Ok(Self::from_cmd_data(first, last))
            },
            [one] => {
                Ok(Self::from_cmd_data(one, one))
            },
            _ => {
                Err(KvsError::DataEmpty)
            },
        }
    }

    /// 由一组SSTable融合成一个scope
    pub(crate) fn fusion_from_vec_ss_table(vec_ss_table :&[SSTable]) -> Result<Self>
    {
        let vec_scope = vec_ss_table.iter()
            .map(|ss_table| ss_table.get_scope())
            .collect_vec();
        Self::fusion(vec_scope)
    }
}

impl SSTable {
    pub(crate) fn get_level(&self) -> usize {
        self.inner.footer.level as usize
    }

    pub(crate) fn get_gen(&self) -> i64 {
        self.inner.gen
    }

    pub(crate) fn get_scope(&self) -> &Scope {
        &self.inner.meta.scope
    }

    pub(crate) fn get_size_of_disk(&self) -> u64 {
        self.inner.footer.size_of_disk as u64
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.meta.len
    }

    /// 通过已经存在的文件构建SSTable
    ///
    /// 使用原有的路径与分区大小恢复出一个有内容的SSTable
    pub(crate) fn load_from_file(reader: Box<dyn IoReader>) -> Result<Self>{
        let gen = reader.get_gen();

        let footer = Footer::read_to_file(&reader)?;
        let Footer { size_of_disk, meta_offset, meta_len ,.. } = &footer;
        info!(
            "[SsTable: {gen}][load_from_file][MetaBlock]: {footer:?}, Size of Disk: {}, IO Type: {:?}",
            size_of_disk ,
            reader.get_type()
        );
        let meta = bincode::deserialize(
            &reader.read_with_pos(*meta_offset as u64, *meta_len as usize)?
        )?;
        Ok(SSTable {
            inner : Arc::new(
                SSTableInner { footer, gen, reader, meta, }
            )
        })
    }

    /// 查询Key对应的Value
    #[allow(clippy::expect_used)]
    pub(crate) fn query_with_key(
        &self,
        key: &[u8],
        block_cache: &BlockCache
    ) -> Result<Option<Vec<u8>>> {
        let inner = &self.inner;
        let reader = &inner.reader;
        if inner.meta.filter.contains(key) {
            if let BlockType::Index(index_block) = block_cache.get_or_insert(
                (self.get_gen(), None),
                |_| {
                    let Footer { index_offset, index_len, .. } = inner.footer;
                    Ok(BlockType::Index(
                        Self::loading_block(
                            reader,
                            index_offset,
                            index_len as usize,
                            CompressType::None
                        )?
                    ))
                }
            )? {
                if let BlockType::Data(data_block) =  block_cache.get_or_insert(
                    (self.get_gen(), Some(index_block.find_with_upper(key))),
                    |(_, index)| {
                        let index = (*index).ok_or_else(|| KvsError::DataEmpty)?;
                        Ok(BlockType::Data(
                            Self::loading_block(
                                reader,
                                index.offset(),
                                index.len(),
                                CompressType::LZ4
                            )?
                        ))
                    }
                )? { return Ok(data_block.find(key)); }
            }
        }

        Ok(None)
    }

    fn loading_block<T>(
        reader: &Box<dyn IoReader>,
        offset: u32,
        len: usize,
        compress_type: CompressType
    ) -> Result<Block<T>>
        where T: BlockItem
    {
        Ok(Block::decode(
            reader.read_with_pos(offset as u64, len)?,
            compress_type
        )?)
    }

    /// 获取SsTable内所有的正常数据
    pub(crate) async fn all(&self) -> Result<Vec<CommandData>> {
        let inner = &self.inner;
        let Footer{ index_offset, index_len, .. } = inner.footer;

        let index_block = Self::loading_block::<Index>(
            &inner.reader,
            index_offset,
            index_len as usize,
            CompressType::None
        )?;
        Ok(index_block.all_value()
            .into_iter()
            .flat_map(|index| {
                Self::loading_block::<Value>(
                    &inner.reader, index.offset(), index.len(), CompressType::LZ4
                ).map(Block::all_entry)
                    .flatten()
            })
            .flatten()
            .map(CommandData::from)
            .collect_vec())
    }

    /// 通过一组SSTable收集对应的Gen
    pub(crate) fn collect_gen(vec_ss_table: &Vec<SSTable>) -> Result<Vec<i64>> {
        Ok(vec_ss_table.iter()
            .map(SSTable::get_gen)
            .unique()
            .collect())
    }

    /// 获取指定SSTable索引位置
    pub(crate) fn find_index_with_level(option_first: Option<i64>, version: &Version, level: usize) -> usize {
        match option_first {
            None => 0,
            Some(gen) => {
                version.get_index(level, gen)
                    .unwrap_or(0)
            }
        }
    }

    /// 通过内存表构建持久化并构建SSTable
    /// 使用目标路径与文件大小，分块大小构建一个有内容的SSTable
    pub(crate) fn create_for_mem_table(
        config: &Config,
        gen: i64,
        io_factory: &IoFactory,
        vec_mem_data: Vec<CommandData>,
        level: usize
    ) -> Result<SSTable>{
        // 获取数据的Key涵盖范围
        let scope = Scope::from_vec_cmd_data(&vec_mem_data)?;
        let writer = io_factory.writer(gen, IoType::Buf)?;
        let len = vec_mem_data.len();
        let mut filter = GrowableBloom::new(config.desired_error_prob, len);

        let mut builder = BlockBuilder::new(
            BlockOptions::from(config).compress_type(CompressType::LZ4)
        );
        for data in vec_mem_data {
            let _ = filter.insert(data.get_key());
            builder.add(data);
        }
        let meta = MetaBlock {
            scope,
            filter,
            len
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
        let _ = writer.write(
            data_bytes.into_iter()
                .chain(index_bytes)
                .chain(meta_bytes)
                .chain(bincode::serialize(&footer)?)
                .collect_vec()
        )?;
        writer.flush()?;
        info!("[SsTable: {}][create_form_index][MetaBlock]: {:?}", gen, meta);
        Ok(SSTable {
            inner: Arc::new(
                SSTableInner {
                    footer,
                    reader: io_factory.reader(gen, IoType::Buf)?,
                    gen,
                    meta,
                }
            )
        })

    }
}

#[cfg(test)]
mod tests {

    use std::collections::hash_map::RandomState;
    use bincode::Options;
    use tempfile::TempDir;
    use crate::kernel::io::{FileExtension, IoFactory, IoType};
    use crate::kernel::lsm::lsm_kv::Config;
    use crate::kernel::lsm::ss_table::SSTable;
    use crate::kernel::lsm::version::DEFAULT_SS_TABLE_PATH;
    use crate::kernel::{CommandData, Result};
    use crate::kernel::utils::lru_cache::ShardingLruCache;

    #[test]
    fn test_sstable() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        tokio_test::block_on(async move {
            let value = b"If you shed tears when you miss the sun, you also miss the stars.";
            let config = Config::new(temp_dir.into_path(), 0, 0);
            let sst_factory = IoFactory::new(
                config.dir_path.join(DEFAULT_SS_TABLE_PATH),
                FileExtension::SSTable
            )?;
            let cache = ShardingLruCache::new(
                config.block_cache_size,
                16,
                RandomState::default()
            )?;
            let mut vec_cmd = Vec::new();
            let times = 2333;

            for i in 0..times {
                vec_cmd.push(
                    CommandData::set(
                        bincode::options().with_big_endian().serialize(&i)?,
                        value.to_vec()
                    )
                );
            }
            let ss_table = SSTable::create_for_mem_table(
                &config,
                1,
                &sst_factory,
                vec_cmd.clone(),
                0
            )?;
            for i in 0..times {
                let key = vec_cmd[i].get_key();
                assert_eq!(ss_table.query_with_key(key, &cache)?, Some(value.to_vec()))
            }
            drop(ss_table);
            let ss_table = SSTable::load_from_file(
                sst_factory.reader(1, IoType::MMap)?
            )?;
            for i in 0..times {
                let key = vec_cmd[i].get_key();
                assert_eq!(ss_table.query_with_key(key, &cache)?, Some(value.to_vec()))
            }

            let vec_cmd_ss = ss_table.all().await?;

            assert_eq!(vec_cmd_ss.len(), vec_cmd.len());

            Ok(())
        })
    }
}