use async_trait::async_trait;
use bytes::Bytes;
use crate::kernel::lsm::block::{BlockCache, Index, Value};
use crate::kernel::lsm::iterator::{DiskIter, ForwardDiskIter, Seek};
use crate::kernel::lsm::iterator::block_iter::BlockIter;
use crate::kernel::lsm::mem_table::KeyValue;
use crate::kernel::lsm::ss_table::SSTable;
use crate::kernel::Result;
use crate::KernelError;

pub(crate) struct SSTableIter<'a> {
    ss_table: &'a SSTable,
    data_iter: BlockIter<'a, Value>,
    index_iter: BlockIter<'a, Index>,
    block_cache: &'a BlockCache
}

impl<'a> SSTableIter<'a> {
    pub(crate) async fn new(ss_table: &'a SSTable, block_cache: &'a BlockCache) -> Result<SSTableIter<'a>> {
        let mut index_iter = BlockIter::new(
            ss_table.get_index_block(block_cache)?
        );
        let data_iter = Self::data_iter_init(
            ss_table,
            block_cache,
            index_iter.next_err().await?.ok_or(KernelError::DataEmpty)?.1
        )?;

        Ok(Self {
            ss_table,
            data_iter,
            index_iter,
            block_cache
        })
    }

    fn data_iter_init(ss_table: &'a SSTable, block_cache: &'a BlockCache, index: Index) -> Result<BlockIter<'a, Value>> {
        Ok(BlockIter::new(
            ss_table.get_data_block(index, block_cache)?
                .ok_or(KernelError::DataEmpty)?
        ))
    }

    async fn data_iter_seek(&mut self, seek: Seek<'_>, index: Index) -> Result<Option<KeyValue>> {
        self.data_iter = Self::data_iter_init(self.ss_table, self.block_cache, index)?;
        Ok(self.data_iter.seek(seek).await?
            .map(|(key, value)| (key, value.bytes)))
    }

    pub(crate) fn len(&self) -> usize {
        self.ss_table.len()
    }
}

#[async_trait]
#[allow(single_use_lifetimes)]
impl ForwardDiskIter for SSTableIter<'_> {
    async fn prev_err(&mut self) -> Result<Option<Self::Item>> {
        match self.data_iter.prev_err().await? {
            None => {
                if let Some((_, index)) = self.index_iter.prev_err().await? {
                    self.data_iter_seek(Seek::Last, index).await
                } else { Ok(None) }
            }
            Some((key, value)) => Ok(Some((key, value.bytes)))
        }
    }
}

#[async_trait]
#[allow(single_use_lifetimes)]
impl DiskIter for SSTableIter<'_> {
    type Item = KeyValue;

    async fn next_err(&mut self) -> Result<Option<Self::Item>> {
        match self.data_iter.next_err().await? {
            None => {
                if let Some((_, index)) = self.index_iter.next_err().await? {
                    self.data_iter_seek(Seek::First, index).await
                } else { Ok(None) }
            }
            Some((key, value)) => Ok(Some((key, value.bytes)))
        }
    }

    fn is_valid(&self) -> bool {
        self.index_iter.is_valid() && self.data_iter.is_valid()
    }

    async fn seek(&mut self, seek: Seek<'_>) -> Result<Option<Self::Item>> {
        if let Some((_, index)) = self.index_iter.seek(seek).await? {
            self.data_iter_seek(seek, index).await
        } else {
            Ok(None)
        }
    }

    fn item_key(item: &Self::Item) -> Bytes {
        item.0.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;
    use bincode::Options;
    use bytes::Bytes;
    use tempfile::TempDir;
    use crate::kernel::io::{FileExtension, IoFactory, IoType};
    use crate::kernel::lsm::lsm_kv::Config;
    use crate::kernel::lsm::ss_table::SSTable;
    use crate::kernel::lsm::version::DEFAULT_SS_TABLE_PATH;
    use crate::kernel::Result;
    use crate::kernel::lsm::iterator::{DiskIter, ForwardDiskIter, Seek};
    use crate::kernel::lsm::iterator::ss_table_iter::SSTableIter;
    use crate::kernel::utils::lru_cache::ShardingLruCache;

    #[test]
    fn test_iterator() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let config = Config::new(temp_dir.into_path());

        let sst_factory = IoFactory::new(
            config.dir_path.join(DEFAULT_SS_TABLE_PATH),
            FileExtension::SSTable
        )?;

        let value = Bytes::from_static(b"What you are you do not see, what you see is your shadow.");
        let mut vec_data = Vec::new();

        let times = 2333;

        // 默认使用大端序进行序列化，保证顺序正确性
        for i in 0..times {
            let mut key = b"KipDB-".to_vec();
            key.append(
                &mut bincode::options().with_big_endian().serialize(&i)?
            );
            vec_data.push(
                (Bytes::from(key), Some(value.clone()))
            );
        }

        let (ss_table, _) = SSTable::create_for_mem_table(
            &config,
            1,
            &sst_factory,
            vec_data.clone(),
            0,
            IoType::Direct
        )?;
        let cache = ShardingLruCache::new(
            config.block_cache_size,
            16,
            RandomState::default()
        )?;

        tokio_test::block_on(async move {
            let mut iterator = SSTableIter::new(&ss_table, &cache).await?;


            for i in 0..times {
                assert_eq!(iterator.next_err().await?.unwrap(), vec_data[i]);
            }

            for i in (0..times - 1).rev() {
                assert_eq!(iterator.prev_err().await?.unwrap(), vec_data[i]);
            }

            assert_eq!(iterator.seek(Seek::Backward(&vec_data[114].0)).await?.unwrap(), vec_data[114]);

            assert_eq!(iterator.seek(Seek::First).await?.unwrap(), vec_data[0]);

            assert_eq!(iterator.seek(Seek::Last).await?.unwrap(), vec_data[times - 1]);

            Ok(())
        })
    }
}