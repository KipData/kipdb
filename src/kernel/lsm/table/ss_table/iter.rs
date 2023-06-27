use crate::kernel::lsm::iterator::{ForwardIter, Iter, Seek};
use crate::kernel::lsm::mem_table::KeyValue;
use crate::kernel::lsm::table::ss_table::block::{BlockType, Index, Value};
use crate::kernel::lsm::table::ss_table::block_iter::BlockIter;
use crate::kernel::lsm::table::ss_table::SSTable;
use crate::kernel::lsm::table::Table;
use crate::kernel::Result;
use crate::KernelError;

pub(crate) struct SSTableIter<'a> {
    ss_table: &'a SSTable,
    data_iter: BlockIter<'a, Value>,
    index_iter: BlockIter<'a, Index>,
}

impl<'a> SSTableIter<'a> {
    pub(crate) fn new(ss_table: &'a SSTable) -> Result<SSTableIter<'a>> {
        let mut index_iter = BlockIter::new(ss_table.index_block()?);
        let index = index_iter.next_err()?.ok_or(KernelError::DataEmpty)?.1;
        let data_iter = Self::data_iter_init(ss_table, index)?;

        Ok(Self {
            ss_table,
            data_iter,
            index_iter,
        })
    }

    fn data_iter_init(
        ss_table: &'a SSTable,
        index: Index,
    ) -> Result<BlockIter<'a, Value>> {
        let block = {
            ss_table.cache
                .get_or_insert((ss_table.gen(), Some(index)), |(_, index)| {
                    let index = (*index).ok_or_else(|| KernelError::DataEmpty)?;
                    Ok(ss_table.data_block(index)?)
                })
                .map(|block_type| match block_type {
                    BlockType::Data(data_block) => Some(data_block),
                    _ => None,
                })?
        }.ok_or(KernelError::DataEmpty)?;

        Ok(BlockIter::new(block))
    }

    fn data_iter_seek(&mut self, seek: Seek<'_>, index: Index) -> Result<Option<KeyValue>> {
        self.data_iter = Self::data_iter_init(self.ss_table, index)?;
        Ok(self
            .data_iter
            .seek(seek)?
            .map(|(key, value)| (key, value.bytes)))
    }
}

impl<'a> ForwardIter<'a> for SSTableIter<'a> {
    fn prev_err(&mut self) -> Result<Option<Self::Item>> {
        match self.data_iter.prev_err()? {
            None => {
                if let Some((_, index)) = self.index_iter.prev_err()? {
                    self.data_iter_seek(Seek::Last, index)
                } else {
                    Ok(None)
                }
            }
            Some((key, value)) => Ok(Some((key, value.bytes))),
        }
    }
}

impl<'a> Iter<'a> for SSTableIter<'a> {
    type Item = KeyValue;

    fn next_err(&mut self) -> Result<Option<Self::Item>> {
        match self.data_iter.next_err()? {
            None => {
                if let Some((_, index)) = self.index_iter.next_err()? {
                    self.data_iter_seek(Seek::First, index)
                } else {
                    Ok(None)
                }
            }
            Some((key, value)) => Ok(Some((key, value.bytes))),
        }
    }

    fn is_valid(&self) -> bool {
        self.data_iter.is_valid()
    }

    fn seek(&mut self, seek: Seek<'_>) -> Result<Option<Self::Item>> {
        if let Some((_, index)) = self.index_iter.seek(seek)? {
            self.data_iter_seek(seek, index)
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;
    use crate::kernel::io::{FileExtension, IoFactory, IoType};
    use crate::kernel::lsm::iterator::{ForwardIter, Iter, Seek};
    use crate::kernel::lsm::storage::Config;
    use crate::kernel::lsm::version::DEFAULT_SS_TABLE_PATH;
    use crate::kernel::Result;
    use bincode::Options;
    use bytes::Bytes;
    use std::sync::Arc;
    use tempfile::TempDir;
    use crate::kernel::lsm::table::ss_table::iter::SSTableIter;
    use crate::kernel::lsm::table::ss_table::SSTable;
    use crate::kernel::utils::lru_cache::ShardingLruCache;

    #[test]
    fn test_iterator() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let config = Config::new(temp_dir.into_path());

        let sst_factory = IoFactory::new(
            config.dir_path.join(DEFAULT_SS_TABLE_PATH),
            FileExtension::SSTable,
        )?;

        let value =
            Bytes::from_static(b"What you are you do not see, what you see is your shadow.");
        let mut vec_data = Vec::new();

        let times = 2333;

        // 默认使用大端序进行序列化，保证顺序正确性
        for i in 0..times {
            let mut key = b"KipDB-".to_vec();
            key.append(&mut bincode::options().with_big_endian().serialize(&i)?);
            vec_data.push((Bytes::from(key), Some(value.clone())));
        }
        let cache = Arc::new(
            ShardingLruCache::new(
                config.table_cache_size,
                16,
                RandomState::default()
            )?
        );

        let ss_table = SSTable::new(
            &sst_factory,
            &config,
            cache,
            1,
            vec_data.clone(),
            0,
            IoType::Direct
        )?;

        let mut iterator = SSTableIter::new(&ss_table)?;

        for i in 0..times {
            assert_eq!(iterator.next_err()?.unwrap(), vec_data[i]);
        }

        for i in (0..times - 1).rev() {
            assert_eq!(iterator.prev_err()?.unwrap(), vec_data[i]);
        }

        assert_eq!(
            iterator.seek(Seek::Backward(&vec_data[114].0))?.unwrap(),
            vec_data[114]
        );

        assert_eq!(iterator.seek(Seek::First)?.unwrap(), vec_data[0]);

        assert_eq!(iterator.seek(Seek::Last)?.unwrap(), vec_data[times - 1]);

        Ok(())
    }
}
