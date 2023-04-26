use crate::kernel::lsm::block::{BlockCache, Index, Value};
use crate::kernel::lsm::iterator::{DiskIter, Seek};
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
    pub(crate) fn new(ss_table: &'a SSTable, block_cache: &'a BlockCache) -> Result<Self> {
        let mut index_iter = BlockIter::new(
            ss_table.get_index_block(block_cache)?
        );
        let data_iter = Self::data_iter_init(
            ss_table, block_cache, index_iter.next_err()?.1
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

    fn data_iter_seek(&mut self, seek: Seek, index: Index) -> Result<KeyValue> {
        self.data_iter = Self::data_iter_init(self.ss_table, self.block_cache, index)?;
        self.data_iter.seek(seek).map(|(key, value)| (key, value.bytes))
    }

    pub(crate) fn len(&self) -> usize {
        self.ss_table.len()
    }

    pub(crate) fn get_gen(&self) -> i64 {
        self.ss_table.get_gen()
    }
}

impl DiskIter<Vec<u8>, Vec<u8>> for SSTableIter<'_> {
    type Item = KeyValue;

    fn next_err(&mut self) -> Result<Self::Item> {
        match DiskIter::next_err(&mut self.data_iter) {
            Ok((key, value)) => Ok((key, value.bytes)),
            Err(KernelError::OutOfBounds) => {
                let index = DiskIter::next_err(&mut self.index_iter)?.1;
                self.data_iter_seek(Seek::First, index)
            }
            Err(e) => Err(e)
        }
    }

    fn prev_err(&mut self) -> Result<Self::Item> {
        match self.data_iter.prev_err() {
            Ok((key, value)) => Ok((key, value.bytes)),
            Err(KernelError::OutOfBounds) => {
                let index = self.index_iter.prev_err()?.1;
                self.data_iter_seek(Seek::Last, index)
            }
            Err(e) => Err(e)
        }
    }

    fn is_valid(&self) -> bool {
        self.index_iter.is_valid() && self.data_iter.is_valid()
    }

    fn seek(&mut self, seek: Seek) -> Result<Self::Item> {
        let index = self.index_iter.seek(
            if let Some(key) = seek.get_key() { Seek::Backward(key) } else { seek }
        )?.1;
        self.data_iter_seek(seek, index)
    }
}

impl Iterator for SSTableIter<'_> {
    type Item = KeyValue;

    fn next(&mut self) -> Option<Self::Item> {
        DiskIter::next_err(self).ok()
    }
}

impl DoubleEndedIterator for SSTableIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        DiskIter::prev_err(self).ok()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;
    use bincode::Options;
    use bytes::Bytes;
    use tempfile::TempDir;
    use crate::kernel::io::{FileExtension, IoFactory};
    use crate::kernel::lsm::lsm_kv::Config;
    use crate::kernel::lsm::ss_table::SSTable;
    use crate::kernel::lsm::version::DEFAULT_SS_TABLE_PATH;
    use crate::kernel::Result;
    use crate::kernel::lsm::iterator::{DiskIter, Seek};
    use crate::kernel::lsm::iterator::sstable_iter::SSTableIter;
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

        let ss_table = SSTable::create_for_mem_table(
            &config,
            1,
            &sst_factory,
            vec_data.clone(),
            0
        )?;
        let cache = ShardingLruCache::new(
            config.block_cache_size,
            16,
            RandomState::default()
        )?;

        let mut iterator = SSTableIter::new(&ss_table, &cache)?;

        for i in 0..times {
            assert_eq!(DiskIter::next_err(&mut iterator)?, vec_data[i]);
        }

        for i in (0..times - 1).rev() {
            assert_eq!(DiskIter::prev_err(&mut iterator)?, vec_data[i]);
        }

        assert_eq!(iterator.seek(Seek::Backward(&vec_data[114].0))?, vec_data[114]);

        assert_eq!(iterator.seek(Seek::Forward(&vec_data[514].0))?, vec_data[514]);

        assert_eq!(iterator.seek(Seek::First)?, vec_data[0]);

        assert_eq!(iterator.seek(Seek::Last)?, vec_data[times - 1]);

        Ok(())
    }
}