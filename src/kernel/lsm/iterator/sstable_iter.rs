use crate::kernel::lsm::block::{BlockCache, Index, Value};
use crate::kernel::lsm::iterator::{DiskIter, Seek};
use crate::kernel::lsm::iterator::block_iter::BlockIter;
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
        let index_iter = BlockIter::new(ss_table.get_index_block(block_cache)?);
        let data_iter = Self::data_iter_sync_(ss_table, block_cache, &index_iter, Seek::First)?;

        Ok(Self {
            ss_table,
            data_iter,
            index_iter,
            block_cache
        })
    }

    fn data_iter_sync_(
        ss_table: &'a SSTable,
        block_cache: &'a BlockCache,
        index_iter: &BlockIter<Index>,
        seek: Seek
    ) -> Result<BlockIter<'a, Value>> {
        let index = index_iter.value().clone();
        let mut iterator = BlockIter::new(
            ss_table.get_data_block(index, block_cache)?
                .ok_or(KernelError::DataEmpty)?
        );
        iterator.seek(seek)?;

        Ok(iterator)
    }

    fn data_iter_sync(&mut self, seek: Seek) -> Result<()> {
        self.data_iter = Self::data_iter_sync_(
            self.ss_table, self.block_cache, &self.index_iter, seek
        )?;
        Ok(())
    }

    pub(crate) fn len(&self) -> usize {
        self.ss_table.len()
    }

    pub(crate) fn get_gen(&self) -> i64 {
        self.ss_table.get_gen()
    }
}

impl DiskIter<Vec<u8>, Value> for SSTableIter<'_> {
    fn next(&mut self) -> Result<()> {
        if let Err(KernelError::OutOfBounds) = self.data_iter.next() {
            self.index_iter.next()?;
            self.data_iter_sync(Seek::First)?;
        }

        Ok(())
    }

    fn prev(&mut self) -> Result<()> {
        if let Err(KernelError::OutOfBounds) = self.data_iter.prev() {
            self.index_iter.prev()?;
            self.data_iter_sync(Seek::Last)?;
        }

        Ok(())
    }

    fn key(&self) -> Vec<u8> {
        self.data_iter.key()
    }

    fn value(&self) -> &Value {
        self.data_iter.value()
    }

    fn is_valid(&self) -> bool {
        self.index_iter.is_valid() && self.data_iter.is_valid()
    }

    fn seek(&mut self, seek: Seek) -> Result<()> {
        self.index_iter.seek(
            if let Some(key) = seek.get_key() { Seek::Backward(key) } else { seek }
        )?;
        self.data_iter_sync(seek)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;
    use bincode::Options;
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

        let value = b"What you are you do not see, what you see is your shadow.";
        let mut vec_data = Vec::new();

        let times = 2333;

        // 默认使用大端序进行序列化，保证顺序正确性
        for i in 0..times {
            let mut key = b"KipDB-".to_vec();
            key.append(
                &mut bincode::options().with_big_endian().serialize(&i)?
            );
            vec_data.push(
                (key, Some(value.to_vec()))
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
        for i in 0..times - 1 {
            assert_eq!(&iterator.key(), &vec_data[i].0);
            iterator.next()?;
        }

        for i in 0..times - 1 {
            assert_eq!(&iterator.key(), &vec_data[times - i - 1].0);
            iterator.prev()?;
        }

        iterator.seek(Seek::Backward(&vec_data[114].0))?;
        assert_eq!(&iterator.key(), &vec_data[114].0);

        iterator.seek(Seek::Forward(&vec_data[514].0))?;
        assert_eq!(&iterator.key(), &vec_data[514].0);

        Ok(())
    }
}