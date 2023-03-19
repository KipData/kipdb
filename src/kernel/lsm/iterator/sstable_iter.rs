use crate::kernel::lsm::block::{BlockCache, Index, KeyValue, Value};
use crate::kernel::lsm::iterator::{Iterator, Seek};
use crate::kernel::lsm::iterator::block_iter::BlockIterator;
use crate::kernel::lsm::ss_table::SSTable;
use crate::kernel::Result;
use crate::KernelError;

pub(crate) struct SSTableIterator<'a> {
    ss_table: &'a SSTable,
    data_iter: BlockIterator<'a, Value>,
    index_iter: BlockIterator<'a, Index>,
    block_cache: &'a BlockCache
}

impl<'a> SSTableIterator<'a> {
    #[allow(dead_code)]
    pub(crate) fn new(ss_table: &'a SSTable, block_cache: &'a BlockCache) -> Result<Self> {
        let index_iter = BlockIterator::new(
            ss_table.get_index_block(block_cache)?);
        let data_iter = Self::data_iter_sync_(ss_table, block_cache, &index_iter, true)?;

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
        index_iter: &BlockIterator<Index>,
        is_next: bool
    ) -> Result<BlockIterator<'a, Value>> {
        let mut iterator = BlockIterator::new(
            ss_table.get_data_block(index_iter.item().1, block_cache)?
                .ok_or(KernelError::DataEmpty)?
        );
        iterator.seek(if is_next { Seek::First } else { Seek::Last })?;

        Ok(iterator)
    }
}

impl Iterator<KeyValue<Value>> for SSTableIterator<'_> {
    fn next(&mut self) -> Result<()> {
        if let Err(KernelError::OutOfBounds) = self.data_iter.next() {
            self.index_iter.next()?;
            self.data_iter_sync(true)?;
        }

        Ok(())
    }

    fn prev(&mut self) -> Result<()> {
        if let Err(KernelError::OutOfBounds) = self.data_iter.prev() {
            self.index_iter.prev()?;
            self.data_iter_sync(false)?;
        }

        Ok(())
    }

    fn item(&self) -> &KeyValue<Value> {
        self.data_iter.item()
    }

    fn is_valid(&self) -> bool {
        self.index_iter.is_valid() && self.data_iter.is_valid()
    }

    fn seek(&mut self, seek: Seek) -> Result<()> {
        self.index_iter.seek(
            if let Some(key) = seek.get_key() { Seek::Backward(key) } else { seek.clone() }
        )?;
        self.data_iter_sync(true)?;
        self.data_iter.seek(seek)?;
        Ok(())
    }
}

impl SSTableIterator<'_> {
    fn data_iter_sync(&mut self, is_next: bool) -> Result<()> {
        self.data_iter = Self::data_iter_sync_(
            self.ss_table, self.block_cache, &self.index_iter, is_next
        )?;
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
    use crate::kernel::{CommandData, Result};
    use crate::kernel::lsm::iterator::{Iterator, Seek};
    use crate::kernel::lsm::iterator::sstable_iter::SSTableIterator;
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
        let mut vec_cmd = Vec::new();

        let times = 2333;

        // 默认使用大端序进行序列化，保证顺序正确性
        for i in 0..times {
            let mut key = b"KipDB-".to_vec();
            key.append(
                &mut bincode::options().with_big_endian().serialize(&i)?
            );
            vec_cmd.push(
                CommandData::set(key, value.to_vec()
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
        let cache = ShardingLruCache::new(
            config.block_cache_size,
            16,
            RandomState::default()
        )?;

        let mut iterator = SSTableIterator::new(&ss_table, &cache)?;
        for i in 0..times - 1 {
            assert_eq!(&iterator.item().0, vec_cmd[i].get_key());
            iterator.next()?;
        }

        for i in 0..times - 1 {
            assert_eq!(&iterator.item().0, vec_cmd[times - i - 1].get_key());
            iterator.prev()?;
        }

        iterator.seek(Seek::Backward(vec_cmd[114].get_key()))?;
        assert_eq!(&iterator.item().0, vec_cmd[114].get_key());

        iterator.seek(Seek::Forward(vec_cmd[514].get_key()))?;
        assert_eq!(&iterator.item().0, vec_cmd[514].get_key());

        Ok(())
    }
}