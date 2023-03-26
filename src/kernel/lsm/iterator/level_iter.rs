use crate::kernel::lsm::block::{BlockCache, Value};
use crate::kernel::lsm::iterator::{DiskIter, Seek};
use crate::kernel::lsm::iterator::sstable_iter::SSTableIter;
use crate::kernel::lsm::ss_table::SSTable;
use crate::kernel::Result;
use crate::KernelError;

pub(crate) struct LevelIter<'a> {
    ss_tables: &'a Vec<SSTable>,
    block_cache: &'a BlockCache,
    level: usize,

    offset: usize,
    sst_iter: SSTableIter<'a>,
}

impl<'a> LevelIter<'a> {
    #[allow(dead_code)]
    pub(crate) fn new(ss_tables: &'a Vec<SSTable>, level: usize, block_cache: &'a BlockCache) -> Result<LevelIter<'a>> {
        let ss_table = &ss_tables[0];
        let sst_iter = SSTableIter::new(&ss_table, block_cache)?;

        Ok(Self {
            ss_tables,
            block_cache,
            level,
            offset: 0,
            sst_iter,
        })
    }

    fn sst_iter_sync(&mut self, ss_table: &'a SSTable, offset: usize, seek: Seek) -> Result<()> {
        if ss_table.get_gen() != self.sst_iter.get_gen() {
            self.sst_iter = SSTableIter::new(ss_table, &self.block_cache)?;
        }
        self.offset = offset;
        self.sst_iter.seek(seek)?;

        Ok(())
    }

    fn seek_ward(&mut self, key: &[u8], seek: Seek) -> Result<()> {
        let i = self.ss_tables.len() - 1;
        // 对Level 0中的SSTable数据范围是可能重复的，因此需要遍历确认数据才能判断是否存在数据
        if self.level > 0 {
            let (offset, ss_table) = self.ss_tables.iter().rev()
                .enumerate()
                .rfind(|(_, ss_table)| ss_table.get_scope().meet_with_key(key))
                .ok_or(KernelError::DataEmpty)?;

            self.sst_iter_sync(ss_table, i - offset, seek)?;
        } else {
            for (offset, ss_table) in self.ss_tables.iter().rev().enumerate() {
                self.sst_iter_sync(ss_table, i - offset, seek)?;
                if key == &self.key() { break }
            }
        }
        Ok(())
    }
}

impl DiskIter<Vec<u8>, Value> for LevelIter<'_> {
    fn next(&mut self) -> Result<()> {
        if let Err(KernelError::OutOfBounds) = self.sst_iter.next() {
            let next_offset = self.offset + 1;
            if next_offset < self.ss_tables.len() && self.is_valid() {
                self.sst_iter_sync(&self.ss_tables[next_offset], next_offset, Seek::First)?;
            } else {
                return Err(KernelError::OutOfBounds);
            }
        }

        Ok(())
    }

    fn prev(&mut self) -> Result<()> {
        if let Err(KernelError::OutOfBounds) = self.sst_iter.prev() {
            if self.offset > 0 && self.is_valid() {
                let last_offset = self.offset - 1;
                self.sst_iter_sync(&self.ss_tables[last_offset], last_offset, Seek::Last)?;
            } else {
                return Err(KernelError::OutOfBounds);
            }
        }

        Ok(())
    }

    fn key(&self) -> Vec<u8> {
        self.sst_iter.key()
    }

    fn value(&self) -> &Value {
        self.sst_iter.value()
    }

    fn is_valid(&self) -> bool {
        self.offset < self.ss_tables.len() && self.sst_iter.is_valid()
    }

    fn seek(&mut self, seek: Seek) -> Result<()> {
        match seek {
            Seek::First => {
                let first = self.ss_tables.first().ok_or(KernelError::DataEmpty)?;
                self.sst_iter_sync(first, 0, Seek::First)?;
            }
            Seek::Last => {
                let last = self.ss_tables.last().ok_or(KernelError::DataEmpty)?;
                self.sst_iter_sync(last, self.ss_tables.len() - 1, Seek::Last)?;
            }
            Seek::Forward(key) => {
                self.seek_ward(key, seek)?;
            }
            Seek::Backward(key) => {
                self.seek_ward(key, seek)?;
            }
        }

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
    use crate::kernel::lsm::iterator::{DiskIter, Seek};
    use crate::kernel::lsm::iterator::level_iter::LevelIter;
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

        let times = 4000;

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
        let (slice_1, slice_2) = vec_cmd.split_at(2000);

        let ss_table_1 = SSTable::create_for_mem_table(
            &config,
            1,
            &sst_factory,
            slice_1.to_vec(),
            0
        )?;
        let ss_table_2 = SSTable::create_for_mem_table(
            &config,
            2,
            &sst_factory,
            slice_2.to_vec(),
            0
        )?;
        let cache = ShardingLruCache::new(
            config.block_cache_size,
            16,
            RandomState::default()
        )?;
        // 注意，SSTables的新旧顺序为旧->新
        let ss_tables = vec![ss_table_1, ss_table_2];

        let mut iterator = LevelIter::new(&ss_tables, 0, &cache)?;
        for i in 0..times - 1 {
            assert_eq!(&iterator.key(), vec_cmd[i].get_key());
            iterator.next()?;
        }

        for i in 0..times - 1 {
            assert_eq!(&iterator.key(), vec_cmd[times - i - 1].get_key());
            iterator.prev()?;
        }

        iterator.seek(Seek::Backward(vec_cmd[114].get_key()))?;
        assert_eq!(&iterator.key(), vec_cmd[114].get_key());

        iterator.seek(Seek::Forward(vec_cmd[1024].get_key()))?;
        assert_eq!(&iterator.key(), vec_cmd[1024].get_key());

        iterator.seek(Seek::Forward(vec_cmd[3333].get_key()))?;
        assert_eq!(&iterator.key(), vec_cmd[3333].get_key());

        iterator.seek(Seek::Backward(vec_cmd[2048].get_key()))?;
        assert_eq!(&iterator.key(), vec_cmd[2048].get_key());

        Ok(())
    }
}