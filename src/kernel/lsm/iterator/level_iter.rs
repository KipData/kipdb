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
        let sst_iter = SSTableIter::new(&ss_tables[0], block_cache)?;

        Ok(Self {
            ss_tables,
            block_cache,
            level,
            offset: 0,
            sst_iter,
        })
    }

    fn sst_iter_seek(&mut self, seek: Seek, offset: usize) -> Result<(Vec<u8>, Option<Vec<u8>>)> {
        self.offset = offset;
        if self.is_valid() {
            let ss_table = &self.ss_tables[offset];
            if ss_table.get_gen() != self.sst_iter.get_gen() {
                self.sst_iter = SSTableIter::new(ss_table, &self.block_cache)?;
            }
            self.sst_iter.seek(seek)
        } else { Err(KernelError::OutOfBounds) }
    }

    fn seek_ward(&mut self, key: &[u8], seek: Seek) -> Result<(Vec<u8>, Option<Vec<u8>>)> {
        let i = self.ss_tables.len() - 1;
        // 对Level 0中的SSTable数据范围是可能重复的，因此需要遍历确认数据才能判断是否存在数据
        if self.level > 0 {
            let (offset, _) = self.ss_tables.iter().rev()
                .enumerate()
                .rfind(|(_, ss_table)| ss_table.get_scope().meet_with_key(key))
                .ok_or(KernelError::DataEmpty)?;

            self.sst_iter_seek(seek, i - offset)
        } else {
            let mut item = (vec![], None);
            for (offset, _) in self.ss_tables.iter().rev().enumerate() {
                item = self.sst_iter_seek(seek, i - offset)?;
                if key == &item.0 { break }
            }
            Ok(item)
        }
    }
}

impl DiskIter<Vec<u8>, Value> for LevelIter<'_> {
    type Item = (Vec<u8>, Option<Vec<u8>>);

    fn next(&mut self) -> Result<Self::Item> {
        match self.sst_iter.next() {
            Ok(item) => Ok(item),
            Err(KernelError::OutOfBounds) => {
                self.sst_iter_seek(Seek::First, self.offset + 1)
            },
            Err(e) => Err(e)
        }
    }

    fn prev(&mut self) -> Result<Self::Item> {
        match self.sst_iter.prev() {
            Ok(item) => Ok(item),
            Err(KernelError::OutOfBounds) => {
                if self.offset > 0 {
                    self.sst_iter_seek(Seek::Last, self.offset - 1)
                } else {
                    Err(KernelError::OutOfBounds)
                }
            },
            Err(e) => Err(e)
        }
    }

    fn is_valid(&self) -> bool {
        self.offset < self.ss_tables.len()
    }

    fn seek(&mut self, seek: Seek) -> Result<Self::Item> {
        match seek {
            Seek::First => {
                self.sst_iter_seek(Seek::First, 0)
            }
            Seek::Last => {
                self.sst_iter_seek(Seek::Last, self.ss_tables.len() - 1)
            }
            Seek::Forward(key) => {
                self.seek_ward(key, seek)
            }
            Seek::Backward(key) => {
                self.seek_ward(key, seek)
            }
        }
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
        let mut vec_data = Vec::new();

        let times = 4000;

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
        let (slice_1, slice_2) = vec_data.split_at(2000);

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
        for i in 0..times {
            assert_eq!(iterator.next()?, vec_data[i]);
        }

        for i in (0..times - 1).rev() {
            assert_eq!(iterator.prev()?, vec_data[i]);
        }

        assert_eq!(iterator.seek(Seek::Backward(&vec_data[114].0))?, vec_data[114]);

        assert_eq!(iterator.seek(Seek::Forward(&vec_data[1024].0))?, vec_data[1024]);

        assert_eq!(iterator.seek(Seek::Forward(&vec_data[3333].0))?, vec_data[3333]);

        assert_eq!(iterator.seek(Seek::Backward(&vec_data[2048].0))?, vec_data[2048]);

        assert_eq!(iterator.seek(Seek::First)?, vec_data[0]);

        assert_eq!(iterator.seek(Seek::Last)?, vec_data[3999]);

        Ok(())
    }
}