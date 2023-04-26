use crate::kernel::lsm::block::{BlockCache, Value};
use crate::kernel::lsm::iterator::{DiskIter, Seek};
use crate::kernel::lsm::iterator::sstable_iter::SSTableIter;
use crate::kernel::lsm::mem_table::KeyValue;
use crate::kernel::lsm::ss_table::SSTable;
use crate::kernel::Result;
use crate::KernelError;

const LEVEL_0_SEEK_MESSAGE: &str = "level 0 cannot seek";

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

    fn sst_iter_seek(&mut self, seek: Seek, offset: usize) -> Result<KeyValue> {
        self.offset = offset;
        if self.is_valid() {
            let ss_table = &self.ss_tables[offset];
            if ss_table.get_gen() != self.sst_iter.get_gen() {
                self.sst_iter = SSTableIter::new(ss_table, self.block_cache)?;
            }
            self.sst_iter.seek(seek)
        } else { Err(KernelError::OutOfBounds) }
    }

    fn seek_ward(&mut self, key: &[u8], seek: Seek) -> Result<KeyValue> {
        if self.level == 0 {
            return Err(KernelError::NotSupport(LEVEL_0_SEEK_MESSAGE));
        }
        let offset = self.ss_tables
            .binary_search_by(|ss_table| ss_table.get_scope().start.as_ref().cmp(key))
            .unwrap_or_else(|index| index.checked_sub(1).unwrap_or(0));

        self.sst_iter_seek(seek, offset)
    }
}

impl DiskIter<Vec<u8>, Value> for LevelIter<'_> {
    type Item = KeyValue;

    fn next_err(&mut self) -> Result<Self::Item> {
        match DiskIter::next_err(&mut self.sst_iter) {
            Ok(item) => Ok(item),
            Err(KernelError::OutOfBounds) => {
                self.sst_iter_seek(Seek::First, self.offset + 1)
            },
            Err(e) => Err(e)
        }
    }

    fn prev_err(&mut self) -> Result<Self::Item> {
        match self.sst_iter.prev_err() {
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

    /// Tips: Level 0的LevelIter不支持Seek
    /// 因为Level 0中的SSTable并非有序排列，其中数据范围是可能交错的
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

impl Iterator for LevelIter<'_> {
    type Item = KeyValue;

    fn next(&mut self) -> Option<Self::Item> {
        DiskIter::next_err(self).ok()
    }
}

impl DoubleEndedIterator for LevelIter<'_> {
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

        let value = Bytes::from_static(b"What you are you do not see, what you see is your shadow.");
        let mut vec_data = Vec::new();

        let times = 4000;

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
        let (slice_1, slice_2) = vec_data.split_at(2000);

        let ss_table_1 = SSTable::create_for_mem_table(
            &config,
            1,
            &sst_factory,
            slice_1.to_vec(),
            1
        )?;
        let ss_table_2 = SSTable::create_for_mem_table(
            &config,
            2,
            &sst_factory,
            slice_2.to_vec(),
            1
        )?;
        let cache = ShardingLruCache::new(
            config.block_cache_size,
            16,
            RandomState::default()
        )?;
        // 注意，SSTables的新旧顺序为旧->新
        let ss_tables = vec![ss_table_1, ss_table_2];

        let mut iterator = LevelIter::new(&ss_tables, 1, &cache)?;
        for i in 0..times {
            assert_eq!(DiskIter::next_err(&mut iterator)?, vec_data[i]);
        }

        for i in (0..times - 1).rev() {
            assert_eq!(DiskIter::prev_err(&mut iterator)?, vec_data[i]);
        }

        assert_eq!(iterator.seek(Seek::Backward(&vec_data[114].0))?, vec_data[114]);

        assert_eq!(iterator.seek(Seek::Forward(&vec_data[1024].0))?, vec_data[1024]);

        assert_eq!(iterator.seek(Seek::Forward(&vec_data[3333].0))?, vec_data[3333]);

        assert_eq!(iterator.seek(Seek::Backward(&vec_data[2048].0))?, vec_data[2048]);

        assert_eq!(iterator.seek(Seek::First)?, vec_data[0]);

        assert_eq!(iterator.seek(Seek::Last)?, vec_data[3999]);

        let mut iterator_level_0 = LevelIter::new(&ss_tables, 0, &cache)?;

        assert!(iterator_level_0.seek(Seek::Forward(&vec_data[3333].0)).is_err());

        Ok(())
    }
}