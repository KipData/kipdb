use crate::kernel::lsm::iterator::{Iter, Seek};
use crate::kernel::lsm::iterator::level_iter::LevelIter;
use crate::kernel::lsm::iterator::merging_iter::MergingIter;
use crate::kernel::lsm::mem_table::KeyValue;
use crate::kernel::lsm::ss_table::ss_table_iter::SSTableIter;
use crate::kernel::lsm::version::Version;
use crate::kernel::Result;

/// Version键值对迭代器
pub struct VersionIter<'a> {
    merge_iter: MergingIter<'a>
}

impl<'a> VersionIter<'a> {
    pub(crate) fn new(version: &'a Version) -> Result<VersionIter<'a>> {
        let vec_iter = Self::merging_with_version(version)?;

        Ok(Self { merge_iter: MergingIter::new(vec_iter)? })
    }

    pub(crate) fn merging_with_version(version: &'a Version) -> Result<Vec<Box<dyn Iter<'a, Item=KeyValue> + 'a>>> {
        let mut vec_iter: Vec<Box<dyn Iter<'a, Item=KeyValue> + 'a>> = Vec::new();

        for ss_table in version.get_ss_tables_with_level_0() {
            vec_iter.push(Box::new(SSTableIter::new(ss_table, &version.block_cache)?));
        }

        for level in 1..6 {
            if let Ok(level_iter) = LevelIter::new(version, level) {
                vec_iter.push(Box::new(level_iter));
            }
        }

        Ok(vec_iter)
    }
}

impl<'a> Iter<'a> for VersionIter<'a> {
    type Item = KeyValue;

    fn next_err(&mut self) -> Result<Option<Self::Item>> {
        self.merge_iter.next_err()
    }

    fn is_valid(&self) -> bool {
        self.merge_iter.is_valid()
    }

    fn seek(&mut self, seek: Seek<'_>) -> Result<Option<Self::Item>> {
        self.merge_iter.seek(seek)
    }
}