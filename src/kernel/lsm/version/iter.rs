use crate::kernel::lsm::iterator::level_iter::LevelIter;
use crate::kernel::lsm::iterator::merging_iter::SeekMergingIter;
use crate::kernel::lsm::iterator::{Iter, Seek, SeekIter};
use crate::kernel::lsm::mem_table::KeyValue;
use crate::kernel::lsm::version::Version;
use crate::kernel::lsm::MAX_LEVEL;
use crate::kernel::KernelResult;

/// Version键值对迭代器
pub struct VersionIter<'a> {
    merge_iter: SeekMergingIter<'a>,
}

impl<'a> VersionIter<'a> {
    pub(crate) fn new(version: &'a Version) -> KernelResult<VersionIter<'a>> {
        let mut vec_iter = Vec::new();
        Self::merging_with_version(version, &mut vec_iter)?;

        Ok(Self {
            merge_iter: SeekMergingIter::new(vec_iter)?,
        })
    }

    pub(crate) fn merging_with_version(
        version: &'a Version,
        iter_vec: &mut Vec<Box<dyn SeekIter<'a, Item = KeyValue> + 'a + Send + Sync>>,
    ) -> KernelResult<()> {
        for table in version.tables_by_level_0() {
            iter_vec.push(table.iter()?);
        }

        for level in 1..MAX_LEVEL - 1 {
            if let Ok(level_iter) = LevelIter::new(version, level) {
                iter_vec.push(Box::new(level_iter));
            }
        }

        Ok(())
    }
}

impl<'a> Iter<'a> for VersionIter<'a> {
    type Item = KeyValue;

    fn try_next(&mut self) -> KernelResult<Option<Self::Item>> {
        self.merge_iter.try_next()
    }

    fn is_valid(&self) -> bool {
        self.merge_iter.is_valid()
    }
}

impl<'a> SeekIter<'a> for VersionIter<'a> {
    fn seek(&mut self, seek: Seek<'_>) -> KernelResult<()> {
        self.merge_iter.seek(seek)
    }
}
