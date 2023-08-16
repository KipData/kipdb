use crate::kernel::lsm::iterator::level_iter::LevelIter;
use crate::kernel::lsm::iterator::merging_iter::MergingIter;
use crate::kernel::lsm::iterator::{Iter, Seek};
use crate::kernel::lsm::mem_table::KeyValue;
use crate::kernel::lsm::version::Version;
use crate::kernel::Result;

/// Version键值对迭代器
pub struct VersionIter<'a> {
    merge_iter: MergingIter<'a>,
}

impl<'a> VersionIter<'a> {
    pub(crate) fn new(version: &'a Version) -> Result<VersionIter<'a>> {
        let vec_iter = Self::merging_with_version(version)?;

        Ok(Self {
            merge_iter: MergingIter::new(vec_iter)?,
        })
    }

    pub(crate) fn merging_with_version(
        version: &'a Version,
    ) -> Result<Vec<Box<dyn Iter<'a, Item = KeyValue> + 'a + Send + Sync>>> {
        let mut vec_iter: Vec<Box<dyn Iter<'a, Item = KeyValue> + 'a + Send + Sync>> = Vec::new();

        for table in version.tables_by_level_0() {
            vec_iter.push(table.iter()?);
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

    fn try_next(&mut self) -> Result<Option<Self::Item>> {
        self.merge_iter.try_next()
    }

    fn is_valid(&self) -> bool {
        self.merge_iter.is_valid()
    }

    fn seek(&mut self, seek: Seek<'_>) -> Result<Option<Self::Item>> {
        self.merge_iter.seek(seek)
    }
}
