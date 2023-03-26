mod block_iter;
mod sstable_iter;
mod level_iter;
pub(crate) mod version_iter;

use crate::kernel::Result;

#[derive(Clone, Copy)]
#[allow(dead_code)]
pub(crate) enum Seek<'s> {
    // 第一个元素
    First,
    // 最后一个元素
    Last,
    // 与key相等或稍小的元素
    Forward(&'s [u8]),
    // 与key相等或稍大的元素
    Backward(&'s [u8])
}

impl<'s> Seek<'s> {
    pub(crate) fn get_key(&self) -> Option<&'s [u8]> {
        match self {
            Seek::Forward(key) => Some(key),
            Seek::Backward(key) => Some(key),

            _ => None
        }
    }
}

/// 硬盘迭代器
pub(crate) trait DiskIter<K, V>: Send + Sync {
    fn next(&mut self) -> Result<()>;

    fn prev(&mut self) -> Result<()>;

    fn key(&self) -> K;

    fn value(&self) -> &V;

    fn is_valid(&self) -> bool;

    fn seek(&mut self, seek: Seek) -> Result<()>;
}