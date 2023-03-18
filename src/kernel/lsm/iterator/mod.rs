mod block_iterator;

use crate::kernel::Result;

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

pub(crate) trait Iterator: Send + Sync {
    fn next(&mut self) -> Result<()>;

    fn prev(&mut self) -> Result<()>;

    fn key(&mut self) -> &[u8];

    fn value(&mut self) -> &Option<Vec<u8>>;

    fn is_valid(&self) -> bool;

    fn seek(&mut self, seek: Seek);
}