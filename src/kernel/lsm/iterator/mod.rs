pub(crate) mod block_iter;
pub(crate) mod ss_table_iter;
pub(crate) mod level_iter;
pub(crate) mod version_iter;
mod merging_iter;

use crate::kernel::Result;

#[derive(Clone, Copy)]
#[allow(dead_code)]
pub(crate) enum Seek<'s> {
    // 第一个元素
    First,
    // 最后一个元素
    Last,
    // 与key相等或稍大的元素
    Backward(&'s [u8])
}

/// 硬盘迭代器
pub(crate) trait Iter<'a> {
    type Item;

    fn next_err(&mut self) -> Result<Option<Self::Item>>;

    fn is_valid(&self) -> bool;

    fn seek(&mut self, seek: Seek<'_>) -> Result<Option<Self::Item>>;
}

/// 向前迭代器
pub(crate) trait ForwardDiskIter<'a>: Iter<'a> {
    fn prev_err(&mut self) -> Result<Option<Self::Item>>;
}