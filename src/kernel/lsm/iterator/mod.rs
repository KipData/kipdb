pub(crate) mod full_iter;
pub(crate) mod level_iter;
pub(crate) mod merging_iter;

use crate::kernel::Result;

#[derive(Clone, Copy)]
#[allow(dead_code)]
pub enum Seek<'s> {
    // 第一个元素
    First,
    // 最后一个元素
    Last,
    // 与key相等或稍大的元素
    Backward(&'s [u8]),
}

/// 硬盘迭代器
pub trait Iter<'a> {
    type Item;

    fn try_next(&mut self) -> Result<Option<Self::Item>>;

    fn is_valid(&self) -> bool;

    fn seek(&mut self, seek: Seek<'_>) -> Result<Option<Self::Item>>;
}

/// 向前迭代器
pub(crate) trait ForwardIter<'a>: Iter<'a> {
    fn try_prev(&mut self) -> Result<Option<Self::Item>>;
}
