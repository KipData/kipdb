pub(crate) mod block_iter;
pub(crate) mod ss_table_iter;
pub(crate) mod level_iter;
pub(crate) mod version_iter;
mod merging_iter;

use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
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
pub(crate) trait Iter {
    type Item;

    fn next_err(&mut self) -> Result<Option<Self::Item>>;

    fn is_valid(&self) -> bool;

    fn seek(&mut self, seek: Seek<'_>) -> Result<Option<Self::Item>>;
}

/// 向前迭代器
pub(crate) trait ForwardDiskIter: Iter {

    fn prev_err(&mut self) -> Result<Option<Self::Item>>;
}

pub(crate) struct InnerPtr<T>(NonNull<T>);

unsafe impl<T: Send> Send for InnerPtr<T> { }
unsafe impl<T: Sync> Sync for InnerPtr<T> { }

impl<T> Clone for InnerPtr<T> {
    fn clone(&self) -> Self {
        InnerPtr(self.0)
    }
}

impl<T> Copy for InnerPtr<T> { }

impl<T> Deref for InnerPtr<T> {
    type Target = NonNull<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for InnerPtr<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}