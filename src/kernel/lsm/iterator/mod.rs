mod block_iter;
mod sstable_iter;

use crate::kernel::Result;

#[derive(Clone)]
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

pub(crate) trait ReadIterator<T>: Iterator<T> {
    fn item(&self) -> &T;
}

pub(crate) trait Iterator<T>: Send + Sync {
    fn next(&mut self) -> Result<()>;

    fn prev(&mut self) -> Result<()>;

    fn item_owner(&self) -> T;

    fn is_valid(&self) -> bool;

    fn seek(&mut self, seek: Seek) -> Result<()>;
}