use core::slice::SlicePattern;
use std::cmp::min;
use std::iter::Iterator;
use bytes::Bytes;
use itertools::Itertools;
use async_trait::async_trait;
use crate::kernel::lsm::iterator::{Seek, DiskIter};
use crate::kernel::lsm::block::{Block, BlockItem};
use crate::kernel::Result;
use crate::KernelError;

/// Block迭代器
///
/// Tips: offset偏移会额外向上偏移一位以使用0作为迭代的下界判断是否向前溢出了
pub(crate) struct BlockIter<'a, T> {
    block: &'a Block<T>,
    entry_len: usize,

    offset: usize,
    buf_shared_key: &'a [u8]
}

impl<'a, T> BlockIter<'a, T> where T: BlockItem {
    pub(crate) fn new(block: &'a Block<T>) -> BlockIter<'a, T> {
        let buf_shared_key = block.shared_key_prefix(
            0, block.restart_shared_len(0)
        );

        BlockIter {
            block,
            entry_len: block.entry_len(),
            offset: 0,
            buf_shared_key,
        }
    }

    fn item(&self) -> (Bytes, T) {
        let offset = self.offset - 1;
        let entry = self.block.get_entry(offset);

        (if offset % self.block.restart_interval() != 0 {
            Bytes::from(self.buf_shared_key.iter()
                .chain(entry.key().as_slice())
                .copied()
                .collect_vec())
        } else { entry.key().clone() }, entry.item().clone())
    }

    fn offset_move(&mut self, offset: usize) -> Result<(Bytes, T)>{
        let block = self.block;
        let restart_interval = block.restart_interval();

        let old_offset = self.offset;
        self.offset = offset;

        if offset > 0 {
            let real_offset = offset - 1;
            if old_offset - 1 / restart_interval != real_offset / restart_interval {
                self.buf_shared_key = block.shared_key_prefix(
                    real_offset, block.restart_shared_len(real_offset)
                );
            }
            Ok(self.item())
        } else { Err(KernelError::OutOfBounds) }
    }
}

#[async_trait]
#[allow(single_use_lifetimes)]
impl<V> DiskIter<Vec<u8>, V> for BlockIter<'_, V>
    where V: Sync + Send + BlockItem
{
    type Item = (Bytes, V);

    async fn next_err(&mut self) -> Result<Self::Item> {
        if self.is_valid() || self.offset == 0 {
            self.offset_move(self.offset + 1)
        } else { Err(KernelError::OutOfBounds) }
    }

    async fn prev_err(&mut self) -> Result<Self::Item> {
        if self.is_valid() || self.offset == self.entry_len {
            self.offset_move(self.offset - 1)
        } else { Err(KernelError::OutOfBounds) }
    }

    fn is_valid(&self) -> bool {
        self.offset > 0 && self.offset < self.entry_len
    }

    async fn seek(&mut self, seek: Seek<'_>) -> Result<Self::Item> {
        self.offset_move(match seek {
            Seek::First => 0,
            Seek::Last => self.entry_len - 1,
            Seek::Forward(key) => {
                self.block.binary_search(key)
                    .unwrap_or_else(|index| index.saturating_sub(1))
            }
            Seek::Backward(key) => {
                self.block.binary_search(key)
                    .unwrap_or_else(|index| min(self.entry_len - 1, index))
            }
        } + 1)
    }
}

#[cfg(test)]
mod tests {
    use std::vec;
    use bincode::Options;
    use bytes::Bytes;
    use crate::kernel::lsm::block::{Block, DEFAULT_DATA_RESTART_INTERVAL, Value};
    use crate::kernel::lsm::iterator::block_iter::BlockIter;
    use crate::kernel::lsm::iterator::{DiskIter, Seek};
    use crate::kernel::Result;

    #[test]
    fn test_iterator() -> Result<()> {
        let data = vec![
            (Bytes::from(vec![b'1']), Value::from(None)),
            (Bytes::from(vec![b'2']), Value::from(Some(Bytes::from(vec![b'0'])))),
            (Bytes::from(vec![b'4']), Value::from(None)),
        ];
        let block = Block::new(data, DEFAULT_DATA_RESTART_INTERVAL);

        tokio_test::block_on(async move {
            let mut iterator = BlockIter::new(&block);

            assert!(!iterator.is_valid());

            assert_eq!(iterator.next_err().await?, (Bytes::from(vec![b'1']), Value::from(None)));

            assert_eq!(iterator.next_err().await?, (Bytes::from(vec![b'2']), Value::from(Some(Bytes::from(vec![b'0'])))));

            assert_eq!(iterator.next_err().await?, (Bytes::from(vec![b'4']), Value::from(None)));

            assert!(iterator.next_err().await.is_err());

            assert_eq!(iterator.prev_err().await?, (Bytes::from(vec![b'2']), Value::from(Some(Bytes::from(vec![b'0'])))));

            assert_eq!(iterator.prev_err().await?, (Bytes::from(vec![b'1']), Value::from(None)));

            assert!(iterator.prev_err().await.is_err());

            assert_eq!(iterator.seek(Seek::First).await?, (Bytes::from(vec![b'1']), Value::from(None)));

            assert_eq!(iterator.seek(Seek::Last).await?, (Bytes::from(vec![b'4']), Value::from(None)));

            assert_eq!(iterator.seek(Seek::Forward(&vec![b'2'])).await?, (Bytes::from(vec![b'2']), Value::from(Some(Bytes::from(vec![b'0'])))));

            assert_eq!(iterator.seek(Seek::Backward(&vec![b'2'])).await?, (Bytes::from(vec![b'2']), Value::from(Some(Bytes::from(vec![b'0'])))));

            assert_eq!(iterator.seek(Seek::Forward(&vec![b'3'])).await?, (Bytes::from(vec![b'2']), Value::from(Some(Bytes::from(vec![b'0'])))));

            assert_eq!(iterator.seek(Seek::Backward(&vec![b'3'])).await?, (Bytes::from(vec![b'4']), Value::from(None)));

            Ok(())
        })
    }

    #[test]
    fn test_iterator_1000() -> Result<()> {
        let mut vec_data = Vec::new();
        let value = Bytes::from_static(b"What you are you do not see, what you see is your shadow.");

        let times = 1000;
        // 默认使用大端序进行序列化，保证顺序正确性
        for i in 0..times {
            let mut key = b"KipDB-".to_vec();
            key.append(
                &mut bincode::options().with_big_endian().serialize(&i)?
            );
            vec_data.push(
                (Bytes::from(key), Value::from(Some(value.clone())))
            );
        }
        let block = Block::new(vec_data.clone(), DEFAULT_DATA_RESTART_INTERVAL);

        tokio_test::block_on(async move {
            let mut iterator = BlockIter::new(&block);

            for i in 0..times {
                assert_eq!(iterator.next_err().await?, vec_data[i]);
            }

            for i in (0..times - 1).rev() {
                assert_eq!(iterator.prev_err().await?, vec_data[i]);
            }

            Ok(())
        })
    }
}