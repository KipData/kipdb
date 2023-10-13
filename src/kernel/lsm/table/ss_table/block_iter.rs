use crate::kernel::lsm::iterator::{ForwardIter, Iter, Seek};
use crate::kernel::lsm::table::ss_table::block::{Block, BlockItem, Entry};
use crate::kernel::KernelResult;
use bytes::Bytes;

/// Block迭代器
///
/// Tips: offset偏移会额外向上偏移一位以使用0作为迭代的下界判断是否向前溢出了
pub(crate) struct BlockIter<'a, T> {
    block: &'a Block<T>,
    entry_len: usize,

    offset: usize,
    buf_shared_key: &'a [u8],
}

impl<'a, T> BlockIter<'a, T>
where
    T: BlockItem,
{
    pub(crate) fn new(block: &'a Block<T>) -> BlockIter<'a, T> {
        let buf_shared_key = block.shared_key_prefix(0, block.restart_shared_len(0));

        BlockIter {
            block,
            entry_len: block.entry_len(),
            offset: 0,
            buf_shared_key,
        }
    }

    fn item(&self) -> (Bytes, T) {
        let offset = self.offset - 1;
        let Entry { key, item, .. } = self.block.get_entry(offset);
        let item_key = if offset % self.block.restart_interval() != 0 {
            Bytes::from([self.buf_shared_key, &key[..]].concat())
        } else {
            key.clone()
        };

        (item_key, item.clone())
    }

    fn offset_move(&mut self, offset: usize) -> Option<(Bytes, T)> {
        let block = self.block;
        let restart_interval = block.restart_interval();

        let old_offset = self.offset;
        self.offset = offset;

        (offset > 0).then(|| {
            let real_offset = offset - 1;
            if old_offset - 1 / restart_interval != real_offset / restart_interval {
                self.buf_shared_key =
                    block.shared_key_prefix(real_offset, block.restart_shared_len(real_offset));
            }
            self.item()
        })
    }
}

impl<'a, V> ForwardIter<'a> for BlockIter<'a, V>
where
    V: Sync + Send + BlockItem,
{
    fn try_prev(&mut self) -> KernelResult<Option<Self::Item>> {
        Ok((self.is_valid() || self.offset == self.entry_len)
            .then(|| self.offset_move(self.offset - 1))
            .flatten())
    }
}

impl<'a, V> Iter<'a> for BlockIter<'a, V>
where
    V: Sync + Send + BlockItem,
{
    type Item = (Bytes, V);

    fn try_next(&mut self) -> KernelResult<Option<Self::Item>> {
        Ok((self.is_valid() || self.offset == 0)
            .then(|| self.offset_move(self.offset + 1))
            .flatten())
    }

    fn is_valid(&self) -> bool {
        self.offset > 0 && self.offset < self.entry_len
    }

    fn seek(&mut self, seek: Seek<'_>) -> KernelResult<Option<Self::Item>> {
        Ok(match seek {
            Seek::First => Some(0),
            Seek::Last => Some(self.entry_len - 1),
            Seek::Backward(key) => match self.block.binary_search(key) {
                Ok(index) => Some(index),
                Err(index) => (index < self.entry_len).then_some(index),
            },
        }
        .and_then(|index| self.offset_move(index + 1)))
    }
}

#[cfg(test)]
mod tests {
    use crate::kernel::lsm::iterator::{ForwardIter, Iter, Seek};
    use crate::kernel::lsm::table::ss_table::block::{Block, Value, DEFAULT_DATA_RESTART_INTERVAL};
    use crate::kernel::lsm::table::ss_table::block_iter::BlockIter;
    use crate::kernel::KernelResult;
    use bincode::Options;
    use bytes::Bytes;
    use std::vec;

    #[test]
    fn test_iterator() -> KernelResult<()> {
        let data = vec![
            (Bytes::from(vec![b'1']), Value::from(None)),
            (
                Bytes::from(vec![b'2']),
                Value::from(Some(Bytes::from(vec![b'0']))),
            ),
            (Bytes::from(vec![b'4']), Value::from(None)),
        ];
        let block = Block::new(data, DEFAULT_DATA_RESTART_INTERVAL);

        let mut iterator = BlockIter::new(&block);

        assert!(!iterator.is_valid());

        assert_eq!(
            iterator.try_next()?,
            Some((Bytes::from(vec![b'1']), Value::from(None)))
        );

        assert_eq!(
            iterator.try_next()?,
            Some((
                Bytes::from(vec![b'2']),
                Value::from(Some(Bytes::from(vec![b'0'])))
            ))
        );

        assert_eq!(
            iterator.try_next()?,
            Some((Bytes::from(vec![b'4']), Value::from(None)))
        );

        assert_eq!(iterator.try_next()?, None);

        assert_eq!(
            iterator.try_prev()?,
            Some((
                Bytes::from(vec![b'2']),
                Value::from(Some(Bytes::from(vec![b'0'])))
            ))
        );

        assert_eq!(
            iterator.try_prev()?,
            Some((Bytes::from(vec![b'1']), Value::from(None)))
        );

        assert_eq!(iterator.try_prev()?, None);

        assert_eq!(
            iterator.seek(Seek::First)?,
            Some((Bytes::from(vec![b'1']), Value::from(None)))
        );

        assert_eq!(
            iterator.seek(Seek::Last)?,
            Some((Bytes::from(vec![b'4']), Value::from(None)))
        );

        assert_eq!(
            iterator.seek(Seek::Backward(&vec![b'2']))?,
            Some((
                Bytes::from(vec![b'2']),
                Value::from(Some(Bytes::from(vec![b'0'])))
            ))
        );

        assert_eq!(
            iterator.seek(Seek::Backward(&vec![b'3']))?,
            Some((Bytes::from(vec![b'4']), Value::from(None)))
        );

        Ok(())
    }

    #[test]
    fn test_iterator_1000() -> KernelResult<()> {
        let mut vec_data = Vec::new();
        let value =
            Bytes::from_static(b"What you are you do not see, what you see is your shadow.");

        let times = 1000;
        // 默认使用大端序进行序列化，保证顺序正确性
        for i in 0..times {
            let mut key = b"KipDB-".to_vec();
            key.append(&mut bincode::options().with_big_endian().serialize(&i)?);
            vec_data.push((Bytes::from(key), Value::from(Some(value.clone()))));
        }
        let block = Block::new(vec_data.clone(), DEFAULT_DATA_RESTART_INTERVAL);

        tokio_test::block_on(async move {
            let mut iterator = BlockIter::new(&block);

            for i in 0..times {
                assert_eq!(iterator.try_next()?.unwrap(), vec_data[i]);
            }

            for i in (0..times - 1).rev() {
                assert_eq!(iterator.try_prev()?.unwrap(), vec_data[i]);
            }

            Ok(())
        })
    }
}
