use crate::kernel::lsm::iterator::{Iterator, Seek};
use crate::kernel::lsm::block::{Block, BlockItem, KeyValue};
use crate::kernel::Result;
use crate::KernelError;

pub(crate) struct BlockIterator<'a, T> {
    block: &'a Block<T>,
    entry_len: usize,

    offset: usize,
    kv_buf: KeyValue<T>
}

impl<'a, T> BlockIterator<'a, T> where T: BlockItem {
    pub(crate) fn new(block: &'a Block<T>) -> BlockIterator<'a, T> {
        let iterator = BlockIterator {
            block,
            entry_len: block.entry_len(),
            offset: 0,
            kv_buf: block.get_item(0),
        };
        iterator
    }

    pub(crate) fn offset_move(&mut self, offset: usize) {
        self.offset = offset;
        self.kv_buf = self.block.get_item(offset);
    }
}

impl<T> Iterator<KeyValue<T>> for BlockIterator<'_, T> where T: Sync + Send + BlockItem {
    fn next(&mut self) -> Result<()> {
        let next_offset = self.offset + 1;
        if next_offset < self.entry_len && self.is_valid() {
            self.offset_move(next_offset)
        } else {
            return Err(KernelError::OutOfBounds);
        }
        Ok(())
    }

    fn prev(&mut self) -> Result<()> {
        if self.is_valid() && self.offset > 0 {
            self.offset_move(self.offset - 1)
        } else {
            return Err(KernelError::OutOfBounds);
        }
        Ok(())
    }

    fn item(&self) -> &KeyValue<T> {
        &self.kv_buf
    }

    fn is_valid(&self) -> bool {
        self.offset < self.entry_len
    }

    fn seek(&mut self, seek: Seek) -> Result<()> {
        match seek {
            Seek::First => {
                self.offset_move(0)
            }
            Seek::Last => {
                self.offset_move(self.entry_len - 1)
            }
            Seek::Forward(key) => {
                let offset = self.block.binary_search(key)
                    .unwrap_or_else(|index| index.checked_sub(1).unwrap_or(0));
                self.offset_move(offset)
            }
            Seek::Backward(key) => {
                let offset = self.block.binary_search(key)
                    .unwrap_or_else(|index| index);
                self.offset_move(offset)
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::vec;
    use crate::kernel::lsm::block::{Block, DEFAULT_DATA_RESTART_INTERVAL, Value};
    use crate::kernel::lsm::iterator::block_iter::BlockIterator;
    use crate::kernel::lsm::iterator::{Iterator, Seek};
    use crate::kernel::Result;

    #[test]
    fn test_iterator() -> Result<()> {
        let data = vec![
            (vec![b'1'], Value::from(None)),
            (vec![b'2'], Value::from(Some(vec![b'0']))),
            (vec![b'4'], Value::from(None)),
        ];
        let block = Block::new(data, DEFAULT_DATA_RESTART_INTERVAL);
        let mut iterator = BlockIterator::new(&block);

        assert!(iterator.is_valid());

        assert_eq!(&iterator.item().0, &vec![b'1']);
        iterator.next()?;
        assert_eq!(&iterator.item().0, &vec![b'2']);
        iterator.next()?;
        assert_eq!(&iterator.item().0, &vec![b'4']);

        assert!(iterator.next().is_err());

        assert_eq!(&iterator.item().0, &vec![b'4']);
        iterator.prev()?;
        assert_eq!(&iterator.item().0, &vec![b'2']);
        iterator.prev()?;
        assert_eq!(&iterator.item().0, &vec![b'1']);

        assert!(iterator.prev().is_err());

        iterator.seek(Seek::First)?;
        assert_eq!(&iterator.item().0, &vec![b'1']);

        iterator.seek(Seek::Last)?;
        assert_eq!(&iterator.item().0, &vec![b'4']);

        iterator.seek(Seek::Forward(&vec![b'3']))?;
        assert_eq!(&iterator.item().0, &vec![b'2']);

        iterator.seek(Seek::Backward(&vec![b'3']))?;
        assert_eq!(&iterator.item().0, &vec![b'4']);

        Ok(())
    }
}