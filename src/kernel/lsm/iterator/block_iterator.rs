use crate::kernel::lsm::iterator::{Iterator, Seek};
use crate::kernel::lsm::block::{Block, KeyValue, Value};
use crate::KvsError;

pub(crate) struct BlockIterator<'a, T> {
    block: &'a Block<T>,
    entry_len: usize,

    offset: usize,
    kv_buf: Option<KeyValue<T>>
}

impl<'a, T> BlockIterator<'a, T> {
    #[allow(dead_code)]
    pub(crate) fn new(block: &'a Block<T>) -> BlockIterator<'a, T> {
        let iterator = BlockIterator {
            block,
            entry_len: block.entry_len(),
            offset: 0,
            kv_buf: None,
        };
        iterator
    }

    pub(crate) fn offset_move(&mut self, offset: usize) {
        self.offset = offset;
        self.kv_buf = None;
    }
}

impl Iterator for BlockIterator<'_, Value> {
    fn next(&mut self) -> crate::kernel::Result<()> {
        let next_offset = self.offset + 1;
        if next_offset >= self.entry_len || !self.is_valid() {
            return Err(KvsError::OutOfBounds)
        } else {
            self.offset_move(next_offset)
        }
        Ok(())
    }

    fn prev(&mut self) -> crate::kernel::Result<()> {
        if self.is_valid() && self.offset > 0 {
            self.offset_move(self.offset - 1)
        } else {
            return Err(KvsError::OutOfBounds)
        }
        Ok(())
    }

    fn key(&mut self) -> &[u8] {
        &self.current_key_value().0
    }

    fn value(&mut self) -> &Option<Vec<u8>> {
        self.current_key_value().1.value()
    }

    fn is_valid(&self) -> bool {
        self.offset < self.entry_len
    }

    fn seek(&mut self, seek: Seek) {
        match seek {
            Seek::First => {
                self.offset_move(0)
            }
            Seek::Last => {
                self.offset_move(self.entry_len - 1)
            }
            Seek::Forward(key) => {
                let offset = self.block.binary_search(key)
                    .unwrap_or_else(|index| index - 1);
                self.offset_move(offset)
            }
            Seek::Backward(key) => {
                let offset = self.block.binary_search(key)
                    .unwrap_or_else(|index| index);
                self.offset_move(offset)
            }
        }
    }
}

impl BlockIterator<'_, Value> {
    fn current_key_value(&mut self) -> &KeyValue<Value> {
        self.kv_buf.get_or_insert_with(|| self.block.get_item(self.offset))
    }
}

#[cfg(test)]
mod tests {
    use std::vec;
    use crate::kernel::lsm::block::{Block, DEFAULT_DATA_RESTART_INTERVAL, Value};
    use crate::kernel::lsm::iterator::block_iterator::BlockIterator;
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

        assert_eq!(iterator.key(), &vec![b'1']);
        iterator.next()?;
        assert_eq!(iterator.key(), &vec![b'2']);
        iterator.next()?;
        assert_eq!(iterator.key(), &vec![b'4']);

        assert!(iterator.next().is_err());

        assert_eq!(iterator.key(), &vec![b'4']);
        iterator.prev()?;
        assert_eq!(iterator.key(), &vec![b'2']);
        iterator.prev()?;
        assert_eq!(iterator.key(), &vec![b'1']);

        assert!(iterator.prev().is_err());

        iterator.seek(Seek::First);
        assert_eq!(iterator.key(), &vec![b'1']);

        iterator.seek(Seek::Last);
        assert_eq!(iterator.key(), &vec![b'4']);

        iterator.seek(Seek::Forward(&vec![b'3']));
        assert_eq!(iterator.key(), &vec![b'2']);

        iterator.seek(Seek::Backward(&vec![b'3']));
        assert_eq!(iterator.key(), &vec![b'4']);

        Ok(())
    }
}