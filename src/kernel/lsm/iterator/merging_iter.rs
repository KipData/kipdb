use std::cmp::Ordering;
use std::collections::BTreeMap;
use async_trait::async_trait;
use bytes::Bytes;
use crate::kernel::lsm::iterator::{Iter, Seek};
use crate::kernel::Result;

/// 用于取值以及对应的Iter下标
/// 通过序号进行同值优先获取
#[derive(Eq, PartialEq, Debug)]
struct IterKey {
    num: usize,
    key: Bytes
}

impl PartialOrd<Self> for IterKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.key.partial_cmp(&other.key)
            .and_then(|ord| {
                match ord {
                    Ordering::Equal => self.num.partial_cmp(&other.num),
                    ordering => Some(ordering)
                }
            })
    }
}

impl Ord for IterKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
            .then_with(|| self.num.cmp(&other.num))
    }
}

pub(crate) struct MergingIter<I: Iter> {
    vec_iter: Vec<I>,
    map_buf: BTreeMap<IterKey, I::Item>
}

impl<I: Iter> MergingIter<I> {
    #[allow(dead_code, clippy::mutable_key_type)]
    pub(crate) fn new(mut vec_iter: Vec<I>) -> Result<Self> {
        let mut map_buf = BTreeMap::new();

        for (num, iter) in vec_iter.iter_mut().enumerate() {
            if let Some(item) = iter.next_err()? {
                Self::buf_map_insert(&mut map_buf, num, item);
            }
        }

        Ok(MergingIter { vec_iter, map_buf })
    }
}

#[async_trait]
impl<I: Iter> Iter for MergingIter<I> {
    type Item = I::Item;

    fn next_err(&mut self) -> Result<Option<Self::Item>> {
        if let Some((IterKey{ num, .. }, old_item)) = self.map_buf.pop_first() {
            if let Some(item) = self.vec_iter[num].next_err()? {
                let _ = self.map_buf.insert(IterKey { num, key: Self::item_key(&item) }, item);
            }
            return Ok(Some(old_item))
        }

        Ok(None)
    }

    fn is_valid(&self) -> bool {
        self.vec_iter.iter()
            .map(|iter| iter.is_valid())
            .all(|is_valid| is_valid)
    }

    #[allow(clippy::mutable_key_type)]
    fn seek(&mut self, seek: Seek<'_>) -> Result<Option<Self::Item>> {
        let mut seek_map = BTreeMap::new();

        for (num, iter) in self.vec_iter.iter_mut().enumerate() {
            if let Some(item) = iter.seek(seek)? {
                Self::buf_map_insert(&mut seek_map, num, item);
            }
        }

        if let Seek::Last = seek {
            self.map_buf.clear();

            Ok(seek_map.pop_last().map(|(_, item)| item))
        } else {
            self.map_buf = seek_map;

            self.next_err()
        }
    }

    fn item_key(item: &Self::Item) -> Bytes {
        I::item_key(item)
    }
}

#[allow(clippy::mutable_key_type)]
impl<I: Iter> MergingIter<I> {
    fn buf_map_insert(seek_map: &mut BTreeMap<IterKey, <I as Iter>::Item>, num: usize, item: <I as Iter>::Item) {
        let _ = seek_map.insert(IterKey { num, key: Self::item_key(&item) }, item);
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use crate::kernel::lsm::block::{Block, DEFAULT_DATA_RESTART_INTERVAL, Value};
    use crate::kernel::lsm::iterator::block_iter::BlockIter;
    use crate::kernel::lsm::iterator::{Iter, Seek};
    use crate::kernel::lsm::iterator::merging_iter::MergingIter;
    use crate::kernel::Result;

    #[test]
    fn test_sequential_iterator() -> Result<()> {
        let data_1 = vec![
            (Bytes::from(vec![b'1']), Value::from(None)),
            (Bytes::from(vec![b'2']), Value::from(Some(Bytes::from(vec![b'0'])))),
            (Bytes::from(vec![b'4']), Value::from(None)),
        ];
        let data_2 = vec![
            (Bytes::from(vec![b'6']), Value::from(None)),
            (Bytes::from(vec![b'7']), Value::from(Some(Bytes::from(vec![b'1'])))),
            (Bytes::from(vec![b'8']), Value::from(None)),
        ];
        let test_sequence = vec![
            (Bytes::from(vec![b'1']), Value::from(None)),
            (Bytes::from(vec![b'2']), Value::from(Some(Bytes::from(vec![b'0'])))),
            (Bytes::from(vec![b'4']), Value::from(None)),
            (Bytes::from(vec![b'6']), Value::from(None)),
            (Bytes::from(vec![b'7']), Value::from(Some(Bytes::from(vec![b'1'])))),
            (Bytes::from(vec![b'8']), Value::from(None)),
            (Bytes::from(vec![b'1']), Value::from(None)),
            (Bytes::from(vec![b'8']), Value::from(None)),
            (Bytes::from(vec![b'6']), Value::from(None))
        ];

        test_with_data(data_1, data_2, test_sequence)
    }

    #[test]
    fn test_cross_iterator() -> Result<()> {
        let data_1 = vec![
            (Bytes::from(vec![b'1']), Value::from(None)),
            (Bytes::from(vec![b'2']), Value::from(Some(Bytes::from(vec![b'0'])))),
            (Bytes::from(vec![b'6']), Value::from(None)),
        ];
        let data_2 = vec![
            (Bytes::from(vec![b'4']), Value::from(None)),
            (Bytes::from(vec![b'7']), Value::from(Some(Bytes::from(vec![b'1'])))),
            (Bytes::from(vec![b'8']), Value::from(None)),
        ];
        let test_sequence = vec![
            (Bytes::from(vec![b'1']), Value::from(None)),
            (Bytes::from(vec![b'2']), Value::from(Some(Bytes::from(vec![b'0'])))),
            (Bytes::from(vec![b'4']), Value::from(None)),
            (Bytes::from(vec![b'6']), Value::from(None)),
            (Bytes::from(vec![b'7']), Value::from(Some(Bytes::from(vec![b'1'])))),
            (Bytes::from(vec![b'8']), Value::from(None)),
            (Bytes::from(vec![b'1']), Value::from(None)),
            (Bytes::from(vec![b'8']), Value::from(None)),
            (Bytes::from(vec![b'6']), Value::from(None))
        ];

        test_with_data(data_1, data_2, test_sequence)
    }

    #[test]
    fn test_same_key_iterator() -> Result<()> {
        let data_1 = vec![
            (Bytes::from(vec![b'4']), Value::from(Some(Bytes::from(vec![b'0'])))),
            (Bytes::from(vec![b'5']), Value::from(None)),
            (Bytes::from(vec![b'6']), Value::from(Some(Bytes::from(vec![b'0'])))),
        ];
        let data_2 = vec![
            (Bytes::from(vec![b'4']), Value::from(None)),
            (Bytes::from(vec![b'5']), Value::from(Some(Bytes::from(vec![b'1'])))),
            (Bytes::from(vec![b'6']), Value::from(None)),
        ];
        let test_sequence = vec![
            (Bytes::from(vec![b'4']), Value::from(Some(Bytes::from(vec![b'0'])))),
            (Bytes::from(vec![b'4']), Value::from(None)),
            (Bytes::from(vec![b'5']), Value::from(None)),
            (Bytes::from(vec![b'5']), Value::from(Some(Bytes::from(vec![b'1'])))),
            (Bytes::from(vec![b'6']), Value::from(Some(Bytes::from(vec![b'0'])))),
            (Bytes::from(vec![b'6']), Value::from(None)),
            (Bytes::from(vec![b'4']), Value::from(Some(Bytes::from(vec![b'0'])))),
            (Bytes::from(vec![b'6']), Value::from(None)),
            (Bytes::from(vec![b'5']), Value::from(None))
        ];

        test_with_data(data_1, data_2, test_sequence)
    }

    fn test_with_data(data_1: Vec<(Bytes, Value)>, data_2: Vec<(Bytes, Value)>, sequence: Vec<(Bytes, Value)>) -> Result<()> {
        let block_1 = Block::new(data_1, DEFAULT_DATA_RESTART_INTERVAL);
        let block_2 = Block::new(data_2, DEFAULT_DATA_RESTART_INTERVAL);

        tokio_test::block_on(async move {
            let iterator_1 = BlockIter::new(&block_1);
            let iterator_2 = BlockIter::new(&block_2);
            let mut sequence_iter = sequence.into_iter();

            let mut merging_iter = MergingIter::new(vec![iterator_1, iterator_2])?;

            assert!(merging_iter.is_valid());

            assert_eq!(
                merging_iter.next_err()?,
                sequence_iter.next()
            );

            assert_eq!(
                merging_iter.next_err()?,
                sequence_iter.next()
            );

            assert_eq!(
                merging_iter.next_err()?,
                sequence_iter.next()
            );

            assert_eq!(
                merging_iter.next_err()?,
                sequence_iter.next()
            );

            assert_eq!(
                merging_iter.next_err()?,
                sequence_iter.next()
            );

            assert_eq!(
                merging_iter.next_err()?,
                sequence_iter.next()
            );

            assert_eq!(
                merging_iter.seek(Seek::First)?,
                sequence_iter.next()
            );

            assert_eq!(
                merging_iter.seek(Seek::Last)?,
                sequence_iter.next()
            );

            assert_eq!(
                merging_iter.seek(Seek::Backward(&vec![b'5']))?,
                sequence_iter.next()
            );

            Ok(())
        })
    }
}