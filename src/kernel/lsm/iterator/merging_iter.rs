use std::cmp::Ordering;
use std::collections::BTreeMap;
use bytes::Bytes;
use crate::kernel::lsm::iterator::{Iter, Seek};
use crate::kernel::lsm::mem_table::KeyValue;
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

pub(crate) struct MergingIter<'a> {
    vec_iter: Vec<&'a mut dyn Iter<Item=KeyValue>>,
    map_buf: BTreeMap<IterKey, KeyValue>
}

impl<'a> MergingIter<'a> {
    #[allow(dead_code, clippy::mutable_key_type)]
    pub(crate) fn new(mut vec_iter: Vec<&'a mut dyn Iter<Item=KeyValue>>) -> Result<Self> {
        let mut map_buf = BTreeMap::new();

        for (num, iter) in vec_iter.iter_mut().enumerate() {
            if let Some(item) = iter.next_err()? {
                Self::buf_map_insert(&mut map_buf, num, item);
            }
        }

        Ok(MergingIter { vec_iter, map_buf })
    }
}

impl Iter for MergingIter<'_> {
    type Item = KeyValue;

    fn next_err(&mut self) -> Result<Option<Self::Item>> {
        if let Some((IterKey{ num, .. }, old_item)) = self.map_buf.pop_first() {
            if let Some(item) = self.vec_iter[num].next_err()? {
                Self::buf_map_insert(&mut self.map_buf, num, item);
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
}

#[allow(clippy::mutable_key_type)]
impl MergingIter<'_> {
    fn buf_map_insert(seek_map: &mut BTreeMap<IterKey, KeyValue>, num: usize, item: KeyValue) {
        let _ = seek_map.insert(IterKey { num, key: item.0.clone() }, item);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;
    use bytes::Bytes;
    use tempfile::TempDir;
    use crate::kernel::io::{FileExtension, IoFactory, IoType};
    use crate::kernel::lsm::iterator::{Iter, Seek};
    use crate::kernel::lsm::iterator::merging_iter::MergingIter;
    use crate::kernel::lsm::iterator::ss_table_iter::SSTableIter;
    use crate::kernel::lsm::lsm_kv::Config;
    use crate::kernel::lsm::mem_table::{InternalKey, KeyValue, MemMap, MemMapIter};
    use crate::kernel::lsm::ss_table::SSTable;
    use crate::kernel::lsm::version::DEFAULT_SS_TABLE_PATH;
    use crate::kernel::Result;
    use crate::kernel::utils::lru_cache::ShardingLruCache;

    #[test]
    fn test_sequential_iterator() -> Result<()> {
        let data_1 = vec![
            (Bytes::from(vec![b'1']), None),
            (Bytes::from(vec![b'2']), Some(Bytes::from(vec![b'0']))),
            (Bytes::from(vec![b'4']), None),
        ];
        let data_2 = vec![
            (Bytes::from(vec![b'6']), None),
            (Bytes::from(vec![b'7']), Some(Bytes::from(vec![b'1']))),
            (Bytes::from(vec![b'8']), None),
        ];
        let test_sequence = vec![
            (Bytes::from(vec![b'1']), None),
            (Bytes::from(vec![b'2']), Some(Bytes::from(vec![b'0']))),
            (Bytes::from(vec![b'4']), None),
            (Bytes::from(vec![b'6']), None),
            (Bytes::from(vec![b'7']), Some(Bytes::from(vec![b'1']))),
            (Bytes::from(vec![b'8']), None),
            (Bytes::from(vec![b'1']), None),
            (Bytes::from(vec![b'8']), None),
            (Bytes::from(vec![b'6']), None)
        ];

        test_with_data(data_1, data_2, test_sequence)
    }

    #[test]
    fn test_cross_iterator() -> Result<()> {
        let data_1 = vec![
            (Bytes::from(vec![b'1']), None),
            (Bytes::from(vec![b'2']), Some(Bytes::from(vec![b'0']))),
            (Bytes::from(vec![b'6']), None),
        ];
        let data_2 = vec![
            (Bytes::from(vec![b'4']), None),
            (Bytes::from(vec![b'7']), Some(Bytes::from(vec![b'1']))),
            (Bytes::from(vec![b'8']), None),
        ];
        let test_sequence = vec![
            (Bytes::from(vec![b'1']), None),
            (Bytes::from(vec![b'2']), Some(Bytes::from(vec![b'0']))),
            (Bytes::from(vec![b'4']), None),
            (Bytes::from(vec![b'6']), None),
            (Bytes::from(vec![b'7']), Some(Bytes::from(vec![b'1']))),
            (Bytes::from(vec![b'8']), None),
            (Bytes::from(vec![b'1']), None),
            (Bytes::from(vec![b'8']), None),
            (Bytes::from(vec![b'6']), None)
        ];

        test_with_data(data_1, data_2, test_sequence)
    }

    #[test]
    fn test_same_key_iterator() -> Result<()> {
        let data_1 = vec![
            (Bytes::from(vec![b'4']), Some(Bytes::from(vec![b'0']))),
            (Bytes::from(vec![b'5']), None),
            (Bytes::from(vec![b'6']), Some(Bytes::from(vec![b'0']))),
        ];
        let data_2 = vec![
            (Bytes::from(vec![b'4']), None),
            (Bytes::from(vec![b'5']), Some(Bytes::from(vec![b'1']))),
            (Bytes::from(vec![b'6']), None),
        ];
        let test_sequence = vec![
            (Bytes::from(vec![b'4']), Some(Bytes::from(vec![b'0']))),
            (Bytes::from(vec![b'4']), None),
            (Bytes::from(vec![b'5']), None),
            (Bytes::from(vec![b'5']), Some(Bytes::from(vec![b'1']))),
            (Bytes::from(vec![b'6']), Some(Bytes::from(vec![b'0']))),
            (Bytes::from(vec![b'6']), None),
            (Bytes::from(vec![b'4']), Some(Bytes::from(vec![b'0']))),
            (Bytes::from(vec![b'6']), None),
            (Bytes::from(vec![b'5']), None)
        ];

        test_with_data(data_1, data_2, test_sequence)
    }

    fn test_with_data(data_1: Vec<KeyValue>, data_2: Vec<KeyValue>, sequence: Vec<KeyValue>) -> Result<()> {
        let map = MemMap::from_iter(
            data_1.into_iter()
                .map(|(key, value)| (InternalKey::new(key), value))
        );

        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let config = Config::new(temp_dir.into_path());

        let sst_factory = IoFactory::new(
            config.dir_path.join(DEFAULT_SS_TABLE_PATH),
            FileExtension::SSTable
        )?;

        let (ss_table, _) = SSTable::create_for_mem_table(
            &config,
            1,
            &sst_factory,
            data_2,
            0,
            IoType::Direct
        )?;
        let cache = ShardingLruCache::new(
            config.block_cache_size,
            16,
            RandomState::default()
        )?;

        let mut map_iter = MemMapIter::new(&map);

        let mut sst_iter = SSTableIter::new(&ss_table, &cache)?;

        let mut sequence_iter = sequence.into_iter();

        let mut merging_iter = MergingIter::new(vec![&mut map_iter, &mut sst_iter])?;

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
    }
}