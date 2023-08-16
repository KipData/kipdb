use crate::kernel::lsm::iterator::{Iter, Seek};
use crate::kernel::lsm::mem_table::KeyValue;
use crate::kernel::Result;
use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Bound::{Included, Unbounded};

/// 用于取值以及对应的Iter下标
/// 通过序号进行同值优先获取
#[derive(Eq, PartialEq, Debug)]
struct IterKey {
    num: usize,
    key: Bytes,
}

impl PartialOrd<Self> for IterKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IterKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.num.cmp(&other.num))
    }
}

pub(crate) struct MergingIter<'a> {
    vec_iter: Vec<Box<dyn Iter<'a, Item = KeyValue> + 'a>>,
    map_buf: BTreeMap<IterKey, KeyValue>,
    pre_key: Option<Bytes>,
}

impl<'a> MergingIter<'a> {
    #[allow(dead_code, clippy::mutable_key_type)]
    pub(crate) fn new(mut vec_iter: Vec<Box<dyn Iter<'a, Item = KeyValue> + 'a>>) -> Result<Self> {
        let mut map_buf = BTreeMap::new();

        for (num, iter) in vec_iter.iter_mut().enumerate() {
            if let Some(item) = iter.try_next()? {
                Self::buf_map_insert(&mut map_buf, num, item);
            }
        }

        Ok(MergingIter {
            vec_iter,
            map_buf,
            pre_key: None,
        })
    }
}

impl<'a> Iter<'a> for MergingIter<'a> {
    type Item = KeyValue;

    fn try_next(&mut self) -> Result<Option<Self::Item>> {
        while let Some((IterKey { num, .. }, old_item)) = self.map_buf.pop_first() {
            if let Some(item) = self.vec_iter[num].try_next()? {
                Self::buf_map_insert(&mut self.map_buf, num, item);
            }

            // 跳过重复元素
            if let Some(key) = &self.pre_key {
                if key == &old_item.0 {
                    continue;
                }
            }
            self.pre_key = Some(old_item.0.clone());

            return Ok(Some(old_item));
        }

        Ok(None)
    }

    fn is_valid(&self) -> bool {
        self.vec_iter
            .iter()
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

            // 有点复杂不是么
            // 先弹出最小的元素
            // 当多个Iter seek至最尾端后存在最小元素且最小元素的key有重复的情况下，去num（iter序号）最小的元素
            // 当num为0时，则直接选择该最优先的iter的元素
            Ok(seek_map.pop_last().map(|(IterKey { key, num }, item)| {
                (num != 0)
                    .then(|| {
                        seek_map
                            .range((Included(&IterKey { num: 0, key }), Unbounded))
                            .next()
                            .map(|(_, range_item)| range_item.clone())
                    })
                    .flatten()
                    .unwrap_or(item)
            }))
        } else {
            self.map_buf = seek_map;

            self.try_next()
        }
    }
}

#[allow(clippy::mutable_key_type)]
impl MergingIter<'_> {
    fn buf_map_insert(seek_map: &mut BTreeMap<IterKey, KeyValue>, num: usize, item: KeyValue) {
        let _ = seek_map.insert(
            IterKey {
                num,
                key: item.0.clone(),
            },
            item,
        );
    }
}

#[cfg(test)]
mod tests {
    use crate::kernel::io::{FileExtension, IoFactory, IoType};
    use crate::kernel::lsm::iterator::merging_iter::MergingIter;
    use crate::kernel::lsm::iterator::{Iter, Seek};
    use crate::kernel::lsm::mem_table::{InternalKey, KeyValue, MemMap, MemMapIter};
    use crate::kernel::lsm::storage::Config;
    use crate::kernel::lsm::table::ss_table::iter::SSTableIter;
    use crate::kernel::lsm::table::ss_table::SSTable;
    use crate::kernel::lsm::version::DEFAULT_SS_TABLE_PATH;
    use crate::kernel::utils::lru_cache::ShardingLruCache;
    use crate::kernel::Result;
    use bytes::Bytes;
    use std::collections::hash_map::RandomState;
    use std::sync::Arc;
    use tempfile::TempDir;

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
            Some((Bytes::from(vec![b'1']), None)),
            Some((Bytes::from(vec![b'2']), Some(Bytes::from(vec![b'0'])))),
            Some((Bytes::from(vec![b'4']), None)),
            Some((Bytes::from(vec![b'6']), None)),
            Some((Bytes::from(vec![b'7']), Some(Bytes::from(vec![b'1'])))),
            Some((Bytes::from(vec![b'8']), None)),
            Some((Bytes::from(vec![b'1']), None)),
            Some((Bytes::from(vec![b'8']), None)),
            Some((Bytes::from(vec![b'6']), None)),
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
            Some((Bytes::from(vec![b'1']), None)),
            Some((Bytes::from(vec![b'2']), Some(Bytes::from(vec![b'0'])))),
            Some((Bytes::from(vec![b'4']), None)),
            Some((Bytes::from(vec![b'6']), None)),
            Some((Bytes::from(vec![b'7']), Some(Bytes::from(vec![b'1'])))),
            Some((Bytes::from(vec![b'8']), None)),
            Some((Bytes::from(vec![b'1']), None)),
            Some((Bytes::from(vec![b'8']), None)),
            Some((Bytes::from(vec![b'6']), None)),
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
            Some((Bytes::from(vec![b'4']), Some(Bytes::from(vec![b'0'])))),
            Some((Bytes::from(vec![b'5']), None)),
            Some((Bytes::from(vec![b'6']), Some(Bytes::from(vec![b'0'])))),
            None,
            None,
            None,
            Some((Bytes::from(vec![b'4']), Some(Bytes::from(vec![b'0'])))),
            Some((Bytes::from(vec![b'6']), Some(Bytes::from(vec![b'0'])))),
            Some((Bytes::from(vec![b'5']), None)),
        ];

        test_with_data(data_1, data_2, test_sequence)
    }

    fn test_with_data(
        data_1: Vec<KeyValue>,
        data_2: Vec<KeyValue>,
        sequence: Vec<Option<KeyValue>>,
    ) -> Result<()> {
        let map = MemMap::from_iter(
            data_1
                .into_iter()
                .map(|(key, value)| (InternalKey::new(key), value)),
        );

        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let config = Config::new(temp_dir.into_path());
        let sst_factory = IoFactory::new(
            config.dir_path.join(DEFAULT_SS_TABLE_PATH),
            FileExtension::SSTable,
        )?;
        let cache = Arc::new(ShardingLruCache::new(
            config.table_cache_size,
            16,
            RandomState::default(),
        )?);

        let ss_table = SSTable::new(
            &sst_factory,
            &config,
            Arc::clone(&cache),
            1,
            data_2,
            0,
            IoType::Direct,
        )?;

        let map_iter = MemMapIter::new(&map);

        let sst_iter = SSTableIter::new(&ss_table)?;

        let mut sequence_iter = sequence.into_iter();

        let mut merging_iter = MergingIter::new(vec![Box::new(map_iter), Box::new(sst_iter)])?;

        assert_eq!(merging_iter.try_next()?, sequence_iter.next().flatten());

        assert_eq!(merging_iter.try_next()?, sequence_iter.next().flatten());

        assert_eq!(merging_iter.try_next()?, sequence_iter.next().flatten());

        assert_eq!(merging_iter.try_next()?, sequence_iter.next().flatten());

        assert_eq!(merging_iter.try_next()?, sequence_iter.next().flatten());

        assert_eq!(merging_iter.try_next()?, sequence_iter.next().flatten());

        assert_eq!(
            merging_iter.seek(Seek::First)?,
            sequence_iter.next().flatten()
        );

        assert_eq!(
            merging_iter.seek(Seek::Last)?,
            sequence_iter.next().flatten()
        );

        assert_eq!(
            merging_iter.seek(Seek::Backward(&vec![b'5']))?,
            sequence_iter.next().flatten()
        );

        Ok(())
    }
}
