use crate::kernel::io::IoWriter;
use crate::kernel::lsm::iterator::{Iter, Seek};
use crate::kernel::lsm::log::{LogLoader, LogWriter};
use crate::kernel::lsm::storage::{Config, Gen, Sequence};
use crate::kernel::lsm::table::ss_table::block::{Entry, Value};
use crate::kernel::lsm::trigger::{Trigger, TriggerFactory};
use crate::kernel::KernelResult;
use bytes::Bytes;
use itertools::Itertools;
use parking_lot::Mutex;
use skiplist::{skipmap, SkipMap};
use std::cmp::Ordering;
use std::collections::Bound;
use std::io::Cursor;
use std::mem;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Acquire;
use std::sync::Arc;
use std::vec::IntoIter;

pub(crate) const DEFAULT_WAL_PATH: &str = "wal";

/// Value为此Key的Records(Key与seq_id)
pub(crate) type MemMap = SkipMap<InternalKey, Option<Bytes>>;

pub(crate) type KeyValue = (Bytes, Option<Bytes>);

/// seq_id的上限值
///
/// 用于默认的key的填充(补充使UserKey为高位，因此默认获取最新的seq_id数据)
const SEQ_MAX: i64 = i64::MAX;

pub(crate) fn key_value_bytes_len(key_value: &KeyValue) -> usize {
    key_value.0.len() + key_value.1.as_ref().map(Bytes::len).unwrap_or(0)
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub(crate) struct InternalKey {
    key: Bytes,
    seq_id: i64,
}

impl PartialOrd<Self> for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.seq_id.cmp(&other.seq_id))
    }
}

impl InternalKey {
    pub(crate) fn new(key: Bytes) -> Self {
        InternalKey {
            key,
            seq_id: Sequence::create(),
        }
    }

    pub(crate) fn new_with_seq(key: Bytes, seq_id: i64) -> Self {
        InternalKey { key, seq_id }
    }

    pub(crate) fn get_key(&self) -> &Bytes {
        &self.key
    }
}

pub(crate) struct MemMapIter<'a> {
    mem_map: &'a MemMap,

    prev_item: Option<(Bytes, Option<Bytes>)>,
    iter: Option<skipmap::Iter<'a, InternalKey, Option<Bytes>>>,
}

impl<'a> MemMapIter<'a> {
    #[allow(dead_code)]
    pub(crate) fn new(mem_map: &'a MemMap) -> Self {
        Self {
            mem_map,
            prev_item: None,
            iter: Some(mem_map.iter()),
        }
    }
}

impl<'a> Iter<'a> for MemMapIter<'a> {
    type Item = KeyValue;

    fn try_next(&mut self) -> KernelResult<Option<Self::Item>> {
        if let Some(iter) = &mut self.iter {
            for (InternalKey { key, .. }, value) in iter.by_ref() {
                if let Some(prev_item) = &self.prev_item {
                    if key != &prev_item.0 {
                        return Ok(mem::replace(
                            &mut self.prev_item,
                            Some((key.clone(), value.clone())),
                        ));
                    }
                }
                self.prev_item = Some((key.clone(), value.clone()));
            }

            return Ok(self.prev_item.take());
        }

        Ok(None)
    }

    fn is_valid(&self) -> bool {
        true
    }

    fn seek(&mut self, seek: Seek<'_>) -> KernelResult<Option<Self::Item>> {
        self.prev_item = None;
        self.iter = match seek {
            Seek::First => Some(self.mem_map.iter()),
            Seek::Last => None,
            Seek::Backward(seek_key) => Some(self.mem_map.range(
                Bound::Included(&InternalKey::new_with_seq(
                    Bytes::copy_from_slice(seek_key),
                    0,
                )),
                Bound::Unbounded,
            )),
        };

        if let Seek::Last = seek {
            Ok(self
                .mem_map
                .iter()
                .last()
                .map(|(InternalKey { key, .. }, value)| (key.clone(), value.clone())))
        } else {
            self.try_next()
        }
    }
}

pub(crate) struct MemTable {
    inner: Mutex<TableInner>,
    pub(crate) tx_count: AtomicUsize,
}

pub(crate) struct TableInner {
    pub(crate) _mem: MemMap,
    pub(crate) _immut: Option<Arc<MemMap>>,
    /// WAL载入器
    ///
    /// 用于异常停机时MemTable的恢复
    /// 同时当Level 0的SSTable异常时，可以尝试恢复
    log_loader: LogLoader,
    log_writer: (LogWriter<Box<dyn IoWriter>>, i64),
    trigger: Box<dyn Trigger + Send>,
}

macro_rules! check_count {
    ($count:ident) => {
        if 0 != $count.load(Acquire) {
            std::hint::spin_loop();
            continue;
        }
    };
}

impl MemTable {
    pub(crate) fn new(config: &Config) -> KernelResult<Self> {
        let (log_loader, log_bytes, log_gen) = LogLoader::reload(
            config.path(),
            (DEFAULT_WAL_PATH, None),
            config.wal_io_type,
            |bytes| Ok(mem::take(bytes)),
        )?;
        let log_writer = (log_loader.writer(log_gen)?, log_gen);
        // Q: 为什么INIT_SEQ作为Seq id?
        // A: 因为此处是当存在有停机异常时使用wal恢复数据,此处也不存在有Version(VersionStatus的初始化在此代码之后)
        // 因此不会影响Version的读取顺序
        let mem_map = MemMap::from_iter(
            logs_decode(log_bytes)?.map(|(key, value)| (InternalKey::new_with_seq(key, 0), value)),
        );
        let (trigger_type, threshold) = config.minor_trigger_with_threshold;

        Ok(MemTable {
            inner: Mutex::new(TableInner {
                _mem: mem_map,
                _immut: None,
                log_loader,
                log_writer,
                trigger: TriggerFactory::create(trigger_type, threshold),
            }),
            tx_count: AtomicUsize::new(0),
        })
    }

    pub(crate) fn check_key_conflict(&self, kvs: &[KeyValue], seq_id: i64) -> bool {
        let inner = self.inner.lock();

        for (key, _) in kvs {
            let internal_key = InternalKey::new_with_seq(key.clone(), seq_id);

            if let Some(true) = inner
                ._mem
                .lower_bound(Bound::Excluded(&internal_key))
                .map(|(lower_key, _)| lower_key.key == key)
            {
                return true;
            }
        }

        false
    }

    /// 插入并判断是否溢出
    ///
    /// 插入时不会去除重复键值，而是进行追加
    pub(crate) fn insert_data(&self, data: KeyValue) -> KernelResult<bool> {
        let (key, value) = data.clone();
        let mut inner = self.inner.lock();

        inner.trigger.item_process(&data);

        let _ = inner.log_writer.0.add_record(&data_to_bytes(data)?)?;
        let _ = inner._mem.insert(InternalKey::new(key), value);

        Ok(inner.trigger.is_exceeded())
    }

    /// Tips: 当数据在插入mem_table中停机，则不会存入日志中
    pub(crate) fn insert_batch_data(
        &self,
        vec_data: Vec<KeyValue>,
        seq_id: i64,
    ) -> KernelResult<bool> {
        let mut inner = self.inner.lock();

        let mut buf = Vec::new();
        for item in vec_data {
            let (key, value) = item.clone();
            inner.trigger.item_process(&item);

            let _ = inner
                ._mem
                .insert(InternalKey::new_with_seq(key, seq_id), value);
            buf.append(&mut data_to_bytes(item)?);
        }
        let _ = inner.log_writer.0.add_record(&buf)?;

        Ok(inner.trigger.is_exceeded())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.inner.lock()._mem.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.lock()._mem.len()
    }

    pub(crate) fn log_loader_clone(&self) -> LogLoader {
        self.inner.lock().log_loader.clone()
    }

    /// MemTable将数据弹出并转移到immut table中  (弹出数据为转移至immut table中数据的迭代器)
    pub(crate) fn swap(&self) -> KernelResult<Option<(i64, Vec<KeyValue>)>> {
        let count = &self.tx_count;

        loop {
            check_count!(count);

            let mut inner = self.inner.lock();
            // 二重检测防止lock时(前)突然出现事务
            // 当lock后，即使出现事务，会因为lock已被Compactor获取而无法读写，
            // 因此不会对读写进行干扰
            // 并且事务即使在lock后出现，所持有的seq为该压缩之前，
            // 也不会丢失该seq的_mem，因为转移到了_immut，可以从_immut得到对应seq的数据
            check_count!(count);

            return if !inner._mem.is_empty() {
                inner.trigger.reset();

                let mut vec_data = inner
                    ._mem
                    .iter()
                    .map(|(k, v)| (k.key.clone(), v.clone()))
                    // rev以使用最后(最新)的key
                    .rev()
                    .unique_by(|(k, _)| k.clone())
                    .collect_vec();

                vec_data.reverse();

                inner._immut = Some(Arc::new(mem::replace(&mut inner._mem, SkipMap::new())));

                let new_gen = Gen::create();
                let new_writer = (inner.log_loader.writer(new_gen)?, new_gen);
                let (mut old_writer, old_gen) = mem::replace(&mut inner.log_writer, new_writer);
                old_writer.flush()?;

                Ok(Some((old_gen, vec_data)))
            } else {
                Ok(None)
            };
        }
    }

    pub(crate) fn find(&self, key: &[u8]) -> Option<KeyValue> {
        // 填充SEQ_MAX使其变为最高位以尽可能获取最新数据
        let internal_key = InternalKey::new_with_seq(Bytes::copy_from_slice(key), SEQ_MAX);
        let inner = self.inner.lock();

        Self::find_(&internal_key, &inner._mem).or_else(|| {
            inner
                ._immut
                .as_ref()
                .and_then(|mem_map| Self::find_(&internal_key, mem_map))
        })
    }

    /// 查询时附带seq_id进行历史数据查询
    pub(crate) fn find_with_sequence_id(&self, key: &[u8], seq_id: i64) -> Option<KeyValue> {
        let internal_key = InternalKey::new_with_seq(Bytes::copy_from_slice(key), seq_id);
        let inner = self.inner.lock();

        if let Some(key_value) = MemTable::find_(&internal_key, &inner._mem) {
            Some(key_value)
        } else if let Some(mem_map) = &inner._immut {
            MemTable::find_(&internal_key, mem_map)
        } else {
            None
        }
    }

    fn find_(internal_key: &InternalKey, mem_map: &MemMap) -> Option<KeyValue> {
        mem_map
            .upper_bound(Bound::Included(internal_key))
            .and_then(|(upper_key, value)| {
                let key = internal_key.get_key();
                (key == &upper_key.key).then(|| (key.clone(), value.clone()))
            })
    }

    /// 范围读取
    ///
    /// MemTable中涉及锁操作，因此若是使用iter进行range操作容易长时间占用锁，因此直接返回范围值并命名为range_scan会比较合适
    #[allow(dead_code)]
    pub(crate) fn range_scan(
        &self,
        min: Bound<&[u8]>,
        max: Bound<&[u8]>,
        option_seq: Option<i64>,
    ) -> Vec<KeyValue> {
        let inner = self.inner.lock();
        let de_dupe_merge_sort_fn = |mem: Vec<KeyValue>, immut_mem: Vec<KeyValue>| {
            assert!(mem.is_sorted_by_key(|(k, _)| k));
            assert!(mem.iter().all_unique());
            assert!(immut_mem.is_sorted_by_key(|(k, _)| k));
            assert!(immut_mem.iter().all_unique());

            let mut merged = Vec::with_capacity(mem.len() + immut_mem.len());
            let (mut mem_iter, mut immut_mem_iter) = (mem.into_iter(), immut_mem.into_iter());
            let (mut mem_current, mut immut_mem_current) = (mem_iter.next(), immut_mem_iter.next());

            while let (Some(mem_item), Some(immut_mem_item)) =
                (mem_current.take(), immut_mem_current.take())
            {
                match mem_item.0.cmp(&immut_mem_item.0) {
                    Ordering::Less => {
                        merged.push(mem_item);
                        immut_mem_current = Some(immut_mem_item);

                        mem_current = mem_iter.next();
                        if mem_current.is_none() {
                            break;
                        }
                    }
                    Ordering::Greater => {
                        merged.push(immut_mem_item);
                        mem_current = Some(mem_item);

                        immut_mem_current = immut_mem_iter.next();
                        if immut_mem_current.is_none() {
                            break;
                        }
                    }
                    Ordering::Equal => merged.push(mem_item),
                }
            }

            if let Some(kv) = mem_current {
                merged.push(kv)
            }
            if let Some(kv) = immut_mem_current {
                merged.push(kv)
            }
            // one of the two is empty
            mem_iter
                .chain(immut_mem_iter)
                .for_each(|kv| merged.push(kv));

            assert!(merged.is_sorted_by_key(|(k, _)| k));
            assert!(merged.iter().all_unique());
            merged
        };
        let mut mem_scan = Self::_range_scan(&inner._mem, min, max, option_seq);

        if let Some(immut) = &inner._immut {
            let immut_scan = Self::_range_scan(immut, min, max, option_seq);

            mem_scan = de_dupe_merge_sort_fn(mem_scan, immut_scan);
        }
        mem_scan
    }

    fn _range_scan(
        mem_map: &MemMap,
        min: Bound<&[u8]>,
        max: Bound<&[u8]>,
        option_seq: Option<i64>,
    ) -> Vec<KeyValue> {
        fn to_internal_key(
            bound: &Bound<&[u8]>,
            included: i64,
            excluded: i64,
        ) -> Bound<InternalKey> {
            bound.map(|key| {
                InternalKey::new_with_seq(
                    Bytes::copy_from_slice(key),
                    if let Bound::Included(_) = &bound {
                        included
                    } else {
                        excluded
                    },
                )
            })
        }

        let min_key = to_internal_key(&min, i64::MIN, i64::MAX);
        let max_key = to_internal_key(&max, i64::MAX, i64::MIN);

        let mut scan = mem_map
            .range(min_key.as_ref(), max_key.as_ref())
            .rev()
            .filter(|(InternalKey { seq_id, .. }, _)| {
                option_seq.map_or(true, |current_seq| &current_seq >= seq_id)
            })
            .unique_by(|(internal_key, _)| &internal_key.key)
            .map(|(key, value)| (key.key.clone(), value.clone()))
            .collect_vec();
        scan.reverse();

        assert!(scan.is_sorted_by_key(|(k, _)| k));
        assert!(scan.iter().all_unique());
        scan
    }
}

pub(crate) fn logs_decode(
    log_bytes: Vec<Vec<u8>>,
) -> KernelResult<IntoIter<(Bytes, Option<Bytes>)>> {
    let flatten_bytes = log_bytes.into_iter().flatten().collect_vec();
    Entry::<Value>::batch_decode(&mut Cursor::new(flatten_bytes)).map(|vec| {
        vec.into_iter()
            .map(|(_, Entry { key, item, .. })| (key, item.bytes))
            .rev()
            .unique_by(|(key, _)| key.clone())
            .sorted_by_key(|(key, _)| key.clone())
    })
}

pub(crate) fn data_to_bytes(data: KeyValue) -> KernelResult<Vec<u8>> {
    let (key, value) = data;
    Entry::new(0, key.len(), key, Value::from(value)).encode()
}

#[cfg(test)]
mod tests {
    use crate::kernel::lsm::iterator::{Iter, Seek};
    use crate::kernel::lsm::mem_table::{
        data_to_bytes, InternalKey, KeyValue, MemMap, MemMapIter, MemTable,
    };
    use crate::kernel::lsm::storage::{Config, Sequence};
    use crate::kernel::KernelResult;
    use bytes::Bytes;
    use std::collections::Bound;
    use tempfile::TempDir;

    impl MemTable {
        pub(crate) fn insert_data_with_seq(&self, data: KeyValue, seq: i64) -> KernelResult<usize> {
            let (key, value) = data.clone();
            let mut inner = self.inner.lock();

            let _ = inner.log_writer.0.add_record(&data_to_bytes(data)?)?;
            let _ = inner
                ._mem
                .insert(InternalKey::new_with_seq(key, seq), value);

            Ok(inner._mem.len())
        }
    }

    #[test]
    fn test_mem_table_find() -> KernelResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let mem_table = MemTable::new(&Config::new(temp_dir.path()))?;

        let data_1 = (Bytes::from(vec![b'k']), Some(Bytes::from(vec![b'1'])));
        let data_2 = (Bytes::from(vec![b'k']), Some(Bytes::from(vec![b'2'])));

        let _ = mem_table.insert_data(data_1)?;

        let old_seq_id = Sequence::create();

        assert_eq!(
            mem_table.find(&[b'k']),
            Some((Bytes::from(vec![b'k']), Some(Bytes::from(vec![b'1']))))
        );

        let _ = mem_table.insert_data(data_2)?;

        assert_eq!(
            mem_table.find(&[b'k']),
            Some((Bytes::from(vec![b'k']), Some(Bytes::from(vec![b'2']))))
        );

        assert_eq!(
            mem_table.find_with_sequence_id(&[b'k'], old_seq_id),
            Some((Bytes::from(vec![b'k']), Some(Bytes::from(vec![b'1']))))
        );

        let new_seq_id = Sequence::create();

        assert_eq!(
            mem_table.find_with_sequence_id(&[b'k'], new_seq_id),
            Some((Bytes::from(vec![b'k']), Some(Bytes::from(vec![b'2']))))
        );

        Ok(())
    }

    #[test]
    fn test_mem_table_swap() -> KernelResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let mem_table = MemTable::new(&Config::new(temp_dir.path()))?;

        let _ = mem_table
            .insert_data((Bytes::from(vec![b'k', b'1']), Some(Bytes::from(vec![b'1']))))?;
        let _ = mem_table
            .insert_data((Bytes::from(vec![b'k', b'1']), Some(Bytes::from(vec![b'2']))))?;
        let _ = mem_table
            .insert_data((Bytes::from(vec![b'k', b'2']), Some(Bytes::from(vec![b'1']))))?;
        let _ = mem_table
            .insert_data((Bytes::from(vec![b'k', b'2']), Some(Bytes::from(vec![b'2']))))?;

        let (_, mut vec) = mem_table.swap()?.unwrap();

        assert_eq!(
            vec.pop(),
            Some((Bytes::from(vec![b'k', b'2']), Some(Bytes::from(vec![b'2']))))
        );
        assert_eq!(
            vec.pop(),
            Some((Bytes::from(vec![b'k', b'1']), Some(Bytes::from(vec![b'2']))))
        );

        Ok(())
    }

    #[test]
    fn test_mem_table_check_key_conflict() -> KernelResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let mem_table = MemTable::new(&Config::new(temp_dir.path()))?;

        let key1 = vec![b'k', b'1'];
        let bytes_key1 = Bytes::copy_from_slice(&key1);
        let kv_1 = (bytes_key1.clone(), Some(bytes_key1.clone()));

        let key2 = vec![b'k', b'2'];
        let bytes_key2 = Bytes::copy_from_slice(&key2);
        let kv_2 = (bytes_key2.clone(), Some(bytes_key2.clone()));

        let _ = mem_table.insert_data_with_seq(kv_1.clone(), 0)?;
        let _ = mem_table.insert_data_with_seq(kv_1.clone(), 1)?;
        let _ = mem_table.insert_data_with_seq(kv_1.clone(), 2)?;
        let _ = mem_table.insert_data_with_seq(kv_2.clone(), 3)?;

        assert!(mem_table.check_key_conflict(&[kv_1.clone()], 1));

        assert!(!mem_table.check_key_conflict(&[kv_1.clone()], 2));

        Ok(())
    }

    #[test]
    fn test_mem_table_range_scan() -> KernelResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let mem_table = MemTable::new(&Config::new(temp_dir.path()))?;

        let key1 = vec![b'k', b'1'];
        let bytes_key1 = Bytes::copy_from_slice(&key1);

        let key2 = vec![b'k', b'2'];
        let bytes_key2 = Bytes::copy_from_slice(&key2);

        let key3 = vec![b'k', b'3'];
        let bytes_key3 = Bytes::copy_from_slice(&key3);

        assert_eq!(
            mem_table
                .insert_data_with_seq((bytes_key1.clone(), Some(Bytes::from(vec![b'1']))), 1)?,
            1
        );
        assert_eq!(
            mem_table
                .insert_data_with_seq((bytes_key1.clone(), Some(Bytes::from(vec![b'2']))), 2)?,
            2
        );
        assert_eq!(
            mem_table
                .insert_data_with_seq((bytes_key2.clone(), Some(Bytes::from(vec![b'1']))), 3)?,
            3
        );
        assert_eq!(
            mem_table
                .insert_data_with_seq((bytes_key2.clone(), Some(Bytes::from(vec![b'2']))), 4)?,
            4
        );
        assert_eq!(
            mem_table
                .insert_data_with_seq((bytes_key3.clone(), Some(Bytes::from(vec![b'1']))), 5)?,
            5
        );
        assert_eq!(
            mem_table
                .insert_data_with_seq((bytes_key3.clone(), Some(Bytes::from(vec![b'2']))), 6)?,
            6
        );

        let mut vec1 = mem_table.range_scan(Bound::Included(&key1), Bound::Included(&key2), None);
        assert_eq!(vec1.len(), 2);
        assert_eq!(
            vec1.pop(),
            Some((Bytes::from(vec![b'k', b'2']), Some(Bytes::from(vec![b'2']))))
        );
        assert_eq!(
            vec1.pop(),
            Some((Bytes::from(vec![b'k', b'1']), Some(Bytes::from(vec![b'2']))))
        );

        let mut vec2 = mem_table.range_scan(Bound::Excluded(&key1), Bound::Excluded(&key3), None);
        assert_eq!(vec2.len(), 1);
        assert_eq!(
            vec2.pop(),
            Some((Bytes::from(vec![b'k', b'2']), Some(Bytes::from(vec![b'2']))))
        );

        let mut vec3 = mem_table.range_scan(Bound::Unbounded, Bound::Unbounded, None);
        assert_eq!(vec3.len(), 3);
        assert_eq!(
            vec3.pop(),
            Some((Bytes::from(vec![b'k', b'3']), Some(Bytes::from(vec![b'2']))))
        );
        assert_eq!(
            vec3.pop(),
            Some((Bytes::from(vec![b'k', b'2']), Some(Bytes::from(vec![b'2']))))
        );
        assert_eq!(
            vec3.pop(),
            Some((Bytes::from(vec![b'k', b'1']), Some(Bytes::from(vec![b'2']))))
        );

        let mut vec4 = mem_table.range_scan(Bound::Unbounded, Bound::Unbounded, Some(3));
        assert_eq!(vec4.len(), 2);
        assert_eq!(
            vec4.pop(),
            Some((Bytes::from(vec![b'k', b'2']), Some(Bytes::from(vec![b'1']))))
        );
        assert_eq!(
            vec4.pop(),
            Some((Bytes::from(vec![b'k', b'1']), Some(Bytes::from(vec![b'2']))))
        );

        Ok(())
    }

    #[test]
    fn test_mem_map_iter() -> KernelResult<()> {
        let mut map = MemMap::new();

        let key_1_1 = InternalKey::new(Bytes::from(vec![b'1']));
        let key_1_2 = InternalKey::new(Bytes::from(vec![b'1']));
        let key_2_1 = InternalKey::new(Bytes::from(vec![b'2']));
        let key_2_2 = InternalKey::new(Bytes::from(vec![b'2']));
        let key_4_1 = InternalKey::new(Bytes::from(vec![b'4']));
        let key_4_2 = InternalKey::new(Bytes::from(vec![b'4']));

        let _ = map.insert(key_1_1.clone(), Some(Bytes::new()));
        let _ = map.insert(key_1_2.clone(), None);
        let _ = map.insert(key_2_1.clone(), Some(Bytes::new()));
        let _ = map.insert(key_2_2.clone(), None);
        let _ = map.insert(key_4_1.clone(), Some(Bytes::new()));
        let _ = map.insert(key_4_2.clone(), None);

        let mut iter = MemMapIter::new(&map);

        assert_eq!(iter.try_next()?, Some((key_1_2.key.clone(), None)));

        assert_eq!(iter.try_next()?, Some((key_2_2.key.clone(), None)));

        assert_eq!(iter.try_next()?, Some((key_4_2.key.clone(), None)));

        assert_eq!(iter.seek(Seek::First)?, Some((key_1_2.key.clone(), None)));

        assert_eq!(iter.seek(Seek::Last)?, Some((key_4_2.key.clone(), None)));

        assert_eq!(iter.try_next()?, None);

        assert_eq!(
            iter.seek(Seek::Backward(&[b'3']))?,
            Some((key_4_2.key.clone(), None))
        );

        Ok(())
    }
}
