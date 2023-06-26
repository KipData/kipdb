use bytes::Bytes;
use skiplist::SkipMap;
use crate::kernel::lsm::mem_table::KeyValue;
use crate::kernel::lsm::table::Table;

pub(crate) struct SkipTable {
    level: usize,
    gen: i64,
    len: usize,
    inner: SkipMap<Bytes, Option<Bytes>>
}

impl SkipTable {
    pub(crate) fn new(level: usize, gen: i64, len: usize, data: Vec<KeyValue>) -> Self {
        SkipTable {
            level,
            gen,
            len,
            inner: SkipMap::from_iter(data),
        }
    }
}

impl Table for SkipTable {
    fn query(&self, key: &[u8]) -> crate::kernel::Result<Option<Bytes>> {
        Ok(self.inner
            .get(key)
            .cloned()
            .flatten())
    }

    fn len(&self) -> usize {
        self.len
    }

    fn size_of_disk(&self) -> u64 {
        0
    }

    fn gen(&self) -> i64 {
        self.gen
    }

    fn level(&self) -> usize {
        self.level
    }
}

