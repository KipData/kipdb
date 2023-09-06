mod iter;

use crate::kernel::lsm::iterator::Iter;
use crate::kernel::lsm::mem_table::KeyValue;
use crate::kernel::lsm::table::Table;
use bytes::Bytes;
use skiplist::SkipMap;

pub(crate) struct SkipTable {
    level: usize,
    gen: i64,
    len: usize,
    inner: SkipMap<Bytes, KeyValue>,
}

impl SkipTable {
    #[allow(dead_code)]
    pub(crate) fn new(level: usize, gen: i64, data: Vec<KeyValue>) -> Self {
        let len = data.len();
        let inner = SkipMap::from_iter(
            data.into_iter()
                .map(|(key, value)| (key.clone(), (key, value))),
        );

        SkipTable {
            level,
            gen,
            len,
            inner,
        }
    }
}

impl Table for SkipTable {
    fn query(&self, key: &[u8]) -> crate::kernel::Result<Option<KeyValue>> {
        Ok(self.inner.get(key).cloned())
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

    fn iter<'a>(&'a self) -> crate::kernel::Result<Box<dyn Iter<'a, Item = KeyValue> + 'a + Send + Sync>> {
        todo!("skiplist cannot support")
    }
}
