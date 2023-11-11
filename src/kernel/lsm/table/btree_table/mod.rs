pub(crate) mod iter;

use std::collections::BTreeMap;
use crate::kernel::lsm::iterator::Iter;
use crate::kernel::lsm::mem_table::KeyValue;
use crate::kernel::lsm::table::Table;
use bytes::Bytes;
use crate::kernel::lsm::table::btree_table::iter::BTreeTableIter;

pub(crate) struct BTreeTable {
    level: usize,
    gen: i64,
    len: usize,
    inner: BTreeMap<Bytes, KeyValue>,
}

impl BTreeTable {
    pub(crate) fn new(level: usize, gen: i64, data: Vec<KeyValue>) -> Self {
        let len = data.len();
        let inner = BTreeMap::from_iter(
            data.into_iter()
                .map(|(key, value)| (key.clone(), (key, value))),
        );

        BTreeTable {
            level,
            gen,
            len,
            inner,
        }
    }
}

impl Table for BTreeTable {
    fn query(&self, key: &[u8]) -> crate::kernel::KernelResult<Option<KeyValue>> {
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

    #[allow(clippy::todo)]
    fn iter<'a>(
        &'a self,
    ) -> crate::kernel::KernelResult<Box<dyn Iter<'a, Item = KeyValue> + 'a + Send + Sync>> {
        Ok(Box::new(BTreeTableIter::new(self)))
    }
}
