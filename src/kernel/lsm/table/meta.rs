use std::borrow::Borrow;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use crate::kernel::lsm::table::Table;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq, Default)]
pub(crate) struct TableMeta {
    pub(crate) size_of_disk: u64,
    pub(crate) len: usize,
}

impl TableMeta {
    pub(crate) fn fusion(metas: &[TableMeta]) -> Self {
        let mut meta = TableMeta {
            size_of_disk: 0,
            len: 0,
        };

        for TableMeta { size_of_disk, len } in metas {
            meta.len += len;
            meta.size_of_disk += size_of_disk;
        }

        meta
    }
}

impl<T: Table> From<&T> for TableMeta {
    fn from(value: &T) -> Self {
        TableMeta {
            size_of_disk: value.size_of_disk(),
            len: value.len(),
        }
    }
}

impl<T: Borrow<Box<dyn Table>>> From<&[T]> for TableMeta {
    fn from(value: &[T]) -> Self {
        let mut sst_meta = TableMeta {
            size_of_disk: 0,
            len: 0,
        };

        for sst in value.iter().map(T::borrow).unique_by(|sst| sst.gen()) {
            sst_meta.len += sst.len();
            sst_meta.size_of_disk += sst.size_of_disk();
        }

        sst_meta
    }
}