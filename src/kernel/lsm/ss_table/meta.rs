use std::borrow::Borrow;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use crate::kernel::lsm::ss_table::SSTable;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq, Default)]
pub(crate) struct SSTableMeta {
    pub(crate) size_of_disk: u64,
    pub(crate) len: usize,
}

impl SSTableMeta {
    pub(crate) fn fusion(metas: &[SSTableMeta]) -> Self {
        let mut meta = SSTableMeta { size_of_disk: 0, len: 0 };

        for SSTableMeta { size_of_disk, len } in metas {
            meta.len += len;
            meta.size_of_disk += size_of_disk;
        }

        meta
    }
}

impl From<&SSTable> for SSTableMeta {
    fn from(value: &SSTable) -> Self {
        SSTableMeta {
            size_of_disk: value.size_of_disk(),
            len: value.len(),
        }
    }
}

impl<T> From<&[T]> for SSTableMeta where T: Borrow<SSTable> {
    fn from(value: &[T]) -> Self {
        let mut sst_meta = SSTableMeta { size_of_disk: 0, len: 0 };

        for sst in value.iter().map(T::borrow).unique_by(|sst| sst.get_gen()) {
            sst_meta.len += sst.len();
            sst_meta.size_of_disk += sst.size_of_disk();
        }

        sst_meta
    }
}