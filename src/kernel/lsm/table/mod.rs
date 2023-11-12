use crate::kernel::lsm::iterator::Iter;
use crate::kernel::lsm::mem_table::KeyValue;
use crate::kernel::lsm::table::meta::TableMeta;
use crate::kernel::KernelResult;
use itertools::Itertools;

pub(crate) mod btree_table;
pub(crate) mod loader;
pub(crate) mod meta;
pub(crate) mod scope;
pub(crate) mod ss_table;

#[derive(Copy, Clone, Debug)]
pub enum TableType {
    SortedString,
    BTree,
}

pub(crate) type BoxTable = Box<dyn Table>;

pub(crate) trait Table: Sync + Send {
    fn query(&self, key: &[u8]) -> KernelResult<Option<KeyValue>>;

    fn len(&self) -> usize;

    fn size_of_disk(&self) -> u64;

    fn gen(&self) -> i64;

    fn level(&self) -> usize;

    fn iter<'a>(&'a self) -> KernelResult<Box<dyn Iter<'a, Item = KeyValue> + 'a + Sync + Send>>;
}

/// 通过一组SSTable收集对应的Gen
pub(crate) fn collect_gen(vec_table: &[&dyn Table]) -> KernelResult<(Vec<i64>, TableMeta)> {
    let meta = TableMeta::from(vec_table);

    Ok((
        vec_table.iter().map(|sst| sst.gen()).unique().collect_vec(),
        meta,
    ))
}
