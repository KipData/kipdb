use crate::kernel::Result;
use bytes::Bytes;
use itertools::Itertools;
use crate::kernel::lsm::table::meta::TableMeta;

pub(crate) mod ss_table;
pub(crate) mod skip_table;
pub(crate) mod scope;
pub(crate) mod meta;

pub(crate) trait Table {
    fn query(&self, key: &[u8]) -> Result<Option<Bytes>>;

    fn len(&self) -> usize;

    fn size_of_disk(&self) -> u64;

    fn gen(&self) -> i64;

    fn level(&self) -> usize;
}

/// 通过一组SSTable收集对应的Gen
pub(crate) fn collect_gen(vec_table: &[&Box<dyn Table>]) -> Result<(Vec<i64>, TableMeta)> {
    let meta = TableMeta::from(vec_table);

    Ok((
        vec_table
            .iter()
            .map(|sst| sst.gen())
            .unique()
            .collect_vec(),
        meta,
    ))
}
