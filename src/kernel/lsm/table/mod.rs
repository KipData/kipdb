use crate::kernel::Result;
use bytes::Bytes;

pub(crate) mod ss_table;
pub(crate) mod skip_table;

trait Table {
    fn query(&self, key: &[u8]) -> Result<Option<Bytes>>;

    fn len(&self) -> usize;

    fn size_of_disk(&self) -> u64;

    fn gen(&self) -> i64;

    fn level(&self) -> usize;
}
