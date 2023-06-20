use crate::kernel::lsm::iterator::{Iter, Seek};
use crate::kernel::lsm::iterator::merging_iter::MergingIter;
use crate::kernel::lsm::version::version_iter::VersionIter;
use crate::kernel::lsm::mem_table::{KeyValue, MemMapIter, TableInner};
use crate::kernel::lsm::version::Version;
use crate::kernel::Result;

/// MemTable + Version键值对迭代器
#[allow(dead_code)]
pub struct FullIter<'a> {
    merge_iter: MergingIter<'a>
}

impl<'a> FullIter<'a> {
    #[allow(dead_code)]
    pub(crate) fn new(mem_table: &'a TableInner, version: &'a Version) -> Result<FullIter<'a>> {
        let mut vec_iter: Vec<Box<dyn Iter<'a, Item=KeyValue> + 'a>> = vec![
            Box::new(MemMapIter::new(&mem_table._mem))
        ];

        if let Some(immut_map) = &mem_table._immut {
            vec_iter.push(Box::new(MemMapIter::new(immut_map)));
        }

        vec_iter.append(&mut VersionIter::merging_with_version(version)?);

        Ok(Self { merge_iter: MergingIter::new(vec_iter)? })
    }
}

impl<'a> Iter<'a> for FullIter<'a> {
    type Item = KeyValue;

    fn next_err(&mut self) -> Result<Option<Self::Item>> {
        self.merge_iter.next_err()
    }

    fn is_valid(&self) -> bool {
        self.merge_iter.is_valid()
    }

    fn seek(&mut self, seek: Seek<'_>) -> Result<Option<Self::Item>> {
        self.merge_iter.seek(seek)
    }
}

#[cfg(test)]
mod tests {
    use bincode::Options;
    use bytes::Bytes;
    use itertools::Itertools;
    use tempfile::TempDir;
    use crate::kernel::{Storage, Result};
    use crate::kernel::lsm::iterator::Iter;
    use crate::kernel::lsm::storage::{Config, LsmStore};
    use crate::kernel::lsm::version::version_iter::VersionIter;

    #[test]
    fn test_iterator() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        tokio_test::block_on(async move {
            let times = 5000;

            let test_str = b"The mystery of creation is like the darkness of night--it is great. \
            Delusions of knowledge are like the fog of the morning.";

            let config = Config::new(temp_dir.path().to_str().unwrap())
                .major_threshold_with_sst_size(4);
            let kv_store = LsmStore::open_with_config(config).await?;
            let mut vec_kv = Vec::new();

            for i in 100..times + 100 {
                let vec_u8 = bincode::options().with_big_endian().serialize(&i)?;
                let bytes = vec_u8.iter()
                    .cloned()
                    .chain(test_str.to_vec())
                    .collect_vec();

                vec_kv.push((vec_u8, bytes));
            }

            assert_eq!(times % 1000, 0);

            for i in 0..times / 1000 {
                for j in 0..1000 {
                    kv_store.set(&vec_kv[i * 1000 + j].0, Bytes::from(vec_kv[i * 1000 + j].1.clone())).await?
                }
                kv_store.flush().await?;
            }

            let version = kv_store.current_version().await;

            let mut version_iter = VersionIter::new(&version)?;

            for (test_key, test_value) in &vec_kv {
                let (key, value) = version_iter.next_err()?.unwrap();
                assert_eq!(key, Bytes::from(test_key.clone()));
                assert_eq!(value, Some(Bytes::from(test_value.clone())))
            }

            let mut temp = Vec::new();

            for i in 0..100 {
                let vec_u8 = bincode::serialize(&i)?;
                let bytes = vec_u8.iter()
                    .cloned()
                    .chain(test_str.to_vec())
                    .collect_vec();

                kv_store.set(&vec_u8, Bytes::from(bytes.clone())).await?;
                temp.push((vec_u8, bytes));
            }

            temp.append(&mut vec_kv);

            let guard = kv_store.guard().await?;
            let mut full_iter = guard.iter()?;

            for (test_key, test_value) in temp {
                let (key, value) = full_iter.next_err()?.unwrap();
                assert_eq!(key, Bytes::from(test_key));
                assert_eq!(value, Some(Bytes::from(test_value)))
            }

            Ok(())
        })
    }
}