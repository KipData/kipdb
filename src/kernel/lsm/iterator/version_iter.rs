
use std::sync::Arc;
use crate::kernel::lsm::compactor::LEVEL_0;
use crate::kernel::lsm::iterator::{Iter, InnerPtr, Seek};
use crate::kernel::lsm::iterator::level_iter::LevelIter;
use crate::kernel::lsm::mem_table::KeyValue;
use crate::kernel::lsm::version::Version;
use crate::kernel::Result;

/// Version键值对迭代器
///
/// Tips: VersionIter与其他迭代器有一个不同点：VersionIter不支持DiskIter
/// 因为VersionIter中各个层级直接的数据是范围重复的，这导致无法实现Seek以支持良好的range查询
pub struct VersionIter<'a> {
    version: InnerPtr<Arc<Version>>,

    offset: usize,
    level_iter: LevelIter<'a>
}

impl<'a> VersionIter<'a> {
    pub(crate) async fn new(version: Arc<Version>) -> Result<VersionIter<'a>> {
        let version: InnerPtr<Arc<Version>> = InnerPtr(
            Box::leak(Box::new(
                version
            )).into()
        );

        let level_iter = unsafe {
            LevelIter::new(
                version.as_ref(),
                LEVEL_0,
                &version.0.as_ref().block_cache
            ).await?
        };

        Ok(Self {
            offset: 0,
            level_iter,
            version,
        })
    }

    fn is_valid(&self) -> bool {
        self.offset < 7
    }

    async fn iter_sync(&mut self, offset: usize, seek: Seek<'_>) -> Result<Option<KeyValue>> {
        let is_level_eq = self.offset != offset;
        self.offset = offset;

        if !self.is_valid() {
            return Ok(None);
        }

        if is_level_eq {
            unsafe {
                let version =  self.version.as_ref();
                self.level_iter = LevelIter::new(
                    version,
                    offset,
                    &self.version.0.as_ref().block_cache
                ).await?;
            }
        }
        self.level_iter.seek(seek).await
    }

    pub async fn next(&mut self) -> Result<Option<KeyValue>> {
        match self.level_iter.next_err().await? {
            None => self.iter_sync(self.offset + 1, Seek::First).await,
            Some(item) => Ok(Some(item))
        }
    }
}

#[allow(clippy::drop_copy)]
impl Drop for VersionIter<'_> {
    fn drop(&mut self) {
        drop(self.version.as_ptr());
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use bincode::Options;
    use bytes::Bytes;
    use itertools::Itertools;
    use tempfile::TempDir;
    use crate::kernel::lsm::lsm_kv::{Config, LsmStore};
    use crate::kernel::{KVStore, Result};

    #[test]
    fn test_iterator() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        tokio_test::block_on(async move {
            let times = 5000;

            let value = b"The mystery of creation is like the darkness of night--it is great. \
            Delusions of knowledge are like the fog of the morning.";

            let config = Config::new(temp_dir.path().to_str().unwrap())
                .minor_threshold_with_len(1000)
                .major_threshold_with_sst_size(4);
            let kv_store = LsmStore::open_with_config(config).await?;
            let mut vec_kv = Vec::new();
            let mut kv_map = HashMap::new();

            for i in 0..times {
                let vec_u8 = bincode::options().with_big_endian().serialize(&i)?;
                let bytes = vec_u8.iter()
                    .cloned()
                    .chain(value.to_vec())
                    .collect_vec();

                vec_kv.push((vec_u8.clone(), bytes.clone()));
                let _ = kv_map.insert(vec_u8, bytes);
            }

            assert_eq!(times % 1000, 0);

            for i in 0..times / 1000 {
                for j in 0..1000 {
                    kv_store.set(&vec_kv[i * 1000 + j].0, Bytes::from(vec_kv[i * 1000 + j].1.clone())).await?
                }
                kv_store.flush().await?;
            }

            let mut iterator = kv_store.disk_iter().await?;

            for _ in (0..times).rev() {
                let (key, _) = iterator.next().await?.unwrap();
                assert!(kv_map.remove(key.as_ref()).is_some())
            }

            Ok(())
        })
    }
}