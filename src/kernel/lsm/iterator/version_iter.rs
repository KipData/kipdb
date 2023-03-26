use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::Arc;
use crate::kernel::lsm::block::Value;
use crate::kernel::lsm::compactor::LEVEL_0;
use crate::kernel::lsm::iterator::{DiskIter, Seek};
use crate::kernel::lsm::iterator::level_iter::LevelIter;
use crate::kernel::lsm::ss_table::SSTable;
use crate::kernel::lsm::version::Version;
use crate::kernel::Result;
use crate::KernelError;

struct InnerPtr<T>(NonNull<T>);

unsafe impl<T: Send> Send for InnerPtr<T> {}
unsafe impl<T: Sync> Sync for InnerPtr<T> {}

impl<T> Clone for InnerPtr<T> {
    fn clone(&self) -> Self {
        InnerPtr(self.0)
    }
}

impl<T> Copy for InnerPtr<T> {

}

impl<T> Deref for InnerPtr<T> {
    type Target = NonNull<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for InnerPtr<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub struct VersionIter<'a> {
    // 该死的生命周期
    all_ss_tables: InnerPtr<Vec<Vec<SSTable>>>,
    version: InnerPtr<Arc<Version>>,

    offset: usize,
    level_iter: LevelIter<'a>
}

impl<'a> VersionIter<'a> {
    pub(crate) async fn new(version: Arc<Version>) -> Result<VersionIter<'a>> {
        let all_ss_tables: InnerPtr<Vec<Vec<SSTable>>> = InnerPtr(
            Box::leak(Box::new(
                version.get_all_ss_tables().await
            )).into()
        );

        let version: InnerPtr<Arc<Version>> = InnerPtr(
            Box::leak(Box::new(
                version
            )).into()
        );

        let level_iter = unsafe {
            LevelIter::new(
                &all_ss_tables.as_ref()[0], LEVEL_0, &version.0.as_ref().block_cache
            )?
        };

        Ok(Self {
            all_ss_tables,
            offset: 0,
            level_iter,
            version,
        })
    }

    fn iter_sync(&mut self, level: usize, seek: Seek) -> Result<()> {
        if self.offset != level {
            unsafe {
                self.level_iter = LevelIter::new(

                    &self.all_ss_tables.as_ref()[level],
                    level,
                    &self.version.0.as_ref().block_cache
                )?;
            }
            self.offset = level;
        }
        self.level_iter.seek(seek)?;

        Ok(())
    }

    fn seek_ward(&mut self, key: &[u8], seek: Seek) -> Result<()> {
        for level in 0..7 {
            self.iter_sync(level, seek)?;
            if self.level_iter.key() == key { break }
        }

        Ok(())
    }
}

impl DiskIter<Vec<u8>, Value> for VersionIter<'_> {
    fn next(&mut self) -> Result<()> {
        if let Err(KernelError::OutOfBounds) = self.level_iter.next() {
            let next_level = self.offset + 1;
            if next_level < 7 && self.is_valid() {
                self.iter_sync(next_level, Seek::First)?;
            } else {
                return Err(KernelError::OutOfBounds);
            }
        }

        Ok(())
    }

    fn prev(&mut self) -> Result<()> {
        if let Err(KernelError::OutOfBounds) = self.level_iter.prev() {
            if self.offset > 0 && self.is_valid() {
                self.iter_sync(self.offset - 1, Seek::Last)?;
            } else {
                return Err(KernelError::OutOfBounds);
            }
        }

        Ok(())
    }

    fn key(&self) -> Vec<u8> {
        self.level_iter.key()
    }

    fn value(&self) -> &Value {
        self.level_iter.value()
    }

    fn is_valid(&self) -> bool {
        self.offset <= 7 && self.level_iter.is_valid()
    }

    fn seek(&mut self, seek: Seek) -> Result<()> {
        match seek {
            Seek::First => {
                self.iter_sync(0, Seek::First)?;
            }
            Seek::Last => {
                self.iter_sync(6, Seek::Last)?;
            }
            Seek::Forward(key) => {
                self.seek_ward(key, seek)?;
            }
            Seek::Backward(key) => {
                self.seek_ward(key, seek)?;
            }
        }

        Ok(())
    }
}

impl Drop for VersionIter<'_> {
    fn drop(&mut self) {
        drop(self.all_ss_tables.as_ptr());
        drop(self.version.as_ptr());
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use itertools::Itertools;
    use tempfile::TempDir;
    use crate::kernel::lsm::lsm_kv::{Config, LsmStore};
    use crate::kernel::{KVStore, Result};
    use crate::kernel::lsm::iterator::{DiskIter, Seek};

    #[test]
    fn test_iterator() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        tokio_test::block_on(async move {
            let times = 5000;

            let value = b"The mystery of creation is like the darkness of night--it is great. \
            Delusions of knowledge are like the fog of the morning.";

            let config = Config::new(temp_dir.path().to_str().unwrap())
                .wal_enable(false)
                .minor_threshold_with_len(1000)
                .major_threshold_with_sst_size(4);
            let kv_store = LsmStore::open_with_config(config).await?;
            let mut vec_kv = Vec::new();

            for i in 0..times {
                let vec_u8 = bincode::serialize(&i)?;
                vec_kv.push((
                    vec_u8.clone(),
                    vec_u8.into_iter()
                        .chain(value.to_vec())
                        .collect_vec()
                ));
            }

            assert_eq!(times % 1000, 0);

            for i in 0..times / 1000 {
                for j in 0..1000 {
                    kv_store.set(&vec_kv[i * 1000 + j].0, vec_kv[i * 1000 + j].1.clone()).await?
                }
                kv_store.flush().await?;
            }

            let (vec_k, vec_v): (HashSet<Vec<u8>>, HashSet<Vec<u8>>) = vec_kv.iter()
                .cloned().unzip();

            let mut iterator = kv_store.disk_iter().await?;

            // 被排列后的SSTable是没法直接进行排序的
            // 因为他们之间的Level顺序和新旧顺序会打乱原有顺序，因此此处使用HashSet对其进行唯一判断
            let mut vec_k_clone_next = vec_k.clone();
            let mut vec_v_clone_next = vec_v.clone();
            for _ in 0..times - 1 {
                assert!(vec_k_clone_next.contains(&iterator.key()));
                let _ = vec_k_clone_next.remove(&iterator.key());
                assert!(vec_v_clone_next.contains(&iterator.value().value_clone().unwrap()));
                let _ = vec_v_clone_next.remove(&iterator.value().value_clone().unwrap());
                iterator.next()?;
            }

            let mut vec_k_clone_prev = vec_k.clone();
            let mut vec_v_clone_prev = vec_v.clone();
            for _ in 0..times - 1 {
                assert!(vec_k_clone_prev.contains(&iterator.key()));
                let _ = vec_k_clone_prev.remove(&iterator.key());
                assert!(vec_v_clone_prev.contains(&iterator.value().value_clone().unwrap()));
                let _ = vec_v_clone_prev.remove(&iterator.value().value_clone().unwrap());
                iterator.prev()?;
            }

            iterator.seek(Seek::Backward(&vec_kv[114].0))?;
            assert!(vec_k.contains(&vec_kv[114].0));

            iterator.seek(Seek::Forward(&vec_kv[1024].0))?;
            assert!(vec_k.contains(&vec_kv[1024].0));

            iterator.seek(Seek::Forward(&vec_kv[2333].0))?;
            assert!(vec_k.contains(&vec_kv[2333].0));

            iterator.seek(Seek::Backward(&vec_kv[2048].0))?;
            assert!(vec_k.contains(&vec_kv[2048].0));

            Ok(())
        })
    }
}