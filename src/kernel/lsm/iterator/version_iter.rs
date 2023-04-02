use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::Arc;
use bytes::Bytes;
use crate::kernel::lsm::block::Value;
use crate::kernel::lsm::compactor::LEVEL_0;
use crate::kernel::lsm::iterator::{DiskIter, Seek};
use crate::kernel::lsm::iterator::level_iter::LevelIter;
use crate::kernel::lsm::mem_table::KeyValue;
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

/// Version键值对迭代器
///
/// Tips: VersionIter与其他迭代器有一个不同点：VersionIter从最新/最大的键值向最后迭代
/// 因为Level0的数据是可能冲突的，因此由最后/新的SSTable开始向前/旧的SSTable进行遍历
pub struct VersionIter<'a> {
    // 该死的生命周期
    all_ss_tables: InnerPtr<Vec<Vec<SSTable>>>,
    version: InnerPtr<Arc<Version>>,
    // 用于SeekLast
    level_upper: usize,

    init_buf: Option<KeyValue>,
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

        let mut level_iter = unsafe {
            LevelIter::new(
                &all_ss_tables.as_ref()[0], LEVEL_0, &version.0.as_ref().block_cache
            )?
        };
        let init_buf = level_iter.seek(Seek::Last).ok();

        // 找到最大Level值
        let mut level_upper = 7;
        unsafe {
            for level_ss_tables in all_ss_tables.as_ref().iter().rev() {
                if level_ss_tables.is_empty() { level_upper -= 1 } else { break }
            }
        };
        if level_upper == 0 {
            return Err(KernelError::DataEmpty)
        }

        Ok(Self {
            all_ss_tables,
            offset: 0,
            level_iter,
            version,
            level_upper,
            init_buf,
        })
    }

    fn iter_sync(&mut self, offset: usize, seek: Seek) -> Result<KeyValue> {
        let is_level_eq = self.offset != offset;
        self.offset = offset;

        if self.is_valid() {
            if is_level_eq {
                unsafe {
                    self.level_iter = LevelIter::new(
                        &self.all_ss_tables.as_ref()[offset],
                        offset,
                        &self.version.0.as_ref().block_cache
                    )?;
                }
            }
            self.level_iter.seek(seek)
        } else {
            Err(KernelError::OutOfBounds)
        }
    }

    fn seek_ward(&mut self, key: &[u8], seek: Seek) -> Result<KeyValue> {
        let mut item = (Bytes::new(), None);
        for level in 0..7 {
            item = self.iter_sync(level, seek)?;
            if key == &item.0 { break }
        }

        Ok(item)
    }
}

impl DiskIter<Vec<u8>, Value> for VersionIter<'_> {
    type Item = KeyValue;

    fn next(&mut self) -> Result<Self::Item> {
        // 弹出初始化seek时的第一位数据
        if let Some(item) = self.init_buf.take() {
            return Ok(item);
        }

        match self.level_iter.prev() {
            Ok(item) => Ok(item),
            Err(KernelError::OutOfBounds) => {
                self.iter_sync(self.offset + 1, Seek::Last)
            },
            Err(e) => Err(e)
        }
    }

    fn prev(&mut self) -> Result<Self::Item> {
        match self.level_iter.next() {
            Ok(item) => Ok(item),
            Err(KernelError::OutOfBounds) => {
                self.iter_sync(self.offset - 1, Seek::First)
            },
            Err(e) => Err(e)
        }
    }

    fn is_valid(&self) -> bool {
        self.offset < 7
    }

    fn seek(&mut self, seek: Seek) -> Result<Self::Item> {
        match seek {
            Seek::First => {
                self.iter_sync(0, Seek::Last)
            }
            Seek::Last => {
                self.iter_sync(self.level_upper - 1, Seek::First)
            }
            Seek::Forward(key) => {
                self.seek_ward(key, seek)
            }
            Seek::Backward(key) => {
                self.seek_ward(key, seek)
            }
        }
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
    use bincode::Options;
    use bytes::Bytes;
    use itertools::Itertools;
    use tempfile::TempDir;
    use crate::kernel::lsm::lsm_kv::{Config, LsmStore};
    use crate::kernel::{KVStore, Result};
    use crate::kernel::lsm::iterator::{DiskIter, Seek};
    use crate::kernel::lsm::mem_table::KeyValue;

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
                let vec_u8 = bincode::options().with_big_endian().serialize(&i)?;
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
                    kv_store.set(&vec_kv[i * 1000 + j].0, Bytes::from(vec_kv[i * 1000 + j].1.clone())).await?
                }
                kv_store.flush().await?;
            }

            let mut iterator = kv_store.disk_iter().await?;

            for i in (0..times).rev() {
                assert_eq!(iterator.next()?, kv_trans(vec_kv[i].clone()));
            }

            for i in 1..times {
                assert_eq!(iterator.prev()?, kv_trans(vec_kv[i].clone()));
            }

            assert_eq!(iterator.seek(Seek::Backward(&vec_kv[114].0))?,  kv_trans(vec_kv[114].clone()));

            assert_eq!(iterator.seek(Seek::Forward(&vec_kv[1024].0))?,  kv_trans(vec_kv[1024].clone()));

            assert_eq!(iterator.seek(Seek::Forward(&vec_kv[2333].0))?,  kv_trans(vec_kv[2333].clone()));

            assert_eq!(iterator.seek(Seek::Backward(&vec_kv[2048].0))?,  kv_trans(vec_kv[2048].clone()));

            assert_eq!(iterator.seek(Seek::First)?,  kv_trans(vec_kv[4999].clone()));

            assert_eq!(iterator.seek(Seek::Last)?,  kv_trans(vec_kv[0].clone()));

            Ok(())
        })
    }

    fn kv_trans(kv: (Vec<u8>, Vec<u8>)) -> KeyValue {
        let (key, value) = kv;
        (Bytes::from(key), Some(Bytes::from(value)))
    }
}