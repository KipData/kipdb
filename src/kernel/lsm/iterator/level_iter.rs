use std::mem;
use async_trait::async_trait;
use crate::kernel::lsm::block::{BlockCache, Value};
use crate::kernel::lsm::iterator::{DiskIter, InnerPtr, Seek};
use crate::kernel::lsm::iterator::ss_table_iter::SSTableIter;
use crate::kernel::lsm::mem_table::KeyValue;
use crate::kernel::lsm::ss_table::SSTable;
use crate::kernel::lsm::version::Version;
use crate::kernel::Result;
use crate::KernelError;

const LEVEL_0_SEEK_MESSAGE: &str = "level 0 cannot seek";

pub(crate) struct LevelIter<'a> {
    version: &'a Version,
    block_cache: &'a BlockCache,
    level: usize,
    level_len: usize,

    ss_table_ptr: InnerPtr<SSTable>,
    offset: usize,
    sst_iter: SSTableIter<'a>,
}

impl<'a> LevelIter<'a> {
    #[allow(dead_code)]
    pub(crate) async fn new(version: &'a Version, level: usize, block_cache: &'a BlockCache) -> Result<LevelIter<'a>> {
        let ss_table_ptr: InnerPtr<SSTable> = Self::get_ss_table_ptr(version, level, 0).await?;
        let sst_iter = unsafe {
            SSTableIter::new(
                ss_table_ptr.0.as_ref(),
                block_cache
            ).await?
        };
        let level_len = version.level_len(level);

        Ok(Self {
            version,
            block_cache,
            level,
            level_len,
            ss_table_ptr,
            offset: 0,
            sst_iter,
        })
    }

    async fn get_ss_table_ptr(version: &Version, level: usize, offset: usize) -> Result<InnerPtr<SSTable>> {
        let ss_table = version.get_ss_table(level, offset)
            .ok_or(KernelError::DataEmpty)?;

        Ok(InnerPtr(Box::leak(Box::new(ss_table)).into()))
    }

    #[allow(clippy::drop_copy)]
    async fn sst_iter_seek(&mut self, seek: Seek<'_>, offset: usize) -> Result<KeyValue> {
        self.offset = offset;
        if self.is_valid() {
            // 手动析构旧的ss_table裸指针
            drop(mem::replace(
                &mut self.ss_table_ptr,
                Self::get_ss_table_ptr(self.version, self.level, offset).await?
            ).as_ptr());

            unsafe {
                let ss_table = self.ss_table_ptr.as_ref();
                self.sst_iter = SSTableIter::new(
                    ss_table,
                    self.block_cache
                ).await?;
            }
            self.sst_iter.seek(seek).await
        } else { Err(KernelError::OutOfBounds) }
    }

    async fn seek_ward(&mut self, key: &[u8], seek: Seek<'_>) -> Result<KeyValue> {
        let level = self.level;

        if level == 0 {
            return Err(KernelError::NotSupport(LEVEL_0_SEEK_MESSAGE));
        }
        self.sst_iter_seek(seek, self.version.query_meet_index(key, level)).await
    }
}

#[async_trait]
#[allow(single_use_lifetimes)]
impl DiskIter<Vec<u8>, Value> for LevelIter<'_> {
    type Item = KeyValue;

    async fn next_err(&mut self) -> Result<Self::Item> {
        match DiskIter::next_err(&mut self.sst_iter).await {
            Err(KernelError::OutOfBounds) => {
                self.sst_iter_seek(Seek::First, self.offset + 1).await
            }
            res => res
        }
    }

    async fn prev_err(&mut self) -> Result<Self::Item> {
        match self.sst_iter.prev_err().await {
            Err(KernelError::OutOfBounds) => {
                if self.offset > 0 {
                    self.sst_iter_seek(Seek::Last, self.offset - 1).await
                } else {
                    Err(KernelError::OutOfBounds)
                }
            }
            res => res
        }
    }

    fn is_valid(&self) -> bool {
        self.offset < self.level_len
    }

    /// Tips: Level 0的LevelIter不支持Seek
    /// 因为Level 0中的SSTable并非有序排列，其中数据范围是可能交错的
    async fn seek(&mut self, seek: Seek<'_>) -> Result<Self::Item> {
        match seek {
            Seek::First => {
                self.sst_iter_seek(Seek::First, 0).await
            }
            Seek::Last => {
                self.sst_iter_seek(Seek::Last, self.level_len - 1).await
            }
            Seek::Forward(key) => {
                self.seek_ward(key, seek).await
            }
            Seek::Backward(key) => {
                self.seek_ward(key, seek).await
            }
        }
    }
}

#[allow(clippy::drop_copy)]
impl Drop for LevelIter<'_> {
    fn drop(&mut self) {
        drop(self.ss_table_ptr.as_ptr());
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;
    use bincode::Options;
    use bytes::Bytes;
    use tempfile::TempDir;
    use crate::kernel::io::{FileExtension, IoFactory, IoType};
    use crate::kernel::lsm::lsm_kv::Config;
    use crate::kernel::lsm::ss_table::SSTable;
    use crate::kernel::lsm::version::{DEFAULT_SS_TABLE_PATH, VersionEdit, VersionStatus};
    use crate::kernel::Result;
    use crate::kernel::lsm::iterator::{DiskIter, Seek};
    use crate::kernel::lsm::iterator::level_iter::LevelIter;
    use crate::kernel::lsm::log::LogLoader;
    use crate::kernel::lsm::mem_table::DEFAULT_WAL_PATH;
    use crate::kernel::utils::lru_cache::ShardingLruCache;

    #[test]
    fn test_iterator() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        tokio_test::block_on(async move {
            let config = Config::new(temp_dir.into_path());

            let (wal, _, _) = LogLoader::reload(
                config.path(),
                (DEFAULT_WAL_PATH, Some(1)),
                IoType::Direct,
                |_| Ok(())
            )?;

            // 注意：将ss_table的创建防止VersionStatus的创建前
            // 因为VersionStatus检测无Log时会扫描当前文件夹下的SSTable进行重组以进行容灾
            let ver_status =
                VersionStatus::load_with_path(config.clone(), wal.clone()).await?;


            let sst_factory = IoFactory::new(
                config.dir_path.join(DEFAULT_SS_TABLE_PATH),
                FileExtension::SSTable
            )?;

            let value = Bytes::from_static(b"What you are you do not see, what you see is your shadow.");
            let mut vec_data = Vec::new();

            let times = 4000;

            // 默认使用大端序进行序列化，保证顺序正确性
            for i in 0..times {
                let mut key = b"KipDB-".to_vec();
                key.append(
                    &mut bincode::options().with_big_endian().serialize(&i)?
                );
                vec_data.push(
                    (Bytes::from(key), Some(value.clone()))
                );
            }
            let (slice_1, slice_2) = vec_data.split_at(2000);

            let (ss_table_1, scope_1) = SSTable::create_for_mem_table(
                &config,
                1,
                &sst_factory,
                slice_1.to_vec(),
                1,
                IoType::Direct
            )?;
            let (ss_table_2, scope_2) = SSTable::create_for_mem_table(
                &config,
                2,
                &sst_factory,
                slice_2.to_vec(),
                1,
                IoType::Direct
            )?;
            let cache = ShardingLruCache::new(
                config.block_cache_size,
                16,
                RandomState::default()
            )?;
            let vec_edit = vec![
                // 由于level 0只是用于测试seek是否发生错误，因此可以忽略此处重复使用
                VersionEdit::NewFile((vec![scope_1.clone()], 0),0),
                VersionEdit::NewFile((vec![scope_1, scope_2], 1),0)
            ];

            ver_status.insert_vec_ss_table(vec![ss_table_1, ss_table_2])?;
            ver_status.log_and_apply(vec_edit).await?;

            let version = ver_status.current().await;

            let mut iterator = LevelIter::new(&version, 1, &cache).await?;
            for i in 0..times {
                assert_eq!(DiskIter::next_err(&mut iterator).await?, vec_data[i]);
            }

            for i in (0..times - 1).rev() {
                assert_eq!(DiskIter::prev_err(&mut iterator).await?, vec_data[i]);
            }

            assert_eq!(iterator.seek(Seek::Backward(&vec_data[114].0)).await?, vec_data[114]);

            assert_eq!(iterator.seek(Seek::Forward(&vec_data[1024].0)).await?, vec_data[1024]);

            assert_eq!(iterator.seek(Seek::Forward(&vec_data[3333].0)).await?, vec_data[3333]);

            assert_eq!(iterator.seek(Seek::Backward(&vec_data[2048].0)).await?, vec_data[2048]);

            assert_eq!(iterator.seek(Seek::First).await?, vec_data[0]);

            assert_eq!(iterator.seek(Seek::Last).await?, vec_data[3999]);

            let mut iterator_level_0 = LevelIter::new(&version, 0, &cache).await?;

            assert!(iterator_level_0.seek(Seek::Forward(&vec_data[3333].0)).await.is_err());


            Ok(())
        })
    }
}