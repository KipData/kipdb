use crate::kernel::lsm::compactor::LEVEL_0;
use crate::kernel::lsm::iterator::{Iter, ForwardIter, Seek};
use crate::kernel::lsm::mem_table::KeyValue;
use crate::kernel::lsm::ss_table::iter::SSTableIter;
use crate::kernel::lsm::version::Version;
use crate::kernel::Result;
use crate::KernelError;

const LEVEL_0_SEEK_MESSAGE: &str = "level 0 cannot seek";

pub(crate) struct LevelIter<'a> {
    version: &'a Version,
    level: usize,
    level_len: usize,

    offset: usize,
    sst_iter: SSTableIter<'a>,
}

impl<'a> LevelIter<'a> {
    #[allow(dead_code)]
    pub(crate) fn new(version: &'a Version, level: usize) -> Result<LevelIter<'a>> {
        let ss_table = version.get_ss_table(level, 0)
            .ok_or(KernelError::DataEmpty)?;
        let sst_iter = SSTableIter::new(ss_table, &version.block_cache)?;
        let level_len = version.level_len(level);

        Ok(Self {
            version,
            level,
            level_len,
            offset: 0,
            sst_iter,
        })
    }

     fn sst_iter_seek(&mut self, seek: Seek<'_>, offset: usize) -> Result<Option<KeyValue>> {
        self.offset = offset;
        if self.is_valid() {
            if let Some(ss_table) = self.version.get_ss_table(self.level, offset) {
                self.sst_iter = SSTableIter::new(ss_table, &self.version.block_cache)?;
                return self.sst_iter.seek(seek);
            }
        }

        Ok(None)
    }

     fn seek_ward(&mut self, key: &[u8], seek: Seek<'_>) -> Result<Option<KeyValue>> {
        let level = self.level;

        if level == LEVEL_0 {
            return Err(KernelError::NotSupport(LEVEL_0_SEEK_MESSAGE));
        }
        self.sst_iter_seek(seek, self.version.query_meet_index(key, level))
    }
}

impl<'a> ForwardIter<'a> for LevelIter<'a> {
     fn prev_err(&mut self) -> Result<Option<Self::Item>> {
        match self.sst_iter.prev_err()? {
            None => {
                if self.offset > 0 {
                    self.sst_iter_seek(Seek::Last, self.offset - 1)
                } else {
                    Ok(None)
                }
            }
            Some(item) => Ok(Some(item))
        }
    }
}

impl<'a> Iter<'a> for LevelIter<'a> {
    type Item = KeyValue;

    fn next_err(&mut self) -> Result<Option<Self::Item>> {
        match self.sst_iter.next_err()? {
            None => self.sst_iter_seek(Seek::First, self.offset + 1),
            Some(item) => Ok(Some(item))
        }
    }

    fn is_valid(&self) -> bool {
        self.offset < self.level_len
    }

    /// Tips: Level 0的LevelIter不支持Seek
    /// 因为Level 0中的SSTable并非有序排列，其中数据范围是可能交错的
     fn seek(&mut self, seek: Seek<'_>) -> Result<Option<Self::Item>> {
        match seek {
            Seek::First => {
                self.sst_iter_seek(Seek::First, 0)
            }
            Seek::Last => {
                self.sst_iter_seek(Seek::Last, self.level_len - 1)
            }
            Seek::Backward(key) => {
                self.seek_ward(key, seek)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bincode::Options;
    use bytes::Bytes;
    use tempfile::TempDir;
    use crate::kernel::io::IoType;
    use crate::kernel::Result;
    use crate::kernel::lsm::iterator::{Iter, ForwardIter, Seek};
    use crate::kernel::lsm::iterator::level_iter::LevelIter;
    use crate::kernel::lsm::log::LogLoader;
    use crate::kernel::lsm::mem_table::DEFAULT_WAL_PATH;
    use crate::kernel::lsm::ss_table::meta::SSTableMeta;
    use crate::kernel::lsm::storage::Config;
    use crate::kernel::lsm::version::edit::VersionEdit;
    use crate::kernel::lsm::version::status::VersionStatus;

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
                VersionStatus::load_with_path(config.clone(), wal.clone())?;

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

            let (scope_1, meta_1) = ver_status
                .loader()
                .create(1, slice_1.to_vec(), 1)?;
            let (scope_2, meta_2) = ver_status
                .loader()
                .create(2, slice_2.to_vec(), 1)?;
            let fusion_meta = SSTableMeta::fusion(&vec![meta_1, meta_2]);

            let vec_edit = vec![
                // Tips: 由于level 0只是用于测试seek是否发生错误，因此可以忽略此处重复使用
                VersionEdit::NewFile((vec![scope_1.clone()], 0),0, SSTableMeta { size_of_disk: 0, len: 0 }),
                VersionEdit::NewFile((vec![scope_1, scope_2], 1),0, fusion_meta)
            ];

            ver_status.log_and_apply(vec_edit, 10).await?;

            let version = ver_status.current().await;

            let mut iterator = LevelIter::new(&version, 1)?;
            for i in 0..times {
                assert_eq!(iterator.next_err()?.unwrap(), vec_data[i]);
            }

            for i in (0..times - 1).rev() {
                assert_eq!(iterator.prev_err()?.unwrap(), vec_data[i]);
            }

            assert_eq!(iterator.seek(Seek::Backward(&vec_data[114].0))?.unwrap(), vec_data[114]);

            assert_eq!(iterator.seek(Seek::Backward(&vec_data[2048].0))?.unwrap(), vec_data[2048]);

            assert_eq!(iterator.seek(Seek::First)?.unwrap(), vec_data[0]);

            assert_eq!(iterator.seek(Seek::Last)?.unwrap(), vec_data[3999]);

            let mut iterator_level_0 = LevelIter::new(&version, 0)?;

            assert!(iterator_level_0.seek(Seek::Backward(&vec_data[3333].0)).is_err());

            Ok(())
        })
    }
}