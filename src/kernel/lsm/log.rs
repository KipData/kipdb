use std::collections::VecDeque;
use std::io::Cursor;
use itertools::Itertools;
use parking_lot::Mutex;
use crate::kernel::{Result, sorted_gen_list};
use crate::kernel::io::{FileExtension, IoFactory, IoType, IoWriter};
use crate::kernel::io::IoReader;
use crate::kernel::lsm::block::{Entry, Value};
use crate::kernel::lsm::lsm_kv::Config;
use crate::kernel::lsm::mem_table::KeyValue;

pub(crate) struct LogLoader {
    factory: IoFactory,
    config: Config,
    inner: Mutex<Inner>,
}

struct Inner {
    current_gen: i64,
    writer: Box<dyn IoWriter>,
    vec_gen: VecDeque<i64>
}

impl LogLoader {
    pub(crate) fn reload(
        config: Config,
        path_name: &str,
        extension: FileExtension
    ) -> Result<(Self, Vec<KeyValue>)> {
        let (loader, last_gen) = Self::reload_(
            config,
            path_name,
            extension
        )?;
        let reload_data = loader.load(last_gen)?;

        Ok((loader, reload_data))
    }

    fn reload_(
        config: Config,
        path_name: &str,
        extension: FileExtension
    ) -> Result<(Self, i64)> {
        let wal_path = config.path().join(path_name);

        let factory = IoFactory::new(
            wal_path.clone(),
            extension
        )?;

        let vec_gen = VecDeque::from_iter(
            sorted_gen_list(&wal_path, extension)?
        );
        let last_gen = vec_gen.back()
            .cloned()
            .unwrap_or(0);

        let inner = Mutex::new(
            Inner {
                current_gen: last_gen,
                writer: factory.writer(last_gen, IoType::Direct)?,
                vec_gen,
            }
        );

        Ok((LogLoader {
            factory,
            config,
            inner,
        }, last_gen))
    }

    pub(crate) fn log(&self, data: KeyValue) -> Result<()> {
        let bytes = Self::data_to_bytes(data)?;

        let _ = self.inner.lock()
            .writer.io_write(bytes)?;
        Ok(())
    }

    fn data_to_bytes(data: KeyValue) -> Result<Vec<u8>> {
        let (key, value) = data;
        Entry::new(0, key.len(), key, Value::from(value)).encode()
    }

    pub(crate) fn log_batch(&self, vec_data: Vec<KeyValue>) -> Result<()> {
        let bytes = vec_data.into_iter()
            .filter_map(|data| Self::data_to_bytes(data).ok())
            .flatten()
            .collect_vec();

        let _ = self.inner.lock()
            .writer.io_write(bytes)?;
        Ok(())
    }

    pub(crate) fn flush(&self) -> Result<()> {
        self.inner.lock()
            .writer.io_flush()
    }

    /// 弹出此日志的Gen并重新以新Gen进行日志记录
    pub(crate) fn switch(&self, next_gen: i64) -> Result<i64> {
        let next_writer = self.factory.writer(next_gen, IoType::Direct)?;
        let mut inner = self.inner.lock();

        let current_gen = inner.current_gen;
        inner.writer.io_flush()?;

        // 去除一半的SSTable
        let vec_len = inner.vec_gen.len();

        if vec_len >= self.config.wal_threshold {
            for _ in 0..vec_len / 2 {
                if let Some(gen) = inner.vec_gen.pop_front() {
                    self.factory.clean(gen)?;
                }
            }
        }

        inner.vec_gen.push_back(next_gen);
        inner.writer = next_writer;
        inner.current_gen = next_gen;

        Ok(current_gen)
    }

    /// 通过Gen载入数据进行读取
    pub(crate) fn load(&self, gen: i64) -> Result<Vec<KeyValue>> {
        Ok(Entry::<Value>::decode_with_cursor(&mut Cursor::new(
            IoReader::bytes(self.factory.reader(gen, IoType::MMap)?.as_ref())?
        ))?.into_iter()
            .map(|(_, Entry{ key, item, .. })| (key, item.bytes))
            .collect_vec())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use crate::kernel::io::FileExtension;
    use crate::kernel::lsm::log::LogLoader;
    use crate::kernel::Result;
    use crate::kernel::lsm::lsm_kv::{Config, DEFAULT_WAL_PATH, Gen};

    #[test]
    fn test_log_load() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let config = Config::new(temp_dir.into_path());

        let (wal, _) = LogLoader::reload(
            config.clone(),
            DEFAULT_WAL_PATH,
            FileExtension::Log
        )?;

        let data_1 = (b"kip_key_1".to_vec(), Some(b"kip_value".to_vec()));
        let data_2 = (b"kip_key_2".to_vec(), Some(b"kip_value".to_vec()));

        wal.log(data_1.clone())?;
        wal.log(data_2.clone())?;

        let gen = wal.switch(Gen::create())?;

        drop(wal);

        let (wal, _) = LogLoader::reload(
            config,
            DEFAULT_WAL_PATH,
            FileExtension::Log
        )?;

        assert_eq!(wal.load(gen)?, vec![data_1, data_2]);

        Ok(())
    }

    #[test]
    fn test_log_reload() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let config = Config::new(temp_dir.into_path());

        let (wal_1, _) = LogLoader::reload(
            config.clone(),
            DEFAULT_WAL_PATH,
            FileExtension::Log
        )?;

        let data_1 = (b"kip_key_1".to_vec(), Some(b"kip_value".to_vec()));
        let data_2 = (b"kip_key_2".to_vec(), Some(b"kip_value".to_vec()));

        wal_1.log(data_1.clone())?;
        wal_1.log(data_2.clone())?;

        wal_1.flush()?;
        // wal_1尚未drop时，则开始reload，模拟SUCCESS_FS未删除的情况(即停机异常)，触发数据恢复

        let (_, reload_data) = LogLoader::reload(
            config,
            DEFAULT_WAL_PATH,
            FileExtension::Log
        )?;

        assert_eq!(reload_data, vec![data_1, data_2]);

        Ok(())
    }
}


