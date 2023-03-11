use std::collections::VecDeque;
use std::sync::Arc;
use parking_lot::RwLock;
use tracing::error;
use crate::kernel::{CommandData, CommandPackage, Result, sorted_gen_list};
use crate::kernel::io::{FileExtension, IoFactory, IoType, IoWriter};
use crate::kernel::lsm::lsm_kv::Config;

const SUCCESS_FS_GEN: i64 = 000_000_000;

// TODO: 使用Block进行替代
pub(crate) struct LogLoader {
    factory: IoFactory,
    config: Arc<Config>,
    inner: RwLock<Inner>,
    check_success: bool,
}

struct Inner {
    current_gen: i64,
    writer: Box<dyn IoWriter>,
    vec_gen: VecDeque<i64>
}

impl LogLoader {
    /// 通过日志进行WalLoader和MemMap的数据重载
    pub(crate) fn reload_with_check(
        config: &Arc<Config>,
        path_name: &str,
        extension: FileExtension
    ) -> Result<(Self, Option<Vec<CommandData>>)> {
        let (mut loader, last_gen) = Self::reload_(
            config,
            path_name,
            extension
        )?;
        loader.check_success = true;

        let option_data = Self::check_and_reload(&loader.factory, last_gen)?;

        Ok((loader, option_data))
    }

    pub(crate) fn reload(
        config: &Arc<Config>,
        path_name: &str,
        extension: FileExtension
    ) -> Result<Self> {
        Self::reload_(config, path_name, extension)
            .map(|(loader, _)| loader)
    }

    fn reload_(
        config: &Arc<Config>,
        path_name: &str,
        extension: FileExtension
    ) -> Result<(Self, i64)> {
        let config = Arc::clone(config);
        let wal_path = config.dir_path
            .join(path_name);

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

        let inner = RwLock::new(
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
            check_success: false,
        }, last_gen))
    }

    /// 同时检测并恢复数据，防止数据异常而丢失
    fn check_and_reload(
        factory: &IoFactory,
        last_gen: i64,
    ) -> Result<Option<Vec<CommandData>>> {
        // 当存在SUCCESS_FS时，代表Drop不正常，因此恢复最新的gen日志进行恢复
        if factory.has_gen(SUCCESS_FS_GEN)? {
            let reader = factory.reader(last_gen, IoType::MMap)?;
            return Ok(Some(CommandPackage::from_read_to_unpack_vec(reader.as_ref())?));
        } else { let _ignore = factory.create_fs(SUCCESS_FS_GEN)?; }

        Ok(None)
    }

    pub(crate) fn log(&self, cmd: &CommandData) -> Result<()> {
        let inner = self.inner.read();
        let _ignore = CommandPackage::write(inner.writer.as_ref(), cmd)?;
        Ok(())
    }

    pub(crate) fn log_batch(&self, vec_cmd: &[CommandData]) -> Result<()> {
        let inner = self.inner.read();
        let _ignore = CommandPackage::write_batch(inner.writer.as_ref(), vec_cmd)?;
        Ok(())
    }

    pub(crate) fn flush(&self) -> Result<()> {
        self.inner.read()
            .writer.flush()
    }

    pub(crate) fn last_gen(&self) -> Option<i64> {
        self.inner.read()
            .vec_gen.back()
            .cloned()
    }

    pub(crate) fn load_last(&self) -> Result<Option<Vec<CommandData>>> {
        if let Some(gen) = self.last_gen() {
            self.load(gen)
        } else { Ok(None) }
    }

    /// 弹出此日志的Gen并重新以新Gen进行日志记录
    pub(crate) fn switch(&self, next_gen: i64) -> Result<i64> {
        let next_writer = self.factory.writer(next_gen, IoType::Direct)?;
        let mut inner = self.inner.write();

        let current_gen = inner.current_gen;
        inner.writer.flush()?;

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
    #[allow(clippy::if_then_some_else_none)]
    pub(crate) fn load(&self, gen: i64) -> Result<Option<Vec<CommandData>>> {
        Ok(if self.factory.has_gen(gen)? {
            let reader = self.factory.reader(gen, IoType::MMap)?;
            Some(CommandPackage::from_read_to_unpack_vec(reader.as_ref())?)
        } else { None })
    }
}

impl Drop for LogLoader {
    // 使用drop释放SUCCESS_FS，以代表此次运行正常
    fn drop(&mut self) {
        let _ignore = self.check_success
            .then(|| {
                if let Err(err) = self.factory.clean(SUCCESS_FS_GEN) {
                    error!("[WALLoader][drop][error]: {err:?}")
                }
            });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tempfile::TempDir;
    use crate::kernel::io::FileExtension;
    use crate::kernel::lsm::log::LogLoader;
    use crate::kernel::{CommandData, Result};
    use crate::kernel::lsm::lsm_kv::{Config, DEFAULT_WAL_PATH};

    #[test]
    fn test_log_load() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let config = Arc::new(Config::new(temp_dir.into_path(), 0, 0));

        let wal = LogLoader::reload(
            &config,
            DEFAULT_WAL_PATH,
            FileExtension::Log
        )?;

        let data_1 = CommandData::set(b"kip_key_1".to_vec(), b"kip_value".to_vec());
        let data_2 = CommandData::set(b"kip_key_2".to_vec(), b"kip_value".to_vec());

        wal.log(&data_1)?;
        wal.log(&data_2)?;

        let gen = wal.switch(config.create_gen_lazy())?;

        drop(wal);

        let wal = LogLoader::reload(
            &config,
            DEFAULT_WAL_PATH,
            FileExtension::Log
        )?;
        let option = wal.load(gen)?;

        assert_eq!(option, Some(vec![data_1, data_2]));

        Ok(())
    }

    #[test]
    fn test_log_reload_check() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let config = Arc::new(Config::new(temp_dir.into_path(), 0, 0));

        let (wal_1, _) = LogLoader::reload_with_check(
            &config,
            DEFAULT_WAL_PATH,
            FileExtension::Log
        )?;

        let data_1 = CommandData::set(b"kip_key_1".to_vec(), b"kip_value".to_vec());
        let data_2 = CommandData::set(b"kip_key_2".to_vec(), b"kip_value".to_vec());

        wal_1.log(&data_1)?;
        wal_1.log(&data_2)?;

        wal_1.flush()?;
        // wal_1尚未drop时，则开始reload，模拟SUCCESS_FS未删除的情况(即停机异常)，触发数据恢复

        let (_, option_vec) = LogLoader::reload_with_check(
            &config,
            DEFAULT_WAL_PATH,
            FileExtension::Log
        )?;

        assert_eq!(option_vec, Some(vec![data_1, data_2]));

        Ok(())
    }
}


