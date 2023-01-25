use std::collections::VecDeque;
use std::sync::Arc;
use skiplist::SkipMap;
use tokio::sync::RwLock;
use tracing::error;
use crate::kernel::{CommandData, CommandPackage, Result, sorted_gen_list};
use crate::kernel::io::{FileExtension, IOHandler, IOHandlerFactory, IOType};
use crate::kernel::lsm::lsm_kv::Config;
use crate::kernel::lsm::MemMap;

const DEFAULT_WAL_PATH: &str = "wal";

const SUCCESS_FS_GEN: i64 = 000_000_000;

pub(crate) struct WalLoader {
    factory: IOHandlerFactory,
    config: Arc<Config>,
    inner: RwLock<Inner>,
    _success_fs: Box<dyn IOHandler>,
}

struct Inner {
    inner: (i64, Box<dyn IOHandler>),
    vec_gen: VecDeque<i64>
}

impl WalLoader {
    /// 通过日志进行WalLoader和MemMap的数据重载
    pub(crate) async fn reload_with_path(config: &Arc<Config>) -> Result<(Self, MemMap)> {
        let config = Arc::clone(config);
        let wal_path = config.dir_path.join(DEFAULT_WAL_PATH);

        let factory = IOHandlerFactory::new(
            wal_path.clone(),
            FileExtension::Log
        )?;

        let vec_gen = VecDeque::from_iter(
            sorted_gen_list(&wal_path, FileExtension::Log)?
        );
        let mem_map = Self::create_mem_map_and_check(&factory, &vec_gen).await?;

        let current_gen = config.create_gen();

        let inner = RwLock::new(
            Inner {
                inner: (current_gen, factory.create(current_gen, IOType::Buf)?),
                vec_gen,
            }
        );
        let success_fs = factory.create(SUCCESS_FS_GEN, IOType::Buf)?;

        Ok((WalLoader {
            factory,
            config,
            inner,
            _success_fs: success_fs,
        }, mem_map))
    }

    /// 创建mem_map，同时检测并恢复数据，防止数据异常而丢失
    async fn create_mem_map_and_check(factory: &IOHandlerFactory, vec_gen: &VecDeque<i64>) -> Result<MemMap> {
        // 当存在SUCCESS_FS时，代表Drop不正常，因此恢复最新的gen日志进行恢复
        if factory.has_gen(SUCCESS_FS_GEN)? {
            if let Some(gen) = vec_gen.back() {
                let reader = factory.create(*gen, IOType::MMapOnlyReader)?;
                return Ok(SkipMap::from_iter(
                    CommandPackage::from_read_to_unpack_vec(&reader).await?
                        .into_iter()
                        .map(|cmd| (cmd.get_key_clone(), cmd))
                ));
            }
        }
        Ok(SkipMap::new())
    }

    pub(crate) async fn log(&self, cmd: &CommandData) -> Result<()> {
        let inner = self.inner.read().await;
        let _ignore = CommandPackage::write(&inner.inner.1, cmd).await?;
        Ok(())
    }

    pub(crate) async fn flush(&self) -> Result<()> {
        self.inner.read().await
            .inner.1.flush().await
    }

    /// 弹出此日志的Gen并重新以新Gen进行日志记录
    pub(crate) async fn switch(&self) -> Result<i64> {
        let next_gen = self.config.create_gen();

        let buf_handler = self.factory.create(next_gen, IOType::Buf)?;
        let mut inner = self.inner.write().await;

        let current_gen = inner.inner.0;
        inner.inner.1.flush().await?;

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
        inner.inner = (next_gen, buf_handler);

        Ok(current_gen)
    }

    /// 通过Gen载入数据进行读取
    pub(crate) async fn load(&self, gen: i64) -> Result<Option<Vec<CommandData>>> {
        Ok(if self.factory.has_gen(gen)? {
            let reader = self.factory.create(gen, IOType::MMapOnlyReader)?;
            Some(CommandPackage::from_read_to_unpack_vec(&reader).await?)
        } else { None })
    }
}

impl Drop for WalLoader {
    // 使用drop释放SUCCESS_FS，以代表此次运行正常
    fn drop(&mut self) {
        if let Err(err) = self.factory.clean(SUCCESS_FS_GEN) {
            error!("[WALLoader][drop][error]: {err:?}")
        }
    }
}

#[test]
fn test_wal_load() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    tokio_test::block_on(async move {

        let config = Arc::new(Config::new(temp_dir.into_path()));

        let (wal, _) = WalLoader::reload_with_path(&config).await.unwrap();

        let data_1 = CommandData::set(b"kip_key_1".to_vec(), b"kip_value".to_vec());
        let data_2 = CommandData::set(b"kip_key_2".to_vec(), b"kip_value".to_vec());

        wal.log(&data_1).await.unwrap();
        wal.log(&data_2).await.unwrap();

        let gen = wal.switch().await.unwrap();

        drop(wal);

        let (wal, _) = WalLoader::reload_with_path(&config).await.unwrap();
        let option = wal.load(gen).await.unwrap();

        assert_eq!(option, Some(vec![data_1, data_2]));
    });
}

#[test]
fn test_wal_reload() {
    use tempfile::TempDir;
    use itertools::Itertools;

    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    tokio_test::block_on(async move {

        let config = Arc::new(Config::new(temp_dir.into_path()));

        let (wal_1, _) = WalLoader::reload_with_path(&config).await.unwrap();

        let data_1 = CommandData::set(b"kip_key_1".to_vec(), b"kip_value".to_vec());
        let data_2 = CommandData::set(b"kip_key_2".to_vec(), b"kip_value".to_vec());

        wal_1.log(&data_1).await.unwrap();
        wal_1.log(&data_2).await.unwrap();

        wal_1.flush().await.unwrap();
        // wal_1尚未drop时，则开始reload，模拟SUCCESS_FS未删除的情况(即停机异常)，触发数据恢复

        let (_wal, mem_map) = WalLoader::reload_with_path(&config).await.unwrap();

        let values = mem_map.values()
            .cloned()
            .collect_vec();

        assert_eq!(values, vec![data_1, data_2]);
    });
}

