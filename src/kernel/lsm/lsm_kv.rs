use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;
use async_trait::async_trait;
use chrono::Local;
use fslock::LockFile;
use itertools::Itertools;
use tokio::select;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;
use tokio::time::sleep;
use tracing::error;
use crate::KvsError;
use crate::kernel::{CommandData, DEFAULT_LOCK_FILE, KVStore, lock_or_time_out};
use crate::kernel::io::FileExtension;
use crate::kernel::lsm::{block, MemMap, MemTable};
use crate::kernel::lsm::compactor::{Compactor, CompactTask};
use crate::kernel::lsm::log::LogLoader;
use crate::kernel::lsm::mvcc::Transaction;
use crate::kernel::lsm::version::{Version, VersionStatus};
use crate::kernel::Result;

pub(crate) const DEFAULT_MINOR_THRESHOLD_WITH_LEN: usize = 2333;

pub(crate) const DEFAULT_SST_FILE_SIZE: usize = 2 * 1024 * 1024;

pub(crate) const DEFAULT_MAJOR_THRESHOLD_WITH_SST_SIZE: usize = 10;

pub(crate) const DEFAULT_MAJOR_SELECT_FILE_SIZE: usize = 3;

pub(crate) const DEFAULT_LEVEL_SST_MAGNIFICATION: usize = 10;

pub(crate) const DEFAULT_DESIRED_ERROR_PROB: f64 = 0.05;

pub(crate) const DEFAULT_BLOCK_CACHE_SIZE: usize = 3200;

pub(crate) const DEFAULT_TABLE_CACHE_SIZE: usize = 112;

pub(crate) const DEFAULT_WAL_THRESHOLD: usize = 20;

pub(crate) const DEFAULT_COMPACTOR_CHECK_TIME: u64 = 100;

pub(crate) const DEFAULT_WAL_PATH: &str = "wal";

pub(crate) const INIT_SEQ: i64 = i64::MIN;

static GEN_BUF: AtomicI64 = AtomicI64::new(0);

/// 基于LSM的KV Store存储内核
/// Leveled Compaction压缩算法
pub struct LsmStore {
    inner: Arc<StoreInner>,
    /// WAL载入器
    ///
    /// 用于异常停机时MemTable的恢复
    /// 同时当Level 0的SSTable异常时，可以尝试恢复
    /// `Config.wal_threshold`用于控制WalLoader的的SSTable数据日志个数
    /// 超出个数阈值时会清空最旧的一半日志
    pub(crate) wal: Arc<LogLoader>,
    /// 多进程文件锁
    /// 避免多进程进行数据读写
    lock_file: LockFile,
    /// Compactor 通信器
    compactor_tx: Sender<CompactTask>
}

pub(crate) struct StoreInner {
    /// MemTable
    /// https://zhuanlan.zhihu.com/p/79064869
    pub(crate) mem_table: MemTable,
    /// VersionVec
    /// 用于管理内部多版本状态
    pub(crate) ver_status: VersionStatus,
    /// LSM全局参数配置
    pub(crate) config: Config,
}

impl StoreInner {
    pub(crate) async fn new(config: Config) -> Result<(Self, LogLoader)> {
        GenBuffer::init();

        let (wal, option_success) = LogLoader::reload_with_check(
            config.clone(),
            DEFAULT_WAL_PATH,
            FileExtension::Log
        )?;

        let mem_map = match option_success {
            None => MemMap::new(),
            Some(vec_data) => {
                // Q: 为什么INIT_SEQ作为Seq id?
                // A: 因为此处是当存在有停机异常时使用wal恢复数据,此处也不存在有Version(VersionStatus的初始化在此代码之后)
                // 因此不会影响Version的读取顺序
                MemMap::from_iter(
                    // 倒序唯一化，保留最新的数据
                    vec_data.into_iter()
                        .rev()
                        .unique_by(CommandData::get_key_clone)
                        .map(|cmd| (cmd.get_key_clone(), (cmd, INIT_SEQ)))
                )
            }
        };

        // 初始化wal日志
        let ver_status = VersionStatus::load_with_path(config.clone().clone(), &wal).await?;

        let mem_table = MemTable::new(mem_map);

        Ok((StoreInner {
            mem_table,
            ver_status,
            config,
        }, wal))
    }
}

#[async_trait]
impl KVStore for LsmStore {
    #[inline]
    fn name() -> &'static str where Self: Sized {
        "LSMStore made in Kould"
    }

    #[inline]
    async fn open(path: impl Into<PathBuf> + Send) -> Result<Self> {
        LsmStore::open_with_config(Config::new(path.into())).await
    }

    #[inline]
    async fn flush(&self) -> Result<()> {
        self.flush_(false).await
    }

    #[inline]
    async fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.append_cmd_data(
            CommandData::set(key.to_vec(), value), true
        ).await
    }

    #[inline]
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(value) = self.mem_table().find(key) {
            return Ok(Some(value));
        }

        if let Some(value) = self.current_version().await
            .find_data_for_ss_tables(key).await?
        {
            return Ok(Some(value));
        }

        Ok(None)
    }

    #[inline]
    async fn remove(&self, key: &[u8]) -> Result<()> {
        match self.get(key).await? {
            Some(_) => {
                self.append_cmd_data(
                   CommandData::remove(key.to_vec()), true
                ).await
            }
            None => { Err(KvsError::KeyNotFound) }
        }
    }

    #[inline]
    async fn size_of_disk(&self) -> Result<u64> {
        Ok(self.current_version().await
            .get_size_of_disk())
    }

    #[inline]
    async fn len(&self) -> Result<usize> {
        Ok(self.current_version().await.get_len()
            + self.mem_table().len())
    }

    #[inline]
    async fn is_empty(&self) -> bool {
        self.current_version().await.is_empty()
            && self.mem_table().is_empty()
    }
}

impl Drop for LsmStore {
    #[inline]
    #[allow(clippy::expect_used)]
    fn drop(&mut self) {
        self.lock_file.unlock()
            .expect("LockFile unlock failed!");
    }
}

impl LsmStore {

    /// 追加数据
    async fn append_cmd_data(&self, cmd: CommandData, wal_write: bool) -> Result<()> {
        // Wal与MemTable双写
        if self.is_enable_wal() && wal_write {
            wal_put(
                &self.wal, &cmd, !self.is_async_wal()
            ).await;
        }

        self.mem_table().insert_data(cmd)?;
        Ok(())
    }

    fn is_enable_wal(&self) -> bool {
        self.config().wal_enable
    }

    fn is_async_wal(&self) -> bool {
        self.config().wal_async_put_enable
    }

    /// 使用Config进行LsmStore初始化
    #[inline]
    pub async fn open_with_config(config: Config) -> Result<Self> where Self: Sized {
        // 若lockfile的文件夹路径不存在则创建
        fs::create_dir_all(&config.dir_path)?;
        let lock_file = lock_or_time_out(
            &config.path().join(DEFAULT_LOCK_FILE)
        ).await?;

        let (inner, wal) = StoreInner::new(config.clone()).await?;

        let inner = Arc::new(inner);
        let wal = Arc::new(wal);

        let mut compactor = Compactor::new(
            Arc::clone(&inner),
            Arc::clone(&wal),
        );

        let check_time = config.compactor_check_time;
        let (task_tx, mut task_rx) = channel(20);

        let _ignore = tokio::spawn(async move {
            loop {
                let option_task: Option<CompactTask> = select! {
                    option_task = task_rx.recv() => Some(option_task.unwrap_or(CompactTask::Drop)),
                    _ = sleep(Duration::from_millis(check_time)) => None,
                };
                if let Some(CompactTask::Flush(resp_tx, enable_caching)) = option_task {
                    compactor.check_then_compaction(enable_caching, Some(resp_tx)).await;
                } else {
                    compactor.check_then_compaction(true, None).await;
                }
            }
        });

        Ok(LsmStore { inner, wal, lock_file, compactor_tx: task_tx })
    }

    pub(crate) fn config(&self) -> &Config {
        &self.inner.config
    }

    pub(crate) fn wal(&self) -> &Arc<LogLoader> {
        &self.wal
    }

    fn mem_table(&self) -> &MemTable {
        &self.inner.mem_table
    }

    async fn current_version(&self) -> Arc<Version> {
        self.inner.ver_status.current().await
    }

    #[allow(clippy::expect_used)]
    pub(crate) async fn flush_(&self, is_drop: bool) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.compactor_tx.send(CompactTask::Flush(tx, !is_drop)).await
            .expect("flush task send error!");

        self.wal.flush()?;
        rx.await.expect("flush task recv error!");

        Ok(())
    }

    /// 创建事务
    #[inline]
    pub async fn new_transaction(&self) -> Result<Transaction> {
        loop {
            if let Ok(inner) = self.mem_table().inner.read() {
                return Transaction::new(
                    self.config(),
                    self.current_version().await,
                    inner,
                    self.wal()
                );
            }
            std::hint::spin_loop();
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    /// 数据目录地址
    pub(crate) dir_path: PathBuf,
    /// WAL数量阈值
    pub(crate) wal_threshold: usize,
    /// SSTable文件大小
    pub(crate) sst_file_size: usize,
    /// Minor触发数据长度
    pub(crate) minor_threshold_with_len: usize,
    /// Major压缩触发阈值
    pub(crate) major_threshold_with_sst_size: usize,
    /// Major压缩选定文件数
    /// Major压缩时通过选定个别SSTable(即该配置项)进行下一级的SSTable选定，
    /// 并将确定范围的下一级SSTable再次对当前等级的SSTable进行范围判定，
    /// 找到最合理的上下级数据范围并压缩
    pub(crate) major_select_file_size: usize,
    /// 每级SSTable数量倍率
    pub(crate) level_sst_magnification: usize,
    /// 布隆过滤器 期望的错误概率
    pub(crate) desired_error_prob: f64,
    /// Block数据块缓存的数量
    /// 由于使用ShardingCache作为并行，以16为单位
    pub(crate) block_cache_size: usize,
    /// 用于缓存SSTable
    pub(crate) table_cache_size: usize,
    /// 开启wal日志写入
    /// 在开启状态时，会在SSTable文件读取失败时生效，避免数据丢失
    /// 不过在设备IO容易成为瓶颈，或使用多节点冗余写入时，建议关闭以提高写入性能
    pub(crate) wal_enable: bool,
    /// wal写入时开启异步写入
    /// 可以提高写入响应速度，但可能会导致wal日志在某种情况下并落盘慢于LSM内核而导致该条wal日志无效
    pub(crate) wal_async_put_enable: bool,
    /// Compactor循环检测时间
    /// 单位为毫秒
    pub(crate) compactor_check_time: u64,
    /// 每个Block之间的大小, 单位为B
    pub(crate) block_size: usize,
    /// DataBloc的前缀压缩Restart间隔
    pub(crate) data_restart_interval: usize,
    /// IndexBloc的前缀压缩Restart间隔
    pub(crate) index_restart_interval: usize,
}

impl Config {
    #[inline]
    pub fn new(path: impl Into<PathBuf> + Send) -> Config {
        Config {
            dir_path: path.into(),
            minor_threshold_with_len: DEFAULT_MINOR_THRESHOLD_WITH_LEN,
            wal_threshold: DEFAULT_WAL_THRESHOLD,
            sst_file_size: DEFAULT_SST_FILE_SIZE,
            major_threshold_with_sst_size: DEFAULT_MAJOR_THRESHOLD_WITH_SST_SIZE,
            major_select_file_size: DEFAULT_MAJOR_SELECT_FILE_SIZE,
            level_sst_magnification: DEFAULT_LEVEL_SST_MAGNIFICATION,
            desired_error_prob: DEFAULT_DESIRED_ERROR_PROB,
            block_cache_size: DEFAULT_BLOCK_CACHE_SIZE,
            table_cache_size: DEFAULT_TABLE_CACHE_SIZE,
            wal_enable: true,
            wal_async_put_enable: true,
            compactor_check_time: DEFAULT_COMPACTOR_CHECK_TIME,
            block_size: block::DEFAULT_BLOCK_SIZE,
            data_restart_interval: block::DEFAULT_DATA_RESTART_INTERVAL,
            index_restart_interval: block::DEFAULT_INDEX_RESTART_INTERVAL,
        }
    }

    pub(crate) fn path(&self) -> &PathBuf {
        &self.dir_path
    }

    #[inline]
    pub fn dir_path(mut self, dir_path: PathBuf) -> Self {
        self.dir_path = dir_path;
        self
    }

    #[inline]
    pub fn minor_threshold_with_len(mut self, minor_threshold_with_len: usize) -> Self {
        self.minor_threshold_with_len = minor_threshold_with_len;
        self
    }

    #[inline]
    pub fn block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self
    }

    #[inline]
    pub fn data_restart_interval(mut self, data_restart_interval: usize) -> Self {
        self.data_restart_interval = data_restart_interval;
        self
    }

    #[inline]
    pub fn index_restart_interval(mut self, index_restart_interval: usize) -> Self {
        self.index_restart_interval = index_restart_interval;
        self
    }

    #[inline]
    pub fn wal_threshold(mut self, wal_threshold: usize) -> Self {
        self.wal_threshold = wal_threshold;
        self
    }

    #[inline]
    pub fn sst_file_size(mut self, sst_file_size: usize) -> Self {
        self.sst_file_size = sst_file_size;
        self
    }

    #[inline]
    pub fn major_threshold_with_sst_size(mut self, major_threshold_with_sst_size: usize) -> Self {
        self.major_threshold_with_sst_size = major_threshold_with_sst_size;
        self
    }

    #[inline]
    pub fn major_select_file_size(mut self, major_select_file_size: usize) -> Self {
        self.major_select_file_size = major_select_file_size;
        self
    }

    #[inline]
    pub fn level_sst_magnification(mut self, level_sst_magnification: usize) -> Self {
        self.level_sst_magnification = level_sst_magnification;
        self
    }

    #[inline]
    pub fn desired_error_prob(mut self, desired_error_prob: f64) -> Self {
        self.desired_error_prob = desired_error_prob;
        self
    }

    #[inline]
    pub fn block_cache_size(mut self, cache_size: usize) -> Self {
        self.block_cache_size = cache_size;
        self
    }

    #[inline]
    pub fn table_cache_size(mut self, cache_size: usize) -> Self {
        self.table_cache_size = cache_size;
        self
    }

    #[inline]
    pub fn compactor_check_time(mut self, compactor_check_time: u64) -> Self {
        self.compactor_check_time = compactor_check_time;
        self
    }

    #[inline]
    pub fn wal_enable(mut self, wal_enable: bool) -> Self {
        self.wal_enable = wal_enable;
        self
    }

    #[inline]
    pub fn wal_async_put_enable(mut self, wal_async_put_enable: bool) -> Self {
        self.wal_async_put_enable = wal_async_put_enable;
        self
    }
}

pub(crate) struct GenBuffer {}

impl GenBuffer {
    /// 将GEN_BUF初始化至当前时间戳
    ///
    /// 与create_gen相对应，需要将GEN初始化为当前时间戳
    pub(crate) fn init() {
        GEN_BUF.store(Local::now().timestamp(), Ordering::Relaxed);
    }

    pub(crate) fn create_gen() -> i64 {
        GEN_BUF.fetch_add(1, Ordering::Relaxed)
    }
}

/// 日志记录，可选以Task类似的异步写数据或同步
pub(crate) async fn wal_put(wal: &Arc<LogLoader>, cmd: &CommandData, is_sync: bool) {
    if is_sync {
        wal_put_(wal, cmd);
    } else {
        let wal = Arc::clone(wal);
        let cmd_clone = cmd.clone();
        let _ignore = tokio::spawn(async move {
            wal_put_(&wal, &cmd_clone);
        });
    }

    fn wal_put_(wal: &Arc<LogLoader>, cmd: &CommandData) {
        if let Err(err) = wal.log(cmd) {
            error!("[LsmStore][wal_put][error happen]: {:?}", err);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;
    use std::time::{Duration, Instant};
    use itertools::Itertools;
    use tempfile::TempDir;
    use crate::kernel::lsm::lsm_kv::{Config, GenBuffer, LsmStore};
    use crate::kernel::{KVStore, Result};

    #[test]
    fn test_gen_create() {
        let i_1 = GenBuffer::create_gen();

        let i_2 = GenBuffer::create_gen();

        assert!(i_1 < i_2);

        GenBuffer::init();
        let i_3 = GenBuffer::create_gen();

        sleep(Duration::from_secs(1));

        GenBuffer::init();
        let i_4 = GenBuffer::create_gen();

        assert!(i_3 > i_2);
        assert!(i_4 > i_3);
    }

    #[test]
    fn test_lsm_major_compactor() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        tokio_test::block_on(async move {
            let times = 5000;

            let value = b"Stray birds of summer come to my window to sing and fly away.
            And yellow leaves of autumn, which have no songs, flutter and fall
            there with a sign.";

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

            let start = Instant::now();
            for i in 0..times {
                kv_store.set(&vec_kv[i].0, vec_kv[i].1.clone()).await?
            }
            println!("[set_for][Time: {:?}]", start.elapsed());

            kv_store.flush().await?;

            let start = Instant::now();
            for i in 0..times {
                assert_eq!(kv_store.get(&vec_kv[i].0).await?, Some(vec_kv[i].1.clone()));
            }
            println!("[get_for][Time: {:?}]", start.elapsed());
            kv_store.flush().await?;

            Ok(())
        })
    }
}