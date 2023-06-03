use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Local;
use fslock::LockFile;
use parking_lot::MutexGuard;
use skiplist::SkipMap;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::oneshot;
use tracing::{error, info};
use crate::kernel::{DEFAULT_LOCK_FILE, KVStore, lock_or_time_out};
use crate::kernel::io::IoType;
use crate::kernel::lsm::{block, is_exceeded_then_minor, version};
use crate::kernel::lsm::compactor::{Compactor, CompactTask};
use crate::kernel::lsm::iterator::full_iter::FullIter;
use crate::kernel::lsm::mem_table::{KeyValue, MemTable, TableInner};
use crate::kernel::lsm::mvcc::Transaction;
use crate::kernel::lsm::version::{Version, VersionStatus};
use crate::kernel::Result;
use crate::KernelError;

pub(crate) const BANNER: &str = "
█████   ████  ███            ██████████   ███████████
▒▒███   ███▒  ▒▒▒            ▒▒███▒▒▒▒███ ▒▒███▒▒▒▒▒███
 ▒███  ███    ████  ████████  ▒███   ▒▒███ ▒███    ▒███
 ▒███████    ▒▒███ ▒▒███▒▒███ ▒███    ▒███ ▒██████████
 ▒███▒▒███    ▒███  ▒███ ▒███ ▒███    ▒███ ▒███▒▒▒▒▒███
 ▒███ ▒▒███   ▒███  ▒███ ▒███ ▒███    ███  ▒███    ▒███
 █████ ▒▒████ █████ ▒███████  ██████████   ███████████
▒▒▒▒▒   ▒▒▒▒ ▒▒▒▒▒  ▒███▒▒▒  ▒▒▒▒▒▒▒▒▒▒   ▒▒▒▒▒▒▒▒▒▒▒
                    ▒███
                    █████
                   ▒▒▒▒▒
Version: 0.1.0-beta.1";

pub(crate) const DEFAULT_MINOR_THRESHOLD_WITH_LEN: usize = 2333;

pub(crate) const DEFAULT_SST_FILE_SIZE: usize = 2 * 1024 * 1024;

pub(crate) const DEFAULT_MAJOR_THRESHOLD_WITH_SST_SIZE: usize = 10;

pub(crate) const DEFAULT_MAJOR_SELECT_FILE_SIZE: usize = 3;

pub(crate) const DEFAULT_LEVEL_SST_MAGNIFICATION: usize = 10;

pub(crate) const DEFAULT_DESIRED_ERROR_PROB: f64 = 0.05;

pub(crate) const DEFAULT_BLOCK_CACHE_SIZE: usize = 3200;

pub(crate) const DEFAULT_TABLE_CACHE_SIZE: usize = 1024;

pub(crate) const DEFAULT_WAL_THRESHOLD: usize = 20;

pub(crate) const DEFAULT_WAL_IO_TYPE: IoType = IoType::Buf;

static SEQ_COUNT: AtomicI64 = AtomicI64::new(1);

static GEN_BUF: AtomicI64 = AtomicI64::new(0);

/// 基于LSM的KV Store存储内核
/// Leveled Compaction压缩算法
pub struct LsmStore {
    inner: Arc<StoreInner>,
    /// 多进程文件锁
    /// 避免多进程进行数据读写
    lock_file: LockFile,
    /// Compactor 通信器
    compactor_tx: UnboundedSender<CompactTask>
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
    pub(crate) async fn new(config: Config) -> Result<Self> {
        let mem_table = MemTable::new(&config)?;

        // 初始化wal日志
        let ver_status = VersionStatus::load_with_path(
            config.clone(),
            mem_table.log_loader_clone()
        )?;

        Ok(StoreInner {
            mem_table,
            ver_status,
            config,
        })
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
        let (tx, rx) = oneshot::channel();

        self.compactor_tx.send(CompactTask::Flush(Some(tx)))?;

        rx.await.map_err(|_| KernelError::ChannelClose)?;

        Ok(())
    }

    #[inline]
    async fn set(&self, key: &[u8], value: Bytes) -> Result<()> {
        self.append_cmd_data(
            (Bytes::copy_from_slice(key), Some(value))
        ).await
    }

    #[inline]
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(value) = self.mem_table().find(key) {
            return Ok(Some(value));
        }

        if let Some(value) = self.current_version().await
            .find_data_for_ss_tables(key)?
        {
            return Ok(Some(value));
        }

        Ok(None)
    }

    #[inline]
    async fn remove(&self, key: &[u8]) -> Result<()> {
        match self.get(key).await? {
            Some(_) => self.append_cmd_data((Bytes::copy_from_slice(key), None)).await,
            None => Err(KernelError::KeyNotFound)
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
    async fn append_cmd_data(&self, data: KeyValue) -> Result<()> {
        let data_len = self.mem_table().insert_data(data)?;

        is_exceeded_then_minor(
            data_len,
            &self.compactor_tx,
            self.config()
        )?;

        Ok(())
    }

    /// 使用Config进行LsmStore初始化
    #[inline]
    pub async fn open_with_config(config: Config) -> Result<Self> where Self: Sized {
        info!("{}", BANNER);
        Gen::init();
        // 若lockfile的文件夹路径不存在则创建
        fs::create_dir_all(&config.dir_path)?;
        let lock_file = lock_or_time_out(
            &config.path().join(DEFAULT_LOCK_FILE)
        ).await?;

        let inner = Arc::new(StoreInner::new(config.clone()).await?);

        let mut compactor = Compactor::new(
            Arc::clone(&inner),
        );

        let (task_tx, mut task_rx) = unbounded_channel();

        let _ignore = tokio::spawn(async move {
            while let Some(CompactTask::Flush(option_tx)) = task_rx.recv().await {
                if let Err(err) = compactor.check_then_compaction(option_tx).await {
                    error!("[Compactor][compaction][error happen]: {:?}", err);
                }
            }
        });

        Ok(LsmStore { inner, lock_file, compactor_tx: task_tx })
    }

    pub(crate) fn config(&self) -> &Config {
        &self.inner.config
    }

    fn mem_table(&self) -> &MemTable {
        &self.inner.mem_table
    }

    pub(crate) async fn current_version(&self) -> Arc<Version> {
        self.inner.ver_status.current().await
    }

    /// 创建事务
    #[inline]
    pub async fn new_transaction(&self) -> Transaction {
        let _ = self.mem_table().tx_count
            .fetch_add(1, Ordering::Release);

        Transaction {
            store_inner: Arc::clone(&self.inner),
            version: self.current_version().await,
            compactor_tx: self.compactor_tx.clone(),

            seq_id: Sequence::create(),
            writer_buf: SkipMap::new(),
        }
    }

    #[inline]
    pub async fn guard(&self) -> Result<Guard> {
        let version = self.current_version().await;

        Ok(Guard {
            _inner: self.mem_table().inner_with_lock(),
            _version: version
        })
    }
}

pub struct Guard<'a> {
    _inner: MutexGuard<'a, TableInner>,
    _version: Arc<Version>
}

impl<'a> Guard<'a> {
    #[inline]
    pub fn iter(&'a self) -> Result<FullIter<'a>> {
        FullIter::new(&self._inner, &self._version)
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
    /// WAL写入类型
    /// 直写: Direct
    /// 异步: Buf、Mmap
    pub(crate) wal_io_type: IoType,
    /// 每个Block之间的大小, 单位为B
    pub(crate) block_size: usize,
    /// DataBloc的前缀压缩Restart间隔
    pub(crate) data_restart_interval: usize,
    /// IndexBloc的前缀压缩Restart间隔
    pub(crate) index_restart_interval: usize,
    /// VersionLog触发快照化的运行时计量阈值
    pub(crate) ver_log_snapshot_threshold: usize,
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
            wal_io_type: DEFAULT_WAL_IO_TYPE,
            block_size: block::DEFAULT_BLOCK_SIZE,
            data_restart_interval: block::DEFAULT_DATA_RESTART_INTERVAL,
            index_restart_interval: block::DEFAULT_INDEX_RESTART_INTERVAL,
            ver_log_snapshot_threshold: version::DEFAULT_VERSION_LOG_THRESHOLD,
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
    pub fn wal_io_type(mut self, wal_io_type: IoType) -> Self {
        self.wal_io_type = wal_io_type;
        self
    }

    #[inline]
    pub fn ver_log_snapshot_threshold(mut self, ver_log_snapshot_threshold: usize) -> Self {
        self.ver_log_snapshot_threshold = ver_log_snapshot_threshold;
        self
    }
}

/// 插入时Sequence id生成器
///
/// 与`Gen`比较大的不同在于
/// - `Sequence`随着每次重启都会重置为0，而seq上限很高，可以在此次运行时生成有序且不相同的id
/// - `Gen`以时间戳为基础，每次保证每次重启都保证时间有序，但不足以作为Seq的生成，因为上限较低
pub(crate) struct Sequence {}

pub(crate) struct Gen {}

impl Sequence {
    pub(crate) fn create() -> i64 {
        SEQ_COUNT.fetch_add(1, Ordering::Relaxed)
    }
}

impl Gen {
    /// 将GEN_BUF初始化至当前时间戳
    ///
    /// 与create_gen相对应，需要将GEN初始化为当前时间戳
    pub(crate) fn init() {
        GEN_BUF.store(Local::now().timestamp_millis(), Ordering::Relaxed);
    }

    pub(crate) fn create() -> i64 {
        GEN_BUF.fetch_add(1, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;
    use std::time::{Duration, Instant};
    use bytes::Bytes;
    use itertools::Itertools;
    use tempfile::TempDir;
    use crate::kernel::lsm::lsm_kv::{Config, Gen, LsmStore, Sequence};
    use crate::kernel::{KVStore, Result};

    #[test]
    fn test_seq_create() {
        let i_1 = Sequence::create();

        let i_2 = Sequence::create();

        assert!(i_1 < i_2);
    }

    #[test]
    fn test_gen_create() {
        let i_1 = Gen::create();

        let i_2 = Gen::create();

        assert!(i_1 < i_2);

        sleep(Duration::from_millis(10));
        Gen::init();
        let i_3 = Gen::create();

        sleep(Duration::from_millis(10));
        Gen::init();
        let i_4 = Gen::create();

        println!("{i_1}");
        println!("{i_2}");
        println!("{i_3}");
        println!("{i_4}");

        assert!(i_3 > i_2);
        assert!(i_4 > i_3 + 1);
    }

    #[test]
    fn test_lsm_major_compactor() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        tokio_test::block_on(async move {
            let times = 10000;

            let value = b"Stray birds of summer come to my window to sing and fly away.
            And yellow leaves of autumn, which have no songs, flutter and fall
            there with a sign.";

            let config = Config::new(temp_dir.path().to_str().unwrap())
                .minor_threshold_with_len(1000)
                .major_threshold_with_sst_size(4);
            let kv_store = LsmStore::open_with_config(config).await?;
            let mut vec_kv = Vec::new();

            for i in 0..times {
                let vec_u8 = bincode::serialize(&i)?;
                vec_kv.push((
                    Bytes::from(vec_u8.clone()),
                    Bytes::from(vec_u8.into_iter()
                        .chain(value.to_vec())
                        .collect_vec())
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