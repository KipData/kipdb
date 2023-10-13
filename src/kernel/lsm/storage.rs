use crate::kernel::io::IoType;
use crate::kernel::lsm::compactor::{CompactTask, Compactor};
use crate::kernel::lsm::mem_table::{KeyValue, MemTable};
use crate::kernel::lsm::mvcc::Transaction;
use crate::kernel::lsm::table::scope::Scope;
use crate::kernel::lsm::table::ss_table::block;
use crate::kernel::lsm::table::TableType;
use crate::kernel::lsm::trigger::TriggerType;
use crate::kernel::lsm::version::status::VersionStatus;
use crate::kernel::lsm::version::Version;
use crate::kernel::lsm::{query_and_compaction, version};
use crate::kernel::KernelResult;
use crate::kernel::{lock_or_time_out, Storage, DEFAULT_LOCK_FILE};
use crate::KernelError;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Local;
use fslock::LockFile;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;
use tracing::{error, info};

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
                   ▒▒▒▒▒";

pub(crate) const DEFAULT_MINOR_THRESHOLD_WITH_SIZE_WITH_MEM: usize = 2 * 1024 * 1024;

pub(crate) const DEFAULT_SST_FILE_SIZE: usize = 2 * 1024 * 1024;

pub(crate) const DEFAULT_MAJOR_THRESHOLD_WITH_SST_SIZE: usize = 10;

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
pub struct KipStorage {
    inner: Arc<StoreInner>,
    /// 多进程文件锁
    /// 避免多进程进行数据读写
    lock_file: LockFile,
    /// Compactor 通信器
    compactor_tx: Sender<CompactTask>,
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
    pub(crate) async fn new(config: Config) -> KernelResult<Self> {
        let mem_table = MemTable::new(&config)?;
        let ver_status =
            VersionStatus::load_with_path(config.clone(), mem_table.log_loader_clone())?;

        Ok(StoreInner {
            mem_table,
            ver_status,
            config,
        })
    }
}

#[async_trait]
impl Storage for KipStorage {
    #[inline]
    fn name() -> &'static str
    where
        Self: Sized,
    {
        "LSMStore made in Kould"
    }

    #[inline]
    async fn open(path: impl Into<PathBuf> + Send) -> KernelResult<Self> {
        KipStorage::open_with_config(Config::new(path.into())).await
    }

    #[inline]
    async fn flush(&self) -> KernelResult<()> {
        let (tx, rx) = oneshot::channel();

        self.compactor_tx.send(CompactTask::Flush(Some(tx))).await?;

        rx.await.map_err(|_| KernelError::ChannelClose)?;

        Ok(())
    }

    #[inline]
    async fn set(&self, key: Bytes, value: Bytes) -> KernelResult<()> {
        self.append_cmd_data((key, Some(value))).await
    }

    #[inline]
    async fn get(&self, key: &[u8]) -> KernelResult<Option<Bytes>> {
        if let Some((_, value)) = self.mem_table().find(key) {
            return Ok(value);
        }

        let version = self.current_version().await;
        if let Some((_, value)) = query_and_compaction(key, &version, &self.compactor_tx)? {
            return Ok(value);
        }

        Ok(None)
    }

    #[inline]
    async fn remove(&self, key: &[u8]) -> KernelResult<()> {
        match self.get(key).await? {
            Some(_) => {
                self.append_cmd_data((Bytes::copy_from_slice(key), None))
                    .await
            }
            None => Err(KernelError::KeyNotFound),
        }
    }

    #[inline]
    async fn size_of_disk(&self) -> KernelResult<u64> {
        Ok(self.current_version().await.size_of_disk())
    }

    #[inline]
    async fn len(&self) -> KernelResult<usize> {
        Ok(self.current_version().await.len() + self.mem_table().len())
    }

    #[inline]
    async fn is_empty(&self) -> bool {
        self.current_version().await.is_empty() && self.mem_table().is_empty()
    }
}

impl Drop for KipStorage {
    #[inline]
    #[allow(clippy::expect_used, clippy::let_underscore_must_use)]
    fn drop(&mut self) {
        self.lock_file.unlock().expect("LockFile unlock failed!");

        let _ = self.compactor_tx.try_send(CompactTask::Flush(None));
    }
}

impl KipStorage {
    /// 追加数据
    async fn append_cmd_data(&self, data: KeyValue) -> KernelResult<()> {
        if self.mem_table().insert_data(data)? {
            if let Err(TrySendError::Closed(_)) =
                self.compactor_tx.try_send(CompactTask::Flush(None))
            {
                return Err(KernelError::ChannelClose);
            }
        }

        Ok(())
    }

    /// 使用Config进行LsmStore初始化
    #[inline]
    pub async fn open_with_config(config: Config) -> KernelResult<Self>
    where
        Self: Sized,
    {
        info!("{} \nVersion: {}", BANNER, env!("CARGO_PKG_VERSION"));
        Gen::init();
        // 若lockfile的文件夹路径不存在则创建
        fs::create_dir_all(&config.dir_path)?;
        let lock_file = lock_or_time_out(&config.path().join(DEFAULT_LOCK_FILE)).await?;
        let inner = Arc::new(StoreInner::new(config.clone()).await?);
        let mut compactor = Compactor::new(Arc::clone(&inner));
        let (task_tx, mut task_rx) = channel(1);

        let _ignore = tokio::spawn(async move {
            while let Some(task) = task_rx.recv().await {
                match task {
                    CompactTask::Seek((scope, level)) => {
                        if let Err(err) =
                            compactor.major_compaction(level, scope, vec![], true).await
                        {
                            error!("[Compactor][manual compaction][error happen]: {:?}", err);
                        }
                    }
                    CompactTask::Flush(option_tx) => {
                        if let Err(err) = compactor.check_then_compaction(option_tx).await {
                            error!("[Compactor][compaction][error happen]: {:?}", err);
                        }
                    }
                }
            }
        });

        Ok(KipStorage {
            inner,
            lock_file,
            compactor_tx: task_tx,
        })
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
        let _ = self.mem_table().tx_count.fetch_add(1, Ordering::Release);

        Transaction {
            store_inner: Arc::clone(&self.inner),
            version: self.current_version().await,
            compactor_tx: self.compactor_tx.clone(),

            seq_id: Sequence::create(),
            write_buf: None,
        }
    }

    #[inline]
    pub async fn manual_compaction(
        &self,
        min: Bytes,
        max: Bytes,
        level: usize,
    ) -> KernelResult<()> {
        if min <= max {
            self.compactor_tx
                .send(CompactTask::Seek((Scope::from_range(0, min, max), level)))
                .await?;
        }

        Ok(())
    }

    #[allow(dead_code)]
    async fn flush_background(&self) -> KernelResult<()> {
        self.compactor_tx.send(CompactTask::Flush(None)).await?;

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    /// 数据目录地址
    pub(crate) dir_path: PathBuf,
    /// 各层级对应Table类型
    /// Tips: SkipTable仅可使用于Level 0之中，否则会因为Level 0外不支持WAL恢复而导致停机后丢失数据
    pub(crate) level_table_type: [TableType; 7],
    /// WAL数量阈值
    pub(crate) wal_threshold: usize,
    /// SSTable文件大小
    pub(crate) sst_file_size: usize,
    /// Minor触发器与阈值
    pub(crate) minor_trigger_with_threshold: (TriggerType, usize),
    /// Major压缩触发阈值
    pub(crate) major_threshold_with_sst_size: usize,
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
            level_table_type: [TableType::SortedString; 7],
            wal_threshold: DEFAULT_WAL_THRESHOLD,
            sst_file_size: DEFAULT_SST_FILE_SIZE,
            minor_trigger_with_threshold: (
                TriggerType::SizeOfMem,
                DEFAULT_MINOR_THRESHOLD_WITH_SIZE_WITH_MEM,
            ),
            major_threshold_with_sst_size: DEFAULT_MAJOR_THRESHOLD_WITH_SST_SIZE,
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
    pub fn enable_level_0_memorization(mut self) -> Self {
        self.level_table_type[0] = TableType::Skip;
        self
    }

    #[inline]
    pub fn dir_path(mut self, dir_path: PathBuf) -> Self {
        self.dir_path = dir_path;
        self
    }

    #[inline]
    pub fn level_table_type(mut self, level: usize, table_type: TableType) -> Self {
        self.level_table_type[level] = table_type;
        self
    }

    #[inline]
    pub fn minor_trigger_with_threshold(
        mut self,
        trigger_type: TriggerType,
        threshold: usize,
    ) -> Self {
        self.minor_trigger_with_threshold = (trigger_type, threshold);
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
    use crate::kernel::lsm::storage::{Gen, Sequence};
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_seq_create() {
        let i_1 = Sequence::create();

        let i_2 = Sequence::create();

        assert!(i_1 < i_2);
    }

    #[test]
    #[ignore]
    fn test_gen_create_1000() {
        for _ in 0..1000 {
            test_gen_create()
        }
    }

    fn test_gen_create() {
        Gen::init();

        let i_1 = Gen::create();
        let i_2 = Gen::create();

        assert!(i_1 < i_2);

        sleep(Duration::from_millis(2));
        Gen::init();
        let i_3 = Gen::create();

        sleep(Duration::from_millis(1));
        Gen::init();
        let i_4 = Gen::create();

        println!("{i_1}");
        println!("{i_2}");
        println!("{i_3}");
        println!("{i_4}");

        assert!(i_3 > i_2);
        assert!(i_4 > i_3);
    }
}
