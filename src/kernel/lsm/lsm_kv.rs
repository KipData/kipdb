use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Instant;
use async_trait::async_trait;
use fslock::LockFile;
use snowflake::SnowflakeIdBucket;
use tokio::sync::Mutex;
use tracing::{error, info};
use crate::{HashStore, KvsError};
use crate::kernel::{CommandData, CommandPackage, DEFAULT_LOCK_FILE, KVStore, lock_or_time_out};
use crate::kernel::lsm::{MemMap, MemTable};
use crate::kernel::lsm::compactor::Compactor;
use crate::kernel::lsm::version::VersionVec;
use crate::kernel::Result;

pub(crate) const DEFAULT_WAL_PATH: &str = "wal";

pub(crate) const DEFAULT_MINOR_THRESHOLD_WITH_DATA_OCCUPIED: u64 = 4 * 1024 * 1024;

pub(crate) const DEFAULT_SPARSE_INDEX_INTERVAL_BLOCK_SIZE: u64 = 4;

pub(crate) const DEFAULT_SST_FILE_SIZE: usize = 32 * 1024 * 1024;

pub(crate) const DEFAULT_MAJOR_THRESHOLD_WITH_SST_SIZE: usize = 10;

pub(crate) const DEFAULT_MAJOR_SELECT_FILE_SIZE: usize = 3;

pub(crate) const DEFAULT_MACHINE_ID: i32 = 1;

pub(crate) const DEFAULT_LEVEL_SST_MAGNIFICATION: usize = 10;

pub(crate) const DEFAULT_DESIRED_ERROR_PROB: f64 = 0.05;

pub(crate) const DEFAULT_BLOCK_CACHE_SIZE: usize = 3200;

pub(crate) const DEFAULT_TABLE_CACHE_SIZE: usize = 112;

pub(crate) const DEFAULT_WAL_COMPACTION_THRESHOLD: u64 = crate::kernel::hash_kv::DEFAULT_COMPACTION_THRESHOLD;

/// 基于LSM的KV Store存储内核
/// Leveled Compaction压缩算法
pub struct LsmStore {
    /// MemTable
    /// https://zhuanlan.zhihu.com/p/79064869
    mem_table: MemTable,
    /// VersionVec
    /// 用于管理内部多版本状态
    ver_vec: Arc<VersionVec>,
    /// LSM全局参数配置
    config: Arc<Config>,
    /// WAL存储器
    ///
    /// SSTable持久化前会将gen写入
    /// 持久化成功后则会删除gen，以此作为是否成功的依据
    ///
    /// 使用HashStore作为wal的原因：
    /// 1、操作简易，不需要重新写一个WAL
    /// 2、作Key-Value分离的准备，当作vLog
    /// 3、HashStore会丢弃超出大小的数据，保证最新数据不会丢失
    wal: Arc<HashStore>,
    /// 多进程文件锁
    /// 避免多进程进行数据读写
    lock_file: LockFile,
    /// 单线程压缩器
    compactor: Arc<Mutex<Compactor>>
}

#[async_trait]
impl KVStore for LsmStore {
    #[inline]
    fn name() -> &'static str where Self: Sized {
        "LsmStore made in Kould"
    }

    #[inline]
    async fn open(path: impl Into<PathBuf> + Send) -> Result<Self> {
        LsmStore::open_with_config(Config::default().dir_path(path.into())).await
    }

    #[inline]
    async fn flush(&self) -> Result<()> {
        self.flush_or_drop(false).await
    }

    #[inline]
    async fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.append_cmd_data(CommandData::Set { key: key.to_vec(), value: Arc::new(value) }, true).await
    }

    #[inline]
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(cmd_data) = self.mem_table.get_cmd_data(key).await {
            return Ok(cmd_data.get_value_clone());
        }

        if let Some(value) = self.ver_vec
            .current()
            .await
            .find_data_for_ss_tables(key)
            .await?
        {
            return Ok(Some(value));
        }
        // TODO: Wal对MVCC支持
        // // 尝试从Wal获取数据
        // if let Some(vec_cmd_u8) = self.wal.get(key).await? {
        //     let wal_cmd = CommandPackage::decode(&vec_cmd_u8)?;
        //     warn!("[Command][reload_from_wal]{:?}", wal_cmd);
        //     let option_value = wal_cmd.get_value_clone();
        //     self.append_cmd_data(wal_cmd, false).await?;
        //     return Ok(option_value);
        // }

        Ok(None)
    }

    #[inline]
    async fn remove(&self, key: &[u8]) -> Result<()> {
        match self.get(key).await? {
            Some(_) => { self.append_cmd_data(CommandData::Remove { key: key.to_vec() }, true).await }
            None => { Err(KvsError::KeyNotFound) }
        }
    }

    #[inline]
    async fn size_of_disk(&self) -> Result<u64> {
        Ok(self.ver_vec.current().await
            .get_size_of_disk()
            + self.wal.size_of_disk().await?)
    }

    #[inline]
    async fn len(&self) -> Result<usize> {
        Ok(self.ver_vec.current().await
            .get_len()
            + self.mem_table.mem_table_len().await)
    }

    #[inline]
    async fn is_empty(&self) -> bool {
        self.ver_vec.current().await
            .is_empty()
            && self.mem_table.mem_table_is_empty().await
    }
}

impl Drop for LsmStore {
    fn drop(&mut self) {
        // 自旋获取Compactor的锁进行Drop
        // tip: tokio的Mutex的lock方法是async的
        loop {
            if let Some(_compactor) = self.compactor.try_lock().ok() {
                self.lock_file.unlock()
                    .expect("LockFile unlock failed!");
                break
            }
        }
    }
}

impl LsmStore {

    /// 追加数据
    async fn append_cmd_data(&self, cmd: CommandData, wal_write: bool) -> Result<()> {
        let mem_table = &self.mem_table;
        let threshold_size = self.config.minor_threshold_with_data_size;

        let key = cmd.get_key();
        // TODO: Wal的MVCC支持
        // Wal与MemTable双写
        if self.config.wal_enable && wal_write {
            wal_put(
                &self.wal,
                key.clone(),
                CommandPackage::encode(&cmd)?,
                !self.config.wal_async_put_enable
            ).await;
        }
        mem_table.insert_data(key.clone(), cmd).await;

        if mem_table.is_threshold_exceeded_minor(threshold_size).await {
            self.minor_compaction().await?;
        }

        Ok(())
    }

    /// 使用Config进行LsmStore初始化
    #[inline]
    pub async fn open_with_config(config: Config) -> Result<Self> where Self: Sized {
        let config = Arc::new(config);
        let path = config.dir_path.clone();
        let wal_compaction_threshold = config.wal_compaction_threshold;

        let mut mem_map = MemMap::new();

        // 若lockfile的文件夹路径不存在则创建
        fs::create_dir_all(&path)?;
        let lock_file = lock_or_time_out(&path.join(DEFAULT_LOCK_FILE)).await?;
        // 初始化wal日志
        let wal = Arc::new(
            HashStore::open_with_compaction_threshold(
                &path.join(DEFAULT_WAL_PATH),
                wal_compaction_threshold
            ).await?
        );
        let ver_vec = Arc::new(
            VersionVec::load_with_path(&config, &mut mem_map, &wal).await?
        );

        let compactor = Arc::new(
            Mutex::new(
                Compactor::new(
                    Arc::clone(&ver_vec),
                    Arc::clone(&config),
                    ver_vec.get_sst_factory(),
                    Arc::clone(&wal)
                )
            )
        );

        Ok(LsmStore {
            mem_table: MemTable::new(mem_map),
            ver_vec,
            config,
            wal,
            lock_file,
            compactor,
        })
    }

    /// 异步持久化immutable_table为SSTable
    #[inline]
    #[allow(clippy::unwrap_used)]
    pub async fn minor_compaction(&self) -> Result<()> {
        let (keys, values) = self.mem_table.table_swap().await;
        if !keys.is_empty() && !values.is_empty() {
            let compactor = Arc::clone(&self.compactor);

            let _ignore = tokio::spawn(async move {
                let start = Instant::now();
                if let Err(err) = compactor
                    .lock().await
                    .minor_compaction(keys, values, false).await
                {
                    error!("[LsmStore][minor_compaction][error happen]: {:?}", err);
                }
                info!("[LsmStore][Compaction Drop][Time: {:?}]", start.elapsed());
            });
        }
        Ok(())
    }

    /// 同步持久化immutable_table为SSTable
    #[inline]
    pub async fn minor_compaction_sync(&self, is_drop: bool) -> Result<()> {
        let (keys, values) = self.mem_table.table_swap().await;

        self.compactor
            .lock()
            .await
            .minor_compaction(keys, values, is_drop)
            .await
    }

    /// 同步进行SSTable基于Level的层级压缩
    #[inline]
    pub async fn major_compaction_sync(&self, level: usize) -> Result<()> {
        self.compactor
            .lock()
            .await
            .major_compaction(level, vec![])
            .await
    }

    /// 通过CommandData的引用解包并克隆出value值
    #[allow(dead_code)]
    fn value_unpack(cmd_data: &CommandData) -> Option<Vec<u8>> {
        cmd_data.get_value_clone()
    }

    #[allow(dead_code)]
    pub(crate) fn ver_vec(&self) -> &Arc<VersionVec> {
        &self.ver_vec
    }
    #[allow(dead_code)]
    pub(crate) fn config(&self) -> &Arc<Config> {
        &self.config
    }
    #[allow(dead_code)]
    pub(crate) fn wal(&self) -> &Arc<HashStore> {
        &self.wal
    }

    pub(crate) async fn flush_or_drop(&self, is_drop: bool) -> Result<()> {
        self.wal.flush().await?;
        if !self.mem_table.mem_table_is_empty().await {
            self.minor_compaction_sync(is_drop).await?;
        }
        Ok(())
    }
}

pub(crate) struct CommandCodec;

impl CommandCodec {
    pub(crate) fn encode_gen(gen: i64) -> Result<Vec<u8>> {
        Ok(bincode::serialize(&gen)?)
    }

    #[allow(dead_code)]
    pub(crate) fn decode_gen(key: &[u8]) -> Result<i64> {
        Ok(bincode::deserialize(key)?)
    }

    pub(crate) fn encode_keys(value: &Vec<Vec<u8>>) -> Result<Vec<u8>> {
        Ok(bincode::serialize(value)?)
    }

    pub(crate) fn decode_keys(vec_u8: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(bincode::deserialize(vec_u8)?)
    }
}
#[derive(Debug)]
pub struct Config {
    /// 数据目录地址
    pub(crate) dir_path: PathBuf,
    /// WAL持久化阈值
    pub(crate) wal_compaction_threshold: u64,
    /// 稀疏索引间间隔的Block(4K字节大小)数量
    pub(crate) sparse_index_interval_block_size: u64,
    /// SSTable文件大小
    pub(crate) sst_file_size: usize,
    /// 持久化阈值(单位: 字节)
    pub(crate) minor_threshold_with_data_size: u64,
    /// Major压缩触发阈值
    pub(crate) major_threshold_with_sst_size: usize,
    /// Major压缩选定文件数
    /// Major压缩时通过选定个别SSTable(即该配置项)进行下一级的SSTable选定，
    /// 并将确定范围的下一级SSTable再次对当前等级的SSTable进行范围判定，
    /// 找到最合理的上下级数据范围并压缩
    pub(crate) major_select_file_size: usize,
    /// 节点Id
    pub(crate) node_id: i32,
    /// 每级SSTable数量倍率
    pub(crate) level_sst_magnification: usize,
    /// 用于ID生成的原子缓冲
    /// 避免极端情况下，SSTable创建重复问题并保持时间有序性
    pub(crate) buffer_i32: AtomicI32,
    /// 布隆过滤器 期望的错误概率
    pub(crate) desired_error_prob: f64,
    /// 数据库全局Position段数据缓存的数量
    /// 一个size大约为4kb(可能更少)
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
    pub(crate) wal_async_put_enable: bool
}

impl Config {

    #[inline]
    pub fn dir_path(mut self, dir_path: PathBuf) -> Self {
        self.dir_path = dir_path;
        self
    }

    #[inline]
    pub fn minor_threshold_with_data_size(mut self, minor_threshold_with_data_size: u64) -> Self {
        self.minor_threshold_with_data_size = minor_threshold_with_data_size;
        self
    }

    #[inline]
    pub fn wal_compaction_threshold(mut self, wal_compaction_threshold: u64) -> Self {
        self.wal_compaction_threshold = wal_compaction_threshold;
        self
    }

    #[inline]
    pub fn sparse_index_interval_block_size(mut self, sparse_index_interval_block_size: u64) -> Self {
        self.sparse_index_interval_block_size = sparse_index_interval_block_size;
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
    pub fn node_id(mut self, node_id: i32) -> Self {
        self.node_id = node_id;
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
    pub fn create_gen(&self) -> i64 {
        SnowflakeIdBucket::new(self.node_id, self.buffer_i32
            .fetch_add(1, Ordering::SeqCst)).get_id()
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

impl Default for Config {
    #[inline]
    fn default() -> Self {
        Self {
            dir_path: DEFAULT_WAL_PATH.into(),
            minor_threshold_with_data_size: DEFAULT_MINOR_THRESHOLD_WITH_DATA_OCCUPIED,
            wal_compaction_threshold: DEFAULT_WAL_COMPACTION_THRESHOLD,
            sparse_index_interval_block_size: DEFAULT_SPARSE_INDEX_INTERVAL_BLOCK_SIZE,
            sst_file_size: DEFAULT_SST_FILE_SIZE,
            major_threshold_with_sst_size: DEFAULT_MAJOR_THRESHOLD_WITH_SST_SIZE,
            major_select_file_size: DEFAULT_MAJOR_SELECT_FILE_SIZE,
            node_id: DEFAULT_MACHINE_ID,
            level_sst_magnification: DEFAULT_LEVEL_SST_MAGNIFICATION,
            buffer_i32: AtomicI32::new(0),
            desired_error_prob: DEFAULT_DESIRED_ERROR_PROB,
            block_cache_size: DEFAULT_BLOCK_CACHE_SIZE,
            table_cache_size: DEFAULT_TABLE_CACHE_SIZE,
            wal_enable: true,
            wal_async_put_enable: true,
        }
    }
}

/// 以Task类似的异步写数据，避免影响数据写入性能
/// 当然，LevelDB的话虽然wal写入会提供是否同步的选项，此处先简化优先使用异步
pub(crate) async fn wal_put(wal: &Arc<HashStore>, key: Vec<u8>, value: Vec<u8>, is_sync: bool) {
    let wal = Arc::clone(wal);
    let wal_closure = async move {
        if let Err(err) = wal.set(&key, value).await {
            error!("[LsmStore][wal_put][error happen]: {:?}", err);
        }
    };
    if is_sync {
        wal_closure.await;
    } else {
        let _ignore = tokio::spawn(wal_closure);
    }

}

#[test]
fn test_lsm_major_compactor() -> Result<()> {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    tokio_test::block_on(async move {
        let config = Config::default()
            .dir_path(temp_dir.into_path())
            .wal_enable(false);
        let kv_store = LsmStore::open_with_config(config).await?;
        let mut vec_key: Vec<Vec<u8>> = Vec::new();

        let start = Instant::now();
        for i in 0..300000 {
            let vec_u8 = rmp_serde::to_vec(&i).unwrap();
            kv_store.set(&vec_u8, vec_u8.clone()).await?;
            vec_key.push(vec_u8);
        }
        kv_store.set(&vec_key[0], vec![b'k']).await?;
        println!("[set_for][Time: {:?}]", start.elapsed());

        let start = Instant::now();
        let _ignore = kv_store.get(&rmp_serde::to_vec(&0).unwrap()).await?.unwrap();
        println!("[compaction_for][Time: {:?}]", start.elapsed());

        kv_store.flush().await?;
        let start = Instant::now();
        assert_eq!(kv_store.get(&vec_key[0]).await?, Some(vec![b'k']));
        for i in 1..300000 {
            assert_eq!(kv_store.get(&vec_key[i]).await?, Some(vec_key[i].clone()));
        }
        println!("[get_for][Time: {:?}]", start.elapsed());
        kv_store.flush().await?;
        Ok(())
    })
}