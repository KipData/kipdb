use std::collections::{BTreeMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Instant;
use async_trait::async_trait;
use snowflake::SnowflakeIdBucket;
use tokio::sync::{Mutex, oneshot, RwLock};
use tokio::sync::oneshot::Sender;
use tracing::{error, info, warn};
use crate::{HashStore, KvsError};
use crate::kernel::{CommandData, CommandPackage, KVStore, sorted_gen_list};
use crate::kernel::io_handler::IOHandlerFactory;
use crate::kernel::lsm::{Manifest, MemMap, MemTable};
use crate::kernel::lsm::compactor::Compactor;
use crate::kernel::lsm::ss_table::SsTable;
use crate::kernel::Result;

pub(crate) type LevelSlice = [Vec<i64>; 7];

pub(crate) type SsTableMap = BTreeMap<i64, SsTable>;

pub(crate) const DEFAULT_WAL_PATH: &str = "wal";

pub(crate) const DEFAULT_MINOR_THRESHOLD_WITH_DATA_OCCUPIED: u64 = 5 * 1024 * 1024;

pub(crate) const DEFAULT_PART_SIZE: u64 = 64;

pub(crate) const DEFAULT_SST_FILE_SIZE: usize = 32 * 1024 * 1024;

pub(crate) const DEFAULT_MAJOR_THRESHOLD_WITH_SST_SIZE: usize = 10;

pub(crate) const DEFAULT_MAJOR_SELECT_FILE_SIZE: usize = 3;

pub(crate) const DEFAULT_MACHINE_ID: i32 = 1;

pub(crate) const DEFAULT_LEVEL_SST_MAGNIFICATION: usize = 10;

pub(crate) const DEFAULT_DESIRED_ERROR_PROB: f64 = 0.05;

pub(crate) const DEFAULT_CACHE_SIZE: usize = 23333;

pub(crate) const DEFAULT_WAL_COMPACTION_THRESHOLD: u64 = crate::kernel::hash_kv::DEFAULT_COMPACTION_THRESHOLD;

pub struct LsmStore {
    /// MemTable
    /// https://zhuanlan.zhihu.com/p/79064869
    mem_table: MemTable,
    /// Manifest
    /// 用于管理内部SSTable的Gen映射以及Level分级结构
    /// TODO：多版本持久化
    manifest: Arc<RwLock<Manifest>>,
    /// LSM全局参数配置
    config: Arc<Config>,
    io_handler_factory: Arc<IOHandlerFactory>,
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
    /// 异步任务阻塞监听器
    vec_rev: Mutex<Vec<oneshot::Receiver<()>>>,
}

#[async_trait]
impl KVStore for LsmStore {
    fn name() -> &'static str where Self: Sized {
        "LsmStore made in Kould"
    }

    async fn open(path: impl Into<PathBuf> + Send) -> Result<Self> {
        LsmStore::open_with_config(Config::new().dir_path(path.into())).await
    }

    async fn flush(&self) -> Result<()> {
        self.wal.flush().await?;
        if !self.mem_table.mem_table_is_empty().await {
            self.minor_compaction().await?;
        }
        self.wait_for_compression_down().await?;

        Ok(())
    }

    async fn set(&self, key: &Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.append_cmd_data(CommandData::Set { key: key.clone(), value }, true).await
    }

    async fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>> {
        if let Some(CommandData::Set { value, ..}) = self.mem_table.get_cmd_data(key).await {
            return Ok(Some(value));
        }
        // 读取前等待压缩完毕
        // 相对来说，消耗较小
        // 当压缩时长高时，说明数据量非常大
        // 此时直接去获取的话可能会既获取不到数据，也花费大量时间
        self.wait_for_compression_down().await?;

        if let Some(value) = self.manifest.write().await
            .get_data_for_ss_tables(key).await? {
            return Ok(Some(value));
        }
        // 尝试从Wal获取数据
        if let Some(vec_cmd_u8) = self.wal.get(key).await? {
            let wal_cmd = CommandPackage::decode(&vec_cmd_u8)?;
            warn!("[Command][reload_from_wal]{:?}", wal_cmd);
            let option_value = wal_cmd.get_value_clone();
            self.append_cmd_data(wal_cmd, false).await?;
            return Ok(option_value);
        }

        Ok(None)
    }

    async fn remove(&self, key: &Vec<u8>) -> Result<()> {
        match self.get(key).await? {
            Some(_) => { self.append_cmd_data(CommandData::Remove { key: key.clone() }, true).await }
            None => { Err(KvsError::KeyNotFound) }
        }
    }

    async fn size_of_disk(&self) -> Result<u64> {
        Ok(self.manifest.read().await
            .ss_tables_map.iter()
            .map(|(_, ss_table)| ss_table.get_size_of_disk())
            .sum::<u64>() + self.wal.size_of_disk().await?)
    }

    async fn len(&self) -> Result<usize> {
        Ok(self.manifest.read().await
            .ss_tables_map.iter()
            .map(|(_, ss_table)| ss_table.len())
            .sum::<usize>() + self.wal.len().await?)
    }
}

impl LsmStore {

    /// 追加数据
    async fn append_cmd_data(&self, cmd: CommandData, wal_write: bool) -> Result<()> {
        let mem_table = &self.mem_table;
        let threshold_size = self.config.minor_threshold_with_data_size;

        let key = cmd.get_key();
        // Wal与MemTable双写
        if self.config.wal_enable && wal_write {
            wal_put(&self.wal, key.clone(), CommandPackage::encode(&cmd)?);
        }
        mem_table.insert_data(key.clone(), cmd).await;

        if mem_table.is_threshold_exceeded_minor(threshold_size).await {
            self.minor_compaction().await?;
        }

        Ok(())
    }

    /// 使用Config进行LsmStore初始化
    pub async fn open_with_config(config: Config) -> Result<Self> where Self: Sized {
        let path = config.dir_path.clone();
        let wal_compaction_threshold = config.wal_compaction_threshold;

        let mut mem_map = MemMap::new();
        let mut ss_tables = BTreeMap::new();

        let mut wal_path = path.clone();
        wal_path.push(DEFAULT_WAL_PATH);

        // 初始化wal日志
        let wal = Arc::new(HashStore::open_with_compaction_threshold(&wal_path, wal_compaction_threshold).await?);
        let io_handler_factory = Arc::new(IOHandlerFactory::new(path.clone()));
        // 持久化数据恢复
        // 倒叙遍历，从最新的数据开始恢复
        for gen in sorted_gen_list(&path).await?.iter().rev() {
            let io_handler = io_handler_factory.create(*gen)?;
            // 尝试初始化Table
            match SsTable::restore_from_file(io_handler).await {
                Ok(ss_table) => {
                    // 初始化成功时直接传入SSTable的索引中
                    ss_tables.insert(*gen, ss_table);
                }
                Err(err) => {
                    error!("[LsmKVStore][Load SSTable][Error]: {:?}", err);
                    // 是否删除可能还是得根据用户选择
                    // io_handler_factory.clean(*gen)?;
                    // 从wal将有问题的ss_table恢复到mem_table中
                    Self::reload_for_wal(&mut mem_map, &wal, *gen).await?;
                }
            }
        }
        // 构建SSTable信息集
        let manifest = Manifest::new(ss_tables, Arc::new(path.clone()), config.cache_size)?;

        Ok(LsmStore {
            mem_table: MemTable::new(mem_map),
            manifest: Arc::new(RwLock::new(manifest)),
            config: Arc::new(config),
            io_handler_factory,
            wal,
            vec_rev: Mutex::new(Vec::new())
        })
    }

    /// 从Wal恢复SSTable数据
    /// 初始化失败时遍历wal的key并检测key是否为gen
    async fn reload_for_wal(mem_table: &mut MemMap, wal: &HashStore, gen: i64) -> Result<()>{
        // 将SSTable持久化失败前预存入的指令键集合从wal中获取
        // 随后将每一条指令键对应的指令恢复到mem_table中
        warn!("[SsTable: {}][reload_from_wal]", gen);
        let key_gen = CommandCodec::encode_gen(gen)?;
        if let Some(key_cmd_u8) = wal.get(&key_gen).await? {
            for key in CommandCodec::decode_keys(&key_cmd_u8)? {
                if let Some(cmd_data_u8) = wal.get(&key).await? {
                    let cmd_data = CommandPackage::decode(&cmd_data_u8)?;

                    mem_table.insert(cmd_data.get_key_clone(), cmd_data);
                } else {
                    return Err(KvsError::WalLoadError);
                }
            };
        } else {
            return Err(KvsError::WalLoadError);
        }
        Ok(())
    }

    /// 异步持久化immutable_table为SSTable
    pub async fn minor_compaction(&self) -> Result<()> {
        let (keys, values) = self.mem_table.table_swap().await;
        if !keys.is_empty() && !values.is_empty() {
            let compactor = Compactor::from_lsm_kv(self);
            let sender = self.live_tag().await;

            tokio::spawn(async move {
                let start = Instant::now();
                // 目前minor触发major时是同步进行的，所以此处对live_tag是在此方法体保持存活
                if let Err(err) = compactor.minor_compaction(keys, values).await {
                    error!("[LsmStore][minor_compaction][error happen]: {:?}", err);
                }
                sender.send(()).unwrap();
                info!("[LsmStore][Compaction Drop][Time: {:?}]", start.elapsed());
            });
        }
        Ok(())
    }

    /// 同步持久化immutable_table为SSTable
    pub async fn minor_compaction_sync(&self) -> Result<()> {
        let (keys, values) = self.mem_table.table_swap().await;
        Ok(Compactor::from_lsm_kv(self).minor_compaction(keys, values).await?)
    }

    /// 同步进行SSTable基于Level的层级压缩
    pub async fn major_compaction_sync(&self, level: usize) -> Result<()> {
        Ok(Compactor::from_lsm_kv(self).major_compaction(level).await?)
    }

    /// 通过CommandData的引用解包并克隆出value值
    fn value_unpack(cmd_data: &CommandData) -> Option<Vec<u8>> {
        cmd_data.get_value_clone()
    }

    /// 通过CommandData的所有权直接返回value值的所有权
    fn value_unpack_with_owner(cmd_data: CommandData) -> Option<Vec<u8>> {
        cmd_data.get_value_owner()
    }
    pub(crate) fn manifest(&self) -> &Arc<RwLock<Manifest>> {
        &self.manifest
    }
    pub(crate) fn config(&self) -> &Arc<Config> {
        &self.config
    }
    pub(crate) fn io_handler_factory(&self) -> &Arc<IOHandlerFactory> {
        &self.io_handler_factory
    }
    pub(crate) fn wal(&self) -> &Arc<HashStore> {
        &self.wal
    }

    /// 存活标记
    /// 返回一个Sender用于存活结束通知
    pub(crate) async fn live_tag(&self) -> Sender<()> {
        let (sender, receiver) = oneshot::channel();

        self.vec_rev.lock()
            .await
            .push(receiver);

        sender
    }

    /// 等待所有压缩结束
    async fn wait_for_compression_down(&self) -> Result<()> {
        // 监听异步任务是否执行完毕
        let mut vec_rev = self.vec_rev.lock().await;
        while let Some(rev) = vec_rev.pop() {
            rev.await?
        }

        Ok(())
    }
}

pub(crate) struct CommandCodec;

impl CommandCodec {
    pub(crate) fn encode_gen(gen: i64) -> Result<Vec<u8>> {
        Ok(bincode::serialize(&gen)?)
    }

    pub(crate) fn decode_gen(key: &Vec<u8>) -> Result<i64> {
        Ok(bincode::deserialize(key)?)
    }

    pub(crate) fn encode_keys(value: &Vec<Vec<u8>>) -> Result<Vec<u8>> {
        Ok(bincode::serialize(value)?)
    }

    pub(crate) fn decode_keys(vec_u8: &Vec<u8>) -> Result<Vec<Vec<u8>>> {
        Ok(bincode::deserialize(vec_u8)?)
    }
}

pub struct Config {
    /// 数据目录地址
    pub(crate) dir_path: PathBuf,
    /// WAL持久化阈值
    pub(crate) wal_compaction_threshold: u64,
    /// 数据分块大小
    pub(crate) part_size: u64,
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
    pub(crate) cache_size: usize,
    /// 开启wal日志写入
    /// 在开启状态时，会在SSTable文件读取失败时生效，避免数据丢失
    /// 不过在设备IO容易成为瓶颈，或使用多节点冗余写入时，建议关闭以提高写入性能
    pub(crate) wal_enable: bool
}

impl Config {

    pub fn dir_path(mut self, dir_path: PathBuf) -> Self {
        self.dir_path = dir_path;
        self
    }

    pub fn minor_threshold_with_data_size(mut self, minor_threshold_with_data_size: u64) -> Self {
        self.minor_threshold_with_data_size = minor_threshold_with_data_size;
        self
    }

    pub fn wal_compaction_threshold(mut self, wal_compaction_threshold: u64) -> Self {
        self.wal_compaction_threshold = wal_compaction_threshold;
        self
    }

    pub fn part_size(mut self, part_size: u64) -> Self {
        self.part_size = part_size;
        self
    }

    pub fn sst_file_size(mut self, sst_file_size: usize) -> Self {
        self.sst_file_size = sst_file_size;
        self
    }

    pub fn major_threshold_with_sst_size(mut self, major_threshold_with_sst_size: usize) -> Self {
        self.major_threshold_with_sst_size = major_threshold_with_sst_size;
        self
    }

    pub fn major_select_file_size(mut self, major_select_file_size: usize) -> Self {
        self.major_select_file_size = major_select_file_size;
        self
    }

    pub fn node_id(mut self, node_id: i32) -> Self {
        self.node_id = node_id;
        self
    }

    pub fn level_sst_magnification(mut self, level_sst_magnification: usize) -> Self {
        self.level_sst_magnification = level_sst_magnification;
        self
    }

    pub fn desired_error_prob(mut self, desired_error_prob: f64) -> Self {
        self.desired_error_prob = desired_error_prob;
        self
    }

    pub fn cache_size(mut self, cache_size: usize) -> Self {
        self.cache_size = cache_size;
        self
    }

    pub fn create_gen(&self) -> i64 {
        SnowflakeIdBucket::new(self.node_id, self.buffer_i32
            .fetch_add(1, Ordering::SeqCst)).get_id()
    }

    pub fn wal_enable(mut self, wal_enable: bool) -> Self {
        self.wal_enable = wal_enable;
        self
    }

    pub fn new() -> Self {
        Self {
            dir_path: DEFAULT_WAL_PATH.into(),
            minor_threshold_with_data_size: DEFAULT_MINOR_THRESHOLD_WITH_DATA_OCCUPIED,
            wal_compaction_threshold: DEFAULT_WAL_COMPACTION_THRESHOLD,
            part_size: DEFAULT_PART_SIZE,
            sst_file_size: DEFAULT_SST_FILE_SIZE,
            major_threshold_with_sst_size: DEFAULT_MAJOR_THRESHOLD_WITH_SST_SIZE,
            major_select_file_size: DEFAULT_MAJOR_SELECT_FILE_SIZE,
            node_id: DEFAULT_MACHINE_ID,
            level_sst_magnification: DEFAULT_LEVEL_SST_MAGNIFICATION,
            buffer_i32: AtomicI32::new(0),
            desired_error_prob: DEFAULT_DESIRED_ERROR_PROB,
            cache_size: DEFAULT_CACHE_SIZE,
            wal_enable: true,
        }
    }
}

/// 以Task类似的异步写数据，避免影响数据写入性能
/// 当然，LevelDB的话虽然wal写入会提供是否同步的选项，此处先简化优先使用异步
pub(crate) fn wal_put(wal: &Arc<HashStore>, key: Vec<u8>, value: Vec<u8>) {
    let wal = Arc::clone(wal);
    tokio::spawn(async move {
        if let Err(err) = wal.set(&key, value).await {
            error!("[LsmStore][wal_put][error happen]: {:?}", err);
        }
    });
}

#[test]
fn test_lsm_major_compactor() -> Result<()> {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    tokio_test::block_on(async move {
        let kv_store = LsmStore::open(temp_dir.path()).await?;
        let mut vec_key: Vec<i32> = Vec::new();

        let start = Instant::now();
        for i in 0..300000 {
            let vec_u8 = rmp_serde::to_vec(&i).unwrap();
            kv_store.set(&vec_u8, vec_u8.clone()).await?;
            vec_key.push(i);
        }
        println!("[set_for][Time: {:?}]", start.elapsed());

        let start = Instant::now();
        kv_store.get(&rmp_serde::to_vec(&0).unwrap()).await?.unwrap();
        println!("[compaction_for][Time: {:?}]", start.elapsed());

        kv_store.flush().await?;
        let start = Instant::now();
        for key in vec_key {
            let vec_u8 = rmp_serde::to_vec(&key).unwrap();
            assert_eq!(kv_store.get(&vec_u8).await?.unwrap(), vec_u8);
        }
        println!("[get_for][Time: {:?}]", start.elapsed());
        kv_store.flush().await?;
        Ok(())
    })
}