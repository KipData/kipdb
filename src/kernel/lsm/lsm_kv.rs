use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use chrono::Local;
use crate::{HashStore, KvsError};
use crate::kernel::{CommandData, CommandPackage, KVStore, sorted_gen_list};
use crate::kernel::io_handler::IOHandlerFactory;
use crate::kernel::lsm::{Manifest, MemTable, MemTableSlice};
use crate::kernel::lsm::ss_table::{LEVEL_0, SsTable};
use crate::kernel::Result;

pub(crate) type LevelVec = Vec<Vec<u64>>;

pub(crate) type SsTableMap = BTreeMap<u64, SsTable>;

pub(crate) const DEFAULT_WAL_PATH: &str = "wal";

pub(crate) const DEFAULT_THRESHOLD_SIZE: u64 = 1024 * 3;

pub(crate) const DEFAULT_PART_SIZE: u64 = 1024 * 2;

pub(crate) const DEFAULT_FILE_SIZE: u64 = 1024 * 1024 * 14;

pub(crate) const DEFAULT_WAL_COMPACTION_THRESHOLD: u64 = crate::kernel::hash_kv::DEFAULT_COMPACTION_THRESHOLD;

pub struct LsmStore {
    // 内存表
    mem_table_slice: Arc<RwLock<MemTableSlice>>,
    // SSTable详情集
    manifest: Arc<RwLock<Manifest>>,
    config: Config,
    io_handler_factory: IOHandlerFactory,
    /// WAL存储器
    ///
    /// SSTable持久化前会将gen写入
    /// 持久化成功后则会删除gen，以此作为是否成功的依据
    ///
    /// 使用HashStore作为wal的原因：
    /// 1、操作简易，不需要重新写一个WAL
    /// 2、作Key-Value分离的准备，当作vLog
    /// 3、HashStore会丢弃超出大小的数据，保证最新数据不会丢失
    wal: HashStore,
}

impl Clone for LsmStore {
    fn clone(&self) -> Self {
        todo!()
    }
}

impl KVStore for LsmStore {
    fn name() -> &'static str where Self: Sized {
        "LsmStore made in Kould"
    }

    async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        LsmStore::open_with_config(Config::new().dir_path(path.into())).await
    }

    async fn flush(&self) -> Result<()> {
        self.wal.flush().await?;

        Ok(())
    }

    async fn set(&self, key: &Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.append_cmd_data(CommandData::Set { key: key.clone(), value }).await
    }

    async fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>> {
        let mem_table_slice = self.mem_table_slice.read().unwrap();
        let manifest = self.manifest.read().unwrap();

        if let Some(cmd_data) = mem_table_slice.get_cmd_data(key) {
            return LsmStore::cmd_unpack(cmd_data);
        }
        for (_, ss_table) in manifest.get_ss_table_map() {
            if let Some(cmd_data) = ss_table.query(key)? {
                return LsmStore::cmd_unpack_with_owner(cmd_data);
            }
        }

        Ok(None)
    }

    async fn remove(&self, key: &Vec<u8>) -> Result<()> {
        match self.get(key).await? {
            Some(_) => { self.append_cmd_data(CommandData::Remove { key: key.clone() }).await }
            None => { Err(KvsError::KeyNotFound) }
        }
    }

    async fn shut_down(&self) -> Result<()> {
        self.wal.flush().await?;
        self.store_to_ss_table().await?;

        Ok(())
    }
}

impl LsmStore {

    /// 追加数据
    async fn append_cmd_data(&self, cmd: CommandData) -> Result<()> {
        let threshold_size = self.config.threshold_size as usize;

        // Wal与MemTable双写
        let key = cmd.get_key();
        self.wal.set(key, CommandPackage::encode(&cmd)?).await?;
        mem_table.insert(key.clone(), cmd);

        if mem_table.len() > threshold_size {
            drop(mem_table);
            self.store_to_ss_table().await?;
        }

        Ok(())
    }

    /// 使用Config进行LsmStore初始化
    pub async fn open_with_config(config: Config) -> Result<Self> where Self: Sized {
        let path = config.dir_path.clone();
        let wal_compaction_threshold = config.wal_compaction_threshold;
        let reader_size = config.reader_size;
        let thread_size = config.thread_size;

        let mut mem_table = MemTable::new();
        let mut ss_tables = BTreeMap::new();

        let mut wal_path = path.clone();
        wal_path.push(DEFAULT_WAL_PATH);

        // 初始化wal日志
        let wal = HashStore::open_with_compaction_threshold(&wal_path, wal_compaction_threshold, reader_size, thread_size).await?;
        let io_handler_factory = IOHandlerFactory::new(path, reader_size, thread_size);
        // 持久化数据恢复
        // 倒叙遍历，从最新的数据开始恢复
        for gen in sorted_gen_list(&path).await?.iter().rev() {
            let mut io_handler = io_handler_factory.create(*gen)?;
            // 尝试初始化Table
            if let Ok(ss_table) = SsTable::restore_from_file(io_handler).await {
                // 初始化成功时直接传入SSTable的索引中
                ss_tables.insert(*gen, ss_table);
            } else {
                io_handler_factory.clean(&mut io_handler)?;
                // 从wal将有问题的ss_table恢复到mem_table中
                Self::reload_for_wal(&mut mem_table, &wal, &gen).await?;
            }
        }

        // 构建SSTable信息集
        let manifest = Arc::new(RwLock::new(Manifest::new(ss_tables, Arc::new(path.clone()))));

        Ok(LsmStore {
            mem_table_slice: Arc::new(RwLock::new(MemTableSlice::load(mem_table))),
            manifest,
            config,
            io_handler_factory,
            wal,
        })
    }

    /// 从Wal恢复SSTable数据
    /// 初始化失败时遍历wal的key并检测key是否为gen
    async fn reload_for_wal(mem_table: &mut MemTable, wal: &HashStore, gen: &u64) -> Result<()>{
        // 将SSTable持久化失败前预存入的指令键集合从wal中获取
        // 随后将每一条指令键对应的指令恢复到mem_table中
        let key_gen = CommandCodec::encode_str_key(gen.to_string())?;
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

    /// 持久化immutable_table为SSTable
    pub(crate) async fn store_to_ss_table(&self) -> Result<()> {
        let mut mem_table_slice = self.mem_table_slice.write().unwrap();
        let mut manifest = self.manifest.write().unwrap();

        // 切换mem_table并准备持久化
        let (vec_keys, vec_values) = mem_table_slice.table_swap();

        // 获取当前时间戳当gen
        let time_stamp = Local::now().timestamp_millis() as u64;
        let vec_ts_u8 = CommandCodec::encode_str_key(time_stamp.to_string())?;


        // 将这些索引的key序列化后预先存入wal中作防灾准备
        // 当持久化异常时将对应gen的key反序列化出来并从wal找到对应值

        self.wal.set(&vec_ts_u8, CommandCodec::encode_keys(&vec_keys)?).await?;

        let handler = self.io_handler_factory.create(time_stamp)?;
        // 从内存表中将数据持久化为ss_table
        let ss_table = SsTable::create_form_immutable_table(&self.config
                                                            , handler
                                                            , &vec_values
                                                            , LEVEL_0).await?;
        manifest.insert(time_stamp, ss_table)?;
        Ok(())
    }

    /// 通过CommandData的引用解包并克隆出value值
    fn cmd_unpack(cmd_data: &CommandData) -> Result<Option<Vec<u8>>> {
        match cmd_data.get_value() {
            None => { Ok(None) }
            Some(value) => { Ok(Some(value.clone())) }
        }
    }

    /// 通过CommandData的所有权直接返回value值的所有权
    fn cmd_unpack_with_owner(cmd_data: CommandData) -> Result<Option<Vec<u8>>> {
        match cmd_data.get_value_owner() {
            None => { Ok(None) }
            Some(value) => { Ok(Some(value)) }
        }
    }
}

pub(crate) struct CommandCodec;

impl CommandCodec {
    pub(crate) fn encode_str_key(key: String) -> Result<Vec<u8>> {
        Ok(bincode::serialize(&key)?)
    }

    pub(crate) fn decode_str_key(key: &Vec<u8>) -> Result<String> {
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
    // 数据目录地址
    pub(crate) dir_path: PathBuf,
    // 持久化阈值
    pub(crate) threshold_size: u64,
    // WAL持久化阈值
    pub(crate) wal_compaction_threshold: u64,
    // 数据分块大小
    pub(crate) part_size: u64,
    pub(crate) reader_size: u64,
    pub(crate) thread_size: usize,
}

impl Config {

    pub fn dir_path(mut self, dir_path: PathBuf) -> Self {
        self.dir_path = dir_path;
        self
    }

    pub fn threshold_size(mut self, threshold_size: u64) -> Self {
        self.threshold_size = threshold_size;
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

    pub fn reader_size(mut self, reader_size: u64) -> Self {
        self.reader_size = reader_size;
        self
    }

    pub fn thread_size(mut self, thread_size: usize) -> Self {
        self.thread_size = thread_size;
        self
    }

    pub fn new() -> Self {
        Self {
            dir_path: DEFAULT_WAL_PATH.into(),
            threshold_size: DEFAULT_THRESHOLD_SIZE,
            wal_compaction_threshold: DEFAULT_WAL_COMPACTION_THRESHOLD,
            part_size: DEFAULT_PART_SIZE,
            reader_size: 0,
            thread_size: 0
        }
    }
}