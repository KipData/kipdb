use std::collections::BTreeMap;
use std::option;
use std::path::PathBuf;
use tracing::error;
use crate::{HashStore, KvsError};
use crate::kernel::{CommandData, CommandPackage, KVStore, MmapReader, MmapWriter, new_log_file, new_log_file_with_gen, sorted_gen_list};
use crate::kernel::lsm::ss_table::SsTable;
use crate::kernel::Result;

pub(crate) const DEFAULT_WAL_PATH: &str = "./wal";

pub(crate) const DEFAULT_DELIMITER: &str = "_";

pub(crate) const DEFAULT_COMPACTION_THRESHOLD: u64 = 1024 * 1024 * 10;

pub(crate) const DEFAULT_PART_SIZE: u64 = 1024 * 2;

pub(crate) const DEFAULT_FILE_SIZE: u64 = 1024 * 1024 * 13;

pub(crate) const DEFAULT_WAL_COMPACTION_THRESHOLD: u64 = crate::kernel::hash_kv::DEFAULT_COMPACTION_THRESHOLD;

pub(crate) const DEFAULT_WAL_REDUNDANCY_SIZE: u64 = crate::kernel::hash_kv::DEFAULT_REDUNDANCY_SIZE;

pub struct LsmStore {
    // 内存表
    mem_table: BTreeMap<Vec<u8>, CommandData>,
    // 不可变内存表 持久化内存表时数据暂存用
    immutable_table: BTreeMap<Vec<u8>, CommandData>,
    // SSTable存储集合
    ss_tables: Vec<SsTable>,
    // 数据目录
    data_dir: PathBuf,
    // 持久化阈值
    compaction_threshold: u64,
    // 数据分区大小
    part_size: u64,
    /// WAL存储器
    ///
    /// WAL写入的开头为gen_key->value
    ///
    /// 使用HashStore作为wal的原因：
    /// 1、操作简易，不需要重新写一个WAL
    /// 2、作Key-Value分离的准备，当作vLog
    /// 3、HashStore会丢弃超出大小的数据，保证最新数据不会丢失
    wal: HashStore,
}

impl KVStore for LsmStore {
    fn name() -> &'static str where Self: Sized {
        "LsmStore made in Kould"
    }

    fn open(path: impl Into<PathBuf>) -> Result<Self> where Self: Sized {
        LsmStore::open_with_config(Config::new())
    }

    fn flush(&mut self) -> Result<()> {
        todo!()
    }

    fn set(&mut self, key: &Vec<u8>, value: Vec<u8>) -> Result<()> {
        todo!()
    }

    fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>> {
        todo!()
    }

    fn remove(&mut self, key: &Vec<u8>) -> Result<()> {
        todo!()
    }

    fn shut_down(&mut self) -> Result<()> {
        todo!()
    }
}

impl LsmStore {

    fn open_with_config(config: Config) -> Result<Self> where Self: Sized {
        let path = config.dir_path;
        let file_size = config.file_size;

        let mut mem_table = BTreeMap::new();
        let mut ss_tables = Vec::new();
        // 初始化wal日志
        let wal = HashStore::open_with_compaction_threshold(DEFAULT_WAL_PATH
                                                              , DEFAULT_WAL_COMPACTION_THRESHOLD
                                                              , DEFAULT_WAL_REDUNDANCY_SIZE)?;

        // 持久化数据恢复
        // 倒叙遍历，从最新的数据开始恢复
        for gen in sorted_gen_list(&path)?.iter().rev() {
            let mut gen_str = gen.to_string();
            gen_str += DEFAULT_DELIMITER;
            // 尝试初始化Table
            if let Ok(ss_table) = SsTable::restore_from_file(&*path, *gen, file_size) {
                // 初始化成功时直接传入SSTable的索引中
                ss_tables.push(ss_table);
            } else {
                // 初始化失败时遍历wal的key并检测key的开头是否为gen
                for key_u8 in wal.keys_from_index()? {
                    let key_str = CommandCodec::decode_str_key(key_u8)?;
                    // 当key的文件名与gen相同时将数据填入mem_table中
                    if key_str.starts_with(&gen_str) {
                        match wal.get_cmd_data(key_u8)? {
                            None => { Err(KvsError::WalLoadError) }
                            Some(cmd_data) => {
                                mem_table.insert(cmd_data.get_key(), cmd_data);
                                Ok(())
                            }
                        }?;
                    }
                }
            }
        }

        Ok(LsmStore {
            mem_table,
            immutable_table: BTreeMap::new(),

            ss_tables,
            data_dir: path,
            compaction_threshold: config.compaction_threshold,
            part_size: config.part_size,
            wal,
        })
    }
}

struct CommandCodec;

impl CommandCodec {
    fn encode_str_key(key: String) -> Result<Vec<u8>> {
        Ok(bincode::serialize(&key)?)
    }

    fn decode_str_key(key: &Vec<u8>) -> Result<String> {
        Ok(bincode::deserialize(key)?)
    }
}

pub struct Config {
    // 数据目录地址
    dir_path: PathBuf,
    // 持久化阈值
    compaction_threshold: u64,
    // WAL持久化阈值
    wal_compaction_threshold: u64,
    // WAL冗余值
    wal_redundancy_size: u64,
    // 数据分块大小
    part_size: u64,
    // 文件大小
    file_size: u64
}

impl Config {

    pub fn dir_path(mut self, dir_path: PathBuf) -> Self {
        self.dir_path = dir_path;
        self
    }

    pub fn compaction_threshold(mut self, compaction_threshold: u64) -> Self {
        self.compaction_threshold = compaction_threshold;
        self
    }

    pub fn wal_compaction_threshold(mut self, wal_compaction_threshold: u64) -> Self {
        self.wal_compaction_threshold = wal_compaction_threshold;
        self
    }

    pub fn wal_redundancy_size(mut self, wal_redundancy_size: u64) -> Self {
        self.wal_redundancy_size = wal_redundancy_size;
        self
    }

    pub fn part_size(mut self, part_size: u64) -> Self {
        self.part_size = part_size;
        self
    }

    pub fn file_size(mut self, file_size: u64) -> Self {
        self.file_size = file_size;
        self
    }

    pub fn new() -> Self {
        Self {
            dir_path: DEFAULT_WAL_PATH.into(),
            compaction_threshold: DEFAULT_COMPACTION_THRESHOLD,
            wal_compaction_threshold: DEFAULT_WAL_COMPACTION_THRESHOLD,
            wal_redundancy_size: DEFAULT_WAL_REDUNDANCY_SIZE,
            part_size: DEFAULT_PART_SIZE,
            file_size: DEFAULT_FILE_SIZE
        }
    }
}