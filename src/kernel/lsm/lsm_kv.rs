use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use chrono::Local;
use itertools::Itertools;
use crate::{HashStore, KvsError};
use crate::kernel::{CommandData, CommandPackage, KVStore, sorted_gen_list};
use crate::kernel::io_handler::IOHandlerFactory;
use crate::kernel::Result;

// pub(crate) type LevelVec = Vec<Vec<u64>>;
//
// pub(crate) type SsTableMap = BTreeMap<u64, SsTable>;
//
// pub(crate) type MemTable = BTreeMap<Vec<u8>, CommandData>;
//
// pub(crate) const DEFAULT_WAL_PATH: &str = "wal";
//
// pub(crate) const DEFAULT_THRESHOLD_SIZE: u64 = 1024 * 3;
//
// pub(crate) const DEFAULT_PART_SIZE: u64 = 1024 * 2;
//
// pub(crate) const DEFAULT_FILE_SIZE: u64 = 1024 * 1024 * 14;
//
// pub(crate) const DEFAULT_WAL_COMPACTION_THRESHOLD: u64 = crate::kernel::hash_kv::DEFAULT_COMPACTION_THRESHOLD;

// pub struct LsmStore {
//     // 内存表
//     mem_table: Arc<RwLock<MemTable>>,
//     // 不可变内存表 持久化内存表时数据暂存用
//     immutable_table_options: Arc<RwLock<MemTable>>,
//     // SSTable详情集
//     manifest: Arc<RwLock<Manifest>>,
//     config: Config,
//     io_handler_factory: IOHandlerFactory,
//     /// WAL存储器
//     ///
//     /// SSTable持久化前会将gen写入
//     /// 持久化成功后则会删除gen，以此作为是否成功的依据
//     ///
//     /// 使用HashStore作为wal的原因：
//     /// 1、操作简易，不需要重新写一个WAL
//     /// 2、作Key-Value分离的准备，当作vLog
//     /// 3、HashStore会丢弃超出大小的数据，保证最新数据不会丢失
//     wal: HashStore,
// }
//
// impl Clone for LsmStore {
//     fn clone(&self) -> Self {
//         todo!()
//     }
// }
//
// impl KVStore for LsmStore {
//     fn name() -> &'static str where Self: Sized {
//         todo!()
//     }
//
//     async fn open(path: impl Into<PathBuf>) -> Result<Self> {
//         todo!()
//     }
//
//     async fn flush(&self) -> Result<()> {
//         todo!()
//     }
//
//     async fn set(&self, key: &Vec<u8>, value: Vec<u8>) -> Result<()> {
//         todo!()
//     }
//
//     async fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>> {
//         todo!()
//     }
//
//     async fn remove(&self, key: &Vec<u8>) -> Result<()> {
//         todo!()
//     }
//
//     fn name() -> &'static str where Self: Sized {
//         "LsmStore made in Kould"
//     }
//
//     fn open(path: impl Into<PathBuf>) -> Result<Self> where Self: Sized {
//         LsmStore::open_with_config(Config::new().dir_path(path.into()))
//     }
//
//     fn flush(&mut self) -> Result<()> {
//         self.wal.flush()?;
//         Ok(())
//     }
//
//     fn set(&mut self, key: &Vec<u8>, value: Vec<u8>) -> Result<()> {
//         self.append_cmd_data(CommandData::Set { key: key.clone(), value })
//     }
//
//     fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>> {
//         let mem_table = self.mem_table
//             .read()
//             .unwrap();
//
//         if let Some(cmd_data) = mem_table.get(key) {
//             return LsmStore::cmd_unpack(cmd_data);
//         }
//         if let Some(index) = &self.immutable_table_options {
//             let index = index.read().unwrap();
//             if let Some(cmd_data) = index.get(key) {
//                 return LsmStore::cmd_unpack(cmd_data);
//             }
//         }
//         for (_, ss_table) in self.manifest.get_ss_table_map() {
//             if let Some(cmd_data) = ss_table.query(key)? {
//                 return LsmStore::cmd_unpack_with_owner(cmd_data);
//             }
//         }
//
//         Ok(None)
//     }
//
//     fn remove(&mut self, key: &Vec<u8>) -> Result<()> {
//         match self.get(key)? {
//             Some(_) => { self.append_cmd_data(CommandData::Remove { key: key.clone() }) }
//             None => { Err(KvsError::KeyNotFound) }
//         }
//     }
// }
//
// impl LsmStore {
//
//     /// 追加数据
//     fn append_cmd_data(&mut self, cmd: CommandData) -> Result<()> {
//         let mut mem_table = self.mem_table
//             .write()
//             .unwrap();
//
//         let threshold_size = self.config.threshold_size as usize;
//
//         // Wal与MemTable双写
//         let key = cmd.get_key();
//         self.wal.set(key, CommandPackage::encode(&cmd)?)?;
//         mem_table.insert(key.clone(), cmd);
//
//         if mem_table.len() > threshold_size {
//             drop(mem_table);
//             self.store_to_ss_table()?;
//         }
//
//         Ok(())
//     }
//
//     /// 使用Config进行LsmStore初始化
//     pub fn open_with_config(config: Config) -> Result<Self> where Self: Sized {
//         let path = config.dir_path.clone();
//         let file_size = config.file_size;
//         let wal_compaction_threshold = config.wal_compaction_threshold;
//         let wal_redundancy_size = config.wal_redundancy_size;
//
//         let mem_table = Arc::new(RwLock::new(BTreeMap::new()));
//         let mut ss_tables = BTreeMap::new();
//
//         let mut wal_path = path.clone();
//         wal_path.push(DEFAULT_WAL_PATH);
//
//         // 初始化wal日志
//         let wal = HashStore::open_with_compaction_threshold(&wal_path
//                                                               , wal_compaction_threshold
//                                                               , wal_redundancy_size)?;
//         // 持久化数据恢复
//         // 倒叙遍历，从最新的数据开始恢复
//         for gen in sorted_gen_list(&path).await?.iter().rev() {
//             // 尝试初始化Table
//             if let Ok(ss_table) = SsTable::restore_from_file(&*path, *gen, file_size) {
//                 // 初始化成功时直接传入SSTable的索引中
//                 ss_tables.insert(*gen, ss_table);
//             } else {
//                 // 从wal将有问题的ss_table恢复到mem_table中
//                 Self::reload_for_wal(&mem_table, &wal, &gen)?;
//             }
//         }
//
//         // 构建SSTable信息集
//         let manifest = Manifest::new(ss_tables, path.clone());
//
//         Ok(LsmStore {
//             mem_table,
//             immutable_table_options: None,
//             manifest,
//             config,
//             wal,
//         })
//     }
//
//     /// 从Wal恢复SSTable数据
//     /// 初始化失败时遍历wal的key并检测key是否为gen
//     fn reload_for_wal(mem_table: &Arc<RwLock<BTreeMap<Vec<u8>, CommandData>>>, wal: &HashStore, gen: &&u64) -> Result<()>{
//         // 将SSTable持久化失败前预存入的指令键集合从wal中获取
//         // 随后将每一条指令键对应的指令恢复到mem_table中
//         let key_gen = CommandCodec::encode_str_key(gen.to_string())?;
//         if let Some(key_cmd_u8) = wal.get(&key_gen)? {
//             for key in CommandCodec::decode_keys(&key_cmd_u8)? {
//                 if let Some(cmd_data_u8) = wal.get(&key)? {
//                     let cmd_data = CommandPackage::decode(&cmd_data_u8)?;
//
//                     mem_table.write()
//                         .unwrap()
//                         .insert(cmd_data.get_key_clone(), cmd_data);
//                 } else {
//                     return Err(KvsError::WalLoadError);
//                 }
//             };
//         } else {
//             return Err(KvsError::WalLoadError);
//         }
//         Ok(())
//     }
//
//     /// 持久化immutable_table为SSTable
//     pub(crate) fn store_to_ss_table(&mut self) -> Result<()> {
//         // 切换mem_table并准备持久化
//         self.immutable_table_options = Some(self.mem_table.clone());
//         self.mem_table = Arc::new(RwLock::new(BTreeMap::new()));
//
//         // 获取当前时间戳当gen
//         let time_stamp = Local::now().timestamp_millis() as u64;
//         let vec_ts_u8 = CommandCodec::encode_str_key(time_stamp.to_string())?;
//         if let Some(immutable_table) = &self.immutable_table_options.clone() {
//
//             // 将这些索引的key序列化后预先存入wal中作防灾准备
//             // 当持久化异常时将对应gen的key反序列化出来并从wal找到对应值
//             let immutable_table = immutable_table.read()
//                 .unwrap();
//             let vec_keys = immutable_table.keys()
//                 .map(|k| k.clone())
//                 .collect_vec();
//
//             self.wal.set(&vec_ts_u8, CommandCodec::encode_keys(&vec_keys)?)?;
//
//             // 从内存表中将数据持久化为ss_table
//             let ss_table = SsTable::create_form_immutable_table(&self.config
//                                                                 , time_stamp
//                                                                 , &immutable_table)?;
//             self.manifest.insert(time_stamp, ss_table)?;
//             self.immutable_table_options = None;
//             self.wal.remove(&vec_ts_u8)?;
//         }
//         Ok(())
//     }
//
//     /// 通过CommandData的引用解包并克隆出value值
//     fn cmd_unpack(cmd_data: &CommandData) -> Result<Option<Vec<u8>>> {
//         match cmd_data.get_value() {
//             None => { Ok(None) }
//             Some(value) => { Ok(Some(value.clone())) }
//         }
//     }
//
//     /// 通过CommandData的所有权直接返回value值的所有权
//     fn cmd_unpack_with_owner(cmd_data: CommandData) -> Result<Option<Vec<u8>>> {
//         match cmd_data.get_value_owner() {
//             None => { Ok(None) }
//             Some(value) => { Ok(Some(value)) }
//         }
//     }
// }
//
// impl Drop for LsmStore {
//     fn drop(&mut self) {
//         self.wal.flush().unwrap();
//         self.store_to_ss_table().unwrap();
//     }
// }
//
// pub(crate) struct CommandCodec;
//
// impl CommandCodec {
//     pub(crate) fn encode_str_key(key: String) -> Result<Vec<u8>> {
//         Ok(bincode::serialize(&key)?)
//     }
//
//     pub(crate) fn decode_str_key(key: &Vec<u8>) -> Result<String> {
//         Ok(bincode::deserialize(key)?)
//     }
//
//     pub(crate) fn encode_keys(value: &Vec<Vec<u8>>) -> Result<Vec<u8>> {
//         Ok(bincode::serialize(value)?)
//     }
//
//     pub(crate) fn decode_keys(vec_u8: &Vec<u8>) -> Result<Vec<Vec<u8>>> {
//         Ok(bincode::deserialize(vec_u8)?)
//     }
// }
//
// pub struct Config {
//     // 数据目录地址
//     pub(crate) dir_path: PathBuf,
//     // 持久化阈值
//     pub(crate) threshold_size: u64,
//     // WAL持久化阈值
//     pub(crate) wal_compaction_threshold: u64,
//     // WAL冗余值
//     pub(crate) wal_redundancy_size: u64,
//     // 数据分块大小
//     pub(crate) part_size: u64,
//     // 文件大小
//     pub(crate) file_size: u64
// }
//
// impl Config {
//
//     pub fn dir_path(mut self, dir_path: PathBuf) -> Self {
//         self.dir_path = dir_path;
//         self
//     }
//
//     pub fn threshold_size(mut self, threshold_size: u64) -> Self {
//         self.threshold_size = threshold_size;
//         self
//     }
//
//     pub fn wal_compaction_threshold(mut self, wal_compaction_threshold: u64) -> Self {
//         self.wal_compaction_threshold = wal_compaction_threshold;
//         self
//     }
//
//     pub fn wal_redundancy_size(mut self, wal_redundancy_size: u64) -> Self {
//         self.wal_redundancy_size = wal_redundancy_size;
//         self
//     }
//
//     pub fn part_size(mut self, part_size: u64) -> Self {
//         self.part_size = part_size;
//         self
//     }
//
//     pub fn file_size(mut self, file_size: u64) -> Self {
//         self.file_size = file_size;
//         self
//     }
//
//     pub fn new() -> Self {
//         Self {
//             dir_path: DEFAULT_WAL_PATH.into(),
//             threshold_size: DEFAULT_THRESHOLD_SIZE,
//             wal_compaction_threshold: DEFAULT_WAL_COMPACTION_THRESHOLD,
//             wal_redundancy_size: DEFAULT_WAL_REDUNDANCY_SIZE,
//             part_size: DEFAULT_PART_SIZE,
//             file_size: DEFAULT_FILE_SIZE
//         }
//     }
// }