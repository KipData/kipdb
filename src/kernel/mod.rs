use std::{path::PathBuf, fs};
use std::cmp::Ordering;
use std::ffi::OsStr;
use std::path::Path;
use serde::{Deserialize, Serialize};
use crate::kernel::io_handler::IOHandler;
use async_trait::async_trait;
use futures::future;
use itertools::Itertools;

use crate::KvsError;
use crate::net::CommandOption;

pub mod hash_kv;

pub mod sled_kv;
pub mod lsm;
pub mod io_handler;

pub type Result<T> = std::result::Result<T, KvsError>;

/// KV持久化内核 操作定义
#[async_trait]
pub trait KVStore: Send + 'static + Sized {
    /// 获取内核名
    fn name() -> &'static str where Self: Sized;

    /// 通过数据目录路径开启数据库
    async fn open(path: impl Into<PathBuf> + Send) -> Result<Self>;

    /// 强制将数据刷入硬盘
    async fn flush(&self) -> Result<()>;

    /// 设置键值对
    async fn set(&self, key: &Vec<u8>, value: Vec<u8>) -> Result<()>;

    /// 通过键获取对应的值
    async fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>>;

    /// 通过键删除键值对
    async fn remove(&self, key: &Vec<u8>) -> Result<()>;

    /// 顺序批量执行
    async fn batch_order(&self, vec_cmd: Vec<CommandData>) -> Result<Vec<Option<Vec<u8>>>> {
        let mut vec_result = Vec::new();
        for cmd in vec_cmd {
            vec_result.push(cmd.apply(self).await?.into())
        }

        Ok(vec_result)
    }

    /// 并行批量执行
    async fn batch_parallel(&self, vec_cmd: Vec<CommandData>) -> Result<Vec<Option<Vec<u8>>>> {
        let map_cmd = vec_cmd.into_iter()
            .map(|cmd| cmd.apply(self));
        Ok(future::try_join_all(map_cmd)
            .await?
            .into_iter()
            .map(CommandOption::into)
            .collect_vec())
    }

    async fn size_of_disk(&self) -> Result<u64>;

    async fn len(&self) -> Result<usize>;
}

/// 用于包装Command交予持久化核心实现使用的操作类
#[derive(Debug)]
struct CommandPackage {
    cmd: CommandData,
    pos: u64,
    len: usize
}

/// CommandPos Command磁盘指针
/// 用于标记对应Command的位置
/// gen 文件序号
/// pos 开头指针
/// len 命令长度
#[derive(Debug, Copy, Clone)]
struct CommandPos {
    gen: i64,
    pos: u64,
    len: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub enum CommandData {
    Set { key: Vec<u8>, value: Vec<u8> },
    Remove { key: Vec<u8> },
    Get { key: Vec<u8> }
}

impl CommandPos {
    /// 重写自身数据
    pub fn change(&mut self, gen: i64, pos: u64, len: usize) {
        self.gen = gen;
        self.pos = pos;
        self.len = len;
    }
}

impl PartialOrd<Self> for CommandData {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Option::from(self.get_key().cmp(other.get_key()))
    }
}

impl Ord for CommandData {
    fn cmp(&self, other: &Self) -> Ordering {
        self.get_key().cmp(other.get_key())
    }
}

impl CommandPackage {

    pub fn encode(cmd: &CommandData) -> Result<Vec<u8>> {
        Ok(rmp_serde::to_vec(cmd)?)
    }

    pub fn decode(vec: &Vec<u8>) -> Result<CommandData> {
        Ok(rmp_serde::from_slice(vec)?)
    }

    pub fn unpack(self) -> CommandData {
        self.cmd
    }

    /// 快进四位
    /// 用于写入除CommandData时，分隔数据使前端CommandData能够被连续识别
    pub async fn end_tag(io_handler: &IOHandler) -> Result<()> {
        io_handler.write(vec![b'\0',4]).await?;
        Ok(())
    }

    /// 实例化一个Command
    pub fn new(cmd: CommandData, pos: u64, len: usize) -> Self {
        CommandPackage{ cmd, pos, len }
    }

    /// 写入一个Command
    /// 写入完成后该cmd的去除len位置的写入起始位置与长度
    pub async fn write(io_handler: &IOHandler, cmd: &CommandData) -> Result<(u64, usize)> {
        let (start, len) = Self::write_back_real_pos(io_handler, cmd).await?;
        Ok((start + 4, len - 4))
    }

    /// 写入一个Command
    /// 写入完成后该cmd的真实写入起始位置与长度
    pub async fn write_back_real_pos(io_handler: &IOHandler, cmd: &CommandData) -> Result<(u64, usize)> {
        Ok(io_handler.write(Self::trans_to_vec_u8(cmd)?).await?)
    }

    pub async fn write_batch_first_pos(io_handler: &IOHandler, vec_cmd: &Vec<CommandData>) -> Result<(u64, usize)> {
        let batch = vec_cmd.into_iter()
            .map(|cmd_data| Self::trans_to_vec_u8(cmd_data).unwrap())
            .flatten()
            .collect_vec();

        Ok(io_handler.write(batch).await?)
    }

    /// 将数据分片集成写入， 返回起始Pos、整段写入Pos、每段数据序列化长度Pos
    pub async fn write_batch_first_pos_with_sharding(io_handler: &IOHandler, vec_sharding: &Vec<Vec<CommandData>>) -> Result<(u64, usize, Vec<usize>)> {
        let (vec_len, vec_sharding_u8): (Vec<usize>, Vec<Vec<u8>>) = vec_sharding.into_iter()
            .map(|sharding| sharding.into_iter()
                .map(|cmd_data| Self::trans_to_vec_u8(cmd_data).unwrap())
                .flatten()
                .collect_vec())
            .map(|sharding_u8| (sharding_u8.len(), sharding_u8))
            .unzip();

        let (start_pos, batch_len) = io_handler.write(vec_sharding_u8.into_iter().flatten().collect()).await?;

        Ok((start_pos, batch_len, vec_len))
    }

    pub(crate) fn trans_to_vec_u8(cmd: &CommandData) -> Result<Vec<u8>> {
        let vec = rmp_serde::to_vec(cmd)?;
        let i = vec.len();
        let vec_head = vec![(i >> 24) as u8,
                                (i >> 16) as u8,
                                (i >> 8) as u8,
                                i as u8];
        Ok(vec_head.into_iter()
            .chain(vec)
            .collect_vec())
    }

    /// IOHandler的对应Gen，以起始位置与长度使用的单个Command
    pub async fn from_pos(io_handler: &IOHandler, start: u64, len: usize) -> Result<Option<CommandPackage>> {
        let option = Self::from_pos_unpack(io_handler, start, len).await?
            .map(|cmd_data| CommandPackage::new(cmd_data, start, len));
        Ok(option)
    }
    /// IOHandler的对应Gen，以起始位置与长度使用的单个Command，不进行CommandPackage包装
    pub async fn from_pos_unpack(io_handler: &IOHandler, start: u64, len: usize) -> Result<Option<CommandData>> {
        let cmd_u8 = io_handler.read_with_pos(start, len).await?;
        Ok(rmp_serde::decode::from_slice(cmd_u8.as_slice()).ok())
    }

    /// 获取zone之中所有的Command
    pub async fn from_zone_to_vec(zone: &[u8]) -> Result<Vec<CommandPackage>> {
        let mut vec: Vec<CommandPackage> = Vec::new();
        let vec_u8 = Self::get_vec_bytes(zone).await;
        let mut pos = 4;
        for &cmd_u8 in vec_u8.iter() {
            let len = cmd_u8.len();
            let cmd: CommandData = rmp_serde::decode::from_slice(cmd_u8)?;
            vec.push(CommandPackage::new(cmd, pos, len));
            // 对pos进行长度自增并对占位符进行跳过
            pos += len as u64 + 4;
        }
        Ok(vec)
    }

    /// 从该数据区间中找到对应Key的CommandData
    pub async fn find_key_with_zone(zone: &[u8], key: &Vec<u8>) -> Result<Option<CommandPackage>> {
        let vec_u8 = Self::get_vec_bytes(zone).await;
        let mut pos = 4;
        for &cmd_u8 in vec_u8.iter() {
            let len = cmd_u8.len();
            let cmd: CommandData = rmp_serde::from_slice(cmd_u8)?;
            if cmd.get_key().eq(key) {
                return Ok(Some(CommandPackage::new(cmd, pos, len)));
            }
            // 对pos进行长度自增并对占位符进行跳过
            pos += len as u64 + 4;
        }
        Ok(None)
    }

    /// 获取reader之中所有的Command
    pub async fn from_read_to_vec(io_handler: &IOHandler) -> Result<Vec<CommandPackage>> {
        let len = io_handler.file_size().await? as usize;
        let zone = io_handler.read_with_pos(0, len).await?;
        Self::from_zone_to_vec(zone.as_slice()).await
    }

    /// 获取此reader的所有命令对应的字节数组段落
    /// 返回字节数组Vec与对应的字节数组长度Vec
    pub async fn get_vec_bytes(zone: &[u8]) -> Vec<&[u8]> {

        let mut vec_cmd_u8 = Vec::new();
        let mut last_pos = 0;
        if zone.len() < 4 {
            return vec_cmd_u8;
        }

        loop {
            let pos = last_pos + 4;
            if pos >= zone.len() {
                break
            }
            let len_u8 = &zone[last_pos..pos];
            let len = Self::from_4_bit_with_start(len_u8);
            if len < 1 || len > zone.len() {
                break
            }

            last_pos += len + 4;
            vec_cmd_u8.push(&zone[pos..last_pos]);
        }

        vec_cmd_u8
    }

    /// 从u8的slice中前四位获取数据的长度
    fn from_4_bit_with_start(len_u8: &[u8]) -> usize {
        usize::from(len_u8[3])
            | usize::from(len_u8[2]) << 8
            | usize::from(len_u8[1]) << 16
            | usize::from(len_u8[0]) << 24
    }
}

impl CommandData {

    pub fn get_key(&self) -> &Vec<u8> {
        match self {
            CommandData::Set { key, .. } => { key }
            CommandData::Remove { key } => { key }
            CommandData::Get { key } => { key }
        }
    }

    pub fn get_key_clone(&self) -> Vec<u8> {
        match self {
            CommandData::Set { key, .. } => { key }
            CommandData::Remove { key } => { key }
            CommandData::Get { key } => { key }
        }.clone()
    }

    pub fn get_key_owner(self) -> Vec<u8> {
        match self {
            CommandData::Set { key, .. } => { key }
            CommandData::Remove { key } => { key }
            CommandData::Get { key } => { key }
        }
    }

    pub fn get_value(&self) -> Option<&Vec<u8>> {
        match self {
            CommandData::Set { value, .. } => { Some(value) }
            _ => { None }
        }
    }

    pub fn get_value_clone(&self) -> Option<Vec<u8>> {
        match self {
            CommandData::Set { value, .. } => { Some(value.clone()) }
            _ => { None }
        }
    }

    pub fn get_value_owner(self) -> Option<Vec<u8>> {
        match self {
            CommandData::Set { value, .. } => { Some(value) }
            _ => { None }
        }
    }

    pub fn get_data_len_for_rmp(&self) -> usize {
        self.get_key().len()
            + self.get_value().map_or(0, |value| value.len())
            + self.get_cmd_len_for_rmp()
    }

    /// 使用下方的测试方法对每个指令类型做测试得出的常量值
    /// 测试并非为准确的常量值，但大多数情况下该值，且极端情况下也不会超过该值
    pub fn get_cmd_len_for_rmp(&self) -> usize {
        match self {
            CommandData::Set { .. } => { 10 }
            CommandData::Remove { .. } => { 12 }
            CommandData::Get { .. } => { 9 }
        }
    }

    /// 命令消费
    ///
    /// Command对象通过调用这个方法调用持久化内核进行命令交互
    /// 参数Arc<RwLock<KvStore>>为持久化内核
    /// 内部对该类型进行模式匹配而进行不同命令的相应操作
    pub async fn apply<K: KVStore>(self, kv_store: &K) -> Result<CommandOption>{
        match self {
            CommandData::Set { key, value } => {
                kv_store.set(&key, value).await.map(|_| CommandOption::None)
            }
            CommandData::Remove { key } => {
                kv_store.remove(&key).await.map(|_| CommandOption::None)
            }
            CommandData::Get { key } => {
                kv_store.get(&key).await.map(CommandOption::from)
            }
        }
    }

    pub fn set(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self::Set { key, value }
    }

    pub fn remove(key: Vec<u8>) -> Self {
        Self::Remove { key }
    }

    pub fn get(key: Vec<u8>) -> Self {
        Self::Get { key }
    }
}

/// Option<String>与CommandOption的转换方法
/// 能够与CommandOption::None或CommandOption::Value进行转换
impl From<Option<Vec<u8>>> for CommandOption {
    fn from(item: Option<Vec<u8>>) -> Self {
        match item {
            None => CommandOption::None,
            Some(vec) => CommandOption::Value(vec)
        }
    }
}

/// 现有日志文件序号排序
async fn sorted_gen_list(path: &Path) -> Result<Vec<i64>> {
    // 读取文件夹路径
    // 获取该文件夹内各个文件的地址
    // 判断是否为文件并判断拓展名是否为log
    //  对文件名进行字符串转换
    //  去除.log后缀
    //  将文件名转换为u64
    // 对数组进行拷贝并收集
    let mut gen_list: Vec<i64> = fs::read_dir(path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<i64>)
        })
        .flatten().collect();
    // 对序号进行排序
    gen_list.sort_unstable();
    // 返回排序好的Vec
    Ok(gen_list)
}

/// 对文件夹路径填充日志文件名
fn log_path(dir: &Path, gen: i64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

// #[test]
// fn test_cmd_len() -> Result<()>{
//     use tracing::info;
//
//     let data_big_p = CommandData::set(b"ABCDEFGHIJKLMNOPQRSTUVWXYZ
//                             abcdefghijklmnopqrstuvwxyz
//                             0123456789)(*&^%$#@!~aopsdjqwpejopwqnapodfjcposzpodadqwpempqownponrpqwojerpoqwepqmweop
//                             qwejpqowjepoqwjeoqwepoq".to_vec(), vec![b'1']);
//
//     let data_big = CommandData::set(b"ABCDEFGHIJKLMNOPQRSTUVWXYZ
//                             abcdefghijklmnopqrstuvwxyz
//                             0123456789)(*&^%$#@!~".to_vec(), vec![b'1']);
//
//     let data_middle = CommandData::set(b"ABCDEFGHIJKLMNOPQRSTUVWXYZ
//                             abcd".to_vec(), vec![b'1']);
//
//     let data_smail = CommandData::set(vec![b'1'], vec![b'1']);
//
//     let data_smail_len = data_smail.get_data_len_for_rmp();
//     let data_middle_len = data_middle.get_data_len_for_rmp();
//     let data_big_len = data_big.get_data_len_for_rmp();
//     let data_big_p_len = data_big_p.get_data_len_for_rmp();
//     let data0_len = rmp_serde::to_vec(&CommandData::get(vec![]))?.len();
//     let data1_len = rmp_serde::to_vec(&data_smail)?.len() - data_smail_len;
//     let data2_len = rmp_serde::to_vec(&data_middle)?.len() - data_middle_len;
//     let data3_len = rmp_serde::to_vec(&data_big)?.len() - data_big_len;
//     let data4_len = rmp_serde::to_vec(&data_big_p)?.len() - data_big_p_len;
//     let cmd_len = vec![data0_len, data1_len, data2_len, data3_len, data4_len].into_iter()
//         .counts().into_iter()
//         .max_by_key(|(_, count)| count.clone())
//         .map(|(len, _)| len).unwrap();
//     info!("{}", cmd_len);
//
//     Ok(())
// }