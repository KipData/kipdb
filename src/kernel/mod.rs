use std::{path::PathBuf, fs};
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use async_trait::async_trait;
use futures::future;
use itertools::Itertools;

use crate::kernel::io::{FileExtension, IOHandler};
use crate::KvsError;
use crate::proto::net_pb::{CommandOption, KeyValue};

pub mod hash_kv;
pub mod sled_kv;
pub mod lsm;
pub mod io;
pub mod utils;

pub type Result<T> = std::result::Result<T, KvsError>;

pub(crate) const DEFAULT_LOCK_FILE: &str = "KipDB.lock";

/// KV持久化内核 操作定义
#[async_trait]
pub trait KVStore: Send + Sync + 'static + Sized {
    /// 获取内核名
    fn name() -> &'static str where Self: Sized;

    /// 通过数据目录路径开启数据库
    async fn open(path: impl Into<PathBuf> + Send) -> Result<Self>;

    /// 强制将数据刷入硬盘
    async fn flush(&self) -> Result<()>;

    /// 设置键值对
    async fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()>;

    /// 通过键获取对应的值
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// 通过键删除键值对
    async fn remove(&self, key: &[u8]) -> Result<()>;

    /// 并行批量执行
    #[inline]
    async fn batch(&self, vec_cmd: Vec<CommandData>) -> Result<Vec<Option<Vec<u8>>>> {
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

    async fn is_empty(&self) -> bool;
}

/// 用于包装Command交予持久化核心实现使用的操作类
#[derive(Debug)]
pub(crate) struct CommandPackage {
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
#[non_exhaustive]
pub enum CommandData {
    Set { key: Vec<u8>, value: Arc<Vec<u8>> },
    Remove { key: Vec<u8> },
    Get { key: Vec<u8> }
}

impl CommandPos {
    /// 重写自身数据
    pub(crate) fn change(&mut self, file_gen: i64, pos: u64, len: usize) {
        self.gen = file_gen;
        self.pos = pos;
        self.len = len;
    }
}

impl CommandPackage {

    pub(crate) fn encode(cmd: &CommandData) -> Result<Vec<u8>> {
        Ok(rmp_serde::to_vec(cmd)?)
    }

    pub(crate) fn decode(vec: &[u8]) -> Result<CommandData> {
        Ok(rmp_serde::from_slice(vec)?)
    }

    /// 实例化一个Command
    pub(crate) fn new(cmd: CommandData, pos: u64, len: usize) -> Self {
        CommandPackage{ cmd, pos, len }
    }

    /// 写入一个Command
    /// 写入完成后该cmd的去除len位置的写入起始位置与长度
    pub(crate) async fn write(io_handler: &Box<dyn IOHandler>, cmd: &CommandData) -> Result<(u64, usize)> {
        let (start, len) = Self::write_back_real_pos(io_handler, cmd).await?;
        Ok((start + 4, len - 4))
    }

    /// 写入一个Command
    /// 写入完成后该cmd的真实写入起始位置与长度
    pub(crate) async fn write_back_real_pos(io_handler: &Box<dyn IOHandler>, cmd: &CommandData) -> Result<(u64, usize)> {
        io_handler.write(Self::trans_to_vec_u8(cmd)?).await
    }

    #[allow(dead_code)]
    pub(crate) async fn write_batch(
        io_handler: &Box<dyn IOHandler>,
        vec_cmd: &Vec<CommandData>
    ) -> Result<(u64, usize)> {
        let bytes = vec_cmd.iter()
            .filter_map(|cmd_data| Self::trans_to_vec_u8(cmd_data).ok())
            .flatten()
            .collect_vec();

        io_handler.write(bytes).await
    }

    /// 将数据分片集成写入， 返回起始Pos、整段写入Pos、每段数据序列化长度Pos
    pub(crate) async fn write_batch_first_pos_with_sharding(
        io_handler: &Box<dyn IOHandler>,
        vec_sharding: &Vec<Vec<CommandData>>
    ) -> Result<(u64, usize, Vec<usize>, u32)>{
        let mut vec_sharding_len = Vec::with_capacity(vec_sharding.len());

        let vec_sharding_u8  = vec_sharding.iter()
            .flat_map(|sharding| {
                let sharding_u8 = sharding.iter()
                    .filter_map(|cmd_data| Self::trans_to_vec_u8(cmd_data).ok())
                    .flatten()
                    .collect_vec();
                vec_sharding_len.push(sharding_u8.len());
                sharding_u8
            })
            .collect_vec();

        let crc_code = crc32fast::hash(vec_sharding_u8.as_slice());

        let (start_pos, batch_len) = io_handler.write(vec_sharding_u8).await?;

        Ok((start_pos, batch_len, vec_sharding_len, crc_code))
    }

    pub(crate) fn trans_to_vec_u8(cmd: &CommandData) -> Result<Vec<u8>> {
        let mut vec = rmp_serde::to_vec(cmd)?;
        let i = vec.len();
        let mut vec_head = vec![(i >> 24) as u8,
                                (i >> 16) as u8,
                                (i >> 8) as u8,
                                i as u8];
        vec_head.append(&mut vec);
        Ok(vec_head)
    }

    /// IOHandler的对应Gen，以起始位置与长度使用的单个Command，不进行CommandPackage包装
    pub(crate) async fn from_pos_unpack(io_handler: &Box<dyn IOHandler>, start: u64, len: usize) -> Result<Option<CommandData>> {
        let cmd_u8 = io_handler.read_with_pos(start, len).await?;
        Ok(rmp_serde::from_slice(cmd_u8.as_slice()).ok())
    }

    /// 获取bytes之中所有的CommandPackage
    pub(crate) fn from_bytes_to_vec(bytes: &[u8]) -> Result<Vec<CommandPackage>> {
        let mut pos = 4;
        Ok(Self::get_vec_bytes(bytes).into_iter()
            .filter_map(|cmd_u8| {
                let len = cmd_u8.len();
                let option = rmp_serde::from_slice::<CommandData>(cmd_u8).ok()
                    .map(|cmd_data| CommandPackage::new(cmd_data, pos, len));
                // 对pos进行长度自增并对占位符进行跳过
                pos += len as u64 + 4;
                option
            })
            .collect_vec())
    }

    /// 获取bytes之中所有的CommandData
    pub(crate) fn from_bytes_to_unpack_vec(bytes: &[u8]) -> Result<Vec<CommandData>> {
        Ok(Self::get_vec_bytes(bytes).into_iter()
            .filter_map(|cmd_u8| rmp_serde::from_slice(cmd_u8).ok())
            .collect_vec())
    }

    /// 获取reader之中所有的CommandPackage
    pub(crate) async fn from_read_to_vec(io_handler: &Box<dyn IOHandler>) -> Result<Vec<CommandPackage>> {
        Self::from_bytes_to_vec(io_handler
            .bytes()
            .await?
            .as_slice())
    }

    #[allow(dead_code)]
    /// 获取reader之中所有的CommandData
    pub(crate) async fn from_read_to_unpack_vec(io_handler: &Box<dyn IOHandler>) -> Result<Vec<CommandData>> {
        Self::from_bytes_to_unpack_vec(io_handler
            .bytes()
            .await?
            .as_slice())
    }

    /// 获取此reader的所有命令对应的字节数组段落
    /// 返回字节数组Vec与对应的字节数组长度Vec
    pub(crate) fn get_vec_bytes(bytes: &[u8]) -> Vec<&[u8]> {
        let mut vec_cmd_u8 = Vec::new();
        let mut last_pos = 0;
        if bytes.len() < 4 {
            return vec_cmd_u8;
        }

        loop {
            let pos = last_pos + 4;
            if pos >= bytes.len() {
                break
            }
            let len_u8 = &bytes[last_pos..pos];
            let len = Self::from_4_bit_with_start(len_u8);
            if len < 1 || len > bytes.len() {
                break
            }

            last_pos += len + 4;
            vec_cmd_u8.push(&bytes[pos..last_pos]);
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

    #[inline]
    pub fn get_key(&self) -> &Vec<u8> {
        match self {
            CommandData::Set { key, .. } => { key }
            CommandData::Remove { key } => { key }
            CommandData::Get { key } => { key }
        }
    }

    #[inline]
    pub fn get_key_clone(&self) -> Vec<u8> {
        self.get_key().clone()
    }

    #[inline]
    pub fn get_key_owner(self) -> Vec<u8> {
        match self {
            CommandData::Set { key, .. } => { key }
            CommandData::Remove { key } => { key }
            CommandData::Get { key } => { key }
        }
    }

    #[inline]
    pub fn get_value(&self) -> Option<&Vec<u8>> {
        match self {
            CommandData::Set { value, .. } => { Some(value) }
            CommandData::Remove{ .. } | CommandData::Get{ .. } => { None }
        }
    }

    #[inline]
    pub fn get_value_clone(&self) -> Option<Vec<u8>> {
        match self {
            CommandData::Set { value, .. } => { Some(Vec::clone(&value)) }
            CommandData::Remove{ .. } | CommandData::Get{ .. } => { None }
        }
    }

    #[inline]
    pub fn get_data_len_for_rmp(&self) -> usize {
        self.get_key().len()
            + self.get_value().map_or(0, Vec::len)
            + self.get_cmd_len_for_rmp()
    }

    /// 使用下方的测试方法对每个指令类型做测试得出的常量值
    /// 测试并非为准确的常量值，但大多数情况下该值，且极端情况下也不会超过该值
    #[inline]
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
    #[inline]
    pub async fn apply<K: KVStore>(self, kv_store: &K) -> Result<CommandOption>{
        match self {
            CommandData::Set { key, value } => {
                kv_store.set(&key, value_unwrap(value)).await.map(|_| options_none())
            }
            CommandData::Remove { key } => {
                kv_store.remove(&key).await.map(|_| options_none())
            }
            CommandData::Get { key } => {
                kv_store.get(&key).await.map(CommandOption::from)
            }
        }
    }

    #[inline]
    pub fn set(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self::Set { key, value: Arc::new(value) }
    }

    #[inline]
    pub fn remove(key: Vec<u8>) -> Self {
        Self::Remove { key }
    }

    #[inline]
    pub fn get(key: Vec<u8>) -> Self {
        Self::Get { key }
    }
}

pub(crate) fn options_none() -> CommandOption {
    CommandOption { r#type: 7, bytes: vec![], value: 0 }
}

impl From<KeyValue> for CommandData {
    #[inline]
    fn from(key_value: KeyValue) -> Self {
        let KeyValue { r#type, key, value } = key_value;
        match r#type {
            0 => CommandData::Get { key },
            2 => CommandData::Remove { key },
            _ => CommandData::Set { key, value: Arc::new(value) }
        }
    }
}

impl From<CommandData> for KeyValue {
    #[inline]
    fn from(cmd_data: CommandData) -> Self {
        match cmd_data {
            CommandData::Set { key, value } => KeyValue {
                key,
                value: value_unwrap(value),
                r#type: 1,
            },
            CommandData::Remove { key } => KeyValue {
                key,
                value: vec![],
                r#type: 2,
            },
            CommandData::Get { key } => KeyValue {
                key,
                value: vec![],
                r#type: 0,
            },
        }
    }
}

/// Option<String>与CommandOption的转换方法
/// 能够与CommandOption::None或CommandOption::Value进行转换
impl From<CommandOption> for Option<Vec<u8>> {
    #[inline]
    fn from(item: CommandOption) -> Self {
        (item.r#type == 2).then_some(item.bytes)
    }
}

impl From<Option<Vec<u8>>> for CommandOption {
    #[inline]
    fn from(item: Option<Vec<u8>>) -> Self {
        match item {
            Some(bytes) => CommandOption { r#type: 2, bytes, value: 0 },
            None => options_none()
        }
    }
}

/// 用于对解除Arc指针
/// 确保value此时无其他数据引用
fn value_unwrap(value: Arc<Vec<u8>>) -> Vec<u8> {
    Arc::try_unwrap(value).unwrap()
}

/// 现有日志文件序号排序
fn sorted_gen_list(file_path: &Path, extension: FileExtension) -> Result<Vec<i64>> {
    let mut gen_list: Vec<i64> = fs::read_dir(file_path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some(extension.extension_str().as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(format!(".{}", extension.extension_str()).as_str()))
                .map(str::parse::<i64>)
        })
        .flatten().collect();
    // 对序号进行排序
    gen_list.sort_unstable();
    // 返回排序好的Vec
    Ok(gen_list)
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