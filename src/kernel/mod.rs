use std::{path::PathBuf, fs};
use std::ffi::OsStr;
use std::path::Path;
use std::time::Duration;
use serde::{Deserialize, Serialize};
use async_trait::async_trait;
use fslock::LockFile;
use futures::future;
use itertools::Itertools;
use tokio::time;

use crate::kernel::io::{FileExtension, IoReader, IoWriter};
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
    Set { key: Vec<u8>, value: Vec<u8> },
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

    /// 实例化一个Command
    pub(crate) fn new(cmd: CommandData, pos: u64, len: usize) -> Self {
        CommandPackage{ cmd, pos, len }
    }

    /// 写入一个Command
    /// 写入完成后该cmd的去除len位置的写入起始位置与长度
    pub(crate) fn write(writer: &mut dyn IoWriter, cmd: &CommandData) -> Result<(u64, usize)> {
        let (start, len) = writer.io_write(
            ByteUtils::tag_with_head(bincode::serialize(cmd)?)
        )?;
        Ok((start + 4, len - 4))
    }

    /// IOHandler的对应Gen，以起始位置与长度使用的单个Command，不进行CommandPackage包装
    pub(crate) fn from_pos_unpack(reader: &dyn IoReader, start: u64, len: usize) -> Result<Option<CommandData>> {
        let cmd_u8 = reader.read_with_pos(start, len)?;
        Ok(bincode::deserialize(cmd_u8.as_slice()).ok())
    }

    /// 获取reader之中所有的CommandPackage
    pub(crate) fn from_read_to_vec(reader: &dyn IoReader) -> Result<Vec<CommandPackage>> {
        Self::from_bytes_to_vec(
            reader.bytes()?
                .as_slice()
        )
    }

    /// 获取bytes之中所有的CommandPackage
    pub(crate) fn from_bytes_to_vec(bytes: &[u8]) -> Result<Vec<CommandPackage>> {
        let mut pos = 4;
        Ok(ByteUtils::sharding_tag_bytes(bytes).into_iter()
            .filter_map(|cmd_u8| {
                let len = cmd_u8.len();
                let option = bincode::deserialize::<CommandData>(cmd_u8).ok()
                    .map(|cmd_data| CommandPackage::new(cmd_data, pos, len));
                // 对pos进行长度自增并对占位符进行跳过
                pos += len as u64 + 4;
                option
            })
            .collect_vec())
    }
}

pub(crate) struct ByteUtils;

impl ByteUtils {
    /// 从u8的slice中前四位获取数据的长度
    pub(crate) fn from_4_bit_with_start(len_u8: &[u8]) -> usize {
        usize::from(len_u8[3])
            | usize::from(len_u8[2]) << 8
            | usize::from(len_u8[1]) << 16
            | usize::from(len_u8[0]) << 24
    }

    /// 返回字节数组Vec与对应的字节数组长度Vec
    ///
    /// bytes必须使用'ByteUtils::tag_with_head'进行标记
    pub(crate) fn sharding_tag_bytes(bytes: &[u8]) -> Vec<&[u8]> {
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

    /// 标记bytes以支持'ByteUtils::sharding_tag_bytes'方法
    pub(crate) fn tag_with_head(mut bytes: Vec<u8>) -> Vec<u8> {
        let i = bytes.len();
        let mut vec_head = vec![(i >> 24) as u8,
                                (i >> 16) as u8,
                                (i >> 8) as u8,
                                i as u8];
        vec_head.append(&mut bytes);
        vec_head
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
            CommandData::Set { value, .. } => { Some(Vec::clone(value)) }
            CommandData::Remove{ .. } | CommandData::Get{ .. } => { None }
        }
    }

    #[inline]
    pub fn bytes_len(&self) -> usize {
        self.get_key().len()
            + self.get_value().map_or(0, Vec::len)
            + match self {
                CommandData::Set { .. } => { 20 }
                CommandData::Remove { .. } => { 12 }
                CommandData::Get { .. } => { 12 }
            }
    }

    /// 命令消费
    ///
    /// Command对象通过调用这个方法调用持久化内核进行命令交互
    /// 内部对该类型进行模式匹配而进行不同命令的相应操作
    #[inline]
    pub async fn apply<K: KVStore>(self, kv_store: &K) -> Result<CommandOption>{
        match self {
            CommandData::Set { key, value } => {
                kv_store.set(&key, value).await.map(|_| options_none())
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
        Self::Set { key, value }
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
            _ => CommandData::Set { key, value }
        }
    }
}

impl From<CommandData> for KeyValue {
    #[inline]
    fn from(cmd_data: CommandData) -> Self {
        match cmd_data {
            CommandData::Set { key, value } => KeyValue {
                key,
                value,
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

/// 尝试锁定文件或超时
async fn lock_or_time_out(path: &PathBuf) -> Result<LockFile> {
    let mut lock_file = LockFile::open(path)?;

    let mut backoff = 1;

    loop {
        if lock_file.try_lock()? {
            return Ok(lock_file)
        } else if backoff > 4 {
            return Err(KvsError::ProcessExistsError);
        } else {
            time::sleep(Duration::from_millis(backoff * 100)).await;

            backoff *= 2;
        }
    };
}