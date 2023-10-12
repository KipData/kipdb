use async_trait::async_trait;
use bytes::Bytes;
use fslock::LockFile;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::path::Path;
use std::time::Duration;
use std::{fs, path::PathBuf};
use tokio::time;

use crate::kernel::io::FileExtension;
use crate::KernelError;

pub mod io;
pub mod lsm;
pub mod sled_storage;
pub mod utils;

pub type Result<T> = std::result::Result<T, KernelError>;

pub(crate) const DEFAULT_LOCK_FILE: &str = "KipDB.lock";

/// KV持久化内核 操作定义
#[async_trait]
pub trait Storage: Send + Sync + 'static + Sized {
    /// 获取内核名
    fn name() -> &'static str
    where
        Self: Sized;

    /// 通过数据目录路径开启数据库
    async fn open(path: impl Into<PathBuf> + Send) -> Result<Self>;

    /// 强制将数据刷入硬盘
    async fn flush(&self) -> Result<()>;

    /// 设置键值对
    async fn set(&self, key: Bytes, value: Bytes) -> Result<()>;

    /// 通过键获取对应的值
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;

    /// 通过键删除键值对
    async fn remove(&self, key: &[u8]) -> Result<()>;

    /// 并行批量执行
    /// TODO need refactor
    // #[inline]
    // async fn join(&self, vec_cmd: Vec<CommandData>) -> Result<Vec<Option<Vec<u8>>>> {}

    async fn size_of_disk(&self) -> Result<u64>;

    async fn len(&self) -> Result<usize>;

    async fn is_empty(&self) -> bool;
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
#[non_exhaustive]
pub enum CommandData {
    Set { key: Vec<u8>, value: Vec<u8> },
    Remove { key: Vec<u8> },
    Get { key: Vec<u8> },
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
                break;
            }
            let len_u8 = &bytes[last_pos..pos];
            let len = Self::from_4_bit_with_start(len_u8);
            if len < 1 || len > bytes.len() {
                break;
            }

            last_pos += len + 4;
            vec_cmd_u8.push(&bytes[pos..last_pos]);
        }

        vec_cmd_u8
    }

    /// 标记bytes以支持'ByteUtils::sharding_tag_bytes'方法
    pub(crate) fn tag_with_head(mut bytes: Vec<u8>) -> Vec<u8> {
        let i = bytes.len();
        let mut vec_head = vec![(i >> 24) as u8, (i >> 16) as u8, (i >> 8) as u8, i as u8];
        vec_head.append(&mut bytes);
        vec_head
    }
}

impl CommandData {
    #[inline]
    pub fn get_key(&self) -> &Vec<u8> {
        match self {
            CommandData::Set { key, .. } => key,
            CommandData::Remove { key } => key,
            CommandData::Get { key } => key,
        }
    }

    #[inline]
    pub fn get_key_clone(&self) -> Vec<u8> {
        self.get_key().clone()
    }

    #[inline]
    pub fn get_key_owner(self) -> Vec<u8> {
        match self {
            CommandData::Set { key, .. } => key,
            CommandData::Remove { key } => key,
            CommandData::Get { key } => key,
        }
    }

    #[inline]
    pub fn get_value(&self) -> Option<&Vec<u8>> {
        match self {
            CommandData::Set { value, .. } => Some(value),
            CommandData::Remove { .. } | CommandData::Get { .. } => None,
        }
    }

    #[inline]
    pub fn get_value_clone(&self) -> Option<Vec<u8>> {
        match self {
            CommandData::Set { value, .. } => Some(Vec::clone(value)),
            CommandData::Remove { .. } | CommandData::Get { .. } => None,
        }
    }

    #[inline]
    pub fn bytes_len(&self) -> usize {
        self.get_key().len()
            + self.get_value().map_or(0, Vec::len)
            + match self {
                CommandData::Set { .. } => 20,
                CommandData::Remove { .. } => 12,
                CommandData::Get { .. } => 12,
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

/// 现有日志文件序号排序
fn sorted_gen_list(file_path: &Path, extension: FileExtension) -> Result<Vec<i64>> {
    let mut gen_list: Vec<i64> = fs::read_dir(file_path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| {
            path.is_file() && path.extension() == Some(extension.extension_str().as_ref())
        })
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(format!(".{}", extension.extension_str()).as_str()))
                .map(str::parse::<i64>)
        })
        .flatten()
        .collect();
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
            return Ok(lock_file);
        } else if backoff > 4 {
            return Err(KernelError::ProcessExists);
        } else {
            time::sleep(Duration::from_millis(backoff * 100)).await;

            backoff *= 2;
        }
    }
}
