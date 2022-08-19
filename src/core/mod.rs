use std::{io::{Write, self}, path::PathBuf, fs::File, fs};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use memmap2::{Mmap, MmapMut};
use tokio::sync::RwLock;

use crate::KvsError;
use crate::net::CommandOption;

pub mod hash_kv;

pub mod sled_kv;

pub type Result<T> = std::result::Result<T, KvsError>;

const DELIMITER_BYTE: &u8 = &b'\0';

const DEFAULT_COMPACTION_THRESHOLD: u64 = 1024 * 1024 * 6;

/// KV持久化内核 操作定义
pub trait KVStore {
    /// 获取内核名
    fn name() -> &'static str where Self: Sized;

    /// 通过数据目录路径开启数据库
    fn open(path: impl Into<PathBuf>) -> Result<Self> where Self:Sized;

    /// 强制将数据刷入硬盘
    fn flush(&mut self) -> Result<()>;

    /// 设置键值对
    fn set(&mut self, key: &Vec<u8>, value: Vec<u8>) -> Result<()>;

    /// 通过键获取对应的值
    fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>>;

    /// 通过键删除键值对
    fn remove(&mut self, key: &Vec<u8>) -> Result<()>;

    /// 持久化内核关闭处理
    fn shut_down(&mut self) ->Result<()>;
}

/// 基于mmap的读取器
struct MmapReader {
    mmap: Mmap,
    pos: usize
}

impl MmapReader {

    fn read_zone(&self, start: usize, end: usize) -> Result<&[u8]> {
        Ok(&self.mmap[start..end])
    }

    fn new(file: &File) -> Result<MmapReader> {
        let mmap = unsafe{ Mmap::map(file) }?;
        Ok(MmapReader{
            mmap,
            pos: 0
        })
    }

    /// 获取此reader的所有命令对应的字节数组段落
    /// 返回字节数组Vec与对应的字节数组长度Vec
    pub fn get_vec_bytes(&self) -> Option<(Vec<&[u8]>,Vec<usize>)> {
        if self.mmap[..].len() < 1 {
            return None;
        }

        let last = self.end_index();

        if last > 1 {
            let vec_cmd_u8: Vec<&[u8]> = self.mmap[..last]
                .split(|byte| byte.eq(DELIMITER_BYTE))
                .collect();
            let vec_cmd_len: Vec<usize> = vec_cmd_u8.iter()
                .map(|item| item.len())
                .collect();

            Some((vec_cmd_u8, vec_cmd_len))
        } else {
            None
        }
    }

    /// 获取有效数据的末尾位置
    pub fn end_index(&self) -> usize {
        for (i, &byte) in self.mmap[..].iter().enumerate() {
            if byte.eq(DELIMITER_BYTE) && i > 0  && self.mmap[i - 1].eq(&byte) {
                return i - 1;
            }
        }
        self.mmap.len() - 1
    }
}

impl Read for MmapReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let last_pos = self.pos;
        let len = (&self.mmap[last_pos..]).read(buf)?;
        self.pos += len;
        Ok(len)
    }
}

/// 基于mmap的写入器
struct MmapWriter {
    mmap_mut: MmapMut,
    pos: u64
}

impl MmapWriter {

    fn new(file: &File) -> Result<MmapWriter> {
        let mmap_mut = unsafe {
            MmapMut::map_mut(file)?
        };
        Ok(MmapWriter{
            pos: 0,
            mmap_mut
        })
    }
}

impl Write for MmapWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let last_pos = self.pos as usize;
        let len = (&mut self.mmap_mut[last_pos..]).write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.mmap_mut.flush()?;
        Ok(())
    }
}

/// 用于包装Command交予持久化核心实现使用的操作类
#[derive(Debug)]
struct CommandPackage {
    cmd: CommandData,
    pos: usize,
    len: usize
}

/// CommandPos Command磁盘指针
/// 用于标记对应Command的位置
/// gen 文件序号
/// pos 开头指针
/// len 命令长度
#[derive(Debug)]
struct CommandPos {
    gen: u64,
    pos: usize,
    len: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum CommandData {
    Set { key: Vec<u8>, value: Vec<u8> },
    Remove { key: Vec<u8> },
    Get { key: Vec<u8> }
}

impl CommandPackage {
    /// 实例化一个Command
    pub fn new(cmd: CommandData, pos: usize, len: usize) -> Self {
        CommandPackage{ cmd, pos, len }
    }

    /// 写入一个Command
    pub fn write<W>(wr: &mut W, cmd: &CommandData) -> Result<()> where W: Write + ?Sized, {
        let mut vec = rmp_serde::encode::to_vec(cmd)?;
        vec.push(b'\0');
        wr.write(&*vec)?;
        Ok(())
    }

    /// 以reader使用两个pos读取范围之中的单个Command
    pub fn form_pos(reader : &MmapReader, start: usize, end: usize) -> Result<CommandPackage> {
        let cmd_u8 = reader.read_zone(start, end)?;
        let cmd: CommandData = rmp_serde::decode::from_slice(cmd_u8)?;
        Ok(CommandPackage::new(cmd, start, end - start))
    }

    /// 获取reader之中所有的Command
    pub fn form_read_to_vec(reader : &mut MmapReader) -> Result<Vec<CommandPackage>>{
        // 将读入器的地址初始化为0
        reader.pos = 0;
        let mut vec: Vec<CommandPackage> = Vec::new();
        if let Some((vec_u8, vec_len)) = reader.get_vec_bytes() {
            let mut pos = 0;
            for (i, &cmd_u8) in vec_u8.iter().enumerate() {
                let len = vec_len.get(i).unwrap();
                let cmd: CommandData = rmp_serde::decode::from_slice(cmd_u8)?;
                vec.push(CommandPackage::new(cmd, pos, *len));
                // 对pos进行长度自增并对占位符进行跳过
                pos += len + 1;
                // 对占位符进行跳过
            }
            Ok(vec)
        } else {
            Ok(Vec::new())
        }
    }
}

impl CommandData {

    /// 命令消费
    ///
    /// Command对象通过调用这个方法调用持久化内核进行命令交互
    /// 参数Arc<RwLock<KvStore>>为持久化内核
    /// 内部对该类型进行模式匹配而进行不同命令的相应操作
    pub async fn apply(self, kv_store: &mut Arc<RwLock<dyn KVStore + Send + Sync>>) -> Result<CommandOption>{
        match self {
            CommandData::Set { key, value } => {
                let mut write_guard = kv_store.write().await;
                match write_guard.set(&key, value) {
                    Ok(_) => Ok(CommandOption::None),
                    Err(e) => Err(e)
                }
            }
            CommandData::Remove { key } => {
                let mut write_guard = kv_store.write().await;
                match write_guard.remove(&key) {
                    Ok(_) => Ok(CommandOption::None),
                    Err(e) => Err(e)
                }
            }
            CommandData::Get { key } => {
                let read_guard = kv_store.read().await;
                match read_guard.get(&key) {
                    Ok(option) => {
                        Ok(CommandOption::from(option))
                    }
                    Err(e) => Err(e)
                }
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
fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    // 读取文件夹路径
    // 获取该文件夹内各个文件的地址
    // 判断是否为文件并判断拓展名是否为log
    //  对文件名进行字符串转换
    //  去除.log后缀
    //  将文件名转换为u64
    // 对数组进行拷贝并收集
    let mut gen_list: Vec<u64> = fs::read_dir(path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten().collect();
    // 对序号进行排序
    gen_list.sort_unstable();
    // 返回排序好的Vec
    Ok(gen_list)
}

/// 对文件夹路径填充日志文件名
fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

/// 新建日志文件
/// 传入文件夹路径、日志名序号、读取器Map
/// 返回对应的写入器
fn new_log_file(path: &Path, gen: u64, readers: &mut HashMap<u64, MmapReader>) -> Result<MmapWriter> {
    // 得到对应日志的路径
    let path = log_path(path, gen);
    // 通过路径构造写入器
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(&path)?;
    file.set_len(DEFAULT_COMPACTION_THRESHOLD).unwrap();

    readers.insert(gen, MmapReader::new(&file)?);
    Ok(MmapWriter::new(&file)?)
}