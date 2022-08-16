use std::{io::{Write, self}, path::PathBuf, fs::File};
use std::io::Read;
use memmap2::{Mmap, MmapMut};

use crate::cmd::Command;

use crate::KvsError;

pub mod hash_kv;

pub type Result<T> = std::result::Result<T, KvsError>;

const DELIMITER_BYTE: &u8 = &b'\0';

/// KV持久化内核 操作定义
pub trait KVStore {
    /// 通过路径打开文件
    fn open(path: impl Into<PathBuf>) -> Result<Self> where Self:Sized ;

    /// 强制将数据刷入硬盘
    fn flush(&mut self) -> Result<()>;

    /// 设置键值对
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// 通过键获取对应的值
    fn get(&self, key: String) -> Result<Option<String>>;

    /// 通过键删除键值对
    fn remove(&mut self, key: String) -> Result<()>;

    /// 持久化内核关闭处理
    fn shut_down(&mut self) ->Result<()>;
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
            let vec_cmd_u8: Vec<&[u8]> = self.mmap[..last].split(|byte| byte.eq(DELIMITER_BYTE)).collect();
            let vec_cmd_len: Vec<usize> = vec_cmd_u8.iter().map(|item| item.len()).collect();

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
    cmd: Command,
    pos: usize,
    len: usize
}

impl CommandPackage {
    /// 实例化一个Command
    pub fn new(cmd: Command, pos: usize, len: usize) -> Self {
        CommandPackage{ cmd, pos, len }
    }

    /// 写入一个Command
    pub fn write<W>(wr: &mut W, cmd: &Command) -> Result<()> where W: Write + ?Sized, {
        let mut vec = rmp_serde::encode::to_vec(cmd)?;
        vec.push(b'\0');
        wr.write(&*vec)?;
        Ok(())
    }

    /// 以reader使用两个pos读取范围之中的单个Command
    pub fn form_pos(reader : &MmapReader, start: usize, end: usize) -> Result<CommandPackage> {
        let cmd_u8 = reader.read_zone(start, end)?;
        let cmd: Command = rmp_serde::decode::from_slice(cmd_u8)?;
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
                let cmd: Command = rmp_serde::decode::from_slice(cmd_u8)?;
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