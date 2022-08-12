use std::{io::{Read, Write, self}, path::{PathBuf, Path}, collections::HashMap, fs::{File, self, OpenOptions}, ffi::OsStr};

use memmap2::{Mmap, MmapMut};
use serde_json::Deserializer;

use crate::{error::{KvsError}};
use crate::cmd::Command;
use crate::core::{KVStore, Result};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

pub struct KvCore {
    path: PathBuf,
    readers: HashMap<u64, MmapReader>,
    index: HashMap<String, CommandPos>,
    current_gen: u64,
    uncompacted: u64,
}

impl KvCore {

    /// 核心压缩方法
    /// 通过compaction_gen决定压缩位置
    fn compact(
        &mut self,
        compaction_gen: u64
    ) -> Result<()> {
        let mut compaction_writer = self.new_log_file(compaction_gen)?;
        // 新的写入位置为原位置的向上两位
        self.current_gen = compaction_gen + 1;

        // 初始化新的写入地址
        let mut new_pos = 0 as usize;
        for cmd_pos in &mut self.index.values_mut() {
            let pos = cmd_pos.pos;
            let cmd_len = cmd_pos.len as usize;
            let last_pos = pos + cmd_len;

            let reader = self.readers.get_mut(&cmd_pos.gen)
                .expect(format!("Can't find reader: {}", &cmd_pos.gen).as_str());
            let mut cmd_reader = reader.read_zone(pos, last_pos)?;
            let len = io::copy(&mut cmd_reader, &mut compaction_writer)? as usize;
            *cmd_pos = CommandPos {gen: compaction_gen, pos: new_pos, len: len as u64 };
            new_pos += len;
        }
        // 将所有写入刷入压缩文件中
        compaction_writer.flush()?;

        // 遍历过滤出小于压缩文件序号的文件号名收集为过期Vec
        let stale_gens: Vec<_> = self.readers.keys()
            .filter(|&&gen| gen < compaction_gen)
            .cloned().collect();

        // 遍历过期Vec对数据进行旧文件删除
        for stale_gen in stale_gens {
            self.readers.remove(&stale_gen);
            fs::remove_file(log_path(&self.path, stale_gen))?;
        }
        // 将压缩阈值调整为为压缩后大小
        self.uncompacted = new_pos as u64;

        Ok(())
    }

    // 新建日志文件方法参数封装
    fn new_log_file(&mut self, gen: u64) -> Result<MmapWriter> {
        new_log_file(&self.path, gen, &mut self.readers)
    }
}

/// The `HashKvStore` stores string key/value pairs.
pub struct HashKvStore {
    kv_core: KvCore,
    writer: MmapWriter
}

impl KVStore for HashKvStore {
    // 通过文件夹路径开启一个HashKvStore
    fn open(path: impl Into<PathBuf>) -> Result<HashKvStore> where Self: Sized {
        // 获取地址
        let path = path.into();
        // 创建文件夹（如果他们缺失）
        fs::create_dir_all(&path)?;
        // 创建读入器Map
        let mut readers = HashMap::<u64, MmapReader>::new();
        // 创建索引
        let mut index = HashMap::<String, CommandPos>::new();

        // 通过path获取有序的log序名Vec
        let gen_list = sorted_gen_list(&path)?;
        // 初始化压缩阈值
        let mut uncompacted = 0;

        // 对读入其Map进行初始化并计算对应的压缩阈值
        for &gen in &gen_list {
            let log_file = File::open(log_path(&path, gen))?;

            let mut mmap_kv = MmapReader::new(&log_file)?;
            uncompacted += load(gen, &mut mmap_kv, &mut index)? as u64;
            readers.insert(gen, mmap_kv);
        }
        // 获取当前最新的写入序名（之前的+1）
        let current_gen = gen_list.last().unwrap_or(&0) + 1;

        // 以最新的写入序名创建新的日志文件
        let new_writer = new_log_file(&path, current_gen + 1, &mut readers)?;
        let mut kv_core = KvCore {
            path,
            readers,
            index,
            current_gen,
            uncompacted
        };
        kv_core.compact(current_gen)?;

        Ok(HashKvStore{
            kv_core,
            writer: new_writer
        })
    }

    fn flush(&mut self) -> Result<()> {
        // 刷入文件中
        Ok(self.writer.flush()?)
    }

    /// 存入数据
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let core = &mut self.kv_core;
        //将数据包装为命令
        let cmd = Command::set(key, value);
        // 获取写入器当前地址
        let pos = self.writer.pos;
        // 以json形式写入该命令
        serde_json::to_writer(&mut self.writer, &cmd)?;
        // 当模式匹配cmd为正确时
        if let Command::Set { key, .. } = cmd {
            // 封装为CommandPos
            let cmd_pos = CommandPos {gen: core.current_gen, pos: pos as usize, len: self.writer.pos - pos };
            // 将封装ComandPos存入索引Map中
            if let Some(old_cmd) = core.index.insert(key, cmd_pos) {
                // 将阈值提升至该命令的大小
                core.uncompacted += old_cmd.len;
            }
        }
        // 阈值过高进行压缩
        if core.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?
        }

        Ok(())
    }

    /// 获取数据
    fn get(&self, key: String) -> Result<Option<String>> {
        let core = &self.kv_core;
        // 若index中获取到了该数据命令
        if let Some(cmd_pos) = core.index.get(&key) {
            // 从读取器Map中通过该命令的序号获取对应的日志读取器
            let reader = core.readers.get(&cmd_pos.gen)
                .expect(format!("Can't find reader: {}", &cmd_pos.gen).as_str());
            let last_pos = cmd_pos.pos + cmd_pos.len as usize;
            // 获取这段内容
            let cmd_reader = reader.read_zone(cmd_pos.pos, last_pos)?;
            // 将命令进行转换
            if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                //返回匹配成功的数据
                Ok(Some(value))
            } else {
                //返回错误（错误的指令类型）
                Err(KvsError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    /// 删除数据
    fn remove(&mut self, key: String) -> Result<()> {
        let core = &mut self.kv_core;
        // 若index中存在这个key
        if core.index.contains_key(&key) {
            // 对这个key做命令封装
            let cmd = Command::remove(key);
            // 将这条命令以json形式写入至当前日志文件
            serde_json::to_writer(&mut self.writer, &cmd)?;
            // 刷入文件中
            self.writer.flush()?;
            // 若cmd模式匹配成功则删除该数据
            if let Command::Remove { key } = cmd {
                core.index.remove(&key).expect("key not found");
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    fn shut_down(&mut self) -> Result<()> {
        Ok(self.writer.flush()?)
    }
}

impl HashKvStore {
    pub fn compact(&mut self) -> Result<()> {
        // 预压缩的数据位置为原文件位置的向上一位
        let core = & mut self.kv_core;
        let compaction_gen = core.current_gen + 1;
        self.writer.flush()?;
        self.writer = core.new_log_file(compaction_gen + 1)?;
        core.compact(compaction_gen)
    }
}

#[derive(Debug)]
struct CommandPos {
    gen: u64,
    pos: usize,
    len: u64,
}

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
}

impl Read for MmapReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let last_pos = self.pos;
        let len = (&self.mmap[last_pos..]).read(buf)?;
        self.pos += len;
        Ok(len)
    }
}

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

/// 通过目录地址加载数据
fn load(gen: u64, reader: &mut MmapReader, index: &mut HashMap<String, CommandPos>) -> Result<u64> {
    // 将读入器的地址初始化为0
    reader.pos = 0;
    // 流式读取将数据序列化为Command
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    // 初始化空间占用为0
    let mut uncompacted = 0;
    let mut pos = 0;
    // 迭代数据
    while let Some(cmd) = stream.next() {
        // 计算这段byte所在位置
        let new_pos = stream.byte_offset();
        if let Ok(cmd) = cmd {
            match cmd {
                Command::Set { key, .. } => {
                    //数据插入索引之中，成功则对空间占用值进行累加
                    if let Some(old_cmd) = index.insert(key, CommandPos {gen, pos, len: (new_pos - pos) as u64 }) {
                        uncompacted += old_cmd.len;
                    }
                }
                Command::Remove { key } => {
                    //索引删除该数据之中，成功则对空间占用值进行累加
                    if let Some(old_cmd) = index.remove(&key) {
                        uncompacted += old_cmd.len;
                    }
                    uncompacted += (new_pos - pos) as u64;
                }
                _ => {}
            }
        } else {
            break;
        }
        // 写入地址等于new_pos
        pos = new_pos;
    }
    Ok(uncompacted)
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
    file.set_len(COMPACTION_THRESHOLD).unwrap();  

    readers.insert(gen, MmapReader::new(&file)?);
    Ok(MmapWriter::new(&file)?)
}