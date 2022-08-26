use std::{io::Write, path::PathBuf, collections::HashMap, fs::{File, self}};
use std::cmp::Ordering;
use std::collections::HashSet;
use itertools::Itertools;

use crate::{error::{KvsError}};
use crate::kernel::{CommandData, CommandPackage, CommandPos, KVStore, log_path, MmapReader, MmapWriter, new_log_file_with_gen, Result, sorted_gen_list};

pub(crate) const DEFAULT_COMPACTION_THRESHOLD: u64 = 1024 * 1024 * 6;

pub(crate) const DEFAULT_REDUNDANCY_SIZE: u64 = 1024 * 1024 * 1;

/// The `HashKvStore` stores string key/value pairs.
pub struct HashStore {
    kv_core: KvCore,
    writer: MmapWriter
}

impl HashStore {
    /// 获取索引中的所有keys
    pub fn keys_from_index(&self) -> Result<Vec<&Vec<u8>>> {
        let keys = self.kv_core.index.keys()
            .collect_vec();
        Ok(keys)
    }

    /// 获取数据指令
    pub fn get_cmd_data(&self, key: &Vec<u8>) -> Result<Option<CommandData>> {
        let core = &self.kv_core;
        // 若index中获取到了该数据命令
        if let Some(cmd_pos) = core.index.get(key) {
            let package = Self::find_package_with_pos(core, &cmd_pos)?;
            Ok(Some(package.cmd))
        } else {
            Ok(None)
        }
    }

    fn find_package_with_pos(core: &KvCore, cmd_pos: &&CommandPos) -> Result<CommandPackage> {
        // 从读取器Map中通过该命令的序号获取对应的日志读取器
        let reader = core.readers.get(&cmd_pos.gen)
            .expect(format!("Can't find reader: {}", &cmd_pos.gen).as_str());
        let last_pos = cmd_pos.pos + cmd_pos.len as usize;
        // 获取这段内容
        CommandPackage::form_pos(reader, cmd_pos.pos, last_pos)
    }

    /// 通过目录路径启动数据库并指定压缩阈值
    pub fn open_with_compaction_threshold(path: impl Into<PathBuf>, compaction_threshold: u64, redundancy_size: u64) -> Result<Self> where Self: Sized {
        // 获取地址
        let path = path.into();
        // 创建文件夹（如果他们缺失）
        fs::create_dir_all(&path)?;
        // 创建读入器Map
        let mut readers = HashMap::<u64, MmapReader>::new();
        // 创建索引
        let mut index = HashMap::<Vec<u8>, CommandPos>::new();

        // 通过path获取有序的log序名Vec
        let gen_list = sorted_gen_list(&path)?;
        // 初始化压缩阈值
        let mut un_compacted = 0;

        // 对读入其Map进行初始化并计算对应的压缩阈值
        for &gen in &gen_list {
            let log_file = File::open(log_path(&path, gen))?;

            let mut mmap_kv = MmapReader::new(&log_file)?;
            un_compacted += load(gen, &mut mmap_kv, &mut index)? as u64;
            readers.insert(gen, mmap_kv);
        }
        // 获取当前最新的写入序名（之前的+1）
        let current_gen = gen_list.last().unwrap_or(&0) + 1;
        let file_size = compaction_threshold + redundancy_size;

        // 以最新的写入序名创建新的日志文件
        let (gen, writer, reader) = new_log_file_with_gen(&path, current_gen + 1, file_size)?;
        readers.insert(gen, reader);

        let mut kv_core = KvCore {
            path,
            readers,
            index,
            current_gen,
            un_compacted,
            compaction_threshold,
            file_size
        };
        kv_core.compact(current_gen)?;

        Ok(HashStore {
            kv_core,
            writer
        })
    }
}

impl KVStore for HashStore {
    fn name() -> &'static str where Self: Sized {
        "HashStore made in Kould"
    }

    // 通过文件夹路径开启一个HashKvStore
    fn open(path: impl Into<PathBuf>) -> Result<HashStore> where Self: Sized {
        HashStore::open_with_compaction_threshold(path, DEFAULT_COMPACTION_THRESHOLD, DEFAULT_REDUNDANCY_SIZE)
    }

    fn flush(&mut self) -> Result<()> {
        // 刷入文件中
        Ok(self.writer.flush()?)
    }

    /// 存入数据
    fn set(&mut self, key: &Vec<u8>, value: Vec<u8>) -> Result<()> {
        let core = &mut self.kv_core;

        //将数据包装为命令
        let cmd = CommandData::Set { key: key.clone(), value };
        // 获取写入器当前地址
        let pos = self.writer.get_cmd_data_pos();
        let new_pos = CommandPackage::write(&mut self.writer, &cmd)?;

        // 模式匹配获取key值
        if let CommandData::Set { key, .. } = cmd {
            // 封装为CommandPos
            let cmd_pos = CommandPos {gen: core.current_gen, pos, len: (new_pos - pos) as u64 };
            // 将封装CommandPos存入索引Map中
            if let Some(old_cmd) = core.index.insert(key, cmd_pos) {
                // 将阈值提升至该命令的大小
                core.un_compacted += old_cmd.len;
            }
        }
        // 阈值过高进行压缩
        if core.un_compacted > self.kv_core.compaction_threshold {
            self.compact()?
        }

        Ok(())
    }

    /// 获取数据
    fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>> {
        let core = &self.kv_core;
        // 若index中获取到了该数据命令
        if let Some(cmd_pos) = core.index.get(key) {
            // 获取这段内容
            let package = Self::find_package_with_pos(core, &cmd_pos)?;
            // 将命令进行转换
            if let CommandData::Set { value, .. } = package.cmd {
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
    fn remove(&mut self, key: &Vec<u8>) -> Result<()> {
        let core = &mut self.kv_core;
        let writer = &mut self.writer;
        // 若index中存在这个key
        if core.index.contains_key(key) {
            // 对这个key做命令封装
            let cmd = CommandData::Remove { key: key.to_vec() };
            CommandPackage::write(writer, &cmd)?;
            core.index.remove(key).expect("key not found");
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    fn shut_down(&mut self) -> Result<()> {
        Ok(self.writer.flush()?)
    }
}

impl HashStore {
    pub fn compact(&mut self) -> Result<()> {
        // 预压缩的数据位置为原文件位置的向上一位
        let core = & mut self.kv_core;
        let compaction_gen = core.current_gen + 1;
        self.writer.flush()?;
        self.writer = core.new_log_file(compaction_gen + 1)?;
        core.compact(compaction_gen)
    }
}

impl Drop for HashStore {
    fn drop(&mut self) {
        self.shut_down().unwrap();
    }
}

/// 通过目录地址加载数据并返回数据总大小
fn load(gen: u64, reader: &mut MmapReader, index: &mut HashMap<Vec<u8>, CommandPos>) -> Result<u64> {
    // 流式读取将数据序列化为Command
    let vec_package = CommandPackage::form_read_to_vec(reader)?;
    // 初始化空间占用为0
    let mut un_compacted = 0;
    // 迭代数据
    for package in vec_package {
        match package.cmd {
            CommandData::Set { key, .. } => {
                //数据插入索引之中，成功则对空间占用值进行累加
                if let Some(old_cmd) = index.insert(key, CommandPos {gen, pos: package.pos, len: package.len as u64 }) {
                    un_compacted += old_cmd.len + 1;
                }
            }
            CommandData::Remove { key } => {
                //索引删除该数据之中，成功则对空间占用值进行累加
                if let Some(old_cmd) = index.remove(&key) {
                    un_compacted += old_cmd.len + 1;
                };
            }
            _ => {}
        }
    }
    Ok(un_compacted)
}

pub struct KvCore {
    path: PathBuf,
    readers: HashMap<u64, MmapReader>,
    index: HashMap<Vec<u8>, CommandPos>,
    current_gen: u64,
    un_compacted: u64,
    compaction_threshold: u64,
    file_size: u64
}

impl KvCore {

    /// 核心压缩方法
    /// 通过compaction_gen决定压缩位置
    fn compact(&mut self, compaction_gen: u64) -> Result<()> {
        let mut compaction_writer = self.new_log_file(compaction_gen)?;
        // 新的写入位置为原位置的向上两位
        self.current_gen = compaction_gen + 1;

        // 压缩时对values进行顺序排序
        // 以gen,pos为最新数据的指标
        let mut vec_cmd_pos = self.index.values_mut()
            .sorted_unstable_by(|a, b| {
                match a.gen.cmp(&b.gen) {
                    Ordering::Less => Ordering::Less,
                    Ordering::Equal => a.pos.cmp(&b.pos),
                    Ordering::Greater => Ordering::Greater,
                }
            })
            .collect_vec();

        // 获取最后一位数据进行可容载数据的范围
        if let Some(last_cmd_pos) = vec_cmd_pos.last() {
            let last_pos = last_cmd_pos.pos + last_cmd_pos.len as usize;
            let skip_index = Self::get_max_new_pos(&vec_cmd_pos, last_pos, self.compaction_threshold);

            // 对skip_index进行旧数据跳过处理
            // 抛弃超过文件大小且数据写入时间最久的数据
            for (i, cmd_pos) in vec_cmd_pos.iter_mut().enumerate() {
                if i >= skip_index {
                    let pos = compaction_writer.get_cmd_data_pos();
                    let len = cmd_pos.len as usize;

                    let reader = self.readers.get_mut(&cmd_pos.gen)
                        .expect(format!("Can't find reader: {}", &cmd_pos.gen).as_str());

                    let cmd_package = CommandPackage::form_pos(reader, cmd_pos.pos, cmd_pos.pos + len)?;
                    CommandPackage::write(&mut compaction_writer, &cmd_package.cmd)?;

                    **cmd_pos = CommandPos { gen: compaction_gen, pos, len: cmd_package.len as u64 };
                }
            }

            // 将所有写入刷入压缩文件中
            compaction_writer.flush()?;
            // 遍历过滤出小于压缩文件序号的文件号名收集为过期Vec
            let stale_gens: HashSet<u64> = self.readers.keys()
                .filter(|&&gen| gen < compaction_gen)
                .cloned()
                .collect();

            // 清除索引中过期Key
            self.index.retain(|_, v| !stale_gens.contains(&v.gen));

            // 遍历过期Vec对数据进行旧文件删除
            for stale_gen in stale_gens.iter() {
                self.readers.remove(&stale_gen);
                fs::remove_file(log_path(&self.path, *stale_gen))?;
            }
            // 将压缩阈值调整为为压缩后大小
            self.un_compacted += compaction_writer.pos;
        }

        Ok(())
    }

    /// 获取可承载范围内最新的数据的起始索引
    /// 要求vec_cmd_pos是有序的
    fn get_max_new_pos(vec_cmd_pos: &Vec<&mut CommandPos>, last_pos: usize, compaction_threshold: u64) -> usize {
        for (i, item) in vec_cmd_pos.iter().enumerate() {
            if last_pos - item.pos < compaction_threshold as usize {
                return i;
            }
        }
        return 0;
    }

    // 新建日志文件方法参数封装
    fn new_log_file(&mut self, gen: u64) -> Result<MmapWriter> {
        let (gen, writer, reader) = new_log_file_with_gen(&self.path, gen, self.file_size)?;
        self.readers.insert(gen, reader);
        Ok(writer)
    }
}