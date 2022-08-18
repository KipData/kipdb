use std::{io::Write, path::PathBuf, collections::HashMap, fs::{File, self}};
use std::cmp::Ordering;
use itertools::Itertools;

use crate::{error::{KvsError}};
use crate::cmd::Command;
use crate::core::{CommandPackage, CommandPos, DEFAULT_COMPACTION_THRESHOLD, KVStore, load, log_path, MmapReader, MmapWriter, new_log_file, Result, sorted_gen_list};

pub struct KvCore {
    path: PathBuf,
    readers: HashMap<u64, MmapReader>,
    index: HashMap<String, CommandPos>,
    current_gen: u64,
    un_compacted: u64,
    compaction_threshold: u64,
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
                    let pos = compaction_writer.pos as usize;
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
            let stale_gens: Vec<_> = self.readers.keys()
                .filter(|&&gen| gen < compaction_gen)
                .cloned().collect();

            // 遍历过期Vec对数据进行旧文件删除
            for stale_gen in stale_gens {
                self.readers.remove(&stale_gen);
                fs::remove_file(log_path(&self.path, stale_gen))?;
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
        new_log_file(&self.path, gen, &mut self.readers)
    }
}

/// The `HashKvStore` stores string key/value pairs.
pub struct HashStore {
    kv_core: KvCore,
    writer: MmapWriter
}

impl HashStore {
    /// 通过目录路径启动数据库并指定压缩阈值
    fn open_with_compaction_threshold(path: impl Into<PathBuf>, compaction_threshold: u64) -> Result<Self> where Self: Sized {
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
            un_compacted: uncompacted,
            compaction_threshold
        };
        kv_core.compact(current_gen)?;

        Ok(HashStore {
            kv_core,
            writer: new_writer
        })
    }
}

impl KVStore for HashStore {
    fn name() -> &'static str where Self: Sized {
        "HashStore made in Kould"
    }

    // 通过文件夹路径开启一个HashKvStore
    fn open(path: impl Into<PathBuf>) -> Result<HashStore> where Self: Sized {
        HashStore::open_with_compaction_threshold(path, DEFAULT_COMPACTION_THRESHOLD)
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

        CommandPackage::write(&mut self.writer, &cmd)?;
        // 当模式匹配cmd为正确时
        if let Command::Set { key, .. } = cmd {
            // 封装为CommandPos
            let cmd_pos = CommandPos {gen: core.current_gen, pos: pos as usize, len: self.writer.pos - pos };
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
    fn get(&self, key: String) -> Result<Option<String>> {
        let core = &self.kv_core;
        // 若index中获取到了该数据命令
        if let Some(cmd_pos) = core.index.get(&key) {
            // 从读取器Map中通过该命令的序号获取对应的日志读取器
            let reader = core.readers.get(&cmd_pos.gen)
                .expect(format!("Can't find reader: {}", &cmd_pos.gen).as_str());
            let last_pos = cmd_pos.pos + cmd_pos.len as usize;
            // 获取这段内容
            let package = CommandPackage::form_pos(reader, cmd_pos.pos, last_pos)?;
            // 将命令进行转换
            if let Command::Set { value, .. } = package.cmd {
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
        let writer = &mut self.writer;
        // 若index中存在这个key
        if core.index.contains_key(&key) {
            // 对这个key做命令封装
            let cmd = Command::remove(key);
            CommandPackage::write(writer, &cmd)?;
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