use std::{path::PathBuf, collections::HashMap};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use itertools::Itertools;
use async_trait::async_trait;
use bytes::Bytes;
use fslock::LockFile;
use parking_lot::RwLock;
use tracing::error;

use crate::kernel::{CommandData, CommandPackage, CommandPos, DEFAULT_LOCK_FILE, FileExtension, KVStore, lock_or_time_out, Result, sorted_gen_list};
use crate::kernel::io::{IoFactory, IoReader, IoType, IoWriter};
use crate::KernelError;

/// 默认压缩大小触发阈值
pub(crate) const DEFAULT_COMPACTION_THRESHOLD: u64 = 1024 * 1024 * 64;

type IoHandler = (Box<dyn IoWriter>, Box<dyn IoReader>);

/// The `HashKvStore` stores string key/value pairs.
pub struct HashStore {
    io_factory: IoFactory,
    manifest: RwLock<Manifest>,
    /// 多进程文件锁
    /// 避免多进程进行数据读写
    lock_file: LockFile,
}
/// 用于状态方面的管理
pub(crate) struct Manifest {
    index: HashMap<Vec<u8>, CommandPos>,
    current_gen: i64,
    un_compacted: u64,
    compaction_threshold: u64,
    io_index: BTreeMap<i64, IoHandler>
}

impl HashStore {

    /// 获取索引中的所有keys
    #[inline]
    pub async fn keys_from_index(&self) -> Vec<Vec<u8>> {
        let manifest = self.manifest.read();

        manifest.clone_index_keys()
    }

    /// 获取数据指令
    #[inline]
    pub async fn get_cmd_data(&self, key: &[u8]) -> Result<Option<CommandData>> {
        let manifest = self.manifest.read();

        // 若index中获取到了该数据命令
        if let Some(cmd_pos) = manifest.get_pos_with_key(key) {
            let reader = manifest.current_io_reader()?;
            Ok(CommandPackage::from_pos_unpack(reader, cmd_pos.pos, cmd_pos.len)?)
        } else {
            Ok(None)
        }
    }

    /// 通过目录路径启动数据库
    #[inline]
    pub async fn open_with_compaction_threshold(
        path: impl Into<PathBuf>,
        compaction_threshold: u64
    ) -> Result<Self> where Self: Sized {
        // 获取地址
        let path = path.into();
        // 创建IOHandlerFactory
        let io_factory =
            IoFactory::new(path.clone(), FileExtension::Log)?;
        let lock_file = lock_or_time_out(&path.join(DEFAULT_LOCK_FILE)).await?;

        let mut io_index = BTreeMap::new();
        // 创建索引
        let mut index = HashMap::<Vec<u8>, CommandPos>::new();
        // 通过path获取有序的log序名Vec
        let gen_list = sorted_gen_list(&path, FileExtension::Log)?;

        // 初始化压缩阈值
        let mut un_compacted = 0;
        // 对读入其Map进行初始化并计算对应的压缩阈值
        for &gen in &gen_list {
            let writer = io_factory.writer(gen, IoType::Buf)?;
            let reader = io_factory.reader(gen, IoType::Buf)?;
            un_compacted += load(reader.as_ref(), &mut index)? as u64;
            let _ignore1 = io_index.insert(gen, (writer, reader));
        }
        let last_gen = *gen_list.last().unwrap_or(&0);
        // 获取当前最新的写入序名
        let current_gen = last_gen;
        // 以最新的写入序名创建新的日志文件
        let _ignore2 = io_index.insert(
            last_gen,
            (io_factory.writer(last_gen, IoType::Buf)?,
                io_factory.reader(last_gen, IoType::Buf)?)
        );

        let manifest = RwLock::new(Manifest {
            index,
            current_gen,
            un_compacted,
            compaction_threshold,
            io_index
        });

        let store = HashStore {
            io_factory,
            manifest,
            lock_file,
        };
        store.compact()?;

        Ok(store)
    }

    /// 核心压缩方法
    /// 通过compaction_gen决定压缩位置
    fn compact(&self) -> Result<()> {
        let mut manifest = self.manifest.write();

        let compaction_threshold = manifest.compaction_threshold;

        let (compact_gen, mut compact_handler) = manifest.compaction_increment(&self.io_factory)?;
        // 压缩时对values进行顺序排序
        // 以gen,pos为最新数据的指标
        let (mut vec_cmd_pos, io_handler_index) = manifest.sort_by_last_vec_mut();

        // 获取最后一位数据进行可容载数据的范围
        if let Some(last_cmd_pos) = vec_cmd_pos.last() {
            let last_pos = last_cmd_pos.pos + last_cmd_pos.len as u64;
            let skip_index = Self::get_max_new_pos(&vec_cmd_pos, last_pos, compaction_threshold);

            let mut write_len = 0;
            // 对skip_index进行旧数据跳过处理
            // 抛弃超过文件大小且数据写入时间最久的数据
            for (i, cmd_pos) in vec_cmd_pos.iter_mut().enumerate() {
                if i >= skip_index {
                    match io_handler_index.get(&cmd_pos.gen) {
                        Some((_, reader)) => {
                            if let Some(cmd_data) =
                            CommandPackage::from_pos_unpack(reader.as_ref(), cmd_pos.pos, cmd_pos.len)? {
                                let (pos, len) = CommandPackage::write(compact_handler.0.as_mut(), &cmd_data)?;
                                write_len += len;
                                cmd_pos.change(compact_gen, pos, len);
                            }
                        }
                        None => {
                            error!("[HashStore][compact][Index data not found!!]")
                        }
                    }
                }
            }

            // 将所有写入刷入压缩文件中
            compact_handler.0.io_flush()?;
            manifest.insert_io_handler(compact_gen, compact_handler);
            // 清除过期文件等信息
            manifest.retain(compact_gen, &self.io_factory)?;
            manifest.un_compacted_add(write_len as u64);
        }

        Ok(())
    }

    /// 获取可承载范围内最新的数据的起始索引
    /// 要求vec_cmd_pos是有序的
    fn get_max_new_pos(vec_cmd_pos: &[&mut CommandPos], last_pos: u64, compaction_threshold: u64) -> usize {
        for (i, item) in vec_cmd_pos.iter().enumerate() {
            if last_pos - item.pos < compaction_threshold {
                return i;
            }
        }
        0
    }
}

impl Drop for HashStore {
    #[inline]
    #[allow(clippy::expect_used)]
    fn drop(&mut self) {
        self.lock_file.unlock()
            .expect("LockFile unlock failed!");
    }
}

#[async_trait]
impl KVStore for HashStore {

    #[inline]
    fn name() -> &'static str where Self: Sized {
        "HashStore made in Kould"
    }

    #[inline]
    async fn open(path: impl Into<PathBuf> + Send) -> Result<Self> {
        HashStore::open_with_compaction_threshold(path, DEFAULT_COMPACTION_THRESHOLD).await
    }

    #[inline]
    async fn flush(&self) -> Result<()> {
        let mut manifest = self.manifest.write();

        Ok(manifest.current_io_writer()?
            .io_flush()?)
    }

    #[inline]
    async fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        let mut manifest = self.manifest.write();

        //将数据包装为命令
        let gen = manifest.current_gen;
        let cmd = CommandData::Set { key: key.to_vec(), value };
        // 获取写入器当前地址
        let io_handler = manifest.current_io_writer()?;
        let (pos, cmd_len) = CommandPackage::write(io_handler, &cmd)?;

        // 模式匹配获取key值
        if let CommandData::Set { key: cmd_key, .. } = cmd {
            // 封装为CommandPos
            let cmd_pos = CommandPos {gen, pos, len: cmd_len };

            // 将封装CommandPos存入索引Map中
            if let Some(old_cmd) = manifest.insert_command_pos(cmd_key, cmd_pos) {
                // 将阈值提升至该命令的大小
                manifest.un_compacted_add(old_cmd.len as u64);
            }
            // 阈值过高进行压缩
            if manifest.is_threshold_exceeded() {
                self.compact()?
            }
        }

        Ok(())
    }

    #[inline]
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let manifest = self.manifest.read();

        // 若index中获取到了该数据命令
        if let Some(cmd_pos) = manifest.get_pos_with_key(key) {
            if let Some(reader) = manifest.get_reader(&cmd_pos.gen) {
                if let Some(cmd) = CommandPackage::from_pos_unpack(reader, cmd_pos.pos, cmd_pos.len)? {
                    // 将命令进行转换
                    return if let CommandData::Set { value, .. } = cmd {
                        //返回匹配成功的数据
                        Ok(Some(Bytes::from(value)))
                    } else {
                        //返回错误（错误的指令类型）
                        Err(KernelError::UnexpectedCommandType)
                    }
                }
            }
        }

        Ok(None)
    }

    #[inline]
    async fn remove(&self, key: &[u8]) -> Result<()> {
        let mut manifest = self.manifest.write();

        // 若index中存在这个key
        if manifest.contains_key_with_pos(key) {
            // 对这个key做命令封装
            let cmd = CommandData::Remove { key: key.to_vec() };
            let _ignore = CommandPackage::write(manifest.current_io_writer()?, &cmd)?;
            let _ignore1 = manifest.remove_key_with_pos(key);
            Ok(())
        } else {
            Err(KernelError::KeyNotFound)
        }
    }

    #[inline]
    async fn size_of_disk(&self) -> Result<u64> {
        let manifest = self.manifest.read();

        Ok(manifest.io_index
            .values()
            .filter_map(|(_, reader)| reader.file_size().ok())
            .sum::<u64>())
    }

    #[inline]
    async fn len(&self) -> Result<usize> {
        Ok(self.manifest.read()
            .index.len())
    }

    #[inline]
    async fn is_empty(&self) -> bool {
        self.manifest.read()
            .index.is_empty()
    }
}

/// 通过目录地址加载数据并返回数据总大小
fn load(reader: &dyn IoReader, index: &mut HashMap<Vec<u8>, CommandPos>) -> Result<usize> {
    let gen = reader.get_gen();

    // 流式读取将数据序列化为Command
    let vec_package = CommandPackage::from_read_to_vec(reader)?;
    // 初始化空间占用为0
    let mut un_compacted = 0;
    // 迭代数据
    for package in vec_package {
        match package.cmd {
            CommandData::Set { key, .. } => {
                //数据插入索引之中，成功则对空间占用值进行累加
                if let Some(old_cmd) = index.insert(key, CommandPos {gen, pos: package.pos, len: package.len }) {
                    un_compacted += old_cmd.len + 1;
                }
            }
            CommandData::Remove { key } => {
                //索引删除该数据之中，成功则对空间占用值进行累加
                if let Some(old_cmd) = index.remove(&key) {
                    un_compacted += old_cmd.len + 1;
                };
            }
            CommandData::Get{ .. }  => {}
        }
    }
    Ok(un_compacted)
}

impl Manifest {
    /// 通过Key获取对应的CommandPos
    fn get_pos_with_key(&self, key: &[u8]) -> Option<&CommandPos> {
        self.index.get(key)
    }
    /// 获取当前最新的IOHandler
    fn current_io_writer(&mut self) -> Result<&mut dyn IoWriter> {
        self.io_index.get_mut(&self.current_gen)
            .map(|(writer, _ )| writer.as_mut())
            .ok_or(KernelError::FileNotFound)
    }
    fn current_io_reader(&self) -> Result<&dyn IoReader> {
        self.io_index.get(&self.current_gen)
            .map(|(_, reader)| reader.as_ref())
            .ok_or(KernelError::FileNotFound)
    }
    /// 通过Gen获取指定的IoReader
    fn get_reader(&self, gen: &i64) -> Option<&dyn IoReader> {
        self.io_index.get(gen)
            .map(|(_, reader)| reader.as_ref())
    }
    /// 判断Index中是否存在对应的Key
    fn contains_key_with_pos(&self, key: &[u8]) -> bool {
        self.index.contains_key(key)
    }
    /// 通过Key移除Index之中对应的CommandPos
    fn remove_key_with_pos(&mut self, key: &[u8]) -> Option<CommandPos>{
        self.index.remove(key)
    }
    /// 克隆出当前的Index的Keys
    fn clone_index_keys(&self) -> Vec<Vec<u8>> {
        self.index.keys()
            .cloned()
            .collect_vec()
    }
    /// 提升最新Gen位置
    fn gen_add(&mut self, num: i64) {
        self.current_gen += num;
    }
    /// 插入新的CommandPos
    fn insert_command_pos(&mut self, key: Vec<u8>, cmd_pos: CommandPos) -> Option<CommandPos> {
        self.index.insert(key, cmd_pos)
    }
    /// 插入新的IOHandler
    fn insert_io_handler(&mut self, gen: i64, io_handler: IoHandler) {
        let _ignore = self.io_index.insert(gen, io_handler);
    }
    /// 保留压缩Gen及以上的IOHandler与文件，其余清除
    fn retain(&mut self, expired_gen: i64, io_handler_factory: &IoFactory) -> Result<()> {
        // 遍历过滤出小于压缩文件序号的文件号名收集为过期Vec
        let stale_gens: HashSet<i64> = self.io_index.keys()
            .filter(|&&stale_gen| stale_gen < expired_gen)
            .cloned()
            .collect();

        // 遍历过期Vec对数据进行旧文件删除
        for stale_gen in stale_gens.iter() {
            if let Some(io_handler) = self.get_reader(stale_gen) {
                io_handler_factory.clean(io_handler.get_gen())?;
            }
        }
        // 清除索引中过期Key
        self.index.retain(|_, v| !stale_gens.contains(&v.gen));
        self.io_index.retain(|k, _| !stale_gens.contains(k));

        Ok(())
    }
    /// 增加压缩阈值
    fn un_compacted_add(&mut self, new_len: u64) {
        // 将压缩阈值调整为为压缩后大小
        self.un_compacted += new_len;
    }
    /// 判断目前是否超出压缩阈值
    fn is_threshold_exceeded(&self) -> bool {
        self.un_compacted > self.compaction_threshold
    }
    /// 将Index中的CommandPos以最新为基准进行排序，由旧往新
    fn sort_by_last_vec_mut(&mut self) -> (Vec<&mut CommandPos>, &BTreeMap<i64, IoHandler>) {
        let vec_values = self.index.values_mut()
            .sorted_unstable_by(|a, b| {
                match a.gen.cmp(&b.gen) {
                    Ordering::Less => Ordering::Less,
                    Ordering::Equal => a.pos.cmp(&b.pos),
                    Ordering::Greater => Ordering::Greater,
                }
            })
            .collect_vec();
        (vec_values, &self.io_index)
    }
    /// 压缩前gen自增
    /// 用于数据压缩前将最新写入位置偏移至新位置
    pub(crate) fn compaction_increment(&mut self, factory: &IoFactory) -> Result<(i64, IoHandler)> {
        // 将数据刷入硬盘防止丢失
        self.current_io_writer()?
            .io_flush()?;
        // 获取当前current
        let current = self.current_gen;
        let next_gen = current + 2;
        // 插入新的写入IOHandler
        self.insert_io_handler(next_gen, (
            factory.writer(next_gen, IoType::Buf)?,
            factory.reader(next_gen, IoType::Buf)?
        ));
        // 新的写入位置为原位置的向上两位
        self.gen_add(2);

        let compaction_gen = current + 1;
        Ok((compaction_gen,
            (factory.writer(compaction_gen, IoType::Buf)?,
             factory.reader(compaction_gen, IoType::Buf)?)
        ))
    }
}