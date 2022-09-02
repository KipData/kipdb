use std::{io::Write, path::PathBuf, collections::HashMap, fs};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicU64;
use itertools::Itertools;
use async_trait::async_trait;

use crate::{error::{KvsError}};
use crate::kernel::{CommandData, CommandPackage, CommandPos, KVStore, Result, sorted_gen_list};
use crate::kernel::io_handler::{IOHandler, IOHandlerFactory};

pub(crate) const DEFAULT_COMPACTION_THRESHOLD: u64 = 1024 * 1024 * 6;

pub(crate) const DEFAULT_READER_SIZE: u64 = 254;

pub(crate) const DEFAULT_THREAD_SIZE: usize = 256;

/// The `HashKvStore` stores string key/value pairs.
pub struct HashStore {
    io_handler_factory: Arc<IOHandlerFactory>,
    manifest: Arc<RwLock<Manifest>>
}

pub(crate) struct Manifest {
    index: HashMap<Vec<u8>, CommandPos>,
    current_gen: Arc<AtomicU64>,
    un_compacted: u64,
    compaction_threshold: u64,
    io_handler_index: BTreeMap<u64, IOHandler>
}

impl Manifest {
    pub(crate) async fn current_io_handler(&self) -> &IOHandler {
        let current_gen = Arc::clone(&self.current_gen);

        self.io_handler_index.get(&current_gen.load(Ordering::SeqCst)).unwrap()
    }

    pub(crate) async fn compaction_increment(&self, factory: &IOHandlerFactory) -> Result<(u64, IOHandler)> {
        let current_gen = Arc::clone(&self.current_gen);
        let current = current_gen.load(Ordering::SeqCst);
        // 新的写入位置为原位置的向上两位
        current_gen.fetch_add(2, Ordering::Relaxed);

        self.current_io_handler().await
            .flush()?;

        Ok((current, factory.create(current + 1)?))
    }
}

impl HashStore {

    /// 获取索引中的所有keys
    pub async fn keys_from_index(&self) -> Result<Vec<&Vec<u8>>> {
        let manifest = self.manifest.read().unwrap();

        let keys = manifest.index.keys()
            .collect_vec();
        Ok(keys)
    }

    /// 获取数据指令
    pub async fn get_cmd_data(&self, key: &Vec<u8>) -> Result<Option<CommandData>> {
        let manifest = self.manifest.read().unwrap();

        // 若index中获取到了该数据命令
        if let Some(cmd_pos) = manifest.index.get(key) {
            let io_handler = manifest.current_io_handler().await;
            let package =
                CommandPackage::form_pos(io_handler, cmd_pos.pos, cmd_pos.len).await?;
            Ok(Some(package.cmd))
        } else {
            Ok(None)
        }
    }

    /// 通过目录路径启动数据库并指定压缩阈值
    pub async fn open_with_compaction_threshold(
        path: impl Into<PathBuf>,
        compaction_threshold: u64,
        reader_size: u64,
        thread_size: usize
    ) -> Result<Self> where Self: Sized {

        // 获取地址
        let path = path.into();
        // 创建文件夹（如果他们缺失）
        fs::create_dir_all(&path)?;
        // 创建IOHandlerFactory
        let io_handler_factory = Arc::new(IOHandlerFactory::new(path, reader_size, thread_size));
        let mut io_handler_index = BTreeMap::new();

        // 创建索引
        let mut index = HashMap::<Vec<u8>, CommandPos>::new();

        // 通过path获取有序的log序名Vec
        let gen_list = sorted_gen_list(&path).await?;
        // 初始化压缩阈值
        let mut un_compacted = 0;

        // 对读入其Map进行初始化并计算对应的压缩阈值
        for &gen in &gen_list {
            let handler = io_handler_factory.create(gen)?;
            un_compacted += load(&handler, &mut index)? as u64;
            io_handler_index.insert(gen, handler);
        }
        let last_gen = gen_list.last().unwrap_or(&0) + 1;
        // 获取当前最新的写入序名（之前的+1）
        let current_gen = Arc::new(AtomicU64::new(last_gen));

        // 以最新的写入序名创建新的日志文件
        io_handler_index.insert(last_gen, io_handler_factory.create(last_gen + 1)?);

        let manifest = Arc::new(RwLock::new(Manifest {
            index,
            current_gen,
            un_compacted,
            compaction_threshold,
            io_handler_index
        }));

        let store = HashStore {
            io_handler_factory,
            manifest
        };
        store.compact()?;

        Ok(store)
    }

    /// 核心压缩方法
    /// 通过compaction_gen决定压缩位置
    async fn compact(&self) -> Result<()> {
        let manifest = self.manifest.read().unwrap();

        let (gen ,compact_handler) = manifest.compaction_increment(&self.io_handler_factory).await?;

        // 压缩时对values进行顺序排序
        // 以gen,pos为最新数据的指标
        let mut vec_cmd_pos = manifest.index.values()
            .copied()
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
            let last_pos = last_cmd_pos.pos + last_cmd_pos.len;
            let skip_index = Self::get_max_new_pos(&vec_cmd_pos, last_pos as usize, manifest.compaction_threshold);

            let mut write_len = 0;
            // 对skip_index进行旧数据跳过处理
            // 抛弃超过文件大小且数据写入时间最久的数据
            for (i, cmd_pos) in vec_cmd_pos.iter_mut().enumerate() {
                if i >= skip_index {
                    let cmd_package = CommandPackage::form_pos(&compact_handler, cmd_pos.pos, cmd_pos.len).await?;
                    let (pos, len) = CommandPackage::write(&compact_handler, &cmd_package.cmd).await?;

                    write_len += len;
                    **cmd_pos = CommandPos { gen, pos, len };
                }
            }

            // 将所有写入刷入压缩文件中
            compact_handler.flush().await?;
            // 遍历过滤出小于压缩文件序号的文件号名收集为过期Vec
            let stale_gens: HashSet<u64> = manifest.io_handler_index.keys()
                .filter(|&&stale_gen| stale_gen < gen)
                .cloned()
                .collect();
            drop(manifest);

            let manifest = self.manifest.write().unwrap();

            // 遍历过期Vec对数据进行旧文件删除
            for stale_gen in stale_gens.iter() {
                if let Some(io_handler) = manifest.io_handler_index.get_mut(&stale_gen) {
                    self.io_handler_factory.clean(io_handler);
                }
            }
            // 清除索引中过期Key
            manifest.index.retain(|_, v| !stale_gens.contains(&v.gen));

            // 将压缩阈值调整为为压缩后大小
            manifest.un_compacted += write_len;
        }

        Ok(())
    }

    /// 获取可承载范围内最新的数据的起始索引
    /// 要求vec_cmd_pos是有序的
    fn get_max_new_pos(vec_cmd_pos: &Vec<CommandPos>, last_pos: usize, compaction_threshold: u64) -> usize {
        for (i, item) in vec_cmd_pos.iter().enumerate() {
            if last_pos - item.pos < compaction_threshold as usize {
                return i;
            }
        }
        return 0;
    }
}

impl Clone for HashStore {
    fn clone(&self) -> Self {
        HashStore {
            io_handler_factory: Arc::clone(&self.io_handler_factory),
            manifest: Arc::clone(&self.manifest)
        }
    }
}

#[async_trait]
impl KVStore for HashStore {

    fn name() -> &'static str where Self: Sized {
        "HashStore made in Kould"
    }

    // 通过文件夹路径开启一个HashKvStore
    async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        HashStore::open_with_compaction_threshold(path, DEFAULT_COMPACTION_THRESHOLD, DEFAULT_READER_SIZE, DEFAULT_THREAD_SIZE)
    }

    async fn flush(&self) -> Result<()> {
        // 刷入文件中
        Ok(self.manifest.write().unwrap()
            .current_io_handler().await
            .flush().await?)
    }

    /// 存入数据
    async fn set(&self, key: &Vec<u8>, value: Vec<u8>) -> Result<()> {
        let manifest = self.manifest.read().unwrap();

        //将数据包装为命令
        let gen = manifest.current_gen.load(Ordering::SeqCst);
        let cmd = CommandData::Set { key: key.clone(), value };
        // 获取写入器当前地址
        let (pos, len) = CommandPackage::write(manifest.current_io_handler(), &cmd).await?;

        // 模式匹配获取key值
        if let CommandData::Set { key, .. } = cmd {
            // 封装为CommandPos
            let cmd_pos = CommandPos {gen, pos, len };
            drop(manifest);

            let manifest = self.manifest.write().unwrap();
            // 将封装CommandPos存入索引Map中
            if let Some(old_cmd) = manifest.index.insert(key, cmd_pos) {
                // 将阈值提升至该命令的大小
                manifest.un_compacted += old_cmd.len;
            }
            // 阈值过高进行压缩
            if manifest.un_compacted > manifest.compaction_threshold {
                self.compact()?
            }
        }

        Ok(())
    }

    /// 获取数据
    async fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>> {
        let manifest = self.manifest.read().unwrap();

        // 若index中获取到了该数据命令
        if let Some(cmd_pos) = manifest.index.get(key) {
            // 获取这段内容
            let io_handler = manifest.io_handler_index.get(&cmd_pos.gen).unwrap();
            let package = CommandPackage::form_pos(io_handler, cmd_pos.pos, cmd_pos.len).await?;

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
    async fn remove(&mut self, key: &Vec<u8>) -> Result<()> {
        let manifest = self.manifest.write().unwrap();

        // 若index中存在这个key
        if manifest.index.contains_key(key) {
            // 对这个key做命令封装
            let cmd = CommandData::Remove { key: key.to_vec() };
            CommandPackage::write(manifest.current_io_handler(), &cmd).await?;
            manifest.index.remove(key);
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
}

// impl Drop for HashStore {
//     fn drop(&mut self) {
//         self.manifest.write().unwrap()
//             .current_io_handler().await
//             .flush().await
//             .unwrap();
//     }
// }

/// 通过目录地址加载数据并返回数据总大小
fn load(io_handler: &IOHandler, index: &mut HashMap<Vec<u8>, CommandPos>) -> Result<usize> {
    let gen = io_handler.get_gen();

    // 流式读取将数据序列化为Command
    let vec_package = CommandPackage::form_read_to_vec(io_handler)?;
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
            _ => {}
        }
    }
    Ok(un_compacted)
}