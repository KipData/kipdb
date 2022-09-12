use std::{path::PathBuf, collections::HashMap, fs};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use itertools::Itertools;
use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::error;

use crate::{error::{KvsError}};
use crate::kernel::{CommandData, CommandPackage, CommandPos, KVStore, Result, sorted_gen_list};
use crate::kernel::io_handler::{IOHandler, IOHandlerFactory};

/// 默认压缩大小触发阈值
pub(crate) const DEFAULT_COMPACTION_THRESHOLD: u64 = 1024 * 1024 * 64;

/// The `HashKvStore` stores string key/value pairs.
pub struct HashStore {
    io_handler_factory: IOHandlerFactory,
    manifest: RwLock<Manifest>
}
/// 用于状态方面的管理
pub(crate) struct Manifest {
    index: HashMap<Vec<u8>, CommandPos>,
    current_gen: u64,
    un_compacted: u64,
    compaction_threshold: u64,
    io_handler_index: BTreeMap<u64, IOHandler>
}

impl HashStore {

    /// 获取索引中的所有keys
    pub async fn keys_from_index(&self) -> Result<Vec<Vec<u8>>> {
        let manifest = self.manifest.read().await;

        Ok(manifest.clone_index_keys())
    }

    /// 获取数据指令
    pub async fn get_cmd_data(&self, key: &Vec<u8>) -> Result<Option<CommandData>> {
        let manifest = self.manifest.read().await;

        // 若index中获取到了该数据命令
        if let Some(cmd_pos) = manifest.get_pos_with_key(key) {
            let io_handler = manifest.current_io_handler();
            Ok(CommandPackage::from_pos_unpack(io_handler, cmd_pos.pos, cmd_pos.len).await?)
        } else {
            Ok(None)
        }
    }

    /// 通过目录路径启动数据库
    pub async fn open_with_compaction_threshold(
        path: impl Into<PathBuf>,
        compaction_threshold: u64
    ) -> Result<Self> where Self: Sized {
        // 获取地址
        let path = path.into();
        // 创建文件夹（如果他们缺失）
        fs::create_dir_all(&path)?;
        let mut io_handler_index = BTreeMap::new();
        // 创建索引
        let mut index = HashMap::<Vec<u8>, CommandPos>::new();
        // 通过path获取有序的log序名Vec
        let gen_list = sorted_gen_list(&path).await?;
        // 创建IOHandlerFactory
        let io_handler_factory = IOHandlerFactory::new(path);
        // 初始化压缩阈值
        let mut un_compacted = 0;
        // 对读入其Map进行初始化并计算对应的压缩阈值
        for &gen in &gen_list {
            let handler = io_handler_factory.create(gen)?;
            un_compacted += load(&handler, &mut index).await? as u64;
            io_handler_index.insert(gen, handler);
        }
        let last_gen = gen_list.last().unwrap_or(&0).clone();
        // 获取当前最新的写入序名
        let current_gen = last_gen;
        // 以最新的写入序名创建新的日志文件
        io_handler_index.insert(last_gen, io_handler_factory.create(last_gen)?);

        let manifest = RwLock::new(Manifest {
            index,
            current_gen,
            un_compacted,
            compaction_threshold,
            io_handler_index
        });

        let store = HashStore {
            io_handler_factory,
            manifest
        };
        store.compact().await?;

        Ok(store)
    }

    /// 核心压缩方法
    /// 通过compaction_gen决定压缩位置
    async fn compact(&self) -> Result<()> {
        let mut manifest = self.manifest.write().await;

        let compaction_threshold = manifest.compaction_threshold;

        let (compact_gen,compact_handler) = manifest.compaction_increment(&self.io_handler_factory).await?;
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
                        Some(io_handler) => {
                            if let Some(cmd_data) =
                            CommandPackage::from_pos_unpack(&io_handler, cmd_pos.pos, cmd_pos.len).await? {
                                let (pos, len) = CommandPackage::write(&compact_handler, &cmd_data).await?;
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
            compact_handler.flush().await?;
            manifest.insert_io_handler(compact_handler);
            // 清除过期文件等信息
            manifest.retain(compact_gen, &self.io_handler_factory)?;
            manifest.un_compacted_add(write_len as u64);
        }

        Ok(())
    }

    /// 获取可承载范围内最新的数据的起始索引
    /// 要求vec_cmd_pos是有序的
    fn get_max_new_pos(vec_cmd_pos: &Vec<&mut CommandPos>, last_pos: u64, compaction_threshold: u64) -> usize {
        for (i, item) in vec_cmd_pos.iter().enumerate() {
            if last_pos - item.pos < compaction_threshold {
                return i;
            }
        }
        return 0;
    }
}

#[async_trait]
impl KVStore for HashStore {

    fn name() -> &'static str where Self: Sized {
        "HashStore made in Kould"
    }

    async fn open(path: impl Into<PathBuf> + Send) -> Result<Self> {
        HashStore::open_with_compaction_threshold(path, DEFAULT_COMPACTION_THRESHOLD).await
    }

    async fn flush(&self) -> Result<()> {
        let manifest = self.manifest.write().await;

        Ok(manifest.current_io_handler()
            .flush().await?)
    }

    async fn set(&self, key: &Vec<u8>, value: Vec<u8>) -> Result<()> {
        let mut manifest = self.manifest.write().await;

        //将数据包装为命令
        let gen = manifest.current_gen;
        let cmd = CommandData::Set { key: key.clone(), value };
        // 获取写入器当前地址
        let io_handler = manifest.current_io_handler();
        let (pos, len) = CommandPackage::write(io_handler, &cmd).await?;

        // 模式匹配获取key值
        if let CommandData::Set { key, .. } = cmd {
            // 封装为CommandPos
            let cmd_pos = CommandPos {gen, pos, len };

            // 将封装CommandPos存入索引Map中
            if let Some(old_cmd) = manifest.insert_command_pos(key, cmd_pos) {
                // 将阈值提升至该命令的大小
                manifest.un_compacted_add(old_cmd.len as u64);
            }
            // 阈值过高进行压缩
            if manifest.is_threshold_exceeded() {
                self.compact().await?
            }
        }

        Ok(())
    }

    async fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>> {
        let manifest = self.manifest.read().await;

        // 若index中获取到了该数据命令
        if let Some(cmd_pos) = manifest.get_pos_with_key(key) {
            // 获取这段内容
            let io_handler = manifest.get_io_handler(&cmd_pos.gen).unwrap();

            if let Some(cmd) = CommandPackage::from_pos_unpack(io_handler, cmd_pos.pos, cmd_pos.len).await? {
                // 将命令进行转换
                return if let CommandData::Set { value, .. } = cmd {
                    //返回匹配成功的数据
                    Ok(Some(value))
                } else {
                    //返回错误（错误的指令类型）
                    Err(KvsError::UnexpectedCommandType)
                }
            }
        }

        Ok(None)
    }

    async fn remove(&self, key: &Vec<u8>) -> Result<()> {
        let mut manifest = self.manifest.write().await;

        // 若index中存在这个key
        if manifest.contains_key_with_pos(key) {
            // 对这个key做命令封装
            let cmd = CommandData::Remove { key: key.to_vec() };
            CommandPackage::write(manifest.current_io_handler(), &cmd).await?;
            manifest.remove_key_with_pos(key);
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    async fn shut_down(&self) -> Result<()> {
        self.flush().await
    }
}

/// 通过目录地址加载数据并返回数据总大小
async fn load(io_handler: &IOHandler, index: &mut HashMap<Vec<u8>, CommandPos>) -> Result<usize> {
    let gen = io_handler.get_gen();

    // 流式读取将数据序列化为Command
    let vec_package = CommandPackage::from_read_to_vec(io_handler).await?;
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

impl Manifest {
    /// 通过Key获取对应的CommandPos
    fn get_pos_with_key(&self, key: &Vec<u8>) -> Option<&CommandPos> {
        self.index.get(key)
    }
    /// 获取当前最新的IOHandler
    fn current_io_handler(&self) -> &IOHandler {
        self.io_handler_index.get(&self.current_gen).unwrap()
    }
    /// 通过Gen获取指定的IOHandler
    fn get_io_handler(&self, gen: &u64) -> Option<&IOHandler> {
        self.io_handler_index.get(&gen)
    }
    /// 通过Gen获取指定的可变IOHandler
    fn get_mut_io_handler(&mut self, gen: &u64) -> Option<&mut IOHandler> {
        self.io_handler_index.get_mut(&gen)
    }
    /// 判断Index中是否存在对应的Key
    fn contains_key_with_pos(&self, key: &Vec<u8>) -> bool {
        self.index.contains_key(key)
    }
    /// 通过Key移除Index之中对应的CommandPos
    fn remove_key_with_pos(&mut self, key: &Vec<u8>) -> Option<CommandPos>{
        self.index.remove(key)
    }
    /// 克隆出当前的Index的Keys
    fn clone_index_keys(&self) -> Vec<Vec<u8>> {
        self.index.keys()
            .cloned()
            .collect_vec()
    }
    /// 提升最新Gen位置
    fn gen_add(&mut self, num: u64) {
        self.current_gen += num;
    }
    /// 插入新的CommandPos
    fn insert_command_pos(&mut self, key: Vec<u8>, cmd_pos: CommandPos) -> Option<CommandPos> {
        self.index.insert(key, cmd_pos)
    }
    /// 插入新的IOHandler
    fn insert_io_handler(&mut self, io_handler: IOHandler) {
        self.io_handler_index.insert(io_handler.get_gen(), io_handler);
    }
    /// 保留压缩Gen及以上的IOHandler与文件，其余清除
    fn retain(&mut self, expired_gen: u64, io_handler_factory: &IOHandlerFactory) -> Result<()> {
        // 遍历过滤出小于压缩文件序号的文件号名收集为过期Vec
        let stale_gens: HashSet<u64> = self.io_handler_index.keys()
            .filter(|&&stale_gen| stale_gen < expired_gen)
            .cloned()
            .collect();

        // 遍历过期Vec对数据进行旧文件删除
        for stale_gen in stale_gens.iter() {
            if let Some(io_handler) = self.get_mut_io_handler(stale_gen) {
                io_handler_factory.clean(io_handler.get_gen())?;
            }
        }
        // 清除索引中过期Key
        self.index.retain(|_, v| !stale_gens.contains(&v.gen));
        self.io_handler_index.retain(|k, _| !stale_gens.contains(k));

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
    fn sort_by_last_vec_mut(&mut self) -> (Vec<&mut CommandPos>, &BTreeMap<u64, IOHandler>) {
        let vec_values = self.index.values_mut()
            .sorted_unstable_by(|a, b| {
                match a.gen.cmp(&b.gen) {
                    Ordering::Less => Ordering::Less,
                    Ordering::Equal => a.pos.cmp(&b.pos),
                    Ordering::Greater => Ordering::Greater,
                }
            })
            .collect_vec();
        (vec_values, &self.io_handler_index)
    }
    /// 压缩前gen自增
    /// 用于数据压缩前将最新写入位置偏移至新位置
    pub(crate) async fn compaction_increment(&mut self, factory: &IOHandlerFactory) -> Result<(u64, IOHandler)> {
        // 将数据刷入硬盘防止丢失
        self.current_io_handler()
            .flush().await?;
        // 获取当前current
        let current = self.current_gen;
        // 插入新的写入IOHandler
        self.insert_io_handler(factory.create(current + 2)?);
        // 新的写入位置为原位置的向上两位
        self.gen_add(2);

        let compaction_gen = current + 1;
        Ok((compaction_gen, factory.create(compaction_gen)?))
    }
}