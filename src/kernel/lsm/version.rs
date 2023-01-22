use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::sync::Arc;
use itertools::Itertools;
use rmp_serde::{from_slice, to_vec};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use crate::{HashStore, KvsError};
use crate::kernel::{CommandData, CommandPackage, KVStore, Result, sorted_gen_list};
use crate::kernel::io::{FileExtension, IOHandler, IOHandlerFactory, IOType};
use crate::kernel::lsm::{MemMap, Position, SSTableMap};
use crate::kernel::lsm::compactor::LEVEL_0;
use crate::kernel::lsm::lsm_kv::{CommandCodec, Config};
use crate::kernel::lsm::ss_table::{Scope, SSTable};
use crate::kernel::utils::lru_cache::ShardingLruCache;
use crate::KvsError::SSTableLostError;

pub(crate) type LevelSlice = [Vec<i64>; 7];

pub(crate) type FileVec = (Vec<i64>, usize);

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum VersionEdit {
    DeleteFile(FileVec),
    // 确保新File的Gen都是比旧Version更大(新鲜)
    // Level 0则请忽略第二位的index参数，默认会放至最尾
    NewFile(FileVec, usize),
    // SSTable's SequenceId
    LastSequenceId(u64),
    // Wal Log Gen
    LogGen(i64),
    // // Level and SSTable Gen List
    // CompactPoint(usize, Vec<i64>),
}

#[derive(Debug)]
enum CleanTag {
    Drop,
    Clean(u64),
    Add(u64, Vec<i64>)
}

/// SSTable的文件删除器
///
/// 整体的设计思路是由`Version::drop`进行删除驱动
/// 考虑过在Compactor中进行文件删除，但这样会需要进行额外的阈值判断以触发压缩(Compactor的阈值判断是通过传入的KV进行累计)
struct Cleaner {
    ss_table_map: Arc<RwLock<SSTableMap>>,
    sst_factory: Arc<IOHandlerFactory>,
    tag_rev: Receiver<CleanTag>,
    del_gens: Vec<(u64, Vec<i64>)>,

}

impl Cleaner {
    fn new(
        ss_table_map: &Arc<RwLock<SSTableMap>>,
        sst_factory: &Arc<IOHandlerFactory>,
        tag_rev: Receiver<CleanTag>
    ) -> Self {
        Self {
            ss_table_map: Arc::clone(&ss_table_map),
            sst_factory: Arc::clone(&sst_factory),
            tag_rev,
            del_gens: vec![],
        }
    }

    async fn loop_and_clean(&mut self) {
        loop {
            if let Some(tag) = self.tag_rev.recv().await {
                match tag {
                    CleanTag::Drop => return,
                    CleanTag::Clean(ver_num) => {
                        self.clean(ver_num).await;
                    }
                    CleanTag::Add(ver_num,  vec_gen) => {
                        self.del_gens
                            .push((ver_num, vec_gen));
                    }
                }
            }
        }
    }

    /// 传入ver_num进行冗余SSTable的删除
    ///
    /// 整体删除逻辑: 当某个Version Drop时，以它的version_num作为基准，
    /// 检测该version_num在del_gens(应以version_num为顺序)的位置
    /// 当为第一位时说明无前置Version在使用，因此可以直接将此version_num的vec_gens全部删除
    /// 否则将对应位置的vec_gens添加至前一位的vec_gens中，使前一个Version开始clean时能将转移过来的vec_gens一起删除
    async fn clean(&mut self, ver_num: u64) {
        if let Some(index) = Self::find_index_with_ver_num(&self.del_gens, ver_num) {
            let (_, mut vec_gen) = self.del_gens.remove(index);
            if index == 0 {
                let mut ss_table_map = self.ss_table_map.write().await;
                // 当此Version处于第一位时，直接将其删除
                for gen in vec_gen {
                    let _ignore = ss_table_map.remove(&gen);
                    if let Err(err) = self.sst_factory.clean(gen) {
                        error!("[Cleaner][clean][SSTables{}]: Remove Error!: {:?}", gen, err);
                    };
                }
            } else {
                // 若非Version并非第一位，为了不影响前面Version对SSTable的读取处理，将待删除的SSTable的gen转移至前一位
                if let Some((_, pre_vec_gen)) = self.del_gens.get_mut(index) {
                    pre_vec_gen.append(&mut vec_gen);
                }
            }
        }
    }

    fn find_index_with_ver_num(del_gen: &Vec<(u64, Vec<i64>)>, ver_num: u64) -> Option<usize> {
        del_gen.iter()
            .enumerate()
            .find(|(_, (vn, _))| {
                vn == &ver_num
            })
            .map(|(index, _)| index)
    }
}

/// 用于切换Version的封装Inner
struct VersionInner {
    inner: Arc<Version>,
}

pub(crate) struct VersionStatus {
    inner: RwLock<VersionInner>,
    ss_table_map: Arc<RwLock<SSTableMap>>,
    sst_factory: Arc<IOHandlerFactory>,
    _ver_factory: Arc<IOHandlerFactory>,
    current_ver_handler: Box<dyn IOHandler>,
    /// 用于Drop时通知Cleaner drop
    tag_sender: Sender<CleanTag>,
}

#[derive(Clone)]
pub(crate) struct MetaData {
    /// SSTable集合占有磁盘大小
    size_of_disk: u64,
    /// SSTable集合中指令数量
    len: usize,
}

#[derive(Clone)]
pub(crate) struct Version {
    version_num: u64,
    /// SSTable存储Map
    /// 全局共享
    ss_tables_map: Arc<RwLock<SSTableMap>>,
    /// Level层级Vec
    /// 以索引0为level-0这样的递推，存储文件的gen值
    /// 每个Version各持有各自的Gen矩阵
    level_slice: LevelSlice,
    /// 统计数据
    meta_data: MetaData,
    /// 稀疏区间数据Block缓存
    block_cache: Arc<ShardingLruCache<(i64, Position), Vec<CommandData>>>,
    /// 用于MVCC的有序编号
    /// TODO: last_sequence_id功能支持
    last_sequence_id: u64,
    /// 该事务所对应的Wal Log Gen
    /// TODO: Wal MVCC支持
    wal_log_gen: i64,
    /// 清除信号发送器
    /// Drop时通知Cleaner进行删除
    clean_sender: Sender<CleanTag>
}

impl VersionStatus {
    pub(crate) fn get_sst_factory(&self) -> Arc<IOHandlerFactory> {
        Arc::clone(&self.sst_factory)
    }

    pub(crate) async fn load_with_path(
        config: &Config,
        mem_map: &mut MemMap,
        wal: &HashStore,
    ) -> Result<Self> {
        let path = config.dir_path.clone();
        let sst_path = path.join("ss_table");
        let ver_path = path.join("version");

        let block_cache = Arc::new(ShardingLruCache::new(
            config.block_cache_size,
            16,
            RandomState::default()
        )?);

        let mut ss_table_map = SSTableMap::new(&config)?;

        let sst_factory = Arc::new(
            IOHandlerFactory::new(
                sst_path.clone(),
                FileExtension::SSTable
            )?
        );
        let ver_factory = Arc::new(
            IOHandlerFactory::new(
                ver_path,
                FileExtension::Manifest
            )?
        );
        // 持久化数据恢复
        // 倒叙遍历，从最新的数据开始恢复
        for gen in sorted_gen_list(&sst_path, FileExtension::SSTable)?.iter().rev() {
            let io_handler = sst_factory.create(*gen, IOType::Buf)?;
            // 尝试初始化Table
            match SSTable::load_from_file(io_handler).await {
                Ok(ss_table) => {
                    // 对Level 0的SSTable进行MMap映射
                    if ss_table.get_level() == LEVEL_0 {
                        let _ignore = ss_table_map.caching(*gen, &sst_factory).await?;
                    }
                    // 初始化成功时直接传入SSTable的索引中
                    let _ignore1 = ss_table_map.insert(ss_table).await;
                }
                Err(err) => {
                    error!("[LSMStore][Load SSTable: {}][Error]: {:?}", gen, err);
                    // TODO: 是否删除可能还是得根据用户选择
                    // io_handler_factory.clean(*gen)?;
                    // 从wal将有问题的ss_table恢复到mem_table中
                    Self::reload_for_wal(mem_map, wal, *gen).await?;
                    // 删除有问题的ss_table
                    sst_factory.clean(*gen)?;
                }
            }
        }

        let ss_table_map = Arc::new(RwLock::new(ss_table_map));

        let last_gen = *sorted_gen_list(&path, FileExtension::Manifest)?
            .last()
            .unwrap_or(&0);

        // 获取最新的Manifest文件进行Version的状态恢复
        // TODO: 使用current指向最新manifest而不需要排序
        let current_ver_handler = ver_factory.create(last_gen, IOType::Buf)?;
        // TODO: 对channel进行配置
        let (tag_sender, tag_rev) = channel(20);
        let version = Arc::new(
            Version::load_from_file(
                &current_ver_handler,
                &ss_table_map,
                &block_cache,
                tag_sender.clone()
            ).await?
        );

        let mut cleaner = Cleaner::new(
            &ss_table_map,
            &sst_factory,
            tag_rev
        );

        let _ignore = tokio::spawn(async move {
            cleaner.loop_and_clean().await;
        });

        Ok(Self {
            inner: RwLock::new(VersionInner { inner: version }),
            ss_table_map,
            sst_factory,
            _ver_factory: ver_factory,
            current_ver_handler,
            tag_sender,
        })
    }

    /// 从Wal恢复SSTable数据
    /// 初始化失败时遍历wal的key并检测key是否为gen
    async fn reload_for_wal(mem_table: &mut MemMap, wal: &HashStore, gen: i64) -> Result<()>{
        // 将SSTable持久化失败前预存入的指令键集合从wal中获取
        // 随后将每一条指令键对应的指令恢复到mem_table中
        warn!("[SSTable: {}][reload_from_wal]", gen);
        let key_gen = CommandCodec::encode_gen(gen)?;
        if let Some(key_cmd_u8) = wal.get(&key_gen).await? {
            for key in CommandCodec::decode_keys(&key_cmd_u8)? {
                if let Some(cmd_data_u8) = wal.get(&key).await? {
                    let cmd_data = CommandPackage::decode(&cmd_data_u8)?;

                    let _ignore = mem_table.insert(cmd_data.get_key_clone(), cmd_data);
                } else {
                    return Err(KvsError::WalLoadError);
                }
            };
        } else {
            return Err(KvsError::WalLoadError);
        }
        Ok(())
    }

    pub(crate) async fn current(&self) -> Arc<Version> {
        Arc::clone(
            &self.inner
                .read().await
                .inner
        )
    }

    pub(crate) async fn insert_vec_ss_table(&self, vec_ss_table: Vec<SSTable>, enable_cache: bool) -> Result<()> {
        let mut ss_table_map = self.ss_table_map.write().await;
        for ss_table in vec_ss_table {
            // 创造缓存成本较大，因此只对Level 0的数据进行MMap映射
            if enable_cache && ss_table.get_level() == LEVEL_0 {
                let _ignore = ss_table_map.caching(ss_table.get_gen(), &self.sst_factory).await?;
            }
            let _ignore = ss_table_map.insert(ss_table).await;
        }

        Ok(())
    }

    /// 对一组VersionEdit持久化并应用
    pub(crate) async fn log_and_apply(
        &self,
        vec_version_edit: Vec<VersionEdit>,
    ) -> Result<()>{
        let mut new_version = Version::clone(
            self.current().await
                .as_ref()
        );

        let vec_cmd_data = vec_version_edit.iter()
            .filter_map(|edit| {
                to_vec(&edit).ok()
                    .map(CommandData::get)
            })
            .collect();

        new_version.apply(vec_version_edit, false).await?;

        version_display(&new_version, "log_and_apply");

        let _ignore = CommandPackage::write_batch(
            &self.current_ver_handler,
            &vec_cmd_data
        ).await?;
        self.inner
            .write().await
            .inner = Arc::new(new_version);

        Ok(())
    }
}

impl Version {
    pub(crate) fn get_len(&self) -> usize {
        self.meta_data.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.get_len() <= 0
    }

    pub(crate) fn get_size_of_disk(&self) -> u64 {
        self.meta_data.size_of_disk
    }

    /// 创建一个空的Version
    fn empty(
        ss_table_map: &Arc<RwLock<SSTableMap>>,
        block_cache: &Arc<ShardingLruCache<(i64, Position), Vec<CommandData>>>,
        clean_sender: Sender<CleanTag>
    ) -> Self {
        Self {
            version_num: 0,
            ss_tables_map: Arc::clone(ss_table_map),
            level_slice: Self::level_slice_new(),
            block_cache: Arc::clone(block_cache),
            last_sequence_id: 0,
            wal_log_gen: 0,
            meta_data: MetaData { size_of_disk: 0, len: 0 },
            clean_sender,
        }
    }

    #[allow(dead_code)]
    /// 通过现有数据创建Version
    async fn new(
        ss_table_map: &Arc<RwLock<SSTableMap>>,
        block_cache: &Arc<ShardingLruCache<(i64, Position), Vec<CommandData>>>,
        clean_sender: Sender<CleanTag>
    ) -> Result<Self> {
        let read_guard_map = ss_table_map.read().await;
        let level_slice = Self::level_layered(
            &read_guard_map
        );

        let mut size_of_disk = 0;

        let mut len = 0;

        for ss_table in read_guard_map.values() {
            size_of_disk += ss_table.get_size_of_disk();
            len += ss_table.len();
        }

        Ok(Self {
            version_num: 0,
            ss_tables_map: Arc::clone(ss_table_map),
            level_slice,
            meta_data: MetaData { size_of_disk, len },
            block_cache: Arc::clone(block_cache),
            last_sequence_id: 0,
            wal_log_gen: 0,
            clean_sender,
        })
    }

    /// 通过IOHandler载入Version
    async fn load_from_file(
        io_handler: &Box<dyn IOHandler>,
        ss_table_map: &Arc<RwLock<SSTableMap>>,
        block_cache: &Arc<ShardingLruCache<(i64, Position), Vec<CommandData>>>,
        sender: Sender<CleanTag>
    ) -> Result<Self>{
        let vec_log = CommandPackage::from_read_to_unpack_vec(io_handler).await?;
        // 当无日志时,尝试通过现有ss_table_map进行Version恢复
        let version = if vec_log.is_empty() {
            Self::new(ss_table_map, block_cache, sender).await?
        } else {
            let mut version = Self::empty(ss_table_map, block_cache, sender);

            let mut vec_edit = vec![];
            for cmd_data in vec_log.into_iter() {
                if let CommandData::Get { key } = cmd_data {
                   vec_edit.push(from_slice::<VersionEdit>(key.as_slice())?);
                }
            }

            version.apply(vec_edit, true).await?;
            version
        };
        version_display(&version, "load_from_file");

        Ok(version)
    }

    /// Version对VersionEdit的应用处理
    ///
    /// Tips: 当此处像Cleaner发送Tag::Add时，此时的version中不需要的gens
    /// 因此每次删除都是删除此Version的前一位所需要删除的Version
    /// 也就是可能存在一次Version的冗余SSTable
    /// 可能是个确定，但是Minor Compactor比较起来更加频繁，也就是大多数情况不会冗余，因此我觉得影响较小
    /// 也可以算作是一种Major Compaction异常时的备份？
    async fn apply(&mut self, vec_version_edit: Vec<VersionEdit>, is_init: bool) -> Result<()> {
        let mut del_gens = vec![];
        let ss_tables_map = self.ss_tables_map.read().await;
        // 初始化时使用gen_set确定最终SSTable的持有状态再进行数据统计处理
        // 避免日志重溯时对最终状态不存在的SSTable进行数据统计处理
        // 导致SSTableMap不存在此SSTable而抛出`KvsError::SSTableLostError`
        let mut gen_set = HashSet::new();

        for version_edit in vec_version_edit {
            match version_edit {
                VersionEdit::DeleteFile((mut vec_gen, level)) => {
                    if !is_init {
                        Self::apply_del_on_running(
                            &mut self.meta_data,
                            &ss_tables_map,
                            &vec_gen
                        ).await?;
                    }

                    for gen in vec_gen.iter() {
                        let _ignore = gen_set.remove(gen);
                    }
                    self.level_slice[level]
                        .retain(|gen| !vec_gen.contains(gen));
                    del_gens.append(&mut vec_gen);
                }
                VersionEdit::NewFile((vec_gen, level), index) => {
                    if !is_init {
                        Self::apply_add(
                            &mut self.meta_data,
                            &ss_tables_map,
                            &vec_gen
                        ).await?;
                    }

                    for gen in vec_gen.iter() {
                        let _ignore = gen_set.insert(*gen);
                    }
                    // Level 0中的SSTable绝对是以gen为优先级
                    // Level N中则不以gen为顺序，此处对gen排序是因为单次NewFile中的gen肯定是有序的
                    if level == LEVEL_0 {
                        for gen in vec_gen
                            .into_iter()
                            .sorted()
                        {
                            self.level_slice[level].push(gen);
                        }
                    } else {
                        for gen in vec_gen
                            .into_iter()
                            .sorted()
                            .rev()
                        {
                            self.level_slice[level].insert(index, gen);
                        }
                    }
                }
                VersionEdit::LastSequenceId(last_sequence_id) => {
                    self.last_sequence_id = last_sequence_id;
                }
                VersionEdit::LogGen(log_gen) => {
                    self.wal_log_gen = log_gen;
                }
            }
        }
        // 在初始化时进行统计数据累加
        // 注意与运行时统计数据处理互斥
        if is_init {
            Self::apply_add(
                &mut self.meta_data,
                &ss_tables_map,
                &Vec::from_iter(gen_set)
            ).await?;
        }

        self.version_num += 1;

        self.clean_sender.send(
            CleanTag::Add(self.version_num, del_gens)
        ).await.unwrap();

        Ok(())
    }

    async fn apply_add(meta_data: &mut MetaData, ss_tables_map: &SSTableMap, vec_gen: &Vec<i64>) -> Result<()>  {
        meta_data.statistical_process(
            &ss_tables_map,
            &vec_gen,
            |meta_data, ss_table| {
                meta_data.size_of_disk += ss_table.get_size_of_disk();
                meta_data.len += ss_table.len();
            }
        ).await?;
        Ok(())
    }

    async fn apply_del_on_running(meta_data: &mut MetaData, ss_tables_map: &SSTableMap, vec_gen: &Vec<i64>) -> Result<()> {
        meta_data.statistical_process(
            &ss_tables_map,
            &vec_gen,
            |meta_data, ss_table| {
                meta_data.size_of_disk -= ss_table.get_size_of_disk();
                meta_data.len -= ss_table.len();
            }
        ).await?;
        Ok(())
    }

    /// 使用ss_tables返回LevelVec
    /// 由于ss_tables是有序的，level_vec的内容应当是从L0->LN，旧->新
    fn level_layered(ss_tables: &SSTableMap) -> LevelSlice {
        let mut level_slice =
            Self::level_slice_new();

        ss_tables.iter()
            .sorted_by_key(|(gen, _)| *gen)
            .for_each(|(gen, ss_table)| {
                let level = ss_table.get_level();
                level_slice[level].push(*gen);
            });

        level_slice
    }

    fn level_slice_new() -> [Vec<i64>; 7] {
        [Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()]
    }

    pub(crate) fn get_index(&self, level: usize, source_gen: i64) -> Option<usize> {
        self.level_slice[level].iter()
            .enumerate()
            .find(|(_ , gen)| source_gen.eq(*gen))
            .map(|(index, _)| index)
    }

    /// TODO: 延迟加载
    async fn get_vec_ss_table_with_level(&self, level: usize) -> Vec<SSTable> {
        let mut vec = Vec::new();
        let ss_table_map = self.ss_tables_map.read().await;

        for gen in self.level_slice[level]
            .iter()
        {
            if let Some(ss_table) = ss_table_map.get(gen).await {
                vec.push(ss_table);
            }
        }
        vec
    }

    pub(crate) async fn get_first_vec_ss_table_with_size(&self, level: usize, size: usize) -> Option<Vec<SSTable>> {
        let mut vec = Vec::new();
        let ss_table_map = self.ss_tables_map.read().await;

        if self.level_slice[level].is_empty() {
            return None
        }

        for gen in self.level_slice[level]
            .iter()
            .take(size)
        {
            if let Some(ss_table) = ss_table_map.get(gen).await {
                vec.push(ss_table);
            }
        }
        Some(vec)
    }

    /// 获取指定level中与scope冲突的
    pub(crate) async fn get_meet_scope_ss_tables(&self, level: usize, scope: &Scope) -> Vec<SSTable> {
        self.get_vec_ss_table_with_level(level)
            .await
            .into_iter()
            .filter(|ss_table| ss_table.get_scope().meet(scope))
            .collect()
    }

    /// 使用Key从现有SSTables中获取对应的数据
    pub(crate) async fn find_data_for_ss_tables(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Level 0的SSTable是无序且SSTable间的数据是可能重复的
        for ss_table in self.get_vec_ss_table_with_level(0)
            .await
            .iter()
            .rev()
        {
            if let Some(cmd_data) = ss_table
                .query_with_key(key, &self.block_cache)
                .await?
            {
                return Ok(cmd_data.get_value_clone());
            }
        }
        // Level 1-7的数据排布有序且唯一，因此在每一个等级可以直接找到唯一一个Key可能在范围内的SSTable
        let key_scope = Scope::from_key(key);
        for level in 1..7 {
            if let Some(ss_table) = self.get_vec_ss_table_with_level(level)
                .await
                .iter()
                .rfind(|ss_table| ss_table.get_scope().meet(&key_scope))
            {
                if let Some(cmd_data) = ss_table
                    .query_with_key(key, &self.block_cache)
                    .await?
                {
                    return Ok(cmd_data.get_value_clone());
                }
            }
        }

        Ok(None)
    }

    /// 判断是否溢出指定的SSTable数量
    pub(crate) fn is_threshold_exceeded_major(&self, config: &Config, level: usize) -> bool {
        self.level_slice[level].len() >
            (config.major_threshold_with_sst_size.pow(level as u32) * config.level_sst_magnification)
    }
}

impl MetaData {
    // MetaData对SSTable统计数据处理
    async fn statistical_process<F>(
        &mut self,
        ss_table_map: &SSTableMap,
        vec_gen: &Vec<i64>,
        fn_process: F
    ) -> Result<()>
        where F: Fn(&mut MetaData, &SSTable)
    {
        for gen in vec_gen.iter() {
            let ss_table = ss_table_map.get(gen).await
                .ok_or_else(|| SSTableLostError)?;
            fn_process(self, &ss_table);
        }

        Ok(())
    }
}

impl Drop for VersionStatus {
    /// 将Cleaner关闭
    fn drop(&mut self) {
        // 自旋取进行发送，可能会导致线程一直占用
        // Rust尚不支持Async Drop
        loop {
            if let Ok(()) = self.tag_sender
                .try_send(CleanTag::Drop)
            {
                return;
            }
        }
    }
}

impl Drop for Version {
    /// 将此Version可删除的版本号发送
    fn drop(&mut self) {
        // 同上
        loop {
            if let Ok(()) = self.clean_sender
                .try_send(CleanTag::Clean(self.version_num))
            {
                return;
            }
        }
    }
}

/// 使用特定格式进行display
fn version_display(new_version: &Version, method: &str) {
    info!(
            "[Version][{}]: version_num: {}, len: {}, size_of_disk: {}, last_sequence_id: {}, wal_log_gen: {}",
            method,
            new_version.version_num,
            new_version.get_len(),
            new_version.get_size_of_disk(),
            new_version.last_sequence_id,
            new_version.wal_log_gen
        );
}



