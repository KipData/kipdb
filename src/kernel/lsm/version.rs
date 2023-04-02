use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::sync::Arc;
use bytes::Bytes;
use tokio::sync::mpsc::error::TrySendError;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use crate::kernel::{Result, sorted_gen_list};
use crate::kernel::io::{FileExtension, IoFactory, IoType};
use crate::kernel::lsm::SSTableMap;
use crate::kernel::lsm::block::BlockCache;
use crate::kernel::lsm::compactor::LEVEL_0;
use crate::kernel::lsm::log::LogLoader;
use crate::kernel::lsm::lsm_kv::Config;
use crate::kernel::lsm::ss_table::{Scope, SSTable};
use crate::kernel::utils::lru_cache::ShardingLruCache;
use crate::KernelError::SSTableLostError;

pub(crate) const DEFAULT_SS_TABLE_PATH: &str = "ss_table";

pub(crate) const DEFAULT_VERSION_PATH: &str = "version";

pub(crate) type LevelSlice = [Vec<i64>; 7];

pub(crate) type FileVec = (Vec<i64>, usize);

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum VersionEdit {
    DeleteFile(FileVec),
    // 确保新File的Gen都是比旧Version更大(新鲜)
    // Level 0则请忽略第二位的index参数，默认会放至最尾
    NewFile(FileVec, usize),
    // // Level and SSTable Gen List
    // CompactPoint(usize, Vec<i64>),
}

#[derive(Debug)]
enum CleanTag {
    Clean(u64),
    Add(u64, Vec<i64>)
}

/// SSTable的文件删除器
///
/// 整体的设计思路是由`Version::drop`进行删除驱动
/// 考虑过在Compactor中进行文件删除，但这样会需要进行额外的阈值判断以触发压缩(Compactor的阈值判断是通过传入的KV进行累计)
struct Cleaner {
    ss_table_map: Arc<RwLock<SSTableMap>>,
    sst_factory: Arc<IoFactory>,
    tag_rx: Receiver<CleanTag>,
    del_gens: Vec<(u64, Vec<i64>)>,

}

impl Cleaner {
    fn new(
        ss_table_map: &Arc<RwLock<SSTableMap>>,
        sst_factory: &Arc<IoFactory>,
        tag_rx: Receiver<CleanTag>
    ) -> Self {
        Self {
            ss_table_map: Arc::clone(ss_table_map),
            sst_factory: Arc::clone(sst_factory),
            tag_rx,
            del_gens: vec![],
        }
    }

    /// 监听tag_rev传递的信号
    ///
    /// 当tag_tx drop后自动关闭
    async fn listen(&mut self) {
        loop {
            match self.tag_rx.recv().await {
                Some(CleanTag::Clean(ver_num)) => self.clean(ver_num).await,
                Some(CleanTag::Add(ver_num,  vec_gen)) => {
                    self.del_gens
                        .push((ver_num, vec_gen));
                },
                // 关闭时对此次运行中的暂存Version全部进行删除
                None => {
                    for (_, vec_gen) in &self.del_gens {
                        for gen in vec_gen {
                            if let Err(err) = self.sst_factory.clean(*gen) {
                                error!("[Cleaner][drop][SSTables{}]: Remove Error!: {:?}", gen, err);
                            };
                        }
                    }
                    return
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
                if let Some((_, pre_vec_gen)) = self.del_gens.get_mut(index - 1) {
                    pre_vec_gen.append(&mut vec_gen);
                }
            }
        }
    }

    fn find_index_with_ver_num(del_gen: &[(u64, Vec<i64>)], ver_num: u64) -> Option<usize> {
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
    sst_factory: Arc<IoFactory>,
    /// TODO: 日志快照
    ver_log: LogLoader,
    /// 用于Drop时通知Cleaner drop
    _cleaner_tx: Sender<CleanTag>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct VersionMeta {
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
    meta_data: VersionMeta,
    /// 稀疏区间数据Block缓存
    pub(crate) block_cache: Arc<BlockCache>,
    /// 清除信号发送器
    /// Drop时通知Cleaner进行删除
    clean_sender: Sender<CleanTag>
}

impl VersionStatus {
    pub(crate) fn get_sst_factory_ref(&self) -> &IoFactory {
        &self.sst_factory
    }

    pub(crate) async fn load_with_path(
        config: Config,
        wal: &LogLoader,
    ) -> Result<Self> {
        let sst_path = config.path().join(DEFAULT_SS_TABLE_PATH);

        let block_cache = Arc::new(ShardingLruCache::new(
            config.block_cache_size,
            16,
            RandomState::default()
        )?);

        let mut ss_table_map = SSTableMap::new(config.clone())?;

        let sst_factory = Arc::new(
            IoFactory::new(
                sst_path.clone(),
                FileExtension::SSTable
            )?
        );
        // 持久化数据恢复
        // 倒叙遍历，从最新的数据开始恢复
        for gen in sorted_gen_list(&sst_path, FileExtension::SSTable)?
            .iter()
            .copied()
            .rev()
        {
            let reader = sst_factory.reader(gen, IoType::Direct)?;

            let ss_table = match SSTable::load_from_file(reader) {
                Ok(ss_table) => ss_table,
                Err(err) => {
                    warn!(
                        "[LSMStore][Load SSTable: {}][try to reload with wal][Error]: {:?}",
                        gen, err
                    );
                    SSTable::create_for_mem_table(
                        &config, gen, &sst_factory, wal.load(gen)?, LEVEL_0
                    )?
                }
            };
            Self::ss_table_insert(&mut ss_table_map, &sst_factory, ss_table, true)?
        }

        let ss_table_map = Arc::new(RwLock::new(ss_table_map));

        let (ver_log, vec_reload_edit) = LogLoader::reload(
            config.clone(),
            DEFAULT_VERSION_PATH,
            FileExtension::Manifest
        )?;


        let vec_log = vec_reload_edit
            .into_iter()
            .filter_map(|key_value| bincode::deserialize(&key_value.0).ok())
            .collect_vec();

        // TODO: 对channel进行配置
        let (tag_sender, tag_rev) = channel(20);
        let version = Arc::new(
            Version::load_from_log(
                vec_log,
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
            cleaner.listen().await;
        });

        Ok(Self {
            inner: RwLock::new(VersionInner { inner: version }),
            ss_table_map,
            sst_factory,
            ver_log,
            _cleaner_tx: tag_sender,
        })
    }

    fn ss_table_insert(
        ss_table_map: &mut SSTableMap,
        sst_factory: &Arc<IoFactory>,
        ss_table: SSTable,
        enable_cache: bool
    ) -> Result<()> {
        // 对Level 0的SSTable进行MMap映射
        if ss_table.get_level() == LEVEL_0 && enable_cache {
            let _ignore = ss_table_map.caching(ss_table.get_gen(), sst_factory)?;
        }
        // 初始化成功时直接传入SSTable的索引中
        let _ignore1 = ss_table_map.insert(ss_table);
        Ok(())
    }

    pub(crate) async fn current(&self) -> Arc<Version> {
        Arc::clone(
            &self.inner.read().await.inner
        )
    }

    pub(crate) async fn insert_vec_ss_table(&self, vec_ss_table: Vec<SSTable>, enable_cache: bool) -> Result<()> {
        let mut ss_table_map = self.ss_table_map.write().await;
        for ss_table in vec_ss_table {
            Self::ss_table_insert(
                &mut ss_table_map,
                &self.sst_factory,
                ss_table,
                enable_cache
            )?;
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

        let vec_data = vec_version_edit.iter()
            .filter_map(|edit| {
                bincode::serialize(&edit).ok()
                    .map(|key| (Bytes::from(key), None))
            })
            .collect_vec();

        new_version.apply(vec_version_edit, false).await?;

        version_display(&new_version, "log_and_apply");

        self.ver_log
            .log_batch(vec_data)?;
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
        self.get_len() == 0
    }

    pub(crate) fn get_size_of_disk(&self) -> u64 {
        self.meta_data.size_of_disk
    }

    /// 创建一个空的Version
    fn empty(
        ss_table_map: &Arc<RwLock<SSTableMap>>,
        block_cache: &Arc<BlockCache>,
        clean_sender: Sender<CleanTag>,
    ) -> Self {
        Self {
            version_num: 0,
            ss_tables_map: Arc::clone(ss_table_map),
            level_slice: Self::level_slice_new(),
            block_cache: Arc::clone(block_cache),
            meta_data: VersionMeta { size_of_disk: 0, len: 0 },
            clean_sender,
        }
    }

    #[allow(dead_code)]
    /// 通过现有数据创建Version
    async fn new(
        ss_table_map: &Arc<RwLock<SSTableMap>>,
        block_cache: &Arc<BlockCache>,
        clean_sender: Sender<CleanTag>,
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
            meta_data: VersionMeta { size_of_disk, len },
            block_cache: Arc::clone(block_cache),
            clean_sender,
        })
    }

    /// 通过一组VersionEdit载入Version
    async fn load_from_log(
        vec_log: Vec<VersionEdit>,
        ss_table_map: &Arc<RwLock<SSTableMap>>,
        block_cache: &Arc<BlockCache>,
        sender: Sender<CleanTag>
    ) -> Result<Self>{
        // 当无日志时,尝试通过现有ss_table_map进行Version恢复
        let version = if vec_log.is_empty() {
            Self::new(
                ss_table_map,
                block_cache,
                sender,
            ).await?
        } else {
            let mut version = Self::empty(
                ss_table_map,
                block_cache,
                sender,
            );

            version.apply(vec_log, true).await?;
            version
        };
        version_display(&version, "load_from_log");

        Ok(version)
    }

    /// Version对VersionEdit的应用处理
    ///
    /// Tips: 当此处像Cleaner发送Tag::Add时，此时的version中不需要的gens
    /// 因此每次删除都是删除此Version的前一位所需要删除的Version
    /// 也就是可能存在一次Version的冗余SSTable
    /// 可能是个确定，但是Minor Compactor比较起来更加频繁，也就是大多数情况不会冗余，因此我觉得影响较小
    /// 也可以算作是一种Major Compaction异常时的备份？
    #[allow(clippy::expect_used)]
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
        ).await.expect("send error!");

        Ok(())
    }

    async fn apply_add(meta_data: &mut VersionMeta, ss_table_map: &SSTableMap, vec_gen: &[i64]) -> Result<()>  {
        meta_data.statistical_process(
            ss_table_map,
            vec_gen,
            |meta_data, ss_table| {
                meta_data.size_of_disk += ss_table.get_size_of_disk();
                meta_data.len += ss_table.len();
            }
        ).await?;
        Ok(())
    }

    async fn apply_del_on_running(meta_data: &mut VersionMeta, ss_table_map: &SSTableMap, vec_gen: &[i64]) -> Result<()> {
        meta_data.statistical_process(
            ss_table_map,
            vec_gen,
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

    async fn rfind_ss_table<F>(
        level_slice: &LevelSlice,
        ss_table_map: &SSTableMap,
        level: usize,
        fn_rfind: F
    ) -> Option<SSTable>
        where F: Fn(&SSTable) -> bool
    {
        for gen in level_slice[level].iter() {
            if let Some(ss_table) = ss_table_map.get(gen) {
                if fn_rfind(&ss_table) {
                    return Some(ss_table);
                }
            }
        }

        None
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
            if let Some(ss_table) = ss_table_map.get(gen) {
                vec.push(ss_table);
            }
        }
        Some(vec)
    }

    #[allow(dead_code)]
    pub(crate) async fn get_ss_tables_for_level(&self, level: usize) -> Vec<SSTable> {
        let ss_table_map = self.ss_tables_map.read().await;

        self.level_slice[level].iter()
            .cloned()
            .filter_map(|gen| ss_table_map.get(&gen))
            .collect_vec()
    }

    /// 获取所有ss_table
    pub(crate) async fn get_all_ss_tables(&self) -> Vec<Vec<SSTable>> {
        let ss_table_map = self.ss_tables_map.read().await;
        let mut all_ss_tables = Vec::with_capacity(7);

        for level in 0..7 {
            all_ss_tables.push(
                self.level_slice[level].iter()
                    .cloned()
                    .filter_map(|gen| ss_table_map.get(&gen))
                    .collect_vec()
            )
        }

        all_ss_tables
    }

    /// 获取指定level中与scope冲突的
    pub(crate) async fn get_meet_scope_ss_tables(&self, level: usize, scope: &Scope) -> Vec<SSTable> {
        let mut vec = Vec::new();
        let ss_table_map = self.ss_tables_map.read().await;

        for gen in self.level_slice[level].iter()
        {
            if let Some(ss_table) = ss_table_map.get(gen) {
                if ss_table.get_scope().meet(scope) {
                    vec.push(ss_table);
                }
            }
        }
        vec
    }

    /// 使用Key从现有SSTables中获取对应的数据
    pub(crate) async fn find_data_for_ss_tables(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let ss_table_map = self.ss_tables_map.read().await;
        let block_cache = &self.block_cache;

        // Level 0的SSTable是无序且SSTable间的数据是可能重复的,因此需要遍历
        for gen in self.level_slice[LEVEL_0]
            .iter()
            .rev()
        {
            if let Some(ss_table) = ss_table_map.get(gen) {
                if ss_table.get_scope().meet_with_key(key) {
                    if let Some(value) =
                        Self::query_with_ss_table(key, block_cache, &ss_table)?
                    {
                        return Ok(Some(value))
                    }
                }
            }
        }
        // Level 1-7的数据排布有序且唯一，因此在每一个等级可以直接找到唯一一个Key可能在范围内的SSTable
        for level in 1..7 {
            if let Some(ss_table) = Self::rfind_ss_table(
                &self.level_slice,
                &ss_table_map,
                level,
                |ss_table| ss_table.get_scope().meet_with_key(key)
            ).await {
                if let Some(value) =
                    Self::query_with_ss_table(key, block_cache, &ss_table)?
                {
                    return Ok(Some(value))
                }
            }
        }

        Ok(None)
    }

    fn query_with_ss_table(
        key: &[u8],
        block_cache: &BlockCache,
        ss_table: &SSTable
    ) -> Result<Option<Bytes>> {
        ss_table.query_with_key(key, block_cache)
    }

    /// 判断是否溢出指定的SSTable数量
    pub(crate) fn is_threshold_exceeded_major(&self, config: &Config, level: usize) -> bool {
        self.level_slice[level].len() >=
            (config.major_threshold_with_sst_size * config.level_sst_magnification.pow(level as u32))
    }
}

impl VersionMeta {
    // MetaData对SSTable统计数据处理
    async fn statistical_process<F>(
        &mut self,
        ss_table_map: &SSTableMap,
        vec_gen: &[i64],
        fn_process: F
    ) -> Result<()>
        where F: Fn(&mut VersionMeta, &SSTable)
    {
        for gen in vec_gen.iter() {
            let ss_table = ss_table_map.get(gen)
                .ok_or_else(|| SSTableLostError)?;
            fn_process(self, &ss_table);
        }

        Ok(())
    }
}

impl Drop for Version {
    /// 将此Version可删除的版本号发送
    fn drop(&mut self) {
        while let Err(TrySendError::Full(_)) = self.clean_sender
            .try_send(CleanTag::Clean(self.version_num)) { continue }
    }
}

/// 使用特定格式进行display
fn version_display(new_version: &Version, method: &str) {
    info!(
            "[Version][{}]: version_num: {}, len: {}, size_of_disk: {}",
            method,
            new_version.version_num,
            new_version.get_len(),
            new_version.get_size_of_disk(),
        );
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;
    use bytes::Bytes;
    use tempfile::TempDir;
    use tokio::time;
    use crate::kernel::io::{FileExtension, IoFactory};
    use crate::kernel::lsm::log::LogLoader;
    use crate::kernel::lsm::lsm_kv::{Config, DEFAULT_WAL_PATH};
    use crate::kernel::lsm::ss_table::SSTable;
    use crate::kernel::lsm::version::{DEFAULT_SS_TABLE_PATH, Version, VersionEdit, VersionStatus};
    use crate::kernel::Result;

    #[test]
    fn test_version_clean() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        tokio_test::block_on(async move {

            let config = Config::new(temp_dir.into_path());

            let (wal, _) = LogLoader::reload(
                config.clone(),
                DEFAULT_WAL_PATH,
                FileExtension::Log
            )?;

            // 注意：将ss_table的创建防止VersionStatus的创建前
            // 因为VersionStatus检测无Log时会扫描当前文件夹下的SSTable进行重组以进行容灾
            let ver_status =
                VersionStatus::load_with_path(config.clone(), &wal).await?;


            let sst_factory = IoFactory::new(
                config.dir_path.join(DEFAULT_SS_TABLE_PATH),
                FileExtension::SSTable
            )?;

            let ss_table_1 = SSTable::create_for_mem_table(
                &config,
                1,
                &sst_factory,
                vec![(Bytes::from_static(b"test"), None)],
                0
            )?;

            let ss_table_2 = SSTable::create_for_mem_table(
                &config,
                2,
                &sst_factory,
                vec![(Bytes::from_static(b"test"), None)],
                0
            )?;

            ver_status.insert_vec_ss_table(vec![ss_table_1], false).await?;
            ver_status.insert_vec_ss_table(vec![ss_table_2], false).await?;

            let vec_edit_1 = vec![
                VersionEdit::NewFile((vec![1], 0),0),
            ];

            ver_status.log_and_apply(vec_edit_1).await?;

            let version_1 = Arc::clone(&ver_status.current().await);

            let vec_edit_2 = vec![
                VersionEdit::NewFile((vec![2], 0),0),
                VersionEdit::DeleteFile((vec![1], 0)),
            ];

            ver_status.log_and_apply(vec_edit_2).await?;

            let version_2 = Arc::clone(&ver_status.current().await);

            let vec_edit_3 = vec![
                VersionEdit::DeleteFile((vec![2], 0)),
            ];

            // 用于去除version2的引用计数
            ver_status.log_and_apply(vec_edit_3).await?;

            assert!(sst_factory.has_gen(1)?);
            assert!(sst_factory.has_gen(2)?);

            drop(version_2);

            assert!(sst_factory.has_gen(1)?);
            assert!(sst_factory.has_gen(2)?);

            drop(version_1);
            time::sleep(Duration::from_secs(1)).await;

            assert!(!sst_factory.has_gen(1)?);
            assert!(sst_factory.has_gen(2)?);

            drop(ver_status);
            time::sleep(Duration::from_secs(1)).await;

            assert!(!sst_factory.has_gen(1)?);
            assert!(!sst_factory.has_gen(2)?);

            Ok(())
        })
    }

    #[test]
    fn test_version_apply_and_log() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        tokio_test::block_on(async move {

            let config = Config::new(temp_dir.into_path());

            let (wal, _) = LogLoader::reload(
                config.clone(),
                DEFAULT_WAL_PATH,
                FileExtension::Log
            )?;

            // 注意：将ss_table的创建防止VersionStatus的创建前
            // 因为VersionStatus检测无Log时会扫描当前文件夹下的SSTable进行重组以进行容灾
            let ver_status_1 =
                VersionStatus::load_with_path(config.clone(), &wal).await?;


            let sst_factory = IoFactory::new(
                config.dir_path.join(DEFAULT_SS_TABLE_PATH),
                FileExtension::SSTable
            )?;

            let ss_table_1 = SSTable::create_for_mem_table(
                &config,
                1,
                &sst_factory,
                vec![(Bytes::from_static(b"test"), None)],
                0
            )?;

            let ss_table_2 = SSTable::create_for_mem_table(
                &config,
                2,
                &sst_factory,
                vec![(Bytes::from_static(b"test"), None)],
                0
            )?;

            let vec_edit = vec![
                VersionEdit::NewFile((vec![1], 0),0),
                VersionEdit::NewFile((vec![2], 0),0),
                VersionEdit::DeleteFile((vec![2], 0)),
            ];

            ver_status_1.insert_vec_ss_table(vec![ss_table_1], false).await?;
            ver_status_1.insert_vec_ss_table(vec![ss_table_2], false).await?;
            ver_status_1.log_and_apply(vec_edit).await?;

            let version_1 = Version::clone(ver_status_1.current().await.as_ref());

            drop(ver_status_1);

            let ver_status_2 =
                VersionStatus::load_with_path(config, &wal).await?;
            let version_2 = ver_status_2.current().await;

            assert_eq!(version_1.level_slice, version_2.level_slice);
            assert_eq!(version_1.meta_data, version_2.meta_data);

            Ok(())
        })
    }
}



