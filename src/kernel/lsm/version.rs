use std::collections::hash_map::RandomState;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use bytes::Bytes;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::RwLock;
use tracing::{error, info};
use crate::kernel::{Result, sorted_gen_list};
use crate::kernel::io::{FileExtension, IoFactory, IoType, IoWriter};
use crate::kernel::lsm::SSTableLoader;
use crate::kernel::lsm::block::BlockCache;
use crate::kernel::lsm::compactor::LEVEL_0;
use crate::kernel::lsm::log::{LogLoader, LogWriter};
use crate::kernel::lsm::lsm_kv::{Config, Gen};
use crate::kernel::lsm::ss_table::{Scope, SSTable};
use crate::kernel::utils::lru_cache::ShardingLruCache;

pub(crate) const DEFAULT_SS_TABLE_PATH: &str = "ss_table";
pub(crate) const DEFAULT_VERSION_PATH: &str = "version";
pub(crate) const DEFAULT_VERSION_LOG_THRESHOLD: usize = 1000;


pub(crate) type LevelSlice = [Vec<Scope>; 7];

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub(crate) enum VersionEdit {
    DeleteFile((Vec<i64>, usize)),
    // 确保新File的Gen都是比旧Version更大(新鲜)
    // Level 0则请忽略第二位的index参数，默认会放至最尾
    NewFile((Vec<Scope>, usize), usize),
    // // Level and SSTable Gen List
    // CompactPoint(usize, Vec<i64>),
}

#[derive(Debug)]
enum CleanTag {
    Clean(u64),
    Add(u64, Vec<i64>),
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
enum EditType {
    Add = 0,
    Del = 1,
}

/// SSTable的文件删除器
///
/// 整体的设计思路是由`Version::drop`进行删除驱动
/// 考虑过在Compactor中进行文件删除，但这样会需要进行额外的阈值判断以触发压缩(Compactor的阈值判断是通过传入的KV进行累计)
struct Cleaner {
    ss_table_loader: Arc<SSTableLoader>,
    tag_rx: UnboundedReceiver<CleanTag>,
    del_gens: Vec<(u64, Vec<i64>)>,
}

impl Cleaner {
    fn new(
        ss_table_loader: &Arc<SSTableLoader>,
        tag_rx: UnboundedReceiver<CleanTag>,
    ) -> Self {
        Self {
            ss_table_loader: Arc::clone(ss_table_loader),
            tag_rx,
            del_gens: Vec::new(),
        }
    }

    /// 监听tag_rev传递的信号
    ///
    /// 当tag_tx drop后自动关闭
    async fn listen(&mut self) {
        loop {
            match self.tag_rx.recv().await {
                Some(CleanTag::Clean(ver_num)) => self.clean(ver_num),
                Some(CleanTag::Add(ver_num, vec_gen)) => {
                    self.del_gens.push((ver_num, vec_gen));
                }
                // 关闭时对此次运行中的暂存Version全部进行删除
                None => {
                    let all_ver_num = self.del_gens.iter()
                        .map(|(ver_num, _)| ver_num)
                        .cloned()
                        .collect_vec();
                    for ver_num in all_ver_num {
                        self.clean(ver_num)
                    }
                    return;
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
    fn clean(&mut self, ver_num: u64) {
        if let Some(index) = Self::find_index_with_ver_num(&self.del_gens, ver_num) {
            let (_, mut vec_gen) = self.del_gens.remove(index);
            if index == 0 {
                let ss_table_loader = &self.ss_table_loader;
                // 当此Version处于第一位时，直接将其删除
                for gen in vec_gen {
                    if let Err(err) = ss_table_loader.clean(gen) {
                        error!("[Cleaner][clean][SSTable: {}]: Remove Error!: {:?}", gen, err);
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
    version: Arc<Version>,
    /// TODO: 日志快照
    ver_log_writer: (LogWriter<Box<dyn IoWriter>>, i64),
}

pub(crate) struct VersionStatus {
    inner: RwLock<VersionInner>,
    ss_table_loader: Arc<SSTableLoader>,
    pub(crate) sst_factory: Arc<IoFactory>,
    pub(crate) log_factory: Arc<IoFactory>,
    edit_approximate_count: AtomicUsize,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub(crate) struct VersionMeta {
    /// SSTable集合占有磁盘大小
    size_of_disk: u64,
    /// SSTable集合中指令数量
    len: usize,
}

#[derive(Clone)]
pub(crate) struct Version {
    version_num: u64,
    /// 稀疏区间数据Block缓存
    pub(crate) block_cache: Arc<BlockCache>,
    /// SSTable存储Map
    /// 全局共享
    ss_tables_loader: Arc<SSTableLoader>,
    /// Level层级Vec
    /// 以索引0为level-0这样的递推，存储文件的gen值
    /// 每个Version各持有各自的Gen矩阵
    level_slice: LevelSlice,
    /// 统计数据
    meta_data: VersionMeta,
    /// 清除信号发送器
    /// Drop时通知Cleaner进行删除
    clean_tx: UnboundedSender<CleanTag>,
}

impl VersionStatus {
    pub(crate) fn load_with_path(
        config: Config,
        wal: LogLoader,
    ) -> Result<Self> {
        let sst_path = config.path().join(DEFAULT_SS_TABLE_PATH);

        let block_cache = Arc::new(ShardingLruCache::new(
            config.block_cache_size,
            16,
            RandomState::default(),
        )?);
        let sst_factory = Arc::new(
            IoFactory::new(
                sst_path,
                FileExtension::SSTable,
            )?
        );

        let ss_table_loader = Arc::new(
            SSTableLoader::new(
                config.clone(),
                Arc::clone(&sst_factory),
                wal,
            )?
        );

        let log_factory = Arc::new(
            IoFactory::new(
                config.path().join(DEFAULT_VERSION_PATH),
                FileExtension::Log,
            )?
        );

        let (ver_log_loader, vec_batch_log, log_gen) = LogLoader::reload(
            config.path(),
            (DEFAULT_VERSION_PATH, Some(snapshot_gen(&log_factory)?)),
            IoType::Direct,
            |bytes| Ok(bincode::deserialize::<Vec<VersionEdit>>(bytes)?),
        )?;

        let vec_log = vec_batch_log.into_iter()
            .flatten()
            .collect_vec();

        let edit_approximate_count = AtomicUsize::new(vec_log.len());

        let (tag_tx, tag_rx) = unbounded_channel();
        let version = Arc::new(
            Version::load_from_log(
                vec_log,
                &ss_table_loader,
                &block_cache,
                tag_tx,
            )?
        );

        let mut cleaner = Cleaner::new(
            &ss_table_loader,
            tag_rx,
        );

        let _ignore = tokio::spawn(async move {
            cleaner.listen().await;
        });

        let ver_log_writer = ver_log_loader.writer(log_gen)?;

        Ok(Self {
            inner: RwLock::new(VersionInner { version, ver_log_writer: ((ver_log_writer), log_gen) }),
            ss_table_loader,
            sst_factory,
            log_factory,
            edit_approximate_count,
        })
    }

    pub(crate) async fn current(&self) -> Arc<Version> {
        Arc::clone(
            &self.inner.read().await.version
        )
    }

    pub(crate) fn insert_vec_ss_table(&self, vec_ss_table: Vec<SSTable>) -> Result<()> {
        for ss_table in vec_ss_table {
            let _ = self.ss_table_loader.insert(ss_table);
        }

        Ok(())
    }

    /// 对一组VersionEdit持久化并应用
    pub(crate) async fn log_and_apply(&self, vec_version_edit: Vec<VersionEdit>, snapshot_threshold: usize) -> Result<()> {
        let mut new_version = Version::clone(
            self.current().await.as_ref()
        );
        let mut inner = self.inner.write().await;
        version_display(&new_version, "log_and_apply");

        if self.edit_approximate_count.load(Ordering::Relaxed) >= snapshot_threshold {
            Self::write_snap_shot(&mut inner, &self.log_factory, &vec_version_edit).await?;
        } else {
            let _ = inner.ver_log_writer.0.add_record(&bincode::serialize(&vec_version_edit)?)?;
            let _ = self.edit_approximate_count.fetch_add(1, Ordering::Relaxed);
        }

        new_version.apply(vec_version_edit)?;
        inner.version = Arc::new(new_version);

        Ok(())
    }

    async fn write_snap_shot(inner: &mut VersionInner, log_factory: &IoFactory, vec_version_edit: &Vec<VersionEdit>) -> Result<()> {
        let version = &inner.version;
        info!("[Version: {}][write_snap_shot]: Start Snapshot!", version.version_num);
        let new_gen = Gen::create();
        let new_writer = log_factory.writer(new_gen, IoType::Direct)?;
        let (mut old_writer, old_gen) = mem::replace(
            &mut inner.ver_log_writer,
            (LogWriter::new(new_writer), new_gen)
        );

        old_writer.flush()?;

        let snap_shot_version_edits = version.to_vec_edit();
        let _ = inner.ver_log_writer.0.add_record(&bincode::serialize(&snap_shot_version_edits)?)?;
        let _ = inner.ver_log_writer.0.add_record(&bincode::serialize(vec_version_edit)?)?;

        // 删除旧的 version log
        log_factory.clean(old_gen)?;

        Ok(())
    }
}

fn snapshot_gen(factory: &IoFactory) -> Result<i64>{
    if let Ok(gen_list) = sorted_gen_list(factory.get_path(), FileExtension::Log) {
        return Ok(match *gen_list.as_slice() {
            [.., old_snapshot, new_snapshot] => {
                factory.clean(new_snapshot)?;
                old_snapshot
            },
            [snapshot] => snapshot,
            _ => Gen::create(),
        });
    }

    Ok(Gen::create())
}


impl Version {
    pub(crate) fn get_len(&self) -> usize {
        self.meta_data.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.get_len() == 0
    }

    pub(crate) fn level_len(&self, level: usize) -> usize {
        self.level_slice[level].len()
    }

    pub(crate) fn get_size_of_disk(&self) -> u64 {
        self.meta_data.size_of_disk
    }

    /// 通过一组VersionEdit载入Version
    fn load_from_log(
        vec_log: Vec<VersionEdit>,
        ss_table_loader: &Arc<SSTableLoader>,
        block_cache: &Arc<BlockCache>,
        clean_tx: UnboundedSender<CleanTag>
    ) -> Result<Self>{
        let mut version = Self {
            version_num: 0,
            ss_tables_loader: Arc::clone(ss_table_loader),
            level_slice: Self::level_slice_new(),
            block_cache: Arc::clone(block_cache),
            meta_data: VersionMeta { size_of_disk: 0, len: 0 },
            clean_tx,
        };

        version.apply(vec_log)?;
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
    fn apply(&mut self, vec_version_edit: Vec<VersionEdit>) -> Result<()> {
        let mut del_gens = Vec::new();
        // 避免日志重溯时对最终状态不存在的SSTable进行数据统计处理
        // 导致SSTableMap不存在此SSTable而抛出`KvsError::SSTableLostError`
        let mut vec_statistics_gen = Vec::new();

        for version_edit in vec_version_edit {
            match version_edit {
                VersionEdit::DeleteFile((mut vec_gen, level)) => {
                    for gen in vec_gen.clone() {
                        vec_statistics_gen.push((EditType::Del, gen));
                    }
                    self.level_slice[level]
                        .retain(|scope| !vec_gen.contains(&scope.get_gen()));
                    del_gens.append(&mut vec_gen);
                }
                VersionEdit::NewFile((vec_scope, level), index) => {
                    for gen in vec_scope.iter().map(Scope::get_gen) {
                        vec_statistics_gen.push((EditType::Add, gen));
                    }
                    // Level 0中的SSTable绝对是以gen为优先级
                    // Level N中则不以gen为顺序，此处对gen排序是因为单次NewFile中的gen肯定是有序的
                    let scope_iter = vec_scope
                        .into_iter()
                        .sorted_by_key(Scope::get_gen);
                    if level == LEVEL_0 {
                        for scope in scope_iter {
                            self.level_slice[level].push(scope);
                        }
                    } else {
                        for scope in scope_iter.rev() {
                            self.level_slice[level].insert(index, scope);
                        }
                    }
                }
            }
        }

        self.meta_data.statistical_process(&self.ss_tables_loader, vec_statistics_gen)?;
        self.version_num += 1;
        self.clean_tx.send(CleanTag::Add(self.version_num, del_gens))?;

        Ok(())
    }

    fn level_slice_new() -> [Vec<Scope>; 7] {
        [Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()]
    }

    /// 把当前version的leveSlice中的数据转化为一组versionEdit 作为新version_log的base
    pub(crate) fn to_vec_edit(&self) -> Vec<VersionEdit> {
        // 在快照中 append edit, 防止快照中宕机发生在删除旧 log 之后造成 增量 edit 未写入新log的问题
        self.level_slice.iter()
            .enumerate()
            .filter_map(|(level, vec_scope)| {
                (!vec_scope.is_empty())
                    .then(|| VersionEdit::NewFile((vec_scope.clone(), level), 0))
            })
            .collect_vec()
    }

    pub(crate) fn get_ss_table(&self, level: usize, offset: usize) -> Option<SSTable> {
        self.level_slice[level].get(offset)
            .and_then(|scope| self.ss_tables_loader.get(scope.get_gen()))
    }

    pub(crate) fn get_index(&self, level: usize, source_gen: i64) -> Option<usize> {
        self.level_slice[level].iter()
            .enumerate()
            .find(|(_, scope)| source_gen.eq(&scope.get_gen()))
            .map(|(index, _)| index)
    }

    pub(crate) fn first_ss_tables(&self, level: usize, size: usize) -> Option<(Vec<&SSTable>, Vec<Scope>)> {
        if self.level_slice[level].is_empty() {
            return None;
        }

        Some(self.level_slice[level]
            .iter()
            .take(size)
            .filter_map(|scope| {
                self.ss_tables_loader
                    .get(scope.get_gen())
                    .map(|ss_table| (ss_table, scope.clone()))
            })
            .unzip())
    }

    /// 获取指定level中与scope冲突的SSTables和Scopes
    pub(crate) fn get_meet_scope_ss_tables_with_scopes(&self, level: usize, target_scope: &Scope) -> (Vec<&SSTable>, Vec<Scope>) {
        self.level_slice[level].iter()
            .filter(|scope| scope.meet(target_scope))
            .filter_map(|scope| {
                self.ss_tables_loader
                    .get(scope.get_gen())
                    .map(|ss_table| (ss_table, scope.clone()))
            })
            .unzip()
    }

    /// 获取指定level中与scope冲突的SSTables
    pub(crate) fn get_meet_scope_ss_tables<F>(&self, level: usize, fn_meet: F) -> Vec<&SSTable>
        where F: Fn(&Scope) -> bool
    {
        self.level_slice[level].iter()
            .filter(|scope| fn_meet(scope))
            .filter_map(|scope| self.ss_tables_loader.get(scope.get_gen()))
            .collect_vec()
    }

    /// 使用Key从现有SSTables中获取对应的数据
    pub(crate) fn find_data_for_ss_tables(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let ss_table_loader = &self.ss_tables_loader;
        let block_cache = &self.block_cache;

        // Level 0的SSTable是无序且SSTable间的数据是可能重复的,因此需要遍历
        for scope in self.level_slice[LEVEL_0]
            .iter()
            .rev()
        {
            if scope.meet_with_key(key) {
                if let Some(ss_table) = ss_table_loader.get(scope.get_gen()) {
                    if let Some(value) = ss_table.query_with_key(key, block_cache)? {
                        return Ok(Some(value));
                    }
                }
            }
        }
        // Level 1-7的数据排布有序且唯一，因此在每一个等级可以直接找到唯一一个Key可能在范围内的SSTable
        for level in 1..7 {
            let offset = self.query_meet_index(key, level);

            if let Some(scope) = self.level_slice[level].get(offset) {
                return if let Some(ss_table) = ss_table_loader.get(scope.get_gen()) {
                    ss_table.query_with_key(key, block_cache)
                } else { Ok(None) };
            }
        }

        Ok(None)
    }

    pub(crate) fn query_meet_index(&self, key: &[u8], level: usize) -> usize {
        self.level_slice[level]
            .binary_search_by(|scope| scope.start.as_ref().cmp(key))
            .unwrap_or_else(|index| index.saturating_sub(1))
    }

    /// 判断是否溢出指定的SSTable数量
    pub(crate) fn is_threshold_exceeded_major(&self, config: &Config, level: usize) -> bool {
        self.level_slice[level].len() >=
            (config.major_threshold_with_sst_size * config.level_sst_magnification.pow(level as u32))
    }
}

impl VersionMeta {
    // MetaData对SSTable统计数据处理
    fn statistical_process(
        &mut self,
        ss_table_loader: &SSTableLoader,
        vec_statistics_gen: Vec<(EditType, i64)>,
    ) -> Result<()> {
        // 优先对新增数据进行统计再统一减去对应的数值避免删除动作聚集在前部分导致数值溢出
        for (event_type, gen) in vec_statistics_gen
            .into_iter()
            .sorted_by_key(|(edit_type, _)| *edit_type)
        {
            // FIXME: 此处通过读取SSTable获取其统计信息，这会导致若是Cleaner删除SSTable后会丢失此统计数据
            if let Some((size_of_disk, len)) = ss_table_loader.get(gen)
                .map(|ss_table| (ss_table.get_size_of_disk(), ss_table.len()))
            {
                match event_type {
                    EditType::Add => {
                        self.size_of_disk += size_of_disk;
                        self.len += len;
                    }
                    EditType::Del => {
                        self.size_of_disk -= size_of_disk;
                        self.len -= len;
                    }
                }
            }
        }

        Ok(())
    }
}

impl Drop for Version {
    /// 将此Version可删除的版本号发送
    fn drop(&mut self) {
        if self.clean_tx.send(CleanTag::Clean(self.version_num)).is_err() {
            error!("[Cleaner][clean][Version: {}]: Channel Close!", self.version_num);
        }
    }
}

/// 使用特定格式进行display
fn version_display(new_version: &Version, method: &str) {
    info!(
            "[Version: {}]: version_num: {}, len: {}, size_of_disk: {}",
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
    use crate::kernel::io::{FileExtension, IoFactory, IoType};
    use crate::kernel::lsm::log::LogLoader;
    use crate::kernel::lsm::lsm_kv::Config;
    use crate::kernel::lsm::ss_table::SSTable;
    use crate::kernel::lsm::version::{DEFAULT_SS_TABLE_PATH, DEFAULT_VERSION_PATH, Version, VersionEdit, VersionStatus};
    use crate::kernel::Result;

    #[test]
    fn test_version_clean() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        tokio_test::block_on(async move {
            let config = Config::new(temp_dir.into_path());

            let (wal, _, _) = LogLoader::reload(
                config.path(),
                (DEFAULT_VERSION_PATH, Some(1)),
                IoType::Direct,
                |_| Ok(()),
            )?;

            // 注意：将ss_table的创建防止VersionStatus的创建前
            // 因为VersionStatus检测无Log时会扫描当前文件夹下的SSTable进行重组以进行容灾
            let ver_status =
                VersionStatus::load_with_path(config.clone(), wal.clone())?;


            let sst_factory = IoFactory::new(
                config.dir_path.join(DEFAULT_SS_TABLE_PATH),
                FileExtension::SSTable,
            )?;

            let (ss_table_1, scope_1) = SSTable::create_for_mem_table(
                &config,
                1,
                &sst_factory,
                vec![(Bytes::from_static(b"test"), None)],
                0,
                IoType::Direct,
            )?;

            let (ss_table_2, scope_2) = SSTable::create_for_mem_table(
                &config,
                2,
                &sst_factory,
                vec![(Bytes::from_static(b"test"), None)],
                0,
                IoType::Direct,
            )?;

            ver_status.insert_vec_ss_table(vec![ss_table_1])?;
            ver_status.insert_vec_ss_table(vec![ss_table_2])?;

            let vec_edit_1 = vec![
                VersionEdit::NewFile((vec![scope_1], 0), 0),
            ];

            ver_status.log_and_apply(vec_edit_1, 2).await?;

            let version_1 = Arc::clone(&ver_status.current().await);

            let vec_edit_2 = vec![
                VersionEdit::NewFile((vec![scope_2.clone()], 0), 0),
                VersionEdit::DeleteFile((vec![1], 0)),
            ];

            ver_status.log_and_apply(vec_edit_2, 2).await?;

            let version_2 = Arc::clone(&ver_status.current().await);

            let vec_edit_3 = vec![
                VersionEdit::DeleteFile((vec![2], 0)),
            ];

            // 用于去除version2的引用计数
            ver_status.log_and_apply(vec_edit_3, 2).await?;

            // 测试对比快照
            let (_, snapshot, _) = LogLoader::reload(
                config.path(),
                (DEFAULT_VERSION_PATH, None),
                IoType::Direct,
                |bytes| Ok(bincode::deserialize::<Vec<VersionEdit>>(bytes)?),
            )?;

            assert_eq!(
                snapshot,
                vec![
                    vec![VersionEdit::NewFile((vec![scope_2], 0), 0)],
                    vec![VersionEdit::DeleteFile((vec![2], 0)),]
                ]
            );


            assert!(sst_factory.exists(1)?);
            assert!(sst_factory.exists(2)?);

            drop(version_2);

            assert!(sst_factory.exists(1)?);
            assert!(sst_factory.exists(2)?);

            drop(version_1);
            time::sleep(Duration::from_secs(1)).await;

            assert!(!sst_factory.exists(1)?);
            assert!(sst_factory.exists(2)?);

            drop(ver_status);
            time::sleep(Duration::from_secs(1)).await;

            assert!(!sst_factory.exists(1)?);
            assert!(!sst_factory.exists(2)?);

            Ok(())
        })
    }

    #[test]
    fn test_version_apply_and_log() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        tokio_test::block_on(async move {
            let config = Config::new(temp_dir.into_path());

            let (wal, _, _) = LogLoader::reload(
                config.path(),
                (DEFAULT_VERSION_PATH, Some(1)),
                IoType::Direct,
                |_| Ok(()),
            )?;

            // 注意：将ss_table的创建防止VersionStatus的创建前
            // 因为VersionStatus检测无Log时会扫描当前文件夹下的SSTable进行重组以进行容灾
            let ver_status_1 =
                VersionStatus::load_with_path(config.clone(), wal.clone())?;


            let sst_factory = IoFactory::new(
                config.dir_path.join(DEFAULT_SS_TABLE_PATH),
                FileExtension::SSTable,
            )?;

            let (ss_table_1, scope_1) = SSTable::create_for_mem_table(
                &config,
                1,
                &sst_factory,
                vec![(Bytes::from_static(b"test"), None)],
                0,
                IoType::Direct,
            )?;

            let (ss_table_2, scope_2) = SSTable::create_for_mem_table(
                &config,
                2,
                &sst_factory,
                vec![(Bytes::from_static(b"test"), None)],
                0,
                IoType::Direct,
            )?;

            let vec_edit = vec![
                VersionEdit::NewFile((vec![scope_1], 0), 0),
                VersionEdit::NewFile((vec![scope_2], 0), 0),
                VersionEdit::DeleteFile((vec![2], 0)),
            ];

            ver_status_1.insert_vec_ss_table(vec![ss_table_1])?;
            ver_status_1.insert_vec_ss_table(vec![ss_table_2])?;
            ver_status_1.log_and_apply(vec_edit, 10).await?;

            let (ss_table_3, scope_3) = SSTable::create_for_mem_table(
                &config,
                3,
                &sst_factory,
                vec![(Bytes::from_static(b"test3"), None)],
                0,
                IoType::Direct,
            )?;

            let (ss_table_4, scope_4) = SSTable::create_for_mem_table(
                &config,
                4,
                &sst_factory,
                vec![(Bytes::from_static(b"test4"), None)],
                0,
                IoType::Direct,
            )?;

            let vec_edit2 = vec![
                VersionEdit::NewFile((vec![scope_3], 0), 0),
                VersionEdit::NewFile((vec![scope_4], 0), 0),
            ];

            ver_status_1.insert_vec_ss_table(vec![ss_table_3])?;
            ver_status_1.insert_vec_ss_table(vec![ss_table_4])?;
            ver_status_1.log_and_apply(vec_edit2, 10).await?;

            let version_1 = Version::clone(ver_status_1.current().await.as_ref());

            drop(ver_status_1);

            let ver_status_2 =
                VersionStatus::load_with_path(config, wal.clone())?;
            let version_2 = ver_status_2.current().await;

            assert_eq!(version_1.level_slice, version_2.level_slice);
            assert_eq!(version_1.meta_data, version_2.meta_data);

            Ok(())
        })
    }
}



