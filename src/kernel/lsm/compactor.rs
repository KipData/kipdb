use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;
use bytes::Bytes;
use futures::future;
use itertools::Itertools;
use tokio::sync::oneshot;
use tracing::info;
use crate::KernelError;
use crate::kernel::io::IoFactory;
use crate::kernel::Result;
use crate::kernel::lsm::block::BlockCache;
use crate::kernel::lsm::lsm_kv::{Config, Gen, StoreInner};
use crate::kernel::lsm::data_sharding;
use crate::kernel::lsm::iterator::DiskIter;
use crate::kernel::lsm::iterator::sstable_iter::SSTableIter;
use crate::kernel::lsm::mem_table::{KeyValue, MemTable};
use crate::kernel::lsm::ss_table::{Scope, SSTable};
use crate::kernel::lsm::version::{VersionEdit, VersionStatus};

pub(crate) const LEVEL_0: usize = 0;

/// 数据分片集
/// 包含对应分片的Gen与数据
pub(crate) type MergeShardingVec = Vec<(i64, Vec<KeyValue>)>;

/// Major压缩时的待删除Gen封装(N为此次Major所压缩的Level)，第一个为Level N级，第二个为Level N+1级
pub(crate) type DelGenVec = (Vec<i64>, Vec<i64>);

/// Store与Compactor的交互信息
#[derive(Debug)]
pub(crate) enum CompactTask {
    Flush(Option<oneshot::Sender<()>>)
}

/// 压缩器
///
/// 负责Minor和Major压缩
pub(crate) struct Compactor {
    store_inner: Arc<StoreInner>,
}

impl Compactor {

    pub(crate) fn new(store_inner: Arc<StoreInner>) -> Self {
        Compactor { store_inner }
    }

    fn sst_factory(&self) -> &IoFactory {
        self.store_inner.ver_status.get_sst_factory_ref()
    }

    /// 检查并进行压缩 （默认为 异步、被动 的Lazy压缩）
    ///
    /// 默认为try检测是否超出阈值，主要思路为以被动定时检测的机制使
    /// 多事务的commit脱离Compactor的耦合，
    /// 同时减少高并发事务或写入时的频繁Compaction，优先写入后统一压缩，
    /// 减少Level 0热数据的SSTable的冗余数据
    pub(crate) async fn check_then_compaction(
        &mut self,
        option_tx: Option<oneshot::Sender<()>>
    ) -> Result<()> {
        if let Some(values) = self.mem_table().swap() {
            if !values.is_empty() {
                let gen = self.switch_wal()?;
                let start = Instant::now();
                // 目前minor触发major时是同步进行的，所以此处对live_tag是在此方法体保持存活
                self.minor_compaction(gen, values).await?;
                info!("[Compactor][Compaction Drop][Time: {:?}]", start.elapsed());
            }
        }

        // 压缩请求响应
        if let Some(tx) = option_tx { tx.send(()).map_err(|_| KernelError::ChannelClose)? }

        Ok(())
    }

    /// 创建gen
    ///
    /// 需要保证获取到了MemTable的写锁以保证wal在switch时MemTable的数据和Wal不一致(多出几条)
    /// 当wal配置启用时，使用预先记录的gen作为结果
    fn switch_wal(&self) -> Result<i64> {
        let next_gen = Gen::create();

        Ok(if self.config().wal_enable {
            self.store_inner.wal.switch(next_gen)?
        } else {
            next_gen
        })
    }

    /// 持久化immutable_table为SSTable
    ///
    /// 请注意：vec_values必须是依照key值有序的
    pub(crate) async fn minor_compaction(&self, gen: i64, values: Vec<KeyValue>) -> Result<()> {
        if !values.is_empty() {
            // 从内存表中将数据持久化为ss_table
            let ss_table = SSTable::create_for_mem_table(
                self.config(),
                gen,
                self.sst_factory(),
                values,
                LEVEL_0
            )?;

            self.ver_status().insert_vec_ss_table(vec![ss_table]).await?;

            // `Compactor::data_loading_with_level`中会检测是否达到压缩阈值，因此此处直接调用Major压缩
            self.major_compaction(
                LEVEL_0,
                vec![VersionEdit::NewFile((vec![gen], 0), 0)]
            ).await?;
        }
        Ok(())
    }

    /// Major压缩，负责将不同Level之间的数据向下层压缩转移
    /// 目前Major压缩的大体步骤是
    /// 1. 获取当前Version，读取当前Level的指定数量SSTable，命名为vec_ss_table_l
    /// 2. vec_ss_table_l的每个SSTable中的scope属性进行融合，并以此获取下一Level与该scope相交的SSTable，命名为vec_ss_table_l_1
    /// 3. 获取的vec_ss_table_l_1向上一Level进行类似第2步骤的措施，获取两级之间压缩范围内最恰当的数据
    /// 4. vec_ss_table_l与vec_ss_table_l_1之间的数据并行取出排序归并去重等处理后，分片成多个Vec<KeyValue>
    /// 6. 并行将每个分片各自生成SSTable
    /// 7. 生成的SSTables插入到vec_ss_table_l的第一个SSTable位置，并将vec_ss_table_l和vec_ss_table_l_1的SSTable删除
    /// 8. 将变更的SSTable插入至vec_ver_edit以持久化
    /// Final: 将vec_ver_edit中的数据进行log_and_apply生成新的Version作为最新状态
    ///
    /// 经过压缩测试，Level 1的SSTable总是较多，根据原理推断：
    /// Level0的Key基本是无序的，容易生成大量的SSTable至Level1
    /// 而Level1-7的Key排布有序，故转移至下一层的SSTable数量较小
    /// 因此大量数据压缩的情况下Level 1的SSTable数量会较多
    pub(crate) async fn major_compaction(&self, mut level: usize, mut vec_ver_edit: Vec<VersionEdit>) -> Result<()> {
        if level > 6 {
            return Err(KernelError::LevelOver);
        }

        while level < 7 {
            if let Some((index, (del_gens_l, del_gens_ll), vec_sharding)) =
                self.data_loading_with_level(level).await?
            {

                let start = Instant::now();
                // 并行创建SSTable
                let ss_table_futures = vec_sharding.into_iter()
                    .map(|(gen, sharding)| {
                        async move {
                            SSTable::create_for_mem_table(
                                self.config(),
                                gen,
                                self.sst_factory(),
                                sharding,
                                level + 1
                            )
                        }
                    });
                let vec_new_ss_table: Vec<SSTable> = future::try_join_all(ss_table_futures).await?;

                let vec_new_sst_gen = vec_new_ss_table.iter()
                    .map(SSTable::get_gen)
                    .collect_vec();
                self.ver_status()
                    .insert_vec_ss_table(vec_new_ss_table).await?;
                vec_ver_edit.append(&mut vec![
                    VersionEdit::NewFile((vec_new_sst_gen, level + 1), index),
                    VersionEdit::DeleteFile((del_gens_l, level)),
                    VersionEdit::DeleteFile((del_gens_ll, level))
                ]);
                info!("[LsmStore][Major Compaction][recreate_sst][Level: {}][Time: {:?}]", level, start.elapsed());
                level += 1;
            } else { break }
        }
        self.ver_status()
            .log_and_apply(vec_ver_edit).await?;
        Ok(())
    }

    /// 通过Level进行归并数据加载
    async fn data_loading_with_level(&self, level: usize) -> Result<Option<(usize, DelGenVec, MergeShardingVec)>> {
        let version = self.ver_status().current().await;
        let config = self.config();
        let major_select_file_size = config.major_select_file_size;

        // 如果该Level的SSTables数量尚未越出阈值则提取返回空
        if level > 5 || !version.is_threshold_exceeded_major(config, level) { return Ok(None); }

        // 此处vec_ss_table_l指此level的Vec<SSTable>, vec_ss_table_ll则是下一级的Vec<SSTable>
        // 类似罗马数字
        if let Some(mut vec_ss_table_l) = version
            .get_first_vec_ss_table_with_size(level, major_select_file_size).await
        {
            let start = Instant::now();
            let scope_l = Scope::fusion_from_vec_ss_table(&vec_ss_table_l)?;
            // 获取下一级中有重复键值范围的SSTable
            let ss_tables_ll = version.get_meet_scope_ss_tables(level + 1, &scope_l).await;
            let index = SSTable::find_index_with_level(
                ss_tables_ll.first().map(SSTable::get_gen),
                &version,
                level + 1
            );

            // 若为Level 0则与获取同级下是否存在有键值范围冲突数据并插入至del_gen_l中
            if level == LEVEL_0 {
                vec_ss_table_l.append(
                    &mut version.get_meet_scope_ss_tables(level, &scope_l).await
                )
            }

            // 收集需要清除的SSTable
            let del_gen_l = SSTable::collect_gen(&vec_ss_table_l)?;
            let del_gen_ll = SSTable::collect_gen(&ss_tables_ll)?;

            // 此处没有chain vec_ss_table_l是因为在vec_ss_table_ll是由vec_ss_table_l检测冲突而获取到的
            // 因此使用vec_ss_table_ll向上检测冲突时获取的集合应当含有vec_ss_table_l的元素
            let ss_tables_l_final = match Scope::fusion_from_vec_ss_table(&ss_tables_ll) {
                Ok(scope_ll) => version.get_meet_scope_ss_tables(level, &scope_ll).await,
                Err(_) => vec_ss_table_l
            }.into_iter()
                .unique_by(SSTable::get_gen)
                .collect_vec();

            // 数据合并并切片
            let vec_merge_sharding =
                Self::data_merge_and_sharding(
                    ss_tables_l_final,
                    ss_tables_ll,
                    &version.block_cache,
                    config.sst_file_size
                ).await?;

            info!(
                "[LsmStore][Major Compaction][data_loading_with_level][Time: {:?}]",
                start.elapsed()
            );

            Ok(Some((index, (del_gen_l, del_gen_ll), vec_merge_sharding)))
        } else {
            Ok(None)
        }
    }

    /// 以SSTables的数据归并再排序后切片，获取以KeyValue的Key值由小到大的切片排序
    /// 1. 并行获取Level l(当前等级)的待合并SSTables_l的全量数据
    /// 2. 基于SSTables_l获取唯一KeySet用于迭代过滤
    /// 3. 并行对Level ll的SSTables_ll通过KeySet进行迭代同时过滤数据
    /// 4. 组合SSTables_l和SSTables_ll的数据合并并进行唯一，排序处理
    async fn data_merge_and_sharding(
        ss_tables_l: Vec<SSTable>,
        ss_tables_ll: Vec<SSTable>,
        block_cache: &BlockCache,
        sst_file_size: usize
    ) -> Result<MergeShardingVec> {
        // SSTables的Gen会基于时间有序生成,所有以此作为SSTables的排序依据
        let map_futures_l = ss_tables_l.iter()
            .sorted_unstable_by_key(|ss_table| ss_table.get_gen())
            .map(|ss_table| Self::ss_table_load_data(block_cache, ss_table, |_| true));

        let sharding_l = future::try_join_all(map_futures_l).await?;

        // 获取Level l的唯一KeySet用于Level ll的迭代过滤数据
        let filter_set_l: HashSet<&Bytes> = sharding_l.iter()
            .flatten()
            .map(|key_value| &key_value.0)
            .collect();

        // 通过KeySet过滤出Level l中需要补充的数据
        // 并行: 因为即使l为0时，此时的ll(Level 1)仍然保证SSTable数据之间排列有序且不冲突，因此并行迭代不会导致数据冲突
        // 过滤: 基于l进行数据过滤避免冗余的数据迭代导致占用大量内存占用
        let sharding_ll = future::try_join_all(ss_tables_ll.iter()
                .map(|ss_table| Self::ss_table_load_data(
                    block_cache, ss_table, |key| !filter_set_l.contains(key))
                )).await?;

        // 使用sharding_ll来链接sharding_l以保持数据倒序的顺序是由新->旧
        let vec_cmd_data = sharding_ll
            .into_iter()
            .chain(sharding_l)
            .flatten()
            .rev()
            .unique_by(|(key, _)| key.clone())
            .sorted_unstable_by_key(|(key, _)| key.clone())
            .collect();
        Ok(data_sharding(vec_cmd_data, sst_file_size))
    }

    async fn ss_table_load_data<F>(block_cache: &BlockCache, ss_table: &SSTable, fn_is_filter: F) -> Result<Vec<KeyValue>>
        where F: Fn(&Bytes) -> bool
    {
        let mut iter = SSTableIter::new(ss_table, block_cache)?;
        let mut vec_cmd = Vec::with_capacity(iter.len());
        loop {
            match iter.next() {
                Ok(item) => {
                    if fn_is_filter(&item.0) { vec_cmd.push(item) }
                }
                Err(KernelError::OutOfBounds) => break,
                Err(e) => { return Err(e) }
            }
        }
        Ok(vec_cmd)
    }

    pub(crate) fn config(&self) -> &Config {
        &self.store_inner.config
    }

    pub(crate) fn mem_table(&self) -> &MemTable {
        &self.store_inner.mem_table
    }

    pub(crate) fn ver_status(&self) -> &VersionStatus {
        &self.store_inner.ver_status
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;
    use bytes::Bytes;
    use tempfile::TempDir;
    use crate::kernel::io::{FileExtension, IoFactory};
    use crate::kernel::lsm::compactor::Compactor;
    use crate::kernel::lsm::lsm_kv::Config;
    use crate::kernel::lsm::ss_table::SSTable;
    use crate::kernel::lsm::version::DEFAULT_SS_TABLE_PATH;
    use crate::kernel::Result;
    use crate::kernel::utils::lru_cache::ShardingLruCache;

    #[test]
    fn test_data_merge() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let config = Config::new(temp_dir.into_path());
        let sst_factory = IoFactory::new(
            config.dir_path.join(DEFAULT_SS_TABLE_PATH),
            FileExtension::SSTable
        )?;
        let cache = ShardingLruCache::new(
            config.block_cache_size,
            16,
            RandomState::default()
        )?;
        let ss_table_1 = SSTable::create_for_mem_table(
            &config,
            1,
            &sst_factory,
            vec![
                (Bytes::from_static(b"1"), Some(Bytes::from_static(b"1"))),
                (Bytes::from_static(b"2"), Some(Bytes::from_static(b"2"))),
                (Bytes::from_static(b"3"), Some(Bytes::from_static(b"31")))
            ],
            0
        )?;
        let ss_table_2 = SSTable::create_for_mem_table(
            &config,
            2,
            &sst_factory,
            vec![
                (Bytes::from_static(b"3"), Some(Bytes::from_static(b"3"))),
                (Bytes::from_static(b"4"), Some(Bytes::from_static(b"4")))
            ],
            0
        )?;
        let ss_table_3 = SSTable::create_for_mem_table(
            &config,
            3,
            &sst_factory,
            vec![
                (Bytes::from_static(b"1"), Some(Bytes::from_static(b"11"))),
                (Bytes::from_static(b"2"), Some(Bytes::from_static(b"21")))
            ],
            1
        )?;
        let ss_table_4 = SSTable::create_for_mem_table(
            &config,
            4,
            &sst_factory,
            vec![
                (Bytes::from_static(b"3"), Some(Bytes::from_static(b"32"))),
                (Bytes::from_static(b"4"), Some(Bytes::from_static(b"41"))),
                (Bytes::from_static(b"5"), Some(Bytes::from_static(b"5")))
            ],
            1
        )?;

        let (_, vec_data) = &tokio_test::block_on(async move {
            Compactor::data_merge_and_sharding(
                vec![ss_table_1, ss_table_2],
                vec![ss_table_3, ss_table_4],
                &cache,
                config.sst_file_size
            ).await
        })?[0];

        assert_eq!(vec_data, &vec![
            (Bytes::from_static(b"1"), Some(Bytes::from_static(b"1"))),
            (Bytes::from_static(b"2"), Some(Bytes::from_static(b"2"))),
            (Bytes::from_static(b"3"), Some(Bytes::from_static(b"3"))),
            (Bytes::from_static(b"4"), Some(Bytes::from_static(b"4"))),
            (Bytes::from_static(b"5"), Some(Bytes::from_static(b"5")))
        ]);
        Ok(())
    }
}
