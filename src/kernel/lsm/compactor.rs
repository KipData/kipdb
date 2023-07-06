use crate::kernel::lsm::data_sharding;
use crate::kernel::lsm::mem_table::{KeyValue, MemTable};
use crate::kernel::lsm::storage::{Config, StoreInner};
use crate::kernel::lsm::table::meta::TableMeta;
use crate::kernel::lsm::table::scope::Scope;
use crate::kernel::lsm::table::{collect_gen, Table};
use crate::kernel::lsm::version::edit::VersionEdit;
use crate::kernel::lsm::version::status::VersionStatus;
use crate::kernel::Result;
use crate::KernelError;
use bytes::Bytes;
use futures::future;
use itertools::Itertools;
use std::collections::HashSet;
use std::mem;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::oneshot;
use tracing::info;

pub(crate) const LEVEL_0: usize = 0;

/// 数据分片集
/// 包含对应分片的Gen与数据
pub(crate) type MergeShardingVec = Vec<(i64, Vec<KeyValue>)>;
pub(crate) type DelNode = (Vec<i64>, TableMeta);
/// Major压缩时的待删除Gen封装(N为此次Major所压缩的Level)，第一个为Level N级，第二个为Level N+1级
pub(crate) type DelNodeTuple = (DelNode, DelNode);

/// Store与Compactor的交互信息
#[derive(Debug)]
pub(crate) enum CompactTask {
    Manual(Scope),
    Flush(Option<oneshot::Sender<()>>),
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

    /// 检查并进行压缩 （默认为 异步、被动 的Lazy压缩）
    ///
    /// 默认为try检测是否超出阈值，主要思路为以被动定时检测的机制使
    /// 多事务的commit脱离Compactor的耦合，
    /// 同时减少高并发事务或写入时的频繁Compaction，优先写入后统一压缩，
    /// 减少Level 0热数据的SSTable的冗余数据
    pub(crate) async fn check_then_compaction(
        &mut self,
        option_tx: Option<oneshot::Sender<()>>,
    ) -> Result<()> {
        if let Some((gen, values)) = self.mem_table().swap()? {
            if !values.is_empty() {
                let start = Instant::now();
                // 目前minor触发major时是同步进行的，所以此处对live_tag是在此方法体保持存活
                self.minor_compaction(gen, values).await?;
                info!("[Compactor][Compaction Drop][Time: {:?}]", start.elapsed());
            }
        }

        // 压缩请求响应
        if let Some(tx) = option_tx {
            tx.send(()).map_err(|_| KernelError::ChannelClose)?
        }

        Ok(())
    }

    /// 持久化immutable_table为SSTable
    ///
    /// 请注意：vec_values必须是依照key值有序的
    pub(crate) async fn minor_compaction(&self, gen: i64, values: Vec<KeyValue>) -> Result<()> {
        if !values.is_empty() {
            let (scope, meta) = self.ver_status().loader().create(
                gen,
                values,
                LEVEL_0,
                self.config().level_table_type[LEVEL_0],
            )?;

            // `Compactor::data_loading_with_level`中会检测是否达到压缩阈值，因此此处直接调用Major压缩
            self.major_compaction(
                LEVEL_0,
                scope.clone(),
                vec![VersionEdit::NewFile((vec![scope], 0), 0, meta)],
            )
            .await?;
        }
        Ok(())
    }

    /// Major压缩，负责将不同Level之间的数据向下层压缩转移
    /// 目前Major压缩的大体步骤是
    /// 1. 获取当前Version，通过传入的指定Scope得到下一Level与该scope相交的SSTable，命名为tables_ll
    /// 2. 获取的tables_ll向上一Level进行类似第2步骤的措施，获取两级之间压缩范围内最恰当的数据
    /// 3. tables_l与tables_ll之间的数据并行取出排序归并去重等处理后，分片成多个Vec<KeyValue>
    /// 4. 并行将每个分片各自生成SSTable
    /// 5. 生成的SSTables插入到tables_l的第一个SSTable位置，并将tables_l和tables_ll的SSTable删除
    /// 6. 将变更的SSTable插入至vec_ver_edit以持久化
    /// Final: 将vec_ver_edit中的数据进行log_and_apply生成新的Version作为最新状态
    ///
    /// 经过压缩测试，Level 1的SSTable总是较多，根据原理推断：
    /// Level0的Key基本是无序的，容易生成大量的SSTable至Level1
    /// 而Level1-7的Key排布有序，故转移至下一层的SSTable数量较小
    /// 因此大量数据压缩的情况下Level 1的SSTable数量会较多
    pub(crate) async fn major_compaction(
        &self,
        mut level: usize,
        scope: Scope,
        mut vec_ver_edit: Vec<VersionEdit>,
    ) -> Result<()> {
        let config = self.config();
        let mut is_over = false;

        if level > 6 {
            return Err(KernelError::LevelOver);
        }

        while level < 7 && !is_over {
            let next_level = level + 1;

            if let Some((
                index,
                ((del_gens_l, del_meta_l), (del_gens_ll, del_meta_ll)),
                vec_sharding,
            )) = self.data_loading_with_level(level, &scope).await?
            {
                let start = Instant::now();
                // 并行创建SSTable
                let table_futures = vec_sharding.into_iter().map(|(gen, sharding)| async move {
                    self.ver_status().loader().create(
                        gen,
                        sharding,
                        next_level,
                        config.level_table_type[next_level],
                    )
                });
                let vec_table_and_scope: Vec<(Scope, TableMeta)> =
                    future::try_join_all(table_futures).await?;
                let (new_scopes, new_metas): (Vec<Scope>, Vec<TableMeta>) =
                    vec_table_and_scope.into_iter().unzip();
                let fusion_meta = TableMeta::fusion(&new_metas);

                vec_ver_edit.append(&mut vec![
                    VersionEdit::NewFile((new_scopes, next_level), index, fusion_meta),
                    VersionEdit::DeleteFile((del_gens_l, level), del_meta_l),
                    VersionEdit::DeleteFile((del_gens_ll, next_level), del_meta_ll),
                ]);
                info!(
                    "[LsmStore][Major Compaction][recreate_sst][Level: {}][Time: {:?}]",
                    level,
                    start.elapsed()
                );
                level += 1;
            } else {
                is_over = true;
            }
            if !vec_ver_edit.is_empty() {
                self.ver_status()
                    .log_and_apply(
                        mem::take(&mut vec_ver_edit),
                        config.ver_log_snapshot_threshold,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    /// 通过Level进行归并数据加载
    async fn data_loading_with_level(
        &self,
        level: usize,
        target: &Scope,
    ) -> Result<Option<(usize, DelNodeTuple, MergeShardingVec)>> {
        let version = self.ver_status().current().await;
        let config = self.config();
        let next_level = level + 1;

        // 如果该Level的SSTables数量尚未越出阈值则提取返回空
        if level > 5 || !version.is_threshold_exceeded_major(config, level) {
            return Ok(None);
        }

        // 此处vec_table_l指此level的Vec<SSTable>, vec_table_ll则是下一级的Vec<SSTable>
        // 类似罗马数字
        let start = Instant::now();

        // 获取此级中有重复键值范围的SSTable
        let (tables_l, scopes_l, _) = version.tables_by_scopes(level, target);
        // 因此使用tables_l向下检测冲突时获取的集合应当含有tables_ll的元素
        let fusion_scope_l = Scope::fusion(&scopes_l).unwrap_or(target.clone());
        // 通过tables_l的scope获取下一级的父集
        let (tables_ll, _, index) = version.tables_by_scopes(next_level, &fusion_scope_l);

        // 收集需要清除的SSTable
        let del_gen_l = collect_gen(&tables_l)?;
        let del_gen_ll = collect_gen(&tables_ll)?;

        // 数据合并并切片
        let vec_merge_sharding =
            Self::data_merge_and_sharding(tables_l, tables_ll, config.sst_file_size).await?;
        info!(
            "[LsmStore][Major Compaction][data_loading_with_level][Time: {:?}]",
            start.elapsed()
        );

        Ok(Some((index, (del_gen_l, del_gen_ll), vec_merge_sharding)))
    }

    /// 以SSTables的数据归并再排序后切片，获取以KeyValue的Key值由小到大的切片排序
    /// 1. 并行获取Level l(当前等级)的待合并SSTables_l的全量数据
    /// 2. 基于SSTables_l获取唯一KeySet用于迭代过滤
    /// 3. 并行对Level ll的SSTables_ll通过KeySet进行迭代同时过滤数据
    /// 4. 组合SSTables_l和SSTables_ll的数据合并并进行唯一，排序处理
    #[allow(clippy::mutable_key_type)]
    async fn data_merge_and_sharding(
        tables_l: Vec<&dyn Table>,
        tables_ll: Vec<&dyn Table>,
        file_size: usize,
    ) -> Result<MergeShardingVec> {
        // SSTables的Gen会基于时间有序生成,所有以此作为SSTables的排序依据
        let map_futures_l = tables_l
            .iter()
            .sorted_unstable_by_key(|table| table.gen())
            .map(|table| async { Self::table_load_data(table, |_| true) });

        let sharding_l = future::try_join_all(map_futures_l).await?;

        // 获取Level l的唯一KeySet用于Level ll的迭代过滤数据
        let filter_set_l: HashSet<&Bytes> = sharding_l
            .iter()
            .flatten()
            .map(|key_value| &key_value.0)
            .collect();

        // 通过KeySet过滤出Level l中需要补充的数据
        // 并行: 因为即使l为0时，此时的ll(Level 1)仍然保证SSTable数据之间排列有序且不冲突，因此并行迭代不会导致数据冲突
        // 过滤: 基于l进行数据过滤避免冗余的数据迭代导致占用大量内存占用
        let sharding_ll = future::try_join_all(tables_ll.iter().map(|table| async {
            Self::table_load_data(table, |key| !filter_set_l.contains(key))
        }))
        .await?;

        // 使用sharding_ll来链接sharding_l以保持数据倒序的顺序是由新->旧
        let vec_cmd_data = sharding_ll
            .into_iter()
            .chain(sharding_l)
            .flatten()
            .rev()
            .unique_by(|(key, _)| key.clone())
            .sorted_unstable_by_key(|(key, _)| key.clone())
            .collect();
        Ok(data_sharding(vec_cmd_data, file_size))
    }

    fn table_load_data<F>(table: &&dyn Table, fn_is_filter: F) -> Result<Vec<KeyValue>>
    where
        F: Fn(&Bytes) -> bool,
    {
        let mut iter = table.iter()?;
        let mut vec_cmd = Vec::with_capacity(table.len());
        while let Some(item) = iter.next_err()? {
            if fn_is_filter(&item.0) {
                vec_cmd.push(item)
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
    use crate::kernel::io::{FileExtension, IoFactory, IoType};
    use crate::kernel::lsm::compactor::{Compactor, LEVEL_0};
    use crate::kernel::lsm::storage::{Config, KipStorage};
    use crate::kernel::lsm::table::ss_table::SSTable;
    use crate::kernel::lsm::version::DEFAULT_SS_TABLE_PATH;
    use crate::kernel::utils::lru_cache::ShardingLruCache;
    use crate::kernel::{Result, Storage};
    use bytes::Bytes;
    use std::collections::hash_map::RandomState;
    use std::sync::Arc;
    use std::time::Instant;
    use itertools::Itertools;
    use tempfile::TempDir;
    use crate::kernel::lsm::table::scope::Scope;
    use crate::kernel::lsm::trigger::TriggerType;

    #[test]
    fn test_lsm_major_compactor() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        tokio_test::block_on(async move {
            let times = 30_000;

            let value = b"Stray birds of summer come to my window to sing and fly away.
            And yellow leaves of autumn, which have no songs, flutter and fall
            there with a sign.";

            // Tips: 此处由于倍率为1且阈值固定为4，因此容易导致Level 1高出阈值时候导致归并转移到Level 2时，
            // 重复触发阈值，导致迁移到Level6之中，此情况是理想之中的
            // 普通场景下每个Level之间的阈值数量是有倍数递增的，因此除了极限情况以外，不会发送这种逐级转移的现象
            let config = Config::new(temp_dir.path().to_str().unwrap())
                .major_threshold_with_sst_size(4)
                .level_sst_magnification(1)
                .minor_trigger_with_threshold(TriggerType::Count, 1000);
            let kv_store = KipStorage::open_with_config(config).await?;
            let mut vec_kv = Vec::new();

            for i in 0..times {
                let vec_u8 = bincode::serialize(&i)?;
                vec_kv.push((
                    Bytes::from(vec_u8.clone()),
                    Bytes::from(vec_u8.into_iter().chain(value.to_vec()).collect_vec()),
                ));
            }

            let start = Instant::now();

            assert_eq!(times % 1000, 0);

            for i in 0..times / 1000 {
                for j in 0..1000 {
                    kv_store
                        .set(&vec_kv[i * 1000 + j].0, vec_kv[i * 1000 + j].1.clone())
                        .await?;
                }
                kv_store.flush().await?;
            }
            println!("[set_for][Time: {:?}]", start.elapsed());

            let version = kv_store.current_version().await;
            let level_slice = &version.level_slice;
            println!("Level_Slice: {:#?}", level_slice);
            assert!(!level_slice[0].is_empty());
            assert!(!level_slice[1].is_empty()
                || !level_slice[2].is_empty()
                || !level_slice[3].is_empty()
                || !level_slice[4].is_empty()
                || !level_slice[5].is_empty()
                || !level_slice[6].is_empty());

            for (level, slice) in level_slice.into_iter().enumerate() {
                if !slice.is_empty() && level != LEVEL_0 {
                    let mut tmp_scope: Option<&Scope> = None;

                    for scope in slice {
                        if let Some(last_scope) = tmp_scope {
                            assert!(last_scope.end < scope.start);
                        }
                        tmp_scope = Some(scope);
                    }
                }
            }

            assert_eq!(kv_store.len().await?, times);

            let start = Instant::now();
            for i in 0..times {
                assert_eq!(kv_store.get(&vec_kv[i].0).await?, Some(vec_kv[i].1.clone()));
            }
            println!("[get_for][Time: {:?}]", start.elapsed());
            kv_store.flush().await?;

            Ok(())
        })
    }

    #[test]
    fn test_data_merge() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let config = Config::new(temp_dir.into_path());
        let sst_factory = IoFactory::new(
            config.dir_path.join(DEFAULT_SS_TABLE_PATH),
            FileExtension::SSTable,
        )?;
        let cache = Arc::new(ShardingLruCache::new(
            config.block_cache_size,
            16,
            RandomState::default(),
        )?);
        let ss_table_1 = SSTable::new(
            &sst_factory,
            &config,
            Arc::clone(&cache),
            1,
            vec![
                (Bytes::from_static(b"1"), Some(Bytes::from_static(b"1"))),
                (Bytes::from_static(b"2"), Some(Bytes::from_static(b"2"))),
                (Bytes::from_static(b"3"), Some(Bytes::from_static(b"31"))),
            ],
            0,
            IoType::Direct,
        )?;
        let ss_table_2 = SSTable::new(
            &sst_factory,
            &config,
            Arc::clone(&cache),
            2,
            vec![
                (Bytes::from_static(b"3"), Some(Bytes::from_static(b"3"))),
                (Bytes::from_static(b"4"), Some(Bytes::from_static(b"4"))),
            ],
            0,
            IoType::Direct,
        )?;
        let ss_table_3 = SSTable::new(
            &sst_factory,
            &config,
            Arc::clone(&cache),
            3,
            vec![
                (Bytes::from_static(b"1"), Some(Bytes::from_static(b"11"))),
                (Bytes::from_static(b"2"), Some(Bytes::from_static(b"21"))),
            ],
            1,
            IoType::Direct,
        )?;
        let ss_table_4 = SSTable::new(
            &sst_factory,
            &config,
            Arc::clone(&cache),
            4,
            vec![
                (Bytes::from_static(b"3"), Some(Bytes::from_static(b"32"))),
                (Bytes::from_static(b"4"), Some(Bytes::from_static(b"41"))),
                (Bytes::from_static(b"5"), Some(Bytes::from_static(b"5"))),
            ],
            1,
            IoType::Direct,
        )?;

        let (_, vec_data) = &tokio_test::block_on(async move {
            Compactor::data_merge_and_sharding(
                vec![&ss_table_1, &ss_table_2],
                vec![&ss_table_3, &ss_table_4],
                config.sst_file_size,
            )
            .await
        })?[0];

        assert_eq!(
            vec_data,
            &vec![
                (Bytes::from_static(b"1"), Some(Bytes::from_static(b"1"))),
                (Bytes::from_static(b"2"), Some(Bytes::from_static(b"2"))),
                (Bytes::from_static(b"3"), Some(Bytes::from_static(b"3"))),
                (Bytes::from_static(b"4"), Some(Bytes::from_static(b"4"))),
                (Bytes::from_static(b"5"), Some(Bytes::from_static(b"5")))
            ]
        );
        Ok(())
    }
}
