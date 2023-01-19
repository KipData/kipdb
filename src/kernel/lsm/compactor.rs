use std::sync::Arc;
use std::time::Instant;
use futures::future;
use itertools::Itertools;
use tokio::sync::RwLock;
use tracing::{error, info};
use crate::{HashStore, KvsError};
use crate::kernel::io::IOHandlerFactory;
use crate::kernel::{CommandData, Result};
use crate::kernel::lsm::lsm_kv::{CommandCodec, Config, LsmStore, wal_put};
use crate::kernel::lsm::{data_sharding, Manifest};
use crate::kernel::lsm::ss_table::{Scope, SSTable};

pub(crate) const LEVEL_0: usize = 0;

/// 数据分片集
/// 包含对应分片的Gen与数据
pub(crate) type MergeShardingVec = Vec<(i64, Vec<CommandData>)>;

pub(crate) type ExpiredGenVec = Vec<i64>;

pub(crate) struct Compactor {
    manifest: Arc<RwLock<Manifest>>,
    config: Arc<Config>,
    io_handler_factory: Arc<IOHandlerFactory>,
    wal: Arc<HashStore>,
}

impl Compactor {

    pub(crate) fn new(manifest: Arc<RwLock<Manifest>>, config: Arc<Config>, io_handler_factory: Arc<IOHandlerFactory>, wal: Arc<HashStore>) -> Self {
        Self { manifest, config, io_handler_factory, wal }
    }

    /// 持久化immutable_table为SSTable
    pub(crate) async fn minor_compaction(&self,vec_keys: Vec<Vec<u8>>, vec_values: Vec<CommandData>) -> Result<()> {
        let mut manifest = self.manifest.write().await;
        let gen = self.config.create_gen();

        // 将这些索引的key序列化后预先存入wal中作防灾准备
        // 当持久化异常时将对应gen的key反序列化出来并从wal找到对应值
        // 此处通过config.wal_enable重复判断以提前避免序列化开销
        if self.config.wal_enable {
            wal_put(
                &self.wal,
                CommandCodec::encode_gen(gen)?,
                CommandCodec::encode_keys(&vec_keys)?,
                !self.config.wal_async_put_enable
            ).await;
        }
        // 从内存表中将数据持久化为ss_table
        let ss_table = SSTable::create_for_immutable_table(
            &self.config,
            gen,
            &self.io_handler_factory,
            vec_values,
            LEVEL_0
        ).await?;
        manifest.insert_ss_table_with_index(ss_table, 0, &self.io_handler_factory).await?;

        drop(manifest);
        if let Err(err) = self.major_compaction(LEVEL_0).await {
            error!("[LsmStore][major_compaction][error happen]: {:?}", err);
        }
        Ok(())
    }

    /// Major压缩，负责将不同Level之间的数据向下层压缩转移
    /// 目前Major压缩的大体步骤是
    /// 1、获取manifest读锁，读取当前Level的指定数量SSTable，命名为vec_ss_table_l
    /// 2、vec_ss_table_l的每个SSTable中的scope属性进行融合，并以此获取下一Level与该scope相交的SSTable，命名为vec_ss_table_l_1
    /// 3、获取的vec_ss_table_l_1向上一Level进行类似第2步骤的措施，获取两级之间压缩范围内最恰当的数据
    /// 4、vec_ss_table_l与vec_ss_table_l_1之间的数据并行取出排序归并去重等处理后，分片成多个Vec<CommandData>
    /// 5、释放manifest读锁
    /// 6、并行将每个分片各自生成SSTable
    /// 6、获取manifest写锁
    /// 7、生成的SSTables插入到vec_ss_table_l的第一个SSTable位置，并将vec_ss_table_l和vec_ss_table_l_1的SSTable删除
    /// 8、释放manifest写锁
    ///
    /// 经过压缩测试，Level 1的SSTable总是较多，根据原理推断：
    /// Level0的Key基本是无序的，容易生成大量的SSTable至Level1
    /// 而Level1-7的Key排布有序，故转移至下一层的SSTable数量较小
    /// 因此大量数据压缩的情况下Level 1的SSTable数量会较多
    /// TODO: SSTable锁,避免并行压缩时数据范围重复
    pub(crate) async fn major_compaction(&self, mut level: usize) -> Result<()> {
        if level > 6 {
            return Err(KvsError::LevelOver);
        }
        let config = &self.config;

        while level < 7 {
            if let Some((index, vec_expire_gen, vec_sharding))
                        = self.data_loading_with_level(level).await? {

                let start = Instant::now();
                // 并行创建SSTable
                let ss_table_futures = vec_sharding.into_iter()
                    .map(|(gen, sharding)| {
                        SSTable::create_for_immutable_table(
                            config,
                            gen,
                            &self.io_handler_factory,
                            sharding,
                            level + 1
                        )
                    });
                let vec_new_ss_table: Vec<SSTable> = future::try_join_all(ss_table_futures).await?;

                let mut manifest = self.manifest.write().await;
                manifest.insert_ss_table_with_index_batch(vec_new_ss_table, index).await;
                manifest.retain_with_vec_gen_and_level(&vec_expire_gen, &self.io_handler_factory).await?;

                info!("[LsmStore][Major Compaction][recreate_sst][Level: {}][Time: {:?}]", level, start.elapsed());
                level += 1;
            } else { break }
        }
        Ok(())
    }

    /// 通过Level进行归并数据加载
    async fn data_loading_with_level(&self, level: usize) -> Result<Option<(usize, ExpiredGenVec, MergeShardingVec)>> {
        let manifest = self.manifest.read().await;
        let config = &self.config;
        let next_level = level + 1;
        let major_select_file_size = self.config.major_select_file_size;

        // 如果该Level的SSTables数量尚未越出阈值则提取返回空
        if level > 5 || !manifest.is_threshold_exceeded_major(config.major_threshold_with_sst_size,
                                                level,
                                                config.level_sst_magnification) {
            return Ok(None);
        }

        if let Some(vec_ss_table_l) = Self::get_first_vec_ss_table(&manifest, level, major_select_file_size).await {
            let start = Instant::now();

            let scope_l = Scope::fusion_from_vec_ss_table(&vec_ss_table_l)?;

            let vec_ss_table_ll =
                manifest.get_meet_scope_ss_tables(next_level, &scope_l).await;
            let vec_ss_table_l_1 =
                manifest.get_meet_scope_ss_tables(level, &scope_l).await;

            let index = SSTable::first_index_with_level(&vec_ss_table_ll, &manifest, next_level);

            let vec_ss_table_final = match Scope::fusion_from_vec_ss_table(&vec_ss_table_ll) {
                Ok(scope) => manifest.get_meet_scope_ss_tables(level, &scope).await,
                Err(_) => vec_ss_table_l
            }.into_iter()
                .chain(vec_ss_table_ll)
                .chain(vec_ss_table_l_1)
                .unique_by(|ss_table| ss_table.get_gen())
                .collect_vec();

            // 数据合并并切片
            let vec_merge_sharding =
                Self::data_merge_and_sharding(&vec_ss_table_final, &self.config).await?;

            // 收集需要清除的SSTable
            let vec_expire_gen = SSTable::collect_gen(&vec_ss_table_final)?;
            info!("[LsmStore][Major Compaction][data_loading_with_level][Time: {:?}]", start.elapsed());

            Ok(Some((index, vec_expire_gen, vec_merge_sharding)))
        } else {
            Ok(None)
        }
    }

    /// 以SSTables的数据归并再排序后切片，获取以Command的Key值由小到大的切片排序
    /// 收集所有SSTable的get_all_data的future，并行执行并对数据进行去重以及排序
    /// 真他妈完美
    async fn data_merge_and_sharding(
        vec_ss_table: &[SSTable],
        config: &Config
    ) -> Result<MergeShardingVec> {
        // 需要对SSTable进行排序，可能并发创建的SSTable可能确实名字会重复，但是目前SSTable的判断新鲜度的依据目前为Gen
        // SSTable使用雪花算法进行生成，所以并行创建也不会导致名字重复(极小概率除外)
        let map_futures = vec_ss_table.iter()
            .sorted_unstable_by_key(|ss_table| ss_table.get_gen())
            .map(|ss_table| ss_table.get_all_data());
        let vec_cmd_data = future::try_join_all(map_futures)
            .await?
            .into_iter()
            .flatten()
            .rev()
            .unique_by(CommandData::get_key_clone)
            .sorted_unstable_by_key(CommandData::get_key_clone)
            .collect();
        Ok(data_sharding(vec_cmd_data, config.sst_file_size, config, true).await)
    }

    /// 获取对应Level的开头指定数量的SSTable
    pub(crate) async fn get_first_vec_ss_table(
        manifest: &Manifest,
        level: usize,
        size: usize
    ) -> Option<Vec<SSTable>>{
        let level_vec = manifest.get_level_vec(level).iter()
            .take(size)
            .cloned()
            .collect_vec();
        manifest.get_ss_table_batch(&level_vec.to_vec()).await
    }

    pub(crate) fn from_lsm_kv(lsm_kv: &LsmStore) -> Self {
        let manifest = Arc::clone(lsm_kv.manifest());
        let config = Arc::clone(lsm_kv.config());
        let wal = Arc::clone(lsm_kv.wal());
        let io_handler_factory = Arc::clone(lsm_kv.io_handler_factory());

        Compactor::new(manifest, config, io_handler_factory, wal)
    }

}

// #[test]
// fn test_sharding() -> Result<()> {
//     tokio_test::block_on(async move {
//         let mut vec_data_1 = Vec::new();
//         for _ in 0..101 {
//             vec_data_1.push(CommandData::Set { key: vec![b'1'], value: vec![b'1'] })
//         }
//
//         let vec_sharding_1 =
//             Compactor::data_sharding(vec_data_1, 10, &Config::new()).await;
//         assert_eq!(vec_sharding_1.len(), 21);
//
//         Ok(())
//     })
// }

impl Clone for Compactor {
    fn clone(&self) -> Self {
        Compactor {
            manifest: Arc::clone(&self.manifest),
            config: Arc::clone(&self.config),
            io_handler_factory: Arc::clone(&self.io_handler_factory),
            wal: Arc::clone(&self.wal)
        }
    }
}
