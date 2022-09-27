use std::sync::Arc;
use futures::future;
use itertools::Itertools;
use tokio::sync::RwLock;
use tracing::error;
use crate::{HashStore, KvsError};
use crate::kernel::io_handler::IOHandlerFactory;
use crate::kernel::{CommandData, Result};
use crate::kernel::lsm::lsm_kv::{CommandCodec, Config, LsmStore, wal_put};
use crate::kernel::lsm::Manifest;
use crate::kernel::lsm::ss_table::{Score, SsTable};

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

        let io_handler = self.io_handler_factory.create(gen)?;

        // 将这些索引的key序列化后预先存入wal中作防灾准备
        // 当持久化异常时将对应gen的key反序列化出来并从wal找到对应值
        wal_put(&self.wal, CommandCodec::encode_gen(gen)?, CommandCodec::encode_keys(&vec_keys)?);
        // 从内存表中将数据持久化为ss_table
        let ss_table = SsTable::create_for_immutable_table(&self.config
                                                           , io_handler
                                                           , &vec_values
                                                           , LEVEL_0).await?;
        manifest.insert_ss_table_with_index(ss_table, 0);

        if manifest.is_threshold_exceeded_major(self.config.major_threshold_with_sst_size) {
            drop(manifest);
            if let Err(err) = self.major_compaction(0).await {
                error!("[LsmStore][major_compaction][error happen]: {:?}", err);
            }
        }
        Ok(())
    }

    /// Major压缩，负责将不同Level之间的数据向下层压缩转移
    /// 目前Major压缩的大体步骤是
    /// 1、获取manifest读锁，读取当前Level的指定数量SSTable，命名为vec_ss_table_l
    /// 2、vec_ss_table_l的每个SSTable中的Score属性进行融合，并以此获取下一Level与该Score相交的SSTable，命名为vec_ss_table_l_1
    /// 3、获取的vec_ss_table_l_1向上一Level进行类似第2步骤的措施，获取两级之间压缩范围内最恰当的数据
    /// 4、vec_ss_table_l与vec_ss_table_l_1之间的数据并行取出排序归并去重等处理后，分片成多个Vec<CommandData>
    /// 5、释放manifest读锁
    /// 6、并行将每个分片各自生成SSTable
    /// 6、获取manifest写锁
    /// 7、生成的SSTables插入到vec_ss_table_l的第一个SSTable位置，并将vec_ss_table_l和vec_ss_table_l_1的SSTable删除
    /// 8、释放manifest写锁
    /// TODO: 多级递增循环压缩
    /// TODO: SSTable锁,避免并行压缩时数据范围重复
    pub(crate) async fn major_compaction(&self, level: usize) -> Result<()> {
        if level > 6 {
            return Err(KvsError::LevelOver);
        }

        let file_size = self.config.sst_file_size;
        if let Some((index, vec_expire_gen, vec_sharding))
                                            = self.data_loading_with_level(level, file_size).await? {

            // 并行创建SSTable
            let ss_table_futures = vec_sharding.iter()
                .map(|(gen, sharding)| {
                    let io_handler = self.io_handler_factory.create(gen.clone()).unwrap();
                    SsTable::create_for_immutable_table(&self.config,
                                                        io_handler,
                                                        sharding,
                                                        level + 1)
                });
            let vec_new_ss_table: Vec<SsTable> = future::try_join_all(ss_table_futures).await?;

            let mut manifest = self.manifest.write().await;
            manifest.insert_ss_table_with_index_batch(vec_new_ss_table, index);
            manifest.retain_with_vec_gen_and_level(&vec_expire_gen)?;
        }
        Ok(())
    }

    /// 通过Level进行归并数据加载
    async fn data_loading_with_level(&self, level: usize, file_size: usize) -> Result<Option<(usize, ExpiredGenVec, MergeShardingVec)>> {
        let manifest = self.manifest.read().await;
        let next_level = level + 1;
        let major_select_file_size = self.config.major_select_file_size;

        if let Some(vec_ss_table) = Self::get_first_vec_ss_table(&manifest, level, major_select_file_size) {
            let vec_ss_table_l_1 =
                manifest.get_meet_score_ss_tables(next_level, &Score::fusion_from_vec_ss_table(&vec_ss_table)?);

            let index = SsTable::first_index_with_level(&vec_ss_table_l_1, &manifest, next_level);

            let vec_ss_table_final = match Score::fusion_from_vec_ss_table(&vec_ss_table_l_1) {
                Ok(score) => manifest.get_meet_score_ss_tables(level, &score),
                Err(_) => vec_ss_table
            }.into_iter()
                .chain(vec_ss_table_l_1)
                .collect_vec();

            // 数据合并并切片
            let vec_merge_sharding =
                Self::data_merge_and_sharding(&vec_ss_table_final, file_size, &self.config).await?;

            // 收集需要清除的SSTable
            let vec_expire_gen = SsTable::collect_gen(vec_ss_table_final)?;

            Ok(Some((index, vec_expire_gen, vec_merge_sharding)))
        } else {
            Ok(None)
        }
    }

    /// 以SSTables的数据归并再排序后切片，获取以Command的Key值由小到大的切片排序
    /// 收集所有SSTable的get_all_data的future，并行执行并对数据进行去重以及排序
    /// 真他妈完美
    async fn data_merge_and_sharding(vec_ss_table: &Vec<&SsTable>, file_size: usize, config: &Config) -> Result<MergeShardingVec>{
        // 需要对SSTable进行排序，可能并发创建的SSTable可能确实名字会重复，但是目前SSTable的判断新鲜度的依据目前为Gen
        // SSTable使用雪花算法进行生成，所以并行创建也不会导致名字重复(极小概率除外)
        let map_futures = vec_ss_table.into_iter()
            .sorted_unstable_by_key(|ss_table| ss_table.get_gen())
            .map(|ss_table| ss_table.get_all_data());
        let vec_cmd_data = future::try_join_all(map_futures)
            .await?
            .into_iter()
            .flatten()
            .unique_by(|cmd| cmd.get_key_clone())
            .sorted_unstable()
            .collect();
        Ok(Self::data_sharding(vec_cmd_data, file_size, config).await)
    }

    /// 获取对应Level的开头指定数量的SSTable
    pub(crate) fn get_first_vec_ss_table(manifest: &Manifest, level: usize, size: usize) -> Option<Vec<&SsTable>> {
        let level_vec = manifest.get_level_vec(level).iter()
            .take(size)
            .cloned()
            .collect_vec();
        manifest.get_ss_table_batch(&level_vec.to_vec())
    }

    /// CommandData数据分片，尽可能将数据按给定的分片大小：file_size，填满一片（可能会溢出一些）
    /// 保持原有数据的顺序进行分片，所有第一片分片中最后的值肯定会比其他分片开始的值Key排序较前（如果vec_data是以Key从小到大排序的话）
    async fn data_sharding(mut vec_data: Vec<CommandData>, file_size: usize, config: &Config) -> MergeShardingVec {
        // 向上取整计算STable数量
        let part_size = (vec_data.iter()
            .map(|cmd| cmd.get_data_len())
            .sum::<usize>() + file_size - 1) / file_size;
        let mut vec_sharding = vec![(0, Vec::new()); part_size];
        let slice = vec_sharding.as_mut_slice();
        for i in 0 .. part_size {
            slice[i].0 = config.create_gen();
            let mut data_len = 0;
            while !vec_data.is_empty() {
                if let Some(cmd_data) = vec_data.pop() {
                    data_len += cmd_data.get_data_len();
                    slice[i].1.push(cmd_data);
                    if data_len >= file_size {
                        break
                    }
                } else { break }
            }
        }
        // 过滤掉没有数据的切片
        vec_sharding.retain(|(_, vec)| vec.len() > 0);
        vec_sharding
    }

    pub(crate) fn from_lsm_kv(lsm_kv: &LsmStore) -> Self {
        let manifest = Arc::clone(&lsm_kv.manifest());
        let config = Arc::clone(&lsm_kv.config());
        let wal = Arc::clone(&lsm_kv.wal());
        let io_handler_factory = Arc::clone(&lsm_kv.io_handler_factory());

        Compactor::new(manifest, config, io_handler_factory, wal)
    }

}

#[test]
fn test_sharding() -> Result<()> {
    tokio_test::block_on(async move {
        let mut vec_data_1 = Vec::new();
        for _ in 0..101 {
            vec_data_1.push(CommandData::Set { key: vec![b'1'], value: vec![b'1'] })
        }

        let vec_sharding_1 =
            Compactor::data_sharding(vec_data_1, 10, &Config::new()).await;
        assert_eq!(vec_sharding_1.len(), 21);

        Ok(())
    })
}

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
