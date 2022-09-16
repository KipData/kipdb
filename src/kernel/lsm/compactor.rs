use std::collections::HashSet;
use std::sync::Arc;
use chrono::Local;
use itertools::Itertools;
use tokio::sync::RwLock;
use crate::{HashStore, KvsError};
use crate::kernel::io_handler::IOHandlerFactory;
use crate::kernel::{CommandData, Result};
use crate::kernel::lsm::lsm_kv::{CommandCodec, Config, LsmStore, wal_put};
use crate::kernel::lsm::Manifest;
use crate::kernel::lsm::ss_table::{LEVEL_0, Score, SsTable};

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
    /// 此处manifest参数需要传入是因为Rust的锁不可重入所以需要从外部将锁对象传入
    pub(crate) async fn minor_compaction(&self) -> Result<()> {
        let mut manifest = self.manifest.write().await;

        // 获取当前时间戳当gen
        let time_stamp = Local::now().timestamp_nanos() as u64;
        let io_handler = self.io_handler_factory.create(time_stamp)?;
        // 切换mem_table并准备持久化
        let (vec_keys, vec_values) = manifest.table_swap();
        let vec_ts_u8 = CommandCodec::encode_gen(time_stamp)?;

        // 将这些索引的key序列化后预先存入wal中作防灾准备
        // 当持久化异常时将对应gen的key反序列化出来并从wal找到对应值
        wal_put(&self.wal, vec_ts_u8, CommandCodec::encode_keys(&vec_keys)?);
        // 从内存表中将数据持久化为ss_table
        let ss_table = SsTable::create_for_immutable_table(&self.config
                                                           , io_handler
                                                           , &vec_values
                                                           , LEVEL_0).await?;
        manifest.insert_ss_table_with_index(ss_table, 0);

        if manifest.is_threshold_exceeded_major(self.config.sst_threshold) {
            drop(manifest);
            self.major_compaction(1);
        }
        Ok(())
    }

    pub(crate) async fn major_compaction(&self, level: usize) -> Result<()> {
        if level > 6 {
            return Err(KvsError::LevelOver);
        }

        let file_size = self.config.sst_size;

        let manifest = self.manifest.read().await;

        if let Some(vec_ss_table) = Self::get_first_3_vec_ss_table(&manifest, level) {
            let next_level = level + 1;
            let mut vec_ss_table_l_1 =
                manifest.get_meet_score_ss_tables(next_level, &Score::fusion_from_vec_ss_table(&vec_ss_table)?);

            let index = match vec_ss_table_l_1.first() {
                None => 0,
                Some(first_ss_table) => {
                    manifest.get_index(next_level, first_ss_table.get_gen())
                        .unwrap_or(0)
                }
            };

            let vec_ss_table_l = match Score::fusion_from_vec_ss_table(&vec_ss_table_l_1) {
                Ok(score) => manifest.get_meet_score_ss_tables(level, &score),
                Err(_) => vec_ss_table
            };

            let mut set_cmd_data = HashSet::new();
            vec_ss_table_l_1.extend(vec_ss_table_l);
            for ss_table in &vec_ss_table_l_1 {
                set_cmd_data.extend(ss_table.get_all_data().await?);
            }
            // 提前收集需要清除的SSTable
            let vec_expire_gen = vec_ss_table_l_1.iter()
                .map(|ss_table| ss_table.get_gen())
                .collect_vec();
            let vec_cmd_data = set_cmd_data.into_iter()
                .sorted_unstable()
                .collect_vec();

            let vec_sharding = Self::data_sharding(vec_cmd_data, file_size);

            drop(manifest);

            let mut manifest = self.manifest.write().await;

            for sharding in vec_sharding {
                let io_handler = self.io_handler_factory.create(Local::now().timestamp_nanos() as u64)?;
                let new_ss_table = SsTable::create_for_immutable_table(&self.config,
                                                                       io_handler,
                                                                       &sharding.iter().collect_vec(),
                                                                       next_level as u64).await?;
                manifest.insert_ss_table_with_index(new_ss_table, index);
            }
            manifest.retain_with_vec_gen_and_level(&vec_expire_gen)?;
        };
        Ok(())
    }

    pub(crate) fn get_first_3_vec_ss_table(manifest: &Manifest, level: usize) -> Option<Vec<&SsTable>> {
        let level_vec = manifest.get_level_vec(level).iter()
            .take(3)
            .cloned()
            .collect_vec();
        manifest.get_ss_table_batch(&level_vec.to_vec())
    }

    /// CommandData数据分片，尽可能将数据按给定的分片大小：file_size，填满一片（可能会溢出一些）
    /// 保持原有数据的顺序进行分片，所有第一片分片中最后的值肯定会比其他分片开始的值Key排序较前（如果vec_data是以Key从小到大排序的话）
    fn data_sharding(mut vec_data: Vec<CommandData>, file_size: usize) -> Vec<Vec<CommandData>> {
        // 向上取整计算STable数量
        let part_size = (vec_data.iter()
            .map(|cmd| cmd.get_data_len())
            .sum::<usize>() + file_size - 1) / file_size;
        let mut vec_sharding = vec![Vec::new(); part_size];
        let slice = vec_sharding.as_mut_slice();
        for i in 0 .. part_size {
            let mut data_len = 0;
            while !vec_data.is_empty() {
                if let Some(cmd_data) = vec_data.pop() {
                    data_len += cmd_data.get_data_len();
                    slice[i].push(cmd_data);
                    if data_len >= file_size {
                        break
                    }
                } else { break }
            }
        }
        // 过滤掉没有数据的切片
        vec_sharding.retain(|vec| vec.len() > 0);
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
    let mut vec_data_1 = Vec::new();
    for _ in 0..101 {
        vec_data_1.push(CommandData::Set { key: vec![b'1'], value: vec![b'1'] })
    }

    let vec_sharding_1 = Compactor::data_sharding(vec_data_1, 10);
    assert_eq!(vec_sharding_1.len(), 21);

    Ok(())
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
