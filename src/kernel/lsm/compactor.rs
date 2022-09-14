use std::sync::Arc;
use chrono::Local;
use itertools::Itertools;
use tokio::sync::RwLock;
use crate::HashStore;
use crate::kernel::io_handler::IOHandlerFactory;
use crate::kernel::Result;
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
        let time_stamp = Local::now().timestamp_millis() as u64;
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
        manifest.insert_ss_table(time_stamp, ss_table);
        Ok(())
    }

    pub(crate) async fn major_compaction(&self, level: usize) -> Result<()> {
        let manifest = self.manifest.read().await;

        if let Some(slice) = Self::get_first_3_slice(&manifest, level) {
            let vec_score = slice.map(|gen| manifest.get_ss_table(&gen).unwrap().get_score())
                .to_vec();
            let first_score = Score::fusion(vec_score)?;
            let mut vec_ss_table_l_1 = manifest.get_level_vec(level + 1).iter()
                .map(|gen| manifest.get_ss_table(gen).unwrap())
                .filter(|ss_table| ss_table.get_score().meet(&first_score))
                .collect_vec();
            let final_score = Score::fusion(vec_ss_table_l_1.iter()
                .map(|ss_table| ss_table.get_score())
                .collect_vec())?;
            let vec_ss_table_l = manifest.get_level_vec(level).iter()
                .map(|gen| manifest.get_ss_table(gen).unwrap())
                .filter(|ss_table| ss_table.get_score().meet(&final_score))
                .collect_vec();

            // let mut vec_cmd_data = Vec::new();
            // vec_ss_table_l.iter().for_each(|ss_table| vec_cmd_data.extend(ss_table.get_all_data().await?));
            // vec_ss_table_l_1.iter().for_each(|ss_table| vec_cmd_data.extend(ss_table.get_all_data().await?));
            // vec_cmd_data.sort_by(|cmd_a, cmd_b| cmd_a.get_key().cmp(cmd_b.get_key()));
            //
            // match vec_ss_table_l_1.pop() {
            //     Some(ss_table) => {
            //
            //     }
            //     None => {
            //
            //     }
            // }

            // match {
            //     // 注意! vec_ss_table_gen是由旧到新的
            //     // 这样归并出来的数据才能保证数据是有效的
            //     Some(vec_shot_snap_gen) => {
            //
            //         let mut vec_cmd_data = Vec::new();
            //         // 将所有Level0的SSTable读取各自所有的key做归并
            //         for gen in vec_shot_snap_gen.iter() {
            //             if let Some(ss_table) = manifest.get_ss_table(gen) {
            //                 vec_cmd_data.extend(ss_table.get_all_data().await?);
            //             } else {
            //                 return Err(KvsError::SSTableLostError);
            //             }
            //         }
            //
            //         // 构建Level1的SSTable
            //         let level_1_ss_table = Self::create_for_immutable_table(&config, io_handler, &vec_cmd_data, 1).await?;
            //
            //         Ok(Some((level_1_ss_table, vec_shot_snap_gen)))
            //     }
            //     None => Ok(None)
            // }
        };
        Ok(())
    }

    fn get_first_3_slice(manifest: &Manifest, level: usize) -> Option<[u64;3]> {
        let level_slice = manifest.get_level_vec(level).as_slice();
        if level_slice.len() > 3 {
            Some([level_slice[1], level_slice[2], level_slice[3]])
        } else { None }
    }

    pub(crate) fn from_lsm_kv(lsm_kv: &LsmStore) -> Self {
        let manifest = Arc::clone(&lsm_kv.manifest());
        let config = Arc::clone(&lsm_kv.config());
        let wal = Arc::clone(&lsm_kv.wal());
        let io_handler_factory = Arc::clone(&lsm_kv.io_handler_factory());

        Compactor::new(manifest, config, io_handler_factory, wal)
    }
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
