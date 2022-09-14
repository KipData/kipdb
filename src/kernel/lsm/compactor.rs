use std::sync::Arc;
use chrono::Local;
use itertools::Itertools;
use tokio::sync::RwLock;
use crate::HashStore;
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

    /// CommandData数据分片，尽可能将数据按给定的分片数量：vec_size均匀切片
    /// 保持原有数据的顺序进行分片，所有第一片分片中最后的值肯定会比其他分片开始的值Key排序较前（如果vec_data是以Key从小到大排序的话）
    /// 注意，最后一片分片大多数情况下会比其他分片更大那么一点点，最极端的情况下不会小于最小的分片超过所有数据中最大的值
    fn data_sharding(mut vec_data: Vec<CommandData>, vec_size: usize) -> Vec<Vec<CommandData>> {
        let part_size = vec_data.iter()
            .map(|cmd| cmd.get_data_len())
            .sum::<usize>() / vec_size;
        let mut vec_sharding = vec![Vec::new(); vec_size];
        let slice = vec_sharding.as_mut_slice();
        for i in 0 .. vec_size {
            let mut data_len :usize = slice[i].iter()
                .map(|cmd: &CommandData| cmd.get_data_len())
                .sum::<usize>();
            while !vec_data.is_empty() {
                if let Some(cmd_data) = vec_data.pop() {
                    let cmd_len = cmd_data.get_data_len();
                    if data_len + cmd_len <= part_size || i >= vec_size - 1 {
                        data_len += cmd_len;
                        slice[i].push(cmd_data);
                    } else {
                        slice[i + 1].push(cmd_data);
                        break
                    }
                } else { break }
            }
        }
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
    use rand::Rng;

    let mut vec_data_1 = Vec::new();
    for _ in 0..100 {
        vec_data_1.push(CommandData::Set { key: vec![b'1'], value: vec![b'1'] })
    }

    let vec_sharding_1 = Compactor::data_sharding(vec_data_1, 4);
    for sharding in vec_sharding_1 {
        assert_eq!(sharding.len(), 25);
    }

    let mut rng = rand::thread_rng();
    let mut vec_data_2 = Vec::new();
    let mut data_len = 0;
    while data_len % 154 != 0 || data_len == 0 {
        let cmd = CommandData::Set { key: rmp_serde::to_vec(&rng.gen::<f64>())?, value: rmp_serde::to_vec(&rng.gen::<f64>())? };
        data_len += cmd.get_data_len();
        vec_data_2.push(cmd);
    }
    let max_len = vec_data_2.iter().map(|cmd| rmp_serde::to_vec(cmd).unwrap().len()).max().unwrap();
    
    let vec_sharding_2 = Compactor::data_sharding(vec_data_2, 4);
    let vec_sharding_2_slice = vec_sharding_2.as_slice();
    let i_0 = rmp_serde::to_vec(&vec_sharding_2_slice[0])?.len();
    let i_1 = rmp_serde::to_vec(&vec_sharding_2_slice[1])?.len();
    let i_2 = rmp_serde::to_vec(&vec_sharding_2_slice[2])?.len();
    let i_3 = rmp_serde::to_vec(&vec_sharding_2_slice[3])?.len();

    assert!(i_3 - i_0 <= max_len);
    assert!(i_3 - i_1 <= max_len);
    assert!(i_3 - i_2 <= max_len);

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
