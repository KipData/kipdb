use std::cmp::Ordering;
use std::collections::btree_map::BTreeMap;
use std::sync::{Arc};
use itertools::Itertools;
use tokio::sync::RwLock;
use tracing::{info, warn};
use crate::kernel::{CommandData, CommandPackage};
use crate::kernel::io_handler::IOHandler;
use crate::kernel::lsm::{Manifest, MetaInfo, Position};
use crate::kernel::lsm::lsm_kv::Config;
use crate::kernel::Result;
use crate::KvsError;

type ExpiredGenVec = Vec<u64>;

pub(crate) const LEVEL_0: u64 = 0;

pub(crate) const LEVEL_1: u64 = 1;

/// SSTable
pub(crate) struct SsTable {
    // 表索引信息
    meta_info: MetaInfo,
    // 字段稀疏索引
    sparse_index: BTreeMap<Vec<u8>, Position>,
    // 文件IO操作器
    io_handler: IOHandler,
    // 文件路径
    gen: u64
}

impl SsTable {

    /// 通过已经存在的文件构建SSTable
    ///
    /// 使用原有的路径与分区大小恢复出一个有内容的SSTable
    pub(crate) async fn restore_from_file(io_handler: IOHandler) -> Result<Self>{
        let gen = io_handler.get_gen();

        let meta_info = MetaInfo::read_to_file(&io_handler).await?;
        info!("[SsTable: {}][restore_from_file][TableMetaInfo]: {:?}", gen, meta_info);

        let index_pos = meta_info.data_len;
        let index_len = meta_info.index_len as usize;

        if let Some(data) = CommandPackage::form_pos_unpack(&io_handler, index_pos, index_len).await? {
            let sparse_index = rmp_serde::from_slice(&data.get_key())?;
            Ok(SsTable {
                meta_info,
                sparse_index,
                gen,
                io_handler
            })
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    /// 写入CommandData数据段
    async fn write_data_part(vec_cmd_data: &mut Vec<&CommandData>, io_handler: &IOHandler, sparse_index: &mut BTreeMap<Vec<u8>, Position>) -> Result<()> {

        let mut start_pos = 0;
        let mut part_len = 0;
        for (index, cmd_data) in vec_cmd_data.iter().enumerate() {
            let (start, len) = CommandPackage::write_back_real_pos(io_handler, cmd_data).await?;
            if index == 0 {
                start_pos = start;
            }
            part_len += len;
        }

        info!("[write_data_part][data_zone]: {} to {}", start_pos, part_len);
        // 获取该段首位数据
        if let Some(cmd) = vec_cmd_data.first() {
            info!("[write_data_part][sparse_index]: index of the part: {:?}", cmd.get_key());
            sparse_index.insert(cmd.get_key_clone(), Position { start: start_pos, len: part_len });
        }

        vec_cmd_data.clear();
        Ok(())
    }

    /// 仅为Level 0 SSTable
    /// 将升级SSTable为1且清除原有Level0的Table
    pub(crate) async fn level_up_for_level_0(level_1_ss_table: SsTable, manifest: Arc<RwLock<Manifest>>, vec_level_0_gen: &ExpiredGenVec) -> Result<()> {
        let mut manifest = manifest.write().await;

        manifest.insert_ss_table(level_1_ss_table.gen, level_1_ss_table);
        manifest.retain_with_vec_gen_and_level(vec_level_0_gen)?;
        Ok(())
    }

    pub(crate) fn level(&mut self, level: u64) {
        self.meta_info.level = level;
    }

    pub(crate) fn get_level(&self) -> u64 {
        self.meta_info.level
    }

    pub(crate) fn get_version(&self) -> u64 {
        self.meta_info.version
    }

    pub(crate) fn get_gen(&self) -> u64 {
        self.gen
    }

    /// Level0的SSTable归并读取生成新的Level1SSTable
    pub(crate) async fn level_0_up_ss_table(config: &Config, manifest: &Manifest, io_handler: IOHandler) -> Result<Option<(SsTable, ExpiredGenVec)>> {
        match manifest.get_level_vec(LEVEL_0 as usize).cloned() {
            // 注意! vec_ss_table_gen是由旧到新的
            // 这样归并出来的数据才能保证数据是有效的
            Some(vec_shot_snap_gen) => {

                let mut vec_cmd_data = Vec::new();
                // 将所有Level0的SSTable读取各自所有的key做归并
                for gen in vec_shot_snap_gen.iter() {
                    if let Some(ss_table) = manifest.get_ss_table(gen) {
                        vec_cmd_data.extend(ss_table.get_all_data().await?);
                    } else {
                        return Err(KvsError::SSTableLostError);
                    }
                }

                // 构建Level1的SSTable
                let level_1_ss_table = Self::create_for_immutable_table(&config, io_handler, &vec_cmd_data, LEVEL_1).await?;

                Ok(Some((level_1_ss_table, vec_shot_snap_gen)))
            }
            None => Ok(None)
        }
    }

    /// 从该sstable中获取指定key对应的CommandData
    pub(crate) async fn query(&self, key: &Vec<u8>) -> Result<Option<CommandData>> {
        let empty_position = &Position::new(0, 0);
        // 第一位是第一个大于的，第二位是最后一个小于的
        let mut position_arr = (empty_position, empty_position);
        for (key_item, value_item) in self.sparse_index.iter() {
            if let Ordering::Greater = key.cmp(key_item) {
                // 找到确定位置后可以提前推出，减少数据读取占用的IO
                position_arr.0 = value_item;
                break;
            } else {
                position_arr.1 = value_item;
            }
        }

        let mut _len = 0;
        let first_big = position_arr.0;
        let last_small = position_arr.1;

        // 找出数据可能存在的区间
        let start_pos = first_big.start;
        if first_big != empty_position {
            _len = first_big.len;
        } else if last_small != empty_position {
            _len = last_small.len + (last_small.start - start_pos) as usize;
        } else {
            return Ok(None);
        }
        info!("[SsTable: {}][query][data_zone]: {} to {}", self.gen, start_pos, _len);
        // 获取该区间段的数据
        let zone = self.io_handler.read_with_pos(start_pos, _len).await?;

        // 返回该区间段对应的数据结果
        Ok(if let Some(cmd_p) =
                CommandPackage::find_key_with_zone(zone.as_slice(), &key).await? {
            Some(cmd_p.cmd)
        } else {
            None
        })
    }

    /// 获取SsTable内所有的正常数据
    pub(crate) async fn get_all_data(&self) -> Result<Vec<CommandData>> {
        let info = &self.meta_info;
        let data_len = info.data_len;

        let all_data_u8 = self.io_handler.read_with_pos(0, data_len as usize).await?;
        let vec_cmd_data =
                    CommandPackage::form_zone_to_vec(all_data_u8.as_slice()).await?
            .into_iter()
            .map(|cmd_package| cmd_package.cmd)
            .collect_vec();
        Ok(vec_cmd_data)
    }

    /// 通过内存表构建持久化并构建SSTable
    ///
    /// 使用目标路径与文件大小，分块大小构建一个有内容的SSTable
    pub(crate) async fn create_for_immutable_table(config: &Config, io_handler: IOHandler, vec_mem_data: &Vec<CommandData>, level: u64) -> Result<Self> {
        // 获取地址
        let part_size = config.part_size;
        let gen = io_handler.get_gen();
        let mut vec_cmd = Vec::new();
        let mut sparse_index: BTreeMap<Vec<u8>, Position> = BTreeMap::new();

        // 将数据按part_size一组分段存入
        for cmd_data in vec_mem_data {
            vec_cmd.push(cmd_data);
            if vec_cmd.len() >= part_size as usize {
                Self::write_data_part(&mut vec_cmd, &io_handler, &mut sparse_index).await?;
            }
        }
        // 将剩余的指令当作一组持久化
        if !vec_cmd.is_empty() {
            Self::write_data_part(&mut vec_cmd, &io_handler, &mut sparse_index).await?;
        }

        // 开始对稀疏索引进行伪装并断点处理
        // 获取指令数据段的数据长度
        // 不使用真实pos作为开始，而是与稀疏索引的伪装CommandData做区别
        let cmd_sparse_index = CommandData::Get { key: rmp_serde::to_vec(&sparse_index)?};
        // 将稀疏索引伪装成CommandData，使最后的MetaInfo位置能够被顺利找到
        let (data_part_len, sparse_index_len) = CommandPackage::write(&io_handler, &cmd_sparse_index).await?;


        // 将以上持久化信息封装为MetaInfo
        let meta_info = MetaInfo{
            level,
            version: 0,
            data_len: data_part_len as u64,
            index_len: sparse_index_len as u64,
            part_size
        };
        meta_info.write_to_file(&io_handler).await?;

        io_handler.flush().await?;

        info!("[SsTable: {}][create_form_index][TableMetaInfo]: {:?}", gen, meta_info);
        Ok(SsTable {
            meta_info,
            sparse_index,
            io_handler,
            gen
        })
    }
}