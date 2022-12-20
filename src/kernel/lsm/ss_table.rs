use std::cmp::Ordering;
use growable_bloom_filter::GrowableBloom;
use itertools::Itertools;
use lru::LruCache;
use tokio::sync::Mutex;
use tracing::{info};
use serde::{Deserialize, Serialize};
use skiplist::SkipMap;
use crate::kernel::{CommandData, CommandPackage};
use crate::kernel::io_handler::IOHandler;
use crate::kernel::lsm::{data_sharding, ExtraInfo, Manifest, MetaInfo, Position};
use crate::kernel::lsm::lsm_kv::Config;
use crate::kernel::Result;
use crate::KvsError;

const ALIGNMENT_4K: usize = 4096;

/// SSTable
pub(crate) struct SsTable {
    // 表索引信息
    meta_info: MetaInfo,
    // 字段稀疏索引
    sparse_index: SkipMap<Vec<u8>, Position>,
    // 文件IO操作器
    io_handler: IOHandler,
    // 该SSTable的唯一编号(时间递增)
    gen: i64,
    // 数据范围索引
    scope: Scope,
    // 过滤器
    filter: GrowableBloom,
    // 硬盘占有大小
    size_of_disk: u64,
    // 数据数量
    size_of_data: usize,
}

/// 数据范围索引
/// 用于缓存SSTable中所有数据的第一个和最后一个数据的Key
/// 标明数据的范围以做到快速区域定位
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct Scope {
    start: Vec<u8>,
    end: Vec<u8>
}

impl PartialEq<Self> for SsTable {
    fn eq(&self, other: &Self) -> bool {
        self.meta_info.eq(&other.meta_info)
    }
}

impl PartialOrd<Self> for SsTable {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Option::from(self.get_gen().cmp(&other.get_gen()))
    }
}

impl Scope {

    /// 由CommandData组成的Key构成scope
    pub(crate) fn from_cmd_data(first: &CommandData, last: &CommandData) -> Self {
        Scope {
            start: first.get_key_clone(),
            end: last.get_key_clone()
        }
    }

    /// 将多个scope重组融合成一个scope
    pub(crate) fn fusion(vec_scope :Vec<&Scope>) -> Result<Self> {
        if vec_scope.len() > 0 {
            let start = vec_scope.iter()
                .map(|scope| &scope.start)
                .min().unwrap()
                .clone();
            let end = vec_scope.iter()
                .map(|scope| &scope.end)
                .max().unwrap()
                .clone();

            Ok(Scope { start, end })
        } else {
            Err(KvsError::DataEmpty)
        }
    }

    /// 判断scope之间是否相交
    pub(crate) fn meet(&self, target: &Scope) -> bool {
        (self.start.le(&target.start) && self.end.ge(&target.start)) ||
            (self.start.le(&target.end) && self.end.ge(&target.end))
    }

    /// 由一组Command组成一个scope
    pub(crate) fn from_vec_cmd_data(vec_mem_data: &Vec<CommandData>) -> Result<Self> {
        match vec_mem_data.as_slice() {
            [first, .., last] => {
                Ok(Self::from_cmd_data(first, last))
            },
            [one] => {
                Ok(Self::from_cmd_data(one, one))
            },
            _ => {
                Err(KvsError::DataEmpty)
            },
        }
    }

    /// 由一组SSTable融合成一个scope
    pub(crate) fn fusion_from_vec_ss_table(vec_ss_table :&Vec<&SsTable>) -> Result<Self> {
        let vec_scope = vec_ss_table.iter()
            .map(|ss_table| ss_table.get_scope())
            .collect_vec();
        Self::fusion(vec_scope)
    }
}

impl SsTable {

    /// 通过已经存在的文件构建SSTable
    ///
    /// 使用原有的路径与分区大小恢复出一个有内容的SSTable
    pub(crate) async fn restore_from_file(io_handler: IOHandler) -> Result<Self>{
        let gen = io_handler.get_gen();

        let meta_info = MetaInfo::read_to_file(&io_handler).await?;
        let size_of_disk = io_handler.file_size().await?;
        info!("[SsTable: {}][restore_from_file][TableMetaInfo]: {:?}, Size of Disk: {}", gen, meta_info, &size_of_disk);

        let index_pos = meta_info.data_len;
        let index_len = meta_info.index_len as usize;

        let buffer = io_handler.read_with_pos(0, index_len + index_pos as usize).await?;
        let crc_code_verification = crc32fast::hash(buffer.as_slice()) as u64;

        if let Some(extra_info_cmd) = CommandPackage::from_pos_unpack(&io_handler, index_pos, index_len).await? {
            match extra_info_cmd {
                CommandData::Get { key: extra_info_bytes } => {
                    match rmp_serde::from_slice::<ExtraInfo>(&extra_info_bytes)? {
                        ExtraInfo { vec_index, scope, filter , size_of_data } => {
                            if crc_code_verification.eq(&meta_info.crc_code) {
                                Ok(SsTable {
                                    meta_info,
                                    sparse_index: SkipMap::from_iter(vec_index),
                                    gen,
                                    io_handler,
                                    scope,
                                    filter,
                                    size_of_disk,
                                    size_of_data,
                                })
                            } else {
                                Err(KvsError::CrcMisMatch)
                            }
                        }
                    }
                }
                _ => Err(KvsError::NotMatchCmd)
            }
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    /// 写入CommandData数据段
    async fn write_data_batch(vec_cmd_data: Vec<Vec<CommandData>>, io_handler: &IOHandler) -> Result<Vec<(Vec<u8>, Position)>> {
        let (start_pos, batch_len, vec_sharding_len) =
            CommandPackage::write_batch_first_pos_with_sharding(io_handler, &vec_cmd_data).await?;
        info!("[SSTable][write_data_batch][data_zone]: start_pos: {}, batch_len: {}, vec_sharding_len: {:?}", start_pos, batch_len, vec_sharding_len);

        let keys = vec_cmd_data.into_iter()
            .filter_map(|sharding| match sharding.as_slice() {
                [first, ..] => Ok(first.get_key_clone()),
                [] => Err(KvsError::DataEmpty)
            }.ok())
            .collect_vec();

        let mut start_len = 0;

        let vec_position = vec_sharding_len.into_iter()
            .map(|sharding_len| {
                let position = Position { start: start_len, len: sharding_len };
                start_len += sharding_len as u64;
                position
            }).collect_vec();
        Ok(keys.into_iter()
            .zip(vec_position)
            .collect())
    }

    pub(crate) fn level(&mut self, level: u64) {
        self.meta_info.level = level;
    }

    pub(crate) fn get_level(&self) -> usize {
        self.meta_info.level as usize
    }

    pub(crate) fn get_version(&self) -> u64 {
        self.meta_info.version
    }

    pub(crate) fn get_gen(&self) -> i64 {
        self.gen
    }

    pub(crate) fn get_scope(&self) -> &Scope {
        &self.scope
    }

    pub(crate) fn get_size_of_disk(&self) -> u64 {
        self.size_of_disk
    }

    pub(crate) fn len(&self) -> usize {
        self.size_of_data
    }

    /// 从该sstable中获取指定key对应数据可能存在的CommandData段
    pub(crate) async fn query_with_key(&self, key: &Vec<u8>, position_cache: &Mutex<LruCache<(i64, Position), Vec<CommandData>>>) -> Result<Option<CommandData>> {
        if self.filter.contains(key) {
            let mut cache = position_cache.lock().await;

            if let Some(position) = Position::from_sparse_index_with_key(&self.sparse_index, key) {
                info!("[SsTable: {}][query_with_key][data_zone]: {:?}", self.gen, position);
                let key_position = (self.gen, position.clone());
                // !!!Async closure cannot be used in get_or_insert
                if !cache.contains(&key_position) {
                    let bytes = self.io_handler.read_with_pos(position.start, position.len).await?;
                    let _ignore = cache.put(key_position.clone(), CommandPackage::from_bytes_to_unpack_vec(&bytes)?);
                }

                return Ok(cache.get(&key_position)
                    .expect("When an error occurs here, it means that the empty data is not filled")
                    .into_iter()
                    .find(|cmd_data| cmd_data.get_key() == key)
                    .map(CommandData::clone));
            }
        }
        Ok(None)
    }

    /// 获取SsTable内所有的正常数据
    pub(crate) async fn get_all_data(&self) -> Result<Vec<CommandData>> {
        let info = &self.meta_info;
        let data_len = info.data_len;

        let all_data_u8 = self.io_handler.read_with_pos(0, data_len as usize).await?;
        Ok(CommandPackage::from_bytes_to_unpack_vec(all_data_u8.as_slice())?)
    }

    /// 通过一组SSTable收集对应的Gen
    pub(crate) fn collect_gen(vec_ss_table: Vec<&SsTable>) -> Result<Vec<i64>> {
        Ok(vec_ss_table.into_iter()
            .map(SsTable::get_gen)
            .collect())
    }

    /// 获取一组SSTable中第一个SSTable的索引位置
    pub(crate) fn first_index_with_level(vec_ss_table: &Vec<&SsTable>, manifest: &Manifest, level: usize) -> usize {
        match vec_ss_table.first() {
            None => 0,
            Some(first_ss_table) => {
                manifest.get_index(level, first_ss_table.get_gen())
                    .unwrap_or(0)
            }
        }
    }

    /// 通过内存表构建持久化并构建SSTable
    ///
    /// 使用目标路径与文件大小，分块大小构建一个有内容的SSTable
    pub(crate) async fn create_for_immutable_table(config: &Config, io_handler: IOHandler, vec_mem_data: Vec<CommandData>, level: usize) -> Result<Self> {
        // 获取数据的Key涵盖范围
        let scope = Scope::from_vec_cmd_data(&vec_mem_data)?;
        // 获取地址
        let interval_block_size = config.sparse_index_interval_block_size;
        let gen = io_handler.get_gen();
        let mut filter = GrowableBloom::new(config.desired_error_prob, vec_mem_data.len());

        for data in vec_mem_data.iter() {
            filter.insert(data.get_key());
        }
        let size_of_data = vec_mem_data.len();
        let vec_sharding = data_sharding(
            vec_mem_data,
            ALIGNMENT_4K * interval_block_size as usize,
            config,
            false
        ).await
            .into_iter()
            .map(|(_, sharding)| sharding)
            .collect();
        let vec_index =Self::write_data_batch(vec_sharding, &io_handler).await?;
        
        let extra_info = ExtraInfo {
            vec_index,
            scope,
            filter,
            size_of_data
        };

        // 开始对稀疏索引进行伪装并断点处理
        // 获取指令数据段的数据长度
        // 不使用真实pos作为开始，而是与稀疏索引的伪装CommandData做区别
        let cmd_sparse_index = CommandData::Get { key: rmp_serde::to_vec(&extra_info)?};
        // 将稀疏索引伪装成CommandData，使最后的MetaInfo位置能够被顺利找到
        let (data_part_len, sparse_index_len) = CommandPackage::write(&io_handler, &cmd_sparse_index).await?;

        // 数据刷入以获取crc_code
        io_handler.flush().await?;

        let crc_code = io_handler.get_crc_code().await? as u64;

        // 将以上持久化信息封装为MetaInfo
        let meta_info = MetaInfo{
            level: level as u64,
            version: 0,
            data_len: data_part_len as u64,
            index_len: sparse_index_len as u64,
            interval_block_size,
            crc_code
        };
        meta_info.write_to_file_and_flush(&io_handler).await?;

        let size_of_disk = io_handler.file_size().await?;

        info!("[SsTable: {}][create_form_index][TableMetaInfo]: {:?}", gen, meta_info);
        match extra_info {
            ExtraInfo { vec_index, scope, filter, size_of_data } => {
                Ok(SsTable {
                    meta_info,
                    sparse_index: SkipMap::from_iter(vec_index),
                    io_handler,
                    gen,
                    scope,
                    filter,
                    size_of_disk,
                    size_of_data,
                })
            }
        }

    }
}