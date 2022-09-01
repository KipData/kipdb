use std::cmp::Ordering;
use std::collections::btree_map::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use chrono::Local;
use itertools::Itertools;
use tracing::info;
use crate::kernel::{CommandData, CommandPackage, MmapReader, MmapWriter, new_log_file_with_gen};
use crate::kernel::lsm::{Position, MetaInfo, Manifest};
use crate::kernel::lsm::lsm_kv::Config;
use crate::kernel::Result;

type ExpiredGenVec = Vec<u64>;

pub(crate) const LEVEL_0: usize = 0;

/// SSTable
pub(crate) struct SsTable {
    // 表索引信息
    meta_info: MetaInfo,
    // 字段稀疏索引
    sparse_index: BTreeMap<Vec<u8>, Position>,
    // 读取器
    reader: MmapReader,
    // 写入器
    writer: MmapWriter,
    // 文件路径
    gen: u64
}

impl SsTable {

    /// 直接构建SSTable
    ///
    /// 使用路径与分区大小创建一个空SSTable
    pub(crate) fn new(path: impl Into<PathBuf>, gen: u64, part_size: u64, file_size: u64) -> Result<Self> {
        // 获取地址
        let path = path.into();
        let (gen, writer, reader) = new_log_file_with_gen(&path, gen, file_size)?;

        Ok(SsTable {
            meta_info: MetaInfo{
                level: 0,
                version: 0,
                data_len: 0,
                index_len: 0,
                part_size
            },
            sparse_index: BTreeMap::new(),
            reader,
            writer,
            gen
        })
    }

    /// 通过已经存在的文件构建SSTable
    ///
    /// 使用原有的路径与分区大小恢复出一个有内容的SSTable
    pub(crate) fn restore_from_file(path: impl Into<PathBuf>, gen: u64, file_size: u64) -> Result<Self>{
        // 获取地址
        let path = path.into();
        let (gen, writer, reader) = new_log_file_with_gen(&path, gen, file_size)?;
        let info = MetaInfo::read_to_file(&reader)?;
        info!("[SsTable: {}][restore_from_file][TableMetaInfo]: {:?}", gen, info);

        let index_pos = info.data_len as usize;
        let index_last_pos = info.index_len as usize + index_pos;

        let package = CommandPackage::form_pos(&reader, index_pos, index_last_pos)?;
        let sparse_index = rmp_serde::from_slice(&package.cmd.get_key())?;
        Ok(SsTable {
            meta_info: info,
            sparse_index,
            reader,
            writer,
            gen
        })


    }

    /// 写入CommandData数据段
    fn write_data_part(vec_cmd_data: &mut Vec<&CommandData>, writer: &mut MmapWriter, sparse_index: &mut BTreeMap<Vec<u8>, Position>) -> Result<()> {
        let start_pos = writer.last_pos();
        for cmd_data in vec_cmd_data.iter() {
           CommandPackage::write(writer, &cmd_data)?;
        }
        // 获取数据段的长度
        let part_len = writer.last_pos() - start_pos;

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
    pub(crate) fn level_up_for_level_0(level_1_ss_table: SsTable, manifest: &mut Manifest, vec_level_0_gen: &ExpiredGenVec) -> Result<()> {
        manifest.insert(level_1_ss_table.gen, level_1_ss_table)?;
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
    pub(crate) fn level_0_up_ss_table(config: &Config, manifest: &Manifest) -> Result<Option<(SsTable, ExpiredGenVec)>> {
        match manifest.get_level_vec(LEVEL_0).cloned() {
            // 注意! vec_ss_table_gen是由旧到新的
            // 这样归并出来的数据才能保证数据是有效的
            Some(vec_shot_snap_gen) => {

                let mut merge_map = BTreeMap::new();
                // 将所有Level0的SSTable读取各自所有的key做归并
                for gen in vec_shot_snap_gen.iter() {
                    let ss_table = manifest.get_ss_table(gen).unwrap();
                    for cmd_data in ss_table.get_all_data()? {
                        merge_map.insert(cmd_data.get_key_clone(), cmd_data);
                    }
                }

                // 获取当前时间戳当Gen
                let gen = Local::now().timestamp_millis() as u64;
                // 构建Level1的SSTable
                let mut level_1_ss_table = Self::create_form_immutable_table(&config, gen, &merge_map)?;
                level_1_ss_table.level(1);

                Ok(Some((level_1_ss_table, vec_shot_snap_gen)))
            }
            None => Ok(None)
        }
    }

    /// 从该sstable中获取指定key对应的CommandData
    pub(crate) fn query(&self, key: &Vec<u8>) -> Result<Option<CommandData>> {
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

        let mut _end_pos = 0;
        let first_big = position_arr.0;
        let last_small = position_arr.1;

        // 找出数据可能存在的区间
        let start_pos = first_big.start;
        if first_big != empty_position {
            _end_pos = first_big.len + start_pos;
        } else if last_small != empty_position {
            _end_pos = last_small.start + last_small.len;
        } else {
            return Ok(None);
        }
        info!("[SsTable: {}][query][data_zone]: {} to {}", self.gen, start_pos, _end_pos);
        // 获取该区间段的数据
        let zone = self.reader.read_zone(start_pos, _end_pos)?;

        // 返回该区间段对应的数据结果
        Ok(if let Some(cmd_p) = CommandPackage::find_key_with_zone(zone, &key)? {
            Some(cmd_p.cmd)
        } else {
            None
        })
    }

    /// 获取SsTable内所有的正常数据
    pub(crate) fn get_all_data(&self) -> Result<Vec<CommandData>> {
        let info = &self.meta_info;
        let data_len = info.data_len;

        let all_data_u8 = self.reader.read_zone(0, data_len as usize)?;
        let vec_cmd_data = CommandPackage::form_zone_to_vec(all_data_u8)?
            .into_iter()
            .map(|cmd_package| cmd_package.cmd)
            .collect_vec();
        Ok(vec_cmd_data)
    }

    /// 通过内存表构建持久化并构建SSTable
    ///
    /// 使用目标路径与文件大小，分块大小构建一个有内容的SSTable
    pub(crate) fn create_form_immutable_table(config: &Config, gen: u64, mem_table: &BTreeMap<Vec<u8>, CommandData>) -> Result<Self> {
        // 获取地址
        let path = config.dir_path.clone();
        let file_size = config.file_size;
        let part_size = config.part_size;
        let (gen, mut writer, reader) = new_log_file_with_gen(&path, gen, file_size)?;
        let mut vec_cmd = Vec::new();
        let mut sparse_index: BTreeMap<Vec<u8>, Position> = BTreeMap::new();

        // 将数据按part_size一组分段存入
        for cmd_data in mem_table.values() {
            vec_cmd.push(cmd_data);
            if vec_cmd.len() >= part_size as usize {
                Self::write_data_part(&mut vec_cmd, &mut writer, &mut sparse_index)?;
            }
        }
        // 将剩余的指令当作一组持久化
        if !vec_cmd.is_empty() {
            Self::write_data_part(&mut vec_cmd, &mut writer, &mut sparse_index)?;
        }

        // 开始对稀疏索引进行伪装并断点处理
        // 获取指令数据段的数据长度
        // 不使用真实pos作为开始，而是与稀疏索引的伪装CommandData做区别
        let data_part_len = writer.get_cmd_data_pos();
        // 将稀疏索引伪装成CommandData，使最后的MetaInfo位置能够被顺利找到
        CommandPackage::write(&mut writer, &CommandData::Get { key: rmp_serde::to_vec(&sparse_index)? })?;
        let sparse_index_len = writer.get_cmd_data_pos() - data_part_len;
        // 进行连续数据的断点处理，让后面写入的数据不影响前段数据的读取
        CommandPackage::end_tag(&mut writer);

        // 将以上持久化信息封装为MetaInfo
        let info = MetaInfo{
            level: 0,
            version: 0,
            data_len: data_part_len as u64,
            index_len: sparse_index_len as u64,
            part_size
        };
        info.write_to_file(&mut writer)?;

        info!("[SsTable: {}][create_form_index][TableMetaInfo]: {:?}", gen, info);
        Ok(SsTable {
            meta_info: info,
            sparse_index,
            reader,
            writer,
            gen
        })
    }
}

impl Drop for SsTable {
    fn drop(&mut self) {
        self.writer.flush().unwrap();
    }
}