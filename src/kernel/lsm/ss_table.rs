use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use tracing::info;
use crate::kernel::{CommandData, CommandPackage, MmapReader, MmapWriter, new_log_file_with_gen};
use crate::kernel::lsm::{Position, MetaInfo};
use crate::kernel::Result;

/// SSTable
pub(crate) struct SsTable {
    // 表索引信息
    _meta_info: MetaInfo,
    // 字段稀疏索引
    sparse_index: BTreeMap<Vec<u8>, Position>,
    // 读取器
    reader: MmapReader,
    // 写入器
    writer: MmapWriter,
    // 文件路径
    _gen: u64
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
            _meta_info: MetaInfo{
                version: 0,
                data_len: 0,
                index_len: 0,
                part_size
            },
            sparse_index: BTreeMap::new(),
            reader,
            writer,
            _gen: gen
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
        info!("[SsTable][restore_from_file][TableMetaInfo]: {:?}", info);

        let index_pos = info.data_len as usize;
        let index_last_pos = info.index_len as usize + index_pos;

        let package = CommandPackage::form_pos(&reader, index_pos, index_last_pos)?;
        let sparse_index = rmp_serde::from_slice(&package.cmd.get_key())?;
        Ok(SsTable {
            _meta_info: info,
            sparse_index,
            reader,
            writer,
            _gen: gen
        })


    }

    /// 通过内存表构建持久化并构建SSTable
    ///
    /// 使用目标路径与文件大小，分块大小构建一个有内容的SSTable
    pub(crate) fn create_form_index(path: impl Into<PathBuf>, gen: u64, file_size: u64, part_size: u64, mem_table: &BTreeMap<Vec<u8>, CommandData>) -> Result<Self> {
        // 获取地址
        let path = path.into();
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
            version: 0,
            data_len: data_part_len as u64,
            index_len: sparse_index_len as u64,
            part_size
        };
        info.write_to_file(&mut writer)?;

        Ok(SsTable {
            _meta_info: info,
            sparse_index,
            reader,
            writer,
            _gen: gen
        })
    }

    /// 从该sstable中获取指定key对应的CommandData
    pub(crate) fn query(&self, key: &Vec<u8>) -> Result<Option<CommandData>> {
        let empty_position = &Position::new(0, 0);
        // 第一位是第一个大于的，第二位是最后一个小于的
        let mut position_arr = (empty_position, empty_position);
        for (key_item, value_item) in self.sparse_index.iter() {
            if let Ordering::Greater = key.cmp(key_item) {
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
        // 获取该区间段的数据
        let zone = self.reader.read_zone(start_pos, _end_pos)?;

        // 返回该区间段对应的数据结果
        Ok(if let Some(cmd_p) = CommandPackage::find_key_with_zone(zone, &key)? {
            Some(cmd_p.cmd)
        } else {
            None
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

        // 获取该段首位数据
        if let Some(cmd) = vec_cmd_data.first() {
            sparse_index.insert(cmd.get_key_clone(), Position { start: start_pos, len: part_len });
        }

        vec_cmd_data.clear();
        Ok(())
    }
}

impl Drop for SsTable {
    fn drop(&mut self) {
        self.writer.flush().unwrap();
    }
}