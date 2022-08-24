use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use tracing::info;
use crate::kernel::{CommandData, CommandPackage, MmapReader, MmapWriter, new_log_file};
use crate::kernel::lsm::{Position, MetaInfo};
use crate::kernel::Result;

struct SsTable {
    // 表索引信息
    meta_info: MetaInfo,
    // 字段稀疏索引
    sparse_index: BTreeMap<Vec<u8>, Position>,
    // 读取器
    reader: MmapReader,
    // 写入器
    writer: MmapWriter,
    // 文件路径
    path: PathBuf
}

impl SsTable {

    /// 直接构建SSTable
    ///
    /// 使用路径与分区大小创建一个空SSTable
    fn new(path: impl Into<PathBuf>, part_size: u64, file_size: u64) -> Result<Self> {
        // 获取地址
        let path = path.into();
        let (writer, reader) = new_log_file(&path, file_size)?;

        Ok(SsTable {
            meta_info: MetaInfo{
                version: 0,
                data_len: 0,
                index_len: 0,
                part_size
            },
            sparse_index: BTreeMap::new(),
            reader,
            writer,
            path
        })
    }

    /// 通过已经存在的文件构建SSTable
    ///
    /// 使用原有的路径与分区大小恢复出一个有内容的SSTable
    fn restore_from_file(path: impl Into<PathBuf>, file_size: u64) -> Result<Self>{
        // 获取地址
        let path = path.into();
        let (writer, reader) = new_log_file(&path, file_size)?;
        let info = MetaInfo::read_to_file(&reader)?;
        info!("[SsTable][restore_from_file][TableMetaInfo]: {:?}", info);

        let index_pos = info.data_len as usize;
        let index_last_pos = info.index_len as usize + index_pos;

        let arr_u8 = reader.read_zone(index_pos, index_last_pos)?;

        Ok(SsTable {
            meta_info: info,
            sparse_index: rmp_serde::from_slice(arr_u8)?,
            reader,
            writer,
            path
        })
    }

    /// 通过内存表构建持久化并构建SSTable
    ///
    /// 使用目标路径与文件大小，分块大小构建一个有内容的SSTable
    fn create_form_index(path: impl Into<PathBuf>, file_size: u64, part_size: u64, table_index: BTreeMap<Vec<u8>, CommandData>) -> Result<Self> {
        // 获取地址
        let path = path.into();
        let (mut writer, reader) = new_log_file(&path, file_size)?;
        let mut vec_cmd = Vec::new();
        let mut sparse_index: BTreeMap<Vec<u8>, Position> = BTreeMap::new();

        for cmd_data in table_index.values() {
            vec_cmd.push(cmd_data);
            if vec_cmd.len() >= part_size as usize {
                Self::write_data_part(&mut vec_cmd, &mut writer, &mut sparse_index)?;
            }
        }
        // 将剩余的指令当作一组持久化
        if !vec_cmd.is_empty() {
            Self::write_data_part(&mut vec_cmd, &mut writer, &mut sparse_index)?;
        }
        // 进行连续数据的断点处理，让后面写入的数据不影响前段数据的读取
        CommandPackage::end_tag(&mut writer);
        // 获取指令数据段的数据长度
        let data_part_len = writer.last_pos();
        writer.write(&*rmp_serde::to_vec(&sparse_index)?)?;
        let sparse_index_len = writer.last_pos() - data_part_len;
        // 将以上持久化信息封装为MetaInfo
        let info = MetaInfo{
            version: 0,
            data_len: data_part_len as u64,
            index_len: sparse_index_len as u64,
            part_size
        };
        info.write_to_file(&mut writer)?;

        Ok(SsTable {
            meta_info: info,
            sparse_index,
            reader,
            writer,
            path
        })
    }

    // fn query(&self, key: Vec<u8>) -> Result<Option<CommandData>> {
    //     let empty_position = &Position::new(0, 0);
    //     // 第一位是第一个大于的，第二位是最后一个小于的
    //     let mut position_arr = (empty_position, empty_position);
    //     for key_item in self.sparse_index.keys() {
    //         if let Ordering::Greater = key.cmp(key_item) {
    //             position_arr.0 = self.sparse_index.get(key_item)?;
    //             break;
    //         } else {
    //             position_arr.1 = self.sparse_index.get(key_item)?;
    //         }
    //     }
    //
    //     let mut len = 0;
    //     let first_big = position_arr.0;
    //     let last_small = position_arr.1;
    //     let start_pos = first_big.start;
    //     if first_big == last_small && first_big != empty_position {
    //         len = first_big.len;
    //     } else if last_small != empty_position {
    //         len = last_small.start + last_small.len - start_pos;
    //     } else {
    //         return Ok(None)
    //     }
    //
    //     let zone = self.reader.read_zone(start_pos, start_pos + len)?;
    //     let vec = CommandPackage::form_read_to_vec(zone)?;
    //
    //     Ok(())
    // }

    // 写入CommandData段
    fn write_data_part(vec_cmd_data: &mut Vec<&CommandData>, writer: &mut MmapWriter, sparse_index: &mut BTreeMap<Vec<u8>, Position>) -> Result<()> {
        let start_pos = writer.last_pos();
        for cmd_data in vec_cmd_data.iter() {
           CommandPackage::write(writer, &cmd_data)?;
        }
        // 获取数据段的长度
        let part_len = writer.last_pos() - start_pos;

        // 获取该段首位数据
        if let Some(cmd) = vec_cmd_data.first() {
            sparse_index.insert(cmd.get_key(), Position { start: start_pos, len: part_len });
        }

        vec_cmd_data.clear();
        Ok(())
    }
}