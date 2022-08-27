use std::io::Write;
use serde::{Deserialize, Serialize};
use crate::kernel::{CommandPackage, MmapReader, MmapWriter, Result};

pub(crate) mod ss_table;
pub mod lsm_kv;

// META_INFO序列化长度定长
const TABLE_META_INFO_SIZE: usize = 40;

#[derive(Serialize, Deserialize, Debug)]
struct MetaInfo {
    level: u64,
    version: u64,
    data_len: u64,
    index_len: u64,
    part_size: u64
}

impl MetaInfo {
    fn write_to_file(&self, writer: &mut MmapWriter) -> Result<()> {
        let vec_u8 = bincode::serialize(self)?;
        writer.write(&*vec_u8)?;
        Ok(())
    }

    fn read_to_file(reader: &MmapReader) -> Result<Self> {
        let start_pos = CommandPackage::bytes_last_pos(reader);
        let last_pos = start_pos + TABLE_META_INFO_SIZE;
        let table_meta_info = reader.read_zone(start_pos, last_pos)?;

        Ok(bincode::deserialize(table_meta_info)?)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Position {
    start: usize,
    len: usize
}

impl Position {
    pub fn new(start: usize, len: usize) -> Self {
        Self { start, len }
    }
}