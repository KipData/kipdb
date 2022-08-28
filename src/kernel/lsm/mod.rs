use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::RwLock;
use serde::{Deserialize, Serialize};
use crate::kernel::{CommandPackage, log_path, MmapReader, MmapWriter, Result};
use crate::kernel::lsm::lsm_kv::{leve_vec_insert, LevelVec, SsTableMap};
use crate::kernel::lsm::ss_table::SsTable;

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

pub(crate) struct Manifest {
    rwlock: RwLock<String>,
    // SSTable有序存储集合
    ss_tables_map: SsTableMap,
    // Level层级Vec
    // 以索引0为level-0这样的递推，存储文件的gen值
    level_vec: LevelVec,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Position {
    start: usize,
    len: usize
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

impl Manifest {
    pub(crate) fn new(mut ss_tables_map: SsTableMap) -> Self {
        // 获取ss_table分级Vec
        let level_vec = Self::level_layered(&mut ss_tables_map);
        let rwlock = RwLock::new("".to_string());
        Self { rwlock, ss_tables_map, level_vec }
    }

    /// 使用ss_tables返回LevelVec
    /// 由于ss_tables是有序的，level_vec的内容应当是从L0->LN，旧->新
    fn level_layered(ss_tables: &mut SsTableMap) -> LevelVec {
        let mut level_vec = Vec::new();
        for ss_table in ss_tables.values() {
            let level = ss_table.get_level();

            leve_vec_insert(&mut level_vec, level, ss_table.get_gen())
        }
        level_vec
    }

    pub(crate) fn get_ss_table_map(&self) -> &SsTableMap {
        let _ = self.rwlock.read().unwrap();
        &self.ss_tables_map
    }

    pub(crate) fn insert(&mut self, gen: u64, ss_table: SsTable) -> Result<()> {
        let _ = self.rwlock.write().unwrap();
        let level = ss_table.get_level();
        self.ss_tables_map.insert(gen, ss_table);
        leve_vec_insert(&mut self.level_vec, level, gen);
        Ok(())
    }

    pub(crate) fn retain_with_vec_gen_and_level(&mut self, vec_expired_gen: &Vec<u64>, level: usize, path: &PathBuf) -> Result<()> {
        let _ = self.rwlock.write().unwrap();
        // 遍历过期Vec对数据进行旧文件删除
        for expired_gen in vec_expired_gen.iter() {
            self.ss_tables_map.remove(expired_gen);
            fs::remove_file(log_path(path, *expired_gen))?;
        }

        let set_expired_gen: HashSet<&u64> = vec_expired_gen.iter().collect();

        let option: Option<&mut Vec<u64>> = self.level_vec.get_mut(level);
        match option {
            Some(vec) => {
                vec.retain(|gen| set_expired_gen.contains(gen));
            }
            None => {}
        }


        Ok(())
    }

    pub(crate) fn get_level_vec(&self, level: usize) -> Option<&Vec<u64>> {
        let _ = self.rwlock.read().unwrap();
        self.level_vec.get(level)
    }

    pub(crate) fn get_mut_level_vec(&mut self, level: usize) -> Option<&mut Vec<u64>> {
        let _ = self.rwlock.read().unwrap();
        self.level_vec.get_mut(level)
    }

    pub(crate) fn get_ss_table(&self, gen: &u64) -> Option<&SsTable> {
        let _ = self.rwlock.read().unwrap();
        self.ss_tables_map.get(&gen)
    }
}

impl Position {
    pub fn new(start: usize, len: usize) -> Self {
        Self { start, len }
    }
}