use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use crate::kernel::{CommandData, log_path, Result};
use crate::kernel::io_handler::IOHandler;
use crate::kernel::lsm::lsm_kv::{LevelSlice, SsTableMap};
use crate::kernel::lsm::ss_table::SsTable;

pub(crate) mod ss_table;
pub mod lsm_kv;
mod compactor;

pub(crate) type MemTable = BTreeMap<Vec<u8>, CommandData>;

/// MetaInfo序列化长度定长
/// 注意MetaInfo序列化时，需要使用类似BinCode这样的定长序列化框架，否则若类似Rmp的话会导致MetaInfo在不同数据时，长度不一致
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
    threshold_size: u64,
    // MemTable切片，管理MemTable和ImmutableMemTable
    mem_table_slice: [MemTable; 2],
    _path: Arc<PathBuf>,
    // SSTable有序存储集合
    ss_tables_map: SsTableMap,
    // Level层级Vec
    // 以索引0为level-0这样的递推，存储文件的gen值
    level_slice: LevelSlice,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Position {
    start: u64,
    len: usize
}

impl MetaInfo {
    /// 将MetaInfo自身写入对应的IOHandler之中
    async fn write_to_file(&self, io_handler: &IOHandler) -> Result<()> {
        io_handler.write(bincode::serialize(&self)?).await?;
        Ok(())
    }

    /// 从对应文件的IOHandler中将MetaInfo读取出来
    async fn read_to_file(io_handler: &IOHandler) -> Result<Self> {
        let start_pos = io_handler.file_size().await? - TABLE_META_INFO_SIZE as u64;
        let table_meta_info = io_handler.read_with_pos(start_pos, TABLE_META_INFO_SIZE).await?;

        Ok(bincode::deserialize(table_meta_info.as_slice())?)
    }
}

impl Manifest {
    pub(crate) fn new(mut ss_tables_map: SsTableMap, path: Arc<PathBuf>, threshold_size: u64) -> Self {
        // 获取ss_table分级Vec
        let level_vec = Self::level_layered(&mut ss_tables_map);
        Self { threshold_size, mem_table_slice: [MemTable::new(), MemTable::new()], _path: path, ss_tables_map, level_slice: level_vec }
    }

    /// 使用ss_tables返回LevelVec
    /// 由于ss_tables是有序的，level_vec的内容应当是从L0->LN，旧->新
    fn level_layered(ss_tables: &mut SsTableMap) -> LevelSlice {
        let mut level_slice = [Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        for ss_table in ss_tables.values() {
            let level = ss_table.get_level();
            level_slice[level as usize].push(ss_table.get_gen());
        }
        level_slice
    }

    pub(crate) fn insert_ss_table(&mut self, gen: u64, ss_table: SsTable) {
        let level = ss_table.get_level();
        self.ss_tables_map.insert(gen, ss_table);
        self.level_slice[level as usize].push(gen);
    }

    pub(crate) fn insert_data(&mut self, key: Vec<u8>, value: CommandData) {
        self.mem_table_slice[0].insert(key, value);
    }

    /// 删除指定的过期gen
    pub(crate) fn retain_with_vec_gen_and_level(&mut self, vec_expired_gen: &Vec<u64>) -> Result<()> {
        // 遍历过期Vec对数据进行旧文件删除
        for expired_gen in vec_expired_gen.iter() {
            self.ss_tables_map.remove(expired_gen);
            fs::remove_file(log_path(&self._path, *expired_gen))?;
        }
        // 将需要删除的Vec转换为HashSet方便使用retain方法
        let set_expired_gen: HashSet<&u64> = vec_expired_gen.iter().collect();
        // 将存储的Level表中含有该gen的SSTable一并删除
        for vec_level in &mut self.level_slice {
            vec_level.retain(|gen| set_expired_gen.contains(gen));
        }

        Ok(())
    }

    pub(crate) fn get_level_vec(&self, level: usize) -> &Vec<u64> {
        &self.level_slice[level]
    }

    pub(crate) fn get_ss_table(&self, gen: &u64) -> Option<&SsTable> {
        self.ss_tables_map.get(&gen)
    }

    pub(crate) fn mem_table_is_empty(&self) -> bool {
        self.mem_table_slice[0].len() < 1 && self.mem_table_slice[1].len() < 1
    }

    pub(crate) fn get_ss_table_map(&self) -> &SsTableMap {
        &self.ss_tables_map
    }

    fn load(&mut self, mem_table: MemTable) {
        self.mem_table_slice[0] = mem_table;
    }

    fn get_cmd_data(&self, key: &Vec<u8>) -> Option<&CommandData> {
        if let Some(data) = self.mem_table_slice[0].get(key) {
            Some(data)
        } else {
            if let Some(data) = self.mem_table_slice[1].get(key) {
                Some(data)
            } else {
                None
            }
        }
    }

    fn is_threshold_exceeded(&self) -> bool {
        self.mem_table_slice[0].len() > self.threshold_size as usize
    }

    /// MemTable交换并分解
    fn table_swap(&mut self) -> (Vec<&Vec<u8>>, Vec<&CommandData>){
        self.mem_table_slice.swap(0, 1);
        self.mem_table_slice[0] = MemTable::new();

        let immut_table = &self.mem_table_slice[1];
        let vec_keys = immut_table.keys().collect_vec();
        let vec_values = immut_table.values().collect_vec();
        (vec_keys, vec_values)
    }
}

impl Position {
    pub fn new(start: u64, len: usize) -> Self {
        Self { start, len }
    }
}

#[test]
fn test_meta_info() -> Result<()> {
    let info = MetaInfo {
        level: 0,
        version: 0,
        data_len: 0,
        index_len: 0,
        part_size: 0
    };

    let vec_u8 = bincode::serialize(&info)?;

    assert_eq!(vec_u8.len(), TABLE_META_INFO_SIZE);

    Ok(())
}