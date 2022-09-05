use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use crate::kernel::{CommandPackage, log_path, Result};
use crate::kernel::io_handler::IOHandler;
use crate::kernel::lsm::lsm_kv::{LevelVec, SsTableMap};
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
    _path: Arc<PathBuf>,
    // SSTable有序存储集合
    ss_tables_map: SsTableMap,
    // Level层级Vec
    // 以索引0为level-0这样的递推，存储文件的gen值
    level_vec: LevelVec,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Position {
    start: u64,
    len: usize
}

impl MetaInfo {
    /// 将MetaInfo自身写入对应的IOHandler之中
    async fn write_to_file(&self, io_handler: &IOHandler) -> Result<()> {
        let vec_u8 = rmp_serde::to_vec(&self)?;
        io_handler.write(vec_u8).await?;
        Ok(())
    }

    /// 从对应文件的IOHandler中将MetaInfo读取出来
    async fn read_to_file(io_handler: &IOHandler) -> Result<Self> {
        let start_pos = CommandPackage::bytes_last_pos(io_handler).await?;
        let table_meta_info = io_handler.read_with_pos(start_pos, TABLE_META_INFO_SIZE)
            .await?;

        Ok(rmp_serde::from_slice(table_meta_info.as_slice())?)
    }
}

impl Manifest {
    pub(crate) fn new(mut ss_tables_map: SsTableMap, path: Arc<PathBuf>) -> Self {
        // 获取ss_table分级Vec
        let level_vec = Self::level_layered(&mut ss_tables_map);
        Self { _path: path, ss_tables_map, level_vec }
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

    pub(crate) fn insert(&mut self, gen: u64, ss_table: SsTable) -> Result<()> {
        let level = ss_table.get_level();
        self.ss_tables_map.insert(gen, ss_table);
        leve_vec_insert(&mut self.level_vec, level, gen);
        Ok(())
    }

    // 删除指定的过期gen
    pub(crate) fn retain_with_vec_gen_and_level(&mut self, vec_expired_gen: &Vec<u64>) -> Result<()> {
        // 遍历过期Vec对数据进行旧文件删除
        for expired_gen in vec_expired_gen.iter() {
            self.ss_tables_map.remove(expired_gen);
            fs::remove_file(log_path(&self._path, *expired_gen))?;
        }
        // 将需要删除的Vec转换为HashSet方便使用retain方法
        let set_expired_gen: HashSet<&u64> = vec_expired_gen.iter().collect();
        // 将存储的Level表中含有该gen的SSTable一并删除
        for vec_level in &mut self.level_vec {
            vec_level.retain(|gen| set_expired_gen.contains(gen));
        }

        Ok(())
    }

    pub(crate) fn get_level_vec(&self, level: usize) -> Option<&Vec<u64>> {
        self.level_vec.get(level)
    }

    pub(crate) fn get_ss_table(&self, gen: &u64) -> Option<&SsTable> {
        self.ss_tables_map.get(&gen)
    }

    pub(crate) fn get_ss_table_map(&self) -> &SsTableMap {
        &self.ss_tables_map
    }
}

impl Position {
    pub fn new(start: u64, len: usize) -> Self {
        Self { start, len }
    }
}

// 对LevelVec插入的封装方法
fn leve_vec_insert(level_vec: &mut LevelVec, level: u64, gen: u64) {
    let option: Option<&mut Vec<u64>> = level_vec.get_mut(level as usize);
    match option {
        Some(vec) => {
            vec.push(gen)
        }
        None => {
            level_vec.push(vec![gen])
        }
    }
}