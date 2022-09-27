use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use crate::kernel::{CommandData, log_path, Result};
use crate::kernel::io_handler::IOHandler;
use crate::kernel::lsm::lsm_kv::{LevelSlice, SsTableMap};
use crate::kernel::lsm::ss_table::{Score, SsTable};

pub(crate) mod ss_table;
pub mod lsm_kv;
mod compactor;

pub(crate) type MemMap = BTreeMap<Vec<u8>, CommandData>;

/// MetaInfo序列化长度定长
/// 注意MetaInfo序列化时，需要使用类似BinCode这样的定长序列化框架，否则若类似Rmp的话会导致MetaInfo在不同数据时，长度不一致
const TABLE_META_INFO_SIZE: usize = 40;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
struct MetaInfo {
    level: u64,
    version: u64,
    data_len: u64,
    index_len: u64,
    part_size: u64
}

struct MemTable {
    // MemTable切片，管理MemTable和ImmutableMemTable
    mem_table_slice: RwLock<[MemMap; 2]>
}

pub(crate) struct Manifest {
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

impl MemTable {
    pub(crate) fn new(mem_map: MemMap) -> Self {
        MemTable { mem_table_slice: RwLock::new([mem_map, MemMap::new()]) }
    }

    pub(crate) async fn insert_data(&self, key: Vec<u8>, value: CommandData) {
        let mut mem_table_slice = self.mem_table_slice.write().await;

        mem_table_slice[0].insert(key, value);
    }

    pub(crate) async fn mem_table_is_empty(&self) -> bool {
        let mem_table_slice = self.mem_table_slice.read().await;

        mem_table_slice[0].len() < 1 && mem_table_slice[1].len() < 1
    }

    async fn is_threshold_exceeded_minor(&self, threshold_size: usize) -> bool {
        self.mem_table_slice.read()
            .await[0]
            .len() > threshold_size
    }

    /// MemTable交换并分解
    async fn table_swap(&self) -> (Vec<Vec<u8>>, Vec<CommandData>){
        let mut mem_table_slice = self.mem_table_slice.write().await;

        mem_table_slice.swap(0, 1);
        mem_table_slice[0] = MemMap::new();
        mem_table_slice[1].clone()
            .into_iter()
            .unzip()
    }

    async fn get_cmd_data(&self, key: &Vec<u8>) -> Option<CommandData> {
        let mem_table_slice = self.mem_table_slice.read().await;

        if let Some(data) = mem_table_slice[0].get(key) {
            Some(data.clone())
        } else {
            mem_table_slice[1].get(key).map(|cmd| cmd.clone())
        }
    }
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
    pub(crate) fn new(mut ss_tables_map: SsTableMap, path: Arc<PathBuf>) -> Self {
        // 获取ss_table分级Vec
        let level_vec = Self::level_layered(&mut ss_tables_map);
        Self { _path: path, ss_tables_map, level_slice: level_vec }
    }

    /// 使用ss_tables返回LevelVec
    /// 由于ss_tables是有序的，level_vec的内容应当是从L0->LN，旧->新
    fn level_layered(ss_tables: &mut SsTableMap) -> LevelSlice {
        let mut level_slice = [Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        for ss_table in ss_tables.values() {
            let level = ss_table.get_level();
            level_slice[level].push(ss_table.get_gen());
        }
        level_slice
    }

    pub(crate) fn insert_ss_table_with_index(&mut self, ss_table: SsTable, index: usize) {
        let gen = ss_table.get_gen();
        let level = ss_table.get_level();
        self.ss_tables_map.insert(gen, ss_table);
        self.level_slice[level].insert(index, gen);
    }

    pub(crate) fn insert_ss_table_with_index_batch(&mut self, ss_tables: Vec<SsTable>, index: usize) {
        ss_tables.into_iter()
            .rev()
            .for_each(|ss_table| {
                self.insert_ss_table_with_index(ss_table, index);
            });
    }

    pub(crate) fn insert_ss_table_with_existed_table(&mut self, ss_table: SsTable, existed_table: &SsTable) -> usize {
        let index = self.get_index(existed_table.get_level(), existed_table.get_gen()).unwrap_or(0);
        self.insert_ss_table_with_index(ss_table, index);
        index
    }

    /// 删除指定的过期gen
    pub(crate) fn retain_with_vec_gen_and_level(&mut self, vec_expired_gen: &Vec<i64>) -> Result<()> {
        // 遍历过期Vec对数据进行旧文件删除
        for expired_gen in vec_expired_gen.iter() {
            self.ss_tables_map.remove(expired_gen);
            fs::remove_file(log_path(&self._path, *expired_gen))?;
        }
        // 将需要删除的Vec转换为HashSet方便使用retain方法
        let set_expired_gen: HashSet<&i64> = vec_expired_gen.iter().collect();
        // 将存储的Level表中含有该gen的SSTable一并删除
        for vec_level in &mut self.level_slice {
            vec_level.retain(|gen| !set_expired_gen.contains(gen));
        }

        Ok(())
    }

    pub(crate) fn get_level_vec(&self, level: usize) -> &Vec<i64> {
        &self.level_slice[level]
    }

    pub(crate) fn get_ss_table(&self, gen: &i64) -> Option<&SsTable> {
        self.ss_tables_map.get(&gen)
    }

    fn is_threshold_exceeded_major(&self, sst_size: usize, level: usize, sst_magnification: usize) -> bool {
        self.level_slice[level].len() > (sst_size * sst_magnification * (level + 1))
    }

    /// 使用Key从现有SSTables中获取对应的数据
    pub(crate) async fn get_data_for_ss_tables(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>> {
        for (_, ss_table) in &self.ss_tables_map {
            if let Some(cmd_data) = ss_table.query(key).await? {
                return Ok(cmd_data.get_value_owner());
            }
        }

        Ok(None)
    }

    pub(crate) fn get_ss_table_batch(&self, vec_gen: &Vec<i64>) -> Option<Vec<&SsTable>> {
        vec_gen.iter()
            .map(|gen| self.get_ss_table(gen))
            .collect::<Option<Vec<&SsTable>>>()
    }

    pub(crate) fn get_meet_score_ss_tables(&self, level: usize, score: &Score) -> Vec<&SsTable> {
        self.get_level_vec(level).iter()
            .map(|gen| self.get_ss_table(gen).unwrap())
            .filter(|ss_table| ss_table.get_score()
                .meet(score))
            .collect_vec()
    }

    pub(crate) fn get_index(&self, level: usize, source_gen: i64) -> Option<usize> {
        self.level_slice[level].iter()
            .enumerate()
            .find(|(_ , gen)| source_gen.eq(*gen))
            .map(|(index, _)| index)
    }
}

impl Position {
    /// 通过稀疏索引与指定Key进行获取对应Position
    pub(crate) fn from_sparse_index_with_key<'a>(sparse_index: &'a BTreeMap<Vec<u8>, Position>, key: &'a Vec<u8>) -> Option<&'a Self> {
        sparse_index.into_iter()
            .rev()
            .find(|(key_item, _)| !key.cmp(key_item).eq(&Ordering::Less))
            .map(|(_, value_item)| value_item)
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