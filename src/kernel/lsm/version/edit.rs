use crate::kernel::lsm::table::meta::TableMeta;
use crate::kernel::lsm::table::scope::Scope;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub(crate) enum VersionEdit {
    /// ((Vec(gen), Level, TableMeta)
    DeleteFile((Vec<i64>, usize), TableMeta),
    // 确保新File的Gen都是比旧Version更大(新鲜)
    // Level 0则请忽略第二位的index参数，默认会放至最尾
    /// ((Vec(scope), Level), Index, TableMeta)
    NewFile((Vec<Scope>, usize), usize, TableMeta),
    // // Level and SSTable Gen List
    // CompactPoint(usize, Vec<i64>),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum EditType {
    Add(TableMeta),
    Del(TableMeta),
}

impl EditType {
    fn ord_num(&self) -> usize {
        match self {
            EditType::Add(_) => 0,
            EditType::Del(_) => 1,
        }
    }
}

impl Ord for EditType {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ord_num().cmp(&other.ord_num())
    }
}

impl PartialOrd for EditType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
