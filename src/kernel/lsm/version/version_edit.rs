use serde::{Deserialize, Serialize};
use crate::kernel::lsm::ss_table::Scope;
use crate::kernel::lsm::ss_table::sst_meta::SSTableMeta;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub(crate) enum VersionEdit {
    /// ((Vec(gen), Level, SSTableMeta)
    DeleteFile((Vec<i64>, usize), SSTableMeta),
    // 确保新File的Gen都是比旧Version更大(新鲜)
    // Level 0则请忽略第二位的index参数，默认会放至最尾
    /// ((Vec(scope), Level), Index, SSTableMeta)
    NewFile((Vec<Scope>, usize), usize, SSTableMeta),
    // // Level and SSTable Gen List
    // CompactPoint(usize, Vec<i64>),
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) enum EditType {
    Add = 0,
    Del = 1,
}