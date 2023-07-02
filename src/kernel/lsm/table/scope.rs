use crate::kernel::lsm::mem_table::KeyValue;
use crate::kernel::Result;
use crate::KernelError;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::Bound;

/// 数据范围索引
/// 用于缓存SSTable中所有数据的第一个和最后一个数据的Key
/// 标明数据的范围以做到快速区域定位
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct Scope {
    pub(crate) start: Bytes,
    pub(crate) end: Bytes,
    gen: i64,
}

impl Scope {
    pub(crate) fn gen(&self) -> i64 {
        self.gen
    }

    /// 由KeyValue组成的Key构成scope
    pub(crate) fn from_key(gen: i64, first: Bytes, last: Bytes) -> Self {
        Scope {
            start: first,
            end: last,
            gen,
        }
    }

    /// 将多个scope重组融合成一个scope
    pub(crate) fn fusion(scopes: &[Scope]) -> Option<Self> {
        let start = scopes.iter().map(|scope| &scope.start).min()?.clone();
        let end = scopes.iter().map(|scope| &scope.end).max()?.clone();

        Some(Scope { start, end, gen: 0 })
    }

    /// 判断scope之间是否相交
    pub(crate) fn meet(&self, target: &Scope) -> bool {
        (self.start.le(&target.start) && self.end.ge(&target.start))
            || (self.start.le(&target.end) && self.end.ge(&target.end))
    }

    /// 判断key与Scope是否相交
    pub(crate) fn meet_by_key(&self, key: &[u8]) -> bool {
        self.start.as_ref().le(key) && self.end.as_ref().ge(key)
    }

    #[allow(dead_code)]
    pub(crate) fn meet_bound(&self, min: Bound<&[u8]>, max: Bound<&[u8]>) -> bool {
        let is_min_inside = match min {
            Bound::Included(key) => self.start.as_ref().le(key),
            Bound::Excluded(key) => self.start.as_ref().lt(key),
            Bound::Unbounded => true,
        };
        let is_max_inside = match max {
            Bound::Included(key) => self.end.as_ref().ge(key),
            Bound::Excluded(key) => self.end.as_ref().gt(key),
            Bound::Unbounded => true,
        };

        is_min_inside && is_max_inside
    }

    /// 由一组KeyValue组成一个scope
    #[allow(clippy::pattern_type_mismatch)]
    pub(crate) fn from_vec_data(gen: i64, vec_mem_data: &Vec<KeyValue>) -> Result<Self> {
        match vec_mem_data.as_slice() {
            [first, .., last] => Ok(Self::from_key(gen, first.0.clone(), last.0.clone())),
            [one] => Ok(Self::from_key(gen, one.0.clone(), one.0.clone())),
            _ => Err(KernelError::DataEmpty),
        }
    }
}
