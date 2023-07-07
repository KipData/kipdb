use crate::kernel::lsm::mem_table::KeyValue;
use crate::kernel::Result;
use crate::KernelError;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::Bound;
use std::sync::atomic::Ordering::{Acquire, SeqCst};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

const SEEK_COMPACTION_COUNT: u32 = 20;

/// 数据范围索引
/// 用于缓存SSTable中所有数据的第一个和最后一个数据的Key
/// 标明数据的范围以做到快速区域定位
#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct Scope {
    pub(crate) start: Bytes,
    pub(crate) end: Bytes,
    gen: i64,
    // SeekMiss计数
    allowed_seeks: Option<Arc<AtomicU32>>,
}

impl PartialEq for Scope {
    fn eq(&self, other: &Self) -> bool {
        if let (Some(a), Some(b)) = (&self.allowed_seeks, &other.allowed_seeks) {
            a.load(Acquire) == b.load(Acquire)
                || self.start == other.start
                || self.end == other.end
                || self.gen == other.gen
        } else {
            false
        }
    }
}

impl Scope {
    /// SeekMiss计数自增
    /// 当跃出阈值且CAS重置成功时返回`true`
    /// 表示应当触发阈值
    pub(crate) fn seeks_increase(&self) -> bool {
        if let Some(seeks) = &self.allowed_seeks {
            let current = seeks.load(Ordering::Relaxed);
            current > SEEK_COMPACTION_COUNT
                && seeks.compare_exchange(current, 0, SeqCst, Acquire).is_ok()
        } else {
            false
        }
    }

    pub(crate) fn gen(&self) -> i64 {
        self.gen
    }

    /// 由KeyValue组成的Key构成scope
    pub(crate) fn from_range(gen: i64, first: Bytes, last: Bytes) -> Self {
        Scope {
            start: first,
            end: last,
            gen,
            allowed_seeks: Some(Arc::new(AtomicU32::new(0))),
        }
    }

    pub(crate) fn from_key(key: &[u8]) -> Self {
        let bytes = Bytes::copy_from_slice(key);
        Scope {
            start: bytes.clone(),
            end: bytes,
            gen: 0,
            allowed_seeks: None,
        }
    }

    /// 将多个scope重组融合成一个scope
    pub(crate) fn fusion(scopes: &[Scope]) -> Option<Self> {
        let start = scopes.iter().map(|scope| &scope.start).min()?.clone();
        let end = scopes.iter().map(|scope| &scope.end).max()?.clone();

        Some(Scope {
            start,
            end,
            gen: 0,
            allowed_seeks: None,
        })
    }

    /// 判断scope之间是否相交或包含
    pub(crate) fn meet(&self, target: &Scope) -> bool {
        (self.start.le(&target.start) && self.end.ge(&target.start))
            || (self.start.le(&target.end) && self.end.ge(&target.end))
            || (self.start.le(&target.start)) && self.end.ge(&target.end)
            || (self.start.ge(&target.start)) && self.end.le(&target.end)
    }

    /// 判断key与Scope是否相交或包含
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

    /// 由一组有序KeyValue组成一个scope
    #[allow(clippy::pattern_type_mismatch)]
    pub(crate) fn from_sorted_vec_data(gen: i64, vec_mem_data: &Vec<KeyValue>) -> Result<Self> {
        match vec_mem_data.as_slice() {
            [first, .., last] => Ok(Self::from_range(gen, first.0.clone(), last.0.clone())),
            [one] => Ok(Self::from_range(gen, one.0.clone(), one.0.clone())),
            _ => Err(KernelError::DataEmpty),
        }
    }
}
