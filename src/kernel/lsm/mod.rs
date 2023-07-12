use crate::kernel::lsm::compactor::{CompactTask, MergeShardingVec};
use crate::kernel::lsm::mem_table::{key_value_bytes_len, KeyValue};
use crate::kernel::lsm::storage::Gen;
use crate::kernel::lsm::version::{SeekOption, Version};
use crate::kernel::Result;
use crate::KernelError;
use bytes::Bytes;
use tokio::sync::mpsc::Sender;

mod compactor;
mod iterator;
mod log;
mod mem_table;
mod mvcc;
pub mod storage;
mod table;
mod trigger;
mod version;

/// KeyValue数据分片，尽可能将数据按给定的分片大小：file_size，填满一片（可能会溢出一些）
/// 保持原有数据的顺序进行分片，所有第一片分片中最后的值肯定会比其他分片开始的值Key排序较前（如果vec_data是以Key从小到大排序的话）
fn data_sharding(mut vec_data: Vec<KeyValue>, file_size: usize) -> MergeShardingVec {
    // 向上取整计算SSTable数量
    let part_size =
        (vec_data.iter().map(key_value_bytes_len).sum::<usize>() + file_size - 1) / file_size;

    vec_data.reverse();
    let mut vec_sharding = vec![(0, Vec::new()); part_size];
    let slice = vec_sharding.as_mut_slice();

    for i in 0..part_size {
        // 减小create_gen影响的时间
        slice[i].0 = Gen::create();
        let mut data_len = 0;
        while !vec_data.is_empty() {
            if let Some(key_value) = vec_data.pop() {
                data_len += key_value_bytes_len(&key_value);
                if data_len >= file_size && i < part_size - 1 {
                    slice[i + 1].1.push(key_value);
                    break;
                }
                slice[i].1.push(key_value);
            } else {
                break;
            }
        }
    }
    // 过滤掉没有数据的切片
    vec_sharding.retain(|(_, vec)| !vec.is_empty());
    vec_sharding
}

/// 使用Version进行Key查询，当触发Seek Miss的阈值时，
/// 使用其第一次Miss的Level进行Seek Compaction
fn query_and_compaction(
    key: &[u8],
    version: &Version,
    compactor_tx: &Sender<CompactTask>,
) -> Result<Option<Bytes>> {
    match version.query(key)? {
        SeekOption::Hit(value) => return Ok(Some(value)),
        SeekOption::Miss(Some(seek_scope)) => {
            compactor_tx
                .try_send(CompactTask::Seek(seek_scope))
                .map_err(|_| KernelError::ChannelClose)?;
        }
        _ => (),
    }
    Ok(None)
}
