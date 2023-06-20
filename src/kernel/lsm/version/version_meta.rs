use itertools::Itertools;
use crate::kernel::lsm::version::version_edit::EditType;
use crate::kernel::Result;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub(crate) struct VersionMeta {
    /// SSTable集合占有磁盘大小
    pub(crate) size_of_disk: u64,
    /// SSTable集合中指令数量
    pub(crate) len: usize,
}

impl VersionMeta {
    // MetaData对SSTable统计数据处理
    pub(crate) fn statistical_process(
        &mut self,
        vec_statistics_sst_meta: Vec<(EditType, u64, usize)>,
    ) -> Result<()> {
        // 优先对新增数据进行统计再统一减去对应的数值避免删除动作聚集在前部分导致数值溢出
        for (event_type, smeta_size_of_disk, smeta_len) in vec_statistics_sst_meta
            .into_iter()
            .sorted_by_key(|(edit_type,_,_)| *edit_type)
        {
            match event_type {
                EditType::Add => {
                    self.size_of_disk += smeta_size_of_disk;
                    self.len += smeta_len;
                }
                EditType::Del => {
                    self.size_of_disk -= smeta_size_of_disk;
                    self.len -= smeta_len;
                }
            }
        }

        Ok(())
    }
}
