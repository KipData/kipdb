use crate::kernel::io::IoReader;
use crate::kernel::KernelResult;
use serde::{Deserialize, Serialize};
use std::io::SeekFrom;

/// Footer序列化长度定长
/// 注意Footer序列化时，需要使用类似BinCode这样的定长序列化框架，否则若类似Rmp的话会导致Footer在不同数据时，长度不一致
pub(crate) const TABLE_FOOTER_SIZE: usize = 21;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[repr(C, align(32))]
pub(crate) struct Footer {
    pub(crate) level: u8,
    pub(crate) index_offset: u32,
    pub(crate) index_len: u32,
    pub(crate) meta_offset: u32,
    pub(crate) meta_len: u32,
    pub(crate) size_of_disk: u32,
}

impl Footer {
    /// 从对应文件的IOHandler中将Footer读取出来
    pub(crate) fn read_to_file(reader: &mut dyn IoReader) -> KernelResult<Self> {
        let mut buf = [0; TABLE_FOOTER_SIZE];

        let _ = reader.seek(SeekFrom::End(-(TABLE_FOOTER_SIZE as i64)))?;
        let _ = reader.read(&mut buf)?;

        Ok(bincode::deserialize(&buf)?)
    }
}

#[cfg(test)]
mod test {
    use crate::kernel::lsm::table::ss_table::footer::{Footer, TABLE_FOOTER_SIZE};
    use crate::kernel::KernelResult;

    #[test]
    fn test_footer() -> KernelResult<()> {
        let info = Footer {
            level: 0,
            index_offset: 0,
            index_len: 0,
            meta_offset: 0,
            meta_len: 0,
            size_of_disk: 0,
        };

        assert_eq!(bincode::serialize(&info)?.len(), TABLE_FOOTER_SIZE);

        Ok(())
    }
}
