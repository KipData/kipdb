use crate::kernel::io::IoReader;
use crate::kernel::KernelResult;
use integer_encoding::{FixedIntReader, FixedIntWriter};
use std::io::SeekFrom;

/// Footer序列化长度定长
/// 注意Footer序列化时，需要使用类似BinCode这样的定长序列化框架，否则若类似Rmp的话会导致Footer在不同数据时，长度不一致
pub(crate) const TABLE_FOOTER_SIZE: usize = 21;

#[derive(Debug, PartialEq, Eq)]
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
    pub(crate) fn read_to_file(mut reader: &mut dyn IoReader) -> KernelResult<Self> {
        let _ = reader.seek(SeekFrom::End(-(TABLE_FOOTER_SIZE as i64)))?;

        Ok(Footer {
            level: reader.read_fixedint()?,
            index_offset: reader.read_fixedint()?,
            index_len: reader.read_fixedint()?,
            meta_offset: reader.read_fixedint()?,
            meta_len: reader.read_fixedint()?,
            size_of_disk: reader.read_fixedint()?,
        })
    }

    pub fn to_raw(&self, bytes: &mut Vec<u8>) -> KernelResult<()> {
        bytes.write_fixedint(self.level)?;
        bytes.write_fixedint(self.index_offset)?;
        bytes.write_fixedint(self.index_len)?;
        bytes.write_fixedint(self.meta_offset)?;
        bytes.write_fixedint(self.meta_len)?;
        bytes.write_fixedint(self.size_of_disk)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::kernel::lsm::table::ss_table::footer::{Footer, TABLE_FOOTER_SIZE};
    use crate::kernel::KernelResult;

    #[test]
    fn test_footer() -> KernelResult<()> {
        let mut bytes = Vec::new();
        let info = Footer {
            level: 0,
            index_offset: 0,
            index_len: 0,
            meta_offset: 0,
            meta_len: 0,
            size_of_disk: 0,
        };
        info.to_raw(&mut bytes)?;

        assert_eq!(bytes.len(), TABLE_FOOTER_SIZE);

        Ok(())
    }
}
