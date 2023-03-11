use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use memmap2::{Mmap, MmapMut};
use parking_lot::Mutex;
use crate::kernel::Result;
use crate::kernel::io::{FileExtension, IoType, IoReader, IoWriter};

/// 使用MMap作为实现的IOHandler
/// 目前主要用途是作为缓存读取器，尽可能减少磁盘IO弥补BufHandler读取性能上的不足
/// 不建议用于写数据，原因:
/// https://zhuanlan.zhihu.com/p/470109297
#[derive(Debug)]
pub(crate) struct MMapIoReader {
    gen: i64,
    dir_path: Arc<PathBuf>,
    reader: MMapReader,
    extension: Arc<FileExtension>,
}

impl MMapIoReader {
    pub(crate) fn new(dir_path: Arc<PathBuf>, gen: i64, extension: Arc<FileExtension>) -> Result<Self> {
        let path = extension.path_with_gen(&dir_path, gen);

        let reader = MMapReader::new(&File::open(path)?)?;

        Ok(MMapIoReader {
            gen,
            dir_path,
            reader,
            extension,
        })
    }
}

pub(crate) struct MMapIoWriter {
    gen: i64,
    dir_path: Arc<PathBuf>,
    writer: Mutex<MMapWriter>,
    extension: Arc<FileExtension>,
}

impl MMapIoWriter {
    pub(crate) fn new(dir_path: Arc<PathBuf>, gen: i64, extension: Arc<FileExtension>) -> Result<Self> {
        // 通过路径构造写入器
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(extension.path_with_gen(&dir_path, gen))?;

        let writer = Mutex::new(MMapWriter::new(&file)?);

        Ok(MMapIoWriter {
            gen,
            dir_path,
            writer,
            extension,
        })
    }
}

impl IoWriter for MMapIoWriter {
    fn get_gen(&self) -> i64 {
        self.gen
    }

    fn get_path(&self) -> PathBuf {
        self.extension
            .path_with_gen(&self.dir_path, self.gen)
    }

    fn write(&self, buf: Vec<u8>) -> Result<(u64, usize)> {
        let mut writer = self.writer.lock();

        let start_pos = writer.pos;
        let slice_buf = buf.as_slice();
        let _ignore = writer.write(slice_buf)?;

        Ok((start_pos, slice_buf.len()))
    }

    fn flush(&self) -> Result<()> {
        self.writer.lock()
            .flush()?;

        Ok(())
    }

    fn get_type(&self) -> IoType {
        IoType::MMap
    }
}

impl IoReader for MMapIoReader {
    fn get_gen(&self) -> i64 {
        self.gen
    }

    fn get_path(&self) -> PathBuf {
        self.extension
            .path_with_gen(&self.dir_path, self.gen)
    }

    fn read_with_pos(&self, start: u64, len: usize) -> Result<Vec<u8>> {
        let start_pos = start as usize;

        self.reader.read_bytes(start_pos, len + start_pos)
            .map(|slice| slice.to_vec())
    }

    fn get_type(&self) -> IoType {
        IoType::MMap
    }
}

#[derive(Debug)]
struct MMapReader {
    mmap: Mmap,
    pos: usize
}

impl MMapReader {
    fn read_bytes(&self, start: usize, end: usize) -> Result<&[u8]> {
        Ok(&self.mmap[start..end])
    }

    #[allow(unsafe_code)]
    fn new(file: &File) -> Result<MMapReader> {
        let mmap = unsafe{ Mmap::map(file) }?;
        Ok(MMapReader {
            mmap,
            pos: 0
        })
    }
}

impl Read for MMapReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let last_pos = self.pos;
        let len = (&self.mmap[last_pos..]).read(buf)?;
        self.pos += len;
        Ok(len)
    }
}

#[derive(Debug)]
struct MMapWriter {
    mmap_mut: MmapMut,
    pos: u64
}

impl MMapWriter {
    #[allow(unsafe_code)]
    fn new(file: &File) -> Result<MMapWriter> {
        let mmap_mut = unsafe {
            MmapMut::map_mut(file)?
        };
        Ok(MMapWriter {
            pos: 0,
            mmap_mut
        })
    }
}

impl Write for MMapWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let last_pos = self.pos as usize;
        let len = (&mut self.mmap_mut[last_pos..]).write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.mmap_mut.flush()?;
        Ok(())
    }
}