use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use memmap2::{Mmap, MmapMut};
use tokio::sync::Mutex;
use async_trait::async_trait;
use crate::kernel::Result;
use crate::kernel::io::{FileExtension,IOHandler, IOType};

/// 使用MMap作为实现的IOHandler
/// 目前主要用途是作为缓存读取器，尽可能减少磁盘IO弥补BufHandler读取性能上的不足
/// 不建议用于写数据，原因:
/// https://zhuanlan.zhihu.com/p/470109297
#[derive(Debug)]
pub(crate) struct MMapHandler {
    gen: i64,
    dir_path: Arc<PathBuf>,
    writer: Mutex<MmapWriter>,
    reader: MmapReader,
    extension: Arc<FileExtension>,
}

impl MMapHandler {
    pub(crate) fn new(dir_path: Arc<PathBuf>, gen: i64, extension: Arc<FileExtension>) -> Result<Self> {
        let path = extension.path_with_gen(&dir_path, gen);

        // 通过路径构造写入器
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&path)?;

        let writer = Mutex::new(MmapWriter::new(&file)?);
        let reader = MmapReader::new(&File::open(path)?)?;

        Ok(MMapHandler {
            gen,
            dir_path,
            writer,
            reader,
            extension,
        })
    }
}

#[async_trait]
impl IOHandler for MMapHandler {
    fn get_gen(&self) -> i64 {
        self.gen
    }

    fn get_path(&self) -> PathBuf {
        self.extension
            .path_with_gen(&self.dir_path, self.gen)
    }

    async fn read_with_pos(&self, start: u64, len: usize) -> Result<Vec<u8>> {
        let start_pos = start as usize;

        self.reader.read_bytes(start_pos, len + start_pos)
            .map(|slice| slice.to_vec())
    }

    async fn write(&self, buf: Vec<u8>) -> Result<(u64, usize)> {
        let mut writer = self.writer.lock().await;

        let start_pos = writer.pos;
        let slice_buf = buf.as_slice();
        let _ignore = writer.write(slice_buf)?;

        Ok((start_pos, slice_buf.len()))
    }

    async fn flush(&self) -> Result<()> {
        self.writer.lock()
            .await
            .flush()?;
        Ok(())
    }

    fn get_type(&self) -> IOType {
        IOType::MMap
    }
}

#[derive(Debug)]
struct MmapReader {
    mmap: Mmap,
    pos: usize
}

impl MmapReader {
    fn read_bytes(&self, start: usize, end: usize) -> Result<&[u8]> {
        Ok(&self.mmap[start..end])
    }

    #[allow(unsafe_code)]
    fn new(file: &File) -> Result<MmapReader> {
        let mmap = unsafe{ Mmap::map(file) }?;
        Ok(MmapReader{
            mmap,
            pos: 0
        })
    }
}

impl Read for MmapReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let last_pos = self.pos;
        let len = (&self.mmap[last_pos..]).read(buf)?;
        self.pos += len;
        Ok(len)
    }
}

#[derive(Debug)]
struct MmapWriter {
    mmap_mut: MmapMut,
    pos: u64
}

impl MmapWriter {
    #[allow(unsafe_code)]
    fn new(file: &File) -> Result<MmapWriter> {
        let mmap_mut = unsafe {
            MmapMut::map_mut(file)?
        };
        Ok(MmapWriter{
            pos: 0,
            mmap_mut
        })
    }
}

impl Write for MmapWriter {
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