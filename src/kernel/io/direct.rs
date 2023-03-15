use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use parking_lot::Mutex;
use crate::kernel::io::{FileExtension, IoReader, IoType, IoWriter};
use crate::kernel::Result;

#[derive(Debug)]
pub(crate) struct DirectIoReader {
    gen: i64,
    dir_path: Arc<PathBuf>,
    fs: Mutex<File>,
    extension: Arc<FileExtension>,
}

#[derive(Debug)]
pub(crate) struct DirectIoWriter {
    fs: File,
}

impl DirectIoReader {
    pub(crate) fn new(dir_path: Arc<PathBuf>, gen: i64, extension: Arc<FileExtension>) -> Result<Self> {
        let path = extension.path_with_gen(&dir_path, gen);
        let fs = Mutex::new(File::open(path)?);

        Ok(DirectIoReader {
            gen,
            dir_path,
            fs,
            extension,
        })
    }
}

impl DirectIoWriter {
    pub(crate) fn new(dir_path: Arc<PathBuf>, gen: i64, extension: Arc<FileExtension>) -> Result<Self> {
        let path = extension.path_with_gen(&dir_path, gen);
        let fs = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;

        Ok(DirectIoWriter { fs })
    }
}

impl IoReader for DirectIoReader {
    fn get_gen(&self) -> i64 {
        self.gen
    }

    fn get_path(&self) -> PathBuf {
        self.extension
            .path_with_gen(&self.dir_path, self.gen)
    }

    fn read_with_pos(&self, start: u64, len: usize) -> Result<Vec<u8>> {
        let mut reader = self.fs.lock();

        let mut buffer = vec![0; len];
        // 使用Vec buffer获取数据
        let _ignore = reader.seek(SeekFrom::Start(start))?;
        let _ignore1 = reader.read(&mut buffer)?;

        Ok(buffer)
    }

    fn get_type(&self) -> IoType {
        IoType::Direct
    }
}

impl IoWriter for DirectIoWriter {
    fn write(&mut self, buf: Vec<u8>) -> Result<(u64, usize)> {
        let start_pos = self.fs.stream_position()?;

        Ok(self.fs.write(&buf).map(|len| (start_pos, len))?)
    }

    fn flush(&mut self) -> Result<()> {
        self.fs.flush()?;
        Ok(())
    }
}