use crate::kernel::io::{FileExtension, IoReader, IoType, IoWriter};
use crate::kernel::Result;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct DirectIoReader {
    gen: i64,
    dir_path: Arc<PathBuf>,
    fs: File,
    extension: Arc<FileExtension>,
}

#[derive(Debug)]
pub(crate) struct DirectIoWriter {
    fs: File,
}

impl DirectIoReader {
    pub(crate) fn new(
        dir_path: Arc<PathBuf>,
        gen: i64,
        extension: Arc<FileExtension>,
    ) -> Result<Self> {
        let path = extension.path_with_gen(&dir_path, gen);
        let fs = File::open(path)?;

        Ok(DirectIoReader {
            gen,
            dir_path,
            fs,
            extension,
        })
    }
}

impl DirectIoWriter {
    pub(crate) fn new(
        dir_path: Arc<PathBuf>,
        gen: i64,
        extension: Arc<FileExtension>,
    ) -> Result<Self> {
        let path = extension.path_with_gen(&dir_path, gen);
        let fs = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;

        Ok(DirectIoWriter { fs })
    }
}

impl Read for DirectIoReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.fs.read(buf)
    }
}

impl Seek for DirectIoReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.fs.seek(pos)
    }
}

impl IoReader for DirectIoReader {
    fn get_gen(&self) -> i64 {
        self.gen
    }

    fn get_path(&self) -> PathBuf {
        self.extension.path_with_gen(&self.dir_path, self.gen)
    }

    fn get_type(&self) -> IoType {
        IoType::Direct
    }
}

impl Write for DirectIoWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.fs.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.fs.flush()
    }
}

impl IoWriter for DirectIoWriter {
    fn current_pos(&mut self) -> Result<u64> {
        Ok(self.fs.stream_position()?)
    }
}
