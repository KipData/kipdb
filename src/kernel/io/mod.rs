pub(crate) mod buf;
pub(crate) mod mmap;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::fs;
use std::fs::{File, OpenOptions};
use async_trait::async_trait;
use crate::kernel::io::buf::{BufIoReader, BufIoWriter};
use crate::kernel::io::mmap::{MMapIoReader, MMapIoWriter};
use crate::kernel::Result;


#[derive(Debug, Copy, Clone)]
pub enum FileExtension {
    Log,
    SSTable,
    Manifest,
}

impl FileExtension {

    pub(crate) fn extension_str(&self) -> &'static str {
        match self {
            FileExtension::Log => "log",
            FileExtension::SSTable => "sst",
            FileExtension::Manifest => "manifest"
        }
    }

    /// 对文件夹路径填充日志文件名
    pub(crate) fn path_with_gen(&self, dir: &Path, gen: i64) -> PathBuf {
        dir.join(format!("{gen}.{}", self.extension_str()))
    }
}

#[derive(Debug)]
pub struct IoFactory {
    dir_path: Arc<PathBuf>,
    extension: Arc<FileExtension>,
}

#[derive(PartialEq, Copy, Clone, Debug)]
pub enum IoType {
    Buf,
    MMap
}

impl IoFactory {
    #[inline]
    pub fn reader(&self, gen: i64, io_type: IoType) -> Result<Box<dyn IoReader>>
    {
        let dir_path = Arc::clone(&self.dir_path);
        let extension = Arc::clone(&self.extension);

        Ok(match io_type {
            IoType::Buf => Box::new(BufIoReader::new(dir_path, gen, extension)?),
            IoType::MMap => Box::new(MMapIoReader::new(dir_path, gen, extension)?),
        })
    }

    #[inline]
    pub fn writer(&self, gen: i64, io_type: IoType) -> Result<Box<dyn IoWriter>>
    {
        let dir_path = Arc::clone(&self.dir_path);
        let extension = Arc::clone(&self.extension);

        Ok(match io_type {
            IoType::Buf => Box::new(BufIoWriter::new(dir_path, gen, extension)?),
            IoType::MMap => Box::new(MMapIoWriter::new(dir_path, gen, extension)?),
        })
    }

    pub fn create_fs(&self, gen: i64) -> Result<File> {
        Ok(OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(
                &self.extension
                    .path_with_gen(&self.dir_path, gen)
            )?)
    }

    #[inline]
    pub fn new(dir_path: impl Into<PathBuf>, extension: FileExtension) -> Result<Self> {
        let path_buf = dir_path.into();
        // 创建文件夹（如果他们缺失）
        fs::create_dir_all(&path_buf)?;
        let dir_path = Arc::new(path_buf);
        let extension = Arc::new(extension);

        Ok(Self { dir_path, extension })
    }

    #[inline]
    pub fn clean(&self, gen: i64) -> Result<()>{
        fs::remove_file(
            self.extension
                .path_with_gen(&self.dir_path, gen)
        )?;
        Ok(())
    }

    #[inline]
    pub fn has_gen(&self, gen: i64) -> Result<bool>{
        Ok(fs::try_exists(
            self.extension
                .path_with_gen(&self.dir_path, gen)
        )?)
    }
}

#[async_trait]
pub trait IoReader: Send + Sync + 'static {

    fn get_gen(&self) -> i64;

    fn get_path(&self) -> PathBuf;

    fn file_size(&self) -> Result<u64> {
        let path_buf = self.get_path();
        Ok(fs::metadata(path_buf)?.len())
    }

    async fn read_with_pos(&self, start: u64, len: usize) -> Result<Vec<u8>>;

    fn get_type(&self) -> IoType;

    async fn bytes(&self) -> Result<Vec<u8>> {
        let len = self.file_size()?;
        self.read_with_pos(0, len as usize).await
    }
}

#[async_trait]
pub trait IoWriter: Send + Sync + 'static {

    fn get_gen(&self) -> i64;

    fn get_path(&self) -> PathBuf;

    fn file_size(&self) -> Result<u64> {
        let path_buf = self.get_path();
        Ok(fs::metadata(path_buf)?.len())
    }

    async fn write(&self, buf: Vec<u8>) -> Result<(u64, usize)>;

    async fn flush(&self) -> Result<()>;

    fn get_type(&self) -> IoType;
}