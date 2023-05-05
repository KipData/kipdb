pub(crate) mod buf;
pub(crate) mod direct;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use crate::kernel::io::buf::{BufIoReader, BufIoWriter};
use crate::kernel::io::direct::{DirectIoReader, DirectIoWriter};
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
    Direct,
}

impl IoFactory {
    #[inline]
    pub fn reader(&self, gen: i64, io_type: IoType) -> Result<Box<dyn IoReader>>
    {
        let dir_path = Arc::clone(&self.dir_path);
        let extension = Arc::clone(&self.extension);

        Ok(match io_type {
            IoType::Buf => Box::new(BufIoReader::new(dir_path, gen, extension)?),
            IoType::Direct => Box::new(DirectIoReader::new(dir_path, gen, extension)?)
        })
    }

    #[inline]
    pub fn writer(&self, gen: i64, io_type: IoType) -> Result<Box<dyn IoWriter>>
    {
        let dir_path = Arc::clone(&self.dir_path);
        let extension = Arc::clone(&self.extension);

        Ok(match io_type {
            IoType::Buf => Box::new(BufIoWriter::new(dir_path, gen, extension)?),
            IoType::Direct => Box::new(DirectIoWriter::new(dir_path, gen, extension)?)
        })
    }

    #[inline]
    pub fn create_fs(&self, gen: i64) -> Result<File> {
        Ok(OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(self.extension.path_with_gen(&self.dir_path, gen))?)
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

pub trait IoReader: Send + Sync + 'static + Read + Seek {

    fn get_gen(&self) -> i64;

    fn get_path(&self) -> PathBuf;

    #[inline]
    fn file_size(&self) -> Result<u64> {
        let path_buf = self.get_path();
        Ok(fs::metadata(path_buf)?.len())
    }

    fn get_type(&self) -> IoType;
}

pub trait IoWriter: Send + Sync + 'static + Write {
    fn current_pos(&mut self) -> Result<u64>;
}