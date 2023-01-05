pub(crate) mod buf_handler;
pub(crate) mod mmap_handler;

use std::path::PathBuf;
use std::sync::Arc;
use std::fs;
use async_trait::async_trait;
use crate::kernel::FileExtension;
use crate::kernel::io::buf_handler::BufHandler;
use crate::kernel::io::mmap_handler::MMapHandler;
use crate::kernel::Result;
use crate::KvsError;

#[derive(Debug)]
pub struct IOHandlerFactory {
    dir_path: Arc<PathBuf>,
    extension: Arc<FileExtension>,
}

#[derive(PartialEq, Copy, Clone)]
pub enum IOType {
    Buf,
    MMap,
}

impl IOHandlerFactory {
    #[inline]
    pub fn create(&self, gen: i64, io_type: IOType) -> Result<Box<dyn IOHandler>>
    {
        let dir_path = Arc::clone(&self.dir_path);
        let extension = Arc::clone(&self.extension);

        if io_type == IOType::Buf {
            Ok(Box::new(BufHandler::new(dir_path, gen, extension)?))
        } else if io_type == IOType::MMap {
            Ok(Box::new(MMapHandler::new(dir_path, gen, extension)?))
        } else {
            Err(KvsError::NotMatchIO)
        }
    }

    #[inline]
    pub fn new(dir_path: impl Into<PathBuf>, extension: FileExtension) -> Self {
        let dir_path = Arc::new(dir_path.into());
        let extension = Arc::new(extension);

        Self { dir_path, extension }
    }

    #[inline]
    pub fn clean(&self, gen: i64) -> Result<()>{
        fs::remove_file(
            self.extension
                .path_with_gen(&self.dir_path, gen)
        )?;
        Ok(())
    }
}

#[async_trait]
pub trait IOHandler: Send + Sync + 'static {

    fn get_gen(&self) -> i64;

    fn get_path(&self) -> PathBuf;

    fn file_size(&self) -> Result<u64> {
        let path_buf = self.get_path();
        Ok(fs::metadata(path_buf)?.len())
    }

    async fn read_with_pos(&self, start: u64, len: usize) -> Result<Vec<u8>>;

    async fn write(&self, buf: Vec<u8>) -> Result<(u64, usize)>;

    async fn flush(&self) -> Result<()>;

    fn get_type(&self) -> IOType;
}