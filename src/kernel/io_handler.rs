use std::fs::{File, OpenOptions};
use std::{fs, io};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use crate::kernel::{log_path};
use crate::kernel::Result;

pub(crate) type SyncWriter = RwLock<BufWriterWithPos<File>>;

pub(crate) type SyncReader = Mutex<BufReaderWithPos<File>>;

pub struct IOHandlerFactory {
    dir_path: Arc<PathBuf>
}

impl IOHandlerFactory {

    pub fn create(&self, gen: u64) -> Result<IOHandler> {
        let dir_path = Arc::clone(&self.dir_path);

        Ok(IOHandler::new(dir_path, gen)?)
    }

    pub fn new(dir_path: impl Into<PathBuf>) -> Self {

        let dir_path = Arc::new(dir_path.into());

        Self { dir_path }
    }

    pub fn clean(&self, gen: u64) -> Result<()>{
        fs::remove_file(log_path(&self.dir_path, gen))?;
        Ok(())
    }
}

/// 对应gen文件的IO处理器
///
/// Reader是通用共享的
/// 这是因为可以重分利用共享的Reader资源避免每个IOHandler都占有一个线程池与读取器池
///
/// Writer是私有的
/// 每个文件的写入是阻塞的
pub struct IOHandler {
    gen: u64,
    dir_path: Arc<PathBuf>,
    writer: SyncWriter,
    reader: SyncReader
}

impl IOHandler {

    pub fn new(dir_path: Arc<PathBuf>, gen: u64) -> Result<Self> {
        let path = log_path(&dir_path, gen);

        // 通过路径构造写入器
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&path)?;

        let writer = RwLock::new(BufWriterWithPos::new(file)?);
        let reader = Mutex::new(BufReaderWithPos::new(File::open(path)?)?);

        Ok(Self {
            gen,
            dir_path,
            writer,
            reader
        })
    }

    pub fn get_gen(&self) -> u64 {
        self.gen
    }

    pub fn get_dir_path(&self) -> Arc<PathBuf> {
        Arc::clone(&self.dir_path)
    }

    pub async fn file_size(&self) -> Result<u64> {
        let path = log_path(&self.dir_path, self.gen);
        Ok(fs::metadata(path)?.len())
    }

    /// 使用自身的gen读取执行起始位置的指定长度的二进制数据
    ///
    /// 通过Reader池与线程池进行多线程读取
    pub async fn read_with_pos(&self, start: u64, len: usize) -> Result<Vec<u8>> {
        let mut reader = self.reader.lock().await;

        let mut buffer = vec![0;len];
        // 使用Vec buffer获取数据
        reader.seek(SeekFrom::Start(start))?;
        reader.read(buffer.as_mut_slice())?;

        Ok(buffer)
    }

    /// 写入并返回起始位置与写入长度
    pub async fn write(&self, buf: Vec<u8>) -> Result<(u64, usize)> {
        let mut writer = self.writer.write().await;

        let start_pos = writer.pos;
        let slice_buf = buf.as_slice();
        writer.write(slice_buf)?;

        Ok((start_pos, slice_buf.len()))
    }

    /// 克隆数据再写入并返回起始位置与写入长度
    pub async fn write_with_clone(&self, buf: &[u8]) -> Result<(u64, usize)> {
        self.write(buf.to_vec()).await
    }

    pub async fn write_pos(&self) -> Result<u64> {
        let writer = self.writer.read().await;

        Ok(writer.pos)
    }

    pub async fn flush(&self) -> Result<()> {
        let mut writer = self.writer.write().await;

        Ok(writer.flush()?)
    }
}

pub(crate) struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

pub(crate) struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
