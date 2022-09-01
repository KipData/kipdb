use std::cell::RefCell;
use std::collections::HashSet;
use std::collections::btree_map::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use crossbeam::queue::ArrayQueue;
use tempfile::TempDir;
use tokio::sync::{oneshot};
use tokio::sync::oneshot::Receiver;
use tracing::{error, Instrument};
use crate::kernel::{log_path};
use crate::kernel::Result;
use crate::kernel::thread_pool::ThreadPool;

pub(crate) type SingleWriter = Arc<RwLock<BufWriterWithPos<File>>>;

pub(crate) struct UniversalReader {
    dir_path: Arc<PathBuf>,
    version: u64,
    expired_set: Arc<RwLock<HashSet<u64>>>,
    readers: RefCell<BTreeMap<u64, BufReaderWithPos<File>>>
}

pub(crate) struct IOHandlerFactory {
    dir_path: Arc<PathBuf>,
    reader_pool: Arc<ArrayQueue<UniversalReader>>,
    safe_point: Arc<AtomicU64>,
    expired_set: Arc<RwLock<HashSet<u64>>>,
    thread_pool: Arc<ThreadPool>
}

impl IOHandlerFactory {

    pub(crate) fn create(&self, gen: u64) -> Result<IOHandler> {
        let expired_set = Arc::clone(&self.expired_set);
        expired_set.write().unwrap().remove(&gen);

        let dir_path = Arc::clone(&self.dir_path);
        let safe_point = Arc::clone(&self.safe_point);
        let reader_pool = Arc::clone(&self.reader_pool);
        let thread_pool = Arc::clone(&self.thread_pool);

        Ok(IOHandler::new(dir_path,gen, reader_pool, thread_pool, safe_point)?)
    }

    pub fn new(dir_path: impl Into<PathBuf>, readers_size: u64, thread_size: usize) -> Self {

        let dir_path = Arc::new(dir_path.into());

        let safe_point = Arc::new(AtomicU64::new(0));

        let expired_set = Arc::new(RwLock::new(HashSet::new()));

        let thread_pool = Arc::new(ThreadPool::new(thread_size));

        let reader = UniversalReader::new(
            Arc::clone(&dir_path),
            0,
            Arc::clone(&expired_set),
            RefCell::new(BTreeMap::new())
        );

        let reader_pool = Arc::new(ArrayQueue::new(readers_size as usize));

        for _ in 1..readers_size {
            reader_pool.push(reader.clone()).unwrap();
        }
        reader_pool.push(reader).unwrap();

        Self { dir_path, reader_pool, safe_point, expired_set, thread_pool }
    }

    pub(crate) fn clean(&self, io_handler: IOHandler) {
        let safe_point = Arc::clone(&io_handler.safe_point);

        self.expired_set.write().unwrap().insert(io_handler.gen);

        safe_point.fetch_add(1, Ordering::Relaxed);

        drop(io_handler)
    }
}

/// 对应gen文件的IO处理器
///
/// Reader是通用共享的
/// 这是因为可以重分利用共享的Reader资源避免每个IOHandler都占有一个线程池与读取器池
///
/// Writer是私有的
/// 每个文件的写入是阻塞的
pub(crate) struct IOHandler {
    gen: u64,
    dir_path: Arc<PathBuf>,
    safe_point: Arc<AtomicU64>,
    reader_pool: Arc<ArrayQueue<UniversalReader>>,
    writer: SingleWriter,
    thread_pool: Arc<ThreadPool>
}

impl IOHandler {
    pub(crate) fn new(
        dir_path: Arc<PathBuf>,
        gen: u64,
        reader_pool: Arc<ArrayQueue<UniversalReader>>,
        thread_pool: Arc<ThreadPool>,
        safe_point: Arc<AtomicU64>,
    ) -> Result<Self> {
        let path = log_path(&dir_path, gen);

        // 通过路径构造写入器
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&path)?;

        let writer = Arc::new(RwLock::new(BufWriterWithPos::new(file)?));

        Ok(Self { gen, dir_path, reader_pool, safe_point, writer, thread_pool })
    }

    /// 读取执行起始位置的指定长度的二进制数据
    ///
    /// 通过Reader池与线程池进行多线程读取
    pub(crate) async fn read_with_pos(&self, start: u64, len: usize) -> Result<Vec<u8>> {
        let reader_pool = Arc::clone(&self.reader_pool);
        let gen = self.gen;
        let version = self.safe_point.load(Ordering::SeqCst);

        let (tx, rx) = oneshot::channel();
        self.thread_pool.execute(move || {
            let res: Result<Vec<u8>> = (|| {
                let reader = reader_pool.pop().unwrap();

                let vec = reader.read(gen, start, len, version)?;

                reader_pool.push(reader).unwrap();
                Ok(vec)
            })();

            if tx.send(res).is_err() {
                error!("[IOHandler][Get][End is Dropped]");
            }
        });

        Ok(rx.await.unwrap()?)
    }

    pub(crate) fn writer_offset(&self) -> Result<u64> {
        let writer = self.writer.read().unwrap();
        Ok(writer.pos)
    }

    /// 写入并返回结束位置
    pub(crate) async fn write(&self, buf: Vec<u8>) -> Result<usize> {
        let writer = Arc::clone(&self.writer);

        let (tx, rx) = oneshot::channel();
        self.thread_pool.execute(move || {
            let res: Result<usize> = (|| {
                let mut writer = writer.write().unwrap();
                let slice_buf = buf.as_slice();
                writer.write(slice_buf)?;
                Ok(slice_buf.len())
            })();

            if tx.send(res).is_err() {
                error!("[IOHandler][Get][End is Dropped]");
            }
        });

        Ok(rx.await.unwrap()?)
    }

    /// 克隆数据再写入并返回结束位置
    pub(crate) async fn write_with_clone(&self, buf: &[u8]) -> Result<usize> {
        self.write(buf.to_vec()).await
    }

    pub(crate) async fn flush(&self) -> Result<()> {
        let writer = Arc::clone(&self.writer);

        let (tx, rx) = oneshot::channel();
        self.thread_pool.execute(move || {
            let res: Result<()> = (|| {
                let mut writer = writer.write().unwrap();
                writer.flush()?;

                Ok(())
            })();

            if tx.send(res).is_err() {
                error!("[IOHandler][Get][End is Dropped]");
            }
        });

        Ok(rx.await.unwrap()?)
    }
}

impl UniversalReader {

    pub(crate) fn close_stale_handles(&self, version: u64) {
        if !self.version.eq(&version) {
            let mut readers = self.readers.borrow_mut();
            readers.retain(|gen, _| {
                let expired_set = self.expired_set.read().unwrap();
                !expired_set.contains(gen)
            })
        }
    }

    pub(crate) fn read(&self, gen: u64, start: u64, len: usize, version: u64) -> Result<Vec<u8>> {
        self.close_stale_handles(version);

        let mut readers = self.readers.borrow_mut();
        // Open the file if we haven't opened it in this `KvStoreReader`.
        // We don't use entry API here because we want the errors to be propogated.
        if !readers.contains_key(&gen) {
            let reader = BufReaderWithPos::new(File::open(log_path(&self.dir_path, gen))?)?;
            readers.insert(gen, reader);
        }
        let reader = readers.get_mut(&gen).unwrap();


        let mut buffer = vec![0;len];
        // 使用Vec buffer获取数据
        reader.seek(SeekFrom::Start(start))?;
        reader.read(buffer.as_mut_slice())?;

        Ok(buffer)
    }
    pub fn new(dir_path: Arc<PathBuf>, version: u64, expired_set: Arc<RwLock<HashSet<u64>>>, readers: RefCell<BTreeMap<u64, BufReaderWithPos<File>>>) -> Self {
        Self { dir_path, version, expired_set, readers }
    }
}

impl Clone for UniversalReader {
    fn clone(&self) -> Self {
        UniversalReader {
            dir_path: Arc::clone(&self.dir_path),
            expired_set: Arc::clone(&self.expired_set),
            // don't use other KvStoreReader's readers
            readers: RefCell::new(BTreeMap::new()),
            version: 0,
        }
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

#[test]
fn test_io() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    tokio_test::block_on(async move {
        let factory = IOHandlerFactory::new(temp_dir.path(), 10, 10);
        let handler1 = factory.create(1)?;
        let data_write1 = vec![b'1', b'2', b'3'];
        let data_write2 = vec![b'4', b'5', b'6'];
        let pos_1 = handler1.writer_offset()?;
        let pos_w_1 = handler1.write(data_write1).await?;
        let pos_2 = handler1.writer_offset()?;
        let pos_w_2 = handler1.write(data_write2).await?;
        let pos_3 = handler1.writer_offset()?;
        handler1.flush().await?;
        let data_read = handler1.read_with_pos(0, 6).await?;

        assert_eq!(vec![b'1', b'2', b'3',b'4', b'5', b'6'], data_read);
        assert_eq!(pos_1, 0);
        assert_eq!(pos_2, 3);
        assert_eq!(pos_3, 6);
        assert_eq!(pos_w_1, 3);
        assert_eq!(pos_w_2, 3);

        Ok(())
    })
}
