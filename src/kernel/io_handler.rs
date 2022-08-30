use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use crossbeam::queue::ArrayQueue;
use tokio::sync::oneshot;
use tracing::error;
use crate::kernel::{log_path};
use crate::kernel::thread_pool::ThreadPool;
use crate::kernel::Result;

pub(crate) type SingleWriter = Arc<Mutex<BufWriter<File>>>;

pub(crate) struct UniversalReader {
    dir_path: Arc<PathBuf>,
    safe_point: Arc<AtomicU64>,
    readers: RefCell<BTreeMap<u64, BufReader<File>>>
}

pub(crate) struct IOHandler {
    gen: u64,
    dir_path: Arc<PathBuf>,
    reader_pool: Arc<ArrayQueue<UniversalReader>>,
    safe_point: Arc<AtomicU64>,
    writer: SingleWriter,
    thread_pool: Arc<ThreadPool>
}

impl IOHandler {
    pub(crate)  fn new(
        dir_path: impl Into<PathBuf>,
        gen: u64,
        reader_pool: Arc<ArrayQueue<UniversalReader>>,
        thread_pool: Arc<ThreadPool>,
        safe_point: Arc<AtomicU64>
    ) -> Result<Self> {
        let dir_path = Arc::new(dir_path.into());

        let path = log_path(&dir_path, gen);

        // 通过路径构造写入器
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&path)?;

        let writer = Arc::new(Mutex::new(BufWriter::new(file)));

        Ok(Self { gen, dir_path, reader_pool, safe_point, writer, thread_pool })
    }

    pub(crate) async fn get_with_pos(&self, start: u64, len: usize) -> Result<Vec<u8>> {
        let reader_pool = self.reader_pool.clone();
        let thread_pool = self.thread_pool.as_ref();

        let (tx, rx) = oneshot::channel();
        thread_pool.execute(move || {
            let res: Result<Vec<u8>> = (|| {
                let reader = reader_pool.pop().unwrap();

                let vec = reader.read(self.gen, start, len)?;

                reader_pool.push(reader).unwrap();
                Ok(vec)
            })();

            if tx.send(res).is_err() {
                error!("[IOHandler][Get][End is Dropped]");
            }
        });

        Ok(rx.await.unwrap()?)
    }
}

impl UniversalReader {

    pub(crate) fn close_stale_handles(&self) {
        let mut readers = self.readers.borrow_mut();
        while !readers.is_empty() {
            let first_gen = *readers.keys().next().unwrap();
            if self.safe_point.load(Ordering::SeqCst) <= first_gen {
                break;
            }
            readers.remove(&first_gen);
        }
    }

    pub(crate) fn read(&self, gen: u64, start: u64, len: usize) -> Result<Vec<u8>> {
        self.close_stale_handles();

        let mut readers = self.readers.borrow_mut();
        // Open the file if we haven't opened it in this `KvStoreReader`.
        // We don't use entry API here because we want the errors to be propogated.
        if !readers.contains_key(&gen) {
            let reader = BufReader::new(File::open(log_path(&self.dir_path, gen))?);
            readers.insert(gen, reader);
        }
        let reader = readers.get_mut(&gen).unwrap();

        // 使用Vec buffer获取数据
        let mut buffer = Vec::with_capacity(len);
        reader.seek(SeekFrom::Start(start))?;
        reader.read(&mut buffer)?;

        Ok(buffer)
    }
}

impl Clone for UniversalReader {
    fn clone(&self) -> Self {
        UniversalReader {
            dir_path: Arc::clone(&self.dir_path),
            safe_point: Arc::clone(&self.safe_point),
            // don't use other KvStoreReader's readers
            readers: RefCell::new(BTreeMap::new()),
        }
    }
}





