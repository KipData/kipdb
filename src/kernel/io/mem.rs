use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use crate::kernel::io::{IoReader, IoType, IoWriter};

pub(crate) struct MemIoReader {
    gen: i64,
    inner: Cursor<Bytes>
}

pub(crate) struct MemIoWriter {
    inner: Arc<Mutex<MemIoWriterInner>>
}

struct MemIoWriterInner {
    bytes: BytesMut,
    pos: u64
}


impl MemIoReader {
    pub(crate) fn new(gen: i64, bytes: Bytes) -> Self {
        MemIoReader { gen, inner: Cursor::new(bytes) }
    }
}

impl MemIoWriter {
    pub(crate) fn new(bytes: BytesMut) -> Self {
        MemIoWriter {
            inner: Arc::new(Mutex::new(MemIoWriterInner { bytes, pos: 0 })),
        }
    }

    pub(crate) fn bytes(&self) -> Bytes {
        Bytes::from(self.inner.lock().bytes.to_vec())
    }
}

impl Read for MemIoReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl Seek for MemIoReader {
    fn seek(&mut self, seek: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(seek)
    }
}

impl IoReader for MemIoReader {
    fn get_gen(&self) -> i64 {
        self.gen
    }

    fn get_path(&self) -> PathBuf {
        PathBuf::new()
    }

    fn file_size(&self) -> crate::kernel::Result<u64> {
        Ok(self.inner.get_ref().len() as u64)
    }

    fn get_type(&self) -> IoType {
        IoType::Mem
    }
}


impl Write for MemIoWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.inner.lock();

        let len = buf.len();
        inner.bytes.put(buf);
        inner.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl IoWriter for MemIoWriter {
    fn current_pos(&mut self) -> crate::kernel::Result<u64> {
        Ok(self.inner.lock().pos)
    }
}

impl Clone for MemIoWriter {
    fn clone(&self) -> Self {
        MemIoWriter { inner: Arc::clone(&self.inner) }
    }
}