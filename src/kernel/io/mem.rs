use std::io::{Error, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use crate::kernel::io::{IoReader, IoType, IoWriter};

pub(crate) struct MemIoReader {
    gen: i64,
    bytes: Bytes,
    pos: usize,
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
        MemIoReader { gen, bytes, pos: 0 }
    }
}

impl MemIoWriter {
    pub(crate) fn new(bytes: BytesMut) -> Self {
        MemIoWriter {
            inner: Arc::new(Mutex::new(MemIoWriterInner { bytes, pos: 0 })),
        }
    }

    fn _write(buf: &[u8], inner: &mut MemIoWriterInner) -> Result<usize, Error> {
        let len = buf.len();
        inner.bytes.put(buf);
        inner.pos += len as u64;
        Ok(len)
    }

    pub(crate) fn bytes(&self) -> Bytes {
        Bytes::from(self.inner.lock().bytes.to_vec())
    }
}

impl Read for MemIoReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let last_pos = self.pos;
        let len = (&self.bytes[last_pos..]).read(buf)?;
        self.pos += len;
        Ok(len)
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
        Ok(self.bytes.len() as u64)
    }

    fn read_with_pos(&self, start: u64, len: usize) -> crate::kernel::Result<Vec<u8>> {
        let start_pos = start as usize;

        Ok(self.bytes[start_pos..len + start_pos].to_vec())
    }

    fn get_type(&self) -> IoType {
        IoType::Mem
    }

    fn bytes(&self) -> crate::kernel::Result<Vec<u8>> {
        Ok(self.bytes.to_vec())
    }
}


impl Write for MemIoWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.inner.lock();

        Self::_write(buf, &mut inner)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl IoWriter for MemIoWriter {
    fn io_write(&mut self, buf: Vec<u8>) -> crate::kernel::Result<(u64, usize)> {
        let mut inner = self.inner.lock();
        let start_pos = inner.pos;

        Ok(Self::_write(&buf, &mut inner).map(|len| (start_pos, len))?)
    }

    fn io_flush(&mut self) -> crate::kernel::Result<()> {
        Ok(())
    }
}

impl Clone for MemIoWriter {
    fn clone(&self) -> Self {
        MemIoWriter { inner: Arc::clone(&self.inner) }
    }
}