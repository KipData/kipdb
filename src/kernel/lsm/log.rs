use std::cmp::min;
/// dermesser/leveldb-rs crates.io: v1.0.6
/// https://github.com/dermesser/leveldb-rs/blob/master/src/log.rs
/// The MIT License (MIT)

/// A log consists of a number of blocks.
/// A block consists of a number of records and an optional trailer (filler).
/// A record is a bytestring: [checksum: uint32, length: uint16, type: uint8, data: [u8]]
/// checksum is the crc32 sum of type and data; type is one of RecordType::{Full/First/Middle/Last}


use std::io::{Read, Write, Seek, SeekFrom};
use std::mem;
use std::path::Path;
use std::sync::Arc;
use integer_encoding::FixedInt;
use crate::kernel::{Result, sorted_gen_list};
use crate::kernel::io::{FileExtension, IoFactory, IoType, IoWriter};
use crate::kernel::lsm::block::{Entry, Value};
use crate::kernel::lsm::lsm_kv::Gen;
use crate::kernel::lsm::mem_table::KeyValue;
use crate::KernelError;

const BLOCK_SIZE: usize = 32 * 1024;
const HEADER_SIZE: usize = 4 + 4 + 1;

pub(crate) struct LogLoader {
    inner: LogLoaderInner,
    writer: LogWriter<Box<dyn IoWriter>>,
}

#[derive(Clone)]
pub(crate) struct LogLoaderInner {
    factory: Arc<IoFactory>,
    current_gen: i64,
    io_type: IoType,
}

impl LogLoader {
    pub(crate) fn reload(
        wal_dir_path: &Path,
        path_name: &str,
        log_type: IoType
    ) -> Result<(Self, Vec<KeyValue>)> {
        let (loader, last_gen) = Self::reload_(
            wal_dir_path,
            path_name,
            log_type
        )?;
        let reload_data = loader.load(last_gen)?;

        Ok((loader, reload_data))
    }

    fn reload_(
        wal_dir_path: &Path,
        path_name: &str,
        io_type: IoType
    ) -> Result<(Self, i64)> {
        let wal_path = wal_dir_path.join(path_name);

        let factory = Arc::new(
            IoFactory::new(
                wal_path.clone(),
                FileExtension::Log
            )?
        );

        let current_gen = sorted_gen_list(&wal_path, FileExtension::Log)?
            .last()
            .cloned()
            .unwrap_or(Gen::create());
        let writer = LogWriter::new(
            factory.writer(current_gen, io_type)?
        );

        Ok((LogLoader {
            inner: LogLoaderInner {
                factory,
                current_gen,
                io_type,
            },
            writer,
        }, current_gen))
    }

    pub(crate) fn add_record(&mut self, data: KeyValue) -> Result<()> {
        let _ = self.writer.add_record(&Self::data_to_bytes(data)?)?;

        Ok(())
    }

    pub(crate) fn batch_add_record(&mut self, vec_data: Vec<KeyValue>) -> Result<()> {
        for record in vec_data {
            let _ = self.writer.add_record(&Self::data_to_bytes(record)?)?;
        }
        self.writer.flush()?;
        Ok(())
    }

    pub(crate) fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;

        Ok(())
    }

    /// 弹出此日志的Gen并重新以新Gen进行日志记录
    pub(crate) fn new_log(&mut self, next_gen: i64) -> Result<i64> {
        let inner = &mut self.inner;
        let new_fs = inner.factory.writer(next_gen, inner.io_type)?;

        mem::replace(&mut self.writer, LogWriter::new(new_fs)).flush()?;

        Ok(mem::replace(&mut inner.current_gen, next_gen))
    }

    /// 通过Gen载入数据进行读取
    pub(crate) fn load(&self, gen: i64) -> Result<Vec<KeyValue>> {
        Self::_load(&self.inner, gen)
    }

    pub(crate) fn _load(inner: &LogLoaderInner, gen: i64) -> Result<Vec<KeyValue>> {
        let mut reader = LogReader::new(
            inner.factory.reader(gen, inner.io_type)?
        );
        let mut vec_data = Vec::new();
        let mut buf = vec![0; 128];

        while reader.read(&mut buf)? > 0 {
            let Entry { key, item, .. } = Entry::<Value>::decode(&mut buf.as_slice())?;
            vec_data.push((key, item.bytes));
        }

        Ok(vec_data)
    }

    #[allow(dead_code)]
    pub(crate) fn clean(&self, gen: i64) -> Result<()> {
        Self::_clean(&self.inner, gen)
    }

    pub(crate) fn _clean(inner: &LogLoaderInner, gen: i64) -> Result<()> {
        inner.factory.clean(gen)
    }

    pub(crate) fn clone_inner(&self) -> LogLoaderInner {
        self.inner.clone()
    }

    fn data_to_bytes(data: KeyValue) -> Result<Vec<u8>> {
        let (key, value) = data;
        Entry::new(0, key.len(), key, Value::from(value)).encode()
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum RecordType {
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

impl From<u8> for RecordType {
    fn from(value: u8) -> Self {
        match value {
            1 => RecordType::Full,
            2 => RecordType::First,
            3 => RecordType::Middle,
            4 => RecordType::Last,
            _ => panic!("Unknown value: {value}"),
        }
    }
}

pub(crate) struct LogWriter<W: Write> {
    dst: W,
    current_block_offset: usize,
    block_size: usize,
}

impl<W: Write> LogWriter<W> {
    pub(crate) fn new(writer: W) -> LogWriter<W> {
        LogWriter {
            dst: writer,
            current_block_offset: 0,
            block_size: BLOCK_SIZE,
        }
    }

    /// new_with_off opens a writer starting at some offset of an existing log file. The file must
    /// have the default block size.
    #[allow(dead_code)]
    pub(crate) fn new_with_off(writer: W, off: usize) -> LogWriter<W> {
        let mut w = LogWriter::new(writer);
        w.current_block_offset = off % BLOCK_SIZE;
        w
    }

    pub(crate) fn add_record(&mut self, r: &[u8]) -> Result<usize> {
        let mut record = r;
        let mut len = 0;

        while !record.is_empty() {
            assert!(self.block_size > HEADER_SIZE);

            let space_left = self.block_size - self.current_block_offset;

            // Fill up block; go to next block.
            if space_left < HEADER_SIZE {
                if space_left > 0 {
                    self.dst.write_all(&vec![0u8; space_left])?;
                }
                self.current_block_offset = 0;
            }

            let avail_for_data = self.block_size - self.current_block_offset - HEADER_SIZE;
            let data_frag_len = min(record.len(), avail_for_data);
            let first_frag = len == 0;

            let record_type = if first_frag && data_frag_len == record.len() {
                RecordType::Full
            } else if first_frag {
                RecordType::First
            } else if data_frag_len == record.len() {
                RecordType::Last
            } else {
                RecordType::Middle
            };

            len = self.emit_record(record_type, record, data_frag_len)?;
            record = &record[data_frag_len..];
        }

        Ok(len)
    }

    fn emit_record(&mut self, t: RecordType, data: &[u8], len: usize) -> Result<usize> {
        let crc = crc32fast::hash(&data[0..len]);

        let mut s = 0;
        s += self.dst.write(&crc.encode_fixed_vec())?;
        s += self.dst.write(&(len as u32).encode_fixed_vec())?;
        s += self.dst.write(&[t as u8])?;
        s += self.dst.write(&data[0..len])?;

        self.current_block_offset += s;
        Ok(s)
    }

    #[allow(dead_code)]
    pub(crate) fn flush(&mut self) -> Result<()> {
        self.dst.flush()?;
        Ok(())
    }
}

pub(crate) struct LogReader<R: Read + Seek> {
    src: R,
    offset: usize,
    block_size: usize,
    head_scratch: [u8; HEADER_SIZE],
}

impl<R: Read + Seek> LogReader<R> {
    pub(crate) fn new(src: R) -> LogReader<R> {
        LogReader {
            src,
            offset: 0,
            block_size: BLOCK_SIZE,
            head_scratch: [0u8; HEADER_SIZE],
        }
    }

    /// EOF is signalled by Ok(0)
    pub(crate) fn read(&mut self, dst: &mut Vec<u8>) -> Result<usize> {
        let mut dst_offset = 0;
        let mut head_pos = 0;

        dst.clear();

        loop {
            let leftover = self.block_size - self.offset;
            if leftover < HEADER_SIZE {
                // skip to next block
                if leftover != 0 {
                    let _ = self.src.seek(SeekFrom::Current((leftover) as i64))?;
                }
                self.offset = 0;
            }

            head_pos += self.src.read(&mut self.head_scratch[head_pos..])?;
            // EOF
            if head_pos == 0 {
                return Ok(dst_offset);
            } else if head_pos != HEADER_SIZE {
                continue
            } else {
                head_pos = 0;
            }

            self.offset += HEADER_SIZE;

            let crc = u32::decode_fixed(&self.head_scratch[0..4]);
            let length = u32::decode_fixed(&self.head_scratch[4..8]) as usize;

            let mut buf = vec![0; length];

            self.src
                .read_exact(&mut buf)?;
            self.offset += length;
            dst_offset += length;

            if crc32fast::hash(&buf) != crc {
                return Err(KernelError::CrcMisMatch);
            }

            dst.append(&mut buf);

            if let RecordType::Full | RecordType::Last = RecordType::from(self.head_scratch[8]) {
                return Ok(dst_offset);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{File, OpenOptions};
    use std::io::Cursor;
    use bytes::Bytes;
    use tempfile::TempDir;
    use crate::kernel::io::IoType;
    use crate::kernel::lsm::log::{HEADER_SIZE, LogLoader, LogReader, LogWriter};
    use crate::kernel::Result;
    use crate::kernel::lsm::lsm_kv::{Config, Gen};
    use crate::kernel::lsm::mem_table::DEFAULT_WAL_PATH;

    #[test]
    fn test_writer() {
        let data = &[
            "hello world. My first log entry.",
            "and my second",
            "and my third",
        ];
        let mut lw = LogWriter::new(Vec::new());
        let total_len = data.iter().fold(0, |l, d| l + d.len());

        for d in data {
            let _ = lw.add_record(d.as_bytes());
        }

        assert_eq!(lw.current_block_offset, total_len + 3 * HEADER_SIZE);
    }

    #[test]
    fn test_writer_append() {
        let data = &[
            "hello world. My first log entry.",
            "and my second",
            "and my third",
        ];

        let mut dst = Vec::new();
        dst.resize(1024, 0u8);

        {
            let mut lw = LogWriter::new(Cursor::new(dst.as_mut_slice()));
            for d in data {
                let _ = lw.add_record(d.as_bytes());
            }
        }

        let old = dst.clone();

        // Ensure that new_with_off positions the writer correctly. Some ugly mucking about with
        // cursors and stuff is required.
        {
            let offset = data[0].len() + HEADER_SIZE;
            let mut lw =
                LogWriter::new_with_off(Cursor::new(&mut dst.as_mut_slice()[offset..]), offset);
            for d in &data[1..] {
                let _ = lw.add_record(d.as_bytes());
            }
        }
        assert_eq!(old, dst);
    }

    #[test]
    fn test_reader() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let file_path = temp_dir.path().join("test.txt");

        let fs = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(file_path.clone())?;

        let data = vec![
            "abcdefghi".as_bytes().to_vec(),    // fits one block of 17
            "123456789012".as_bytes().to_vec(), // spans two blocks of 17
            "0101010101010101010101".as_bytes().to_vec(),
        ]; // spans three blocks of 17
        let mut lw = LogWriter::new(fs);

        for e in data.iter() {
            assert!(lw.add_record(e).is_ok());
        }

        assert_eq!(lw.dst.metadata()?.len(), 70);

        let mut lr = LogReader::new(File::open(file_path)?);
        let mut dst = Vec::with_capacity(128);

        let mut i = 0;
        loop {
            let r = lr.read(&mut dst);

            if !r.is_ok() {
                panic!("{}", r.unwrap_err());
            } else if r.unwrap() == 0 {
                break;
            }

            assert_eq!(dst, data[i]);
            i += 1;
        }
        assert_eq!(i, data.len());

        Ok(())
    }

    #[test]
    fn test_log_load() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let config = Config::new(temp_dir.into_path());

        let (mut wal, _) = LogLoader::reload(
            config.path(),
            DEFAULT_WAL_PATH,
            IoType::Buf
        )?;

        let data_1 = (Bytes::from_static(b"kip_key_1"), Some(Bytes::from_static(b"kip_value")));
        let data_2 = (Bytes::from_static(b"kip_key_2"), Some(Bytes::from_static(b"kip_value")));

        wal.add_record(data_1.clone())?;
        wal.add_record(data_2.clone())?;

        let gen = wal.new_log(Gen::create())?;

        drop(wal);

        let (wal, _) = LogLoader::reload(
            config.path(),
            DEFAULT_WAL_PATH,
            IoType::Buf
        )?;

        assert_eq!(wal.load(gen)?, vec![data_1, data_2]);

        Ok(())
    }

    #[test]
    fn test_log_reload() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let config = Config::new(temp_dir.into_path());

        let (mut wal_1, _) = LogLoader::reload(
            config.path(),
            DEFAULT_WAL_PATH,
            IoType::Buf
        )?;

        let data_1 = (Bytes::from_static(b"kip_key_1"), Some(Bytes::from_static(b"kip_value")));
        let data_2 = (Bytes::from_static(b"kip_key_2"), Some(Bytes::from_static(b"kip_value")));

        wal_1.add_record(data_1.clone())?;
        wal_1.add_record(data_2.clone())?;

        wal_1.flush()?;
        // wal_1尚未drop时，则开始reload，模拟SUCCESS_FS未删除的情况(即停机异常)，触发数据恢复

        let (_, reload_data) = LogLoader::reload(
            config.path(),
            DEFAULT_WAL_PATH,
            IoType::Buf
        )?;

        assert_eq!(reload_data, vec![data_1, data_2]);

        Ok(())
    }
}


