use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use parking_lot::Mutex;
use crate::kernel::io::{FileExtension, IoReader, IoType, IoWriter};
use crate::kernel::Result;

#[derive(Debug)]
pub(crate) struct DirectIoHandler {
    gen: i64,
    dir_path: Arc<PathBuf>,
    fs: Mutex<File>,
    extension: Arc<FileExtension>,
}

impl DirectIoHandler {
    pub(crate) fn new(
        dir_path: Arc<PathBuf>,
        gen: i64,
        extension: Arc<FileExtension>,
        is_writer: bool
    ) -> Result<Self> {
        let path = extension.path_with_gen(&dir_path, gen);

        let fs = Mutex::new(if !is_writer { File::open(path)? } else {
            OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(&path)?
        });

        Ok(DirectIoHandler {
            gen,
            dir_path,
            fs,
            extension,
        })
    }
}

impl IoReader for DirectIoHandler {
    fn get_gen(&self) -> i64 {
        self.gen
    }

    fn get_path(&self) -> PathBuf {
        self.extension
            .path_with_gen(&self.dir_path, self.gen)
    }

    fn read_with_pos(&self, start: u64, len: usize) -> Result<Vec<u8>> {
        let mut reader = self.fs.lock();

        let mut buffer = vec![0; len];
        // 使用Vec buffer获取数据
        let _ignore = reader.seek(SeekFrom::Start(start))?;
        let _ignore1 = reader.read(buffer.as_mut_slice())?;

        Ok(buffer)
    }

    fn get_type(&self) -> IoType {
        IoType::Direct
    }
}

impl IoWriter for DirectIoHandler {
    fn get_gen(&self) -> i64 {
        self.gen
    }

    fn get_path(&self) -> PathBuf {
        self.extension
            .path_with_gen(&self.dir_path, self.gen)
    }

    fn write(&self, buf: Vec<u8>) -> Result<(u64, usize)> {
        let mut fs = self.fs.lock();

        let start_pos = fs.stream_position()?;
        let slice_buf = buf.as_slice();
        let _ignore = fs.write(slice_buf)?;

        Ok((start_pos, slice_buf.len()))
    }

    fn flush(&self) -> Result<()> {
        self.fs.lock()
            .flush()?;
        Ok(())
    }

    fn get_type(&self) -> IoType {
        IoType::Direct
    }
}