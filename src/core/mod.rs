use std::{io::{Write, self}, path::PathBuf, fs::File};
use std::io::Read;
use serde::{Deserialize, Serialize};
use memmap2::{Mmap, MmapMut};
use serde_json::Deserializer;

use crate::cmd::Command;

use crate::KvsError;

pub mod hash_kv;

pub type Result<T> = std::result::Result<T, KvsError>;

pub trait KVStore {
    fn open(path: impl Into<PathBuf>) -> Result<Self> where Self:Sized ;

    fn flush(&mut self) -> Result<()>;

    fn set(&mut self, key: String, value: String) -> Result<()>;

    fn get(&self, key: String) -> Result<Option<String>>;

    fn remove(&mut self, key: String) -> Result<()>;

    fn shut_down(&mut self) ->Result<()>;
}

#[derive(Debug)]
struct CommandPos {
    gen: u64,
    pos: usize,
    len: u64,
}

struct MmapReader {
    mmap: Mmap,
    pos: usize
}

impl MmapReader {

    fn read_zone(&self, start: usize, end: usize) -> Result<&[u8]> {
        Ok(&self.mmap[start..end])
    }

    fn new(file: &File) -> Result<MmapReader> {
        let mmap = unsafe{ Mmap::map(file) }?;
        Ok(MmapReader{
            mmap,
            pos: 0
        })
    }
}

impl Read for MmapReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let last_pos = self.pos;
        let len = (&self.mmap[last_pos..]).read(buf)?;
        self.pos += len;
        Ok(len)
    }
}

struct MmapWriter {
    mmap_mut: MmapMut,
    pos: u64
}

impl MmapWriter {

    fn new(file: &File) -> Result<MmapWriter> {
        let mmap_mut = unsafe {
            MmapMut::map_mut(file)?
        };
        Ok(MmapWriter{
            pos: 0,
            mmap_mut
        })
    }
}

impl Write for MmapWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let last_pos = self.pos as usize;
        let len = (&mut self.mmap_mut[last_pos..]).write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.mmap_mut.flush()?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct CommandPackage {
    cmd: Command,
    pos: usize,
    len: usize
}

impl CommandPackage {
    pub fn new(cmd: Command, pos: usize, len: usize) -> Self {
        CommandPackage{ cmd, pos, len }
    }

    pub fn write<W>(wr: &mut W, cmd: &Command) -> Result<()> where W: Write + ?Sized, {
        serde_json::to_writer(wr, cmd)?;
        Ok(())
    }

    pub fn form_pos(reader : &MmapReader, start: usize, end: usize) -> Result<CommandPackage> {
        let cmd_u8 = reader.read_zone(start, end)?;
        let cmd: Command = serde_json::from_slice(cmd_u8)?;
        Ok(CommandPackage::new(cmd, start, end - start))
    }

    pub fn form_read_to_vec(reader : &mut MmapReader) -> Result<Vec<CommandPackage>>{
        // 将读入器的地址初始化为0
        reader.pos = 0;
        let mut vec = Vec::new();
        let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
        let mut pos = 0;
        while let Some(cmd) = stream.next() {
            let len = stream.byte_offset() - pos;
            if let Ok(cmd) = cmd {
                vec.push(CommandPackage::new(cmd, pos, len));
            } else {
                break;
            }
            pos += len;
        }
        Ok(vec)
    }
}