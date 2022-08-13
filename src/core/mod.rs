use std::{io::{Write, self}, path::PathBuf, fs::File};
use std::io::Read;
use serde::{Deserialize, Serialize};
use memmap2::{Mmap, MmapMut};
use tracing::info;

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
    pos: usize
}

impl CommandPackage {
    pub fn new(cmd: Command, pos: usize) -> Self {
        CommandPackage{ cmd, pos }
    }

    pub fn write<W>(&self, wr: &mut W) -> Result<()> where W: Write + ?Sized, {
        serde_json::to_writer(wr, self)?;
        Ok(())
    }

    pub fn form_pos(reader : &MmapReader, start: usize, end: usize) -> Result<CommandPackage> {
        let cmd_p_u8 = reader.read_zone(start, end)?;
        Ok(serde_json::from_slice(cmd_p_u8)?)
    }

    pub fn form_read_to_vec(reader : &mut MmapReader) -> Result<Vec<CommandPackage>>{
        let mut vec = Vec::new();
        for package in serde_json::from_reader::<&mut MmapReader, CommandPackage>(reader) {
            info!("{:?}", package);
            vec.push(package)
        }
        Ok(vec)
    }

    // pub fn from_read()
}