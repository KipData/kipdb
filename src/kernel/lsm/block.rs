use std::io::{Cursor, Read, Write};
use bytes::{Buf, BufMut};
use itertools::Itertools;
use lz4::Decoder;
use serde::{Deserialize, Serialize};
use varuint::{ReadVarint, WriteVarint};
use crate::kernel::Result;

const DEFAULT_BLOCK_SIZE: usize = 64 * 1024;

const BLOCK_INFO_SIZE: usize = 8;

#[derive(Debug, PartialEq, Eq, Clone)]
struct Entry {
    unshared_len: u32,
    shared_len: u32,
    value_len: u32,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Entry {
    pub(crate) fn new(
        shared_len: u32,
        unshared_len: u32,
        key: Vec<u8>,
        value: Vec<u8>
    ) -> Self {
        Entry {
            unshared_len,
            shared_len,
            value_len: value.len() as u32,
            key,
            value,
        }
    }

    fn encode(&self) -> Result<Vec<u8>> {
        let mut vector = Cursor::new(vec![0u8; 0]);

        let _ignore = vector.write_varint(self.unshared_len)?;
        let _ignore1 = vector.write_varint(self.shared_len)?;
        let _ignore2 = vector.write_varint(self.value_len)?;
        let _ignore3 = vector.write(&self.key)?;
        let _ignore4 = vector.write(&self.value)?;

        Ok(vector.into_inner())
    }

    fn decode(bytes: Vec<u8>, end_pos: u64) -> Result<Vec<Self>> {
        let mut cursor = Cursor::new(bytes);
        cursor.set_position(0);
        let mut vec_entry = Vec::new();

        while cursor.position() < end_pos {
            let unshared_len = cursor.read_varint()?;
            let shared_len = cursor.read_varint()?;
            let value_len = cursor.read_varint()?;

            let mut key = vec![0u8; (unshared_len + shared_len) as usize];
            let mut value = vec![0u8; value_len as usize];

            let _ignore = cursor.read(&mut key)?;
            let _ignore1 = cursor.read(&mut value)?;

            vec_entry.push(Self {
                unshared_len,
                shared_len,
                value_len,
                key,
                value,
            })
        }

        Ok(vec_entry)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
struct BlockInfo {
    restart: u32,
    check_crc: u32,
}

#[derive(Clone, Copy)]
enum CompressType {
    None,
    LZ4
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct Block {
    vec_entry: Vec<Entry>,
    vec_restart: Vec<u32>,
}

impl Block {
    pub(crate) fn encode(&self, compress_type: CompressType) -> Result<Vec<u8>> {
        let buf = self.to_raw()?;
        Ok(match compress_type {
            CompressType::None => buf,
            CompressType::LZ4 => {
                let mut encoder = lz4::EncoderBuilder::new()
                    .level(4)
                    .build(Vec::with_capacity(buf.len()).writer())?;
                let _ignore = encoder.write(&buf[..])?;

                let (writer, result) = encoder.finish();
                result?;
                writer.into_inner()
            }
        })
    }

    pub(crate) fn decode(buf: Vec<u8>, compress_type: CompressType) -> Result<Self> {
        let buf = match compress_type {
            CompressType::None => buf,
            CompressType::LZ4 => {
                let mut decoder = Decoder::new(buf.reader())?;
                let mut decoded = Vec::with_capacity(DEFAULT_BLOCK_SIZE);
                let _ignore = decoder.read_to_end(&mut decoded)?;
                decoded
            }
        };
        Self::from_raw(buf)
    }

    pub(crate) fn from_raw(buf: Vec<u8>) -> Result<Self> {
        let info = bincode::deserialize::<BlockInfo>(
            &buf[buf.len() - BLOCK_INFO_SIZE..]
        )?;
        let vec_restart = bincode::deserialize(
            &buf[info.restart as usize ..]
        )?;
        let vec_entry = Entry::decode(
            buf, info.restart as u64
        )?;
        Ok(Self {
            vec_entry,
            vec_restart,
        })
    }

    pub(crate) fn to_raw(&self) -> Result<Vec<u8>> {
        let mut bytes_block = Vec::with_capacity(DEFAULT_BLOCK_SIZE);

        bytes_block.append(
            &mut self.vec_entry
                .iter()
                .flat_map(Entry::encode)
                .flatten()
                .collect_vec()
        );

        let restart = bytes_block.len() as u32;
        bytes_block.append(&mut bincode::serialize(&self.vec_restart)?);

        let check_crc = crc32fast::hash(bytes_block.as_slice());
        let info = BlockInfo { restart, check_crc };
        bytes_block.append(&mut bincode::serialize(&info)?);

        Ok(bytes_block)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use crate::kernel::Result;
    use crate::kernel::lsm::block::{Block, BLOCK_INFO_SIZE, BlockInfo, CompressType, Entry};

    #[test]
    fn test_block_info() -> Result<()> {
        let info = BlockInfo {
            restart: 0,
            check_crc: 0,
        };

        assert_eq!(bincode::serialize(&info)?.len(), BLOCK_INFO_SIZE);

        Ok(())
    }

    #[test]
    fn test_entry_serialization() -> Result<()> {
        let entry1 = Entry::new(0, 1,vec![b'1'], vec![b'1']);
        let entry2 = Entry::new(0, 1,vec![b'1'], vec![b'1']);

        let bytes_vec_entry = entry1.encode()?
            .into_iter()
            .chain(entry2.encode()?)
            .collect_vec();

        let end_pos = (bytes_vec_entry.len() - 1) as u64;
        let vec_entry = Entry::decode(
            bytes_vec_entry, end_pos
        )?;

        assert_eq!(vec![entry1, entry2], vec_entry);

        Ok(())
    }

    #[test]
    fn test_block_serialization() -> Result<()> {
        let vec_entry = vec![
            Entry::new(0, 1, vec![b'1'], vec![b'1']),
            Entry::new(0, 1, vec![b'1'], vec![b'1'])
        ];
        let vec_restart = vec![0];
        let block = Block {
            vec_entry,
            vec_restart,
        };

        test_block_serialization_(block.clone(), CompressType::None)?;
        test_block_serialization_(block.clone(), CompressType::LZ4)?;

        Ok(())
    }

    fn test_block_serialization_(block: Block, compress_type: CompressType) -> Result<()> {
        let bytes_block = block.encode(compress_type)?;
        let de_block = Block::decode(bytes_block, compress_type)?;

        assert_eq!(block, de_block);

        Ok(())
    }
}