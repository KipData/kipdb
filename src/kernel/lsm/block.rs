use std::cmp::min;
use std::io::{Cursor, Read, Write};
use std::sync::Arc;
use bytes::{Buf, BufMut};
use itertools::Itertools;
use lz4::Decoder;
use serde::{Deserialize, Serialize};
use varuint::{ReadVarint, WriteVarint};
use crate::kernel::{CommandData, Result};

const DEFAULT_BLOCK_SIZE: usize = 64 * 1024;

/// TODO: 不使用固定枚举值进行前缀查找, 而是动态查找合适的restart
const DEFAULT_RESTART_SIZE: usize = 16;

const BLOCK_INFO_SIZE: usize = 8;

#[derive(Debug, PartialEq, Eq, Clone)]
struct Entry {
    unshared_len: u32,
    shared_len: u32,
    value_len: u32,
    key: Vec<u8>,
    value: Arc<Vec<u8>>,
}

impl Entry {
    pub(crate) fn new(
        shared_len: u32,
        unshared_len: u32,
        key: Vec<u8>,
        value: Arc<Vec<u8>>
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

            let mut key = vec![0u8; unshared_len as usize];
            let mut value = vec![0u8; value_len as usize];

            let _ignore = cursor.read(&mut key)?;
            let _ignore1 = cursor.read(&mut value)?;

            vec_entry.push(Self {
                unshared_len,
                shared_len,
                value_len,
                key,
                value: Arc::new(value),
            });
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
    vec_restart: Vec<usize>,
}

/// 由一组CommandData转换成一个Block
///
/// 注意：value需要传入前保证按Key有序
impl From<Vec<CommandData>> for Block {
    fn from(value: Vec<CommandData>) -> Self {
        let mut vec_restart = Vec::with_capacity(
            (value.len() + DEFAULT_RESTART_SIZE - 1) / DEFAULT_RESTART_SIZE
        );
        let none_value = Arc::new(vec![]);

        let vec_entry = value.into_iter()
            .filter_map(|cmd| {
                match cmd {
                    CommandData::Set { key, value } => Some((key, value)),
                    CommandData::Remove { key } => Some((key, Arc::clone(&none_value))),
                    CommandData::Get { .. } => None,
                }
            })
            .enumerate()
            // 使用DEFAULT_RESTART_SIZE进行求余以进行对应数量的分组
            .group_by(|(i, _)| i / DEFAULT_RESTART_SIZE)
            .into_iter()
            .enumerate()
            .map(|(i, (_, group))| {
                // group只能用一次有点坑爹
                let sharding = group
                    .map(|(_, kv)| kv)
                    .collect_vec();
                // 找出该组尽可能长的前缀
                let common_shared_len = Self::longest_shared_len(&sharding);
                // 填充restart到vec_restart中
                vec_restart.push(i * DEFAULT_RESTART_SIZE);
                // 通过common_shared_len进行key压缩
                // Tips: 跳过第一条数据
                sharding.into_iter()
                    .enumerate()
                    .map(|(j, (key, value))| {
                        if common_shared_len > 1 {
                            let _w = 1;
                        }
                        let shared_len = if j == 0 { 0 } else { common_shared_len };
                        Entry::new(
                            shared_len as u32,
                            (key.len() - shared_len) as u32,
                            key[shared_len..].into(),
                            value
                        )
                    })
                    .collect_vec()
            })
            .flatten()
            .collect_vec();

        Block {
            vec_entry,
            vec_restart,
        }
    }
}

impl Block {
    /// 通过Key查询对应Value
    pub(crate) fn find(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>> {
        let index = match self.vec_restart
            .binary_search_by(|restart| {
                self.vec_entry[*restart].key
                    .cmp(key)
            })
        {
            Ok(i) => i,
            Err(i) => i - 1,
        } * DEFAULT_RESTART_SIZE;
        // 先使用二分查询找到对应Restart区间
        Ok(self.vec_entry[index..min(index + DEFAULT_RESTART_SIZE, self.vec_entry.len())]
            .binary_search_by(|entry| {
                entry.key.as_slice()
                    .cmp(&key[entry.shared_len as usize..])
            })
            .ok()
            .map(|i| Vec::clone(&self.vec_entry[i].value)))
    }

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

    /// 查询一组KV的Key最长前缀计数
    fn longest_shared_len(sharding: &Vec<(Vec<u8>, Arc<Vec<u8>>)>) -> usize {
        let mut shared_len = 0;
        for i in 0..sharding[0].0.len() {
            for s in sharding.iter().map(|(key, _)| key) {
                if s.len() == i || sharding[0].0[i] != s[i] {
                    return shared_len;
                }
            }
            shared_len += 1;
        }
        shared_len
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use bincode::Options;
    use itertools::Itertools;
    use crate::kernel::{CommandData, Result};
    use crate::kernel::lsm::block::{Block, BLOCK_INFO_SIZE, BlockInfo, CompressType, Entry};

    #[test]
    fn test_block_info() -> Result<()> {
        let info = BlockInfo {
            restart: 0,
            check_crc: 0,
        };

        assert_eq!(bincode::options().with_big_endian().serialize(&info)?.len(), BLOCK_INFO_SIZE);

        Ok(())
    }

    #[test]
    fn test_entry_serialization() -> Result<()> {
        let entry1 = Entry::new(0, 1,vec![b'1'], Arc::new(vec![b'1']));
        let entry2 = Entry::new(0, 1,vec![b'1'], Arc::new(vec![b'1']));

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
    fn test_block() -> Result<()> {
        let value = b"Let life be beautiful like summer flowers";
        let mut vec_cmd = Vec::new();

        let times = 1000;

        // 默认使用大端序进行序列化，保证顺序正确性
        for i in 0..times {
            vec_cmd.push(
                CommandData::set(bincode::options().with_big_endian().serialize(&i)?, value.to_vec())
            );
        }
        let block = Block::from(vec_cmd.clone());

        for i in 0..times {
            assert_eq!(block.find(vec_cmd[i].get_key()).unwrap(), Some(value.to_vec()))
        }

        test_block_serialization_(block.clone(), CompressType::None)?;
        test_block_serialization_(block.clone(), CompressType::LZ4)?;

        Ok(())
    }

    fn test_block_serialization_(block: Block, compress_type: CompressType) -> Result<()> {
        let de_block = Block::decode(
            block.encode(compress_type)?, compress_type
        )?;
        assert_eq!(block, de_block);

        Ok(())
    }
}