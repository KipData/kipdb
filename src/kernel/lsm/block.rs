use std::cmp::min;
use std::io::{Cursor, Read, Write};
use std::mem;
use bytes::{Buf, BufMut};
use itertools::Itertools;
use lz4::Decoder;
use varuint::{ReadVarint, WriteVarint};
use crate::kernel::{CommandData, Result};
use crate::kernel::utils::lru_cache::ShardingLruCache;
use crate::KvsError;

/// BlockCache类型 使用对应的SSTable的Gen与BlockIndex进行Block索引
pub(crate) type BlockCache = ShardingLruCache<(i64, Index), Block<Value>>;

const DEFAULT_BLOCK_SIZE: usize = 64 * 1024;

/// 不动态决定Restart是因为Restart的范围固定可以做到更简单的Entry二分查询，提高性能
const DEFAULT_RESTART_SIZE: usize = 16;

const CRC_SIZE: usize = 4;

type KeyValue = (Vec<u8>, Option<Vec<u8>>);

#[derive(Debug, PartialEq, Eq, Clone)]
struct Entry<T> {
    unshared_len: usize,
    shared_len: usize,
    key: Vec<u8>,
    item: T
}

impl<T> Entry<T> where T: BlockItem {
    pub(crate) fn new(
        shared_len: usize,
        unshared_len: usize,
        key: Vec<u8>,
        item: T
    ) -> Self {
        Entry {
            unshared_len,
            shared_len,
            key,
            item,
        }
    }

    fn encode(&self) -> Result<Vec<u8>> {
        let mut vector = Cursor::new(vec![0u8; 0]);

        let _ = vector.write_varint(self.unshared_len as u32)?;
        let _ = vector.write_varint(self.shared_len as u32)?;
        let _ = vector.write(&self.key)?;
        let _ = vector.write(&self.item.encode()?);

        Ok(vector.into_inner())
    }

    fn batch_decode(bytes: Vec<u8>) -> Result<Vec<(usize, Self)>> {
        let mut cursor = Cursor::new(bytes);
        let mut vec_entry = Vec::new();
        let mut index = 0;

        while !cursor.is_empty() {
            let unshared_len = ReadVarint::<u32>::read_varint(&mut cursor)? as usize;
            let shared_len = ReadVarint::<u32>::read_varint(&mut cursor)? as usize;

            let mut key = vec![0u8; unshared_len];
            let _ = cursor.read(&mut key)?;

            let item = T::decode(&mut cursor)?;

            vec_entry.push((index, Self {
                unshared_len,
                shared_len,
                key,
                item,
            }));
            index += 1;
        }

        Ok(vec_entry)
    }
}

/// 键值对对应的Value
#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct Value {
    value_len: usize,
    bytes: Option<Vec<u8>>
}

impl From<Option<Vec<u8>>> for Value {
    fn from(bytes: Option<Vec<u8>>) -> Self {
        let value_len = bytes.as_ref()
            .map_or(0, Vec::len);
        Value {
            value_len,
            bytes,
        }
    }
}

/// Block索引
#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct Index {
    offset: usize,
    len: usize,
}


pub(crate) trait BlockItem: Sized {
    /// 由于需要直接连续序列化，因此使用Read进行Bytes读取
    fn decode<T>(reader: &mut T) -> Result<Self> where T: Read;

    fn encode(&self) -> Result<Vec<u8>>;
}

impl BlockItem for Value {
    fn decode<T>(mut reader: &mut T) -> Result<Self> where T: Read {
        let value_len = ReadVarint::<u32>::read_varint(&mut reader)? as usize;
        let bytes = if value_len > 0 {
            let mut value = vec![0u8; value_len];
            let _ = reader.read(&mut value)?;
            Some(value)
        } else { None };

        Ok(Value {
            value_len,
            bytes,
        })
    }

    fn encode(&self) -> Result<Vec<u8>> {
        let mut vector = Cursor::new(vec![0u8; 0]);
        let _ = vector.write_varint(self.value_len as u32)?;
        if let Some(value) = &self.bytes {
            let _ = vector.write(value)?;
        }
        Ok(vector.into_inner())
    }
}

impl BlockItem for Index {
    fn decode<T>(mut reader: &mut T) -> Result<Self> where T: Read {
        let offset = ReadVarint::<u32>::read_varint(&mut reader)? as usize;
        let len = ReadVarint::<u32>::read_varint(&mut reader)? as usize;

        Ok(Index {
            offset,
            len,
        })
    }

    fn encode(&self) -> Result<Vec<u8>> {
        let mut vector = Cursor::new(vec![0u8; 0]);
        let _ = vector.write_varint(self.offset as u32)?;
        let _ = vector.write_varint(self.len as u32)?;

        Ok(vector.into_inner())
    }
}

#[derive(Clone, Copy)]
pub(crate) enum CompressType {
    None,
    LZ4
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct Block<T> {
    vec_entry: Vec<(usize, Entry<T>)>,
}

#[derive(Clone)]
struct BlockOptions {
    block_size: usize,
    compress_type: CompressType,
}

impl BlockOptions {
    fn new() -> Self {
        BlockOptions {
            block_size: DEFAULT_BLOCK_SIZE,
            compress_type: CompressType::None,
        }
    }

    fn block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self
    }

    fn compress_type(mut self, compress_type: CompressType) -> Self {
        self.compress_type = compress_type;
        self
    }
}

pub(crate) struct BlockIndex {
    start: usize,
    offset: usize,
    key_len: usize,
    last_key: Vec<u8>,
}

impl BlockIndex {
    fn new(start: usize, offset: usize, last_key: Vec<u8>) -> Self {
        let key_len = last_key.len();
        BlockIndex {
            start,
            offset,
            key_len,
            last_key,
        }
    }
}

struct BlockBuf {
    bytes_size: usize,
    vec_key_value: Vec<KeyValue>,
}

impl BlockBuf {
    fn new() -> Self {
        BlockBuf {
            bytes_size: 0,
            vec_key_value: Vec::new(),
        }
    }

    fn add(&mut self, key_value: KeyValue) {
        // 断言新插入的键值对的Key大于buf中最后的key
        if let Some(last_key) = self.last_key() {
            assert!(key_value.0.cmp(last_key).is_gt());
        }
        self.bytes_size += key_value_bytes_len(&key_value);
        self.vec_key_value.push(key_value);
    }

    /// 获取最后一个Key
    fn last_key(&self) -> Option<&Vec<u8>> {
        self.vec_key_value
            .last()
            .map(|key_value| key_value.0.as_ref())
    }

    /// 刷新且弹出其缓存的键值对与其中最后的Key
    fn flush(&mut self) -> (Vec<KeyValue>, Option<Vec<u8>>) {
        self.bytes_size = 0;
        let last_key = self.last_key()
            .cloned();
        (mem::replace(
            &mut self.vec_key_value, Vec::new()
        ), last_key)
    }
}

/// Block构建器
///
/// 请注意add时
struct BlockBuilder {
    options: BlockOptions,
    len: usize,
    buf: BlockBuf,
    vec_block: Vec<(Block<Value>, Vec<u8>)>
}

impl From<CommandData> for Option<KeyValue> {
    fn from(value: CommandData) -> Self {
        match value {
            CommandData::Set { key, value } => Some((key, Some(Vec::clone(&value)))),
            CommandData::Remove { key } => Some((key, None)),
            CommandData::Get { .. } => None,
        }
    }
}

/// 获取键值对得到其空间占用数
fn key_value_bytes_len(key_value: &KeyValue) -> usize {
    let (key, value) = key_value;
    key.len() + value.as_ref()
        .map_or(0, Vec::len)
}

impl BlockBuilder {
    pub(crate) fn new(options: BlockOptions) -> Self {
        BlockBuilder {
            options,
            len: 0,
            buf: BlockBuf::new(),
            vec_block: Vec::new(),
        }
    }

    /// 查看已参与构建的键值对数量
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// 插入需要构建为Block的键值对
    ///
    /// 请注意add的键值对需要自行保证key顺序插入,否则可能会出现问题
    pub(crate) fn add<T>(&mut self, into_key_value: T) where T: Into<Option<KeyValue>> {
        if let Some(key_value) = into_key_value.into() {
            self.buf.add(key_value);
            self.len += 1;
        }
        // 超过指定的Block大小后进行Block构建(默认为4K大小)
        if self.is_out_of_byte() {
            self.build_();
        }
    }

    fn is_out_of_byte(&self) -> bool {
        self.buf.bytes_size >= self.options.block_size
    }

    /// 封装用的构建Block方法
    ///
    /// 刷新buf获取其中的所有键值对与其中最大的key进行前缀压缩构建为Block
    fn build_(&mut self) {
        if let (vec_kv, Some(last_key)) = self.buf.flush() {
            let restart = DEFAULT_RESTART_SIZE * self.vec_block.len();
            let vec_sharding_len = Self::sharding_shared_len(&vec_kv);
            let vec_entry = vec_kv.into_iter()
                .enumerate()
                .map(|(index, (key, value))| {
                    let shared_len = if index % DEFAULT_RESTART_SIZE == 0 { 0 } else {
                        vec_sharding_len[index / DEFAULT_RESTART_SIZE]
                    };
                    (restart + index, Entry::new(
                        shared_len,
                        key.len() - shared_len,
                        key[shared_len..].into(),
                        Value::from(value)
                    ))
                })
                .collect_vec();
            self.vec_block.push(
                (Block { vec_entry }, last_key)
            );
        }
    }

    /// 构建多个Block连续序列化组合成的Bytes以及各个Block的所属的BlockIndex
    pub(crate) fn build(mut self) -> (Vec<u8>, Vec<BlockIndex>) {
        self.build_();

        let mut start = 0;
        let mut vec_index = Vec::with_capacity(
            self.vec_block.len()
        );

        (self.vec_block
            .into_iter()
            .flat_map(|(block, last_key)| {
                block.encode(self.options.compress_type)
                    .map(|block_bytes| {
                        let block_bytes_len = block_bytes.len();
                        vec_index.push(
                            BlockIndex::new(start, block_bytes_len, last_key)
                        );
                        start += block_bytes_len;
                        block_bytes
                    })
            })
            .flatten()
            .collect_vec(), vec_index)
    }

    /// 批量以RESTART_SIZE进行shared_len的获取
    fn sharding_shared_len(vec_kv: &Vec<KeyValue>) -> Vec<usize> {
        let mut vec_shared_key = Vec::with_capacity(
            (vec_kv.len() + DEFAULT_RESTART_SIZE - 1) / DEFAULT_RESTART_SIZE
        );
        for (_, group) in &vec_kv.iter()
            .enumerate()
            .group_by(|(i, _)| i / DEFAULT_RESTART_SIZE)
        {
            vec_shared_key.push(
                Self::longest_shared_len(
                    group.map(|(_, item)| item)
                        .collect_vec()
                )
            )
        }
        vec_shared_key
    }

    /// 查询一组KV的Key最长前缀计数
    fn longest_shared_len(sharding: Vec<&KeyValue>) -> usize {
        if sharding.is_empty() {
            return 0
        }
        let mut min_len = usize::MAX;
        for kv in &sharding {
            min_len = min(min_len, kv.0.len());
        }
        let mut low = 0;
        let mut high = min_len;
        while low < high {
            let mid = (high - low + 1) / 2 + low;
            if is_common_prefix(&sharding, mid) {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        return low;

        fn is_common_prefix(sharding: &Vec<&KeyValue>, len: usize) -> bool {
            let first = sharding[0];
            for i in 1..sharding.len() {
                let kv = sharding[i];
                for j in 0..len {
                    if first.0[j] != kv.0[j] {
                        return false
                    }
                }
            }
            true
        }
    }
}

impl Block<Value> {
    /// 通过Key查询对应Value
    pub(crate) fn find(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.vec_entry
            .binary_search_by(|(index, entry)| {
                if entry.shared_len > 0 {
                    // 对有前缀压缩的Key进行前缀拼接
                    let shared_len = entry.shared_len;
                    key[0..shared_len]
                        .cmp(self.shared_key_prefix(*index, shared_len))
                        .then_with(|| key[shared_len..].cmp(&entry.key))
                } else {
                    key.cmp(&entry.key)
                }.reverse()
            })
            .ok()
            .map(|index| {
                self.vec_entry[index].1.item
                    .bytes.clone()
            })
            .flatten())
    }
}

impl<T> Block<T> where T: BlockItem {
    /// 获取该Entry对应的shared_key前缀
    ///
    /// 具体原理是通过被固定的RESTART_SIZE进行前缀压缩的Block，
    /// 通过index获取前方最近的Restart，得到的Key通过shared_len进行截取以此得到shared_key
    fn shared_key_prefix(&self, index: usize, shared_len: usize) -> &[u8] {
        &self.vec_entry[(index - index % DEFAULT_RESTART_SIZE)]
            .1.key[0..shared_len]
    }

    /// 序列化后进行压缩
    ///
    /// 可选LZ4与不压缩
    pub(crate) fn encode(&self, compress_type: CompressType) -> Result<Vec<u8>> {
        let buf = self.to_raw()?;
        Ok(match compress_type {
            CompressType::None => buf,
            CompressType::LZ4 => {
                let mut encoder = lz4::EncoderBuilder::new()
                    .level(4)
                    .build(Vec::with_capacity(buf.len()).writer())?;
                let _ = encoder.write(&buf[..])?;

                let (writer, result) = encoder.finish();
                result?;
                writer.into_inner()
            }
        })
    }

    /// 解压后反序列化
    ///
    /// 与encode对应，进行数据解压操作并反序列化为Block
    pub(crate) fn decode(buf: Vec<u8>, compress_type: CompressType) -> Result<Self> {
        let buf = match compress_type {
            CompressType::None => buf,
            CompressType::LZ4 => {
                let mut decoder = Decoder::new(buf.reader())?;
                let mut decoded = Vec::with_capacity(DEFAULT_BLOCK_SIZE);
                let _ = decoder.read_to_end(&mut decoded)?;
                decoded
            }
        };
        Self::from_raw(buf)
    }

    /// 读取Bytes进行Block的反序列化
    pub(crate) fn from_raw(mut buf: Vec<u8>) -> Result<Self> {
        let date_bytes_len = buf.len() - CRC_SIZE;
        if crc32fast::hash(&buf) == bincode::deserialize::<u32>(
            &buf[date_bytes_len..]
        )? {
            return Err(KvsError::CrcMisMatch)
        }
        buf.truncate(date_bytes_len);
        Ok(Self {
            vec_entry: Entry::<T>::batch_decode(buf)?
        })
    }

    /// 序列化该Block
    ///
    /// 与from_raw对应，序列化时会生成crc_code用于反序列化时校验
    pub(crate) fn to_raw(&self) -> Result<Vec<u8>> {
        let mut bytes_block = Vec::with_capacity(DEFAULT_BLOCK_SIZE);

        bytes_block.append(
            &mut self.vec_entry
                .iter()
                .flat_map(|(_, entry)| entry.encode())
                .flatten()
                .collect_vec()
        );
        let check_crc = crc32fast::hash(&bytes_block);
        bytes_block.append(&mut bincode::serialize(&check_crc)?);

        Ok(bytes_block)
    }
}

#[cfg(test)]
mod tests {
    use bincode::Options;
    use itertools::Itertools;
    use crate::kernel::{CommandData, Result};
    use crate::kernel::lsm::block::{Block, BlockBuilder, BlockOptions, CompressType, Entry, Value};

    #[test]
    fn test_entry_serialization() -> Result<()> {
        let entry1 = Entry::new(0, 1,vec![b'1'], Value::from(Some(vec![b'1'])));
        let entry2 = Entry::new(0, 1,vec![b'1'], Value::from(Some(vec![b'1'])));

        let bytes_vec_entry = entry1.encode()?
            .into_iter()
            .chain(entry2.encode()?)
            .collect_vec();

        let vec_entry = Entry::batch_decode(bytes_vec_entry)?;

        assert_eq!(vec![(0, entry1), (1, entry2)], vec_entry);

        Ok(())
    }

    #[test]
    fn test_block() -> Result<()> {
        let value = b"Let life be beautiful like summer flowers";
        let mut vec_cmd = Vec::new();

        let times = 10;
        let options = BlockOptions::new();
        let mut builder = BlockBuilder::new(options.clone());
        // 默认使用大端序进行序列化，保证顺序正确性
        for i in 0..times {
            let mut key = b"KipDB-".to_vec();
            key.append(
                &mut bincode::options().with_big_endian().serialize(&i)?
            );
            vec_cmd.push(
                CommandData::set(key, value.to_vec()
                )
            );
        }

        for cmd in vec_cmd.iter().cloned() {
            builder.add(cmd);
        }

        //Tip: 注意只取一个作为测试,请勿让times过多
        let (block_bytes, _) = builder.build();

        let block = Block::<Value>::decode(block_bytes, options.compress_type)?;

        for i in 0..times {
            assert_eq!(block.find(vec_cmd[i].get_key()).unwrap(), Some(value.to_vec()))
        }

        test_block_serialization_(block.clone(), CompressType::None)?;
        test_block_serialization_(block.clone(), CompressType::LZ4)?;

        Ok(())
    }

    fn test_block_serialization_(block: Block<Value>, compress_type: CompressType) -> Result<()> {
        let de_block = Block::decode(
            block.encode(compress_type)?, compress_type
        )?;
        assert_eq!(block, de_block);

        Ok(())
    }
}