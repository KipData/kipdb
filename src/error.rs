use std::io;
use failure::Fail;
use tokio::sync::oneshot::error::RecvError;

/// Error type for kvs
#[derive(Fail, Debug)]
#[non_exhaustive]
pub enum KernelError {
    /// IO error
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    #[fail(display = "{}", _0)]
    Recv(#[cause] RecvError),

    /// Serialization or deserialization error
    #[fail(display = "{}", _0)]
    SerdeBinCode(#[cause] Box<bincode::ErrorKind>),
    /// Remove no-existent key error
    #[fail(display = "Key not found")]
    KeyNotFound,
    #[fail(display = "Data is empty")]
    DataEmpty,
    #[fail(display = "Max Level is 7")]
    LevelOver,
    #[fail(display = "Not the correct type of Cmd")]
    NotMatchCmd,
    #[fail(display = "CRC code does not match")]
    CrcMisMatch,
    #[fail(display = "{}", _0)]
    Sled(#[cause] sled::Error),
    #[fail(display = "Cache size overflow")]
    CacheSizeOverFlow,
    #[fail(display = "Cache sharding and size overflow")]
    CacheShardingNotAlign,
    #[fail(display = "File not found")]
    FileNotFound,
    /// 正常情况wal在内存中存在索引则表示硬盘中存在有对应的数据
    /// 而错误则是内存存在索引却在硬盘中不存在这个数据
    #[fail(display = "WAL log load error")]
    WalLoadError,
    #[fail(display = "Could not found the SSTable")]
    SSTableLostError,
    /// Unexpected command type error.
    /// It indicated a corrupted log or a program bug.
    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,
    #[fail(display = "Process already exists")]
    ProcessExistsError,
    #[fail(display = "数据索引越界")]
    OutOfBounds,
}

#[derive(Fail, Debug)]
#[non_exhaustive]
pub enum ConnectionError {
    #[fail(display = "{}", _0)]
    IO(#[cause] io::Error),
    #[fail(display = "disconnected")]
    Disconnected,
    #[fail(display = "write failed")]
    WriteFailed,
    #[fail(display = "wrong instruction")]
    WrongInstruction,
    #[fail(display = "encode error")]
    EncodeError,
    #[fail(display = "decode error")]
    DecodeError,
    #[fail(display = "server flush error")]
    FlushError,
    #[fail(display = "{}", _0)]
    KvStoreError(#[cause] KernelError),
}

#[derive(Fail, Debug)]
#[non_exhaustive]
#[allow(missing_copy_implementations)]
pub enum CacheError {
    #[fail(display = "The number of caches cannot be divisible by the number of shards")]
    ShardingNotAlign,
    #[fail(display = "Cache size overflow")]
    CacheSizeOverFlow,
    #[fail(display = "{}", _0)]
    KvStoreError(#[cause] KernelError),
}

impl From<io::Error> for ConnectionError {
    #[inline]
    fn from(err: io::Error) -> Self {
        ConnectionError::IO(err)
    }
}

impl From<io::Error> for KernelError {
    #[inline]
    fn from(err: io::Error) -> Self {
        KernelError::Io(err)
    }
}

impl From<RecvError> for KernelError {
    #[inline]
    fn from(err: RecvError) -> Self {
        KernelError::Recv(err)
    }
}

impl From<Box<bincode::ErrorKind>> for KernelError {
    #[inline]
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        KernelError::SerdeBinCode(err)
    }
}

impl From<sled::Error> for KernelError {
    #[inline]
    fn from(err: sled::Error) -> Self {
        KernelError::Sled(err)
    }
}

impl From<KernelError> for ConnectionError {
    #[inline]
    fn from(err: KernelError) -> Self {
        ConnectionError::KvStoreError(err)
    }
}

impl From<CacheError> for KernelError {
    #[inline]
    fn from(value: CacheError) -> Self {
        match value {
            CacheError::KvStoreError(kv_error) => kv_error,
            CacheError::CacheSizeOverFlow => KernelError::CacheSizeOverFlow,
            CacheError::ShardingNotAlign => KernelError::CacheShardingNotAlign,
        }
    }
}

impl From<KernelError> for CacheError {
    #[inline]
    fn from(value: KernelError) -> Self {
        CacheError::KvStoreError(value)
    }
}