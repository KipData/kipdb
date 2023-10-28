use crate::kernel::lsm::compactor::CompactTask;
use crate::kernel::lsm::version::cleaner::CleanTag;
use std::io;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::error::RecvError;

/// Error type for kvs
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum KernelError {
    /// IO error
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    RecvError(#[from] RecvError),

    #[error("Failed to send compact task")]
    SendCompactTaskError(#[from] SendError<CompactTask>),

    #[error("Failed to send clean tag")]
    SendCleanTagError(#[from] SendError<CleanTag>),

    /// Serialization or deserialization error
    #[error(transparent)]
    SerdeBinCode(#[from] Box<bincode::ErrorKind>),

    /// Remove no-existent key error
    #[error("Key not found")]
    KeyNotFound,

    #[error("Data is empty")]
    DataEmpty,

    #[error("Max Level is 7")]
    LevelOver,

    #[error("Not the correct type of Cmd")]
    NotMatchCmd,

    #[error("CRC code does not match")]
    CrcMisMatch,

    #[cfg(feature = "sled")]
    #[error(transparent)]
    SledErr(#[from] sled::Error),

    #[cfg(feature = "rocksdb")]
    #[error(transparent)]
    RocksdbErr(#[from] rocksdb::Error),

    #[error("Cache size overflow")]
    CacheSizeOverFlow,

    #[error("Cache sharding and size overflow")]
    CacheShardingNotAlign,

    #[error("File not found")]
    FileNotFound,

    /// 正常情况wal在内存中存在索引则表示硬盘中存在有对应的数据
    /// 而错误则是内存存在索引却在硬盘中不存在这个数据
    #[error("WAL log load error")]
    WalLoad,

    /// Unexpected command type error.
    /// It indicated a corrupted log or a program bug.
    #[error("Unexpected command type")]
    UnexpectedCommandType,

    #[error("Process already exists")]
    ProcessExists,

    #[error("channel is closed")]
    ChannelClose,

    #[error("{0}")]
    NotSupport(&'static str),

    #[error("The number of caches cannot be divisible by the number of shards")]
    ShardingNotAlign,
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ConnectionError {
    #[error(transparent)]
    IO(#[from] io::Error),

    #[error("disconnected")]
    Disconnected,

    #[error("write failed")]
    WriteFailed,

    #[error("wrong instruction")]
    WrongInstruction,

    #[error("encode error")]
    EncodeErr,

    #[error("decode error")]
    DecodeErr,

    #[error("server flush error")]
    FlushError,

    #[error("Failed to connect to server, {0}")]
    TonicTransportErr(#[from] tonic::transport::Error),

    #[error("Failed to call server, {0}")]
    TonicFailureStatus(#[from] tonic::Status),

    #[error("Failed to parse addr, {0}")]
    AddrParseError(#[from] std::net::AddrParseError),

    #[error(transparent)]
    KernelError(#[from] KernelError),
}
