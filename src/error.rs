use std::io;
use failure::Fail;

/// Error type for kvs
#[derive(Fail, Debug)]
pub enum KvsError {
    /// IO error
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),

    /// Serialization or deserialization error
    #[fail(display = "{}", _0)]
    SerdeJson(#[cause] serde_json::Error),
    #[fail(display = "{}", _0)]
    SerdeMPEncode(#[cause] rmp_serde::encode::Error),
    #[fail(display = "{}", _0)]
    SerdeMPDecode(#[cause] rmp_serde::decode::Error),
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
    #[fail(display = "{}", _0)]
    Sled(#[cause] sled::Error),

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

}

#[derive(Fail, Debug)]
pub enum ConnectionError {
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    #[fail(display = "{}", _0)]
    Serde(#[cause] Box<bincode::ErrorKind>),
    #[fail(display = "disconnected")]
    Disconnected,
    #[fail(display = "write failed")]
    WriteFailed,
    #[fail(display = "wrong instruction")]
    WrongInstruction,
    #[fail(display = "{}", _0)]
    SerdeMPEncode(#[cause] rmp_serde::encode::Error),
    #[fail(display = "{}", _0)]
    SerdeMPDecode(#[cause] rmp_serde::decode::Error),
}

impl From<io::Error> for ConnectionError {
    fn from(err: io::Error) -> Self {
        ConnectionError::Io(err)
    }
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> Self {
        KvsError::Io(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> Self {
        KvsError::SerdeJson(err)
    }
}

impl From<Box<bincode::ErrorKind>> for KvsError {
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        KvsError::SerdeBinCode(err)
    }
}

impl From<rmp_serde::encode::Error> for KvsError {
    fn from(err: rmp_serde::encode::Error) -> Self {
        KvsError::SerdeMPEncode(err)
    }
}

impl From<rmp_serde::decode::Error> for KvsError {
    fn from(err: rmp_serde::decode::Error) -> Self {
        KvsError::SerdeMPDecode(err)
    }
}

impl From<rmp_serde::encode::Error> for ConnectionError {
    fn from(err: rmp_serde::encode::Error) -> Self {
        ConnectionError::SerdeMPEncode(err)
    }
}

impl From<rmp_serde::decode::Error> for ConnectionError {
    fn from(err: rmp_serde::decode::Error) -> Self {
        ConnectionError::SerdeMPDecode(err)
    }
}

impl From<Box<bincode::ErrorKind>> for ConnectionError {
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        ConnectionError::Serde(err)
    }
}

impl From<sled::Error> for KvsError {
    fn from(err: sled::Error) -> Self {
        KvsError::Sled(err)
    }
}