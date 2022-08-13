use std::io;
use bincode::ErrorKind;
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
    /// Remove no-existent key error
    #[fail(display = "Key not found")]
    KeyNotFound,

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
    Serde(#[cause] Box<ErrorKind>),
    #[fail(display = "disconnected")]
    Disconnected,
    #[fail(display = "write failed")]
    WriteFailed,
    #[fail(display = "wrong instruction")]
    WrongInstruction
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

impl From<Box<ErrorKind>> for ConnectionError {
    fn from(err: Box<ErrorKind>) -> Self {
        ConnectionError::Serde(err)
    }
}