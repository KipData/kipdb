use std::io;

mod connection;
mod codec;
pub mod client;

#[derive(Debug)]
pub enum ConnectionError {
    Io(io::Error),
    Disconnected,
    WriteFailed
}

impl From<io::Error> for ConnectionError {
    fn from(err: io::Error) -> Self {
        ConnectionError::Io(err)
    }
}

type Result<T> = std::result::Result<T, ConnectionError>;