use crate::error::ConnectionError;

use serde::{Deserialize, Serialize};
use crate::kernel::CommandData;

mod connection;
mod codec;
pub mod client;
pub mod server;

pub type Result<T> = std::result::Result<T, ConnectionError>;

/// 用于TCP连接命令交互时的数据封装
#[derive(Serialize,Deserialize)]
pub enum CommandOption {
    Cmd(CommandData),
    Value(Vec<u8>),
    None
}

impl Into<Option<Vec<u8>>> for CommandOption {
    fn into(self) -> Option<Vec<u8>> {
        match self {
            CommandOption::Value(value) => { Some(value) }
            _ => { None }
        }
    }
}