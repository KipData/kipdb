use crate::error::ConnectionError;

use serde::{Deserialize, Serialize};
use crate::core::CommandData;

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