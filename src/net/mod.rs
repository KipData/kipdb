use crate::cmd::Command;
use crate::error::ConnectionError;

use serde::{Deserialize, Serialize};

mod connection;
mod codec;
pub mod client;
pub mod server;

pub type Result<T> = std::result::Result<T, ConnectionError>;

/// 用于TCP连接命令交互时的数据封装
#[derive(Serialize,Deserialize)]
pub enum CommandOption {
    Cmd(Command),
    Value(String),
    None
}