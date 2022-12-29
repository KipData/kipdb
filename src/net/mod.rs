use crate::error::ConnectionError;

use serde::{Deserialize, Serialize};
use crate::kernel::CommandData;

mod connection;
mod codec;
pub mod client;
pub mod server;
mod shutdown;

pub type Result<T> = std::result::Result<T, ConnectionError>;

/// 用于TCP连接命令交互时的数据封装
#[derive(Serialize, Deserialize, Debug)]
#[non_exhaustive]
pub enum CommandOption {
    Cmd(CommandData),
    VecCmd(Vec<CommandData>, bool),
    Value(Vec<u8>),
    ValueVec(Vec<Option<Vec<u8>>>),
    SizeOfDisk(u64),
    Len(usize),
    Flush,
    None
}

impl From<CommandOption> for Option<Vec<u8>> {
    #[inline]
    fn from(value: CommandOption) -> Self {
        match value {
            CommandOption::Value(value) => { Some(value) }
            _ => { None }
        }
    }
}