use std::sync::Arc;
use serde::{Deserialize, Serialize};
use clap::{Subcommand};
use tokio::sync::RwLock;
use crate::core::{KVStore, Result};
use crate::net::CommandOption;

#[derive(Serialize, Deserialize, Debug, Subcommand)]
pub enum Command {
    Set { key: String, value: String },
    Remove { key: String },
    Get { key: String }
}

impl Command {
    pub fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }

    pub fn remove(key: String) -> Command {
        Command::Remove { key }
    }

    pub fn get(key: String) -> Command {
        Command::Get { key }
    }

    /// 命令消费
    ///
    /// Command对象通过调用这个方法调用持久化内核进行命令交互
    /// 参数Arc<RwLock<KvStore>>为持久化内核
    /// 内部对该类型进行模式匹配而进行不同命令的相应操作
    pub async fn apply(self, kv_store: &mut Arc<RwLock<dyn KVStore + Send + Sync>>) -> Result<CommandOption>{
        match self {
            Command::Set { key, value } => {
                let mut write_guard = kv_store.write().await;
                match write_guard.set(key, value) {
                    Ok(_) => Ok(CommandOption::None),
                    Err(e) => Err(e)
                }
            }
            Command::Remove { key } => {
                let mut write_guard = kv_store.write().await;
                match write_guard.remove(key) {
                    Ok(_) => Ok(CommandOption::None),
                    Err(e) => Err(e)
                }
            }
            Command::Get { key } => {
                let read_guard = kv_store.read().await;
                match read_guard.get(key) {
                    Ok(option) => {
                        Ok(CommandOption::from(option))
                    }
                    Err(e) => Err(e)
                }
            }
        }
    }
}

/// Option<String>与CommandOption的转换方法
/// 能够与CommandOption::None或CommandOption::Value进行转换
impl From<Option<String>> for CommandOption {
    fn from(item: Option<String>) -> Self {
        match item {
            None => CommandOption::None,
            Some(str) => CommandOption::Value(str)
        }
    }
}