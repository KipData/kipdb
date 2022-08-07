use std::sync::Arc;
use serde::{Deserialize, Serialize};
use clap::{Subcommand};
use tokio::sync::RwLock;
use crate::KvStore;
use crate::core::Result;
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

    pub async fn apply(self, kv_store: &mut Arc<RwLock<KvStore>>) -> Result<CommandOption>{
        match self {
            Command::Set { key, value } => {
                let mut write_guard = kv_store.write().await;
                match write_guard.set(key, value).await {
                    Ok(_) => Ok(CommandOption::None),
                    Err(e) => Err(e)
                }
            }
            Command::Remove { key } => {
                let mut write_guard = kv_store.write().await;
                match write_guard.remove(key).await {
                    Ok(_) => Ok(CommandOption::None),
                    Err(e) => Err(e)
                }
            }
            Command::Get { key } => {
                let read_guard = kv_store.read().await;
                match read_guard.get(key).await {
                    Ok(option) => {
                        Ok(CommandOption::from(option))
                    }
                    Err(e) => Err(e)
                }
            }
        }
    }
}

impl From<Option<String>> for CommandOption {
    fn from(item: Option<String>) -> Self {
        match item {
            None => CommandOption::None,
            Some(str) => CommandOption::Value(str)
        }
    }
}