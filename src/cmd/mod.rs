use serde::{Deserialize, Serialize};
use clap::{Subcommand};

#[derive(Serialize, Deserialize, Debug, Subcommand)]
pub enum Command {
    Set { key: String, value: String },
    Remove { key: String },
    Get { key: String },

    #[clap(help = "cli.exe batch-set ['(key2-value2)']...")]
    BatchSet { batch: Vec<String> },
    BatchRemove { keys: Vec<String> },
    BatchGet { keys: Vec<String> },
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

    pub fn batch_set(batch: Vec<String>) -> Command {
        Command::BatchSet { batch }
    }

    pub fn batch_remove(keys: Vec<String>) -> Command {
        Command::BatchRemove { keys }
    }

    pub fn batch_get(keys: Vec<String>) -> Command {
        Command::BatchGet { keys }
    }
}

