use serde::{Deserialize, Serialize};
use clap::Subcommand;

#[derive(Serialize, Deserialize, Debug, Subcommand)]
#[non_exhaustive]
pub enum Command {
    Set { key: String, value: String },
    Remove { key: String },
    Get { key: String },
    Flush,

    #[clap(help = "cli.exe batch-set [keys]... [values]...")]
    BatchSet { batch: Vec<String> },
    BatchRemove { keys: Vec<String> },
    BatchGet { keys: Vec<String> },
    SizeOfDisk,
    Len
}

impl Command {
    #[inline]
    pub fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }

    #[inline]
    pub fn remove(key: String) -> Command {
        Command::Remove { key }
    }

    #[inline]
    pub fn get(key: String) -> Command {
        Command::Get { key }
    }

    #[inline]
    pub fn flush() -> Command {
        Command::Flush
    }

    #[inline]
    pub fn batch_set(batch: Vec<String>) -> Command {
        Command::BatchSet { batch }
    }

    #[inline]
    pub fn batch_remove(keys: Vec<String>) -> Command {
        Command::BatchRemove { keys }
    }

    #[inline]
    pub fn batch_get(keys: Vec<String>) -> Command {
        Command::BatchGet { keys }
    }
}

