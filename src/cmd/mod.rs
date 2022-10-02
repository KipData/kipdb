use serde::{Deserialize, Serialize};
use clap::{Subcommand};

#[derive(Serialize, Deserialize, Debug, Subcommand)]
pub enum Command {
    Set { key: String, value: String },
    Remove { key: String },
    Get { key: String },

    #[clap(help = "cli.exe batch-set [keys]... [values]...")]
    BatchSet { batch: Vec<String> },
    #[clap(help = "cli.exe batch-set-parallel [keys]... [values]...")]
    BatchSetParallel { batch: Vec<String> },
    BatchRemove { keys: Vec<String> },
    BatchRemoveParallel { keys: Vec<String> },
    BatchGet { keys: Vec<String> },
    BatchGetParallel { keys: Vec<String> },
    SizeOfDisk,
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

    pub fn batch_set_parallel(batch: Vec<String>) -> Command {
        Command::BatchSetParallel { batch }
    }

    pub fn batch_remove_parallel(keys: Vec<String>) -> Command {
        Command::BatchRemoveParallel { keys }
    }

    pub fn batch_get_parallel(keys: Vec<String>) -> Command {
        Command::BatchGetParallel { keys }
    }
}

