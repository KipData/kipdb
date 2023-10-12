use clap::Parser;
use itertools::Itertools;
use kip_db::cmd::Command;
use kip_db::server::client::KipdbClient;
use kip_db::server::client::Result;
use kip_db::DEFAULT_PORT;
use tracing::{error, info};

const DONE: &str = "Done!";

const UNKNOWN_COMMAND: &str = "Unknown Command!";

#[derive(Parser, Debug)]
#[clap(name = "KipDB-Cli", version, author, about = "Issue KipDB Commands")]
struct Cli {
    #[clap(subcommand)]
    command: Command,

    #[clap(name = "hostname", long, default_value = "127.0.0.1")]
    host: String,

    #[clap(long, default_value_t = DEFAULT_PORT)]
    port: u16,
}

/// Entry point for CLI tool.
///
/// The `[tokio::main]` annotation signals that the Tokio runtime should be
/// started when the function is called. The body of the function is executed
/// within the newly spawned runtime.
///
/// `flavor = "current_thread"` is used here to avoid spawning background
/// threads. The CLI tool use case benefits more by being lighter instead of
/// multi-threaded.
/// 就是说客户端没必要多线程，强制单线程避免产生额外线程
/// 调用方法基本:./kip-db-cli get key1 value1
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // Enable logging
    tracing_subscriber::fmt::try_init().unwrap();
    let cli: Cli = Cli::parse();

    let addr = format!("http://{}:{}", cli.host, cli.port);

    let mut client = KipdbClient::connect(addr).await?;

    let line = match cli.command {
        Command::Set { key, value } => {
            client.set(encode(&key), encode(&value)).await?;
            DONE.to_string()
        }
        Command::Remove { key } => {
            client.remove(encode(&key)).await?;
            DONE.to_string()
        }
        Command::Get { key } => {
            format!("{:?}", client.get(encode(&key)).await?.map(decode))
        }
        Command::BatchSet { batch } => {
            if batch.len() % 2 != 0 {
                error!(
                    "BatchSet len is:{}, key-value cannot be aligned",
                    batch.len()
                )
            }
            let (keys, values) = batch.split_at(batch.len() / 2);
            let kvs = keys
                .iter()
                .zip(values)
                .map(|(key, value)| (encode(key), encode(value)))
                .collect_vec();
            client.batch_set(kvs).await?;
            DONE.to_string()
        }
        Command::BatchRemove { keys } => {
            let keys = keys.into_iter().map(|key| encode(&key)).collect_vec();
            client.batch_remove(keys).await?;
            DONE.to_string()
        }
        Command::BatchGet { keys } => {
            let keys = keys.into_iter().map(|key| encode(&key)).collect_vec();
            format!(
                "{:?}",
                client
                    .batch_get(keys)
                    .await?
                    .into_iter()
                    .map(|value| decode(value))
                    .collect_vec()
            )
        }
        Command::SizeOfDisk => client.size_of_disk().await?.to_string(),
        Command::Len => client.len().await?.to_string(),
        Command::Flush => {
            client.flush().await?;
            DONE.to_string()
        }
        _ => UNKNOWN_COMMAND.to_string(),
    };

    info!("{line}");

    Ok(())
}

fn encode(value: &String) -> Vec<u8> {
    bincode::serialize(value).unwrap()
}

fn decode(value: Vec<u8>) -> String {
    bincode::deserialize(value.as_slice()).unwrap()
}
