use std::sync::Arc;
use clap::{Parser};
use itertools::Itertools;
use tracing::{error, info};
use kip_db::cmd::Command;
use kip_db::DEFAULT_PORT;
use kip_db::kernel::CommandData;
use kip_db::net::{Result, client::Client};

#[derive(Parser, Debug)]
#[clap(
name = "KipDB-Cli",
version,
author,
about = "Issue KipDB Commands"
)]
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

    let addr = format!("{}:{}", cli.host, cli.port);

    let mut client = Client::connect(&addr).await?;

    match cli.command {
        Command::Set { key, value } => {
            client.set(encode(&key), encode(&value)).await?;
            info!("Done!");
        }
        Command::Remove { key } => {
            client.remove(encode(&key)).await?;
            info!("Done!");
        }
        Command::Get { key } => {
            if let Some(value) = client.get(encode(&key)).await? {
                info!("\"{}\"", decode(value));
            } else {
                info!("(Nil)");
            }
        }
        Command::BatchSet { batch } => {
            batch_set(&mut client, batch).await?
        }
        Command::BatchRemove { keys } => {
            let vec_batch_rm = keys.into_iter()
                .map(|key|
                    CommandData::Remove {
                        key: encode(&key)
                    }).collect_vec();
            batch_run_and_print(&mut client, vec_batch_rm, "Done!").await?;
        }
        Command::BatchGet { keys } => {
            let vec_batch_get = keys.into_iter()
                .map(|key|
                    CommandData::Get {
                        key: encode(&key)
                    }).collect_vec();
            batch_run_and_print(&mut client, vec_batch_get, "Nil").await?;
        }
        Command::SizeOfDisk => {
            info!("{}", client.size_of_disk().await?);
        }
        Command::Len => {
            info!("{}", client.len().await?);
        }
        Command::Flush => {
            client.flush().await?;
            info!("Done!");
        }
        _ => {}
    }

    Ok(())
}

async fn batch_set(client: &mut Client, batch: Vec<String>) -> Result<()> {
    if batch.len() % 2 == 0 {
        let (keys, values) = batch.split_at(batch.len() / 2);
        let vec_batch_set = keys.iter().zip(values)
            .into_iter()
            .map(|(key, value)| {
                CommandData::Set {
                    key: encode(key),
                    value: Arc::new(encode(value))
                }
            }).collect_vec();
        batch_run_and_print(client, vec_batch_set, "Done!").await?;
    } else {
        error!("BatchSet len is:{}, key-value cannot be aligned", batch.len())
    }

    Ok(())
}

async fn batch_run_and_print(
    client: &mut Client,
    vec_batch: Vec<CommandData>,
    default_null: &str
) -> Result<()> {
    let vec_result = batch_run(client, vec_batch, )
        .await?
        .into_iter()
        .map(|option| match option {
            None => default_null.to_string(),
            Some(value) => value
        })
        .collect_vec();
    info!("{vec_result:?}");

    Ok(())
}

async fn batch_run(client: &mut Client, vec_batch: Vec<CommandData>) -> Result<Vec<Option<String>>> {
    // 需要保留None
    Ok(client.batch(vec_batch).await?
        .into_iter()
        .map(|option_vec_u8: Option<Vec<u8>>| option_vec_u8
            .map_or(None, |bytes| {
                if bytes.is_empty() {
                    None
                } else {
                    Some(decode(bytes))
                }
            }))
        .collect_vec())
}

fn encode(value: &String) -> Vec<u8>{
    bincode::serialize(value).unwrap()
}

fn decode(value: Vec<u8>) -> String {
    bincode::deserialize(value.as_slice()).unwrap()
}