use clap::{Parser};
use itertools::Itertools;
use kip_db::cmd::Command;
use kip_db::DEFAULT_PORT;
use kip_db::kernel::CommandData;
use kip_db::net::{Result, client::Client};

#[derive(Parser, Debug)]
#[clap(
name = "KipDB-Cli",
version,
author,
about = "Issue Redis commands"
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
            client.set(encode(key), encode(value)).await?;
            print!("Done!");
        }
        Command::Remove { key } => {
            client.remove(encode(key)).await?;
            print!("Done!");
        }
        Command::Get { key } => {
            if let Some(value) = client.get(encode(key)).await? {
                print!("\"{}\"", decode(value));
            } else {
                print!("(Nil)");
            }
        }
        Command::BatchSet { batch } => {
            let vec_batch_set = batch.into_iter()
                .map(|cmd_str| {
                    let coords:Vec<&str> = cmd_str.trim_matches(|p| p == '(' || p == ')' )
                        .split('-')
                        .collect();

                    let key = coords[0].parse::<String>().unwrap();
                    let value = coords[1].parse::<String>().unwrap();
                    CommandData::Set {
                        key: encode(key),
                        value: encode(value)
                    }
                }).collect_vec();
            batch_run_and_print(&mut client, vec_batch_set, "Done!").await?;
        }
        Command::BatchRemove { keys } => {
            let vec_batch_rm = keys.into_iter()
                .map(|key|
                    CommandData::Remove {
                        key: encode(key)
                    }).collect_vec();
            batch_run_and_print(&mut client, vec_batch_rm, "Done!").await?;
        }
        Command::BatchGet { keys } => {
            let vec_batch_get = keys.into_iter()
                .map(|key|
                    CommandData::Get {
                        key: encode(key)
                    }).collect_vec();
            batch_run_and_print(&mut client, vec_batch_get, "Nil").await?;
        }
    }

    Ok(())
}

async fn batch_run_and_print(mut client: &mut Client, vec_batch: Vec<CommandData>, default: &str) -> Result<()> {
    let vec_result = batch_run(&mut client, vec_batch).await?
        .into_iter()
        .map(|option| match option {
            None => { default.to_string() }
            Some(value) => { value }
        })
        .collect_vec();
    print!("{:?}", vec_result);

    Ok(())
}

async fn batch_run(client: &mut Client, vec_batch_get: Vec<CommandData>) -> Result<Vec<Option<String>>> {
    Ok(client.batch(vec_batch_get).await?
        .into_iter()
        .map(|option_vec_u8| option_vec_u8.map(decode))
        .collect_vec())
}

fn encode(value: String) -> Vec<u8>{
    bincode::serialize(&value).unwrap()
}

fn decode(value: Vec<u8>) -> String {
    bincode::deserialize(&*value).unwrap()
}