use clap::{Parser};
use kip_db::cmd::Command;
use kip_db::DEFAULT_PORT;
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
            client.set(encode_key(key)?, encode_value_one(value)?).await?;
            print!("Done!");
        }
        Command::Remove { key } => {
            client.remove(encode_key(key)?).await?;
            print!("Done!");
        }
        Command::Get { key } => {
            if let Some(value) = client.get(encode_key(key)?).await? {
                print!("\"{}\"", decode_value_one(value)?);
            } else {
                print!("(Nil)");
            }
        }
    }

    Ok(())
}

fn encode_key(key: String) -> Result<Vec<u8>>{
    Ok(bincode::serialize(&key)?)
}

fn encode_value_one(value: String) -> Result<Vec<u8>>{
    Ok(bincode::serialize(&value)?)
}

fn decode_value_one(value: Vec<u8>) -> Result<String> {
    Ok(bincode::deserialize(&*value)?)
}