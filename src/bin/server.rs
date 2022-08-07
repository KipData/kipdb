use clap::Parser;
use tokio::net::TcpListener;
use tokio::signal;

use kip_db::DEFAULT_PORT;
use kip_db::net::{server, Result};

#[tokio::main]
pub async fn main() -> Result<()> {
    tracing_subscriber::fmt::try_init().unwrap();

    let cli = Cli::parse();
    let port = cli.port.unwrap_or(DEFAULT_PORT);

    // Bind a TCP listener
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;

    server::run(listener, signal::ctrl_c()).await;

    Ok(())
}

#[derive(Parser, Debug)]
#[clap(name = "KipDB-Server", version, author, about = "A KV-Store server")]
struct Cli {
    #[clap(long)]
    port: Option<u16>
}