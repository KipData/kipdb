use clap::Parser;
use tokio::net::TcpListener;
use tokio::signal;

use kvs::DEFAULT_PORT;
use kvs::net::server;

#[tokio::main]
pub async fn main() -> kvs::net::Result<()> {
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