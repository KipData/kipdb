use clap::Parser;
use tokio::net::TcpListener;

use kip_db::{DEFAULT_PORT, LOCAL_IP};
use kip_db::net::{server, Result};

/// 服务启动方法
/// 二进制执行文件调用方法:./kip-db-cli
#[tokio::main]
pub async fn main() -> Result<()> {
    tracing_subscriber::fmt::try_init().unwrap();

    let cli = Cli::parse();
    let ip = cli.ip.unwrap_or(LOCAL_IP.to_string());
    let port = cli.port.unwrap_or(DEFAULT_PORT);

    // Bind a TCP listener
    let listener = TcpListener::bind(&format!("{}:{}", ip, port)).await?;

    server::run(listener, quit()).await;

    Ok(())
}

pub async fn quit() -> Result<()> {
    #[cfg(unix)]
    {
        let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            _ = interrupt.recv() => (),
            _ = terminate.recv() => (),
        }
        Ok(())
    }
    #[cfg(windows)]
    {
        Ok(tokio::signal::ctrl_c().await?)
    }
}

#[derive(Parser, Debug)]
#[clap(name = "KipDB-Server", version, author, about = "A KV-Store server")]
struct Cli {
    #[clap(long)]
    ip: Option<String>,
    #[clap(long)]
    port: Option<u16>
}