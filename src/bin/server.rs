use clap::Parser;

use kip_db::server::client::Result;
use kip_db::server::server::serve;
use kip_db::{DEFAULT_PORT, LOCAL_IP};

/// 服务启动方法
/// 二进制执行文件调用方法:./kip-db-cli
#[tokio::main]
pub async fn main() -> Result<()> {
    tracing_subscriber::fmt::try_init().unwrap();

    let cli = Cli::parse();
    let ip = cli.ip.unwrap_or(LOCAL_IP.to_string());
    let port = cli.port.unwrap_or(DEFAULT_PORT);

    serve(&ip, port).await?;

    Ok(())
}

#[derive(Parser, Debug)]
#[clap(name = "KipDB-Server", version, author, about = "KipDB Net Server")]
struct Cli {
    #[clap(long)]
    ip: Option<String>,
    #[clap(long)]
    port: Option<u16>,
}
