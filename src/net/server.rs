use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, RwLock, Semaphore};
use tokio::time;
use tracing::{debug, error, info};
use crate::kernel::hash_kv::HashStore;
use crate::kernel::KVStore;
use crate::error::ConnectionError;
use crate::net::connection::Connection;
use crate::net::Result;
use crate::net::CommandOption;

const MAX_CONNECTIONS: usize = 250;

/// 服务器监听器
/// 用于监听端口的连接并分发给Handler进行多线程处理连接
pub struct Listener {
    kv_store_root: Arc<RwLock<dyn KVStore + Send + Sync>>,
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
    notify_shutdown_sender: broadcast::Sender<()>,
    _shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>
}

/// 连接处理器
/// 用于每个连接的响应处理
struct Handler {
    kv_store: Arc<RwLock<dyn KVStore + Send + Sync>>,
    connection: Connection,
    notify_receiver: broadcast::Receiver<()>,
    shutdown: bool,
    // 用于与Listener保持连接而感应是否全部关闭
    _shutdown_complete: mpsc::Sender<()>
}

pub async fn run(listener: TcpListener, shutdown: impl Future) {
    let store = HashStore::open("./data").unwrap();
    let (notify_shutdown_sender, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    let sync_store = Arc::new(RwLock::new(store));

    let mut server = Listener {
        listener,
        kv_store_root: sync_store.clone(),
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown_sender,
        shutdown_complete_tx,
        _shutdown_complete_rx: shutdown_complete_rx
    };

    tokio::select! {
        res = server.run() => {
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            sync_store.write()
            .await
            .shut_down()
            .expect("persistence exception");
            info!("shutting down");
        }
    }
}

impl Listener {
    async fn run(&mut self) -> Result<()> {
        info!("accepting inbound connections");
        loop {
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            let socket = self.accept().await?;
            let addr = socket.peer_addr()?;

            let mut handler = Handler {
                kv_store: self.kv_store_root.clone(),
                connection: Connection::new(socket),
                notify_receiver: self.notify_shutdown_sender.subscribe(),
                shutdown: false,
                _shutdown_complete: self.shutdown_complete_tx.clone()
            };

            tokio::spawn(async move {
                info!("#### new connection! ####");
                let id = snowflake::ProcessUniqueId::new();
                info!("id: {}", &id);
                info!("ip addr: {}", &addr);
                let start = Instant::now();
                if let Err(err) = handler.run().await {
                    error!(cause = ?err,"connection error");
                }
                let duration = start.elapsed();
                drop(permit);
                info!("time: {:?}", duration);
                info!("#### connection drop! ####")
            });

        }

    }

    /// 获取连接
    async fn accept(&mut self) -> Result<TcpStream> {
        let mut backoff = 1;

        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => {
                    return Ok(socket)
                }
                Err(err) => {
                    if backoff > 64 {
                        return Err(err.into())
                    }
                }
            }

            time::sleep(Duration::from_secs(backoff)).await;

            backoff *= 2;

        }
    }
}

impl Handler {
    async fn run(&mut self) -> Result<()> {
        while !self.shutdown {

            let option: CommandOption = tokio::select! {
                res = self.connection.read() => res?,
                _ = self.notify_receiver.recv() => {
                    return Ok(());
                }
            };

            return match option {
                CommandOption::Cmd(cmd) => {
                    debug!(?cmd);

                    let option = cmd.apply(&mut self.kv_store).await.unwrap();

                    self.connection.write(option).await?;

                    return Ok(());
                }
                _ => Err(ConnectionError::WrongInstruction)
            }
        }

        Ok(())
    }
}