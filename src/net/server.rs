use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use chrono::Local;
use itertools::Itertools;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time;
use tracing::{error, info};
use crate::error::ConnectionError;
use crate::kernel::{CommandData, CommandPackage, KVStore};
use crate::kernel::lsm::lsm_kv::LsmStore;
use crate::net::connection::Connection;
use crate::net::{data_from_option, Result};
use crate::net::shutdown::Shutdown;
use crate::proto::net_pb::CommandOption;

const MAX_CONNECTIONS: usize = 250;

/// 服务器监听器
/// 用于监听端口的连接并分发给Handler进行多线程处理连接
#[derive(Debug)]
pub struct Listener {
    kv_store_root: Arc<LsmStore>,
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>
}

/// 连接处理器
/// 用于每个连接的响应处理
struct Handler {
    kv_store: Arc<LsmStore>,
    connection: Connection,
    shutdown: Shutdown,
    // 用于与Listener保持连接而感应是否全部关闭
    _shutdown_complete: mpsc::Sender<()>
}

#[inline]
pub async fn run(listener: TcpListener, shutdown: impl Future) -> Result<()> {
    let kv_store_root = Arc::new(LsmStore::open("./data").await?);
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    let mut server = Listener {
        listener,
        kv_store_root,
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
        shutdown_complete_rx,
    };

    tokio::select! {
        res = server.run() => {
            if let Err(err) = res {
                error!(cause = %err, "[Listener][Failed To Accept]");
            }
        }
        _ = shutdown => {
            server.kv_store_root.flush().await?;
            info!("[Listener][Shutting Down]");
        }
    }
    // Extract the `shutdown_complete` receiver and transmitter
    // explicitly drop `shutdown_transmitter`. This is important, as the
    // `.await` below would otherwise never complete.
    let Listener {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;

    // When `notify_shutdown` is dropped, all tasks which have `subscribe`d will
    // receive the shutdown signal and can exit
    drop(notify_shutdown);
    // Drop final `Sender` so the `Receiver` below can complete
    drop(shutdown_complete_tx);

    // Wait for all active connections to finish processing. As the `Sender`
    // handle held by the listener has been dropped above, the only remaining
    // `Sender` instances are held by connection handler tasks. When those drop,
    // the `mpsc` channel will close and `recv()` will return `None`.
    let _ = shutdown_complete_rx.recv().await;

    Ok(())
}

impl Listener {
    #[allow(clippy::unwrap_used)]
    async fn run(&mut self) -> Result<()> {
        info!("[Listener][Inbound Connections]");
        loop {
            let permit = Arc::clone(&self
                .limit_connections)
                .acquire_owned()
                .await
                .unwrap();

            let socket = self.accept().await?;
            let addr = socket.peer_addr()?;

            let mut handler = Handler {
                kv_store: Arc::clone(&self.kv_store_root),
                connection: Connection::new(socket),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone()
            };

            let _ignore = tokio::spawn(async move {
                info!("[Listener][New Connection][Time: {}][Ip Addr]: {}", Local::now(), &addr);
                let start = Instant::now();
                if let Err(err) = handler.run().await {
                    error!(cause = ?err,"[Listener][Handler Running Error]");
                }
                drop(permit);
                info!("[Listener][Connection Drop][Time: {:?}]", start.elapsed());
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
        while !self.shutdown.is_shutdown() {
            let client_option: CommandOption = tokio::select! {
                res = self.connection.read() => res?,
                _ = self.shutdown.recv() => {
                    // If a shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    return Ok(());
                }
            };

            match client_option.r#type {
                0 => {
                    let res_option = data_from_option(&client_option)?
                        .apply(&*self.kv_store).await?;

                    self.connection.write(res_option).await?;
                }
                1 => {
                    let vec_cmd = CommandPackage::from_bytes_to_unpack_vec(&client_option.bytes)?;
                    let bytes = self.kv_store.batch(vec_cmd).await?
                        .into_iter()
                        .filter_map(|value_option| {
                            let command_data = CommandData::get(
                                value_option.map_or(vec![], |value| value)
                            );
                            CommandPackage::trans_to_vec_u8(&command_data).ok()
                        })
                        .flatten()
                        .collect_vec();
                    self.connection.write(CommandOption { r#type: 1, bytes }).await?;
                }
                4 => {
                    let size_of_disk = self.kv_store.size_of_disk().await?;
                    self.value_options(size_of_disk, 4).await?;
                }
                5 => {
                    let len = self.kv_store.len().await? as u64;
                    self.value_options(len, 5).await?;
                }
                6 => {
                    self.kv_store.flush().await?;
                    self.connection.write(CommandOption { r#type: 6, bytes: vec![] }).await?;
                }
                7 => {
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }

    async fn value_options(&mut self, size_of_disk: u64, type_num: i32) -> Result<()> {
        let bytes = bincode::serialize(&size_of_disk)
            .map_err(|_| ConnectionError::EncodeError)?;
        self.connection.write(CommandOption { r#type: type_num, bytes }).await?;

        Ok(())
    }
}

