use tokio::net::{TcpStream, ToSocketAddrs};
use crate::error::ConnectionError;
use crate::kernel::CommandData;
use crate::KvsError;
use crate::net::connection::Connection;
use crate::net::{Result, CommandOption};

#[allow(missing_debug_implementations)]
pub struct Client {
    connection: Connection
}

impl Client {
    /// 与客户端进行连接
    #[inline]
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> Result<Client> {
        let socket = TcpStream::connect(addr).await?;

        let connection = Connection::new(socket);

        Ok(Client{
            connection
        })
    }

    /// 存入数据
    #[inline]
    pub async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>{
        let _ignore = self.send_cmd(CommandOption::Cmd(CommandData::set(key, value))).await?;
        Ok(())
    }

    /// 删除数据
    #[inline]
    pub async fn remove(&mut self, key: Vec<u8>) -> Result<()>{
        let _ignore = self.send_cmd(CommandOption::Cmd(CommandData::remove(key))).await?;
        Ok(())
    }

    /// 获取数据
    #[inline]
    pub async fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>>{
        match self.send_cmd(CommandOption::Cmd(CommandData::get(key))).await? {
            CommandOption::Value(vec) => Ok(Some(vec)),
            _ => Err(ConnectionError::KvStoreError(KvsError::NotMatchCmd))
        }
    }

    /// 刷入硬盘
    #[inline]
    pub async fn flush(&mut self) -> Result<()>{
        match self.send_cmd(CommandOption::Flush).await? {
            CommandOption::Flush => Ok(()),
            _ => Err(ConnectionError::RemoteFlushError)
        }
    }

    /// 批量处理
    #[inline]
    pub async fn batch(&mut self, batch_cmd: Vec<CommandData>, is_parallel: bool) -> Result<Vec<Option<Vec<u8>>>>{
        match self.send_cmd(CommandOption::VecCmd(batch_cmd, is_parallel)).await? {
            CommandOption::ValueVec(vec) => Ok(vec),
            _ => Err(ConnectionError::KvStoreError(KvsError::NotMatchCmd))
        }
    }

    /// 磁盘占用
    #[inline]
    pub async fn size_of_disk(&mut self) -> Result<u64> {
        match self.send_cmd(CommandOption::SizeOfDisk(0)).await? {
            CommandOption::SizeOfDisk(size_of_disk) => {Ok(size_of_disk)},
            _ => Err(ConnectionError::KvStoreError(KvsError::NotMatchCmd))
        }
    }

    /// 数据数量
    #[inline]
    pub async fn len(&mut self) -> Result<usize> {
        match self.send_cmd(CommandOption::Len(0)).await? {
            CommandOption::Len(len) => {Ok(len)},
            _ => Err(ConnectionError::KvStoreError(KvsError::NotMatchCmd))
        }
    }

    #[inline]
    async fn send_cmd(&mut self, cmd_option: CommandOption) -> Result<CommandOption>{
        self.connection.write(cmd_option).await?;
        self.connection.read().await
    }
}