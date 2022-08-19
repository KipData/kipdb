use tokio::net::{TcpStream, ToSocketAddrs};
use crate::core::CommandData;
use crate::net::connection::Connection;
use crate::net::{Result, CommandOption};

pub struct Client {
    connection: Connection
}

impl Client {
    /// 与客户端进行连接
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> Result<Client> {
        let socket = TcpStream::connect(addr).await?;

        let connection = Connection::new(socket);

        Ok(Client{
            connection
        })
    }

    /// 存入数据
    pub async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>{
        self.send_cmd(CommandData::set(key, value)).await?;
        Ok(())
    }

    /// 删除数据
    pub async fn remove(&mut self, key: Vec<u8>) -> Result<()>{
        self.send_cmd(CommandData::remove(key)).await?;
        Ok(())
    }

    /// 获取数据
    pub async fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>>{
        match self.send_cmd(CommandData::get(key)).await? {
            CommandOption::Value(vec) => Ok(Some(vec)),
            _ => Ok(None)
        }
    }

    async fn send_cmd(&mut self, cmd: CommandData) -> Result<CommandOption>{
        self.connection.write(CommandOption::Cmd(cmd)).await?;
        Ok(self.connection.read().await?)
    }
}