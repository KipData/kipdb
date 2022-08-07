use tokio::net::{TcpStream, ToSocketAddrs};
use crate::cmd::Command;
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
    pub async fn set(&mut self, key: String, value: String) -> Result<()>{
        self.send_cmd(Command::set(key, value)).await?;
        Ok(())
    }

    /// 删除数据
    pub async fn remove(&mut self, key: String) -> Result<()>{
        self.send_cmd(Command::remove(key)).await?;
        Ok(())
    }

    /// 获取数据
    pub async fn get(&mut self, key: String) -> Result<core::option::Option<String>>{
        match self.send_cmd(Command::get(key)).await? {
            CommandOption::Value(str) => Ok(Some(str)),
            _ => Ok(None)
        }
    }

    async fn send_cmd(&mut self, cmd: Command) -> Result<CommandOption>{
        self.connection.write(CommandOption::Cmd(cmd)).await?;
        Ok(self.connection.read().await?)
    }
}