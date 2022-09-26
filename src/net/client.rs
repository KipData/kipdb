use tokio::net::{TcpStream, ToSocketAddrs};
use crate::kernel::CommandData;
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
        self.send_cmd(CommandOption::Cmd(CommandData::set(key, value))).await?;
        Ok(())
    }

    /// 删除数据
    pub async fn remove(&mut self, key: Vec<u8>) -> Result<()>{
        self.send_cmd(CommandOption::Cmd(CommandData::remove(key))).await?;
        Ok(())
    }

    /// 获取数据
    pub async fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>>{
        match self.send_cmd(CommandOption::Cmd(CommandData::get(key))).await? {
            CommandOption::Value(vec) => Ok(Some(vec)),
            _ => Ok(None)
        }
    }

    /// 批量处理
    pub async fn batch(&mut self, batch_cmd: Vec<CommandData>) -> Result<Vec<Option<Vec<u8>>>>{
        match self.send_cmd(CommandOption::VecCmd(batch_cmd)).await? {
            CommandOption::ValueVec(vec) => Ok(vec),
            _ => Ok(Vec::new())
        }
    }

    async fn send_cmd(&mut self, cmd_option: CommandOption) -> Result<CommandOption>{
        self.connection.write(cmd_option).await?;
        Ok(self.connection.read().await?)
    }
}