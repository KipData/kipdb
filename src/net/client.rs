use tokio::net::{TcpStream, ToSocketAddrs};
use crate::cmd::Command;
use crate::net::connection::Connection;
use crate::net::Result;

pub struct Client {
    connection: Connection
}

impl Client {
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> Result<Client> {
        let socket = TcpStream::connect(addr).await?;

        let connection = Connection::new(socket);

        Ok(Client{
            connection
        })
    }

    pub async fn set(&mut self, key: String, value: String) -> Result<()>{
        self.send_cmd(Command::set(key, value))?;
        Ok(())
    }

    pub async fn remove(&mut self, key: String) -> Result<()>{
        self.send_cmd(Command::remove(key))?;
        Ok(())
    }

    pub async fn get(&mut self, key: String) -> Result<Option<Command>>{
        self.send_cmd(Command::get(key))
    }

    async fn send_cmd(&mut self, cmd: Command) -> Result<Option<Command>>{
        self.connection.write(cmd).await?;
        Ok(self.connection.read().await?)
    }
}