use itertools::Itertools;
use tokio::net::{TcpStream, ToSocketAddrs};
use crate::error::ConnectionError;
use crate::kernel::{CommandData, CommandPackage};
use crate::KvsError;
use crate::net::connection::Connection;
use crate::net::{option_from_data, Result};
use crate::proto::net_pb::CommandOption;

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
        let cmd = &CommandData::set(key, value);
        
        let _ignore = self.send_cmd(option_from_data(cmd)?).await?;
        Ok(())
    }

    /// 删除数据
    #[inline]
    pub async fn remove(&mut self, key: Vec<u8>) -> Result<()>{
        let cmd = &CommandData::remove(key);
        
        let _ignore = self.send_cmd(option_from_data(cmd)?).await?;
        Ok(())
    }

    /// 获取数据
    #[inline]
    pub async fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>>{
        let cmd = &CommandData::get(key);

        Ok(self.send_cmd(option_from_data(cmd)?)
            .await?
            .into())
    }

    /// 刷入硬盘
    #[inline]
    pub async fn flush(&mut self) -> Result<()>{
        let option = CommandOption {
            r#type: 6,
            bytes: vec![],
        };

        if self.send_cmd(option).await?.r#type == 6 {
            Ok(())
        } else { Err(ConnectionError::FlushError) }
    }

    /// 批量处理
    #[inline]
    pub async fn batch(&mut self, batch_cmd: Vec<CommandData>) -> Result<Vec<Option<Vec<u8>>>>{
        let bytes = batch_cmd
            .iter()
            .filter_map(|data| CommandPackage::trans_to_vec_u8(data).ok())
            .flatten()
            .collect_vec();

        let send_option = CommandOption {
            r#type: 1,
            bytes,
        };

        let result_option = self.send_cmd(send_option).await?;

        if result_option.r#type == 1 {
            Ok(CommandPackage::from_bytes_to_unpack_vec(&result_option.bytes)?
                .into_iter()
                .map(|cmd_data| {
                    let key = cmd_data.get_key_owner();
                    if key.is_empty() {
                        None
                    } else {
                        Some(key)
                    }
                })
                .collect())
        } else {
            Err(ConnectionError::KvStoreError(KvsError::NotMatchCmd))
        }
    }

    /// 磁盘占用
    #[inline]
    pub async fn size_of_disk(&mut self) -> Result<u64> {
        self.value_option(4).await
    }

    /// 数据数量
    #[inline]
    pub async fn len(&mut self) -> Result<usize> {
        self.value_option(5).await
            .map(|len| len as usize)
    }

    /// 数值控制选项通用流程
    async fn value_option(&mut self, type_num: i32) -> Result<u64>
    {
        let send_option = CommandOption {
            r#type: type_num,
            bytes: vec![],
        };

        let result_option = self.send_cmd(send_option).await?;

        if result_option.r#type == type_num {
            Ok(bincode::deserialize(&result_option.bytes)
                .map_err(|_| ConnectionError::DecodeError)?)
        } else {
            Err(ConnectionError::KvStoreError(KvsError::NotMatchCmd))
        }
    }

    #[inline]
    async fn send_cmd(&mut self, cmd_option: CommandOption) -> Result<CommandOption>{
        self.connection.write(cmd_option).await?;
        self.connection.read().await
    }
}