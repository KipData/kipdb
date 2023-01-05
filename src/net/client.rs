use itertools::Itertools;
use tokio::net::{TcpStream, ToSocketAddrs};
use prost::Message;
use crate::error::ConnectionError;
use crate::kernel::{CommandData, CommandPackage};
use crate::KvsError;
use crate::net::connection::Connection;
use crate::net::{kv_encode_with_len, option_from_key_value, Result};
use crate::proto::net_pb::{CommandOption, KeyValue};

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
        let key_value = KeyValue { key, value, r#type: 1 };
        
        let _ignore = self.send_cmd(option_from_key_value(&key_value)?).await?;
        Ok(())
    }

    /// 删除数据
    #[inline]
    pub async fn remove(&mut self, key: Vec<u8>) -> Result<()>{
        let key_value = KeyValue { key, value: vec![], r#type: 2 };
        
        let _ignore = self.send_cmd(option_from_key_value(&key_value)?).await?;
        Ok(())
    }

    /// 获取数据
    #[inline]
    pub async fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>>{
        let key_value = KeyValue { key, value: vec![], r#type: 0 };

        Ok(self.send_cmd(option_from_key_value(&key_value)?)
            .await?
            .into())
    }

    /// 刷入硬盘
    #[inline]
    pub async fn flush(&mut self) -> Result<()>{
        let option = CommandOption {
            r#type: 6,
            bytes: vec![],
            value: 0,
        };

        if self.send_cmd(option).await?.r#type == 6 {
            Ok(())
        } else { Err(ConnectionError::FlushError) }
    }

    /// 批量处理
    #[inline]
    pub async fn batch(&mut self, batch_cmd: Vec<CommandData>) -> Result<Vec<Option<Vec<u8>>>>{
        // 将KeyValue序列化后合并以传递给服务端
        let bytes = batch_cmd
            .into_iter()
            .map(KeyValue::from)
            .filter_map(|key_value|
                kv_encode_with_len(&key_value).ok())
            .flatten()
            .collect_vec();

        let send_option = CommandOption {
            r#type: 1,
            bytes,
            value: 0,
        };

        let result_option = self.send_cmd(send_option).await?;

        if result_option.r#type == 1 {
            Ok(CommandPackage::get_vec_bytes(&result_option.bytes)
                .into_iter()
                .map(|vec_u8| {
                    KeyValue::decode(vec_u8).ok()
                        .map(|key_value| key_value.value)
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
            value: 0,
        };

        let result_option = self.send_cmd(send_option).await?;

        // 此处与服务端呼应使用value进行数据传递
        if result_option.r#type == type_num {
            Ok(result_option.value)
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