use crate::error::ConnectionError;

use prost::Message;
use crate::KvsError;
use crate::proto::net_pb::{CommandOption, KeyValue};

mod connection;
mod codec;
pub mod client;
pub mod server;
mod shutdown;

pub type Result<T> = std::result::Result<T, ConnectionError>;

/// KeyValue转换为CommandOption
fn option_from_key_value(kv: &KeyValue) -> Result<CommandOption> {
    let mut bytes = vec![];
    kv.encode(&mut bytes)
        .map_err(|_| ConnectionError::EncodeError)?;

    Ok(CommandOption {
        r#type: 0,
        bytes,
        value: 0,
    })
}

/// CommandOption转换为KeyValue
fn key_value_from_option(option: &CommandOption) -> Result<KeyValue> {
    if option.r#type != 0 {
        Err(ConnectionError::KvStoreError(KvsError::NotMatchCmd))
    } else {
        Ok(KeyValue::decode(&*option.bytes)
            .map_err(|_| ConnectionError::DecodeError)?)
    }
}

/// 模拟`CommandPackage::trans_to_vec_u8`方法的序列化
/// 与`CommandPackage::get_vec_bytes`可以对应使用
fn kv_encode_with_len(key_value: &KeyValue) -> Result<Vec<u8>> {
    let mut vec = vec![];
    key_value
        .encode(&mut vec)
        .map_err(|_| ConnectionError::EncodeError)?;

    if !vec.is_empty() {
        let i = vec.len();
        let mut vec_head = vec![(i >> 24) as u8,
                                (i >> 16) as u8,
                                (i >> 8) as u8,
                                i as u8];
        vec_head.append(&mut vec);
        Ok(vec_head)
    } else {
        Err(ConnectionError::KvStoreError(KvsError::DataEmpty))
    }
}