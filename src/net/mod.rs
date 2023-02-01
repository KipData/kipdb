use crate::error::ConnectionError;

use prost::Message;
use crate::kernel::ByteUtils;
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

fn kv_encode_with_len(key_value: &KeyValue) -> Result<Vec<u8>> {
    let mut vec = vec![];

    key_value.encode(&mut vec)
        .map_err(|_| ConnectionError::EncodeError)?;

    if !vec.is_empty() {
        Ok(ByteUtils::tag_with_head(vec))
    } else {
        Err(ConnectionError::KvStoreError(KvsError::DataEmpty))
    }
}