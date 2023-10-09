use crate::error::ConnectionError;

use crate::kernel::ByteUtils;
use crate::proto::{CommandOption, KeyValue};
use crate::KernelError;
use prost::Message;

pub mod client;
mod codec;
mod connection;
pub mod server;
mod shutdown;

pub type Result<T> = std::result::Result<T, ConnectionError>;

/// KeyValue转换为CommandOption
fn option_from_key_value(kv: &KeyValue) -> Result<CommandOption> {
    let mut bytes = vec![];
    kv.encode(&mut bytes)
        .map_err(|_| ConnectionError::EncodeErr)?;

    Ok(CommandOption {
        r#type: 0,
        bytes,
        value: 0,
    })
}

/// CommandOption转换为KeyValue
fn key_value_from_option(option: &CommandOption) -> Result<KeyValue> {
    if option.r#type != 0 {
        Err(ConnectionError::StoreErr(KernelError::NotMatchCmd))
    } else {
        Ok(KeyValue::decode(&*option.bytes).map_err(|_| ConnectionError::DecodeErr)?)
    }
}

fn kv_encode_with_len(key_value: &KeyValue) -> Result<Vec<u8>> {
    let mut vec = vec![];

    key_value
        .encode(&mut vec)
        .map_err(|_| ConnectionError::EncodeErr)?;

    if !vec.is_empty() {
        Ok(ByteUtils::tag_with_head(vec))
    } else {
        Err(ConnectionError::StoreErr(KernelError::DataEmpty))
    }
}
