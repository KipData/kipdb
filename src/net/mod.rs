use crate::error::ConnectionError;

use crate::kernel::CommandData;
use crate::KvsError;
use crate::proto::net_pb::CommandOption;

mod connection;
mod codec;
pub mod client;
pub mod server;
mod shutdown;

pub type Result<T> = std::result::Result<T, ConnectionError>;

fn option_from_data(data: &CommandData) -> Result<CommandOption> {
    Ok(CommandOption {
        r#type: 0,
        bytes: bincode::serialize(data)
            .map_err(|_| ConnectionError::EncodeError)?,
    })
}

fn data_from_option(option: &CommandOption) -> Result<CommandData> {
    if option.r#type != 0 {
        Err(ConnectionError::KvStoreError(KvsError::NotMatchCmd))
    } else {
        Ok(bincode::deserialize(&option.bytes)
            .map_err(|_| ConnectionError::DecodeError)?)
    }
}