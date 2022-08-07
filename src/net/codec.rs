use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};
use crate::error::ConnectionError;
use crate::net::CommandOption;

pub struct CommandCodec;

/// CommandOption编码器
/// 用于CommandOption网络传输解析抽象
impl CommandCodec {
    pub fn new() -> CommandCodec{
        CommandCodec{ }
    }
}

impl Encoder<CommandOption> for CommandCodec {
    type Error = ConnectionError;

    fn encode(&mut self, item: CommandOption, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend(bincode::serialize(&item)?);
        Ok(())
    }
}

impl Decoder for CommandCodec {
    type Item = CommandOption;
    type Error = ConnectionError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<std::option::Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None)
        }

        let data = src.split();

        Ok(Some(bincode::deserialize(&data[..])?))
    }
}