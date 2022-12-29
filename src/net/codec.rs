use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};
use crate::error::ConnectionError;
use crate::net::CommandOption;

pub(crate) struct NetCommandCodec;

/// CommandOption编码器
/// 用于CommandOption网络传输解析抽象
impl NetCommandCodec {
    pub(crate) fn new() -> NetCommandCodec {
        NetCommandCodec { }
    }
}

impl Encoder<CommandOption> for NetCommandCodec {
    type Error = ConnectionError;

    fn encode(&mut self, item: CommandOption, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend(bincode::serialize(&item)?);
        Ok(())
    }
}

impl Decoder for NetCommandCodec {
    type Item = CommandOption;
    type Error = ConnectionError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None)
        }

        let data = src.split();

        Ok(Some(bincode::deserialize(&data[..])?))
    }
}