use crate::error::ConnectionError;
use crate::proto::CommandOption;
use bytes::BytesMut;
use prost::Message;
use tokio_util::codec::{Decoder, Encoder};

pub(crate) struct NetCommandCodec;

/// CommandOption编码器
/// 用于CommandOption网络传输解析抽象
impl NetCommandCodec {
    pub(crate) fn new() -> NetCommandCodec {
        NetCommandCodec {}
    }
}

impl Encoder<CommandOption> for NetCommandCodec {
    type Error = ConnectionError;

    fn encode(&mut self, item: CommandOption, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut buf = vec![];
        item.encode(&mut buf)
            .map_err(|_| ConnectionError::EncodeErr)?;
        dst.extend(buf);
        Ok(())
    }
}

impl Decoder for NetCommandCodec {
    type Item = CommandOption;
    type Error = ConnectionError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        Ok(if src.is_empty() {
            None
        } else {
            Some(CommandOption::decode(src.split()).map_err(|_| ConnectionError::DecodeErr)?)
        })
    }
}
