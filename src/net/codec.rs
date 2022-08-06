use futures::AsyncReadExt;
use tokio_util::codec::{Decoder, Encoder};
use crate::cmd::Command;
use crate::net::ConnectionError;

pub struct CommandCodec;

impl CommandCodec {
    pub fn new() -> CommandCodec{
        CommandCodec{ }
    }
}

impl Encoder<Command> for CommandCodec {
    type Error = ConnectionError;

    fn encode(&mut self, item: Command, dst: &mut bytes::bytes_mut::BytesMut) -> Result<(), Self::Error> {
        dst.extend(bincode::serialize(&item));
        Ok(())
    }
}

impl Decoder for CommandCodec {
    type Item = Command;
    type Error = ConnectionError;

    fn decode(&mut self, src: &mut bytes::bytes_mut::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None)
        }

        let data = src.split();

        Ok(Some(bincode::deserialize(&data[..])?))
    }
}